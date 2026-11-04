"""多次中断、存储与审计故障、实现变化及恢复用量结算。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
from pathlib import Path

import pytest

import lake_agent
from lake_agent.models import Usage
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink
from tests.support import RUNTIME_ROOT
from tests.support.recovery import (
    FrontierTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    cancel_at_gate,
    request,
    trace_events,
)


class FailingCallStore(SqliteStateStore):
    """使一次公开工具调用账本写入在真实 SQLite 提交之前或之后失败。"""

    def __init__(self, path, *, boundary, after_commit):
        super().__init__(path)
        self.boundary = boundary
        self.after_commit = after_commit
        self.failed = False

    async def begin_call(self, lease, call_id, value):
        if self.boundary == "begin" and value["phase"] == "tool" and not self.failed:
            self.failed = True
            if self.after_commit:
                await super().begin_call(lease, call_id, value)
            raise StateStoreError("storage_error")
        await super().begin_call(lease, call_id, value)

    async def finish_call(self, lease, call_id, value):
        if self.boundary == "finish" and value["phase"] == "tool" and not self.failed:
            self.failed = True
            if self.after_commit:
                await super().finish_call(lease, call_id, value)
            raise StateStoreError("storage_error")
        await super().finish_call(lease, call_id, value)


class FailingToolResultTrace(JsonlTraceSink):
    """拒绝第一个已返回工具结果的产物，且不写入任何字节。"""

    def __init__(self, root):
        super().__init__(root)
        self.failed = False

    async def put_artifact(self, ref, value):
        if (
            not self.failed
            and isinstance(value, dict)
            and isinstance(value.get("data"), dict)
            and "evidence" in value["data"]
        ):
            self.failed = True
            raise OSError("controlled tool-result artifact failure")
        await super().put_artifact(ref, value)


async def test_multiple_interruptions_rebind_each_fresh_content_instance(tmp_path):
    """辅助重新绑定的历史不能改变 ID、重放调用或重置计数。"""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    adapters = []
    interrupted = []
    saved_traces = {}
    for index, event_type in enumerate(
        ["evidence.added", "planner.decide.call.completed", "content.compose.call.completed"]
    ):
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        adapters.append((data, content, planner))
        trace = FrontierTrace(trace_root, event_type)
        store = SqliteStateStore(state_path)
        try:
            if index == 0:
                operation = run(
                    request(),
                    data=data,
                    content=content,
                    planner=planner,
                    trace=trace,
                    state_store=store,
                )
            else:
                operation = resume(
                    "recoverable-run",
                    data=data,
                    content=content,
                    planner=planner,
                    trace=trace,
                    state_store=store,
                )
            await cancel_at_gate(asyncio.create_task(operation), trace.entered)
        finally:
            await store.aclose()
        interrupted.append(trace.interrupted_event)
        for path, expected in saved_traces.items():
            assert path.read_bytes() == expected
        saved_traces = {path: path.read_bytes() for path in trace_root.rglob("events.jsonl")}
        assert len(saved_traces) == index + 1

    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    adapters.append((data, content, planner))
    store = SqliteStateStore(state_path)
    try:
        result = await resume(
            "recoverable-run",
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=store,
        )
    finally:
        await store.aclose()
    assert (result.status, result.stop_reason) == ("completed", "agent_finished")
    assert (result.call_counts.catalog, result.call_counts.tools, result.call_counts.steps) == (
        1,
        1,
        1,
    )
    assert (result.call_counts.planner, result.call_counts.model_calls) == (5, 5)
    assert result.call_counts.content == 7  # 四次普通内容调用，三次本地重新绑定。
    assert (result.usage.input_tokens, result.usage.output_tokens) == (35, 15)
    assert sum(len(d.catalog_requests) for d, _, _ in adapters) == 1
    assert sum(len(d.tool_requests) for d, _, _ in adapters) == 1
    assert sum(len(c.preparations) for _, c, _ in adapters) == 4
    assert sum(len(c.searches) for _, c, _ in adapters) == 1
    assert sum(len(c.compositions) for _, c, _ in adapters) == 1
    assert sum(len(c.verifications) for _, c, _ in adapters) == 1
    assert sum(len(p.requests) for _, _, p in adapters) == 5
    assert data.catalog_requests == data.tool_requests == []
    assert content.searches == content.compositions == []
    assert len(planner.requests) == 2
    assert result.answer.citations == [interrupted[0].data_ref]
    assert content.verifications[0].evidence[0].evidence_id == interrupted[0].data_ref
    paths = list(trace_root.rglob("events.jsonl"))
    assert len(paths) == 4
    assert len({path.parent for path in paths}) == 4
    for path, expected in saved_traces.items():
        assert path.read_bytes() == expected
    for path in paths:
        events = trace_events(path)
        assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
        assert events[0]["event_type"] in {"run.started", "run.resumed"}
        assert all(event["run_id"] == result.run_id for event in events)


@pytest.mark.parametrize("boundary", ["begin", "finish"])
@pytest.mark.parametrize("after_commit", [False, True], ids=["before-commit", "after-commit"])
async def test_storage_failure_freezes_dispatch_and_recovers_only_known_outcomes(
    tmp_path, boundary, after_commit
):
    """只有持久化记录能证明未派发调用或已取得有效回复时，才允许继续。"""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    store = FailingCallStore(state_path, boundary=boundary, after_commit=after_commit)
    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    try:
        with pytest.raises(StateStoreError) as failed:
            await run(
                request(),
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(trace_root),
                state_store=store,
            )
        assert failed.value.code == "storage_error"
        assert store.failed
        assert len(data.catalog_requests) == 1
        assert len(data.tool_requests) == (0 if boundary == "begin" else 1)
        assert len(planner.requests) == 2
        assert len(content.searches) == len(content.preparations) == 1
        assert content.compositions == content.verifications == []
    finally:
        await store.aclose()

    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        # 若意图已持久化却没有回复，结果便属于未知，即使测试知道
        # 所注入的故障发生在真正进入适配器函数体之前。
        unknown = (boundary == "begin" and after_commit) or (
            boundary == "finish" and not after_commit
        )
        if unknown:
            with pytest.raises(StateStoreError) as blocked:
                await resume(
                    "recoverable-run",
                    data=data,
                    content=content,
                    planner=planner,
                    trace=JsonlTraceSink(trace_root),
                    state_store=restarted,
                )
            assert blocked.value.code == "recovery_outcome_unknown"
            assert_no_port_calls(data, content, planner)
        else:
            result = await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(trace_root),
                state_store=restarted,
            )
            assert result.status == "completed"
            assert data.catalog_requests == []
            assert len(data.tool_requests) == (1 if boundary == "begin" else 0)
            assert (result.call_counts.catalog, result.call_counts.tools) == (1, 1)
            assert result.call_counts.model_calls == 5
    finally:
        await restarted.aclose()


async def test_valid_reply_survives_failure_writing_its_trace_artifact(tmp_path):
    """在可能失败的审计结果写入开始前，调用日志必须已经持久化保存回复。"""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    store = SqliteStateStore(state_path)
    data = RecordingData()
    trace = FailingToolResultTrace(trace_root)
    try:
        stopped = await run(
            request(),
            data=data,
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=trace,
            state_store=store,
        )
        assert trace.failed
        assert (stopped.status, stopped.stop_reason) == ("failed", "trace_write_failed")
        assert len(data.tool_requests) == 1
        call_id = data.tool_requests[0].call_id
        events = trace_events(stopped.trace_ref)
        assert not any(event["event_type"] == "tool.call.completed" for event in events)
    finally:
        await store.aclose()
    fresh_data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        result = await resume(
            stopped.run_id,
            data=fresh_data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=restarted,
        )
    finally:
        await restarted.aclose()
    assert result.status == "completed"
    assert fresh_data.catalog_requests == fresh_data.tool_requests == []
    assert content.compositions[0].evidence[0].call_id == call_id
    assert result.call_counts.tools == 1
    replayed = [
        event
        for event in trace_events(result.trace_ref)
        if event["event_type"] == "tool.call.completed"
    ]
    assert len(replayed) == 1
    assert replayed[0]["detail"]["replayed"] is True


async def test_runtime_source_change_refuses_recovery_in_fresh_interpreter(tmp_path):
    """实现发生变化后，不能静默沿用旧的调用日志。"""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    store = SqliteStateStore(state_path)
    trace = FrontierTrace(trace_root, "evidence.added")
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=RecordingData(),
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=trace,
                state_store=store,
            )
        )
        await cancel_at_gate(task, trace.entered)
    finally:
        await store.aclose()
    isolated_root = tmp_path / "different-runtime"
    isolated_package = isolated_root / "lake_agent"
    shutil.copytree(
        Path(lake_agent.__file__).parent,
        isolated_package,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    # 即使源代码差异无害，也会改变严格的实现身份；
    # 不修改私有摘要辅助函数或持久化状态模式。
    with (isolated_package / "runtime.py").open("a") as changed:
        changed.write("\n# Deliberately different runtime build for compatibility acceptance.\n")
    program = """
import asyncio, json, sys
sys.path.insert(0, sys.argv[3])
sys.path.insert(1, sys.argv[4])
from lake_agent.runtime import resume
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink
from tests.support.recovery import RecordingData, RecordingContent, RecordingPlanner, assert_no_port_calls

async def main():
    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    store = SqliteStateStore(sys.argv[1])
    try:
        try:
            await resume('recoverable-run', state_store=store, data=data, content=content,
                         planner=planner, trace=JsonlTraceSink(sys.argv[2]))
        except StateStoreError as error:
            assert_no_port_calls(data, content, planner)
            print(json.dumps({'code': error.code}))
        else:
            raise AssertionError('changed runtime unexpectedly resumed')
    finally:
        await store.aclose()
asyncio.run(main())
"""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        program,
        str(state_path),
        str(trace_root),
        str(isolated_root),
        str(RUNTIME_ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(child.communicate(), timeout=10)
        assert child.returncode == 0, stderr.decode()
        assert json.loads(stdout) == {"code": "recovery_version_mismatch"}
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


class MeteredContent(RecordingContent):
    """保持实现不变，仅更改该实例的用量报告。"""

    def __init__(self, reported_usage=None):
        super().__init__()
        self.reported_usage = reported_usage or Usage.local()

    async def prepare_catalog(self, req):
        reply = await super().prepare_catalog(req)
        return reply.model_copy(update={"usage": self.reported_usage})


async def interrupt_original(state_path, trace_root):
    """留下两次已知付费规划调用和一次已知成功的工具回复。"""
    store = SqliteStateStore(state_path)
    trace = FrontierTrace(trace_root, "evidence.added")
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=RecordingData(),
                content=MeteredContent(),
                planner=RecordingPlanner(),
                trace=trace,
                state_store=store,
            )
        )
        await cancel_at_gate(task, trace.entered)
    finally:
        await store.aclose()


async def test_auxiliary_local_port_violation_preserves_actual_provider_charge(tmp_path):
    """拒绝新增执行工作，同时保留已经报告的提供方调用。"""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    await interrupt_original(state_path, trace_root)
    usage = Usage(provider_requests=1, input_tokens=11, output_tokens=5)
    data, content, planner = RecordingData(), MeteredContent(usage), RecordingPlanner()
    store = SqliteStateStore(state_path)
    try:
        with pytest.raises(StateStoreError) as invalid:
            await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(trace_root),
                state_store=store,
            )
        assert invalid.value.code == "recovery_catalog_mismatch"
        assert len(content.preparations) == 1
        assert content.searches == content.compositions == content.verifications == []
        assert data.catalog_requests == data.tool_requests == []
        assert planner.requests == []
        # 约定的拒绝方式是抛出异常，而不是返回业务结果。
        # 通过公开存储检查接口，仍必须能够查看最后已知的记账状态。
        lease = await store.acquire("recoverable-run")
        try:
            progress = (await store.load(lease)).state["progress"]
        finally:
            await store.release(lease)
        assert progress["call_counts"]["content"] == 3  # 原始 prepare/search 调用，再加上重新绑定。
        assert progress["call_counts"]["model_calls"] == 3  # 两次规划调用，加上违例调用。
        assert (progress["usage"]["input_tokens"], progress["usage"]["output_tokens"]) == (25, 11)
        failed_events = [
            event
            for path in trace_root.rglob("events.jsonl")
            for event in trace_events(path)
            if event["event_type"] == "recovery.prepare_catalog.call.failed"
        ]
        assert len(failed_events) == 1
        assert failed_events[0]["call_id"] == content.preparations[0].call_id
        assert failed_events[0]["usage"] == usage.model_dump(mode="json")
        assert failed_events[0]["status"] == "invalid_response"
    finally:
        await store.aclose()
    # 无效注册仍可供检查，但不能授权再次恢复。
    # 拒绝调用日志时，不能调用另一个本地端口，也不能增加其请求用量。
    fresh_data, fresh_content, fresh_planner = RecordingData(), MeteredContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        with pytest.raises(StateStoreError) as blocked:
            await resume(
                "recoverable-run",
                data=fresh_data,
                content=fresh_content,
                planner=fresh_planner,
                trace=JsonlTraceSink(trace_root),
                state_store=restarted,
            )
        assert blocked.value.code == "recovery_state_invalid"
        assert_no_port_calls(fresh_data, fresh_content, fresh_planner)
    finally:
        await restarted.aclose()


@pytest.mark.parametrize(
    "reported_usage,expected_tokens",
    [
        (Usage(provider_requests=0, input_tokens=2, output_tokens=1), (37, 16)),
        (Usage(provider_requests=0, input_tokens=None, output_tokens=None), (None, None)),
    ],
    ids=["measured-local-tokens", "unknown-local-tokens"],
)
async def test_prior_auxiliary_usage_survives_another_interruption(
    tmp_path, reported_usage, expected_tokens
):
    """重建普通调用时，也必须保留此前本地重新绑定所报告的用量。"""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    await interrupt_original(state_path, trace_root)
    first_content = MeteredContent(reported_usage)
    first_resume_trace = FrontierTrace(trace_root, "content.compose.call.completed")
    store = SqliteStateStore(state_path)
    try:
        task = asyncio.create_task(
            resume(
                "recoverable-run",
                data=RecordingData(),
                content=first_content,
                planner=RecordingPlanner(),
                trace=first_resume_trace,
                state_store=store,
            )
        )
        await cancel_at_gate(task, first_resume_trace.entered)
        assert len(first_content.preparations) == len(first_content.compositions) == 1
        assert first_content.searches == []
    finally:
        await store.aclose()
    data, content, planner = RecordingData(), MeteredContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        result = await resume(
            "recoverable-run",
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=restarted,
        )
    finally:
        await restarted.aclose()
    assert result.status == "completed"
    assert data.catalog_requests == data.tool_requests == []
    assert content.searches == []
    assert len(content.preparations) == len(content.verifications) == 1
    assert content.compositions == []
    assert len(planner.requests) == 2
    assert result.call_counts.content == 6  # 四次普通内容调用，加上两次重新绑定。
    assert result.call_counts.model_calls == result.usage.provider_requests == 5
    assert (result.usage.input_tokens, result.usage.output_tokens) == expected_tokens
