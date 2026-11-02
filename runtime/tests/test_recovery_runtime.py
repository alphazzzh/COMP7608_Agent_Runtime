"""跨全新适配器实例的持久化执行公开验收测试。

测试替身使用本地端口、真实 SQLite 存储与真实 JSONL 审计文件。
事件关卡在回复已知之后，或调用结果未知期间中断执行；
测试不访问控制器内部，也不编辑检查点记录。
全新的内容实例刻意要求使用各自的临时目录句柄。
"""

from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import pytest

from lake_agent import models as m
from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink


def recovery_api():
    """在测试内部解析拟定的公开接口，保留有诊断价值的初始失败输出。"""
    runtime = importlib.import_module("lake_agent.runtime")
    state = importlib.import_module("lake_agent.state")
    return runtime.resume, state.SqliteStateStore, state.StateStoreError


def request(**config):
    """使用严格的数据调用额度，使重复的目录或工具派发可被观察。"""
    limits = {"max_data_calls": 2, "max_model_calls": 4, **config}
    return m.RunRequest(
        run_id="recoverable-run",
        task_id="durable-offline-fixture",
        question="What was 2024 annual revenue?",
        config=m.RunConfig(**limits),
    )


class RecordingData(DemoData):
    """统计真实数据调用，并通过事件暴露结果未知或并行执行状态。"""

    def __init__(self, *, hold_tool=False, parallel=False, contract_revision=1):
        super().__init__()
        self.catalog_requests = []
        self.tool_requests = []
        self.returned_sources = []
        self.entered = asyncio.Event()
        self.quarter_returned = asyncio.Event()
        self.hold_tool = hold_tool
        self.parallel = parallel
        self.contract_revision = contract_revision

    def tool_specs(self):
        specs = super().tool_specs()
        return [
            spec.model_copy(
                update={
                    "parallel_safe": self.parallel,
                    "description": f"Recovery fixture contract v{self.contract_revision}",
                }
            )
            for spec in specs
        ]

    async def catalog(self, req):
        self.catalog_requests.append(req)
        return await super().catalog(req)

    async def invoke(self, req):
        self.tool_requests.append(req)
        self.entered.set()
        if self.hold_tool:
            await asyncio.Future()
        # 第二个数据源必须先完成，以检查每个已缓冲原始回复都能恢复，
        # 且不受串行审计提交顺序影响。
        if self.parallel and req.args["source_id"] == "demo-annual":
            await self.quarter_returned.wait()
        reply = await super().invoke(req)
        self.returned_sources.append(req.args["source_id"])
        if req.args["source_id"] == "demo-quarter":
            self.quarter_returned.set()
        return reply


class RecordingContent(DemoContent):
    """使用每个实例独立的句柄，并在最终消费者处验证来源信息。"""

    def __init__(self):
        super().__init__()
        self.instance_id = uuid4().hex
        self.preparations = []
        self.searches = []
        self.compositions = []
        self.verifications = []

    async def prepare_catalog(self, req):
        self.preparations.append(req)
        reply = await super().prepare_catalog(req)
        transient = f"{reply.data.handle}-{self.instance_id}"
        self.catalogs[transient] = self.catalogs.pop(reply.data.handle)
        return reply.model_copy(
            update={"data": reply.data.model_copy(update={"handle": transient})}
        )

    async def search(self, req):
        self.searches.append(req)
        return await super().search(req)

    async def compose(self, req):
        self.compositions.append(req)
        # compose/verify 也可以访问本地目录索引。即使跳过缓存中的搜索，
        # 也必须已经对新实例执行重新绑定。
        assert self.catalogs, "recovery did not bind the fresh content instance"
        return await super().compose(req)

    async def verify(self, req):
        self.verifications.append(req)
        return await super().verify(req)


class RecordingPlanner(DemoPlanner):
    """报告实际模型用量，依据请求状态而非调用历史作出决策。"""

    def __init__(self, *, two_tools=False):
        self.decompositions = []
        self.selections = []
        self.two_tools = two_tools

    @staticmethod
    def paid(reply):
        return reply.model_copy(
            update={"usage": m.Usage(provider_requests=1, input_tokens=7, output_tokens=3)}
        )

    async def decompose(self, req):
        self.decompositions.append(req)
        return self.paid(await super().decompose(req))

    async def select_tools(self, req):
        self.selections.append(req)
        reply = await super().select_tools(req)
        if self.two_tools and not req.evidence_summary:
            steps = [
                m.ToolStep(
                    step_id=f"extract-{candidate.source_id}",
                    subtask_id=req.subtask.subtask_id,
                    tool_name="extract",
                    source_ids=[candidate.source_id],
                    args={
                        "source_id": candidate.source_id,
                        "query": req.subtask.search_query,
                        "max_chars": 4000,
                    },
                    purpose="Read independently recoverable synthetic evidence",
                )
                for candidate in req.candidates
            ]
            reply = reply.model_copy(update={"data": m.ToolPlan(steps=steps)})
        return self.paid(reply)


class InterruptingTrace(JsonlTraceSink):
    """提交指定的公开事件，再取消后续审计过程或使其失败。"""

    def __init__(self, root, *, event_type="evidence.added", fail=False):
        super().__init__(root)
        self.event_type = event_type
        self.fail = fail
        self.entered = asyncio.Event()
        self.interrupted_event = None

    async def append(self, event):
        await super().append(event)
        if event.event_type == self.event_type and self.interrupted_event is None:
            self.interrupted_event = event
            self.entered.set()
            if self.fail:
                raise OSError("controlled audit continuation failure")
            await asyncio.Future()


async def cancel_at_gate(task, entered):
    """始终等待所有者任务结束，包括缺陷实现发生挂起时。"""
    gate = asyncio.create_task(entered.wait())
    try:
        done, _ = await asyncio.wait({task, gate}, timeout=3, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
            pytest.fail("run terminated before reaching the interruption gate")
        assert gate in done, "run did not reach the interruption gate"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
    finally:
        gate.cancel()
        with suppress(asyncio.CancelledError):
            await gate
        if not task.done():
            task.cancel()
        # 即使执行在抵达事件关卡前失败，也要获取其异常。
        # try 块中的原始故障仍是有用的诊断信息。
        with suppress(asyncio.CancelledError, Exception):
            await task


def trace_events(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()]


def assert_no_port_calls(data, content, planner):
    """元数据发现属于本地操作；拒绝恢复时不得运行任何异步适配器方法。"""
    assert data.catalog_requests == data.tool_requests == []
    assert content.preparations == content.searches == content.compositions == []
    assert content.verifications == []
    assert planner.decompositions == planner.selections == []


@pytest.mark.parametrize("event_type", ["tool.observed", "evidence.added"])
async def test_resume_reconstructs_known_replies_without_redispatch(tmp_path, event_type):
    """控制器内存丢失后，不能重复已经成功的外部工具调用。"""
    resume, Store, _ = recovery_api()
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    old_data, old_content, old_planner = RecordingData(), RecordingContent(), RecordingPlanner()
    interrupted = InterruptingTrace(trace_root, event_type=event_type)
    store = Store(state_path)
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=old_data,
                content=old_content,
                planner=old_planner,
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
    finally:
        await store.aclose()
    original_paths = sorted(trace_root.rglob("events.jsonl"))
    assert len(original_paths) == 1
    original_bytes = original_paths[0].read_bytes()
    assert len(old_data.catalog_requests) == len(old_data.tool_requests) == 1
    assert len(old_planner.decompositions) == len(old_planner.selections) == 1

    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    restarted = Store(state_path)
    try:
        result = await resume(
            "recoverable-run",
            state_store=restarted,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
        )
    finally:
        await restarted.aclose()
    assert (result.run_id, result.status, result.stop_reason) == (
        "recoverable-run",
        "completed",
        "verified_supported",
    )
    assert data.catalog_requests == data.tool_requests == []
    assert planner.decompositions == []
    assert len(planner.selections) == 1
    assert len(content.preparations) == 1  # 显式重新绑定本地临时句柄。
    assert len(content.searches) == 1  # 常规的工具调用后搜索属于新的执行前沿。
    assert content.searches[0].catalog_handle in content.catalogs
    assert len(content.compositions) == len(content.verifications) == 1
    assert (result.call_counts.catalog, result.call_counts.tools, result.call_counts.steps) == (
        1,
        1,
        1,
    )
    assert (result.call_counts.planner, result.call_counts.model_calls) == (3, 3)
    assert result.call_counts.content == 6  # 原始五次调用，加上一次重新绑定。
    assert (result.usage.input_tokens, result.usage.output_tokens) == (21, 9)
    evidence = content.compositions[0].evidence
    assert len(evidence) == 1
    assert evidence[0].call_id == old_data.tool_requests[0].call_id
    assert result.answer.citations == [evidence[0].evidence_id]
    if event_type == "evidence.added":
        assert interrupted.interrupted_event.data_ref == evidence[0].evidence_id

    assert original_paths[0].read_bytes() == original_bytes
    assert Path(result.trace_ref) != original_paths[0]
    assert len(list(trace_root.rglob("events.jsonl"))) == 2
    events = trace_events(result.trace_ref)
    assert events[0]["event_type"] == "run.resumed"
    assert events[-1]["event_type"] == "run.completed"
    assert all(event["run_id"] == result.run_id for event in events)
    assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
    cached = [
        event
        for event in events
        if event["event_type"].endswith(".call.completed") and event["detail"].get("replayed")
    ]
    assert {event["event_type"] for event in cached} >= {
        "catalog.call.completed",
        "content.prepare_catalog.call.completed",
        "planner.decompose.call.completed",
        "content.search.call.completed",
        "planner.select_tools.call.completed",
        "tool.call.completed",
    }
    assert {
        event["call_id"] for event in cached if event["event_type"] == "tool.call.completed"
    } == {old_data.tool_requests[0].call_id}
    for event in events:
        if event["data_ref"]:
            assert (
                Path(result.trace_ref).parent / "artifacts" / f"{event['data_ref']}.json"
            ).exists()


async def test_resume_refuses_unknown_tool_outcome_even_when_idempotent(tmp_path):
    """幂等性允许正常重试，但无法判定已丢失的调用结果。"""
    resume, Store, StateStoreError = recovery_api()
    store = Store(tmp_path / "state.db")
    first = RecordingData(hold_tool=True)
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=first,
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        )
        await cancel_at_gate(task, first.entered)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        with pytest.raises(StateStoreError) as unknown:
            await resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        assert unknown.value.code == "recovery_outcome_unknown"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


@pytest.mark.parametrize("data_limit", [1, 2])
async def test_resume_returns_stored_terminal_result_without_calls(tmp_path, data_limit):
    """成功运行与正常因预算停止的运行都属于终结状态快照。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request().model_copy(update={"config": m.RunConfig(max_data_calls=data_limit)})
    try:
        result = await run(
            req,
            data=RecordingData(),
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=JsonlTraceSink(tmp_path / "traces"),
            state_store=store,
        )
        assert result.status == ("completed" if data_limit == 2 else "budget_exceeded")
        for _ in range(2):
            data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
            recovered = await resume(
                result.run_id,
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
            assert recovered.model_dump(mode="json") == result.model_dump(mode="json")
            assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


async def test_resume_rejects_changed_trusted_tool_contract_before_calls(tmp_path):
    """缓存计划不能静默继承已经变化的工具能力元数据。"""
    resume, Store, StateStoreError = recovery_api()
    store = Store(tmp_path / "state.db")
    interrupted = InterruptingTrace(tmp_path / "traces")
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=RecordingData(),
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        data, content, planner = (
            RecordingData(contract_revision=2),
            RecordingContent(),
            RecordingPlanner(),
        )
        with pytest.raises(StateStoreError) as changed:
            await resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        assert changed.value.code == "recovery_contract_mismatch"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


async def test_resume_preserves_original_deadline_during_downtime(tmp_path):
    """重启进程不能重新获得一份完整的总运行时间额度。"""
    resume, Store, StateStoreError = recovery_api()
    store = Store(tmp_path / "state.db")
    interrupted = InterruptingTrace(tmp_path / "traces")
    try:
        task = asyncio.create_task(
            run(
                request(max_wall_time_ms=1000),
                data=RecordingData(),
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        await asyncio.sleep(1.05)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        try:
            result = await resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        except StateStoreError as expired:
            assert expired.code == "recovery_deadline_expired"
        else:
            assert (result.status, result.stop_reason) == ("timeout", "time_budget")
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


async def test_resume_keeps_consumed_model_budget(tmp_path):
    """重放已付费的回复时，仍须计入原始运行的模型额度。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    interrupted = InterruptingTrace(tmp_path / "traces")
    req = request().model_copy(update={"config": m.RunConfig(max_model_calls=3, max_data_calls=2)})
    try:
        task = asyncio.create_task(
            run(
                req,
                data=RecordingData(),
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        result = await resume(
            req.run_id,
            state_store=store,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert (result.status, result.stop_reason) == ("budget_exceeded", "model_call_budget")
        assert result.call_counts.model_calls == result.call_counts.planner == 3
        assert (result.call_counts.catalog, result.call_counts.tools) == (1, 1)
        assert data.catalog_requests == data.tool_requests == []
        assert planner.decompositions == []
        assert len(planner.selections) == 1
        assert len(content.preparations) == 1
        assert content.compositions == content.verifications == []
    finally:
        await store.aclose()


async def test_audit_failure_does_not_repeat_already_returned_tool(tmp_path):
    """必须先持久化回复，再进行可能失败的观测审计写入。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    data = RecordingData()
    interrupted = InterruptingTrace(tmp_path / "traces", event_type="tool.observed", fail=True)
    try:
        stopped = await run(
            request(),
            data=data,
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=interrupted,
            state_store=store,
        )
        assert stopped.stop_reason == "trace_write_failed"
        assert len(data.tool_requests) == 1
        fresh_data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        result = await resume(
            stopped.run_id,
            state_store=store,
            data=fresh_data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "completed"
        assert fresh_data.catalog_requests == fresh_data.tool_requests == []
        assert content.compositions[0].evidence[0].call_id == data.tool_requests[0].call_id
        assert result.call_counts.tools == 1
    finally:
        await store.aclose()


async def test_parallel_reverse_completion_reuses_every_known_reply(tmp_path):
    """首次串行审计期间崩溃，不能抹掉同批已返回任务的回复。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request().model_copy(update={"config": m.RunConfig(max_data_calls=3, max_model_calls=4)})
    data = RecordingData(parallel=True)
    interrupted = InterruptingTrace(tmp_path / "traces", event_type="tool.observed")
    try:
        task = asyncio.create_task(
            run(
                req,
                data=data,
                content=RecordingContent(),
                planner=RecordingPlanner(two_tools=True),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        assert data.returned_sources == ["demo-quarter", "demo-annual"]
        fresh_data = RecordingData(parallel=True)
        content, planner = RecordingContent(), RecordingPlanner(two_tools=True)
        result = await resume(
            req.run_id,
            state_store=store,
            data=fresh_data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "completed"
        assert fresh_data.catalog_requests == fresh_data.tool_requests == []
        assert (result.call_counts.catalog, result.call_counts.tools, result.call_counts.steps) == (
            1,
            2,
            2,
        )
        evidence = content.compositions[0].evidence
        assert {item.source_id for item in evidence} == {"demo-annual", "demo-quarter"}
        assert {item.call_id for item in evidence} == {req.call_id for req in data.tool_requests}
        assert len(evidence) == 2
    finally:
        await store.aclose()
