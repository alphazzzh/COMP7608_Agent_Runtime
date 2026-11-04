"""审计准入、恢复请求与源码身份、调用日志的严格兼容边界。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
from contextlib import suppress
from importlib import metadata
from pathlib import Path

import pytest

import lake_agent
from lake_agent.engine.recovery import _implementation_digest
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink
from tests.support.recovery import (
    InterruptingTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    UnavailableAttemptTrace,
    assert_no_port_calls,
    cancel_at_gate,
    recovery_api,
    request,
)


async def test_healthy_resume_after_initial_trace_admission_failure(tmp_path):
    """不可用的审计接收器不能将尚未开始工作的运行永久缓存为终结状态。"""
    store = SqliteStateStore(tmp_path / "state.db")
    trace_root = tmp_path / "traces"
    try:
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        stopped = await run(
            request(),
            data=data,
            content=content,
            planner=planner,
            trace=UnavailableAttemptTrace(trace_root),
            state_store=store,
        )
        assert (stopped.status, stopped.stop_reason) == ("failed", "trace_sink_unavailable")
        assert_no_port_calls(data, content, planner)

        fresh_data, fresh_content, fresh_planner = (
            RecordingData(),
            RecordingContent(),
            RecordingPlanner(),
        )
        result = await resume(
            stopped.run_id,
            data=fresh_data,
            content=fresh_content,
            planner=fresh_planner,
            trace=JsonlTraceSink(trace_root),
            state_store=store,
        )
        assert (result.status, result.stop_reason) == ("completed", "agent_finished")
        assert result.run_id == stopped.run_id
        assert len(fresh_data.catalog_requests) == len(fresh_data.tool_requests) == 1
        assert len(fresh_planner.requests) == 5
        assert len(fresh_content.preparations) == 1
    finally:
        await store.aclose()


async def interrupted_run(store, trace_root):
    """仅创建结果已知的调用，将模式校验拒绝与 I/O 结果丢失区分开来。"""
    trace = InterruptingTrace(trace_root)
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


@pytest.mark.parametrize(
    "key,value",
    [
        ("seed", ""),
        ("seed", 123),
        ("attempts", "not-a-list"),
        ("started_epoch", True),
        ("deadline_epoch", -1),
    ],
)
async def test_invalid_run_metadata_refused_before_ports(tmp_path, key, value):
    """格式错误的种子或时间戳绝不能静默切换到新的调用序列。"""
    store = SqliteStateStore(tmp_path / "state.db")
    try:
        await interrupted_run(store, tmp_path / "traces")
        lease = await store.acquire("recoverable-run")
        state = (await store.load(lease)).state
        state[key] = value
        await store.save(lease, state)
        await store.release(lease)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        with pytest.raises(StateStoreError) as refused:
            await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        assert refused.value.code == "recovery_state_invalid"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


class ForeignRequestStore(SqliteStateStore):
    """自定义提供方返回有效 JSON，但其中请求的任务所有者属于其他任务。"""

    corrupt = False

    async def load(self, lease):
        loaded = await super().load(lease)
        if self.corrupt:
            entry = next(v for v in loaded.calls.values() if v["phase"] == "tool")
            entry["request"]["task_id"] = "another-task"
        return loaded


async def test_foreign_request_task_refused_during_preflight(tmp_path):
    """整个调用日志通过所有权校验之前，不得开始重建。"""
    store = ForeignRequestStore(tmp_path / "state.db")
    try:
        await interrupted_run(store, tmp_path / "traces")
        store.corrupt = True
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        with pytest.raises(StateStoreError) as refused:
            await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        assert refused.value.code == "recovery_state_invalid"
        assert_no_port_calls(data, content, planner)
        assert len(list((tmp_path / "traces").rglob("events.jsonl"))) == 1
    finally:
        await store.aclose()


@pytest.mark.parametrize(
    "package", ["langgraph", "langchain-core", "langgraph-checkpoint", "langsmith"]
)
def test_graph_dependency_version_changes_recovery_identity(monkeypatch, package):
    """仅变更引擎版本也必须被源代码兼容性关卡拦截。"""
    original = metadata.version
    before = _implementation_digest()

    def changed_version(name):
        if name == package:
            return "999.0.0-test"
        return original(name)

    monkeypatch.setattr(metadata, "version", changed_version)
    assert _implementation_digest() != before


@pytest.mark.parametrize(
    "relative_path", ["engine/graph_runner.py", "adapters/planner.py", "state.py"]
)
def test_production_source_in_subpackages_changes_recovery_identity(monkeypatch, relative_path):
    """目录归档后仍检查真实执行代码，不能因只扫描顶层而跨实现恢复。"""
    target = Path(lake_agent.__file__).parent / relative_path
    original = Path.read_bytes
    before = _implementation_digest()

    def changed_source(path):
        content = original(path)
        return content + b"\n# simulated implementation change\n" if path == target else content

    monkeypatch.setattr(Path, "read_bytes", changed_source)
    assert _implementation_digest() != before


def test_report_schema_does_not_change_execution_recovery_identity(monkeypatch):
    """仅修改报告字段不会改变模型执行、预算或持久化响应的解释。"""
    target = Path(lake_agent.__file__).parent / "evaluation" / "models.py"
    original = Path.read_bytes
    before = _implementation_digest()

    def changed_source(path):
        content = original(path)
        return content + b"\n# simulated report-only change\n" if path == target else content

    monkeypatch.setattr(Path, "read_bytes", changed_source)
    assert _implementation_digest() == before


def test_ordinary_runtime_import_does_not_require_posix_state_backend():
    """缺少可选 fcntl 模块时，普通 run 接口仍必须可以导入。"""
    script = textwrap.dedent(
        """
        import builtins
        original_import = builtins.__import__

        def without_fcntl(name, *args, **kwargs):
            if name == "fcntl":
                raise ModuleNotFoundError("fcntl is unavailable on this platform")
            return original_import(name, *args, **kwargs)

        builtins.__import__ = without_fcntl
        from lake_agent.runtime import run
        assert callable(run)
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr


async def _interrupt_known_run(store, trace_root, *, event_type, req=None):
    """留下包含已知调用的日志，并在后续审计过程被中断时停止。"""
    trace = InterruptingTrace(trace_root, event_type=event_type)
    task = asyncio.create_task(
        run(
            req or request(),
            data=RecordingData(),
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=trace,
            state_store=store,
        )
    )
    await cancel_at_gate(task, trace.entered)


@pytest.mark.parametrize("invalid_reply", [False, True])
async def test_swallowed_rebind_cancellation_blocks_new_frontier_calls(tmp_path, invalid_reply):
    """本地注册不能吞掉调用方的取消信号并继续执行。"""

    class SuppressingContent(RecordingContent):
        def __init__(self):
            super().__init__()
            self.entered = asyncio.Event()

        async def prepare_catalog(self, req):
            self.entered.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                # 故意使用有缺陷的适配器，验证控制器在这一额外边界
                # 仍有责任保留取消信号。
                if invalid_reply:
                    return {"call_id": req.call_id, "status": "invalid"}
                return await super().prepare_catalog(req)

    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    task = None
    try:
        await _interrupt_known_run(store, tmp_path / "traces", event_type="evidence.added")
        data, content, planner = RecordingData(), SuppressingContent(), RecordingPlanner()
        task = asyncio.create_task(
            resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        )
        await asyncio.wait_for(content.entered.wait(), timeout=3)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert content.searches == content.compositions == content.verifications == []
        assert data.catalog_requests == data.tool_requests == []
        assert planner.requests == []
        assert task.cancelled(), "a swallowed local cancellation became a completed resume"
    finally:
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        await store.aclose()


async def test_frontier_planner_gets_remaining_wall_time_after_rebind(tmp_path):
    """新规划请求的预算必须反映本地注册消耗，以及原始到期时间。"""

    class DelayedContent(RecordingContent):
        async def prepare_catalog(self, req):
            # 模拟索引重建工作。断言比较两个请求字段，
            # 不使用机器速度或经过时间的阈值。
            await asyncio.sleep(0.05)
            return await super().prepare_catalog(req)

    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request(max_wall_time_ms=10_000, per_call_timeout_ms=10_000)
    try:
        await _interrupt_known_run(
            store,
            tmp_path / "traces",
            event_type="tool.call.started",
            req=req,
        )
        planner = RecordingPlanner()
        result = await resume(
            req.run_id,
            state_store=store,
            data=RecordingData(),
            content=DelayedContent(),
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "completed"
        assert planner.requests
        frontier = planner.requests[0]
        # 单次调用上限等于原始整次运行额度，因此两个边界均使用相同的全局到期时间。
        # 跨过恢复边界后，整数向下或向上取整使得
        # 剩余时长满足 wall_time_ms <= timeout_ms。
        assert frontier.budget_remaining.wall_time_ms <= frontier.timeout_ms
    finally:
        await store.aclose()


async def test_expired_auxiliary_intent_never_dispatches_registration(tmp_path):
    """持久化准入过程可能在进入适配器前耗尽剩余时间。"""
    resume, Store, _ = recovery_api()

    class SlowAuxiliaryIntentStore(Store):
        async def begin_call(self, lease, call_id, value):
            await super().begin_call(lease, call_id, value)
            if value.get("auxiliary"):
                # 模拟原始截止时间检查之后的慢速持久化存储。
                # 仅靠已经到期的超时上下文，无法在下一次事件循环调度前
                # 阻止同步进入适配器。
                await asyncio.sleep(1.05)

    store = SlowAuxiliaryIntentStore(tmp_path / "state.db")
    try:
        await _interrupt_known_run(
            store,
            tmp_path / "traces",
            event_type="evidence.added",
            req=request(max_wall_time_ms=1000),
        )
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        result = await resume(
            "recoverable-run",
            state_store=store,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "timeout"
        assert content.preparations == content.searches == content.compositions == []
        assert content.verifications == []
        assert data.catalog_requests == data.tool_requests == []
        assert planner.requests == []
    finally:
        await store.aclose()


async def test_frontier_tool_gets_fresh_call_window_after_registration(tmp_path):
    """独立的恢复注册过程不能占用新工具调用的超时额度。"""

    class DelayedContent(RecordingContent):
        async def prepare_catalog(self, req):
            await asyncio.sleep(0.3)
            return await super().prepare_catalog(req)

    class DelayedData(RecordingData):
        async def invoke(self, req):
            await asyncio.sleep(0.3)
            return await super().invoke(req)

    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request(
        max_wall_time_ms=10_000,
        per_call_timeout_ms=500,
        max_retries_per_data_operation=0,
    )
    try:
        # 在这一审计关卡尚无工具适配器调用，也无持久化工具意图。
        # 因此，恢复过程在工具批次准入时才抵达首次新 I/O。
        await _interrupt_known_run(
            store, tmp_path / "traces", event_type="tool.call.started", req=req
        )
        data, content, planner = DelayedData(), DelayedContent(), RecordingPlanner()
        result = await resume(
            req.run_id,
            state_store=store,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        # 每项操作均短于各自独立的半秒上限。
        # 两者总和超过单个上限，从而暴露在重新绑定前就开始计时的超时窗口。
        assert (result.status, result.stop_reason) == ("completed", "agent_finished")
        assert len(data.tool_requests) == 1
        assert len(content.preparations) == 1
        assert (result.call_counts.tools, result.call_counts.steps) == (1, 1)
    finally:
        await store.aclose()


async def test_caller_cancellation_survives_durable_release_failure(tmp_path):
    """后续存储错误不能替换调用方的取消信号。"""
    _, Store, StateStoreError = recovery_api()

    class ReleaseFailingStore(Store):
        async def release(self, lease):
            await super().release(lease)
            raise StateStoreError("storage_error")

    store = ReleaseFailingStore(tmp_path / "state.db")
    task = None
    try:
        data = RecordingData(hold_tool=True)
        task = asyncio.create_task(
            run(
                request(),
                data=data,
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        )
        await asyncio.wait_for(data.entered.wait(), timeout=3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert task.cancelled()
        # release 在抛出异常前已经解锁，因此仍可取得新租约；
        # 清理失败也不能掩盖原始取消。
        lease = await store.acquire("recoverable-run")
        with pytest.raises(StateStoreError):
            await store.release(lease)
    finally:
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        await store.aclose()
