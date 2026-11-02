"""在任何新适配器接收 I/O 前，拒绝格式错误的持久化封装。

存储层验证 JSON 完整性；这些测试覆盖独立的运行时模式与所有权边界，
也包括自定义 StateStore 提供方。
"""

import asyncio

import pytest
from test_recovery_runtime import (
    InterruptingTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    cancel_at_gate,
    request,
)

from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink


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
