"""辅助目录重新注册所声明用量的公开回归测试。

仅执行本地操作的端口仍可报告已测量或未知的 token 数量。
控制器必须跨重启累计这些声明。意外向提供方派发请求属于契约违例，
但该请求的实际用量记账必须保留。
"""

from __future__ import annotations

import asyncio

import pytest
from test_recovery_edges import FrontierTrace
from test_recovery_runtime import (
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    cancel_at_gate,
    request,
    trace_events,
)

from lake_agent.models import Usage
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink


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
        assert planner.decompositions == planner.selections == []
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
        (Usage(provider_requests=0, input_tokens=2, output_tokens=1), (23, 10)),
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
    first_resume_trace = FrontierTrace(trace_root, "content.search.call.completed")
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
        assert len(first_content.preparations) == len(first_content.searches) == 1
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
    assert len(content.preparations) == len(content.compositions) == len(content.verifications) == 1
    assert planner.decompositions == []
    assert len(planner.selections) == 1
    assert result.call_counts.content == 7  # 五次普通调用，加上两次重新绑定。
    assert result.call_counts.model_calls == result.usage.provider_requests == 3
    assert (result.usage.input_tokens, result.usage.output_tokens) == expected_tokens
