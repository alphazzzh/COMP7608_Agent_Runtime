"""agent-loop-v1 协议准入、决策日志、模型预算和规划取消恢复。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest
from pydantic import ValidationError

from lake_agent import models as m
from lake_agent.engine.agent_protocol import validate_agent_planner
from lake_agent.engine.errors import RunStopped
from lake_agent.engine.recovery import _request_type, _response_type
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink
from tests.support.recovery import (
    InterruptingTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    cancel_at_gate,
    request,
    trace_events,
)


class IncompatiblePlanner(RecordingPlanner):
    """模拟宿主传入不支持当前动作协议的端口；绝不允许实际派发。"""

    agent_protocol = "unsupported-protocol"

    async def decide(self, request):
        raise AssertionError("invalid protocol must be rejected before planner dispatch")


class AgentRecoveryPlanner:
    """根据请求中的真实观察选择下一步，跨实例不依赖调用列表做决定。"""

    agent_protocol = "agent-loop-v1"

    def __init__(self, *, hold=False):
        self.requests = []
        self.hold = hold
        self.entered = asyncio.Event()
        self.finished = asyncio.Event()

    async def decide(self, req):
        self.requests.append(req)
        if self.hold:
            self.entered.set()
            try:
                await asyncio.Future()
            finally:
                self.finished.set()
        context = req.context
        if context["has_answer"]:
            decision = (
                m.AgentVerifyDecision()
                if req.require_verification and context["verification_verdict"] is None
                else m.AgentFinishDecision()
            )
        elif context["evidence_summary"]:
            decision = m.AgentComposeDecision()
        elif context["candidates"]:
            source_id = context["candidates"][0]["source_id"]
            decision = m.AgentToolsDecision(
                steps=[
                    m.AgentToolStep(
                        step_id=f"extract-{req.call_id}",
                        tool_name="extract",
                        source_ids=[source_id],
                        args={"source_id": source_id, "query": req.question, "max_chars": 4000},
                        purpose="Read independently recoverable synthetic evidence",
                    )
                ]
            )
        else:
            decision = m.AgentSearchDecision(query=req.question)
        return m.ModuleResult(
            call_id=req.call_id,
            data=decision,
            usage=m.Usage(provider_requests=1, input_tokens=7, output_tokens=3),
        )


def agent_request(**limits):
    """为完整 search/tools/compose/verify/finish 路径保留明确的模型额度。"""
    return request(**{"max_model_calls": 8, "require_verification": True, **limits})


@pytest.fixture
async def store(tmp_path):
    """每个用例独占临时存储，并等待工作线程和租约完成清理。"""
    instance = SqliteStateStore(tmp_path / "state.db")
    try:
        yield instance
    finally:
        await instance.aclose()


def test_agent_planner_requires_explicit_protocol_and_callable_decide():
    """不完整的端口不能触发另一套工作流或任何外部调用。"""
    validate_agent_planner(RecordingPlanner())

    class IncompletePlanner:
        agent_protocol = "agent-loop-v1"
        decide = None

    class UndeclaredPlanner:
        async def decide(self, request):
            raise AssertionError("capability detection may not dispatch")

    for planner in (IncompletePlanner(), UndeclaredPlanner(), IncompatiblePlanner()):
        with pytest.raises(RunStopped) as refused:
            validate_agent_planner(planner)
        assert refused.value.reason == "invalid_planner_protocol"


def test_decide_journal_phase_uses_strict_agent_models():
    """新调用能通过恢复预检，但未知动作和跨协议字段不能被默默接受。"""
    assert _request_type("planner.decide") is m.AgentDecisionRequest
    response_type = _response_type("planner.decide")
    assert response_type is not None
    reply = response_type.model_validate(
        {
            "call_id": "agent-call",
            "status": "ok",
            "data": {"kind": "compose"},
            "usage": m.Usage.local().model_dump(mode="json"),
        }
    )
    assert isinstance(reply.data, m.AgentComposeDecision)
    for data in ({"kind": "undeclared"}, {"kind": "finish", "reason": "legacy"}):
        with pytest.raises(ValidationError):
            response_type.model_validate({"call_id": "agent-call", "status": "ok", "data": data})


async def test_durable_run_records_selected_planner_engine(tmp_path, store):
    """持久化保存控制流身份，不能只比较模块源代码哈希。"""
    await run(
        request(),
        data=RecordingData(),
        content=RecordingContent(),
        planner=RecordingPlanner(),
        trace=JsonlTraceSink(tmp_path / "traces"),
        state_store=store,
    )
    lease = await store.acquire("recoverable-run")
    try:
        loaded = await store.load(lease)
    finally:
        await store.release(lease)
    assert loaded.state.get("planner_engine") == "agent-loop-v1"


async def test_resume_rejects_incompatible_planner_before_ports(tmp_path, store):
    """更换成不支持动作协议的规划器时，在读取历史与端口调用之前拒绝。"""
    result = await run(
        request(),
        data=RecordingData(),
        content=RecordingContent(),
        planner=RecordingPlanner(),
        trace=InterruptingTrace(tmp_path / "traces", event_type="evidence.added", fail=True),
        state_store=store,
    )
    assert result.stop_reason == "trace_write_failed"
    data, content, planner = RecordingData(), RecordingContent(), IncompatiblePlanner()
    with pytest.raises(StateStoreError) as failed:
        await resume(
            "recoverable-run",
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
            state_store=store,
        )
    assert failed.value.code == "recovery_engine_mismatch"
    assert data.catalog_requests == data.tool_requests == []
    assert content.preparations == content.searches == content.compositions == []
    assert content.verifications == planner.requests == []


async def test_cached_terminal_result_cannot_bypass_engine_check(tmp_path, store):
    """即使已有缓存终态，也不能把另一引擎解释为原执行实例。"""
    result = await run(
        request(),
        data=RecordingData(),
        content=RecordingContent(),
        planner=RecordingPlanner(),
        trace=JsonlTraceSink(tmp_path / "traces"),
        state_store=store,
    )
    assert result.status == "completed"
    with pytest.raises(StateStoreError) as failed:
        await resume(
            "recoverable-run",
            data=RecordingData(),
            content=RecordingContent(),
            planner=IncompatiblePlanner(),
            trace=JsonlTraceSink(tmp_path / "traces"),
            state_store=store,
        )
    assert failed.value.code == "recovery_engine_mismatch"
    assert len(list((tmp_path / "traces").rglob("events.jsonl"))) == 1


@pytest.mark.parametrize("phase", ["planner.decide", "content.compose"])
async def test_agent_known_response_survives_trace_failure_without_redispatch(
    tmp_path, store, phase
):
    """响应先存入 SQLite；审计失败后从新端口实例重建，不能重复已付费调用。"""
    old_data, old_content, old_planner = RecordingData(), RecordingContent(), AgentRecoveryPlanner()
    trace_root = tmp_path / "traces"
    trace = InterruptingTrace(trace_root, event_type=f"{phase}.call.completed", fail=True)
    failed = await run(
        agent_request(),
        data=old_data,
        content=old_content,
        planner=old_planner,
        trace=trace,
        state_store=store,
    )
    assert failed.stop_reason == "trace_write_failed"
    original_path = next(trace_root.rglob("events.jsonl"))
    original_bytes = original_path.read_bytes()
    data, content, planner = RecordingData(), RecordingContent(), AgentRecoveryPlanner()
    result = await resume(
        "recoverable-run",
        data=data,
        content=content,
        planner=planner,
        trace=JsonlTraceSink(trace_root),
        state_store=store,
    )
    assert result.status == "completed", result
    assert original_path.read_bytes() == original_bytes
    assert len(old_planner.requests) + len(planner.requests) == 5
    assert len(old_content.searches) + len(content.searches) == 1
    assert len(old_content.compositions) + len(content.compositions) == 1
    assert len(old_content.verifications) + len(content.verifications) == 1
    assert len(old_data.catalog_requests) + len(data.catalog_requests) == 1
    assert len(old_data.tool_requests) + len(data.tool_requests) == 1
    assert result.call_counts.planner == result.call_counts.model_calls == 5
    assert (result.usage.input_tokens, result.usage.output_tokens) == (35, 15)
    dispatched = old_planner.requests + planner.requests
    assert [req.remaining_turns for req in dispatched] == [32, 31, 30, 29, 28]
    assert [req.budget_remaining.model_calls for req in dispatched] == [8, 7, 6, 5, 4]
    assert len({req.call_id for req in dispatched}) == 5
    composed = (old_content.compositions + content.compositions)[0]
    assert result.answer.citations == [composed.evidence[0].evidence_id]
    events = trace_events(result.trace_ref)
    assert any(
        event["event_type"] == f"{phase}.call.completed" and event["detail"].get("replayed")
        for event in events
    )


async def test_agent_unknown_decide_outcome_refused_before_ports(tmp_path, store):
    """模型意图已提交但没有已知响应时，取消不能授权恢复后重试。"""
    planner = AgentRecoveryPlanner(hold=True)
    trace_root = tmp_path / "traces"
    task = asyncio.create_task(
        run(
            agent_request(),
            data=RecordingData(),
            content=RecordingContent(),
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=store,
        )
    )
    await cancel_at_gate(task, planner.entered)
    assert planner.finished.is_set()
    data, content, fresh = RecordingData(), RecordingContent(), AgentRecoveryPlanner()
    with pytest.raises(StateStoreError) as failed:
        await resume(
            "recoverable-run",
            data=data,
            content=content,
            planner=fresh,
            trace=JsonlTraceSink(trace_root),
            state_store=store,
        )
    assert failed.value.code == "recovery_outcome_unknown"
    assert data.catalog_requests == data.tool_requests == []
    assert content.preparations == content.searches == content.compositions == []
    assert content.verifications == fresh.requests == []
    events = trace_events(next(trace_root.rglob("events.jsonl")))
    assert events[-1]["event_type"] == "run.cancelled"
    assert events[-1]["detail"]["active_call"]["phase"] == "planner.decide"
    assert events[-1]["detail"]["call_counts"]["model_calls"] == 1


async def test_agent_resume_keeps_paid_decisions_and_composition_budget(tmp_path, store):
    """历史模型决策和已返回的付费 compose 都继续占用同一运行额度。"""

    class PaidContent(RecordingContent):
        async def compose(self, req):
            return RecordingPlanner.paid(await super().compose(req))

    trace_root = tmp_path / "traces"
    failed = await run(
        request(max_model_calls=4),
        data=RecordingData(),
        content=PaidContent(),
        planner=AgentRecoveryPlanner(),
        trace=InterruptingTrace(trace_root, event_type="content.compose.call.completed", fail=True),
        state_store=store,
    )
    assert failed.stop_reason == "trace_write_failed"
    data, content, planner = RecordingData(), PaidContent(), AgentRecoveryPlanner()
    result = await resume(
        "recoverable-run",
        data=data,
        content=content,
        planner=planner,
        trace=JsonlTraceSink(trace_root),
        state_store=store,
    )
    assert (result.status, result.stop_reason) == ("budget_exceeded", "model_call_budget")
    assert result.call_counts.planner == 3
    assert result.call_counts.model_calls == result.usage.provider_requests == 4
    assert (result.usage.input_tokens, result.usage.output_tokens) == (28, 12)
    assert data.catalog_requests == data.tool_requests == []
    assert content.searches == content.compositions == content.verifications == []
    assert planner.requests == []


async def test_agent_repeated_cancellation_drains_planner_before_lease_release(tmp_path, store):
    """重复取消只向图转发一次；等待规划器收尾后才关闭审计并释放租约。"""

    class CleanupPlanner(AgentRecoveryPlanner):
        def __init__(self):
            super().__init__()
            self.cleanup_entered = asyncio.Event()
            self.release_cleanup = asyncio.Event()
            self.cleanup_cancellations = 0

        async def decide(self, req):
            self.requests.append(req)
            self.entered.set()
            try:
                await asyncio.Future()
            finally:
                self.cleanup_entered.set()
                try:
                    await self.release_cleanup.wait()
                except asyncio.CancelledError:
                    self.cleanup_cancellations += 1
                    raise
                finally:
                    self.finished.set()

    planner = CleanupPlanner()
    trace_root = tmp_path / "traces"
    task = asyncio.create_task(
        run(
            agent_request(),
            data=RecordingData(),
            content=RecordingContent(),
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=store,
        )
    )
    try:
        await asyncio.wait_for(planner.entered.wait(), timeout=3)
        task.cancel()
        await asyncio.wait_for(planner.cleanup_entered.wait(), timeout=3)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not planner.finished.is_set()
        planner.release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert planner.finished.is_set()
        assert planner.cleanup_cancellations == 0
        lease = await store.acquire("recoverable-run")
        await store.release(lease)
        path = next(trace_root.rglob("events.jsonl"))
        settled = path.read_bytes()
        await asyncio.sleep(0.01)
        assert path.read_bytes() == settled
        assert trace_events(path)[-1]["event_type"] == "run.cancelled"
    finally:
        planner.release_cleanup.set()
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
