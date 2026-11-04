"""内容反馈及修正请求在 SQLite 重放和失败恢复中的一致性。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio

import pytest

from lake_agent import models as m
from lake_agent.engine.recovery import RecoverySession, _request_type
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
    request,
    trace_events,
)


@pytest.mark.parametrize("phase", ["compose", "verify"])
def test_feedback_request_registry_keeps_base_and_extended_contracts_distinct(phase):
    """旧内容请求不产生新字段，只有已记录的扩展请求使用显式反馈模型。"""
    basic = getattr(m, "ComposeRequest" if phase == "compose" else "VerifyRequest")
    assert _request_type(f"content.{phase}") is basic
    extended = _request_type(f"content.{phase}", {"feedback": {}})
    assert extended.__name__ == (
        "ComposeFeedbackRequest" if phase == "compose" else "VerifyFeedbackRequest"
    )


class FeedbackContent(RecordingContent):
    """显式声明修正协议，具体调用行为仍由本地记录端口提供。"""

    content_feedback_protocol = "content-feedback-v1"


@pytest.mark.parametrize("original_support", [False, True])
async def test_content_feedback_capability_change_refuses_resume_before_ports(
    tmp_path, original_support
):
    """恢复不能把原有修正请求交给不支持的C，也不能换能力改变原执行解释。"""
    store = SqliteStateStore(tmp_path / "state.db")
    root = tmp_path / "traces"
    original = FeedbackContent() if original_support else RecordingContent()
    try:
        result = await run(
            request(),
            data=RecordingData(),
            content=original,
            planner=RecordingPlanner(),
            trace=UnavailableAttemptTrace(root),
            state_store=store,
        )
        assert result.stop_reason == "trace_sink_unavailable"
        lease = await store.acquire(result.run_id)
        try:
            saved = await store.load(lease)
        finally:
            await store.release(lease)
        assert saved.state.get("content_feedback_protocol") == (
            "content-feedback-v1" if original_support else None
        )
        data, planner = RecordingData(), RecordingPlanner()
        changed = RecordingContent() if original_support else FeedbackContent()
        with pytest.raises(StateStoreError) as failed:
            await resume(
                result.run_id,
                data=data,
                content=changed,
                planner=planner,
                trace=JsonlTraceSink(root),
                state_store=store,
            )
        assert failed.value.code == "recovery_contract_mismatch"
        assert_no_port_calls(data, changed, planner)
    finally:
        await store.aclose()


@pytest.mark.parametrize("phase", ["compose", "verify"])
async def test_unknown_repair_call_outcome_refuses_resume_before_any_port(tmp_path, phase):
    """修正同样是外部调用，意图已保存但结果未知时不能因可纠错而重发。"""

    class HoldingRepair(RepairingContent):
        def __init__(self):
            super().__init__(phase)
            self.entered = asyncio.Event()

        async def compose(self, req):
            if self.phase == "compose" and isinstance(req, m.ComposeFeedbackRequest):
                self.compositions.append(req)
                self.entered.set()
                await asyncio.Future()
            return await super().compose(req)

        async def verify(self, req):
            if self.phase == "verify" and isinstance(req, m.VerifyFeedbackRequest):
                self.verifications.append(req)
                self.entered.set()
                await asyncio.Future()
            return await super().verify(req)

    store = SqliteStateStore(tmp_path / "state.db")
    root = tmp_path / "traces"
    old = HoldingRepair()
    try:
        task = asyncio.create_task(
            run(
                request(max_model_calls=16),
                data=RecordingData(),
                content=old,
                planner=RecordingPlanner(),
                trace=JsonlTraceSink(root),
                state_store=store,
            )
        )
        await cancel_at_gate(task, old.entered)
        data, content, planner = RecordingData(), RepairingContent(phase), RecordingPlanner()
        with pytest.raises(StateStoreError) as failed:
            await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(root),
                state_store=store,
            )
        assert failed.value.code == "recovery_outcome_unknown"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


async def test_unknown_feedback_protocol_is_not_persisted_as_capability(tmp_path):
    """声明未知值等同于没有当前能力；持久化元数据只保存受支持的枚举。"""

    class UnknownContent(RecordingContent):
        content_feedback_protocol = "unrecognized-private-adapter-value"

    store = SqliteStateStore(tmp_path / "state.db")
    try:
        result = await run(
            request(),
            data=RecordingData(),
            content=UnknownContent(),
            planner=RecordingPlanner(),
            trace=UnavailableAttemptTrace(tmp_path / "traces"),
            state_store=store,
        )
        lease = await store.acquire(result.run_id)
        try:
            saved = await store.load(lease)
        finally:
            await store.release(lease)
        assert saved.state["content_feedback_protocol"] is None
        assert "unrecognized-private-adapter-value" not in str(saved.state)
    finally:
        await store.aclose()


async def test_cached_terminal_result_still_checks_content_feedback_capability(tmp_path):
    """缓存结果不能绕过能力身份，避免同一run_id被另一种内容契约解释。"""
    store = SqliteStateStore(tmp_path / "state.db")
    root = tmp_path / "traces"
    try:
        result = await run(
            request(),
            data=RecordingData(),
            content=FeedbackContent(),
            planner=RecordingPlanner(),
            trace=JsonlTraceSink(root),
            state_store=store,
        )
        assert result.status == "completed"
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        with pytest.raises(StateStoreError) as failed:
            await resume(
                result.run_id,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(root),
                state_store=store,
            )
        assert failed.value.code == "recovery_contract_mismatch"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


def test_malformed_prior_feedback_record_is_a_structured_recovery_rejection():
    """损坏的引用记录必须规范拒绝，不能在预检中泄漏属性异常。"""
    correction = m.ComposeFeedbackRequest(
        run_id="run-feedback",
        task_id="task-feedback",
        operation_id="op-repair",
        call_id="call-repair",
        attempt=1,
        timeout_ms=1000,
        max_output_bytes=65536,
        model_id="offline",
        model_params={},
        question="Question",
        feedback=m.ContentFeedback(
            phase="compose",
            call_id="call-failed",
            error=m.ErrorInfo(code="invalid_model_citations", message="Unknown evidence ID"),
        ),
    )
    # 只调用无I/O的预检，故意把被引用的记录改为非对象；修改在内存中进行。
    session = RecoverySession.__new__(RecoverySession)
    session.state = {"content_feedback_protocol": "content-feedback-v1"}
    session.calls = {
        correction.call_id: {
            "status": "completed",
            "phase": "content.compose",
            "request": correction.model_dump(mode="json"),
            "response": {},
            "auxiliary": False,
        },
        "call-failed": 7,
    }
    with pytest.raises(StateStoreError) as failed:
        session._inspect_journal()
    assert failed.value.code == "recovery_state_invalid"


class RepairingContent(FeedbackContent):
    """每个新实例都没有上一轮状态，唯有请求自带反馈才返回修正结果。"""

    def __init__(self, phase):
        super().__init__()
        self.phase = phase

    @staticmethod
    def paid(reply):
        return reply.model_copy(
            update={"usage": m.Usage(provider_requests=1, input_tokens=5, output_tokens=2)}
        )

    def failure(self, req):
        return m.ModuleResult(
            call_id=req.call_id,
            status="error",
            error=m.ErrorInfo(
                code="invalid_model_citations",
                message="The model returned unknown evidence citations.",
                details={
                    "issues": [{"path": "citations[0]", "actual": "demo-annual"}],
                    "allowed_evidence_ids": [item.evidence_id for item in req.evidence],
                    "rejected_output": {"citations": ["demo-annual"]},
                },
            ),
            usage=m.Usage(provider_requests=1, input_tokens=5, output_tokens=2),
        )

    async def compose(self, req):
        if self.phase == "compose" and not isinstance(req, m.ComposeFeedbackRequest):
            self.compositions.append(req)
            return self.failure(req)
        return self.paid(await super().compose(req))

    async def verify(self, req):
        if self.phase == "verify" and not isinstance(req, m.VerifyFeedbackRequest):
            self.verifications.append(req)
            return self.failure(req)
        return self.paid(await super().verify(req))


@pytest.mark.parametrize("phase", ["compose", "verify"])
@pytest.mark.parametrize("interrupt_at", ["failure", "repair"])
async def test_known_content_failure_and_repair_rebuild_without_redispatch(
    tmp_path, phase, interrupt_at
):
    """失败或修正响应后中断都能恢复；新C只接收尚未执行的显式修正请求。"""
    store = SqliteStateStore(tmp_path / "state.db")
    root = tmp_path / "traces"
    old_data, old_content, old_planner = (
        RecordingData(),
        RepairingContent(phase),
        RecordingPlanner(),
    )
    event = f"content.{phase}.call.{'failed' if interrupt_at == 'failure' else 'completed'}"
    try:
        stopped = await run(
            request(max_model_calls=16),
            data=old_data,
            content=old_content,
            planner=old_planner,
            trace=InterruptingTrace(root, event_type=event, fail=True),
            state_store=store,
        )
        assert stopped.stop_reason == "trace_write_failed"
        lease = await store.acquire(stopped.run_id)
        try:
            before = await store.load(lease)
        finally:
            await store.release(lease)
        data, content, planner = RecordingData(), RepairingContent(phase), RecordingPlanner()
        result = await resume(
            stopped.run_id,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(root),
            state_store=store,
        )
        assert (result.status, result.stop_reason) == ("completed", "agent_finished")
        assert result.failure is None
        assert data.catalog_requests == data.tool_requests == []
        calls = old_content.compositions + content.compositions
        if phase == "verify":
            calls = old_content.verifications + content.verifications
        assert len(calls) == 2
        first, correction = calls
        assert not hasattr(first, "feedback")
        assert correction.feedback.phase == phase
        assert correction.feedback.call_id == first.call_id
        assert correction.feedback.error.details["rejected_output"] == {
            "citations": ["demo-annual"]
        }
        assert correction.feedback.error.details["allowed_evidence_ids"] == [
            item.evidence_id for item in correction.evidence
        ]
        dispatched = old_planner.requests + planner.requests
        assert (
            result.call_counts.model_calls == result.usage.provider_requests == len(dispatched) + 3
        )
        assert result.usage.input_tokens == len(dispatched) * 7 + 3 * 5
        assert [r.remaining_turns for r in dispatched] == list(range(32, 32 - len(dispatched), -1))
        lease = await store.acquire(result.run_id)
        try:
            after = await store.load(lease)
        finally:
            await store.release(lease)
        assert after.state["started_epoch"] == before.state["started_epoch"]
        assert after.state["deadline_epoch"] == before.state["deadline_epoch"]
        # 失败响应保留且可重放，修正请求的完整反馈也由严格扩展模型解释。
        known = after.calls[first.call_id]
        assert known["response"]["error"]["code"] == "invalid_model_citations"
        assert after.calls[correction.call_id]["request"]["feedback"] == (
            correction.feedback.model_dump(mode="json")
        )
        events = trace_events(result.trace_ref)
        assert any(
            value["event_type"] == f"content.{phase}.call.failed"
            and value["detail"].get("replayed")
            for value in events
        )
    finally:
        await store.aclose()
