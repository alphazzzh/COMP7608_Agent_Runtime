"""恢复后完整核验状态和结构化修订请求不变且不重复派发。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import json

from lake_agent import models as m
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink
from tests.support.agent import EvidenceContent, RevenueData, StatelessRevisionContent, revenue_tool
from tests.support.recovery import RecordingData, trace_events


class RevisionStatePlanner:
    """依照实际答案与当前核验状态决策，不按本实例调用次数续跑。"""

    agent_protocol = "agent-loop-v1"
    instructions = json.dumps({"suffix": " Corrected currency."})

    def __init__(self):
        self.requests = []

    async def decide(self, request):
        self.requests.append(request.model_copy(deep=True))
        context = request.context
        if not context["has_answer"]:
            action = m.AgentComposeDecision()
        elif context["verification_status"] != "current":
            action = m.AgentVerifyDecision()
        elif context["verification_verdict"] == "supported":
            action = m.AgentFinishDecision()
        else:
            action = m.AgentComposeDecision(revision_instructions=self.instructions)
        return m.ModuleResult(call_id=request.call_id, data=action, usage=m.Usage.local())


class FailAfterRevisionResponse(JsonlTraceSink):
    """第二个生成响应已进入调用日志后，终止该次审计延续。"""

    def __init__(self, root):
        super().__init__(root)
        self.completed_compositions = 0

    async def append(self, event):
        await super().append(event)
        if event.event_type == "content.compose.call.completed":
            self.completed_compositions += 1
            if self.completed_compositions == 2:
                raise OSError("Controlled audit failure after a known revision response")


async def test_saved_revision_rebuilds_with_exact_feedback_without_recomposing(tmp_path):
    """缓存修订须完整匹配旧答案、具体问题与修改目标，再核验新的当前答案。"""
    store = SqliteStateStore(tmp_path / "state.db")
    root = tmp_path / "traces"
    old_data, old_content, old_planner = (
        RecordingData(),
        StatelessRevisionContent(),
        RevisionStatePlanner(),
    )
    try:
        interrupted = await run(
            m.RunRequest(
                run_id="answer-revision-recovery",
                task_id="answer-revision-recovery-task",
                question="Which currency is stated?",
                config=m.RunConfig(require_verification=True),
            ),
            data=old_data,
            content=old_content,
            planner=old_planner,
            trace=FailAfterRevisionResponse(root),
            state_store=store,
        )
        assert interrupted.stop_reason == "trace_write_failed"
        assert len(old_content.compositions) == 2
        assert len(old_content.verifications) == 1
        revision_request = old_content.compositions[1]
        assert revision_request.revision.previous_answer.text == "Revenue was 120 million USD."
        assert revision_request.revision.verification.verdict == "conflicting"
        assert json.loads(revision_request.revision.verification.issues[0].detail) == {
            "find": "USD",
            "replace": "HKD",
        }
        assert revision_request.revision.verification_is_current is True
        assert revision_request.revision.instructions == old_planner.instructions
        lease = await store.acquire(interrupted.run_id)
        try:
            before = await store.load(lease)
        finally:
            await store.release(lease)
        original_call = before.calls[revision_request.call_id]
        assert original_call["request"]["revision"] == (
            revision_request.revision.model_dump(mode="json")
        )
        assert original_call["response"]["data"]["text"] == (
            "Revenue was 120 million HKD. Corrected currency."
        )

        data, content, planner = RecordingData(), StatelessRevisionContent(), RevisionStatePlanner()
        result = await resume(
            interrupted.run_id,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(root),
            state_store=store,
        )
        assert (result.status, result.stop_reason) == ("completed", "agent_finished")
        assert result.answer.text == "Revenue was 120 million HKD. Corrected currency."
        assert content.compositions == []
        assert len(content.verifications) == 1
        assert content.verifications[0].answer == result.answer
        assert data.catalog_requests == data.tool_requests == []
        assert len(old_planner.requests) == 3
        assert len(planner.requests) == 2
        assert planner.requests[0].context["verification_status"] == "missing"
        assert planner.requests[1].context["verification_verdict"] == "supported"
        lease = await store.acquire(result.run_id)
        try:
            after = await store.load(lease)
        finally:
            await store.release(lease)
        assert after.calls[revision_request.call_id] == original_call
        assert any(
            event["event_type"] == "content.compose.call.completed"
            and event["call_id"] == revision_request.call_id
            and event["detail"].get("replayed")
            for event in trace_events(result.trace_ref)
        )
    finally:
        await store.aclose()


class EvidenceStatePlanner:
    """由真实上下文选动作，恢复时不依赖实例内调用次数继续流程。"""

    agent_protocol = "agent-loop-v1"

    def __init__(self):
        self.requests = []

    async def decide(self, request):
        self.requests.append(request.model_copy(deep=True))
        context = request.context
        count = context["context_status"]["evidence"]["total"]
        last = context.get("last_action") or {}
        if count == 0:
            action = revenue_tool("first")
        elif not context["has_answer"]:
            action = {"kind": "compose"}
        elif count == 1:
            action = (
                revenue_tool("second", "conflict")
                if context["verification_verdict"] == "supported"
                else {"kind": "verify"}
            )
        elif last.get("code") == "verification_stale":
            action = {"kind": "verify"}
        elif context["verification_verdict"] == "conflicting":
            action = {"kind": "stop", "reason": "Rechecked evidence conflicts."}
        else:
            # 故意先提议完成；只有真实控制器应拒绝旧supported凭证。
            action = {"kind": "finish"}
        return m.ModuleResult(call_id=request.call_id, data=action, usage=m.Usage.local())


class FailAfterSecondEvidence(JsonlTraceSink):
    """已知第二条工具响应已落SQLite后，破坏审计延续以建立可恢复前沿。"""

    def __init__(self, root):
        super().__init__(root)
        self.accepted_evidence = 0

    async def append(self, event):
        await super().append(event)
        if event.event_type == "evidence.added":
            self.accepted_evidence += 1
            if self.accepted_evidence == 2:
                raise OSError("Controlled audit failure after known second evidence")


async def test_resume_reconstructs_stale_verification_without_replaying_known_requests(tmp_path):
    """旧核验、完整证据ID与新一次恢复的attempt元数据必须彼此独立。"""
    store = SqliteStateStore(tmp_path / "state.db")
    root = tmp_path / "traces"
    data, content, planner = RevenueData(), EvidenceContent(), EvidenceStatePlanner()
    try:
        interrupted = await run(
            m.RunRequest(
                run_id="freshness-recovery",
                task_id="freshness-recovery-task",
                question="What is the annual revenue?",
                config=m.RunConfig(require_verification=True, max_model_calls=16),
            ),
            data=data,
            content=content,
            planner=planner,
            trace=FailAfterSecondEvidence(root),
            state_store=store,
        )
        assert interrupted.stop_reason == "trace_write_failed"
        assert len(data.tool_requests) == 2
        assert len(content.compositions) == len(content.verifications) == 1
        initial_verification = content.verifications[0]

        new_data, new_content, new_planner = (
            RevenueData(),
            EvidenceContent(),
            EvidenceStatePlanner(),
        )
        result = await resume(
            interrupted.run_id,
            data=new_data,
            content=new_content,
            planner=new_planner,
            trace=JsonlTraceSink(root),
            state_store=store,
        )
        assert (result.status, result.stop_reason) == ("insufficient_evidence", "agent_stopped")
        assert new_data.catalog_requests == new_data.tool_requests == []
        assert new_content.compositions == []
        assert len(new_content.verifications) == 1
        assert new_content.verifications[0].answer == initial_verification.answer
        assert new_content.verifications[0].evidence[0] == initial_verification.evidence[0]
        assert len(new_content.verifications[0].evidence) == 2
        assert len(planner.requests) == 4
        assert len(new_planner.requests) == 3
        assert new_planner.requests[0].context["verification_status"] == "stale"
        assert new_planner.requests[1].context["last_action"]["code"] == "verification_stale"
        assert new_planner.requests[2].context["verification_verdict"] == "conflicting"
        events = trace_events(result.trace_ref)
        cached_verification = [
            event
            for event in events
            if event["event_type"] == "content.verify.call.completed"
            and event["detail"].get("replayed")
        ]
        assert len(cached_verification) == 1
        assert cached_verification[0]["call_id"] == initial_verification.call_id
    finally:
        await store.aclose()
