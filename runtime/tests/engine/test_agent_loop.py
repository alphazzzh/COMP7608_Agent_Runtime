"""唯一自主动作循环、可更新计划、完成政策与无状态答案修订。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import json

import pytest

from lake_agent import models as m
from lake_agent.runtime import run
from lake_agent.validation import canonical_json
from tests.support.agent import (
    ActionPlanner,
    FlexibleContent,
    MemoryTrace,
    StatelessRevisionContent,
    execute,
    tool_action,
)
from tests.support.ports import DemoData


async def test_model_can_compose_without_decomposition_search_tools_or_body_evidence():
    result, planner, content, trace = await execute([{"kind": "compose"}, {"kind": "finish"}])
    assert (result.status, result.stop_reason) == ("completed", "agent_finished")
    assert result.answer.citations == []
    assert result.call_counts.tools == 0
    assert not content.searches and not content.verifications
    assert len(planner.requests) == 2 and content.compositions[0].evidence == []
    assert planner.requests[0].context["catalog"]["total"] == 2
    assert not any(e.event_type == "planner.decompose.call.started" for e in trace.events)


async def test_model_searches_once_then_tools_compose_verify_and_finish():
    actions = [
        {"kind": "search", "query": "annual"},
        tool_action(),
        {"kind": "compose"},
        {"kind": "verify"},
        {"kind": "finish"},
    ]
    result, planner, content, _ = await execute(actions)
    assert result.status == "completed"
    assert len(content.searches) == 1 and content.searches[0].subtask.search_query == "annual"
    assert result.call_counts.tools == 1 and len(content.verifications) == 1
    assert planner.requests[2].context["tool_observations"]
    assert result.answer.citations == [content.compositions[0].evidence[0].evidence_id]


async def test_registered_catalog_source_can_be_read_without_search():
    result, _, content, _ = await execute([tool_action(), {"kind": "compose"}, {"kind": "finish"}])
    assert result.status == "completed" and result.call_counts.tools == 1
    assert not content.searches


async def test_plan_can_be_added_replaced_and_removed_without_controlling_action_order():
    task = {"subtask_id": "optional", "objective": "Inspect", "search_query": "inspect"}
    result, planner, content, _ = await execute(
        [
            {"kind": "plan", "subtasks": [task]},
            {"kind": "plan", "subtasks": []},
            {"kind": "compose"},
            {"kind": "finish"},
        ]
    )
    assert result.status == "completed" and not content.searches
    assert planner.requests[1].context["plan"][0]["subtask_id"] == "optional"
    assert planner.requests[2].context["plan"] == []


async def test_failed_verification_can_be_followed_by_new_answer_and_new_verification():
    content = FlexibleContent(["insufficient", "supported"])
    result, planner, _, _ = await execute(
        [
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "finish"},
        ],
        content=content,
    )
    assert result.status == "completed" and len(content.compositions) == 2
    assert planner.requests[2].context["verification"]["verdict"] == "insufficient"
    assert planner.requests[3].context["verification"] is None
    assert result.answer.text.startswith("Answer 2")


async def test_host_verification_policy_rejects_early_finish_and_returns_to_model():
    result, planner, content, _ = await execute(
        [{"kind": "compose"}, {"kind": "finish"}, {"kind": "verify"}, {"kind": "finish"}],
        require_verification=True,
    )
    assert result.status == "completed" and len(content.verifications) == 1
    assert planner.requests[2].context["last_action"]["status"] == "rejected"
    assert planner.requests[2].context["last_action"]["code"] == "verification_required"


async def test_verify_before_answer_is_rejected_without_calling_content_then_model_can_correct():
    result, planner, content, _ = await execute(
        [{"kind": "verify"}, {"kind": "compose"}, {"kind": "finish"}]
    )
    assert result.status == "completed" and not content.verifications
    assert planner.requests[1].context["last_action"]["code"] == "answer_required"


async def test_verification_failure_cannot_be_reported_as_success_for_the_same_answer():
    result, _, _, _ = await execute(
        [{"kind": "compose"}, {"kind": "verify"}, {"kind": "finish"}],
        content=FlexibleContent(["insufficient"]),
    )
    assert result.status == "insufficient_evidence" and result.answer is not None


async def test_unknown_catalog_source_is_not_authorized_by_a_model_action():
    result, _, _, _ = await execute([tool_action("unknown")])
    assert result.status == "failed" and result.stop_reason == "invalid_plan"
    assert result.call_counts.tools == 0


async def test_local_search_only_loop_stops_at_agent_turn_budget_beyond_graph_default():
    result, planner, content, _ = await execute(
        [{"kind": "search", "query": "again"}] * 30, max_agent_turns=30
    )
    assert (result.status, result.stop_reason) == ("budget_exceeded", "agent_turn_budget")
    assert len(planner.requests) == len(content.searches) == 30
    assert result.call_counts.model_calls == 0


async def test_model_can_stop_without_generating_an_answer():
    result, _, content, _ = await execute([{"kind": "stop", "reason": "Need user clarification"}])
    assert result.status == "insufficient_evidence" and result.stop_reason == "agent_stopped"
    assert result.answer is None and not content.compositions


async def test_model_can_page_the_registered_catalog_locally():
    result, planner, _, trace = await execute(
        [{"kind": "read_catalog", "offset": 1, "limit": 1}, {"kind": "compose"}, {"kind": "finish"}]
    )
    assert result.status == "completed"
    page = planner.requests[1].context["catalog_page"]
    assert page["offset"] == 1 and len(page["sources"]) == 1 and page["next_offset"] is None
    assert any(e.event_type == "catalog.read" for e in trace.events)


async def test_consecutive_observation_pages_remain_complete_and_reconstruct_the_json_value():
    """偏移量属于实际返回页，页不可被上下文构造器再次截断。"""
    pages = []
    payload = "资料😀" * 300

    class LargeData(DemoData):
        async def invoke(self, request):
            result = await super().invoke(request)
            result.data["long_value"] = payload
            return result

    class PagingPlanner(ActionPlanner):
        def __init__(self):
            super().__init__([])

        async def decide(self, request):
            self.requests.append(request.model_copy(deep=True))
            context = request.context
            assert len(canonical_json(context)) <= 1024
            if not context["context_status"]["observations"]["total"]:
                action = tool_action()
            elif context["observation_read"] is not None:
                page = context["observation_read"]
                assert "pointer" in page and "offset" in page and "next_offset" in page
                pages.append(page)
                if page["next_offset"] is None:
                    action = {"kind": "stop", "reason": "Read complete"}
                else:
                    action = {
                        "kind": "read_observation",
                        "observation_id": page["observation_id"],
                        "pointer": "/data/long_value",
                        "offset": page["next_offset"],
                        "max_chars": 100,
                    }
            else:
                observation = next(
                    e.detail["observation_id"]
                    for e in trace.events
                    if e.event_type == "tool.observed"
                )
                action = {
                    "kind": "read_observation",
                    "observation_id": observation,
                    "pointer": "/data/long_value",
                    "offset": 0,
                    "max_chars": 100,
                }
            return m.ModuleResult(
                call_id=request.call_id, status="ok", data=action, usage=m.Usage.local()
            )

    planner, trace = PagingPlanner(), MemoryTrace()
    result = await run(
        m.RunRequest(
            task_id="pages",
            question="Read the long value",
            config=m.RunConfig(
                max_planner_context_bytes=1024,
                planner_preview_bytes=512,
                max_observation_reads=80,
                max_agent_turns=85,
            ),
        ),
        data=LargeData(),
        content=FlexibleContent(),
        planner=planner,
        trace=trace,
    )
    assert result.stop_reason == "agent_stopped"
    assert len(pages) > 1
    assert json.loads("".join(page["text"] for page in pages)) == payload


async def test_empty_source_filter_is_forwarded_as_an_empty_set():
    class EmptySearch(FlexibleContent):
        async def search(self, request):
            self.searches.append(request)
            assert request.source_id_filter == []
            return m.ModuleResult(
                call_id=request.call_id, status="ok", data=m.SearchResponse(), usage=m.Usage.local()
            )

    result, _, content, _ = await execute(
        [
            {"kind": "search", "query": "nothing", "source_ids": []},
            {"kind": "stop", "reason": "Empty selection"},
        ],
        content=EmptySearch(),
    )
    assert result.stop_reason == "agent_stopped" and len(content.searches) == 1


async def test_new_answer_cannot_reuse_a_previous_supported_verification():
    result, planner, _, _ = await execute(
        [
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "compose"},
            {"kind": "finish"},
            {"kind": "verify"},
            {"kind": "finish"},
        ],
        require_verification=True,
    )
    assert result.status == "completed"
    assert planner.requests[4].context["last_action"]["code"] == "verification_required"


async def test_unknown_citation_is_rejected_on_the_unrestricted_compose_path():
    class BadCitation(FlexibleContent):
        async def compose(self, request):
            reply = await super().compose(request)
            reply.data.citations = ["invented"]
            return reply

    result, _, _, _ = await execute([{"kind": "compose"}], content=BadCitation())
    assert (result.status, result.stop_reason) == ("failed", "invalid_module_response")


async def test_required_verification_cannot_be_combined_with_no_verifier():
    planner, content, trace = ActionPlanner([]), FlexibleContent(), MemoryTrace()
    result = await run(
        m.RunRequest(
            task_id="policy",
            question="q",
            mode="no_verifier",
            config=m.RunConfig(require_verification=True),
        ),
        data=DemoData(),
        content=content,
        planner=planner,
        trace=trace,
    )
    assert (result.status, result.stop_reason) == ("failed", "invalid_run_policy")
    assert result.call_counts.catalog == 0 and not planner.requests


async def test_no_verifier_rejects_verification_and_returns_feedback_without_calling_content():
    """消融组即使模型主动选 verify，也不能通过真实内容端口核验。"""
    planner = ActionPlanner([{"kind": "compose"}, {"kind": "verify"}, {"kind": "finish"}])
    content = FlexibleContent()
    result = await run(
        m.RunRequest(task_id="ablation-policy", question="q", mode="no_verifier"),
        data=DemoData(),
        content=content,
        planner=planner,
        trace=MemoryTrace(),
    )
    assert result.status == "completed" and not content.verifications
    assert planner.requests[2].context["last_action"]["code"] == "verification_disabled"


async def test_recent_observation_pages_are_available_to_the_stateless_next_decision():
    def first_page(request):
        observation = request.context["tool_observations"][0]["observation_id"]
        return {
            "kind": "read_observation",
            "observation_id": observation,
            "pointer": "/data",
            "max_chars": 20,
        }

    def next_page(request):
        page = request.context["observation_read"]
        return {
            "kind": "read_observation",
            "observation_id": page["observation_id"],
            "pointer": "/data",
            "offset": page["next_offset"],
            "max_chars": 20,
        }

    result, planner, _, _ = await execute(
        [tool_action(), first_page, next_page, {"kind": "stop", "reason": "Done"}]
    )
    assert result.stop_reason == "agent_stopped"
    page = planner.requests[3].context["observation_read"]
    assert len(page["previous_pages"]) == 1 and page["previous_pages"][0]["offset"] == 0
    assert page["omitted_pages"] == 0


async def test_identical_recomposition_does_not_clear_an_unsuccessful_verification():
    class SameAnswer(FlexibleContent):
        async def compose(self, request):
            result = await super().compose(request)
            result.data.text = "The same answer"
            return result

    result, planner, _, _ = await execute(
        [{"kind": "compose"}, {"kind": "verify"}, {"kind": "compose"}, {"kind": "finish"}],
        content=SameAnswer(["insufficient"]),
    )
    assert result.status == "insufficient_evidence"
    assert planner.requests[3].context["verification_verdict"] == "insufficient"


async def test_small_context_reclaims_optional_previews_for_complete_requested_pages():
    class LongAnswer(FlexibleContent):
        async def compose(self, request):
            reply = await super().compose(request)
            reply.data.text = "x" * 1000
            return reply

    def first_page(request):
        observation = next(
            e.detail["observation_id"] for e in trace.events if e.event_type == "tool.observed"
        )
        return {
            "kind": "read_observation",
            "observation_id": observation,
            "pointer": "/data",
            "max_chars": 1,
        }

    def next_page(request):
        page = request.context["observation_read"]
        return {
            "kind": "read_observation",
            "observation_id": page["observation_id"],
            "pointer": "/data",
            "offset": page["next_offset"],
            "max_chars": 40,
        }

    planner = ActionPlanner(
        [
            tool_action(),
            {"kind": "compose"},
            {"kind": "verify"},
            first_page,
            next_page,
            {"kind": "stop", "reason": "Done"},
        ]
    )
    trace = MemoryTrace()
    result = await run(
        m.RunRequest(
            task_id="small-context",
            question="q",
            config=m.RunConfig(max_planner_context_bytes=1024, planner_preview_bytes=512),
        ),
        data=DemoData(),
        content=LongAnswer(["insufficient"]),
        planner=planner,
        trace=trace,
    )
    assert result.stop_reason == "agent_stopped"
    assert len(planner.requests) == 6
    assert all(len(canonical_json(request.context)) <= 1024 for request in planner.requests)
    assert planner.requests[-1].context["observation_read"]["offset"] == 1


async def test_stateless_composer_revises_from_old_answer_exact_issue_and_action_goal():
    content = StatelessRevisionContent()
    instructions = json.dumps({"suffix": " Corrected currency."})
    result, _, _, _ = await execute(
        [
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "compose", "revision_instructions": instructions},
            {"kind": "verify"},
            {"kind": "finish"},
        ],
        content=content,
        require_verification=True,
    )
    assert result.status == "completed"
    assert result.answer.text == "Revenue was 120 million HKD. Corrected currency."
    first, revised = content.compositions
    assert first.revision is None
    assert first.question == revised.question
    assert first.evidence == revised.evidence == []
    assert first.completed_subtasks == revised.completed_subtasks == []
    assert revised.revision.previous_answer.text == "Revenue was 120 million USD."
    assert revised.revision.verification.verdict == "conflicting"
    assert revised.revision.verification_is_current is True
    assert revised.revision.instructions == instructions


async def test_revision_goal_without_an_existing_answer_is_rejected_before_content():
    content = FlexibleContent()
    result, planner, _, _ = await execute(
        [
            {"kind": "compose", "revision_instructions": "Correct the earlier currency."},
            {"kind": "compose"},
            {"kind": "finish"},
        ],
        content=content,
    )
    assert result.status == "completed"
    assert len(content.compositions) == 1
    assert planner.requests[1].context["last_action"]["code"] == "revision_answer_required"


async def test_cancel_during_revision_propagates_and_keeps_call_outcome_unknown():
    entered, released = asyncio.Event(), asyncio.Event()

    class BlockingRevision(StatelessRevisionContent):
        async def compose(self, request):
            if request.revision is not None:
                assert request.revision.verification.issues[0].type == "currency"
                entered.set()
                try:
                    await asyncio.Future()
                finally:
                    released.set()
            return await super().compose(request)

    trace = MemoryTrace()
    task = asyncio.create_task(
        run(
            m.RunRequest(task_id="cancel-revision", question="Which currency?"),
            data=DemoData(),
            content=BlockingRevision(),
            planner=ActionPlanner([{"kind": "compose"}, {"kind": "verify"}, {"kind": "compose"}]),
            trace=trace,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert released.is_set()
    assert trace.closed
    event = trace.events[-1]
    assert event.event_type == "run.cancelled"
    assert event.detail["active_call"]["phase"] == "content.compose"
    assert event.detail["active_call"]["outcome"] == "unknown"
    assert event.detail["call_counts"]["model_calls"] == 1


async def test_resolving_verification_keeps_an_unresolved_composition_failure():
    class InterleavedContent(FlexibleContent):
        content_feedback_protocol = "content-feedback-v1"

        async def compose(self, request):
            response = await super().compose(request)
            if len(self.compositions) == 2:
                response.data.citations = ["invalid-compose-id"]
            return response

        async def verify(self, request):
            response = await super().verify(request)
            if len(self.verifications) == 1:
                response.data.issues = [
                    m.VerificationIssue(
                        type="unsupported",
                        evidence_ids=["invalid-verify-id"],
                        detail="Bad reference",
                    )
                ]
            return response

    result, planner, _, _ = await execute(
        [
            {"kind": "compose"},
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "verify"},
            {"kind": "stop", "reason": "无法完成生成修正"},
        ],
        content=InterleavedContent(),
    )
    assert result.stop_reason == "agent_stopped"
    assert len(planner.requests[-1].context["content_feedback"]) == 1
    assert result.failure.phase == "content.compose"
    assert result.failure.error.details["issues"][0]["actual"] == "invalid-compose-id"


async def test_successful_data_retry_does_not_leave_a_stale_terminal_failure():
    class RetryCatalog(DemoData):
        def __init__(self):
            super().__init__()
            self.calls = 0

        async def catalog(self, request):
            self.calls += 1
            if self.calls == 1:
                return m.CatalogResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(
                        code="temporary_data_error", message="Try again", retryable=True
                    ),
                )
            return await super().catalog(request)

    result = await run(
        m.RunRequest(task_id="retry-state", question="Question"),
        data=RetryCatalog(),
        content=FlexibleContent(),
        planner=ActionPlanner([{"kind": "stop", "reason": "No answer needed"}]),
        trace=MemoryTrace(),
    )
    assert result.stop_reason == "agent_stopped"
    assert result.call_counts.retries == 1
    assert result.failure is None
