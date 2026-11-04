"""被拒内容输出后的自主纠错、预算、反馈并存和上下文分页。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import httpx
import pytest

from lake_agent import models as m
from lake_agent.config import load_config
from tests.support.agent import FlexibleContent, execute, tool_action
from tests.support.configuration import write_config
from tests.support.content_requests import envelope
from tests.support.model_content import ModelContent


class RepairContent(FlexibleContent):
    """显式支持修正请求的内容替身，每次生成恰好报告一个提供方请求。"""

    content_feedback_protocol = "content-feedback-v1"

    def __init__(self, *, failures=1, code="invalid_model_citations", invalid_success=False):
        super().__init__()
        self.failures, self.code, self.invalid_success = failures, code, invalid_success

    async def compose(self, request):
        self.compositions.append(request.model_copy(deep=True))
        usage = m.Usage(provider_requests=1, input_tokens=7, output_tokens=3)
        if len(self.compositions) <= self.failures:
            if self.invalid_success:
                return m.ModuleResult(
                    call_id=request.call_id,
                    data=m.AnswerRecord(text="未接纳的答案", citations=["demo-annual"]),
                    usage=usage,
                )
            return m.ModuleResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(code=self.code, message="Content output was rejected."),
                usage=usage,
            )
        return m.ModuleResult(
            call_id=request.call_id,
            data=m.AnswerRecord(
                text="修正后的答案", citations=[entry.evidence_id for entry in request.evidence]
            ),
            usage=usage,
        )


@pytest.mark.parametrize("invalid_success", [False, True])
async def test_rejected_compose_reaches_both_planner_and_next_composer(invalid_success):
    content = RepairContent(invalid_success=invalid_success)
    result, planner, _, trace = await execute(
        [tool_action(), {"kind": "compose"}, {"kind": "compose"}, {"kind": "finish"}],
        content=content,
    )
    assert result.status == "completed"
    assert result.answer.text == "修正后的答案"
    assert result.failure is None
    assert result.call_counts.model_calls == result.usage.provider_requests == 2
    assert not hasattr(content.compositions[0], "feedback")
    repair = content.compositions[1]
    assert isinstance(repair, m.ComposeFeedbackRequest)
    assert repair.question == content.compositions[0].question
    assert repair.evidence == content.compositions[0].evidence
    assert repair.feedback.call_id == content.compositions[0].call_id
    assert repair.feedback.error.code == "invalid_model_citations"
    observed = planner.requests[2].context["content_feedback"][0]
    assert observed["error"]["code"] == "invalid_model_citations"
    assert any(event.event_type == "content.output.rejected" for event in trace.events)
    assert any(event.event_type == "content.feedback.resolved" for event in trace.events)


async def test_repair_feedback_survives_an_intermediate_model_chosen_action():
    content = RepairContent()
    result, planner, _, _ = await execute(
        [
            {"kind": "compose"},
            {"kind": "read_catalog"},
            {"kind": "compose"},
            {"kind": "finish"},
        ],
        content=content,
    )
    assert result.status == "completed"
    assert planner.requests[2].context["last_action"]["kind"] == "read_catalog"
    assert planner.requests[2].context["content_feedback"][0]["phase"] == "compose"
    assert content.compositions[1].feedback.error.code == "invalid_model_citations"


@pytest.mark.parametrize(
    "code",
    ["provider_http_error", "provider_timeout", "invalid_model_input", "catalog_not_registered"],
)
async def test_infrastructure_or_host_errors_stop_with_specific_failure(code):
    result, planner, content, _ = await execute(
        [{"kind": "compose"}, {"kind": "compose"}], content=RepairContent(code=code)
    )
    assert result.status == "failed"
    assert len(content.compositions) == len(planner.requests) == 1
    assert result.failure.phase == "content.compose"
    assert result.failure.error.code == code


async def test_legacy_content_keeps_original_request_and_terminal_behavior():
    content = RepairContent()
    content.content_feedback_protocol = None
    result, _, _, _ = await execute([{"kind": "compose"}], content=content)
    assert result.stop_reason == "module_error"
    assert result.failure.error.code == "invalid_model_citations"
    assert not hasattr(content.compositions[0], "feedback")


async def test_repeated_rejections_exhaust_original_turn_budget_without_accepting_answer():
    content = RepairContent(failures=10)
    result, _, _, _ = await execute([{"kind": "compose"}] * 3, content=content, max_agent_turns=3)
    assert (result.status, result.stop_reason) == ("budget_exceeded", "agent_turn_budget")
    assert result.answer is None
    assert len(content.compositions) == 3
    assert result.call_counts.model_calls == 3
    assert result.failure.error.code == "invalid_model_citations"


async def test_repairs_cannot_refresh_model_request_budget():
    content = RepairContent(failures=10)
    result, _, _, _ = await execute([{"kind": "compose"}] * 4, content=content, max_model_calls=2)
    assert result.stop_reason == "model_call_budget"
    assert len(content.compositions) == result.call_counts.model_calls == 2
    assert result.answer is None


async def test_model_can_stop_after_rejection_with_the_specific_failure_preserved():
    result, _, content, _ = await execute(
        [{"kind": "compose"}, {"kind": "stop", "reason": "无法完成有效回答"}],
        content=RepairContent(),
    )
    assert result.stop_reason == "agent_stopped"
    assert len(content.compositions) == 1
    assert result.failure.call_id == content.compositions[0].call_id


async def test_finish_cannot_submit_an_older_answer_while_new_output_is_rejected():
    class LaterFailure(FlexibleContent):
        content_feedback_protocol = "content-feedback-v1"

        async def compose(self, request):
            reply = await super().compose(request)
            if len(self.compositions) == 2:
                reply.data.citations = ["nonexistent"]
            return reply

    result, planner, _, _ = await execute(
        [
            {"kind": "compose"},
            {"kind": "compose"},
            {"kind": "finish"},
            {"kind": "stop", "reason": "无法修复最新答案"},
        ],
        content=LaterFailure(),
    )
    assert result.stop_reason == "agent_stopped"
    assert result.answer.text.startswith("Answer 1:")
    assert planner.requests[3].context["last_action"]["code"] == "unresolved_content_failure"
    assert result.failure.error.code == "invalid_model_citations"


async def test_small_planner_context_keeps_feedback_identity_separate_from_large_diagnostics():
    content = RepairContent(invalid_success=True)
    result, planner, _, _ = await execute(
        [{"kind": "compose"}, {"kind": "compose"}, {"kind": "finish"}],
        content=content,
        max_planner_context_bytes=1024,
        planner_preview_bytes=256,
    )
    assert result.status == "completed"
    feedback = planner.requests[1].context["content_feedback"][0]
    assert feedback["error"]["code"] == "invalid_model_citations"
    assert feedback["call_id"] == content.compositions[0].call_id


async def test_verify_output_reference_failure_can_be_corrected_before_required_finish():
    class RepairVerify(FlexibleContent):
        content_feedback_protocol = "content-feedback-v1"

        async def verify(self, request):
            self.verifications.append(request.model_copy(deep=True))
            issues = (
                [m.VerificationIssue(type="unsupported", evidence_ids=["unknown"], detail="Bad ID")]
                if len(self.verifications) == 1
                else []
            )
            return m.ModuleResult(
                call_id=request.call_id,
                data=m.Verification(
                    verdict="supported",
                    issues=issues,
                    recommendation=m.Recommendation(action="none"),
                ),
                usage=m.Usage.local(),
            )

    content = RepairVerify()
    result, _, _, _ = await execute(
        [
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "finish"},
            {"kind": "verify"},
            {"kind": "finish"},
        ],
        content=content,
        require_verification=True,
    )
    assert result.status == "completed"
    assert isinstance(content.verifications[1], m.VerifyFeedbackRequest)
    assert content.verifications[1].feedback.error.code == "invalid_model_citations"


async def test_actual_isolated_content_model_receives_reference_feedback_in_second_prompt(tmp_path):
    """跨越真实控制器、HTTP适配器和序列化，确保不是只有测试端口看到反馈。"""
    settings = load_config(write_config(tmp_path))
    calls = []

    def provider(request):
        import json

        body = json.loads(request.content)
        incoming = json.loads(body["messages"][1]["content"])["input"]
        calls.append((body, incoming))
        if len(calls) == 1:
            answer = {"text": "第一轮未接纳", "citations": ["demo-annual"]}
        else:
            feedback = incoming["feedback"]
            assert feedback["error"]["details"]["issues"][0]["actual"] == "demo-annual"
            assert feedback["error"]["details"]["issues"][0]["path"] == "/citations/0"
            answer = {"text": "修正完成", "citations": [incoming["evidence"][0]["evidence_id"]]}
        return httpx.Response(
            200, json=envelope(answer, {"prompt_tokens": 11, "completion_tokens": 5})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with ModelContent(settings.llm, client=client) as content:
            result, planner, _, _ = await execute(
                [tool_action(), {"kind": "compose"}, {"kind": "compose"}, {"kind": "finish"}],
                content=content,
            )
    assert result.status == "completed"
    assert result.answer.text == "修正完成"
    assert len(calls) == result.call_counts.model_calls == 2
    assert result.usage.input_tokens == 22 and result.usage.output_tokens == 10
    assert "feedback" not in calls[1][0]
    assert calls[0][1]["question"] == calls[1][1]["question"]
    assert calls[0][1]["evidence"] == calls[1][1]["evidence"]
    assert (
        planner.requests[2].context["content_feedback"][0]["error"]["code"]
        == "invalid_model_citations"
    )


@pytest.mark.parametrize(
    "message", ["引用证据错误" * 1000, '\\"\n\t' * 1000], ids=["long-chinese", "escaped-text"]
)
async def test_minimum_context_does_not_terminate_on_long_multibyte_or_escaped_feedback(message):
    class VerboseFailure(RepairContent):
        async def compose(self, request):
            reply = await super().compose(request)
            if reply.status == "error":
                reply.error.message = message
            return reply

    result, planner, _, _ = await execute(
        [{"kind": "compose"}, {"kind": "compose"}, {"kind": "finish"}],
        content=VerboseFailure(),
        max_planner_context_bytes=1024,
        planner_preview_bytes=256,
    )
    assert result.status == "completed"
    feedback = planner.requests[1].context["content_feedback"][0]
    assert feedback["error"]["code"] == "invalid_model_citations"
    assert feedback["call_id"]
    from lake_agent.validation import canonical_json

    assert all(len(canonical_json(request.context)) <= 1024 for request in planner.requests)


@pytest.mark.parametrize(
    "message", ["引用证据错误" * 1000, '\\"\n\t' * 1000], ids=["long-chinese", "escaped-text"]
)
def test_minimum_context_prioritizes_complete_current_page_over_feedback_previews(message):
    from types import SimpleNamespace

    from lake_agent.engine.agent_context import build_agent_context
    from lake_agent.validation import canonical_json

    feedback = m.ContentFeedback(
        phase="compose",
        call_id="call-" + "a" * 32,
        error=m.ErrorInfo(
            code="invalid_model_citations",
            message=message,
            details={"issues": [{"actual": "invented"}]},
        ),
    )
    controller = SimpleNamespace(
        cfg=m.RunConfig(max_planner_context_bytes=1024, planner_preview_bytes=512),
        content_feedback={"compose": feedback},
        sources={},
        evidence=[],
        observations=[],
        answer=None,
        subtasks=[],
        completed_subtasks=[],
    )
    page = {
        "observation_id": "obs-" + "b" * 32,
        "text": "正文😀" * 18,
        "offset": 0,
        "next_offset": 18,
        "previous_pages": [],
        "omitted_pages": 0,
    }
    context = build_agent_context(controller, {"read_result": page})
    assert context["observation_read"] == page
    assert context["content_feedback"][0]["error"]["code"] == "invalid_model_citations"
    assert len(canonical_json(context)) <= 1024


@pytest.mark.parametrize("text_length", [150, 179, 200, 512])
def test_stale_page_may_be_omitted_when_feedback_and_working_memory_markers_need_space(text_length):
    from types import SimpleNamespace

    from lake_agent.engine.agent_context import build_agent_context
    from lake_agent.validation import canonical_json

    controller = SimpleNamespace(
        cfg=m.RunConfig(max_planner_context_bytes=1024, planner_preview_bytes=512),
        sources={},
        evidence=[],
        observations=[],
        answer=m.AnswerRecord(text="old"),
        subtasks=[],
        completed_subtasks=[],
        content_feedback={
            phase: m.ContentFeedback(
                phase=phase,
                call_id="call-" + suffix * 32,
                error=m.ErrorInfo(code="invalid_model_output", message="bad"),
            )
            for phase, suffix in (("compose", "a"), ("verify", "b"))
        },
    )
    page = {
        "observation_id": "obs-" + "c" * 32,
        "text": "x" * text_length,
        "offset": 0,
        "next_offset": text_length,
        "previous_pages": [],
        "omitted_pages": 0,
    }
    context = build_agent_context(
        controller, {"read_result": page, "last_action": {"kind": "verify"}}
    )
    assert len(canonical_json(context)) <= 1024
    assert {item["phase"] for item in context["content_feedback"]} == {"compose", "verify"}
    if context["observation_read"] is None:
        assert "observation_read" in context["context_status"]["omitted_fields"]
    else:
        assert context["observation_read"] == page


class InterleavedOutputFailures(FlexibleContent):
    """保留一个旧答案，同时产生两种内容阶段的未解决输出错误。"""

    content_feedback_protocol = "content-feedback-v1"

    async def compose(self, request):
        self.compositions.append(request.model_copy(deep=True))
        if len(self.compositions) > 1:
            return m.ModuleResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(code="invalid_model_citations", message="Bad compose"),
                usage=m.Usage.local(),
            )
        return m.ModuleResult(
            call_id=request.call_id,
            data=m.AnswerRecord(text="Accepted original answer"),
            usage=m.Usage.local(),
        )

    async def verify(self, request):
        self.verifications.append(request.model_copy(deep=True))
        return m.ModuleResult(
            call_id=request.call_id,
            status="error",
            error=m.ErrorInfo(code="invalid_model_output", message="Bad verify"),
            usage=m.Usage.local(),
        )


@pytest.mark.parametrize("read_kind", ["read_observation", "read_catalog"])
@pytest.mark.parametrize("read_before_failures", [False, True])
async def test_minimum_context_pages_and_two_feedback_phases_can_coexist_or_explicitly_omit_old_pages(
    read_kind, read_before_failures
):
    from lake_agent.validation import canonical_json

    observed = {}

    def remember_then_compose(request):
        observed["observation_id"] = request.context["tool_observations"][0]["observation_id"]
        return {"kind": "compose"}

    def read_action(request):
        if read_kind == "read_catalog":
            return {"kind": "read_catalog", "offset": 0, "limit": 1}
        return {
            "kind": "read_observation",
            "observation_id": observed["observation_id"],
            "pointer": "",
            "max_chars": 4096,
        }

    def after_read(request):
        field = "observation_read" if read_kind == "read_observation" else "catalog_page"
        observed["page"] = request.context[field]
        assert observed["page"] is not None
        if not read_before_failures:
            assert {item["phase"] for item in request.context["content_feedback"]} == {
                "compose",
                "verify",
            }
        return (
            {"kind": "compose"}
            if read_before_failures
            else {"kind": "stop", "reason": "Read feedback details"}
        )

    actions = [tool_action(), remember_then_compose]
    if read_before_failures:
        actions += [
            read_action,
            after_read,
            {"kind": "verify"},
            {"kind": "stop", "reason": "Both failed after reading"},
        ]
    else:
        actions += [{"kind": "compose"}, {"kind": "verify"}, read_action, after_read]
    result, planner, _, trace = await execute(
        actions,
        content=InterleavedOutputFailures(),
        max_planner_context_bytes=1024,
        planner_preview_bytes=512,
    )
    assert result.stop_reason == "agent_stopped"
    assert all(len(canonical_json(request.context)) <= 1024 for request in planner.requests)
    assert {item["phase"] for item in planner.requests[-1].context["content_feedback"]} == {
        "compose",
        "verify",
    }
    feedbacks = {item["phase"]: item for item in planner.requests[-1].context["content_feedback"]}
    assert feedbacks["compose"]["error"]["code"] == "invalid_model_citations"
    assert feedbacks["verify"]["error"]["code"] == "invalid_model_output"
    assert all(item["call_id"] for item in feedbacks.values())
    assert feedbacks["compose"]["call_id"] != feedbacks["verify"]["call_id"]
    event_type = "observation.read" if read_kind == "read_observation" else "catalog.read"
    event = next(event for event in trace.events if event.event_type == event_type)
    stored = trace.artifacts[event.data_ref]
    if read_kind == "read_observation":
        assert observed["page"]["text"]
        assert observed["page"]["text"] == stored["text"]
        assert observed["page"]["next_offset"] == stored["next_offset"]
    else:
        assert observed["page"] == stored
        assert observed["page"]["sources"][0]["source_id"] == "demo-annual"
        assert observed["page"]["sources"][0]["format"] == "text"
    if read_before_failures:
        field = "observation_read" if read_kind == "read_observation" else "catalog_page"
        final_context = planner.requests[-1].context
        if final_context[field] is None:
            assert field in final_context["context_status"]["omitted_fields"]
