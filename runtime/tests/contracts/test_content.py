"""答案修订、内容输出反馈及有界诊断的共享契约回归。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from lake_agent import models as m
from tests.support.content_requests import compose_request


def rejected_verification():
    return m.Verification(
        verdict="conflicting",
        issues=[m.VerificationIssue(type="currency", detail="将美元修为港元。")],
        recommendation=m.Recommendation(action="none"),
    )


def test_revision_preserves_full_answer_and_concrete_feedback_without_policy_fields():
    old = m.AnswerRecord(
        text="营收为1.2亿美元。",
        citations=["evidence-annual"],
        unresolved_questions=["币种待核对"],
        structured_result={"currency": "USD"},
    )
    revision = m.AnswerRevision(
        previous_answer=old,
        verification=rejected_verification(),
        verification_is_current=True,
        instructions="修正币种，保留已有正确数值。",
    )
    request = compose_request(revision=revision)
    encoded = json.loads(request.model_dump_json())
    assert encoded["revision"]["previous_answer"] == old.model_dump(mode="json")
    assert encoded["revision"]["verification"]["issues"][0]["detail"] == "将美元修为港元。"
    assert encoded["question"] == "目前的合成资料有哪些"
    assert "require_verification" not in encoded["revision"]
    assert m.ComposeRequest.model_validate(encoded).revision.verification_is_current is True


def test_first_compose_and_default_action_keep_old_wire_shape_including_feedback_subclass():
    request = compose_request()
    assert request.revision is None
    assert "revision" not in request.model_dump()
    assert "revision" not in json.loads(request.model_dump_json())
    feedback = m.ContentFeedback(
        phase="compose",
        call_id="failed",
        error=m.ErrorInfo(code="invalid_model_output", message="bad"),
    )
    extended = m.ComposeFeedbackRequest(**request.model_dump(), feedback=feedback)
    assert "revision" not in extended.model_dump()
    assert extended.feedback == feedback
    action = m.AgentComposeDecision()
    assert action.revision_instructions is None
    assert action.model_dump() == {"kind": "compose"}


@pytest.mark.parametrize(
    "changes",
    [
        {"verification_is_current": True},
        {"verification_is_current": "true"},
        {"require_verification": False},
        {"instructions": ""},
        {"instructions": 123},
        {"instructions": "文" * 4001},
    ],
)
def test_invalid_or_privileged_revision_payloads_are_rejected(changes):
    with pytest.raises(ValidationError):
        m.AnswerRevision(previous_answer=m.AnswerRecord(text="old"), **changes)


def test_revision_target_has_strict_limit_and_stale_feedback_is_explicit():
    target = "文" * 4000
    decision = m.AgentComposeDecision(revision_instructions=target)
    assert decision.revision_instructions == target
    revision = m.AnswerRevision(
        previous_answer=m.AnswerRecord(text="old"),
        verification=rejected_verification(),
        verification_is_current=False,
        instructions=target,
    )
    assert m.AnswerRevision.model_validate_json(revision.model_dump_json()) == revision
    for value in ("", 123, "文" * 4001):
        with pytest.raises(ValidationError):
            m.AgentComposeDecision(revision_instructions=value)
    with pytest.raises(ValidationError):
        m.AgentComposeDecision(revision_instructions="fix", require_verification=False)


def helper():
    return importlib.import_module("lake_agent.engine.content_feedback")


def test_old_error_shape_omits_optional_diagnostics_even_when_nested():
    error = m.ErrorInfo(code="old_error", message="Known error")
    assert error.model_dump() == {
        "code": "old_error",
        "message": "Known error",
        "retryable": False,
        "recoverable": False,
    }
    response = m.ModuleResult[dict](call_id="call", status="error", error=error)
    assert "details" not in json.loads(response.model_dump_json())["error"]


def test_feedback_requests_extend_only_explicit_capability_requests():
    from tests.support.content_requests import compose_request, verify_request

    feedback = m.ContentFeedback(
        phase="compose",
        call_id="rejected",
        error=m.ErrorInfo(code="invalid_model_citations", message="Unknown evidence"),
    )
    fields = compose_request().model_dump()
    assert "feedback" not in fields
    request = m.ComposeFeedbackRequest(**fields, feedback=feedback)
    assert request.feedback.call_id == "rejected"
    with pytest.raises(ValidationError):
        m.ComposeRequest.model_validate(request.model_dump())
    verification = m.VerifyFeedbackRequest(
        **verify_request().model_dump(), feedback=feedback.model_copy(update={"phase": "verify"})
    )
    assert verification.feedback.phase == "verify"


def test_error_diagnostics_require_finite_json_with_a_byte_limit():
    for value in ({"text": "x" * 8001}, {"number": float("nan")}, {"bytes": b"private"}):
        with pytest.raises(ValidationError):
            m.ErrorInfo(code="invalid", message="Invalid", details=value)


def test_reference_diagnostics_locate_every_unknown_id_and_preserve_answer():
    output = m.AnswerRecord(text="Original model answer", citations=["known", "invented"])
    error = helper().reference_error("compose", output, allowed_evidence_ids=["known"])
    assert error.code == "invalid_model_citations"
    assert error.details["issues"] == [
        {
            "path": "/citations/1",
            "code": "unknown_evidence_id",
            "actual": "invented",
            "expected": "an evidence_id from allowed_evidence_ids",
        }
    ]
    assert error.details["allowed_evidence_ids"] == ["known"]
    assert error.details["rejected_output"]["text"] == output.text
    assert (
        helper().reference_error(
            "compose", m.AnswerRecord(text="Catalog answer"), allowed_evidence_ids=[]
        )
        is None
    )


def test_verification_diagnostics_locate_issue_citations_and_focus_sources():
    output = m.Verification(
        verdict="insufficient",
        issues=[m.VerificationIssue(type="missing", evidence_ids=["bad"], detail="Missing")],
        recommendation=m.Recommendation(
            action="query_again", focus=m.Focus(query="q", subtask_id="s", source_ids=["other"])
        ),
    )
    error = helper().reference_error(
        "verify", output, allowed_evidence_ids=["known"], allowed_source_ids=["source"]
    )
    assert {issue["path"] for issue in error.details["issues"]} == {
        "/issues/0/evidence_ids/0",
        "/recommendation/focus/source_ids/0",
    }
    assert error.details["allowed_source_ids"] == ["source"]


def test_diagnostics_are_bounded_and_explicit_about_omissions():
    details = helper().build_diagnostics(
        issues=[
            {
                "path": "/" + "p" * 20000,
                "code": "invalid",
                "actual": "x" * 30000,
                "expected": "valid",
            }
        ]
        * 100,
        allowed_evidence_ids=["e" * 500 + str(i) for i in range(100)],
        rejected_output={"text": "原文" * 20000},
        max_bytes=512,
    )
    assert len(json.dumps(details, ensure_ascii=False, separators=(",", ":")).encode()) <= 512
    assert details["omitted"]
    m.ErrorInfo(code="invalid", message="Invalid", details=details)
    with pytest.raises(ValueError):
        helper().build_diagnostics(max_bytes=0)


def test_feedback_capability_accepts_only_the_exact_protocol():
    for value in (None, True, "future-protocol", {"secret": "value"}):
        assert helper().feedback_protocol(SimpleNamespace(content_feedback_protocol=value)) is None
    assert (
        helper().feedback_protocol(
            SimpleNamespace(content_feedback_protocol=m.CONTENT_FEEDBACK_PROTOCOL)
        )
        == "content-feedback-v1"
    )


def test_run_failure_is_optional_and_preserved_only_when_present():
    fields = dict(
        run_id="run",
        task_id="task",
        mode="full_agent",
        status="failed",
        stop_reason="module_error",
        started_at="now",
        ended_at="now",
        duration_ms=1,
        versions=m.Versions(
            model_id="model",
            model_params={},
            prompt_versions=m.PromptVersions(),
            config_version="test",
        ),
    )
    result = m.RunResult(**fields)
    assert "failure" not in json.loads(result.model_dump_json())
    failure = m.RunFailure(
        phase="content.compose",
        call_id="call",
        error=m.ErrorInfo(
            code="invalid_model_citations", message="Unknown evidence", details={"issues": []}
        ),
    )
    result = m.RunResult(**fields, failure=failure)
    assert result.model_dump()["failure"]["call_id"] == "call"
    assert m.RunResult.model_validate_json(result.model_dump_json()).failure == failure


def test_diagnostics_do_not_list_partial_ids_or_expand_extreme_integers():
    details = helper().build_diagnostics(
        allowed_evidence_ids=["known", "x" * 5000], rejected_output={"integer": 10**5000}
    )
    assert details["allowed_evidence_ids"] == ["known"]
    assert details["omitted"]["allowed_evidence_ids"] == 1
    assert len(json.dumps(details)) < 8000
