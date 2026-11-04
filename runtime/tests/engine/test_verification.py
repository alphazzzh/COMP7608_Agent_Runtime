"""核验与完整答案和证据绑定的新鲜度、指纹及完成门禁。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import pytest

from lake_agent import models as m
from lake_agent.runtime import run
from lake_agent.validation import canonical_json
from tests.support.agent import (
    ActionPlanner,
    EvidenceContent,
    FlexibleContent,
    MemoryTrace,
    RevenueData,
    revenue_tool,
)


async def execute_freshness(actions, *, content=None, data=None, **config):
    planner, trace = ActionPlanner(actions), MemoryTrace()
    content, data = content or EvidenceContent(), data or RevenueData()
    result = await run(
        m.RunRequest(
            task_id="verification-freshness",
            question="What is the annual revenue?",
            config=m.RunConfig(**config),
        ),
        data=data,
        content=content,
        planner=planner,
        trace=trace,
    )
    return result, planner, content, data, trace


async def test_supported_verification_cannot_finish_after_conflicting_evidence_arrives():
    result, planner, content, _, _ = await execute_freshness(
        [
            revenue_tool("a"),
            {"kind": "compose"},
            {"kind": "verify"},
            revenue_tool("b", "conflict"),
            {"kind": "finish"},
            {"kind": "verify"},
            {"kind": "finish"},
            {"kind": "stop", "reason": "Current evidence conflicts."},
        ],
        require_verification=True,
    )
    assert (result.status, result.stop_reason) == ("insufficient_evidence", "agent_stopped")
    assert len(content.verifications) == 2
    assert len(content.verifications[0].evidence) == 1
    assert len(content.verifications[1].evidence) == 2
    after_evidence = planner.requests[4].context
    assert after_evidence["verification_status"] == "stale"
    assert after_evidence["verification_verdict"] is None
    assert planner.requests[5].context["last_action"]["code"] == "verification_stale"
    assert planner.requests[6].context["verification_verdict"] == "conflicting"
    assert planner.requests[7].context["last_action"]["code"] == "verification_required"


async def test_only_a_new_current_supported_verification_can_authorize_required_finish():
    result, planner, content, _, _ = await execute_freshness(
        [
            revenue_tool("a"),
            {"kind": "compose"},
            {"kind": "verify"},
            revenue_tool("b", "confirmation"),
            {"kind": "finish"},
            {"kind": "verify"},
            {"kind": "finish"},
        ],
        require_verification=True,
    )
    assert result.status == "completed"
    assert len(content.verifications) == 2
    assert planner.requests[5].context["last_action"]["code"] == "verification_stale"
    assert planner.requests[6].context["verification_status"] == "current"
    assert planner.requests[6].context["verification_verdict"] == "supported"


async def test_changed_answer_does_not_reuse_old_supported_verification():
    result, planner, content, _, _ = await execute_freshness(
        [
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "compose"},
            {"kind": "finish"},
            {"kind": "verify"},
            {"kind": "finish"},
        ],
        content=FlexibleContent(),
        require_verification=True,
    )
    assert result.status == "completed"
    assert len(content.verifications) == 2
    assert planner.requests[3].context["verification_verdict"] is None
    assert planner.requests[4].context["last_action"]["status"] == "rejected"


async def test_identical_recomposition_cannot_wash_a_current_failed_verdict():
    result, planner, content, _, _ = await execute_freshness(
        [
            revenue_tool("a"),
            revenue_tool("b", "conflict"),
            {"kind": "compose"},
            {"kind": "verify"},
            {"kind": "compose"},
            {"kind": "finish"},
        ]
    )
    assert result.status == "insufficient_evidence"
    assert content.compositions[0].evidence == content.compositions[1].evidence
    assert len(content.verifications) == 1
    assert planner.requests[-1].context["verification_verdict"] == "conflicting"


@pytest.mark.parametrize(
    "operation", ["plan", "catalog", "search", "observation", "empty-tool", "failed-tool"]
)
async def test_operations_without_new_verification_inputs_do_not_force_reverification(operation):
    def read_observation(request):
        return {
            "kind": "read_observation",
            "observation_id": request.context["tool_observations"][0]["observation_id"],
            "pointer": "/data",
        }

    operations = {
        "plan": {"kind": "plan", "subtasks": []},
        "catalog": {"kind": "read_catalog", "limit": 1},
        "search": {"kind": "search", "query": "annual"},
        "observation": read_observation,
        "empty-tool": revenue_tool("inspect", "inspect-only"),
        "failed-tool": revenue_tool("unavailable", "recoverable-error"),
    }
    result, planner, content, _, _ = await execute_freshness(
        [
            revenue_tool("a"),
            {"kind": "compose"},
            {"kind": "verify"},
            operations[operation],
            {"kind": "finish"},
        ],
        require_verification=True,
    )
    assert result.status == "completed"
    assert len(content.verifications) == 1
    assert planner.requests[-1].context["verification_verdict"] == "supported"


async def test_stale_negative_verdict_is_not_a_current_rejection_when_verification_is_optional():
    result, planner, content, _, _ = await execute_freshness(
        [
            revenue_tool("a"),
            revenue_tool("b", "conflict"),
            {"kind": "compose"},
            {"kind": "verify"},
            revenue_tool("c", "confirmation"),
            {"kind": "finish"},
        ],
        require_verification=False,
    )
    # completed 仅表达宿主政策接受；这里明确未证明冲突答案正确。
    assert result.status == "completed"
    assert len(content.verifications) == 1
    assert planner.requests[-1].context["verification_verdict"] is None


async def test_small_truncated_context_cannot_make_a_stale_verification_current():
    result, planner, content, _, _ = await execute_freshness(
        [
            revenue_tool("a"),
            {"kind": "compose"},
            {"kind": "verify"},
            revenue_tool("b", "conflict"),
            {"kind": "finish"},
            {"kind": "stop", "reason": "Must inspect the conflicting full input."},
        ],
        data=RevenueData(padding="A shared preview prefix. " * 400),
        require_verification=True,
        max_planner_context_bytes=1024,
        planner_preview_bytes=256,
    )
    assert result.stop_reason == "agent_stopped"
    assert len(content.verifications[0].evidence[0].payload["notes"]) > 1024
    assert all(len(canonical_json(request.context)) <= 1024 for request in planner.requests)
    assert planner.requests[5].context["last_action"]["code"] == "verification_stale"


def fingerprint(answer=None, evidence=None, **policy):
    from lake_agent.engine.verification_state import verification_fingerprint

    return verification_fingerprint(
        question="What was revenue?",
        answer=answer or m.AnswerRecord(text="120 HKD", citations=["ev-a"]),
        evidence=[record()] if evidence is None else evidence,
        mode=policy.get("mode", "full_agent"),
        require_verification=policy.get("require_verification", True),
    )


def record():
    return m.EvidenceRecord(
        evidence_id="ev-a",
        call_id="call-a",
        source_id="annual",
        content_type="text",
        payload="Revenue was 120 HKD.",
        locator={"page": 1},
    )


@pytest.mark.parametrize(
    "field", ["text", "citations", "unresolved_questions", "structured_result"]
)
def test_entire_answer_affects_verification_identity(field):
    answer = m.AnswerRecord(text="120 HKD", citations=["ev-a"])
    changed = answer.model_copy(deep=True)
    setattr(
        changed,
        field,
        {
            "text": "130 HKD",
            "citations": [],
            "unresolved_questions": ["Which year?"],
            "structured_result": {"amount": 130},
        }[field],
    )
    assert fingerprint(answer) != fingerprint(changed)


def test_full_evidence_payload_and_order_affect_verification_identity():
    first = record()
    changed = first.model_copy(deep=True)
    changed.payload += " " + "x" * 4000 + " Conflicting source detail beyond preview."
    assert fingerprint(evidence=[first]) != fingerprint(evidence=[changed])
    second = first.model_copy(update={"evidence_id": "ev-b", "call_id": "call-b"})
    assert fingerprint(evidence=[first, second]) != fingerprint(evidence=[second, first])


def test_equivalent_json_and_acceptance_policy_have_deterministic_identities():
    answer = m.AnswerRecord(text="120 HKD", structured_result={"a": 1, "b": 2})
    reordered = m.AnswerRecord(text="120 HKD", structured_result={"b": 2, "a": 1})
    assert fingerprint(answer) == fingerprint(reordered)
    assert fingerprint() != fingerprint(require_verification=False)
    assert fingerprint() != fingerprint(mode="no_verifier", require_verification=False)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_id", "quarter"),
        ("evidence_id", "ev-other"),
        ("call_id", "call-other"),
        ("content_type", "json"),
        ("locator", {"page": 2}),
        ("warnings", ["Value has not been audited."]),
    ],
)
def test_evidence_identity_covers_provenance_and_warnings_beyond_payload(field, value):
    original = record()
    changed = original.model_copy(deep=True)
    setattr(changed, field, value)
    assert fingerprint(evidence=[original]) != fingerprint(evidence=[changed])


def test_empty_evidence_is_a_distinct_complete_verification_input():
    assert fingerprint(evidence=[]) != fingerprint(evidence=[record()])
