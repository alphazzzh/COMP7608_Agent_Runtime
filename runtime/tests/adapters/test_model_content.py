"""隔离内容适配器的模型消息、引用校验、反馈及定向答案修订。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import importlib
import json

import httpx
import pytest
from pydantic import ValidationError

from lake_agent import models as m
from lake_agent.config import load_config
from tests.support.agent import execute
from tests.support.configuration import write_config
from tests.support.content_requests import (
    compose_request,
    envelope,
    evidence,
    register,
    verify_request,
)
from tests.support.model_content import ModelContent


def api():
    return importlib.import_module("tests.support.model_content")


async def test_same_evidence_questions_reach_model_and_distinct_answers_are_preserved(tmp_path):
    received = []
    answers = {
        "目前的合成资料有哪些": "目前有年度营收报告和季度背景资料两份合成资料。",
        "2024 年年度营收是多少": "2024 年年度营收为 1.2 亿港元。",
    }

    def respond(request):
        body = json.loads(request.content)
        incoming = json.loads(body["messages"][1]["content"])
        received.append(incoming)
        assert incoming["phase"] == "compose"
        assert request.headers["Authorization"] == "Bearer private-test-key"
        assert str(request.url) == "https://planner.example/v1/chat/completions"
        assert body["model"] == "configured-model"
        assert body["temperature"] == 0.25
        assert body["seed"] == 7
        assert body["response_format"] == {"type": "json_object"}
        assert body["stream"] is False
        assert "private-task" not in request.content.decode()
        return httpx.Response(
            200,
            json=envelope(
                {
                    "text": answers[incoming["input"]["question"]],
                    "citations": ["evidence-annual"],
                    "structured_result": {"model_field": 72},
                },
                {"prompt_tokens": 51, "completion_tokens": 19},
            ),
        )

    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            replies = [await content.compose(compose_request(question)) for question in answers]
        assert not client.is_closed
    assert [reply.data.text for reply in replies] == list(answers.values())
    assert received[0]["input"]["evidence"] == received[1]["input"]["evidence"]
    assert received[0]["input"]["question"] != received[1]["input"]["question"]
    catalog = received[0]["input"]["catalog_context"]
    assert catalog["catalog_snapshot_id"] == "demo-v1"
    assert {source["name"] for source in catalog["sources"]} == {
        "synthetic-report.txt",
        "synthetic-quarter.txt",
    }
    assert all(source["metadata"]["year"] == 2024 for source in catalog["sources"])
    for reply in replies:
        assert reply.status == "ok"
        assert reply.usage.provider_requests == 1
        assert reply.usage.input_tokens == 51
        assert reply.data.citations == ["evidence-annual"]
        assert reply.data.structured_result["model_field"] == 72
        assert reply.data.structured_result["test_scope"] == {
            "data": "synthetic_fixture",
            "retrieval": "local_fixture",
            "composition": "llm",
            "verification": "llm",
            "model_generated_answer": True,
        }
    assert content.catalogs == {}


async def test_verify_revenue_answer_for_inventory_question_honors_model_insufficiency(tmp_path):
    received = []

    def respond(request):
        incoming = json.loads(json.loads(request.content)["messages"][1]["content"])
        received.append(incoming)
        return httpx.Response(
            200,
            json=envelope(
                {
                    "verdict": "insufficient",
                    "issues": [
                        {
                            "type": "missing_support",
                            "evidence_ids": ["evidence-annual"],
                            "detail": "The cited evidence does not establish this answer.",
                        }
                    ],
                    "recommendation": {"action": "stop"},
                }
            ),
        )

    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.verify(
                verify_request(
                    answer=m.AnswerRecord(
                        text="2024 年年度营收为 1.2 亿港元。", citations=["evidence-annual"]
                    )
                )
            )
    assert reply.status == "ok"
    assert reply.data.verdict == "insufficient"
    assert reply.data.recommendation.action == "stop"
    assert reply.usage.provider_requests == 1
    assert received[0]["phase"] == "verify"
    assert received[0]["input"]["question"] == "目前的合成资料有哪些"
    assert received[0]["input"]["answer"]["text"] == "2024 年年度营收为 1.2 亿港元。"
    assert received[0]["input"]["evidence"] == [entry.model_dump() for entry in evidence()]


@pytest.mark.parametrize("model_result", [["model", 72], "model-scalar", None])
async def test_model_structured_result_and_empty_citations_are_preserved(tmp_path, model_result):
    settings = load_config(write_config(tmp_path))
    response = {"text": "模型原文保持不变。", "citations": [], "structured_result": model_result}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope(response)))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request())
    assert reply.data.text == response["text"]
    assert reply.data.citations == []
    if model_result is not None:
        assert reply.data.structured_result["model_result"] == model_result
    assert reply.data.structured_result["test_scope"]["composition"] == "llm"


async def test_model_test_scope_collision_preserves_complete_original_result(tmp_path):
    settings = load_config(write_config(tmp_path))
    original = {"test_scope": {"model_defined": "preserve this"}, "values": [1, {"amount": 72}]}
    response = {
        "text": "模型正文不应被改写。",
        "citations": ["evidence-quarter"],
        "structured_result": original,
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope(response)))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request())
    assert reply.data.text == response["text"]
    assert reply.data.citations == response["citations"]
    assert reply.data.structured_result["model_result"] == original
    assert reply.data.structured_result["test_scope"]["data"] == "synthetic_fixture"
    assert reply.data.structured_result["test_scope"]["model_generated_answer"] is True


@pytest.mark.parametrize("phase", ["compose", "verify"])
@pytest.mark.parametrize(
    "failure", ["http", "transport", "invalid-json", "invalid-schema", "oversize"]
)
async def test_model_failures_are_bounded_counted_once_and_do_not_leak_secrets(
    tmp_path, phase, failure
):
    calls = []

    def respond(request):
        calls.append(request)
        if failure == "http":
            return httpx.Response(429, text="private-provider-body")
        if failure == "transport":
            raise httpx.ConnectError("private-provider-detail", request=request)
        if failure == "invalid-json":
            return httpx.Response(200, content=b"private-non-json")
        if failure == "oversize":
            return httpx.Response(200, content=b"x" * 20000)
        return httpx.Response(200, json=envelope({"private-invalid-schema": True}))

    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            request = compose_request() if phase == "compose" else verify_request()
            reply = await getattr(content, phase)(request)
    assert reply.status == "error"
    assert reply.data is None
    assert reply.usage.provider_requests == 1
    assert len(calls) == 1
    # 模型自己的字段与错误值现在会作为诊断预览；HTTP封装和本地凭据仍不能回显。
    encoded = reply.model_dump_json()
    for secret in (
        "private-test-key",
        "private-provider-body",
        "private-provider-detail",
        "private-non-json",
    ):
        assert secret not in encoded


@pytest.mark.parametrize("phase", ["compose", "verify"])
async def test_timeout_counts_one_request_and_cancellation_propagates(tmp_path, phase):
    started = asyncio.Event()
    calls = []

    async def respond(request):
        calls.append(request)
        started.set()
        await asyncio.Future()

    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            request = compose_request() if phase == "compose" else verify_request()
            reply = await getattr(content, phase)(request.model_copy(update={"timeout_ms": 10}))
            assert reply.error.code == "provider_timeout"
            assert reply.usage.provider_requests == 1
            started.clear()
            task = asyncio.create_task(getattr(content, phase)(request))
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert len(calls) == 2


@pytest.mark.parametrize("phase", ["compose", "verify"])
@pytest.mark.parametrize("guard", ["params", "tokens", "catalog", "foreign-run", "foreign-source"])
async def test_invalid_local_requests_fail_before_dispatch(tmp_path, phase, guard):
    calls = []
    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            if guard != "catalog":
                await register(content)
            request = compose_request() if phase == "compose" else verify_request()
            if guard == "params":
                request.model_params = {"messages": []}
            elif guard == "tokens":
                request.token_budget = m.TokenBudget(remaining_output_tokens=10)
            elif guard == "foreign-run":
                request.run_id = "unregistered-other-run"
            elif guard == "foreign-source":
                request.evidence[0].source_id = "unregistered-source"
            reply = await getattr(content, phase)(request)
    assert reply.status == "error"
    assert reply.usage.provider_requests == 0
    assert calls == []


@pytest.mark.parametrize("phase", ["compose", "verify"])
async def test_unknown_output_citations_cannot_become_success(tmp_path, phase):
    value = (
        {"text": "Invalid citation", "citations": ["demo-annual"]}
        if phase == "compose"
        else {
            "verdict": "insufficient",
            "issues": [
                {"type": "unsupported", "evidence_ids": ["unknown-evidence"], "detail": "Missing"}
            ],
            "recommendation": {"action": "stop"},
        }
    )
    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope(value)))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            request = compose_request() if phase == "compose" else verify_request()
            reply = await getattr(content, phase)(request)
    assert reply.status == "error"
    assert reply.data is None
    assert reply.usage.provider_requests == 1


async def test_input_size_and_closed_client_fail_without_request(tmp_path):
    settings = load_config(write_config(tmp_path))
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request))
    ) as client:
        async with api().ModelContent(settings.llm, client=client, max_request_bytes=64) as content:
            await register(content)
            reply = await content.compose(compose_request())
            assert reply.error.code == "model_input_too_large"
            assert reply.usage.provider_requests == 0
        closed = await content.compose(compose_request())
        assert closed.error.code == "content_closed"
        assert closed.usage.provider_requests == 0
    assert calls == []


@pytest.mark.parametrize("phase", ["compose", "verify"])
async def test_explicit_content_feedback_reaches_messages_without_new_http_fields(tmp_path, phase):
    """上一轮具体错误通过请求扩展进入内容模型；提供方API仍用原来的字段。"""
    received = []
    result = (
        {"text": "Corrected", "citations": []}
        if phase == "compose"
        else {"verdict": "supported", "issues": [], "recommendation": {"action": "none"}}
    )

    def respond(request):
        body = json.loads(request.content)
        received.append(body)
        return httpx.Response(200, json=envelope(result))

    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            assert content.content_feedback_protocol == m.CONTENT_FEEDBACK_PROTOCOL
            await register(content)
            base = compose_request() if phase == "compose" else verify_request()
            request_type = (
                m.ComposeFeedbackRequest if phase == "compose" else m.VerifyFeedbackRequest
            )
            feedback = m.ContentFeedback(
                phase=phase,
                call_id="old-call",
                error=m.ErrorInfo(
                    code="invalid_model_citations",
                    message="Unknown citation",
                    details={
                        "issues": [{"path": "/citations/0", "actual": "invented"}],
                        "rejected_output": {"text": "Previous answer", "citations": ["invented"]},
                    },
                ),
            )
            reply = await getattr(content, phase)(
                request_type(**base.model_dump(), feedback=feedback)
            )
    assert reply.status == "ok"
    assert len(received) == reply.usage.provider_requests == 1
    assert set(received[0]) == {
        "temperature",
        "seed",
        "model",
        "messages",
        "response_format",
        "stream",
        "n",
    }
    incoming = json.loads(received[0]["messages"][1]["content"])
    assert incoming["input"]["feedback"] == feedback.model_dump()
    assert incoming["input"]["question"] == base.question


@pytest.mark.parametrize(
    "phase,path", [("compose", "/citations/0"), ("verify", "/issues/0/evidence_ids/0")]
)
async def test_unknown_citation_error_contains_exact_diff_and_original_output(
    tmp_path, phase, path
):
    value = (
        {"text": "Original answer", "citations": ["demo-annual"]}
        if phase == "compose"
        else {
            "verdict": "insufficient",
            "issues": [{"type": "missing", "evidence_ids": ["demo-annual"], "detail": "Missing"}],
            "recommendation": {"action": "stop"},
        }
    )
    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope(value)))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await getattr(content, phase)(
                compose_request() if phase == "compose" else verify_request()
            )
    details = reply.error.details
    assert details["issues"][0]["path"] == path
    assert details["issues"][0]["actual"] == "demo-annual"
    assert set(details["allowed_evidence_ids"]) == {"evidence-annual", "evidence-quarter"}
    if phase == "compose":
        assert details["rejected_output"]["text"] == value["text"]


async def test_verification_unknown_source_has_location_and_allowed_source_samples(tmp_path):
    value = {
        "verdict": "insufficient",
        "issues": [],
        "recommendation": {
            "action": "query_again",
            "focus": {"query": "Find details", "subtask_id": "s", "source_ids": ["unregistered"]},
        },
    }
    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope(value)))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.verify(verify_request())
    assert reply.error.code == "invalid_model_sources"
    assert reply.error.details["issues"][0]["path"] == "/recommendation/focus/source_ids/0"
    assert set(reply.error.details["allowed_source_ids"]) == {"demo-annual", "demo-quarter"}


async def test_model_schema_failure_has_field_path_and_actual_value(tmp_path):
    settings = load_config(write_config(tmp_path))
    value = {"text": "Model draft", "citations": [False]}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope(value)))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request())
    assert reply.error.code == "invalid_model_output"
    assert reply.error.details["issues"][0]["path"] == "/citations/0"
    assert reply.error.details["issues"][0]["actual"] is False
    assert reply.error.details["rejected_output"] == value


async def test_rejected_output_is_bounded_and_never_includes_http_secrets(tmp_path):
    settings = load_config(write_config(tmp_path))
    value = {"text": "x" * 20000 + " private-test-key", "citations": ["invented"]}
    provider = envelope(value)
    provider["headers"] = {"Authorization": "private-provider-header"}
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json=provider)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request(max_output_bytes=100000))
    encoded = reply.model_dump_json()
    assert reply.error.details["omitted"]
    assert (
        len(json.dumps(reply.error.details, ensure_ascii=False, separators=(",", ":")).encode())
        <= 8000
    )
    assert "private-test-key" not in encoded and "private-provider-header" not in encoded
    assert "Authorization" not in encoded and "planner.example" not in encoded
    assert len(calls) == reply.usage.provider_requests == 1


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"choices": []},
        {"choices": [{"finish_reason": "length", "message": {"content": "{}"}}]},
        {
            "choices": [
                {"finish_reason": "stop", "message": {"refusal": "blocked", "content": "{}"}}
            ]
        },
    ],
)
async def test_provider_contract_failures_are_not_model_output_correction_errors(tmp_path, payload):
    settings = load_config(write_config(tmp_path))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request())
    assert reply.error.code == "invalid_provider_response"
    assert reply.error.details is None
    assert reply.usage.provider_requests == 1


async def test_model_json_failure_identifies_parse_location_without_provider_envelope(tmp_path):
    settings = load_config(write_config(tmp_path))
    payload = {
        "choices": [{"finish_reason": "stop", "message": {"content": '{"text": invalid}'}}],
        "secret": "private-provider-envelope",
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request())
    assert reply.error.code == "invalid_model_output"
    assert reply.error.details["issues"][0]["line"] == 1
    assert reply.error.details["issues"][0]["column"] > 0
    assert reply.error.details["rejected_output"] == '{"text": invalid}'
    assert "private-provider-envelope" not in reply.model_dump_json()


async def test_foreign_phase_feedback_is_rejected_before_dispatch(tmp_path):
    settings = load_config(write_config(tmp_path))
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            request = m.ComposeFeedbackRequest(
                **compose_request().model_dump(),
                feedback=m.ContentFeedback(
                    phase="verify",
                    call_id="old",
                    error=m.ErrorInfo(code="invalid", message="Wrong phase"),
                ),
            )
            reply = await content.compose(request)
    assert reply.error.code == "invalid_content_feedback"
    assert reply.usage.provider_requests == 0 and calls == []


async def test_secret_crossing_preview_boundary_is_redacted_before_truncation(tmp_path):
    settings = load_config(write_config(tmp_path))
    value = {"text": "x" * 995 + "private-test-key" + "y" * 2000, "citations": ["invented"]}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope(value)))
    ) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request())
    assert "priva" not in reply.model_dump_json()
    assert reply.error.details["rejected_output"]["text"] == "x" * 995 + "yyyyy"


@pytest.mark.parametrize("budget", [512, 1024])
@pytest.mark.parametrize("failure", ["citations", "unicode", "schema", "json"])
async def test_entire_error_response_fits_output_budget_without_implicit_retry(
    tmp_path, budget, failure
):
    from lake_agent.validation import canonical_json

    settings = load_config(write_config(tmp_path))
    calls = []
    size = 100 if budget == 512 else 400
    if failure == "schema":
        content_text = json.dumps({"text": "x" * size, "citations": [False]})
    elif failure == "json":
        content_text = '{"text":"' + "x" * size + '" nope}'
    else:
        content_text = json.dumps(
            {
                "text": ("文" * (size // 6) if failure == "unicode" else "x" * size),
                "citations": ["wrong-id"],
            }
        )
    payload = {"choices": [{"message": {"content": content_text}, "finish_reason": "stop"}]}
    assert len(canonical_json(payload)) < budget

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request(max_output_bytes=budget))
    assert reply.status == "error"
    assert reply.error.code == (
        "invalid_model_output" if failure in {"schema", "json"} else "invalid_model_citations"
    )
    assert len(canonical_json(reply.model_dump(mode="json"))) <= budget
    assert reply.usage.provider_requests == len(calls) == 1


async def test_deep_schema_error_is_bounded_before_private_diagnostic_copy(tmp_path):
    from lake_agent.validation import canonical_json

    settings = load_config(write_config(tmp_path))
    model_text = '{"text":"answer","citations":[],"extra":' + "[" * 990 + "0" + "]" * 990 + "}"
    payload = {"choices": [{"finish_reason": "stop", "message": {"content": model_text}}]}
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async with api().ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(compose_request(max_output_bytes=20000))
    assert reply.error.code == "invalid_model_output"
    assert reply.error.details["omitted"]
    assert len(canonical_json(reply.model_dump(mode="json"))) <= 20000
    assert len(calls) == reply.usage.provider_requests == 1


def currency_revision(*, instructions="将美元替换为港元。", current=True):
    return m.AnswerRevision(
        previous_answer=m.AnswerRecord(text="营收为1.2亿美元。", citations=["evidence-annual"]),
        verification=m.Verification(
            verdict="conflicting",
            issues=[
                m.VerificationIssue(
                    type="currency", evidence_ids=["evidence-annual"], detail="币种错误：应为港元。"
                )
            ],
            recommendation=m.Recommendation(action="none"),
        ),
        verification_is_current=current,
        instructions=instructions,
    )


async def test_stateless_provider_can_revise_only_from_received_answer_issue_and_target(tmp_path):
    settings = load_config(write_config(tmp_path))
    calls = []

    def provider(request):
        body = json.loads(request.content)
        incoming = json.loads(body["messages"][1]["content"])["input"]
        calls.append((body, incoming))
        revision = incoming.get("revision")
        if revision is None:
            answer = {"text": "缺少旧答案与具体核验意见，无法执行定向修订。", "citations": []}
        else:
            old = revision["previous_answer"]
            issue = revision["verification"]["issues"][0]
            assert issue["detail"] == "币种错误：应为港元。"
            source, target = (
                revision["instructions"].removesuffix("。").removeprefix("将").split("替换为")
            )
            answer = {"text": old["text"].replace(source, target), "citations": old["citations"]}
        return httpx.Response(
            200, json=envelope(answer, {"prompt_tokens": 9, "completion_tokens": 4})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with ModelContent(settings.llm, client=client) as content:
            await register(content)
            first = await content.compose(compose_request(question="修正答案中的币种"))
            revised = await content.compose(
                compose_request(question="修正答案中的币种", revision=currency_revision())
            )
            alternate = currency_revision(instructions="将美元替换为香港元。")
            third = await content.compose(
                compose_request(question="修正答案中的币种", revision=alternate)
            )
    assert first.data.text == "缺少旧答案与具体核验意见，无法执行定向修订。"
    assert revised.data.text == "营收为1.2亿港元。"
    assert third.data.text == "营收为1.2亿香港元。"
    assert revised.data.citations == third.data.citations == ["evidence-annual"]
    assert len(calls) == 3
    assert [
        first.usage.provider_requests,
        revised.usage.provider_requests,
        third.usage.provider_requests,
    ] == [1, 1, 1]
    assert all("revision" not in body for body, _ in calls)
    assert calls[0][1]["question"] == calls[1][1]["question"] == calls[2][1]["question"]
    assert calls[0][1]["evidence"] == calls[1][1]["evidence"]
    assert calls[1][1]["revision"]["verification_is_current"] is True


async def test_revision_input_limit_is_checked_before_any_provider_dispatch(tmp_path):
    settings = load_config(write_config(tmp_path))
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request))
    ) as client:
        async with ModelContent(settings.llm, client=client, max_request_bytes=10000) as content:
            await register(content)
            reply = await content.compose(
                compose_request(revision=currency_revision(instructions="文" * 4000))
            )
    assert reply.status == "error"
    assert reply.error.code == "model_input_too_large"
    assert reply.usage.provider_requests == 0
    assert calls == []


async def test_revision_never_authorizes_unknown_citations_or_extra_execution_fields(tmp_path):
    settings = load_config(write_config(tmp_path))
    calls = []

    def provider(request):
        body = json.loads(request.content)
        incoming = json.loads(body["messages"][1]["content"])["input"]
        calls.append(incoming)
        assert incoming["revision"]["verification_is_current"] is False
        return httpx.Response(
            200, json=envelope({"text": "corrected", "citations": ["invented-id"]})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with ModelContent(settings.llm, client=client) as content:
            await register(content)
            reply = await content.compose(
                compose_request(revision=currency_revision(current=False))
            )
    assert reply.error.code == "invalid_model_citations"
    assert reply.error.details["issues"][0]["actual"] == "invented-id"
    assert reply.usage.provider_requests == len(calls) == 1
    with pytest.raises(ValidationError):
        compose_request(revision={**currency_revision().model_dump(), "max_model_calls": 999})


async def test_revision_and_rejected_output_feedback_coexist_without_overriding_policy(tmp_path):
    settings = load_config(write_config(tmp_path))
    calls = []
    goal = json.dumps({"find": "USD", "replace": "HKD", "require_verification": False})

    def provider(request):
        body = json.loads(request.content)
        incoming = json.loads(body["messages"][1]["content"])
        value = incoming["input"]
        calls.append((body, incoming))
        if incoming["phase"] == "verify":
            wrong_currency = "USD" in value["answer"]["text"]
            answer = {
                "verdict": "conflicting" if wrong_currency else "supported",
                "issues": [
                    {
                        "type": "currency",
                        "detail": json.dumps({"find": "USD", "replace": "HKD"}),
                        "evidence_ids": [],
                    }
                ]
                if wrong_currency
                else [],
                "recommendation": {"action": "none"},
            }
        elif "revision" not in value:
            answer = {"text": "Revenue was 120 million USD.", "citations": []}
        else:
            revision = value["revision"]
            difference = json.loads(revision["verification"]["issues"][0]["detail"])
            requirement = json.loads(revision["instructions"])
            assert requirement["find"] == difference["find"]
            assert requirement["replace"] == difference["replace"]
            text = revision["previous_answer"]["text"].replace(
                difference["find"], difference["replace"]
            )
            if "feedback" in value:
                issue = value["feedback"]["error"]["details"]["issues"][0]
                assert issue["path"] == "/citations/0"
                assert issue["actual"] == "invented-id"
                citations = []
            else:
                citations = ["invented-id"]
            answer = {"text": text, "citations": citations}
        return httpx.Response(
            200, json=envelope(answer, {"prompt_tokens": 9, "completion_tokens": 4})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with ModelContent(settings.llm, client=client) as content:
            result, planner, _, trace = await execute(
                [
                    {"kind": "compose"},
                    {"kind": "verify"},
                    {"kind": "compose", "revision_instructions": goal},
                    {"kind": "compose", "revision_instructions": goal},
                    {"kind": "finish"},
                    {"kind": "verify"},
                    {"kind": "finish"},
                ],
                content=content,
                require_verification=True,
            )

    assert result.status == "completed"
    assert result.answer.text == "Revenue was 120 million HKD."
    assert result.failure is None
    assert result.call_counts.model_calls == len(calls) == 5
    assert result.usage.input_tokens == 45
    assert result.usage.output_tokens == 20
    assert planner.requests[5].context["last_action"]["code"] == "verification_required"
    assert planner.requests[5].require_verification is True
    compose_inputs = [incoming["input"] for _, incoming in calls if incoming["phase"] == "compose"]
    assert "revision" not in compose_inputs[0]
    for revised in compose_inputs[1:]:
        assert revised["question"] == compose_inputs[0]["question"]
        assert revised["evidence"] == []
        assert revised["revision"]["previous_answer"]["text"] == "Revenue was 120 million USD."
        assert revised["revision"]["verification"]["verdict"] == "conflicting"
        assert revised["revision"]["verification_is_current"] is True
        assert json.loads(revised["revision"]["instructions"])["require_verification"] is False
    assert "feedback" not in compose_inputs[1]
    assert "feedback" in compose_inputs[2]
    assert all("revision" not in body and "feedback" not in body for body, _ in calls)
    assert any(event.event_type == "content.output.rejected" for event in trace.events)
    assert any(event.event_type == "content.feedback.resolved" for event in trace.events)
