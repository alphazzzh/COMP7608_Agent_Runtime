"""内置内容端口与 Runtime 的离线回归测试。"""

from __future__ import annotations

import json

import httpx
import pytest

from lake_agent import models as m
from lake_agent.adapters.content_adapter import JsonVerifierAdapter
from lake_agent.config import LLMSettings
from lake_agent.validation import canonical_json, catalog_digest
from lake_agent.web.configuration import _import_factory
from tests.support.agent import execute, tool_action
from tests.support.content_requests import compose_request, envelope, verify_request

SETTINGS = LLMSettings(base_url="https://content.example/v1", api_key="fixture", model_id="fixture")


async def test_content_factory_exposes_async_port():
    """生产配置可直接加载内容工厂，并显式声明纠错能力。"""
    factory = _import_factory("lake_agent.adapters.content_adapter:create_port")
    async with factory(options={}, llm=SETTINGS) as content:
        assert content.content_feedback_protocol == m.CONTENT_FEEDBACK_PROTOCOL
        assert all(
            callable(getattr(content, method))
            for method in ("prepare_catalog", "search", "compose", "verify")
        )


async def test_verifier_does_not_send_large_catalog_with_single_evidence_record():
    """注册目录不会扩大单条证据的核验请求。"""
    request = verify_request()
    sources = [
        m.SourceRecord(
            source_id=f"{'directory/' * 23}source-{index}.csv",
            name="Annual data",
            format="csv",
            summary="Annual report",
        )
        for index in range(1200)
    ]
    requests = []

    def provider(http_request):
        requests.append(json.loads(http_request.content))
        return httpx.Response(
            200, json=envelope({"verdict": "supported", "recommendation": {"action": "none"}})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with JsonVerifierAdapter(options={}, llm=SETTINGS, client=client) as content:
            registered = await content.prepare_catalog(
                m.CatalogRegistrationRequest(
                    **request.model_dump(include=set(m.CallContext.model_fields)),
                    catalog_snapshot_id="snapshot",
                    catalog_digest=catalog_digest(sources),
                    sources=sources,
                )
            )
            assert m.ModuleResult[m.CatalogHandle].model_validate(registered).status == "ok"
            response = await content.verify(request)

    assert m.ModuleResult[m.Verification].model_validate(response).data.verdict == "supported"
    (body,) = requests
    assert sources[-1].source_id not in body["messages"][1]["content"]


@pytest.mark.parametrize("method", ["compose", "verify"])
async def test_content_forwards_output_cap_and_actual_usage(method):
    """内容端口传递单次输出上限和实测用量供累计预算使用。"""
    requests = []
    value = (
        {"text": "120", "citations": ["evidence-annual"]}
        if method == "compose"
        else {"verdict": "supported", "recommendation": {"action": "none"}}
    )

    def provider(http_request):
        requests.append(json.loads(http_request.content))
        return httpx.Response(
            200, json=envelope(value, usage={"prompt_tokens": 90, "completion_tokens": 9})
        )

    request_factory = compose_request if method == "compose" else verify_request
    request = request_factory(
        token_budget=m.TokenBudget(
            remaining_input_tokens=500, remaining_output_tokens=30, max_output_tokens_this_call=30
        )
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with JsonVerifierAdapter(options={}, llm=SETTINGS, client=client) as content:
            reply = m.ModuleResult.model_validate(await getattr(content, method)(request))
    assert reply.status == "ok"
    assert reply.usage.input_tokens == 90
    assert reply.usage.output_tokens == 9
    (body,) = requests
    assert body["max_tokens"] == 30


async def test_runtime_routes_content_feedback_to_both_model_calls():
    """真实控制器将确定的输出拒绝分别送回下一次生成和核验。"""
    stages = {"compose": [], "verify": []}

    def provider(http_request):
        body = json.loads(http_request.content)
        payload = json.loads(body["messages"][1]["content"])
        phase = "verify" if "answer" in payload else "compose"
        stage = stages[phase]
        stage.append(payload)
        if phase == "compose":
            value = {"text": "120", "citations": [payload["evidence"][0]["evidence_id"]]}
            if len(stage) == 1:
                value["citations"] = ["unknown-evidence"]
        else:
            value = {"verdict": "supported", "recommendation": {"action": "none"}}
            if len(stage) == 1:
                value["recommendation"] = {"action": "invalid"}
        return httpx.Response(
            200, json=envelope(value, usage={"prompt_tokens": 20, "completion_tokens": 6})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with JsonVerifierAdapter(options={}, llm=SETTINGS, client=client) as content:
            result, _, _, trace = await execute(
                [
                    tool_action(),
                    {"kind": "compose"},
                    {"kind": "compose"},
                    {"kind": "verify"},
                    {"kind": "verify"},
                    {"kind": "finish"},
                ],
                content=content,
                require_verification=True,
            )

    assert result.status == "completed"
    assert result.answer.text == "120"
    assert result.usage.provider_requests == 4
    for phase, (first, second) in stages.items():
        assert first.get("feedback") is None
        assert second["feedback"]["phase"] == phase
        assert second["feedback"]["error"]["code"] in {
            "invalid_model_citations",
            "invalid_model_output",
        }
        assert second["question"] == first["question"]
    assert sum(event.event_type == "content.feedback.resolved" for event in trace.events) == 2


async def test_small_output_budget_contains_error_reply():
    """输出过大时用预算内的错误封套报告失败。"""
    request = compose_request(max_output_bytes=230)
    async with JsonVerifierAdapter(
        options={"enable_model_compose": False}, llm=SETTINGS
    ) as content:
        reply = await content.compose(request)
    assert len(canonical_json(reply)) <= request.max_output_bytes
    assert m.ModuleResult.model_validate(reply).error.code == "content_response_too_large"


async def test_catalog_uses_path_to_find_generic_filenames():
    """LakeQA 的通用文件名需要和来源路径一起建立检索索引。"""
    sources = [
        m.SourceRecord(
            source_id=f"reports/{topic}/rows.txt", name="rows.txt", format="txt", summary="Rows"
        )
        for topic in ("annual-revenue", "inventory")
    ]
    context = compose_request().model_dump(include=set(m.CallContext.model_fields))
    async with JsonVerifierAdapter(options={}, llm=SETTINGS) as content:
        registered = m.ModuleResult[m.CatalogHandle].model_validate(
            await content.prepare_catalog(
                m.CatalogRegistrationRequest(
                    **context,
                    catalog_snapshot_id="snapshot",
                    catalog_digest=catalog_digest(sources),
                    sources=sources,
                )
            )
        )
        found = m.ModuleResult[m.SearchResponse].model_validate(
            await content.search(
                m.SearchRequest(
                    **context,
                    question="What was the annual revenue?",
                    catalog_snapshot_id="snapshot",
                    catalog_handle=registered.data.handle,
                    limit=2,
                    subtask=m.Subtask(
                        subtask_id="find", objective="Find revenue", search_query="revenue"
                    ),
                )
            )
        )
    assert [candidate.source_id for candidate in found.data.candidates] == [sources[0].source_id]


async def test_invalid_provider_envelope_does_not_retry_as_content_feedback():
    """只有模型正文错误进入内容纠错，提供方封装错误保留原始失败。"""
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(200, json={"choices": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        async with JsonVerifierAdapter(options={}, llm=SETTINGS, client=client) as content:
            result, _, _, trace = await execute([{"kind": "compose"}], content=content)
    assert result.status == "failed"
    assert result.failure.error.code == "invalid_provider_response"
    assert len(calls) == 1
    assert all(event.event_type != "content.output.rejected" for event in trace.events)
