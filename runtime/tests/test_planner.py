"""单请求异步规划适配器的离线契约测试。"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from lake_agent.models import PlannerRequest, TokenBudget, ToolPlanningRequest
from lake_agent.planner import JsonPlanner


def request(**changes: object) -> PlannerRequest:
    """构造最小且符合实际的控制器到规划器请求。"""
    fields = {
        "run_id": "run-1",
        "task_id": "opaque-task",
        "operation_id": "plan-1",
        "call_id": "plan-1-attempt-1",
        "timeout_ms": 1000,
        "max_output_bytes": 50_000,
        "model_id": "example-model",
        "prompt_version": "planner-v1",
        "question": "What was the annual revenue?",
        "catalog_snapshot_id": "snapshot-1",
        "tool_specs": [],
        "budget_remaining": {
            "wall_time_ms": 5000,
            "model_calls": 5,
            "data_calls": 5,
            "plan_steps": 5,
            "subtasks": 5,
        },
    }
    fields.update(changes)
    return PlannerRequest.model_validate(fields)


def task_plan() -> dict[str, object]:
    return {
        "subtasks": [
            {
                "subtask_id": "subtask-1",
                "objective": "Find revenue",
                "search_query": "annual revenue",
                "required_evidence": ["value", "year"],
                "depends_on": [],
            }
        ]
    }


def provider_response(value: object, *, usage: object = None) -> dict[str, object]:
    result = {"choices": [{"message": {"content": json.dumps(value)}, "finish_reason": "stop"}]}
    if usage is not None:
        result["usage"] = usage
    return result


async def test_decompose_validates_output_and_reports_actual_usage() -> None:
    calls = []

    async def respond(incoming: httpx.Request) -> httpx.Response:
        calls.append(incoming)
        body = json.loads(incoming.content)
        assert incoming.url == "https://planner.example/v1/chat/completions"
        assert incoming.headers["Authorization"] == "Bearer private-key"
        assert body["model"] == "example-model"
        assert body["response_format"] == {"type": "json_object"}
        assert body["stream"] is False
        assert "opaque-task" not in json.dumps(body)
        assert "subtasks" in body["messages"][0]["content"]
        assert (
            "Every ToolStep.subtask_id must exactly equal input.subtask.subtask_id"
            in (body["messages"][0]["content"])
        )
        return httpx.Response(
            200,
            json=provider_response(
                task_plan(), usage={"prompt_tokens": 42, "completion_tokens": 24}
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        planner = JsonPlanner("https://planner.example/v1/", "private-key", client=client)
        result = await planner.decompose(request(model_params={"temperature": 0}))
        await planner.aclose()
        assert not client.is_closed
    assert result.call_id == "plan-1-attempt-1"
    assert result.status == "ok"
    assert result.data.subtasks[0].subtask_id == "subtask-1"
    assert result.usage.input_tokens == 42
    assert result.usage.output_tokens == 24
    assert result.usage.provider_requests == 1
    assert result.usage.cost is None
    assert len(calls) == 1


async def test_select_tools_accepts_finish_decision() -> None:
    fields = request().model_dump()
    fields.pop("catalog_snapshot_id")
    fields.update(
        subtask=task_plan()["subtasks"][0],
        candidates=[],
        evidence_summary=[],
        verifier_focus=None,
    )
    tool_request = ToolPlanningRequest.model_validate(fields)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json=provider_response({"kind": "finish", "reason": "insufficient_evidence"}),
            )
        )
    ) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).select_tools(
            tool_request
        )
    assert result.status == "ok"
    assert result.data.kind == "finish"


@pytest.mark.parametrize(
    "content",
    [
        '{"subtasks": []}',
        '{"subtasks": [], "subtasks": []}',
        '{"subtasks": NaN}',
        '```json\n{"subtasks": []}\n```',
    ],
)
async def test_invalid_output_returns_error_without_repair_request(content: str) -> None:
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200, json={"choices": [{"message": {"content": content}, "finish_reason": "stop"}]}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decompose(
            request()
        )
    assert result.status == "error"
    assert result.error.code == "invalid_model_output"
    assert result.usage.provider_requests == 1
    assert calls == 1


@pytest.mark.parametrize("params", [{"messages": []}, {"model": "other"}, {"foo": 1}])
async def test_protected_or_unknown_params_fail_before_dispatch(params: dict[str, object]) -> None:
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: calls.append(req))
    ) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decompose(
            request(model_params=params)
        )
    assert result.status == "error"
    assert result.error.code == "invalid_model_params"
    assert result.usage.provider_requests == 0
    assert result.usage.input_tokens == result.usage.output_tokens == 0
    assert not calls


async def test_token_caps_rejected_without_matching_tokenizer() -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: None)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decompose(
            request(token_budget=TokenBudget(remaining_input_tokens=100))
        )
    assert result.error.code == "token_budget_unsupported"
    assert result.usage.provider_requests == 0


async def test_network_failure_counts_one_dispatched_request_and_leaks_no_secrets() -> None:
    calls = 0

    def fail(incoming: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("private-key and arbitrary remote detail", request=incoming)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
        result = await JsonPlanner(
            "https://planner.example/v1", "private-key", client=client
        ).decompose(request())
    assert result.error.code == "provider_transport_error"
    assert "private-key" not in result.model_dump_json()
    assert result.usage.provider_requests == 1
    assert result.usage.input_tokens is None
    assert calls == 1


async def test_http_error_is_not_retried_or_echoed() -> None:
    calls = 0

    def fail(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, json={"error": "secret-provider-body"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decompose(
            request()
        )
    assert result.error.code == "provider_http_error"
    assert "secret-provider-body" not in result.model_dump_json()
    assert calls == 1


async def test_response_limit_closes_stream_and_returns_error() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 4096))
    ) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decompose(
            request(max_output_bytes=1024)
        )
    assert result.error.code == "model_response_too_large"
    assert result.usage.provider_requests == 1


async def test_adapter_timeout_ends_request_and_is_not_retried() -> None:
    calls = 0

    async def slow(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        await asyncio.sleep(1)
        return httpx.Response(200, json=provider_response(task_plan()))

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decompose(
            request(timeout_ms=10)
        )
    assert result.error.code == "provider_timeout"
    assert result.usage.provider_requests == 1
    assert calls == 1


async def test_cancellation_propagates_to_caller() -> None:
    started = asyncio.Event()

    async def wait(_: httpx.Request) -> httpx.Response:
        started.set()
        await asyncio.Future()

    async with httpx.AsyncClient(transport=httpx.MockTransport(wait)) as client:
        planner = JsonPlanner("https://planner.example/v1", "key", client=client)
        running = asyncio.create_task(planner.decompose(request()))
        await started.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running


async def test_closed_adapter_fails_before_dispatch() -> None:
    planner = JsonPlanner("https://planner.example/v1", "key")
    await planner.aclose()
    result = await planner.decompose(request())
    assert result.error.code == "planner_closed"
    assert result.usage.provider_requests == 0
