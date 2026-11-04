"""自主规划器动作解析、提示、HTTP 请求和上下文预算的离线回归。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from pydantic import TypeAdapter, ValidationError

from lake_agent import models, ports
from lake_agent import models as m
from lake_agent.adapters.planner import JsonPlanner
from lake_agent.models import TokenBudget
from tests.support.planner_requests import provider_response, request, task_plan


async def test_decide_validates_output_and_reports_actual_usage() -> None:
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
        assert "no required action order" in body["messages"][0]["content"]
        return httpx.Response(
            200,
            json=provider_response(
                {"kind": "plan", **task_plan()},
                usage={"prompt_tokens": 42, "completion_tokens": 24},
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        planner = JsonPlanner("https://planner.example/v1/", "private-key", client=client)
        result = await planner.decide(request(model_params={"temperature": 0}))
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


async def test_decide_accepts_finish_decision() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=provider_response({"kind": "finish"}))
        )
    ) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            request()
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
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
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
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            request(model_params=params)
        )
    assert result.status == "error"
    assert result.error.code == "invalid_model_params"
    assert result.usage.provider_requests == 0
    assert result.usage.input_tokens == result.usage.output_tokens == 0
    assert not calls


async def test_token_caps_rejected_without_matching_tokenizer() -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: None)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
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
        ).decide(request())
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
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            request()
        )
    assert result.error.code == "provider_http_error"
    assert "secret-provider-body" not in result.model_dump_json()
    assert calls == 1


async def test_response_limit_closes_stream_and_returns_error() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 4096))
    ) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
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
        return httpx.Response(200, json=provider_response({"kind": "plan", **task_plan()}))

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
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
        running = asyncio.create_task(planner.decide(request()))
        await started.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running


async def test_closed_adapter_fails_before_dispatch() -> None:
    planner = JsonPlanner("https://planner.example/v1", "key")
    await planner.aclose()
    result = await planner.decide(request())
    assert result.error.code == "planner_closed"
    assert result.usage.provider_requests == 0


_ACTIONS = [
    {"kind": "search", "query": "annual revenue"},
    {"kind": "search", "query": "annual revenue", "source_ids": ["source-1"]},
    {
        "kind": "tools",
        "steps": [
            {
                "step_id": "step-1",
                "tool_name": "read_source",
                "source_ids": ["source-1"],
                "args": {"source_id": "source-1"},
                "purpose": "Read the registered annual report",
            }
        ],
    },
    {"kind": "read_observation", "observation_id": "observation-1"},
    {"kind": "read_catalog"},
    {"kind": "plan"},
    {
        "kind": "plan",
        "subtasks": task_plan()["subtasks"],
        "completed_subtask_ids": ["previous-subtask"],
    },
    {"kind": "compose"},
    {"kind": "verify"},
    {"kind": "finish"},
    {"kind": "stop", "reason": "No further useful action"},
]


def decision_request(**changes: object) -> models.AgentDecisionRequest:
    """沿用既有控制器元数据，加入新协议专属且可公开的决策上下文。"""
    fields = request().model_dump()
    fields.update(context={"answer": None}, remaining_turns=8)
    fields.update(changes)
    return models.AgentDecisionRequest.model_validate(fields)


@pytest.mark.parametrize("value", _ACTIONS)
def test_agent_actions_validate_without_legacy_subtask_binding(value: dict[str, object]) -> None:
    assert hasattr(models, "AgentAction")
    action = TypeAdapter(models.AgentAction).validate_python(value, strict=True)
    assert action.kind == value["kind"]
    if action.kind == "tools":
        assert "subtask_id" not in action.steps[0].model_dump()
    if action.kind == "read_observation":
        assert isinstance(action, models.ReadObservationDecision)
    if action.kind == "read_catalog":
        assert action.offset == 0
        assert action.limit == 20


@pytest.mark.parametrize(
    "value",
    [
        {"kind": "unknown"},
        {"kind": "actions", "steps": []},
        {"kind": "tools", "steps": []},
        {"kind": "search", "query": ""},
        {"kind": "search", "query": "q", "source_ids": [""]},
        {"kind": "search", "query": "q", "source_ids": "source-1"},
        {"kind": "read_catalog", "offset": -1},
        {"kind": "read_catalog", "offset": True},
        {"kind": "read_catalog", "limit": 0},
        {"kind": "read_catalog", "limit": 101},
        {"kind": "read_catalog", "limit": "20"},
        {"kind": "plan", "subtasks": None},
        {"kind": "plan", "completed_subtask_ids": [""]},
        {"kind": "finish", "reason": "evidence_complete"},
        {"kind": "compose", "answer": "Invented answer"},
        {"kind": "stop", "reason": ""},
        {"kind": "stop"},
        {"kind": "read_observation", "observation_id": "o", "max_chars": 4097},
    ],
)
def test_agent_actions_reject_invalid_or_legacy_payloads(value: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(models.AgentAction).validate_python(value, strict=True)


def test_agent_tool_steps_reject_subtask_binding_and_unknown_fields() -> None:
    step = _ACTIONS[2]["steps"][0]
    with pytest.raises(ValidationError):
        models.AgentToolStep.model_validate({**step, "subtask_id": "legacy-subtask"})


def test_agent_request_and_run_config_have_strict_safe_defaults() -> None:
    config = models.RunConfig()
    assert config.max_agent_turns == 32
    assert config.require_verification is False
    fields = request().model_dump()
    fields["remaining_turns"] = 0
    incoming = models.AgentDecisionRequest.model_validate(fields)
    assert incoming.context == {}
    assert incoming.require_verification is False
    assert incoming.remaining_turns == 0
    assert models.RunConfig(max_agent_turns=1, require_verification=True).require_verification


@pytest.mark.parametrize(
    "changes",
    [
        {"max_agent_turns": 0},
        {"max_agent_turns": True},
        {"max_agent_turns": "32"},
        {"require_verification": "false"},
    ],
)
def test_agent_run_config_rejects_coerced_or_invalid_limits(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        models.RunConfig.model_validate(changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"remaining_turns": -1},
        {"remaining_turns": True},
        {"remaining_turns": "8"},
        {"require_verification": "false"},
        {"context": []},
    ],
)
def test_agent_request_rejects_invalid_context_controls(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        decision_request(**changes)


def test_agent_protocol_is_the_only_planner_interface() -> None:
    assert getattr(JsonPlanner, "agent_protocol", None) == "agent-loop-v1"
    assert models.AGENT_PROTOCOL == JsonPlanner.agent_protocol
    assert callable(JsonPlanner.decide)
    assert not hasattr(JsonPlanner, "decompose")
    assert not hasattr(JsonPlanner, "select_tools")
    assert "agent_protocol" in ports.AgentPlannerPort.__annotations__
    assert callable(ports.AgentPlannerPort.decide)
    assert not hasattr(ports, "PlannerPort")


async def test_decide_sends_only_public_context_and_dedicated_prompt() -> None:
    calls: list[httpx.Request] = []

    def respond(incoming: httpx.Request) -> httpx.Response:
        calls.append(incoming)
        body = json.loads(incoming.content)
        assert incoming.url == "https://planner.example/v1/chat/completions"
        assert incoming.headers["Authorization"] == "Bearer private-key"
        assert body["model"] == "example-model"
        assert body["temperature"] == 0
        assert body["seed"] == 7
        assert body["response_format"] == {"type": "json_object"}
        assert body["stream"] is False
        assert body["n"] == 1
        public = json.loads(body["messages"][1]["content"])
        assert public["phase"] == "decide"
        assert set(public["input"]) == {
            "question",
            "catalog_snapshot_id",
            "tool_specs",
            "budget_remaining",
            "context",
            "remaining_turns",
            "require_verification",
        }
        assert public["input"]["context"] == {"answer": None}
        assert public["input"]["require_verification"] is True
        assert public["input"]["remaining_turns"] == 8
        serialized = json.dumps(body)
        for private in ("run-1", "opaque-task", "plan-1", "planner-v1", "private-key"):
            assert private not in serialized
        system = body["messages"][0]["content"]
        assert "Every ToolStep.subtask_id must exactly equal" not in system
        assert "no required action order" in system
        assert "untrusted data" in system
        assert "read_catalog" in system
        return httpx.Response(
            200,
            json=provider_response(
                {"kind": "compose"}, usage={"prompt_tokens": 21, "completion_tokens": 9}
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        planner = JsonPlanner("https://planner.example/v1/", "private-key", client=client)
        result = await planner.decide(
            decision_request(model_params={"temperature": 0, "seed": 7}, require_verification=True)
        )
        await planner.aclose()
        assert not client.is_closed
    assert result.status == "ok"
    assert result.data.kind == "compose"
    assert result.call_id == "plan-1-attempt-1"
    assert result.usage.provider_requests == 1
    assert result.usage.input_tokens == 21
    assert result.usage.output_tokens == 9
    assert len(calls) == 1


async def test_prompt_guides_working_memory_schema_discovery_and_targeted_repair() -> None:
    """确认实际提供方提示携带启发，但仍明确允许模型自由选择动作顺序。"""

    def respond(incoming: httpx.Request) -> httpx.Response:
        prompt = json.loads(incoming.content)["messages"][0]["content"]
        for guidance in (
            "no required action order",
            "without imposing a fixed action order",
            "plan DAG as working memory",
            "update completed_subtask_ids",
            "When structure is unknown",
            "read_observation pages",
            "before choosing filters or aggregations",
            "Do not guess columns or types",
            "use its issues and recommendation",
            "revise the answer with compose",
            "Avoid repeating failed arguments",
        ):
            assert guidance in prompt
        return httpx.Response(200, json=provider_response({"kind": "compose"}))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        reply = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            decision_request()
        )
    assert reply.status == "ok"
    assert reply.usage.provider_requests == 1


@pytest.mark.parametrize("action", _ACTIONS)
async def test_decide_validates_every_action_from_provider_envelope(
    action: dict[str, object],
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=provider_response(action)))
    ) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            decision_request()
        )
    assert result.status == "ok"
    assert result.data.kind == action["kind"]
    assert result.usage.provider_requests == 1


@pytest.mark.parametrize(
    "content",
    [
        '{"kind":"tools","steps":[]}',
        '{"kind":"finish","reason":"evidence_complete"}',
        '{"kind":"compose","kind":"finish"}',
        '{"kind":"read_catalog","limit":"20"}',
        '{"kind":"stop","reason":NaN}',
        '```json\n{"kind":"compose"}\n```',
    ],
)
async def test_decide_returns_safe_output_errors_without_repair(content: str) -> None:
    calls = []

    def respond(incoming: httpx.Request) -> httpx.Response:
        calls.append(incoming)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": content}, "finish_reason": "stop"}]},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            decision_request()
        )
    assert result.error.code == "invalid_model_output"
    assert result.usage.provider_requests == 1
    assert content not in result.model_dump_json()
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"model_params": {"messages": []}}, "invalid_model_params"),
        ({"model_params": {"temperature": True}}, "invalid_model_params"),
        ({"token_budget": {"remaining_input_tokens": 100}}, "token_budget_unsupported"),
    ],
)
async def test_decide_rejects_unsupported_controls_before_dispatch(
    changes: dict[str, object], code: str
) -> None:
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda incoming: calls.append(incoming))
    ) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            decision_request(**changes)
        )
    assert result.error.code == code
    assert result.usage.provider_requests == 0
    assert not calls


async def test_decide_timeout_is_bounded_and_not_retried() -> None:
    calls = []

    async def slow(incoming: httpx.Request) -> httpx.Response:
        calls.append(incoming)
        await asyncio.sleep(1)
        return httpx.Response(200, json=provider_response({"kind": "finish"}))

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        result = await JsonPlanner("https://planner.example/v1", "key", client=client).decide(
            decision_request(timeout_ms=10)
        )
    assert result.error.code == "provider_timeout"
    assert result.usage.provider_requests == 1
    assert len(calls) == 1


async def test_decide_cancellation_propagates() -> None:
    started = asyncio.Event()

    async def wait(_: httpx.Request) -> httpx.Response:
        started.set()
        await asyncio.Future()

    async with httpx.AsyncClient(transport=httpx.MockTransport(wait)) as client:
        planner = JsonPlanner("https://planner.example/v1", "key", client=client)
        running = asyncio.create_task(planner.decide(decision_request()))
        await asyncio.wait_for(started.wait(), timeout=1)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running


async def test_decide_closed_client_is_rejected_locally() -> None:
    planner = JsonPlanner("https://planner.example/v1", "key")
    await planner.aclose()
    result = await planner.decide(decision_request())
    assert result.error.code == "planner_closed"
    assert result.usage.provider_requests == 0


async def test_decide_transport_error_does_not_expose_provider_details() -> None:
    calls = []

    def fail(incoming: httpx.Request) -> httpx.Response:
        calls.append(incoming)
        raise httpx.ConnectError("private-key secret-body", request=incoming)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
        result = await JsonPlanner(
            "https://planner.example/v1", "private-key", client=client
        ).decide(decision_request())
    assert result.error.code == "provider_transport_error"
    assert "private-key" not in result.model_dump_json()
    assert "secret-body" not in result.model_dump_json()
    assert result.usage.provider_requests == 1
    assert len(calls) == 1


def planning_request():
    """构造一个小型真实请求，其中的观测与证据相互区分。"""
    return m.AgentDecisionRequest(
        run_id="private-run",
        task_id="private-task",
        operation_id="private-op",
        call_id="call-1",
        timeout_ms=1000,
        max_output_bytes=16000,
        model_id="model",
        question="Find the matching entries",
        catalog_snapshot_id="snapshot-1",
        remaining_turns=8,
        tool_specs=[],
        budget_remaining=m.BudgetRemaining(
            wall_time_ms=1000, model_calls=4, data_calls=4, plan_steps=4, subtasks=0
        ),
        context={
            "tool_observations": [
                {"observation_id": "obs-1", "result": {"data": {"schema": ["region", "total"]}}}
            ],
            "context_status": {"observations": {"total": 1, "included": 1, "omitted": 0}},
            "observation_read": {"observation_id": "obs-1", "text": "Hong Kong"},
        },
    )


async def test_observations_reach_provider_and_read_decision_is_validated():
    received = []

    def respond(request):
        body = json.loads(request.content)
        received.append(body)
        visible = json.loads(body["messages"][1]["content"])["input"]
        assert visible["context"]["tool_observations"][0]["observation_id"] == "obs-1"
        assert visible["context"]["context_status"]["observations"]["omitted"] == 0
        assert visible["context"]["observation_read"]["text"] == "Hong Kong"
        assert "private-run" not in request.content.decode()
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "kind": "read_observation",
                                    "observation_id": "obs-1",
                                    "pointer": "/data/schema",
                                    "offset": 0,
                                    "max_chars": 200,
                                }
                            )
                        },
                        "finish_reason": "stop",
                    }
                ]
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        planner = JsonPlanner("https://model.example/v1", "secret", client=client)
        result = await planner.decide(planning_request())
    assert result.status == "ok"
    assert result.data.kind == "read_observation"
    assert result.data.pointer == "/data/schema"
    assert len(received) == 1


async def test_full_request_byte_limit_rejects_before_provider_dispatch():
    dispatched = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: dispatched.append(request))
    ) as client:
        planner = JsonPlanner(
            "https://model.example/v1", "secret", client=client, max_request_bytes=512
        )
        result = await planner.decide(planning_request())
    assert result.status == "error"
    assert result.error.code == "planner_input_too_large"
    assert result.usage.provider_requests == 0
    assert not dispatched


async def test_full_request_byte_limit_accepts_exact_utf8_boundary_and_rejects_one_byte_less():
    """Unicode 字符与 JSON 转义后的大小必须符合实际 HTTP 请求体的限制。"""
    dispatched = []

    def respond(request):
        dispatched.append(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {"content": '{"kind":"stop","reason":"insufficient evidence"}'},
                        "finish_reason": "stop",
                    }
                ]
            },
        )

    request = planning_request().model_copy(update={"question": '香港資料池 "rows"\n下一行'})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        unrestricted = JsonPlanner("https://model.example/v1", "secret", client=client)
        assert (await unrestricted.decide(request)).status == "ok"
        encoded = dispatched[0]
        assert len(encoded) > len(encoded.decode("utf-8"))
        exact = JsonPlanner(
            "https://model.example/v1", "secret", client=client, max_request_bytes=len(encoded)
        )
        result = await exact.decide(request)
        assert result.status == "ok"
        assert result.usage.provider_requests == 1
        assert dispatched[1] == encoded

        too_small = JsonPlanner(
            "https://model.example/v1", "secret", client=client, max_request_bytes=len(encoded) - 1
        )
        result = await too_small.decide(request)
        assert result.status == "error"
        assert result.error.code == "planner_input_too_large"
        assert result.usage.provider_requests == 0
        assert len(dispatched) == 2


@pytest.mark.parametrize("limit", [0, -1, True, 2.5])
def test_request_limit_rejects_invalid_values(limit):
    with pytest.raises(ValueError):
        JsonPlanner("https://model.example/v1", "secret", max_request_bytes=limit)


def test_context_limits_and_recovery_flag_are_strict():
    cfg = m.RunConfig(max_planner_context_bytes=1024, planner_preview_bytes=256)
    assert cfg.max_planner_context_bytes == 1024
    with pytest.raises(ValidationError):
        m.RunConfig(max_planner_context_bytes=1024, planner_preview_bytes=600)
    assert m.ErrorInfo(code="invalid_query", message="bad column").recoverable is False
    with pytest.raises(ValidationError):
        m.ErrorInfo(code="invalid_query", message="bad column", recoverable="true")
