"""Provider boundary regressions for observation-aware planning and input limits.

The tests use the real JsonPlanner with an HTTP transport stub. They check the
bytes sent to the provider, local rejection before dispatch, and new decisions;
no network credentials or external service are involved.
"""

import json

import httpx
import pytest
from pydantic import ValidationError

from lake_agent import models as m
from lake_agent.planner import JsonPlanner


def planning_request():
    """A small realistic request containing observations distinct from evidence."""
    return m.ToolPlanningRequest(
        run_id="private-run",
        task_id="private-task",
        operation_id="private-op",
        call_id="call-1",
        timeout_ms=1000,
        max_output_bytes=16000,
        model_id="model",
        question="Find the matching entries",
        subtask=m.Subtask(subtask_id="s", objective="Inspect", search_query="entries"),
        candidates=[],
        tool_specs=[],
        budget_remaining=m.BudgetRemaining(
            wall_time_ms=1000, model_calls=4, data_calls=4, plan_steps=4, subtasks=0
        ),
        tool_observations=[
            {"observation_id": "obs-1", "result": {"data": {"schema": ["region", "total"]}}}
        ],
        context_status={"observations": {"total": 1, "included": 1, "omitted": 0}},
        observation_read={"observation_id": "obs-1", "text": "Hong Kong"},
    )


async def test_observations_reach_provider_and_read_decision_is_validated():
    received = []

    def respond(request):
        body = json.loads(request.content)
        received.append(body)
        visible = json.loads(body["messages"][1]["content"])["input"]
        assert visible["tool_observations"][0]["observation_id"] == "obs-1"
        assert visible["context_status"]["observations"]["omitted"] == 0
        assert visible["observation_read"]["text"] == "Hong Kong"
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
        result = await planner.select_tools(planning_request())
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
        result = await planner.select_tools(planning_request())
    assert result.status == "error"
    assert result.error.code == "planner_input_too_large"
    assert result.usage.provider_requests == 0
    assert not dispatched


async def test_full_request_byte_limit_accepts_exact_utf8_boundary_and_rejects_one_byte_less():
    """Unicode and JSON escaping must fit the actual body sent over HTTP."""
    dispatched = []

    def respond(request):
        dispatched.append(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": '{"kind":"finish","reason":"insufficient_evidence"}'
                        },
                        "finish_reason": "stop",
                    }
                ]
            },
        )

    request = planning_request().model_copy(update={"question": '香港資料池 "rows"\n下一行'})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        unrestricted = JsonPlanner("https://model.example/v1", "secret", client=client)
        assert (await unrestricted.select_tools(request)).status == "ok"
        encoded = dispatched[0]
        assert len(encoded) > len(encoded.decode("utf-8"))
        exact = JsonPlanner(
            "https://model.example/v1", "secret", client=client, max_request_bytes=len(encoded)
        )
        result = await exact.select_tools(request)
        assert result.status == "ok"
        assert result.usage.provider_requests == 1
        assert dispatched[1] == encoded

        too_small = JsonPlanner(
            "https://model.example/v1", "secret", client=client, max_request_bytes=len(encoded) - 1
        )
        result = await too_small.select_tools(request)
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
