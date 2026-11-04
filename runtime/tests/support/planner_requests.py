"""规划器请求和 HTTP 响应构造器，不访问外部提供方。

只使用显式测试端口、临时目录和内存传输；测试场景不代表真实模型质量。
"""

from __future__ import annotations

import json

from lake_agent.models import AgentDecisionRequest


def request(**changes: object) -> AgentDecisionRequest:
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
        "remaining_turns": 8,
        "budget_remaining": {
            "wall_time_ms": 5000,
            "model_calls": 5,
            "data_calls": 5,
            "plan_steps": 5,
            "subtasks": 5,
        },
    }
    fields.update(changes)
    return AgentDecisionRequest.model_validate(fields)


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
