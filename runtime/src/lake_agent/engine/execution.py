"""执行隔离的工具调用尝试，并保守、确定地选择就绪批次。

本模块不持有控制器状态、审计存储、重试策略或证据。调用方在执行工作任务前预留预算
并记录意图，再通过控制器的正常流程结算已验证的响应载荷。
仅适配器等待可以重叠，来源和共享资源占用须显式声明。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ValidationError

from .. import models as m
from ..validation import canonical_json


@dataclass
class ToolOutcome:
    """单次尝试的原始结果；异常消息不跨越此边界传递。"""

    kind: Literal["response", "timeout", "invalid_response", "exception"]
    duration_ms: int
    response: m.ToolResult | None = None
    error_type: str | None = None


@dataclass
class ToolAttempt:
    """一次尝试的本地调用标识，以及可观察的派发和返回状态。"""

    request: m.ToolRequest
    deadline: float
    started_at: float | None = None
    outcome: ToolOutcome | None = None

    @property
    def dispatched(self) -> bool:
        """仅准备调用意图，并不能证明已向适配器派发调用。"""
        return self.started_at is not None


async def invoke_tool_attempt(
    attempt: ToolAttempt,
    method: Callable[[m.ToolRequest], Awaitable[Any]],
    *,
    max_output_bytes: int,
) -> ToolOutcome:
    """等待一次适配器调用，传播取消，并验证响应。

    协作式超时无法强制适配器结束，因此必须显式拒绝适配器吞掉截止时间取消后才返回的
    响应。每次普通返回都会保存结果；取消时不虚构响应或失败，避免控制器误认为
    工具结果已经确定。
    """
    loop = asyncio.get_running_loop()
    owner = asyncio.current_task()
    cancellation_requests = owner.cancelling() if owner is not None else 0
    if loop.time() >= attempt.deadline:
        attempt.outcome = ToolOutcome(kind="timeout", duration_ms=0)
        return attempt.outcome

    try:
        async with asyncio.timeout_at(attempt.deadline):
            # 此赋值须紧邻适配器 await：审计意图或等待调度名额所耗时间
            # 不等于实际派发。
            attempt.started_at = loop.time()
            raw = await method(attempt.request)
        if loop.time() >= attempt.deadline:
            raise TimeoutError
        # 适配器持有的模型不能直接视为可信的已验证实例：调用方可通过
        # model_copy(update=...) 绕过 Pydantic 验证，因此须重建模型。
        raw = raw.model_dump(mode="json") if isinstance(raw, BaseModel) else raw
        if len(canonical_json(raw)) > max_output_bytes:
            raise ValueError("response exceeds max_output_bytes")
        response = m.ToolResult.model_validate(raw)
        if response.call_id != attempt.request.call_id:
            raise ValueError("response call_id mismatch")
    except asyncio.CancelledError:
        raise
    except TimeoutError as exc:
        outcome = ToolOutcome(kind="timeout", duration_ms=0, error_type=type(exc).__name__)
    except (ValidationError, ValueError, TypeError) as exc:
        outcome = ToolOutcome(kind="invalid_response", duration_ms=0, error_type=type(exc).__name__)
    except Exception as exc:
        outcome = ToolOutcome(kind="exception", duration_ms=0, error_type=type(exc).__name__)
    else:
        outcome = ToolOutcome(kind="response", duration_ms=0, response=response)

    # timeout_at 退出时会移除自身的取消请求。剩余新增请求属于
    # 调用方，即使适配器吞掉 CancelledError 后返回响应或抛出其他
    # 错误也如此；绝不能将这种情况当作成功。
    if owner is not None and owner.cancelling() > cancellation_requests:
        raise asyncio.CancelledError
    assert attempt.started_at is not None
    outcome.duration_ms = max(0, int((loop.time() - attempt.started_at) * 1000))
    attempt.outcome = outcome
    return outcome


def select_ready_wave(
    pending: Sequence[m.ToolStep],
    succeeded: set[str],
    specs: Mapping[str, m.ToolSpec],
    limit: int,
) -> list[m.ToolStep]:
    """从已验证的计划中，按稳定顺序选择来源和资源互不冲突的批次。

    依赖步骤必须在本批次之前成功；选中前驱并不会使后继立即就绪。不支持安全并行或
    没有来源的条目是全局串行屏障，即使尚未就绪也如此。遇到冲突即停止选择，
    防止后续条目越过首个发生冲突的就绪操作。
    """
    if limit <= 0:
        return []
    selected: list[m.ToolStep] = []
    selected_sources: set[str] = set()
    selected_resources: set[str] = set()
    for step in pending:
        spec = specs[step.tool_name]
        ready = set(step.depends_on) <= succeeded
        if not spec.parallel_safe or not step.source_ids:
            if not selected and ready:
                return [step]
            break
        if not ready:
            continue
        sources = set(step.source_ids)
        resources = set(spec.exclusive_resource_keys)
        if sources & selected_sources or resources & selected_resources:
            break
        selected.append(step)
        selected_sources.update(sources)
        selected_resources.update(resources)
        if len(selected) >= limit:
            break
    return selected
