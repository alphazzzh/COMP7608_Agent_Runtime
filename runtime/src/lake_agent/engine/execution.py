"""执行隔离的工具调用尝试，并保守、确定地选择就绪批次。

本模块不持有控制器状态、审计存储、重试策略或证据。调用方在执行工作任务前预留预算
并记录意图，再通过控制器的正常流程结算已验证的响应载荷。
仅适配器等待可以重叠，来源和共享资源占用须显式声明。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ValidationError

from .. import models as m
from ..validation import canonical_json
from .errors import RunStopped

if TYPE_CHECKING:
    from .recovery import RecoverySession


async def await_adapter_response(
    method, request, response_type, *, deadline, max_output_bytes, on_dispatch=None
):
    """仅给适配器等待设限；复制请求并按原身份验证回复，写入由调用方处理。"""
    loop = asyncio.get_running_loop()
    adapter_request = request.model_copy(deep=True)
    if loop.time() >= deadline:
        raise TimeoutError
    owner = asyncio.current_task()
    cancellation_requests = owner.cancelling() if owner is not None else 0
    try:
        async with asyncio.timeout_at(deadline):
            if on_dispatch is not None:
                on_dispatch()
            raw = await method(adapter_request)
    finally:
        # timeout_at 会撤回自己的取消。适配器即使吞掉调用方取消，也不能继续结算。
        if owner is not None and owner.cancelling() > cancellation_requests:
            raise asyncio.CancelledError
    if loop.time() >= deadline:
        raise TimeoutError  # 拒绝适配器吞掉期限取消后返回的迟到响应。
    # model_copy(update=...) 可绕过 Pydantic；始终从 JSON 重新验证适配器回复。
    raw = raw.model_dump(mode="json") if isinstance(raw, BaseModel) else raw
    if len(canonical_json(raw)) > max_output_bytes:
        raise ValueError("response exceeds max_output_bytes")
    response = response_type.model_validate(raw)
    if response.call_id != request.call_id:
        raise ValueError("response call_id mismatch")
    return response


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
    recovery: RecoverySession | None = None,
) -> ToolOutcome:
    """只标记实际派发和已验证的结果；已知响应的持久化不受单次调用期限截断。"""
    loop = asyncio.get_running_loop()
    replayable = recovery is not None and recovery.has_cached_response(attempt.request.call_id)
    if loop.time() >= attempt.deadline and not replayable:
        attempt.outcome = ToolOutcome(kind="timeout", duration_ms=0)
        return attempt.outcome

    def dispatched():
        attempt.started_at = loop.time()

    def elapsed_ms():
        return (
            max(0, int((loop.time() - attempt.started_at) * 1000))
            if attempt.started_at is not None
            else 0
        )

    def known(response):
        # 先标记已知结果；取消若发生于 SQLite 结算期间，审计仍能区分已返回和未知。
        attempt.outcome = ToolOutcome(kind="response", duration_ms=elapsed_ms(), response=response)

    try:
        if recovery is None:
            known(
                await await_adapter_response(
                    method,
                    attempt.request,
                    m.ToolResult,
                    deadline=attempt.deadline,
                    max_output_bytes=max_output_bytes,
                    on_dispatch=dispatched,
                )
            )
        else:
            await recovery.invoke(
                "tool",
                method,
                attempt.request,
                m.ToolResult,
                deadline=attempt.deadline,
                on_dispatch=dispatched,
                on_response=known,
            )
    except (asyncio.CancelledError, RunStopped):
        raise
    except TimeoutError as exc:
        outcome = ToolOutcome(kind="timeout", duration_ms=0, error_type=type(exc).__name__)
    except (ValidationError, ValueError, TypeError) as exc:
        outcome = ToolOutcome(kind="invalid_response", duration_ms=0, error_type=type(exc).__name__)
    except Exception as exc:
        outcome = ToolOutcome(kind="exception", duration_ms=0, error_type=type(exc).__name__)
    else:
        assert attempt.outcome is not None
        return attempt.outcome

    outcome.duration_ms = elapsed_ms()
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
