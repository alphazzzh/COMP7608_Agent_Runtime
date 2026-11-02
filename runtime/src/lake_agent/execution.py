"""Run isolated tool attempts and select conservative deterministic ready waves.

This module owns no controller state, audit storage, retries or evidence. The
caller reserves budgets and records intent before invoking a worker, then
settles its validated response payload through the controller's normal path.
Only adapter waits overlap; source and shared-resource claims remain explicit.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ValidationError

from . import models as m
from .validation import canonical_json


@dataclass
class ToolOutcome:
    """A raw attempt result; exception messages never cross this boundary."""

    kind: Literal["response", "timeout", "invalid_response", "exception"]
    duration_ms: int
    response: m.ToolResult | None = None
    error_type: str | None = None


@dataclass
class ToolAttempt:
    """Local call identity and observable dispatch/return state for one attempt."""

    request: m.ToolRequest
    deadline: float
    started_at: float | None = None
    outcome: ToolOutcome | None = None

    @property
    def dispatched(self) -> bool:
        """Intent preparation alone never establishes an adapter dispatch."""
        return self.started_at is not None


async def invoke_tool_attempt(
    attempt: ToolAttempt,
    method: Callable[[m.ToolRequest], Awaitable[Any]],
    *,
    max_output_bytes: int,
) -> ToolOutcome:
    """Await one adapter call, propagate cancellation, and validate its reply.

    Cooperative timeouts cannot force an adapter to finish. A reply returned
    after suppressed deadline cancellation is therefore rejected explicitly.
    Every ordinary return stores its outcome; cancellation leaves no invented
    response or failure for a controller to mistake for a known tool outcome.
    """
    loop = asyncio.get_running_loop()
    owner = asyncio.current_task()
    cancellation_requests = owner.cancelling() if owner is not None else 0
    if loop.time() >= attempt.deadline:
        attempt.outcome = ToolOutcome(kind="timeout", duration_ms=0)
        return attempt.outcome

    try:
        async with asyncio.timeout_at(attempt.deadline):
            # Keep this assignment adjacent to the adapter await: audit intent
            # or time spent waiting for a scheduler slot is not a dispatch.
            attempt.started_at = loop.time()
            raw = await method(attempt.request)
        if loop.time() >= attempt.deadline:
            raise TimeoutError
        # Adapter-owned models are not trusted validated instances: callers can
        # bypass Pydantic validation with model_copy(update=...). Rebuild them.
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

    # timeout_at removes its own cancellation request on exit. A remaining new
    # request belongs to the caller, even if an adapter swallowed CancelledError
    # and returned (or raised a different error). Never turn that into success.
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
    """Select a stable, source/resource-disjoint wave from a validated plan.

    Dependencies must have succeeded before this wave; selecting a predecessor
    does not release its child. Unsafe or source-less entries are global serial
    barriers, including when unready. Conflicts end selection so later entries
    cannot overtake the first conflicting ready action.
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
