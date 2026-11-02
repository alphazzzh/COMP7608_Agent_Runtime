"""Async coordination with bounded tool waves, run-local budgets and provenance.

Adapters perform all parsing, retrieval and generation. This module authorizes their
requests, validates their replies, and serially commits state transitions. Only
explicitly safe data-tool waits overlap; model/content calls remain sequential.
It never runs model
generated code, reads evaluation labels, or interprets source locators.
"""

from __future__ import annotations

import asyncio
import math
import random
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from . import models as m
from .context import build_planner_context, read_json_page
from .errors import FATAL_TOOL_CODES as _FATAL_TOOL_CODES
from .errors import RunStopped as _Stop
from .ports import ContentPort, DataPort, PlannerPort, TraceSink
from .tool_runtime import ToolExecutionMixin
from .validation import (
    canonical_json,
    catalog_digest,
    source_ids_for,
    validate_dag,
    validate_tool_specs,
)

if TYPE_CHECKING:
    from .state import StateStore


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _utc() -> str:
    return datetime.now(UTC).isoformat()


class _Controller(ToolExecutionMixin):
    """One instance per run, including when callers share adapter instances."""

    def __init__(self, request, data, content, planner, trace, *, recovery=None):
        self.req, self.data, self.content, self.planner, self.trace = (
            request,
            data,
            content,
            planner,
            trace,
        )
        self.recovery = recovery
        self.cfg = request.config
        self.run_id = request.run_id or self._id("run")
        self.loop = asyncio.get_running_loop()
        self.start = self.loop.time()
        self.deadline = self.start + self.cfg.max_wall_time_ms / 1000
        self.started_at = _utc()
        self.seq = 0
        self.counts = m.CallCounts()
        self.model_calls_reserved = 0
        self.pending_call: m.JsonObject | None = None
        self.active_call: m.JsonObject | None = None
        self.usage = {"input_tokens": 0, "output_tokens": 0, "cost": None, "currency": None}
        self.usage_unknown = False
        self.trace_open = False
        self.trace_broken = False
        self.answer = None
        self.sources = {}
        self.evidence = []
        self.visited = set()
        self.step_ids = set()
        self.subtasks = []
        self.completed_subtasks = []
        self.snapshot = None
        self.handle = None
        self.specs = {}
        self.observations: list[dict] = []
        self.observations_by_id: dict[str, dict] = {}
        self.accepted_steps: dict[str, m.ToolStep] = {}
        self.action_counts: dict[str, int] = {}
        self._init_tool_execution()
        if self.recovery is not None:
            self.recovery.attach(self)

    def _id(self, prefix):
        """Use a per-run stable stream only when durable execution is enabled."""
        return self.recovery.identifier(prefix) if self.recovery is not None else _id(prefix)

    async def _invoke_port(self, phase, method, request, response_type):
        """Keep persistence admission outside adapter-owned implementations."""
        if self.recovery is not None:
            return await self.recovery.invoke(phase, method, request, response_type)
        return await method(request)

    async def _invoke_tool_port(self, request):
        """The isolated worker saves each known reply before sibling settlement."""
        return await self._invoke_port("tool", self.data.invoke, request, m.ToolResult)

    async def _observe(self, request, response, artifact_ref):
        """Index a valid tool reply after its full audit artifact is committed.

        Observations include failed attempts but never acquire evidence IDs.
        Source authorization comes from the accepted plan, not tool prose.
        Only this run's index is exposed to the local JSON page reader.
        """
        step = self.accepted_steps[request.step_id]
        observation = dict(
            observation_id=self._id("obs"),
            call_id=request.call_id,
            operation_id=request.operation_id,
            attempt=request.attempt,
            step_id=step.step_id,
            subtask_id=step.subtask_id,
            tool_name=step.tool_name,
            source_ids=list(step.source_ids),
            args=step.args,
            status=response.status,
            artifact_ref=artifact_ref,
            result=response.model_dump(mode="json"),
        )
        await self._event(
            "tool.observed",
            step_id=step.step_id,
            call_id=request.call_id,
            data_ref=artifact_ref,
            detail={"observation_id": observation["observation_id"]},
        )
        self.observations.append(observation)
        self.observations_by_id[observation["observation_id"]] = observation

    async def _read_observation(self, decision):
        """Read only a known result; never interpret IDs as filesystem paths."""
        self._time()
        if self.counts.observation_reads >= self.cfg.max_observation_reads:
            raise _Stop("budget_exceeded", "observation_read_budget")
        observation = self.observations_by_id.get(decision.observation_id)
        if observation is None:
            raise _Stop("failed", "invalid_observation_read")
        # Reserve space for the stable observation ID in addition to the page
        # envelope. The context builder keeps requested pages whole.
        overhead = len(canonical_json({"observation_id": decision.observation_id})) + 1
        history_overhead = (
            len(
                canonical_json(
                    {"previous_pages": [], "omitted_pages": self.cfg.max_observation_reads}
                )
            )
            + 1
        )
        page_budget = min(
            self.cfg.planner_preview_bytes,
            self.cfg.max_planner_context_bytes // 2 - history_overhead,
        )
        try:
            page = read_json_page(
                observation["result"],
                pointer=decision.pointer,
                offset=decision.offset,
                max_chars=decision.max_chars,
                max_bytes=page_budget - overhead,
            )
        except ValueError as exc:
            raise _Stop("failed", "invalid_observation_read") from exc
        page["observation_id"] = decision.observation_id
        self.counts.observation_reads += 1
        ref = await self._artifact(page)
        await self._event(
            "observation.read", data_ref=ref, detail={"observation_id": decision.observation_id}
        )
        return page

    def _time(self):
        if self.loop.time() >= self.deadline:
            raise _Stop("timeout", "time_budget")

    def _remaining(self):
        return m.BudgetRemaining(
            wall_time_ms=max(0, int((self.deadline - self.loop.time()) * 1000)),
            model_calls=max(0, self.cfg.max_model_calls - self.model_calls_reserved),
            data_calls=max(0, self.cfg.max_data_calls - self.counts.catalog - self.counts.tools),
            plan_steps=max(0, self.cfg.max_plan_steps - self.counts.steps),
            subtasks=max(0, self.cfg.max_subtasks - len(self.subtasks)),
        )

    async def _write(self, factory: Callable, *, terminal=False):
        """Normal writes share run deadline; terminal settlement gets bounded grace."""
        deadline = self.deadline
        if terminal:
            deadline = self.loop.time() + self.cfg.cancel_cleanup_timeout_ms / 1000
        try:
            async with asyncio.timeout_at(deadline):
                return await factory()
        except Exception as exc:
            self.trace_broken = True
            raise _Stop("failed", "trace_write_failed") from exc

    async def _artifact(self, value):
        ref = self._id("artifact")
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        await self._write(lambda: self.trace.put_artifact(ref, value))
        return ref

    async def _event(self, kind, *, terminal=False, **fields):
        self.seq += 1
        if self.recovery is not None:
            fields = self.recovery.annotate(kind, fields)
        event = m.TraceEvent(
            run_id=self.run_id,
            task_id=self.req.task_id,
            seq=self.seq,
            event_type=kind,
            timestamp_utc=_utc(),
            **fields,
        )
        await self._write(lambda: self.trace.append(event), terminal=terminal)
        if self.recovery is not None:
            await self.recovery.progress()

    def _add_usage(self, usage):
        for name in ("input_tokens", "output_tokens"):
            old, new = self.usage[name], getattr(usage, name)
            self.usage[name] = None if old is None or new is None else old + new
        if usage.cost is not None:
            if usage.provider_requests and usage.currency is None:
                self.usage["cost"] = None
                self.usage_unknown = True
            elif self.usage["currency"] not in (None, usage.currency):
                self.usage["cost"] = None
                self.usage_unknown = True
            elif not self.usage_unknown:
                self.usage["cost"] = (self.usage["cost"] or 0) + usage.cost
                self.usage["currency"] = usage.currency
        elif usage.provider_requests:
            self.usage_unknown = True
            self.usage["cost"] = None

    async def _call(
        self,
        phase,
        method,
        request_type,
        fields,
        response_type,
        *,
        provider=False,
        data_kind=None,
        idempotent=False,
        step_id=None,
    ):
        """Persist intent before dispatch; only idempotent data operations retry.

        A model slot is reserved before await. A valid reply explicitly reports
        provider_requests=0/1; timeout or broken envelopes retain the reservation
        because actual network dispatch is then unobservable to the controller.
        """
        operation_id = self._id("op")
        for attempt in range(1, self.cfg.max_retries_per_data_operation + 2):
            self._time()
            if data_kind and self.counts.catalog + self.counts.tools >= self.cfg.max_data_calls:
                raise _Stop("budget_exceeded", "data_call_budget")
            if provider and self.model_calls_reserved >= self.cfg.max_model_calls:
                raise _Stop("budget_exceeded", "model_call_budget")
            if step_id and attempt == 1 and self.counts.steps >= self.cfg.max_plan_steps:
                raise _Stop("budget_exceeded", "step_budget")
            call_deadline = min(
                self.deadline, self.loop.time() + self.cfg.per_call_timeout_ms / 1000
            )
            context = dict(
                run_id=self.run_id,
                task_id=self.req.task_id,
                operation_id=operation_id,
                call_id=self._id("call"),
                attempt=attempt,
                timeout_ms=max(1, math.ceil((call_deadline - self.loop.time()) * 1000)),
                max_output_bytes=self.cfg.max_output_bytes_per_call,
            )
            if provider:
                context.update(
                    model_id=self.cfg.model_id,
                    model_params=self.cfg.model_params,
                    prompt_version=getattr(
                        self.cfg.prompt_versions,
                        "planner" if phase.startswith("planner") else "content",
                    ),
                    token_budget=m.TokenBudget(),
                )
            # Pydantic preserves already-instantiated nested models. Detach
            # every port request so planner/content adapters cannot mutate the
            # controller's trusted capabilities, sources or original evidence.
            req = request_type(**context, **fields).model_copy(deep=True)
            if self.recovery is not None:
                req = await self.recovery.prepare_serial(phase, req)
                # Rebinding the fresh ContentPort can consume time. New calls
                # receive only the original run's remaining dispatch window.
                call_deadline = min(
                    self.deadline, self.loop.time() + self.cfg.per_call_timeout_ms / 1000
                )
                req = req.model_copy(
                    update={
                        "timeout_ms": max(1, math.ceil((call_deadline - self.loop.time()) * 1000))
                    }
                )
            request_ref = await self._artifact(req)
            meta = dict(
                operation_id=operation_id, call_id=req.call_id, attempt=attempt, step_id=step_id
            )
            # Reserve budgets before awaiting the start-event commit. If caller
            # cancellation races a thread-backed append, the commit outcome is
            # uncertain; retain physical/model reservations and expose the ID
            # in run.cancelled. Normal write failures roll back because no port
            # call can follow them.
            if data_kind:
                setattr(self.counts, data_kind, getattr(self.counts, data_kind) + 1)
                if step_id and attempt == 1:
                    self.counts.steps += 1
            if provider:
                self.model_calls_reserved += 1
            self.pending_call = {"phase": phase, **meta}
            try:
                await self._event(f"{phase}.call.started", data_ref=request_ref, **meta)
            except asyncio.CancelledError:
                if step_id and attempt == 1:
                    self.counts.steps -= 1
                raise
            except _Stop:
                if data_kind:
                    setattr(self.counts, data_kind, getattr(self.counts, data_kind) - 1)
                    if step_id and attempt == 1:
                        self.counts.steps -= 1
                if provider:
                    self.model_calls_reserved -= 1
                self.pending_call = None
                raise
            self.pending_call = None
            # A slow trace write can consume the dispatch window. The persisted
            # call.started attempt remains budgeted, but must not reach the port.
            if self.loop.time() >= call_deadline:
                if step_id and attempt == 1:
                    # The logical invoke did not start even though its physical
                    # attempt was reserved after call.started was committed.
                    self.counts.steps -= 1
                if self.loop.time() >= self.deadline:
                    await self._event(
                        f"{phase}.call.failed",
                        status="timeout",
                        terminal=True,
                        duration_ms=0,
                        error=m.ErrorInfo(
                            code="time_budget",
                            message="Run deadline expired before dispatch.",
                            retryable=True,
                        ),
                        detail={"code": "time_budget", "outcome": "not_dispatched"},
                        **meta,
                    )
                    raise _Stop("timeout", "time_budget")
                await self._event(
                    f"{phase}.call.failed",
                    status="timeout",
                    duration_ms=0,
                    error=m.ErrorInfo(
                        code="timeout",
                        message="Call deadline expired before dispatch.",
                        retryable=True,
                    ),
                    detail={"code": "timeout", "outcome": "not_dispatched"},
                    **meta,
                )
                raise _Stop("timeout", "call_timeout")
            begun = self.loop.time()
            error = None
            response = None
            provider_usage_settled = False
            cancelled = False
            try:
                if not data_kind:
                    count_field = "planner" if phase.startswith("planner") else "content"
                    setattr(self.counts, count_field, getattr(self.counts, count_field) + 1)
                if data_kind and attempt > 1:
                    self.counts.retries += 1
                # A committed start is distinct from a call currently awaiting
                # its adapter or result persistence. Preserve the latter when
                # caller cancellation makes its final outcome unknowable.
                self.active_call = {"phase": phase, **meta, "outcome": "unknown"}
                owner = asyncio.current_task()
                cancelled_before = owner.cancelling() if owner is not None else 0
                try:
                    async with asyncio.timeout_at(call_deadline):
                        raw = await self._invoke_port(phase, method, req, response_type)
                finally:
                    # timeout_at removes its own cancellation request on exit.
                    # A new pending request belongs to the caller, including
                    # when the adapter suppresses cancel then returns or raises.
                    if owner is not None and owner.cancelling() > cancelled_before:
                        raise asyncio.CancelledError
                # timeout_at is cooperative: an adapter may suppress its
                # injected cancellation, but a late response cannot be accepted.
                if self.loop.time() >= call_deadline:
                    raise TimeoutError
                # Revalidate even a model instance: model_copy(update=...) bypasses Pydantic.
                raw = raw.model_dump(mode="json") if isinstance(raw, BaseModel) else raw
                if len(canonical_json(raw)) > self.cfg.max_output_bytes_per_call:
                    raise ValueError("response exceeds max_output_bytes")
                response = response_type.model_validate(raw)
                if response.call_id != req.call_id:
                    raise ValueError("response call_id mismatch")
                self.active_call["outcome"] = "returned"
                if hasattr(response, "usage"):
                    if provider:
                        self.counts.model_calls += response.usage.provider_requests
                        # A valid zero-request report proves that no provider
                        # dispatch consumed this slot. Keep completed actual
                        # calls reserved, but release unused reservations.
                        self.model_calls_reserved -= 1 - response.usage.provider_requests
                        provider_usage_settled = True
                    self._add_usage(response.usage)
                    if not provider and response.usage.provider_requests != 0:
                        # Preserve a declared actual dispatch in accounting even
                        # while failing the local-port contract violation.
                        self.counts.model_calls += response.usage.provider_requests
                        raise ValueError("local method must not make provider requests")
                ref = await self._artifact(response)
                await self._event(
                    f"{phase}.call.{'completed' if response.status == 'ok' else 'failed'}",
                    status=response.status,
                    data_ref=ref,
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=response.error if response.status == "error" else None,
                    usage=getattr(response, "usage", None),
                    **meta,
                )
                if isinstance(response, m.ToolResult):
                    await self._observe(req, response, ref)
                if response.status == "ok":
                    return response
                error = response.error
            except asyncio.CancelledError:
                cancelled = True
                if provider and not provider_usage_settled:
                    # No valid usage report reached the controller. Charge the
                    # reserved attempt once, matching timeout/error accounting;
                    # a returned local response must remain a known zero.
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                raise
            except _Stop:
                raise
            except TimeoutError as exc:
                if provider:
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                error = m.ErrorInfo(
                    code="timeout", message="Cooperative call deadline exceeded", retryable=True
                )
                time_budget = self.loop.time() >= self.deadline
                await self._event(
                    f"{phase}.call.failed",
                    status="timeout",
                    terminal=time_budget,
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=error,
                    detail={
                        "code": "time_budget" if time_budget else "timeout",
                        "outcome": "unknown",
                    },
                    **meta,
                )
                if time_budget:
                    raise _Stop("timeout", "time_budget") from exc
            except (ValidationError, ValueError, TypeError) as exc:
                if provider:
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                await self._event(
                    f"{phase}.call.failed",
                    status="invalid_response",
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=m.ErrorInfo(
                        code="invalid_response",
                        message="Adapter returned an invalid response.",
                        retryable=False,
                    ),
                    detail={"error_type": type(exc).__name__},
                    **meta,
                )
                raise _Stop("failed", "invalid_module_response") from exc
            except Exception as exc:
                if provider:
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                await self._event(
                    f"{phase}.call.failed",
                    status="error",
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=m.ErrorInfo(
                        code="adapter_error",
                        message="Adapter call failed.",
                        retryable=False,
                    ),
                    detail={"error_type": type(exc).__name__},
                    **meta,
                )
                # Exceptions have no reliable retryable contract. Never blindly replay tools.
                raise _Stop("failed", "tool_error" if data_kind else "module_error") from exc
            finally:
                if not cancelled:
                    self.active_call = None
            if not (
                data_kind
                and idempotent
                and error.retryable
                and attempt <= self.cfg.max_retries_per_data_operation
            ):
                if (
                    isinstance(response, m.ToolResult)
                    and error.recoverable
                    and error.code not in _FATAL_TOOL_CODES
                    and self.req.mode != "fixed_retrieval"
                ):
                    # Replanning is a new, budgeted decision. It is not a
                    # transport retry and does not replay unknown side effects.
                    return response
                if error.code == "timeout":
                    raise _Stop("timeout", "call_timeout")
                if error.code == "token_budget_exceeded":
                    raise _Stop("budget_exceeded", "token_budget")
                reason = (
                    "retry_exhausted"
                    if data_kind and idempotent and error.retryable
                    else ("tool_error" if data_kind else "module_error")
                )
                raise _Stop("failed", reason)
            # Capped full jitter. No task is spawned; cancellation interrupts sleep directly.
            delay = random.uniform(0, min(0.05 * 2 ** min(attempt - 1, 6), 1.0))
            await self._event(
                "data.retry", detail={"delay_ms": delay * 1000, "error_code": error.code}, **meta
            )
            async with asyncio.timeout_at(self.deadline):
                try:
                    if self.recovery is None or not self.recovery.rebuilding:
                        await asyncio.sleep(delay)
                except TimeoutError as exc:
                    raise _Stop("timeout", "time_budget") from exc
        raise AssertionError("retry loop did not settle")

    async def _catalog(self):
        cursor, cursors = None, set()
        snapshot = self.cfg.dataset_snapshot
        while True:
            result = await self._call(
                "catalog",
                self.data.catalog,
                m.CatalogRequest,
                dict(snapshot_id=snapshot, cursor=cursor, page_size=self.cfg.catalog_page_size),
                m.CatalogResult,
                data_kind="catalog",
                idempotent=True,
            )
            if snapshot is not None and result.catalog_snapshot_id != snapshot:
                raise _Stop("failed", "catalog_snapshot_mismatch")
            snapshot = result.catalog_snapshot_id
            if not snapshot or len(result.sources) > self.cfg.catalog_page_size:
                raise _Stop("failed", "invalid_module_response")
            for source in result.sources:
                if source.source_id in self.sources:
                    raise _Stop("failed", "invalid_module_response")
                self.sources[source.source_id] = source
            if (
                len(self.sources) > self.cfg.max_catalog_sources
                or len(canonical_json(list(self.sources.values()))) > self.cfg.max_catalog_bytes
            ):
                raise _Stop("budget_exceeded", "catalog_limit")
            if not result.has_more:
                break
            if not result.next_cursor or result.next_cursor in cursors:
                raise _Stop("failed", "invalid_module_response")
            cursors.add(result.next_cursor)
            cursor = result.next_cursor
        self.snapshot = snapshot
        sources = sorted(self.sources.values(), key=lambda source: source.source_id)
        digest = catalog_digest(sources)
        reply = await self._call(
            "content.prepare_catalog",
            self.content.prepare_catalog,
            m.CatalogRegistrationRequest,
            dict(catalog_snapshot_id=snapshot, catalog_digest=digest, sources=sources),
            m.ModuleResult[m.CatalogHandle],
        )
        if reply.data.catalog_snapshot_id != snapshot or reply.data.catalog_digest != digest:
            raise _Stop("failed", "catalog_snapshot_mismatch")
        self.handle = reply.data.handle

    async def _search(self, subtask, focus=None):
        query = focus.query if focus else subtask.search_query
        subtask = subtask.model_copy(update={"search_query": query})
        source_filter = focus.source_ids if focus and focus.source_ids else None
        reply = await self._call(
            "content.search",
            self.content.search,
            m.SearchRequest,
            dict(
                question=self.req.question,
                subtask=subtask,
                catalog_snapshot_id=self.snapshot,
                catalog_handle=self.handle,
                visited_source_ids=sorted(self.visited),
                source_id_filter=source_filter,
                limit=self.cfg.candidate_top_k,
            ),
            m.ModuleResult[m.SearchResponse],
        )
        candidates = reply.data.candidates
        ids = [c.source_id for c in candidates]
        if (
            len(ids) > self.cfg.candidate_top_k
            or len(set(ids)) != len(ids)
            or not set(ids) <= self.sources.keys()
            or (source_filter is not None and not set(ids) <= set(source_filter))
        ):
            raise _Stop("failed", "invalid_module_response")
        return sorted(candidates, key=lambda item: (-item.score, item.source_id))

    async def _execute_plan(self, plan, subtask, candidates):
        if len(plan.steps) > self.cfg.max_plan_steps - self.counts.steps:
            raise _Stop("budget_exceeded", "step_budget")
        allowed = {c.source_id for c in candidates} | {e.source_id for e in self.evidence}
        # Inspect may establish a useful schema without returning evidence.
        # Search adapters can omit visited sources, so keep accepted observation
        # sources authorized independently of the next search's candidate list.
        allowed.update(source for item in self.observations for source in item["source_ids"])
        try:
            steps = validate_dag(plan.steps, "step_id")
            ids = {step.step_id for step in steps}
            if ids & self.step_ids:
                raise ValueError("step IDs reused across plans")
            for step in steps:
                if step.subtask_id != subtask.subtask_id or step.tool_name not in self.specs:
                    raise ValueError("unknown tool or wrong subtask")
                sources = source_ids_for(self.specs[step.tool_name], step.args)
                if sources != set(step.source_ids) or not sources <= allowed:
                    raise ValueError("source authorization failed")
                if len(step.source_ids) != len(set(step.source_ids)):
                    raise ValueError("duplicate source IDs")
        except (ValueError, ValidationError) as exc:
            raise _Stop("failed", "invalid_plan") from exc
        # Validate the entire plan before committing any ID or invoking any step.
        self.step_ids.update(ids)
        self.accepted_steps.update({step.step_id: step for step in steps})
        ref = await self._artifact(plan)
        await self._event("plan.accepted", data_ref=ref)
        await self._dispatch_tool_plan(steps)

    async def _subtask(self, subtask, focus=None):
        await self._event(
            "subtask.state", status="running", detail={"subtask_id": subtask.subtask_id}
        )
        read_result = None
        read_pages = []
        candidates = await self._search(subtask, focus)
        while True:
            self._time()
            context = build_planner_context(
                self.observations,
                self.evidence,
                subtask_id=subtask.subtask_id,
                max_bytes=self.cfg.max_planner_context_bytes,
                preview_bytes=self.cfg.planner_preview_bytes,
                read_result=read_result,
            )
            context["context_status"]["remaining_observation_reads"] = max(
                0, self.cfg.max_observation_reads - self.counts.observation_reads
            )
            # The four-field byte cap includes metadata. Keep the optional
            # remaining-read count out if it would overflow the exact budget.
            if len(canonical_json(context)) > self.cfg.max_planner_context_bytes:
                del context["context_status"]["remaining_observation_reads"]
            result = await self._call(
                "planner.select_tools",
                self.planner.select_tools,
                m.ToolPlanningRequest,
                dict(
                    question=self.req.question,
                    subtask=subtask,
                    candidates=candidates,
                    **context,
                    verifier_focus=focus,
                    tool_specs=list(self.specs.values()),
                    budget_remaining=self._remaining(),
                ),
                m.ModuleResult[m.ToolPlan | m.FinishDecision | m.ReadObservationDecision],
                provider=True,
            )
            decision = result.data
            if isinstance(decision, m.ReadObservationDecision):
                page = await self._read_observation(decision)
                # Planner API requests are stateless. Preserve recent pages so
                # a later page does not erase the earlier information needed
                # to form a decision. Only this subtask's contiguous read phase
                # is retained, capped to half the whole working context.
                read_result = dict(page, previous_pages=[], omitted_pages=len(read_pages))
                for previous in reversed(read_pages):
                    read_result["previous_pages"].insert(0, previous)
                    read_result["omitted_pages"] -= 1
                    if len(canonical_json(read_result)) > self.cfg.max_planner_context_bytes // 2:
                        read_result["previous_pages"].pop(0)
                        read_result["omitted_pages"] += 1
                        break
                read_pages.append(page)
                continue
            if isinstance(decision, m.FinishDecision):
                await self._event(
                    "subtask.state",
                    status=(
                        "succeeded"
                        if decision.reason == "evidence_complete"
                        else "skipped"
                        if decision.reason == "not_applicable"
                        else "failed"
                    ),
                    detail={"subtask_id": subtask.subtask_id, "reason": decision.reason},
                )
                return decision.reason
            await self._execute_plan(decision, subtask, candidates)
            read_result = None
            read_pages = []
            candidates = await self._search(subtask, focus)

    async def _compose(self):
        reply = await self._call(
            "content.compose",
            self.content.compose,
            m.ComposeRequest,
            dict(
                question=self.req.question,
                completed_subtasks=self.completed_subtasks,
                evidence=self.evidence,
            ),
            m.ModuleResult[m.AnswerRecord],
            provider=True,
        )
        if not set(reply.data.citations) <= {e.evidence_id for e in self.evidence}:
            raise _Stop("failed", "invalid_module_response")
        self.answer = reply.data

    async def _workflow(self):
        if self.cfg.max_input_tokens is not None or self.cfg.max_output_tokens is not None:
            raise _Stop("failed", "unsupported_token_budget")
        try:
            self.specs = validate_tool_specs(self.data.tool_specs())
        except Exception as exc:
            raise _Stop("failed", "invalid_tool_contract") from exc
        await self._catalog()
        if self.req.mode == "fixed_retrieval":
            subtask = m.Subtask(
                subtask_id="fixed", objective=self.req.question, search_query=self.req.question
            )
            candidates = await self._search(subtask)
            if not candidates:
                raise _Stop("insufficient_evidence", "no_candidates")
            if "extract" not in self.specs:
                raise _Stop("failed", "invalid_tool_contract")
            steps = [
                m.ToolStep(
                    step_id=self._id("step"),
                    subtask_id="fixed",
                    tool_name="extract",
                    source_ids=[c.source_id],
                    args={
                        "source_id": c.source_id,
                        "query": self.req.question,
                        "max_chars": self.cfg.fixed_extract_max_chars,
                    },
                    purpose="Read fixed top-k evidence",
                )
                for c in candidates
            ]
            await self._execute_plan(m.ToolPlan(steps=steps), subtask, candidates)
            if not self.evidence:
                raise _Stop("insufficient_evidence", "no_evidence")
            self.completed_subtasks = [subtask]
            await self._compose()
            return "fixed_retrieval_completed"
        reply = await self._call(
            "planner.decompose",
            self.planner.decompose,
            m.PlannerRequest,
            dict(
                question=self.req.question,
                catalog_snapshot_id=self.snapshot,
                tool_specs=list(self.specs.values()),
                budget_remaining=self._remaining(),
            ),
            m.ModuleResult[m.TaskPlan],
            provider=True,
        )
        try:
            if len(reply.data.subtasks) > self.cfg.max_subtasks:
                raise ValueError("too many subtasks")
            self.subtasks = validate_dag(reply.data.subtasks, "subtask_id")
        except ValueError as exc:
            raise _Stop("failed", "invalid_plan") from exc
        ref = await self._artifact(reply.data)
        await self._event("task_plan.accepted", data_ref=ref)
        failed = set()
        for subtask in self.subtasks:
            if set(subtask.depends_on) & failed:
                failed.add(subtask.subtask_id)
                await self._event(
                    "subtask.state", status="blocked", detail={"subtask_id": subtask.subtask_id}
                )
                continue
            reason = await self._subtask(subtask)
            if reason == "insufficient_evidence":
                failed.add(subtask.subtask_id)
            elif reason == "evidence_complete":
                self.completed_subtasks.append(subtask)
            else:
                # A skipped/non-applicable prerequisite is not a successful
                # dependency; descendants need its evidence before execution.
                failed.add(subtask.subtask_id)
        if not self.evidence:
            raise _Stop("insufficient_evidence", "planner_finished_no_evidence")
        await self._compose()
        if self.req.mode == "no_verifier":
            return "planner_finished"
        rounds = 0
        while True:
            reply = await self._call(
                "content.verify",
                self.content.verify,
                m.VerifyRequest,
                dict(question=self.req.question, answer=self.answer, evidence=self.evidence),
                m.ModuleResult[m.Verification],
                provider=True,
            )
            verification = reply.data
            if any(
                not set(issue.evidence_ids) <= {e.evidence_id for e in self.evidence}
                for issue in verification.issues
            ):
                raise _Stop("failed", "invalid_module_response")
            recommendation = verification.recommendation
            focus = recommendation.focus
            if focus and not set(focus.source_ids) <= self.sources.keys():
                raise _Stop("failed", "invalid_module_response")
            ref = await self._artifact(verification)
            await self._event("verification", data_ref=ref, status=verification.verdict)
            if verification.verdict == "supported":
                return "verified_supported"
            if recommendation.action in ("stop", "none"):
                raise _Stop(
                    "insufficient_evidence",
                    "verifier_stop" if recommendation.action == "stop" else "no_further_action",
                )
            if rounds >= self.cfg.max_feedback_rounds:
                raise _Stop("insufficient_evidence", "feedback_exhausted")
            rounds += 1
            await self._event(
                "feedback.accepted", detail={"round": rounds, "action": recommendation.action}
            )
            if recommendation.action != "compose_again":
                # Feedback is an additional pass over the existing task, not an
                # unbudgeted new TaskPlan or a replay of a failed physical call.
                if focus is None:
                    raise _Stop("failed", "invalid_module_response")
                target = next(
                    (item for item in self.subtasks if item.subtask_id == focus.subtask_id),
                    None,
                )
                if target is None:
                    raise _Stop("failed", "invalid_module_response")
                reason = await self._subtask(target, focus)
                # A feedback pass replaces the target's previous completion
                # state. Rebuild from the accepted DAG to keep topological
                # order and avoid duplicates when successful work is revisited.
                completed_ids = {item.subtask_id for item in self.completed_subtasks}
                if reason == "evidence_complete":
                    completed_ids.add(target.subtask_id)
                else:
                    completed_ids.discard(target.subtask_id)
                self.completed_subtasks = [
                    item for item in self.subtasks if item.subtask_id in completed_ids
                ]
            await self._compose()  # Every revised answer is verified on the next iteration.

    async def _cancel_cleanup(self):
        self._sync_tool_dispatches()
        if not self.trace_open:
            return
        cleanup_deadline = self.loop.time() + self.cfg.cancel_cleanup_timeout_ms / 1000
        if not self.trace_broken:
            try:
                async with asyncio.timeout_at(cleanup_deadline):
                    await self._event(
                        "run.cancelled",
                        status="cancelled",
                        terminal=True,
                        detail={
                            "call_counts": self.counts.model_dump(),
                            # A cancellation may interrupt the await that
                            # commits ``call.started``. Keep the reservation
                            # and expose the call identity so an auditor can
                            # identify an unconfirmed start-event commit.
                            # The adapter has not been dispatched in this path.
                            **self._call_snapshots(),
                        },
                    )
            except Exception:
                pass
        # Closing is independent of appending the terminal event so a failed
        # cancellation record cannot prevent the sink from being released.
        try:
            async with asyncio.timeout_at(cleanup_deadline):
                await self.trace.close()
        except Exception:
            pass

    async def _propagate_cancellation(self):
        """Bound trace cleanup and observe it before returning cancellation."""
        cleanup = asyncio.create_task(self._cancel_cleanup())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            cleanup.cancel()
            try:
                await cleanup
            except asyncio.CancelledError:
                pass

    async def execute(self):
        trace_ref = None
        try:
            try:
                # Cancellation during a thread-backed create may arrive after
                # the directory is reserved. Mark cleanup eligible before await;
                # an ordinary initialization failure clears it below.
                self.trace_open = True
                async with asyncio.timeout_at(self.deadline):
                    if self.recovery is not None:
                        await self.trace.create_attempt(self.run_id, self.recovery.attempt_id)
                    else:
                        await self.trace.create_run(self.run_id)
            except Exception as exc:
                self.trace_open = False
                raise _Stop("failed", "trace_sink_unavailable") from exc
            ref = await self._artifact(self.req)
            resumed = self.recovery is not None and self.recovery.resuming
            await self._event("run.resumed" if resumed else "run.started", data_ref=ref)
            reason = await self._workflow()
            status = "completed"
        except asyncio.CancelledError:
            # Shield the short cleanup, preserve original cancellation even if a
            # second cancel arrives, and never leave an unobserved cleanup task.
            await self._propagate_cancellation()
            raise
        except _Stop as stop:
            status, reason = stop.status, stop.reason
        except TimeoutError:
            status, reason = "timeout", "time_budget"
        except Exception:
            status, reason = "failed", "module_error"
        try:
            if self.trace_open:
                if not self.trace_broken:
                    try:
                        await self._event(
                            "run.completed" if status == "completed" else "run.stopped",
                            terminal=True,
                            status=status,
                            detail={
                                "stop_reason": reason,
                                "call_counts": self.counts.model_dump(),
                                **self._call_snapshots(),
                            },
                        )
                    except _Stop:
                        status, reason = "failed", "trace_write_failed"
                try:
                    async with asyncio.timeout(self.cfg.cancel_cleanup_timeout_ms / 1000):
                        trace_ref = await self.trace.close()
                except Exception:
                    status, reason, trace_ref = "failed", "trace_close_failed", None
        except asyncio.CancelledError:
            # Final event/close awaitables are also cancellable by the caller.
            await self._propagate_cancellation()
            raise
        return m.RunResult(
            run_id=self.run_id,
            task_id=self.req.task_id,
            mode=self.req.mode,
            status=status,
            stop_reason=reason,
            answer=self.answer,
            usage=m.AggregateUsage(**self.usage, provider_requests=self.counts.model_calls),
            call_counts=self.counts,
            started_at=self.started_at,
            ended_at=_utc(),
            duration_ms=int((self.loop.time() - self.start) * 1000),
            versions=m.Versions(
                dataset_snapshot=self.snapshot,
                model_id=self.cfg.model_id,
                model_params=self.cfg.model_params,
                prompt_versions=self.cfg.prompt_versions,
                code_version=self.cfg.code_version,
                config_version=self.cfg.config_version,
            ),
            trace_ref=trace_ref,
        )


async def run(
    request: m.RunRequest,
    *,
    data: DataPort,
    content: ContentPort,
    planner: PlannerPort,
    trace: TraceSink,
    state_store: StateStore | None = None,
) -> m.RunResult:
    """Run once on the caller's loop; caller cancellation always propagates.

    Use a fresh TraceSink for every run. Adapters may be shared only if reentrant.
    Request dictionaries can be validated with RunRequest.model_validate first.
    """
    request = m.RunRequest.model_validate(request.model_dump(mode="json"))
    if state_store is not None:
        from .recovery import durable_run

        return await durable_run(
            request,
            data=data,
            content=content,
            planner=planner,
            trace=trace,
            state_store=state_store,
        )
    return await _Controller(request, data, content, planner, trace).execute()


async def resume(
    run_id: str,
    *,
    data: DataPort,
    content: ContentPort,
    planner: PlannerPort,
    trace: TraceSink,
    state_store: StateStore,
) -> m.RunResult:
    """Reconstruct a durable run from known replies with fresh port instances.

    The original deadline and budgets remain authoritative. Incompatible state
    or any unresolved external outcome raises StateStoreError before dispatch.
    The caller owns the store lifecycle; each execution needs a fresh trace sink.
    """
    from .recovery import durable_run

    return await durable_run(
        None,
        run_id=run_id,
        data=data,
        content=content,
        planner=planner,
        trace=trace,
        state_store=state_store,
    )
