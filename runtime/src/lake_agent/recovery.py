"""Rebuild a single controller from a durable, validated adapter-call journal.

This optional coordinator borrows the durable-intent and attempt separation
patterns documented in dsh-superagent-review-2026-11-01.md. It does not execute
reference code or depend on DeepSeek Harness. Rebuilding consumes known replies
through the existing controller so its authorization, evidence and budget rules
stay authoritative. Unknown outcomes never become permission to call an adapter.

Recovery is deliberately strict: identical runtime implementation and trusted
tool contracts, a finite original deadline including downtime, and one local
owner. Each execution writes a separate audit segment. This is not arbitrary
coroutine restoration, cross-version migration or exactly-once external I/O.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from . import __version__
from . import models as m
from .errors import RunStopped
from .state import StateLease, StateStore, StateStoreError
from .validation import canonical_json, catalog_digest, validate_tool_specs

_JOURNAL_VERSION = 1
_RECOVERABLE_STOPS = {
    "trace_sink_unavailable",
    "trace_write_failed",
    "trace_close_failed",
    "state_store_failed",
}


class _RunState(m.Model):
    """Validate metadata independently of SQLite's JSON/hash guarantees."""

    journal_version: int
    runtime_version: m.NonEmpty
    implementation_digest: m.NonEmpty
    tool_digest: m.NonEmpty
    request: m.RunRequest
    seed: str = Field(pattern=r"^[a-f0-9]{32}$")
    started_epoch: float
    started_at: m.NonEmpty
    deadline_epoch: float
    attempts: list[m.NonEmpty]
    result: m.RunResult | None
    progress: m.JsonObject


class _CallRecord(m.Model):
    """A closed envelope; per-phase request/reply validation follows below."""

    status: Literal["started", "completed"]
    phase: m.NonEmpty
    request: m.JsonObject
    auxiliary: bool
    response: m.JsonObject | None = None


def _request_type(phase):
    """Only the runtime's fixed ports can be represented by a durable intent."""
    return {
        "catalog": m.CatalogRequest,
        "content.prepare_catalog": m.CatalogRegistrationRequest,
        "content.search": m.SearchRequest,
        "content.compose": m.ComposeRequest,
        "content.verify": m.VerifyRequest,
        "planner.decompose": m.PlannerRequest,
        "planner.select_tools": m.ToolPlanningRequest,
        "tool": m.ToolRequest,
    }.get(phase)


def _implementation_digest() -> str:
    """Reject source changes even when a developer forgot to bump the version."""
    digest = hashlib.sha256()
    for path in sorted(Path(__file__).parent.glob("*.py")):
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _tool_digest(data) -> str:
    """Hash validated, detached local capabilities; no adapter I/O is allowed."""
    try:
        specs = validate_tool_specs(data.tool_specs())
        value = [specs[name].model_dump(mode="json") for name in sorted(specs)]
        return hashlib.sha256(canonical_json(value)).hexdigest()
    except Exception as exc:
        raise StateStoreError("recovery_contract_mismatch") from exc


def _response_type(phase):
    """Closed response registry prevents loading arbitrary persisted types."""
    return {
        "catalog": m.CatalogResult,
        "content.prepare_catalog": m.ModuleResult[m.CatalogHandle],
        "content.search": m.ModuleResult[m.SearchResponse],
        "content.compose": m.ModuleResult[m.AnswerRecord],
        "content.verify": m.ModuleResult[m.Verification],
        "planner.decompose": m.ModuleResult[m.TaskPlan],
        "planner.select_tools": m.ModuleResult[
            m.ToolPlan | m.FinishDecision | m.ReadObservationDecision
        ],
        "tool": m.ToolResult,
    }.get(phase)


class RecoverySession:
    """Own durable admission and cached replies, never tool scheduling policy."""

    def __init__(self, store: StateStore, lease: StateLease, state, calls, *, resuming):
        self.store, self.lease = store, lease
        self.state, self.calls = state, calls
        self.resuming = resuming
        self.rebuilding = resuming
        self.counter = 0
        self.controller = None
        self.failure = None
        self.prepared = set()
        self.cached = {key for key, value in calls.items() if not value.get("auxiliary", False)}
        self.unseen = set(self.cached)
        self.rebind_call = None
        self.handles = set()
        # Auxiliary registration is a real local invocation on each recovery,
        # distinct from replaying the original logical prepare_catalog call.
        self.extra_content_calls = sum(bool(v.get("auxiliary")) for v in calls.values())
        self.attempt_id = f"attempt-{uuid4().hex}"
        self._inspect_journal()

    def _inspect_journal(self):
        """Validate the complete journal before permitting any adapter access."""
        for key, entry in self.calls.items():
            try:
                envelope = _CallRecord.model_validate(entry)
                phase = envelope.phase
                request_kind = _request_type(phase)
                if request_kind is None:
                    raise ValueError("unknown request phase")
                parsed = request_kind.model_validate(envelope.request)
                request = parsed.model_dump(mode="json")
                if (
                    parsed.call_id != key
                    or parsed.run_id != self.lease.run_id
                    or parsed.task_id != self.state["request"]["task_id"]
                ):
                    raise ValueError("call ownership")
                if envelope.auxiliary and phase != "content.prepare_catalog":
                    raise ValueError("unknown auxiliary call")
                if entry["status"] != "completed":
                    raise StateStoreError("recovery_outcome_unknown")
                kind = _response_type(phase)
                if kind is None:
                    raise ValueError("unknown phase")
                reply = kind.model_validate(entry["response"])
                if (
                    reply.call_id != key
                    or len(canonical_json(entry["response"])) > parsed.max_output_bytes
                ):
                    raise ValueError("reply ownership or size")
                if phase == "content.prepare_catalog" and reply.status == "ok":
                    sources = [m.SourceRecord.model_validate(v) for v in request["sources"]]
                    if (
                        reply.data.catalog_snapshot_id != request["catalog_snapshot_id"]
                        or reply.data.catalog_digest != request["catalog_digest"]
                        or catalog_digest(sources) != request["catalog_digest"]
                        or reply.usage.provider_requests != 0
                    ):
                        raise ValueError("catalog registration")
                    self.handles.add(reply.data.handle)
                    if not entry.get("auxiliary", False):
                        self.rebind_call = entry
            except StateStoreError:
                raise
            except (KeyError, TypeError, ValueError, ValidationError) as exc:
                raise StateStoreError("recovery_state_invalid") from exc

    def attach(self, controller):
        """Reconstruction gets a bounded local window; new I/O uses original UTC."""
        self.controller = controller
        elapsed = max(0, time.time() - self.state["started_epoch"])
        controller.start = controller.loop.time() - elapsed
        controller.started_at = self.state["started_at"]
        controller.counts.content = self.extra_content_calls
        for entry in self.calls.values():
            if entry.get("auxiliary"):
                reply = m.ModuleResult[m.CatalogHandle].model_validate(entry["response"])
                controller._add_usage(reply.usage)
        if not self.rebuilding:
            self._set_original_deadline()

    def identifier(self, prefix):
        """One sequence includes artifacts, observations and evidence identities."""
        self.counter += 1
        value = f"{self.state['seed']}:{self.counter}:{prefix}".encode()
        return f"{prefix}-{hashlib.sha256(value).hexdigest()[:32]}"

    def _set_original_deadline(self):
        remaining = self.state["deadline_epoch"] - time.time()
        self.controller.deadline = self.controller.loop.time() + max(0, remaining)

    def annotate(self, kind, fields):
        """A reconstructed call is explicitly historical in the new audit segment."""
        if fields.get("call_id") in self.cached:
            fields["detail"] = {**fields.get("detail", {}), "replayed": True}
        return fields

    def _normalized(self, request):
        value = request.model_dump(mode="json") if isinstance(request, BaseModel) else request
        # JSON roundtrip creates a detached copy before normalizing nested fields.
        value = json.loads(canonical_json(value))
        value.pop("timeout_ms", None)
        if "budget_remaining" in value:
            value["budget_remaining"].pop("wall_time_ms", None)
        if "catalog_handle" in value:
            if value["catalog_handle"] not in self.handles:
                raise StateStoreError("recovery_catalog_mismatch")
            # Only handles returned by a validated registration of this exact
            # stored snapshot enter the alias set. Arbitrary values never match.
            value["catalog_handle"] = {
                "registered_catalog": self.rebind_call["request"]["catalog_digest"]
            }
        return canonical_json(value)

    def _matches(self, phase, request):
        entry = self.calls.get(request.call_id)
        if entry is None:
            return False
        if entry.get("auxiliary") or phase != entry["phase"]:
            raise StateStoreError("recovery_request_mismatch")
        if self._normalized(request) != self._normalized(entry["request"]):
            raise StateStoreError("recovery_request_mismatch")
        return True

    async def _guard_match(self, phase, request):
        """Do not let a mismatch become an ordinary adapter/retry error."""
        try:
            return self._matches(phase, request)
        except StateStoreError as exc:
            self.failure = exc.code
            raise RunStopped("failed", "state_store_failed") from exc

    async def _stored(self, method, *args):
        """A failed persistence boundary freezes all subsequent dispatch."""
        try:
            return await method(*args)
        except StateStoreError as exc:
            self.failure = exc.code
            raise RunStopped("failed", "state_store_failed") from exc

    async def progress(self):
        """Persist inspectable progress; replies remain the reconstruction source."""
        if self.failure:
            raise RunStopped("failed", "state_store_failed")
        c = self.controller
        self.state["progress"] = {
            "call_counts": c.counts.model_dump(mode="json"),
            "model_calls_reserved": c.model_calls_reserved,
            "usage": c.usage,
            "snapshot": c.snapshot,
            "completed_subtasks": [v.subtask_id for v in c.completed_subtasks],
            "evidence_ids": [v.evidence_id for v in c.evidence],
            "observation_ids": list(c.observations_by_id),
            "pending": c._call_snapshots(),
        }
        await self._stored(self.store.save, self.lease, self.state)

    async def _activate(self, permitted=()):
        """Cross from history to new work only after every earlier call matches."""
        if self.failure:
            raise RunStopped("failed", "state_store_failed")
        if self.unseen - set(permitted):
            raise StateStoreError("recovery_request_mismatch")
        if not self.rebuilding:
            return
        self.rebuilding = False
        self._set_original_deadline()
        self.controller._time()
        if self.rebind_call is not None:
            await self._register_catalog()

    async def _register_catalog(self):
        """Rebuild only the local ContentPort index, charging a separate call."""
        c = self.controller
        old = self.rebind_call["request"]
        if c.snapshot != old["catalog_snapshot_id"]:
            raise StateStoreError("recovery_catalog_mismatch")
        request = m.CatalogRegistrationRequest.model_validate(
            {
                **old,
                "call_id": f"rebind-{uuid4().hex}",
                "operation_id": f"rebind-op-{uuid4().hex}",
                "timeout_ms": max(
                    1,
                    min(
                        self.state["request"]["config"]["per_call_timeout_ms"],
                        int((c.deadline - c.loop.time()) * 1000),
                    ),
                ),
            }
        )
        # Independent IDs keep auxiliary work from changing the historical
        # controller stream. It still produces ordinary paired audit records.
        ref = f"rebind-artifact-{uuid4().hex}"
        await c._write(lambda: c.trace.put_artifact(ref, request.model_dump(mode="json")))
        c.counts.content += 1
        await c._event(
            "recovery.prepare_catalog.call.started",
            call_id=request.call_id,
            operation_id=request.operation_id,
            attempt=1,
            data_ref=ref,
        )
        reply = await self._dispatch(
            "content.prepare_catalog",
            c.content.prepare_catalog,
            request,
            m.ModuleResult[m.CatalogHandle],
            auxiliary=True,
        )
        # Rebinding remains a local-port contract, but a violation cannot erase
        # a declared real provider dispatch or turn unknown usage into zero.
        c._add_usage(reply.usage)
        c.counts.model_calls += reply.usage.provider_requests
        c.model_calls_reserved += reply.usage.provider_requests
        valid = (
            reply.status == "ok"
            and reply.usage.provider_requests == 0
            and reply.data.catalog_snapshot_id == old["catalog_snapshot_id"]
            and reply.data.catalog_digest == old["catalog_digest"]
        )
        ref = f"rebind-artifact-{uuid4().hex}"
        await c._write(lambda: c.trace.put_artifact(ref, reply.model_dump(mode="json")))
        await c._event(
            "recovery.prepare_catalog.call.completed"
            if valid
            else "recovery.prepare_catalog.call.failed",
            call_id=request.call_id,
            operation_id=request.operation_id,
            attempt=1,
            data_ref=ref,
            usage=reply.usage,
            status=reply.status if valid else "invalid_response",
        )
        if not valid:
            raise StateStoreError("recovery_catalog_mismatch")
        self.handles.add(reply.data.handle)
        c.handle = reply.data.handle

    async def prepare_serial(self, phase, request):
        """Check a cached request or activate before its new intent is committed."""
        if await self._guard_match(phase, request):
            return request
        await self._stored(self._activate)
        updates = {}
        if isinstance(request, m.SearchRequest):
            updates["catalog_handle"] = self.controller.handle
        if hasattr(request, "budget_remaining"):
            # Fields were assembled before optional catalog re-registration.
            # Refresh only wall time: other budgets are reserved by the caller.
            updates["budget_remaining"] = request.budget_remaining.model_copy(
                update={"wall_time_ms": self.controller._remaining().wall_time_ms}
            )
        return request.model_copy(update=updates)

    async def prepare_wave(self):
        """Rebind a new frontier before constructing its tool call windows.

        Durable admission records every intent before starting any worker, so a
        resumable historical wave is wholly cached. A partly admitted wave has
        unknown intents and was refused during preflight. With no unseen calls,
        activation here cannot skip history or disturb deterministic IDs.
        """
        if self.rebuilding and not self.unseen:
            await self._stored(self._activate)

    async def admit_wave(self, attempts):
        """Persist every new wave intent before starting any of its workers."""
        known, new = set(), []
        for attempt in attempts:
            request = attempt.request
            if await self._guard_match("tool", request):
                known.add(request.call_id)
            else:
                new.append(request)
        if new:
            await self._stored(self._activate, known)
            for request in new:
                await self._begin("tool", request)
        for attempt in attempts:
            if attempt.request.call_id not in known:
                attempt.deadline = min(attempt.deadline, self.controller.deadline)

    async def _begin(self, phase, request, *, auxiliary=False):
        value = {
            "status": "started",
            "phase": phase,
            "request": request.model_dump(mode="json"),
            "auxiliary": auxiliary,
        }
        await self._stored(self.store.begin_call, self.lease, request.call_id, value)
        self.calls[request.call_id] = value
        self.prepared.add(request.call_id)

    async def _dispatch(self, phase, method, request, response_type, *, auxiliary=False):
        if self.failure:
            raise RunStopped("failed", "state_store_failed")
        self.controller._time()
        if request.call_id not in self.prepared:
            await self._begin(phase, request, auxiliary=auxiliary)
        # Storage is awaited; the original deadline may expire while committing
        # intent. timeout_at(past_deadline) alone can still enter coroutine code.
        self.controller._time()
        deadline = min(
            self.controller.deadline, self.controller.loop.time() + request.timeout_ms / 1000
        )
        owner = asyncio.current_task()
        cancelled_before = owner.cancelling() if owner is not None else 0
        try:
            async with asyncio.timeout_at(deadline):
                raw = await method(request.model_copy(deep=True))
            if self.controller.loop.time() >= deadline:
                raise TimeoutError
            raw = raw.model_dump(mode="json") if isinstance(raw, BaseModel) else raw
            if len(canonical_json(raw)) > request.max_output_bytes:
                raise ValueError("response exceeds max_output_bytes")
            response = response_type.model_validate(raw)
            if response.call_id != request.call_id:
                raise ValueError("response call_id mismatch")
            value = {
                **self.calls[request.call_id],
                "status": "completed",
                "response": response.model_dump(mode="json"),
            }
            # Each worker commits its own validated reply immediately. A slow sibling
            # or subsequent trace failure must not erase a known external outcome.
            await self._stored(self.store.finish_call, self.lease, request.call_id, value)
            self.calls[request.call_id] = value
            if phase == "content.prepare_catalog" and response.status == "ok":
                self.handles.add(response.data.handle)
                if not auxiliary:
                    self.rebind_call = value
            return response
        finally:
            # Preserve caller intent through malformed returns and storage
            # errors too. A valid response is committed before reaching here.
            if owner is not None and owner.cancelling() > cancelled_before:
                raise asyncio.CancelledError

    async def invoke(self, phase, method, request, response_type):
        if request.call_id in self.cached:
            if not await self._guard_match(phase, request):
                self.failure = "recovery_request_mismatch"
                raise RunStopped("failed", "state_store_failed")
            self.unseen.discard(request.call_id)
            return response_type.model_validate(self.calls[request.call_id]["response"])
        return await self._dispatch(phase, method, request, response_type)


async def durable_run(request, *, data, content, planner, trace, state_store, run_id=None):
    """Own one lease through reconstruction, final persistence and cancellation."""
    # Local imports keep the existing runtime usable without loading the
    # optional POSIX storage backend and avoid a runtime/recovery import cycle.
    from .runtime import _Controller

    if not callable(getattr(trace, "create_attempt", None)):
        raise StateStoreError("recovery_trace_unsupported")
    resuming = run_id is not None
    if resuming:
        lease = await state_store.acquire(run_id)
    else:
        request = m.RunRequest.model_validate(request.model_dump(mode="json"))
        run_id = request.run_id or f"run-{uuid4().hex}"
        request = request.model_copy(update={"run_id": run_id})
        now = time.time()
        state = {
            "journal_version": _JOURNAL_VERSION,
            "runtime_version": __version__,
            "implementation_digest": _implementation_digest(),
            "tool_digest": _tool_digest(data),
            "request": request.model_dump(mode="json"),
            "seed": uuid4().hex,
            "started_epoch": now,
            "started_at": datetime.fromtimestamp(now, UTC).isoformat(),
            "deadline_epoch": now + request.config.max_wall_time_ms / 1000,
            "attempts": [],
            "result": None,
            "progress": {},
        }
        lease = await state_store.create(run_id, state)
    try:
        loaded = await state_store.load(lease)
        state = loaded.state
        try:
            parsed_state = _RunState.model_validate(state)
            if (
                type(state["started_epoch"]) is not float
                or type(state["deadline_epoch"]) is not float
                or state["started_epoch"] <= 0
                or not math.isclose(
                    state["deadline_epoch"] - state["started_epoch"],
                    parsed_state.request.config.max_wall_time_ms / 1000,
                    rel_tol=0,
                    abs_tol=0.00001,
                )
                or not math.isclose(
                    datetime.fromisoformat(state["started_at"]).timestamp(),
                    state["started_epoch"],
                    rel_tol=0,
                    abs_tol=0.000001,
                )
            ):
                raise ValueError("invalid original deadline")
            if (
                state["journal_version"] != _JOURNAL_VERSION
                or state["runtime_version"] != __version__
                or state["implementation_digest"] != _implementation_digest()
            ):
                raise StateStoreError("recovery_version_mismatch")
            request = m.RunRequest.model_validate(state["request"])
            if request.run_id != run_id:
                raise ValueError("run ownership")
            if parsed_state.result is not None:
                if (
                    parsed_state.result.run_id != run_id
                    or parsed_state.result.task_id != request.task_id
                ):
                    raise ValueError("terminal result ownership")
                return parsed_state.result
            if state["tool_digest"] != _tool_digest(data):
                raise StateStoreError("recovery_contract_mismatch")
            session = RecoverySession(state_store, lease, state, loaded.calls, resuming=resuming)
            if resuming and time.time() >= state["deadline_epoch"]:
                raise StateStoreError("recovery_deadline_expired")
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise StateStoreError("recovery_state_invalid") from exc
        state["attempts"].append(session.attempt_id)
        await state_store.save(lease, state)
        controller = _Controller(request, data, content, planner, trace, recovery=session)
        result = await controller.execute()
        if session.failure:
            raise StateStoreError(session.failure)
        if session.unseen:
            raise StateStoreError("recovery_request_mismatch")
        # Audit failures are recoverable only through the separate journal.
        # Normal business stops (including a provider 403) are durable terminal
        # outcomes, not invitations to repeatedly spend another model budget.
        if result.stop_reason not in _RECOVERABLE_STOPS:
            state["result"] = result.model_dump(mode="json")
        await state_store.save(lease, state)
        return result
    finally:
        # Cleanup must run on every path, but storage failure during teardown
        # must not replace an already propagating caller cancellation or the
        # primary recovery error. The SQLite backend drains its disk worker and
        # closes the OS lock even when updating the persisted lease token fails.
        primary = sys.exception()
        try:
            await state_store.release(lease)
        except Exception:
            if primary is None:
                raise
