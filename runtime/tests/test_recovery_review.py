"""Review regressions for optional imports and the recovery-to-live boundary.

Tests use public run/resume APIs, real temporary storage, and the existing
recording fixtures. Cancellation and audit gates model interruptions without
editing checkpoints or depending on controller implementation details.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
from contextlib import suppress

import pytest
from test_recovery_runtime import (
    InterruptingTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    cancel_at_gate,
    recovery_api,
    request,
)

from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink


def test_ordinary_runtime_import_does_not_require_posix_state_backend():
    """Importing ordinary run must work where the optional fcntl is absent."""
    script = textwrap.dedent(
        """
        import builtins
        original_import = builtins.__import__

        def without_fcntl(name, *args, **kwargs):
            if name == "fcntl":
                raise ModuleNotFoundError("fcntl is unavailable on this platform")
            return original_import(name, *args, **kwargs)

        builtins.__import__ = without_fcntl
        from lake_agent.runtime import run
        assert callable(run)
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr


async def _interrupt_known_run(store, trace_root, *, event_type, req=None):
    """Leave a journal of known calls and an interrupted audit continuation."""
    trace = InterruptingTrace(trace_root, event_type=event_type)
    task = asyncio.create_task(
        run(
            req or request(),
            data=RecordingData(),
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=trace,
            state_store=store,
        )
    )
    await cancel_at_gate(task, trace.entered)


@pytest.mark.parametrize("invalid_reply", [False, True])
async def test_swallowed_rebind_cancellation_blocks_new_frontier_calls(tmp_path, invalid_reply):
    """A local registration cannot swallow caller cancellation and resume work."""

    class SuppressingContent(RecordingContent):
        def __init__(self):
            super().__init__()
            self.entered = asyncio.Event()

        async def prepare_catalog(self, req):
            self.entered.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                # A deliberately broken adapter exercises the controller's
                # obligation to preserve cancellation at this extra boundary.
                if invalid_reply:
                    return {"call_id": req.call_id, "status": "invalid"}
                return await super().prepare_catalog(req)

    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    task = None
    try:
        await _interrupt_known_run(store, tmp_path / "traces", event_type="evidence.added")
        data, content, planner = RecordingData(), SuppressingContent(), RecordingPlanner()
        task = asyncio.create_task(
            resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        )
        await asyncio.wait_for(content.entered.wait(), timeout=3)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert content.searches == content.compositions == content.verifications == []
        assert data.catalog_requests == data.tool_requests == []
        assert planner.decompositions == planner.selections == []
        assert task.cancelled(), "a swallowed local cancellation became a completed resume"
    finally:
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        await store.aclose()


async def test_frontier_planner_gets_remaining_wall_time_after_rebind(tmp_path):
    """New planner budgets must reflect local registration and original expiry."""

    class DelayedContent(RecordingContent):
        async def prepare_catalog(self, req):
            # Simulate index reconstruction work. The assertion compares two
            # request fields and has no machine-speed or elapsed-time threshold.
            await asyncio.sleep(0.05)
            return await super().prepare_catalog(req)

    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request(max_wall_time_ms=10_000, per_call_timeout_ms=10_000)
    try:
        await _interrupt_known_run(
            store,
            tmp_path / "traces",
            event_type="planner.select_tools.call.started",
            req=req,
        )
        planner = RecordingPlanner()
        result = await resume(
            req.run_id,
            state_store=store,
            data=RecordingData(),
            content=DelayedContent(),
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "completed"
        assert planner.selections
        frontier = planner.selections[0]
        # Both bounds use the same global expiry because the per-call maximum
        # equals the original whole-run allowance. Integer floor/ceiling gives
        # wall_time_ms <= timeout_ms after the recovery boundary is crossed.
        assert frontier.budget_remaining.wall_time_ms <= frontier.timeout_ms
    finally:
        await store.aclose()


async def test_expired_auxiliary_intent_never_dispatches_registration(tmp_path):
    """Durable admission time can exhaust expiry before adapter entry."""
    resume, Store, _ = recovery_api()

    class SlowAuxiliaryIntentStore(Store):
        async def begin_call(self, lease, call_id, value):
            await super().begin_call(lease, call_id, value)
            if value.get("auxiliary"):
                # Represent slow durable storage after the original deadline
                # check. A timeout context with a past expiry cannot by itself
                # stop synchronous adapter entry before the next event-loop tick.
                await asyncio.sleep(1.05)

    store = SlowAuxiliaryIntentStore(tmp_path / "state.db")
    try:
        await _interrupt_known_run(
            store,
            tmp_path / "traces",
            event_type="evidence.added",
            req=request(max_wall_time_ms=1000),
        )
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        result = await resume(
            "recoverable-run",
            state_store=store,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "timeout"
        assert content.preparations == content.searches == content.compositions == []
        assert content.verifications == []
        assert data.catalog_requests == data.tool_requests == []
        assert planner.decompositions == planner.selections == []
    finally:
        await store.aclose()


async def test_frontier_tool_gets_fresh_call_window_after_registration(tmp_path):
    """A separate recovery registration cannot consume a new tool's timeout."""

    class DelayedContent(RecordingContent):
        async def prepare_catalog(self, req):
            await asyncio.sleep(0.3)
            return await super().prepare_catalog(req)

    class DelayedData(RecordingData):
        async def invoke(self, req):
            await asyncio.sleep(0.3)
            return await super().invoke(req)

    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request(
        max_wall_time_ms=10_000,
        per_call_timeout_ms=500,
        max_retries_per_data_operation=0,
    )
    try:
        # No tool adapter or durable tool intent exists at this audit gate.
        # Recovery therefore reaches its first new I/O at tool-wave admission.
        await _interrupt_known_run(
            store, tmp_path / "traces", event_type="tool.call.started", req=req
        )
        data, content, planner = DelayedData(), DelayedContent(), RecordingPlanner()
        result = await resume(
            req.run_id,
            state_store=store,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        # Each operation takes less than its independent half-second limit.
        # Their sum exceeds one limit and exposes a window started before rebind.
        assert (result.status, result.stop_reason) == ("completed", "verified_supported")
        assert len(data.tool_requests) == 1
        assert len(content.preparations) == 1
        assert (result.call_counts.tools, result.call_counts.steps) == (1, 1)
    finally:
        await store.aclose()


async def test_caller_cancellation_survives_durable_release_failure(tmp_path):
    """A secondary storage error cannot replace the caller's cancellation."""
    _, Store, StateStoreError = recovery_api()

    class ReleaseFailingStore(Store):
        async def release(self, lease):
            await super().release(lease)
            raise StateStoreError("storage_error")

    store = ReleaseFailingStore(tmp_path / "state.db")
    task = None
    try:
        data = RecordingData(hold_tool=True)
        task = asyncio.create_task(
            run(
                request(),
                data=data,
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        )
        await asyncio.wait_for(data.entered.wait(), timeout=3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert task.cancelled()
        # release already unlocked before raising. A new lease can be acquired
        # even though its failed cleanup must not obscure the original cancel.
        lease = await store.acquire("recoverable-run")
        with pytest.raises(StateStoreError):
            await store.release(lease)
    finally:
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        await store.aclose()
