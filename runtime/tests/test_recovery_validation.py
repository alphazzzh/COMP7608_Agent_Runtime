"""Refuse malformed durable envelopes before any fresh adapter receives I/O.

The storage layer verifies JSON integrity; these tests cover the independent
runtime schema and ownership boundary, including custom StateStore providers.
"""

import asyncio

import pytest
from test_recovery_runtime import (
    InterruptingTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    cancel_at_gate,
    request,
)

from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink


async def interrupted_run(store, trace_root):
    """Create only known outcomes, isolating schema refusal from lost I/O."""
    trace = InterruptingTrace(trace_root)
    task = asyncio.create_task(
        run(
            request(),
            data=RecordingData(),
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=trace,
            state_store=store,
        )
    )
    await cancel_at_gate(task, trace.entered)


@pytest.mark.parametrize(
    "key,value",
    [
        ("seed", ""),
        ("seed", 123),
        ("attempts", "not-a-list"),
        ("started_epoch", True),
        ("deadline_epoch", -1),
    ],
)
async def test_invalid_run_metadata_refused_before_ports(tmp_path, key, value):
    """Malformed seeds/timestamps must never silently select a new call stream."""
    store = SqliteStateStore(tmp_path / "state.db")
    try:
        await interrupted_run(store, tmp_path / "traces")
        lease = await store.acquire("recoverable-run")
        state = (await store.load(lease)).state
        state[key] = value
        await store.save(lease, state)
        await store.release(lease)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        with pytest.raises(StateStoreError) as refused:
            await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        assert refused.value.code == "recovery_state_invalid"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


class ForeignRequestStore(SqliteStateStore):
    """A custom provider returns valid JSON with a foreign request task owner."""

    corrupt = False

    async def load(self, lease):
        loaded = await super().load(lease)
        if self.corrupt:
            entry = next(v for v in loaded.calls.values() if v["phase"] == "tool")
            entry["request"]["task_id"] = "another-task"
        return loaded


async def test_foreign_request_task_refused_during_preflight(tmp_path):
    """Do not begin reconstruction before the whole journal validates ownership."""
    store = ForeignRequestStore(tmp_path / "state.db")
    try:
        await interrupted_run(store, tmp_path / "traces")
        store.corrupt = True
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        with pytest.raises(StateStoreError) as refused:
            await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        assert refused.value.code == "recovery_state_invalid"
        assert_no_port_calls(data, content, planner)
        assert len(list((tmp_path / "traces").rglob("events.jsonl"))) == 1
    finally:
        await store.aclose()
