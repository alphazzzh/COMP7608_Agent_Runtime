"""Public regressions for usage declared by auxiliary catalog re-registration.

A local-only port can still report measured or unknown token quantities. The
controller must aggregate those declarations across restarts. An unexpected
provider dispatch is a contract failure, but its actual charge must survive.
"""

from __future__ import annotations

import asyncio

import pytest
from test_recovery_edges import FrontierTrace
from test_recovery_runtime import (
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    cancel_at_gate,
    request,
    trace_events,
)

from lake_agent.models import Usage
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink


class MeteredContent(RecordingContent):
    """Keep the same implementation while changing this instance's usage report."""

    def __init__(self, reported_usage=None):
        super().__init__()
        self.reported_usage = reported_usage or Usage.local()

    async def prepare_catalog(self, req):
        reply = await super().prepare_catalog(req)
        return reply.model_copy(update={"usage": self.reported_usage})


async def interrupt_original(state_path, trace_root):
    """Leave two known paid planner calls and one known successful tool reply."""
    store = SqliteStateStore(state_path)
    trace = FrontierTrace(trace_root, "evidence.added")
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=RecordingData(),
                content=MeteredContent(),
                planner=RecordingPlanner(),
                trace=trace,
                state_store=store,
            )
        )
        await cancel_at_gate(task, trace.entered)
    finally:
        await store.aclose()


async def test_auxiliary_local_port_violation_preserves_actual_provider_charge(tmp_path):
    """Reject new frontier work without erasing a provider call already reported."""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    await interrupt_original(state_path, trace_root)
    usage = Usage(provider_requests=1, input_tokens=11, output_tokens=5)
    data, content, planner = RecordingData(), MeteredContent(usage), RecordingPlanner()
    store = SqliteStateStore(state_path)
    try:
        with pytest.raises(StateStoreError) as invalid:
            await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(trace_root),
                state_store=store,
            )
        assert invalid.value.code == "recovery_catalog_mismatch"
        assert len(content.preparations) == 1
        assert content.searches == content.compositions == content.verifications == []
        assert data.catalog_requests == data.tool_requests == []
        assert planner.decompositions == planner.selections == []
        # The documented refusal raises rather than returning a business result.
        # Public store inspection must still expose the last known accounting.
        lease = await store.acquire("recoverable-run")
        try:
            progress = (await store.load(lease)).state["progress"]
        finally:
            await store.release(lease)
        assert progress["call_counts"]["content"] == 3  # Original prepare/search, then rebind.
        assert progress["call_counts"]["model_calls"] == 3  # Two planner calls plus violation.
        assert (progress["usage"]["input_tokens"], progress["usage"]["output_tokens"]) == (25, 11)
        failed_events = [
            event
            for path in trace_root.rglob("events.jsonl")
            for event in trace_events(path)
            if event["event_type"] == "recovery.prepare_catalog.call.failed"
        ]
        assert len(failed_events) == 1
        assert failed_events[0]["call_id"] == content.preparations[0].call_id
        assert failed_events[0]["usage"] == usage.model_dump(mode="json")
        assert failed_events[0]["status"] == "invalid_response"
    finally:
        await store.aclose()
    # The invalid registration remains inspectable but cannot authorize another
    # recovery. Refusing the journal cannot invoke or charge another local port.
    fresh_data, fresh_content, fresh_planner = RecordingData(), MeteredContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        with pytest.raises(StateStoreError) as blocked:
            await resume(
                "recoverable-run",
                data=fresh_data,
                content=fresh_content,
                planner=fresh_planner,
                trace=JsonlTraceSink(trace_root),
                state_store=restarted,
            )
        assert blocked.value.code == "recovery_state_invalid"
        assert_no_port_calls(fresh_data, fresh_content, fresh_planner)
    finally:
        await restarted.aclose()


@pytest.mark.parametrize(
    "reported_usage,expected_tokens",
    [
        (Usage(provider_requests=0, input_tokens=2, output_tokens=1), (23, 10)),
        (Usage(provider_requests=0, input_tokens=None, output_tokens=None), (None, None)),
    ],
    ids=["measured-local-tokens", "unknown-local-tokens"],
)
async def test_prior_auxiliary_usage_survives_another_interruption(
    tmp_path, reported_usage, expected_tokens
):
    """Rebuilding ordinary calls must also retain usage from prior local rebinds."""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    await interrupt_original(state_path, trace_root)
    first_content = MeteredContent(reported_usage)
    first_resume_trace = FrontierTrace(trace_root, "content.search.call.completed")
    store = SqliteStateStore(state_path)
    try:
        task = asyncio.create_task(
            resume(
                "recoverable-run",
                data=RecordingData(),
                content=first_content,
                planner=RecordingPlanner(),
                trace=first_resume_trace,
                state_store=store,
            )
        )
        await cancel_at_gate(task, first_resume_trace.entered)
        assert len(first_content.preparations) == len(first_content.searches) == 1
    finally:
        await store.aclose()
    data, content, planner = RecordingData(), MeteredContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        result = await resume(
            "recoverable-run",
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=restarted,
        )
    finally:
        await restarted.aclose()
    assert result.status == "completed"
    assert data.catalog_requests == data.tool_requests == []
    assert content.searches == []
    assert len(content.preparations) == len(content.compositions) == len(content.verifications) == 1
    assert planner.decompositions == []
    assert len(planner.selections) == 1
    assert result.call_counts.content == 7  # Five ordinary calls plus two rebinds.
    assert result.call_counts.model_calls == result.usage.provider_requests == 3
    assert (result.usage.input_tokens, result.usage.output_tokens) == expected_tokens
