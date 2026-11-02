"""Recovery boundaries that require more than a single cancelled execution.

These acceptance tests keep SQLite and audit files real, inject faults only at
public storage/trace methods, and rebuild with new port instances. The version
compatibility check imports a temporary source copy in a child interpreter so
the working tree and the parent interpreter remain untouched.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
from pathlib import Path

import pytest
from test_recovery_runtime import (
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    cancel_at_gate,
    request,
    trace_events,
)

import lake_agent
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink


class FrontierTrace(JsonlTraceSink):
    """Pause a newly executed event while allowing historical events to replay."""

    def __init__(self, root, event_type):
        super().__init__(root)
        self.event_type = event_type
        self.entered = asyncio.Event()
        self.interrupted_event = None

    async def append(self, event):
        await super().append(event)
        if (
            event.event_type == self.event_type
            and not event.detail.get("replayed")
            and self.interrupted_event is None
        ):
            self.interrupted_event = event
            self.entered.set()
            await asyncio.Future()


class FailingCallStore(SqliteStateStore):
    """Fail one public tool-ledger write, before or after its real SQLite commit."""

    def __init__(self, path, *, boundary, after_commit):
        super().__init__(path)
        self.boundary = boundary
        self.after_commit = after_commit
        self.failed = False

    async def begin_call(self, lease, call_id, value):
        if self.boundary == "begin" and value["phase"] == "tool" and not self.failed:
            self.failed = True
            if self.after_commit:
                await super().begin_call(lease, call_id, value)
            raise StateStoreError("storage_error")
        await super().begin_call(lease, call_id, value)

    async def finish_call(self, lease, call_id, value):
        if self.boundary == "finish" and value["phase"] == "tool" and not self.failed:
            self.failed = True
            if self.after_commit:
                await super().finish_call(lease, call_id, value)
            raise StateStoreError("storage_error")
        await super().finish_call(lease, call_id, value)


class FailingToolResultTrace(JsonlTraceSink):
    """Refuse the first returned tool-result artifact, before writing any bytes."""

    def __init__(self, root):
        super().__init__(root)
        self.failed = False

    async def put_artifact(self, ref, value):
        if (
            not self.failed
            and isinstance(value, dict)
            and isinstance(value.get("data"), dict)
            and "evidence" in value["data"]
        ):
            self.failed = True
            raise OSError("controlled tool-result artifact failure")
        await super().put_artifact(ref, value)


async def test_multiple_interruptions_rebind_each_fresh_content_instance(tmp_path):
    """Auxiliary rebind history cannot change IDs, replay calls, or reset counts."""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    adapters = []
    interrupted = []
    saved_traces = {}
    for index, event_type in enumerate(
        ["evidence.added", "content.search.call.completed", "content.compose.call.completed"]
    ):
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        adapters.append((data, content, planner))
        trace = FrontierTrace(trace_root, event_type)
        store = SqliteStateStore(state_path)
        try:
            if index == 0:
                operation = run(
                    request(),
                    data=data,
                    content=content,
                    planner=planner,
                    trace=trace,
                    state_store=store,
                )
            else:
                operation = resume(
                    "recoverable-run",
                    data=data,
                    content=content,
                    planner=planner,
                    trace=trace,
                    state_store=store,
                )
            await cancel_at_gate(asyncio.create_task(operation), trace.entered)
        finally:
            await store.aclose()
        interrupted.append(trace.interrupted_event)
        for path, expected in saved_traces.items():
            assert path.read_bytes() == expected
        saved_traces = {path: path.read_bytes() for path in trace_root.rglob("events.jsonl")}
        assert len(saved_traces) == index + 1

    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    adapters.append((data, content, planner))
    store = SqliteStateStore(state_path)
    try:
        result = await resume(
            "recoverable-run",
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=store,
        )
    finally:
        await store.aclose()
    assert (result.status, result.stop_reason) == ("completed", "verified_supported")
    assert (result.call_counts.catalog, result.call_counts.tools, result.call_counts.steps) == (
        1,
        1,
        1,
    )
    assert (result.call_counts.planner, result.call_counts.model_calls) == (3, 3)
    assert result.call_counts.content == 8  # Five ordinary calls, three local rebinds.
    assert (result.usage.input_tokens, result.usage.output_tokens) == (21, 9)
    assert sum(len(d.catalog_requests) for d, _, _ in adapters) == 1
    assert sum(len(d.tool_requests) for d, _, _ in adapters) == 1
    assert sum(len(c.preparations) for _, c, _ in adapters) == 4
    assert sum(len(c.searches) for _, c, _ in adapters) == 2
    assert sum(len(c.compositions) for _, c, _ in adapters) == 1
    assert sum(len(c.verifications) for _, c, _ in adapters) == 1
    assert sum(len(p.decompositions) for _, _, p in adapters) == 1
    assert sum(len(p.selections) for _, _, p in adapters) == 2
    assert data.catalog_requests == data.tool_requests == []
    assert content.searches == content.compositions == []
    assert planner.decompositions == planner.selections == []
    assert result.answer.citations == [interrupted[0].data_ref]
    assert content.verifications[0].evidence[0].evidence_id == interrupted[0].data_ref
    paths = list(trace_root.rglob("events.jsonl"))
    assert len(paths) == 4
    assert len({path.parent for path in paths}) == 4
    for path, expected in saved_traces.items():
        assert path.read_bytes() == expected
    for path in paths:
        events = trace_events(path)
        assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
        assert events[0]["event_type"] in {"run.started", "run.resumed"}
        assert all(event["run_id"] == result.run_id for event in events)


@pytest.mark.parametrize("boundary", ["begin", "finish"])
@pytest.mark.parametrize("after_commit", [False, True], ids=["before-commit", "after-commit"])
async def test_storage_failure_freezes_dispatch_and_recovers_only_known_outcomes(
    tmp_path, boundary, after_commit
):
    """Only durable evidence of no dispatch or a valid reply permits continuation."""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    store = FailingCallStore(state_path, boundary=boundary, after_commit=after_commit)
    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    try:
        with pytest.raises(StateStoreError) as failed:
            await run(
                request(),
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(trace_root),
                state_store=store,
            )
        assert failed.value.code == "storage_error"
        assert store.failed
        assert len(data.catalog_requests) == 1
        assert len(data.tool_requests) == (0 if boundary == "begin" else 1)
        assert len(planner.decompositions) == len(planner.selections) == 1
        assert len(content.searches) == len(content.preparations) == 1
        assert content.compositions == content.verifications == []
    finally:
        await store.aclose()

    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        # An intent persisted without a reply is unknown even when the test
        # knows the injected failure occurred before the actual adapter body.
        unknown = (boundary == "begin" and after_commit) or (
            boundary == "finish" and not after_commit
        )
        if unknown:
            with pytest.raises(StateStoreError) as blocked:
                await resume(
                    "recoverable-run",
                    data=data,
                    content=content,
                    planner=planner,
                    trace=JsonlTraceSink(trace_root),
                    state_store=restarted,
                )
            assert blocked.value.code == "recovery_outcome_unknown"
            assert_no_port_calls(data, content, planner)
        else:
            result = await resume(
                "recoverable-run",
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(trace_root),
                state_store=restarted,
            )
            assert result.status == "completed"
            assert data.catalog_requests == []
            assert len(data.tool_requests) == (1 if boundary == "begin" else 0)
            assert (result.call_counts.catalog, result.call_counts.tools) == (1, 1)
            assert result.call_counts.model_calls == 3
    finally:
        await restarted.aclose()


async def test_valid_reply_survives_failure_writing_its_trace_artifact(tmp_path):
    """The journal must own the reply before a fallible audit-result write starts."""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    store = SqliteStateStore(state_path)
    data = RecordingData()
    trace = FailingToolResultTrace(trace_root)
    try:
        stopped = await run(
            request(),
            data=data,
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=trace,
            state_store=store,
        )
        assert trace.failed
        assert (stopped.status, stopped.stop_reason) == ("failed", "trace_write_failed")
        assert len(data.tool_requests) == 1
        call_id = data.tool_requests[0].call_id
        events = trace_events(stopped.trace_ref)
        assert not any(event["event_type"] == "tool.call.completed" for event in events)
    finally:
        await store.aclose()
    fresh_data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    restarted = SqliteStateStore(state_path)
    try:
        result = await resume(
            stopped.run_id,
            data=fresh_data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=restarted,
        )
    finally:
        await restarted.aclose()
    assert result.status == "completed"
    assert fresh_data.catalog_requests == fresh_data.tool_requests == []
    assert content.compositions[0].evidence[0].call_id == call_id
    assert result.call_counts.tools == 1
    replayed = [
        event
        for event in trace_events(result.trace_ref)
        if event["event_type"] == "tool.call.completed"
    ]
    assert len(replayed) == 1
    assert replayed[0]["detail"]["replayed"] is True


async def test_runtime_source_change_refuses_recovery_in_fresh_interpreter(tmp_path):
    """An implementation change must not silently consume an old call journal."""
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    store = SqliteStateStore(state_path)
    trace = FrontierTrace(trace_root, "evidence.added")
    try:
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
    finally:
        await store.aclose()
    isolated_root = tmp_path / "different-runtime"
    isolated_package = isolated_root / "lake_agent"
    shutil.copytree(
        Path(lake_agent.__file__).parent,
        isolated_package,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    # A harmless source difference still changes the strict implementation
    # identity; no private digest helper or persisted state schema is patched.
    with (isolated_package / "runtime.py").open("a") as changed:
        changed.write("\n# Deliberately different runtime build for compatibility acceptance.\n")
    program = """
import asyncio, json, sys
sys.path.insert(0, sys.argv[3])
sys.path.insert(1, sys.argv[4])
from lake_agent.runtime import resume
from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.trace import JsonlTraceSink
from test_recovery_runtime import RecordingData, RecordingContent, RecordingPlanner, assert_no_port_calls

async def main():
    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    store = SqliteStateStore(sys.argv[1])
    try:
        try:
            await resume('recoverable-run', state_store=store, data=data, content=content,
                         planner=planner, trace=JsonlTraceSink(sys.argv[2]))
        except StateStoreError as error:
            assert_no_port_calls(data, content, planner)
            print(json.dumps({'code': error.code}))
        else:
            raise AssertionError('changed runtime unexpectedly resumed')
    finally:
        await store.aclose()
asyncio.run(main())
"""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        program,
        str(state_path),
        str(trace_root),
        str(isolated_root),
        str(Path(__file__).parent),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(child.communicate(), timeout=10)
        assert child.returncode == 0, stderr.decode()
        assert json.loads(stdout) == {"code": "recovery_version_mismatch"}
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
