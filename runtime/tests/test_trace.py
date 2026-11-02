"""Exercise durable trace ordering, exclusive ownership, and cancellation handling."""

import asyncio
import json
import threading
from datetime import UTC, datetime

import pytest

from lake_agent.models import TraceEvent
from lake_agent.trace import JsonlTraceSink


def event(seq: int = 1, **changes: object) -> TraceEvent:
    values = {
        "run_id": "run-1",
        "task_id": "task-1",
        "seq": seq,
        "event_type": "run.started",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        **changes,
    }
    return TraceEvent(**values)


async def test_artifact_is_committed_before_referencing_event(tmp_path):
    sink = JsonlTraceSink(tmp_path)
    await sink.create_run("run-1")
    await sink.put_artifact("evidence-1", {"text": "evidence", "numbers": [1, 2]})
    await sink.append(event(data_ref="evidence-1"))

    trace_ref = await sink.close()

    assert trace_ref == str(tmp_path / "run-1" / "events.jsonl")
    assert json.loads((tmp_path / "run-1" / "artifacts" / "evidence-1.json").read_text()) == {
        "text": "evidence",
        "numbers": [1, 2],
    }
    rows = [
        json.loads(line) for line in (tmp_path / "run-1" / "events.jsonl").read_text().splitlines()
    ]
    assert rows[0]["data_ref"] == "evidence-1"
    assert rows[0]["seq"] == 1
    assert await sink.close() == trace_ref


async def test_duplicate_run_creation_is_atomic_and_preserves_original(tmp_path):
    first, second = JsonlTraceSink(tmp_path), JsonlTraceSink(tmp_path)
    outcomes = await asyncio.gather(
        first.create_run("run-1"), second.create_run("run-1"), return_exceptions=True
    )
    assert sum(value is None for value in outcomes) == 1
    assert sum(isinstance(value, FileExistsError) for value in outcomes) == 1
    winner = first if outcomes[0] is None else second
    await winner.append(event())
    await winner.close()
    assert len((tmp_path / "run-1" / "events.jsonl").read_text().splitlines()) == 1


@pytest.mark.parametrize("unsafe", ["../escape", "a/b", ".", "..", "", "/tmp/run", "a\\b"])
async def test_run_ids_cannot_escape_storage_root(tmp_path, unsafe):
    with pytest.raises(ValueError, match="safe"):
        await JsonlTraceSink(tmp_path).create_run(unsafe)


async def test_rejects_invalid_event_order_owner_and_uncommitted_reference(tmp_path):
    sink = JsonlTraceSink(tmp_path)
    await sink.create_run("run-1")
    with pytest.raises(ValueError, match="sequence"):
        await sink.append(event(2))
    with pytest.raises(ValueError, match="run_id"):
        await sink.append(event(run_id="other"))
    with pytest.raises(ValueError, match="artifact"):
        await sink.append(event(data_ref="not-written"))
    await sink.append(event())
    with pytest.raises(ValueError, match="sequence"):
        await sink.append(event())
    await sink.append(event(2))
    await sink.close()


async def test_artifact_ownership_is_exclusive_and_invalid_values_leave_no_file(tmp_path):
    sink = JsonlTraceSink(tmp_path)
    await sink.create_run("run-1")
    with pytest.raises(ValueError, match="safe"):
        await sink.put_artifact("../escape", {})
    with pytest.raises(ValueError):
        await sink.put_artifact("invalid", {"value": float("nan")})
    await sink.put_artifact("evidence-1", {"value": "original"})
    with pytest.raises(FileExistsError):
        await sink.put_artifact("evidence-1", {"value": "replacement"})
    assert json.loads((tmp_path / "run-1" / "artifacts" / "evidence-1.json").read_text()) == {
        "value": "original"
    }
    assert not (tmp_path / "run-1" / "artifacts" / "invalid.json").exists()
    await sink.close()
    with pytest.raises(RuntimeError, match="closed"):
        await sink.append(event())
    with pytest.raises(RuntimeError, match="closed"):
        await sink.put_artifact("later", {})


async def test_operations_require_created_run_and_instance_is_run_owned(tmp_path):
    sink = JsonlTraceSink(tmp_path)
    with pytest.raises(RuntimeError, match="create_run"):
        await sink.append(event())
    with pytest.raises(RuntimeError, match="create_run"):
        await sink.close()
    await sink.create_run("run-1")
    with pytest.raises(RuntimeError, match="already"):
        await sink.create_run("run-2")
    await sink.close()


async def test_failed_event_commit_prevents_accidental_duplicate_append(tmp_path, monkeypatch):
    sink = JsonlTraceSink(tmp_path)
    await sink.create_run("run-1")
    original = __import__("os").fsync

    def fail_fsync(fd):
        raise OSError("simulated device failure")

    monkeypatch.setattr("lake_agent.trace.os.fsync", fail_fsync)
    with pytest.raises(OSError, match="device failure"):
        await sink.append(event())
    monkeypatch.setattr("lake_agent.trace.os.fsync", original)
    # Bytes may already be visible even though durable commit failed. Retrying
    # must not produce a second event with the same sequence number.
    with pytest.raises(RuntimeError, match="failed"):
        await sink.append(event())
    assert len((tmp_path / "run-1" / "events.jsonl").read_text().splitlines()) == 1
    await sink.close()


async def test_cancelled_create_propagates_promptly_and_leaves_closeable_sink(
    tmp_path, monkeypatch
):
    sink = JsonlTraceSink(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = sink._create_run_sync

    def slow_create(run_id):
        entered.set()
        assert release.wait(2)
        original(run_id)

    monkeypatch.setattr(sink, "_create_run_sync", slow_create)
    task = asyncio.create_task(sink.create_run("run-1"))
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)
    release.set()
    # Disk work already running in a thread cannot be forcibly stopped. The
    # storage lock guarantees that a later operation waits for its commit.
    await sink.append(event())
    assert await sink.close() == str(tmp_path / "run-1" / "events.jsonl")


async def test_cancelled_append_does_not_overlap_another_disk_operation(tmp_path, monkeypatch):
    sink = JsonlTraceSink(tmp_path)
    await sink.create_run("run-1")
    entered, release = threading.Event(), threading.Event()
    original = sink._append_sync

    def slow_append(item):
        entered.set()
        assert release.wait(2)
        original(item)

    monkeypatch.setattr(sink, "_append_sync", slow_append)
    task = asyncio.create_task(sink.append(event()))
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)
    closing = asyncio.create_task(sink.close())
    await asyncio.sleep(0)
    assert not closing.done()
    release.set()
    await closing
    rows = (tmp_path / "run-1" / "events.jsonl").read_text().splitlines()
    assert len(rows) == 1
