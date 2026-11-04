"""JSONL 审计工件、运行标识和独立执行尝试。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime
from pathlib import Path

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
    # 即使持久化提交失败，字节也可能已经可见。重试时
    # 不得产生具有相同序列号的第二个事件。
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
    # 线程中已经开始的磁盘操作不能强制停止。
    # 存储锁保证后续操作等待该次提交完成。
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


def read_run_id(path):
    """在异步文件系统代码之外读取已关闭的审计分段。"""
    return json.loads(Path(path).read_text())["run_id"]


async def test_two_attempts_keep_independent_sequences_and_same_run(tmp_path):
    paths = []
    for attempt in ("attempt-1", "attempt-2"):
        trace = JsonlTraceSink(tmp_path)
        await trace.create_attempt("stable-run", attempt)
        await trace.append(
            TraceEvent(
                run_id="stable-run",
                task_id="task",
                seq=1,
                event_type="run.started",
                timestamp_utc="2026-11-01T00:00:00Z",
            )
        )
        paths.append(await trace.close())
    assert paths[0] != paths[1]
    assert all(read_run_id(p) == "stable-run" for p in paths)
    duplicate = JsonlTraceSink(tmp_path)
    with pytest.raises(FileExistsError):
        await duplicate.create_attempt("stable-run", "attempt-1")


async def test_attempt_identity_does_not_allow_path_traversal(tmp_path):
    trace = JsonlTraceSink(tmp_path)
    with pytest.raises(ValueError):
        await trace.create_attempt("run", "../escaped")
    assert not list(tmp_path.iterdir())
