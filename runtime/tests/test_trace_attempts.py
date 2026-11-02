"""Recovery segments preserve stable run ownership without overwriting old audit logs."""

import json
from pathlib import Path

import pytest

from lake_agent.models import TraceEvent
from lake_agent.trace import JsonlTraceSink


def read_run_id(path):
    """Read a closed audit segment outside asynchronous filesystem code."""
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
