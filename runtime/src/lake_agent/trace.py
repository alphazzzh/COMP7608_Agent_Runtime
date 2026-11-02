"""Durable per-run JSONL audit storage with asynchronous filesystem dispatch.

Each sink instance owns one newly created run directory or execution-attempt
segment. Events are committed before ``append`` returns; artifacts are persisted
before their references can appear in events. Recovery segments retain the
original run identity with independent event sequences and artifact storage;
the log itself is an audit trail, not a resumable checkpoint.

Filesystem calls run in the event loop's worker pool. Cancelling an await stops
waiting promptly, but Python cannot interrupt an already running filesystem
syscall. A thread lock serializes such residual work with later cleanup. No
long-lived file descriptor or background asyncio writer task is retained.
"""

from __future__ import annotations

import asyncio
import os
import re
import threading
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from .models import JsonValue, TraceEvent
from .validation import canonical_json

_T = TypeVar("_T")
_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}\Z")


def _safe_component(value: str, label: str) -> str:
    """Reject path traversal instead of silently rewriting opaque IDs."""
    if not isinstance(value, str) or not _SAFE_COMPONENT.fullmatch(value):
        raise ValueError(f"{label} must be a safe opaque filename component")
    return value


def _sync_directory(path: Path) -> None:
    """Persist directory entries on local filesystems that implement fsync."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class JsonlTraceSink:
    """Write an exclusive run or attempt without overwriting prior audit data.

    Create a new instance per run or execution attempt. ``create_run`` reserves
    ``root/run_id`` exclusively; ``create_attempt`` reserves the leaf under
    ``root/run_id/attempts/attempt_id`` exclusively. Both use the original run
    ID for event ownership. Artifact, run and attempt IDs must be safe ASCII
    filename components of at most 200 characters. The configured root is
    trusted storage and must not be mutated externally while a sink is active.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self._lock = threading.Lock()
        self._run_id: str | None = None
        self._run_dir: Path | None = None
        self._events_path: Path | None = None
        self._artifacts_dir: Path | None = None
        self._last_seq = 0
        self._artifacts: set[str] = set()
        self._closed = False
        self._write_failed = False

    async def create_run(self, run_id: str) -> None:
        """Reserve a fresh run directory; an existing ID fails atomically."""
        _safe_component(run_id, "run_id")
        await asyncio.to_thread(self._locked, self._create_run_sync, run_id)

    async def create_attempt(self, run_id: str, attempt_id: str) -> None:
        """Reserve a fresh audit segment while keeping the stable run owner.

        Validate both IDs before creating any directory. Previous attempts are
        never opened for writing; every segment starts at sequence one and has
        its own immutable artifacts, just like an ordinary fresh-run sink.
        """
        _safe_component(run_id, "run_id")
        _safe_component(attempt_id, "attempt_id")
        await asyncio.to_thread(self._locked, self._create_attempt_sync, run_id, attempt_id)

    async def append(self, event: TraceEvent) -> None:
        """Commit one event after checking run ownership and sequence."""
        await asyncio.to_thread(self._locked, self._append_sync, event)

    async def put_artifact(self, ref: str, json_value: JsonValue) -> None:
        """Persist an immutable JSON artifact before any referencing event."""
        _safe_component(ref, "artifact ref")
        # Validate and serialize before touching disk: invalid JSON must never
        # leave a partial or reserved artifact file. This also freezes mutable
        # caller-owned input for the lifetime of the queued disk operation.
        encoded = canonical_json(json_value)
        await asyncio.to_thread(self._locked, self._put_artifact_sync, ref, encoded)

    async def close(self) -> str:
        """Finish the audit log and return its absolute path; safe to repeat."""
        return await asyncio.to_thread(self._locked, self._close_sync)

    def _locked(self, operation: Callable[..., _T], *args: object) -> _T:
        # An asyncio.Lock cannot protect residual disk operations after their
        # await was cancelled. Hold the lock inside the worker instead.
        with self._lock:
            return operation(*args)

    def _create_run_sync(self, run_id: str) -> None:
        if self._run_id is not None:
            raise RuntimeError("sink already owns a run; create a new sink instance")
        self.root.mkdir(parents=True, exist_ok=True)
        run_dir = self.root / run_id
        # mkdir is the cross-instance/process exclusive ownership primitive. A
        # failed initialization keeps the reservation for forensic inspection.
        run_dir.mkdir(mode=0o700, exist_ok=False)
        self._initialize_directory(run_id, run_dir, (self.root,))

    def _create_attempt_sync(self, run_id: str, attempt_id: str) -> None:
        if self._run_id is not None:
            raise RuntimeError("sink already owns a run; create a new sink instance")
        self.root.mkdir(parents=True, exist_ok=True)
        owner_dir = self.root / run_id
        owner_dir.mkdir(mode=0o700, exist_ok=True)
        attempts_dir = owner_dir / "attempts"
        attempts_dir.mkdir(mode=0o700, exist_ok=True)
        attempt_dir = attempts_dir / attempt_id
        # Parent directories may already contain prior records. Only the leaf
        # is reserved exclusively; no previous log or artifact is truncated.
        attempt_dir.mkdir(mode=0o700, exist_ok=False)
        self._initialize_directory(run_id, attempt_dir, (attempts_dir, owner_dir, self.root))

    def _initialize_directory(
        self, run_id: str, run_dir: Path, parent_dirs: tuple[Path, ...]
    ) -> None:
        """Commit a newly reserved log and its ancestors before exposing it."""
        artifact_dir = run_dir / "artifacts"
        artifact_dir.mkdir(mode=0o700)
        events_path = run_dir / "events.jsonl"
        with events_path.open("xb") as output:
            output.flush()
            os.fsync(output.fileno())
        _sync_directory(artifact_dir)
        _sync_directory(run_dir)
        for parent_dir in parent_dirs:
            _sync_directory(parent_dir)
        self._run_id = run_id
        self._run_dir = run_dir
        self._events_path = events_path
        self._artifacts_dir = artifact_dir

    def _require_run(self, *, writable: bool = True) -> None:
        if self._run_id is None:
            raise RuntimeError("create_run must succeed before using the sink")
        if writable and self._closed:
            raise RuntimeError("trace sink is closed")
        if writable and self._write_failed:
            raise RuntimeError("trace sink has failed; further writes are unsafe")

    def _append_sync(self, event: TraceEvent) -> None:
        self._require_run()
        if event.run_id != self._run_id:
            raise ValueError("event run_id does not match this sink")
        if event.seq != self._last_seq + 1:
            raise ValueError("event sequence must advance by one, starting at 1")
        if event.data_ref is not None and event.data_ref not in self._artifacts:
            raise ValueError("event references an artifact that has not been committed")
        encoded = canonical_json(event.model_dump(mode="json")) + b"\n"
        assert self._events_path is not None
        try:
            with self._events_path.open("ab") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
        except OSError:
            # A write/fsync error has an uncertain commit outcome. Refuse all
            # subsequent writes rather than risk duplicate/partial JSONL rows.
            self._write_failed = True
            raise
        self._last_seq = event.seq

    def _put_artifact_sync(self, ref: str, encoded: bytes) -> None:
        self._require_run()
        assert self._artifacts_dir is not None
        destination = self._artifacts_dir / f"{ref}.json"
        # The exclusive open leaves pre-existing bytes untouched. All sink
        # operations hold the storage lock, so no event can observe a partial
        # artifact. Commit the directory entry before marking the ref available.
        try:
            with destination.open("xb") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            _sync_directory(self._artifacts_dir)
        except FileExistsError:
            # This is a safe validation failure: the existing artifact was
            # untouched, so other new artifacts and events remain writable.
            raise
        except OSError:
            self._write_failed = True
            raise
        self._artifacts.add(ref)

    def _close_sync(self) -> str:
        self._require_run(writable=False)
        assert self._events_path is not None
        if not self._closed:
            # Each append already fsyncs and closes its descriptor; fsync here
            # also verifies that the log is still accessible during finalization.
            with self._events_path.open("rb") as output:
                os.fsync(output.fileno())
            self._closed = True
        return str(self._events_path)
