"""Optional durable run state and call ledger for a single local POSIX host.

SQLite WAL transactions with FULL synchronization protect committed snapshots.
An adjacent private directory contains one advisory ``flock`` file per run;
the open descriptor, rather than a timestamp, owns the lease. Process exit
releases that lock, and a new owner replaces the persisted random token. These
are local filesystem locks, not distributed leases or network-filesystem claims.

The caller owns this store's lifecycle and must call ``aclose``. All filesystem
work runs in threads. Cancellation waits for the dispatched thread to finish
before propagating, so a cancelled write cannot mutate a later owner's state.
A cancelled acquisition also releases its otherwise unreturned lease.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import stat
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

_SCHEMA_VERSION = 1
_SCHEMA_COLUMNS = {
    "run_states": ["run_id", "state_json", "state_hash", "lease_token"],
    "call_records": ["run_id", "call_id", "value_json", "value_hash", "status"],
}
_T = TypeVar("_T")
_MESSAGES = {
    "run_exists": "The run already exists.",
    "run_not_found": "The run does not exist.",
    "lease_conflict": "The run is already owned by another lease.",
    "lease_invalid": "The lease is invalid or no longer owns the run.",
    "call_exists": "The call already exists.",
    "call_not_found": "The call has no started record.",
    "call_completed": "The call has already completed.",
    "invalid_json": "The value must be a finite JSON object.",
    "invalid_record": "The identifier or call status is invalid.",
    "schema_mismatch": "The state database schema is unsupported.",
    "corrupt_state": "Stored state failed its integrity checks.",
    "store_closed": "The state store is closed.",
    "storage_error": "The local state store could not complete the operation.",
}


class StateStoreError(Exception):
    """Stable error code with a fixed message that excludes stored data and paths."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(_MESSAGES.get(code, "The state store operation failed."))


@dataclass(frozen=True)
class StateLease:
    """Opaque ownership proof; a previous acquisition's token cannot be reused."""

    run_id: str
    token: str


@dataclass
class StoredRun:
    """Detached JSON snapshots that callers may safely mutate in memory."""

    state: dict
    calls: dict[str, dict]


class StateStore(Protocol):
    """Minimal persistence boundary; implementations own storage, not execution."""

    async def create(self, run_id: str, state: dict) -> StateLease: ...
    async def acquire(self, run_id: str) -> StateLease: ...
    async def load(self, lease: StateLease) -> StoredRun: ...
    async def save(self, lease: StateLease, state: dict) -> None: ...
    async def begin_call(self, lease: StateLease, call_id: str, value: dict) -> None: ...
    async def finish_call(self, lease: StateLease, call_id: str, value: dict) -> None: ...
    async def release(self, lease: StateLease) -> None: ...
    async def aclose(self) -> None: ...


def _identifier(value: str) -> str:
    """Run identifiers become digest filenames, never caller-selected paths."""
    if not isinstance(value, str) or not value:
        raise StateStoreError("invalid_record")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise StateStoreError("invalid_record") from None
    return value


def _encode(value: dict) -> tuple[str, str]:
    """Reject Python coercions (non-string keys, tuples, sets) before encoding."""
    if type(value) is not dict:
        raise StateStoreError("invalid_json")
    pending: list[Any] = [value]
    visited: set[int] = set()
    while pending:
        item = pending.pop()
        kind = type(item)
        if kind in (dict, list):
            if id(item) in visited:
                continue  # json.dumps below detects actual circular references.
            visited.add(id(item))
            if kind is dict:
                if any(type(key) is not str for key in item):
                    raise StateStoreError("invalid_json")
                pending.extend(item.values())
            else:
                pending.extend(item)
        elif kind is float:
            if not math.isfinite(item):
                raise StateStoreError("invalid_json")
        elif kind not in (str, bool, int, type(None)):
            raise StateStoreError("invalid_json")
    try:
        text = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise StateStoreError("invalid_json") from None
    return text, digest


def _decode(text: str, digest: str) -> dict:
    """Check the exact persisted bytes before decoding a detached object."""
    try:
        actual = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(actual, digest):
            raise ValueError
        value = json.loads(text)
        canonical, _ = _encode(value)
        if canonical != text:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, UnicodeError, RecursionError, StateStoreError):
        raise StateStoreError("corrupt_state") from None


async def _observe_thread(function: Callable[..., _T], *args: Any) -> tuple[_T, bool]:
    """Drain a shielded worker even under repeated caller cancellation.

    Merely shielding ``to_thread`` is insufficient: it lets its caller return
    while the worker still holds a descriptor or may still commit a write.
    Retain the task, observe its result, and only then propagate cancellation.
    """
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break  # Retrieve the exception below, without losing cancellation.
    try:
        result = task.result()
    except BaseException:
        if cancelled:
            raise asyncio.CancelledError from None
        raise
    return result, cancelled


class SqliteStateStore:
    """Caller-managed local SQLite backend with one nonblocking lease per run.

    Construction performs no disk I/O. Each operation opens and closes its own
    SQLite connection in a worker. A thread mutex serializes instance lifecycle
    changes and descriptor bookkeeping; SQLite serializes database transactions
    across instances. Lock files stay in place permanently: unlinking a live
    lock file would allow two processes to lock different inodes for one run.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().absolute()
        self._lock_dir = Path(str(self.path) + ".locks")
        self._mutex = threading.Lock()
        self._leases: dict[str, tuple[StateLease, int]] = {}
        self._released: set[StateLease] = set()
        self._closed = False

    async def create(self, run_id: str, state: dict) -> StateLease:
        """Create a new run and acquire its ownership without overwriting a run."""
        return await self._invoke(self._create, run_id, state, acquires=True)

    async def acquire(self, run_id: str) -> StateLease:
        """Acquire an existing run, replacing a departed owner's stale token."""
        return await self._invoke(self._acquire, run_id, acquires=True)

    async def load(self, lease: StateLease) -> StoredRun:
        """Read one consistent state/ledger snapshot and verify every checksum."""
        return await self._invoke(self._load, lease)

    async def save(self, lease: StateLease, state: dict) -> None:
        """Atomically replace the state object while retaining the call ledger."""
        await self._invoke(self._save, lease, state)

    async def begin_call(self, lease: StateLease, call_id: str, value: dict) -> None:
        """Insert the immutable started record before dispatching a side effect."""
        await self._invoke(self._write_call, lease, call_id, value, False)

    async def finish_call(self, lease: StateLease, call_id: str, value: dict) -> None:
        """Replace a started record once with its complete, validated response."""
        await self._invoke(self._write_call, lease, call_id, value, True)

    async def release(self, lease: StateLease) -> None:
        """Release ownership; repeating a release of this lease is harmless."""
        await self._invoke(self._release, lease, allow_closed=True)

    async def aclose(self) -> None:
        """Release all owned descriptors, even if one database update fails."""
        await self._invoke(self._close, allow_closed=True)

    async def _invoke(
        self,
        action: Callable[..., _T],
        *args: Any,
        acquires: bool = False,
        allow_closed: bool = False,
    ) -> _T:
        result, cancelled = await _observe_thread(self._perform, action, args, allow_closed)
        if cancelled:
            if acquires:
                # The caller never received this ownership proof. Clean it up
                # before returning cancellation, including repeated cancels.
                try:
                    await _observe_thread(self._perform, self._release, (result,), True)
                except Exception:
                    pass  # _release always closes the descriptor in its finally.
            raise asyncio.CancelledError
        return result

    def _perform(self, action: Callable[..., _T], args: tuple, allow_closed: bool) -> _T:
        with self._mutex:
            if self._closed and not allow_closed:
                raise StateStoreError("store_closed")
            try:
                return action(*args)
            except StateStoreError:
                raise
            except (sqlite3.Error, OSError):
                # SQLite/OS errors may include paths, SQL or stored values.
                raise StateStoreError("storage_error") from None

    @staticmethod
    def _private_directory(path: Path) -> None:
        """Create missing parents privately, and refuse symlink lock directories."""
        if not path.exists():
            if not path.parent.exists():
                SqliteStateStore._private_directory(path.parent)
            try:
                path.mkdir(mode=0o700)
            except FileExistsError:
                pass  # A cooperating instance may have created it concurrently.
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise StateStoreError("storage_error")

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Gate schema before changing pragmas, permissions, or existing tables."""
        self._private_directory(self.path.parent)
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        descriptor = os.open(self.path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise StateStoreError("storage_error")
        finally:
            os.close(descriptor)
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, _SCHEMA_VERSION):
                raise StateStoreError("schema_mismatch")
            if version == 0:
                # An unversioned nonempty database belongs to someone else.
                if connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone():
                    # These are separate autocommit reads: another initializer
                    # may have committed both tables and the version between
                    # them. Accept its now-current schema instead of reporting
                    # an unversioned foreign database.
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    if version != _SCHEMA_VERSION:
                        raise StateStoreError("schema_mismatch")
            if version == 0:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    # Recheck after the write lock: another instance may have
                    # initialized the empty database while we were waiting.
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    if version == 0:
                        if connection.execute(
                            "SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
                        ).fetchone():
                            raise StateStoreError("schema_mismatch")
                        connection.execute(
                            "CREATE TABLE run_states (run_id TEXT PRIMARY KEY, "
                            "state_json TEXT NOT NULL, state_hash TEXT NOT NULL, lease_token TEXT)"
                        )
                        connection.execute(
                            "CREATE TABLE call_records (run_id TEXT NOT NULL, call_id TEXT NOT NULL, "
                            "value_json TEXT NOT NULL, value_hash TEXT NOT NULL, status TEXT NOT NULL "
                            "CHECK(status IN ('started','completed')), PRIMARY KEY(run_id,call_id), "
                            "FOREIGN KEY(run_id) REFERENCES run_states(run_id))"
                        )
                        connection.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
                    elif version != _SCHEMA_VERSION:
                        raise StateStoreError("schema_mismatch")
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if tables != _SCHEMA_COLUMNS.keys():
                raise StateStoreError("schema_mismatch")
            for table, expected in _SCHEMA_COLUMNS.items():
                # Names come exclusively from the fixed local schema constant.
                columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
                if columns != expected:
                    raise StateStoreError("schema_mismatch")
            os.chmod(self.path, 0o600)
            # Set WAL outside a transaction. Sidecar permissions inherit the
            # already-private database mode, avoiding process-global umask edits.
            # Simultaneous first-open journal transitions can return SQLITE_BUSY
            # immediately instead of honoring SQLite's busy timeout. Retry this
            # one idempotent pragma within the same bounded five-second window.
            deadline = time.monotonic() + 5
            while True:
                try:
                    mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                    break
                except sqlite3.OperationalError as error:
                    code = getattr(error, "sqlite_errorcode", 0) & 0xFF
                    if code not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                        raise
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)
            if mode.lower() != "wal":
                raise StateStoreError("storage_error")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            self._private_directory(self._lock_dir)
            os.chmod(self._lock_dir, 0o700)
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """Use a single snapshot for token validation and the associated change."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _lock_run(self, run_id: str) -> int:
        if run_id in self._leases:
            raise StateStoreError("lease_conflict")
        name = hashlib.sha256(run_id.encode("utf-8")).hexdigest() + ".lock"
        descriptor = os.open(
            self._lock_dir / name,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise StateStoreError("storage_error")
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise StateStoreError("lease_conflict") from None
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    @staticmethod
    def _unlock(descriptor: int) -> None:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _take_lease(self, run_id: str, state: dict | None) -> StateLease:
        run_id = _identifier(run_id)
        encoded = _encode(state) if state is not None else None
        # Initialize before opening the adjacent lock file. No lock ownership
        # is registered until the database transaction commits successfully.
        with self._connection():
            pass
        descriptor = self._lock_run(run_id)
        lease = StateLease(run_id, secrets.token_hex(32))
        try:
            with self._transaction() as connection:
                row = connection.execute(
                    "SELECT 1 FROM run_states WHERE run_id=?", (run_id,)
                ).fetchone()
                if encoded is not None:
                    if row:
                        raise StateStoreError("run_exists")
                    connection.execute(
                        "INSERT INTO run_states VALUES (?,?,?,?)",
                        (run_id, *encoded, lease.token),
                    )
                else:
                    if not row:
                        raise StateStoreError("run_not_found")
                    connection.execute(
                        "UPDATE run_states SET lease_token=? WHERE run_id=?",
                        (lease.token, run_id),
                    )
            self._leases[run_id] = (lease, descriptor)
            return lease
        except BaseException:
            self._unlock(descriptor)
            raise

    def _create(self, run_id: str, state: dict) -> StateLease:
        if state is None:
            raise StateStoreError("invalid_json")
        return self._take_lease(run_id, state)

    def _acquire(self, run_id: str) -> StateLease:
        return self._take_lease(run_id, None)

    def _owned(self, lease: StateLease) -> tuple[StateLease, int]:
        if (
            not isinstance(lease, StateLease)
            or type(lease.run_id) is not str
            or type(lease.token) is not str
        ):
            raise StateStoreError("lease_invalid")
        owned = self._leases.get(lease.run_id)
        if owned is None or owned[0] != lease:
            raise StateStoreError("lease_invalid")
        return owned

    def _verify(self, connection: sqlite3.Connection, lease: StateLease) -> None:
        self._owned(lease)
        row = connection.execute(
            "SELECT lease_token FROM run_states WHERE run_id=?", (lease.run_id,)
        ).fetchone()
        if row is None or row[0] != lease.token:
            raise StateStoreError("lease_invalid")

    def _load(self, lease: StateLease) -> StoredRun:
        self._owned(lease)
        with self._transaction() as connection:
            self._verify(connection, lease)
            row = connection.execute(
                "SELECT state_json,state_hash FROM run_states WHERE run_id=?", (lease.run_id,)
            ).fetchone()
            state = _decode(*row)
            calls = {}
            for call_id, text, digest, status in connection.execute(
                "SELECT call_id,value_json,value_hash,status FROM call_records WHERE run_id=?",
                (lease.run_id,),
            ):
                value = _decode(text, digest)
                if status not in ("started", "completed") or value.get("status") != status:
                    raise StateStoreError("corrupt_state")
                calls[call_id] = value
            return StoredRun(state, calls)

    def _save(self, lease: StateLease, state: dict) -> None:
        self._owned(lease)
        encoded = _encode(state)
        with self._transaction() as connection:
            self._verify(connection, lease)
            connection.execute(
                "UPDATE run_states SET state_json=?,state_hash=? WHERE run_id=?",
                (*encoded, lease.run_id),
            )

    def _write_call(self, lease: StateLease, call_id: str, value: dict, finish: bool) -> None:
        self._owned(lease)
        call_id = _identifier(call_id)
        encoded = _encode(value)
        status = "completed" if finish else "started"
        if value.get("status") != status:
            raise StateStoreError("invalid_record")
        with self._transaction() as connection:
            self._verify(connection, lease)
            row = connection.execute(
                "SELECT value_json,value_hash,status FROM call_records WHERE run_id=? AND call_id=?",
                (lease.run_id, call_id),
            ).fetchone()
            if finish:
                if row is None:
                    raise StateStoreError("call_not_found")
                previous = _decode(row[0], row[1])
                if previous.get("status") != row[2] or row[2] not in ("started", "completed"):
                    raise StateStoreError("corrupt_state")
                if row[2] == "completed":
                    raise StateStoreError("call_completed")
                connection.execute(
                    "UPDATE call_records SET value_json=?,value_hash=?,status=? "
                    "WHERE run_id=? AND call_id=?",
                    (*encoded, status, lease.run_id, call_id),
                )
            else:
                if row is not None:
                    raise StateStoreError("call_exists")
                connection.execute(
                    "INSERT INTO call_records VALUES (?,?,?,?,?)",
                    (lease.run_id, call_id, *encoded, status),
                )

    def _release(self, lease: StateLease) -> None:
        if (
            isinstance(lease, StateLease)
            and type(lease.run_id) is str
            and type(lease.token) is str
            and lease in self._released
        ):
            return
        _, descriptor = self._owned(lease)
        try:
            with self._transaction() as connection:
                self._verify(connection, lease)
                connection.execute(
                    "UPDATE run_states SET lease_token=NULL WHERE run_id=?", (lease.run_id,)
                )
        finally:
            # Even corrupted/unavailable storage must not leak OS ownership.
            self._leases.pop(lease.run_id, None)
            self._released.add(lease)
            self._unlock(descriptor)

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        first_error = None
        for lease, _ in list(self._leases.values()):
            try:
                self._release(lease)
            except (StateStoreError, sqlite3.Error, OSError) as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error
