"""面向单台本地 POSIX 主机的可选持久化运行状态及调用账本。

采用 FULL 同步的 SQLite WAL 事务保护已提交快照。相邻私有目录为每次运行保存一个
咨询锁 ``flock`` 文件；租约由打开的描述符持有，不依赖时间戳。进程退出会释放锁，
新所有者会替换持久化随机令牌。这些是本地文件系统锁，不是分布式租约，
也不提供网络文件系统上的所有权保证。

调用方负责存储生命周期，必须调用 ``aclose``。所有文件系统工作在线程中执行。
取消须等待已派发线程结束后才传播，确保已取消的写入不会修改后续所有者的状态。
获取租约时若被取消，也会释放尚未返回给调用方的租约。
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
    """提供稳定错误码及固定消息，不暴露存储数据或路径。"""

    def __init__(self, code: str):
        self.code = code
        super().__init__(_MESSAGES.get(code, "The state store operation failed."))


@dataclass(frozen=True)
class StateLease:
    """不透明的所有权凭证；之前获取租约时的令牌不能复用。"""

    run_id: str
    token: str


@dataclass
class StoredRun:
    """与存储对象分离的 JSON 快照，调用方可安全地在内存中修改。"""

    state: dict
    calls: dict[str, dict]


class StateStore(Protocol):
    """最小持久化边界；实现只负责存储，不负责执行。"""

    async def create(self, run_id: str, state: dict) -> StateLease: ...
    async def acquire(self, run_id: str) -> StateLease: ...
    async def load(self, lease: StateLease) -> StoredRun: ...
    async def save(self, lease: StateLease, state: dict) -> None: ...
    async def begin_call(self, lease: StateLease, call_id: str, value: dict) -> None: ...
    async def finish_call(self, lease: StateLease, call_id: str, value: dict) -> None: ...
    async def release(self, lease: StateLease) -> None: ...
    async def aclose(self) -> None: ...


def _identifier(value: str) -> str:
    """运行标识符会转换为摘要文件名，绝不作为调用方指定的路径使用。"""
    if not isinstance(value, str) or not value:
        raise StateStoreError("invalid_record")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise StateStoreError("invalid_record") from None
    return value


def _encode(value: dict) -> tuple[str, str]:
    """编码前拒绝 Python 的隐式转换，包括非字符串键、元组和集合。"""
    if type(value) is not dict:
        raise StateStoreError("invalid_json")
    pending: list[Any] = [value]
    visited: set[int] = set()
    while pending:
        item = pending.pop()
        kind = type(item)
        if kind in (dict, list):
            if id(item) in visited:
                continue  # 下面的 json.dumps 会检测真正的循环引用。
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
    """先校验持久化的确切字节，再解码为独立对象。"""
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
    """即使调用方反复取消，也等待受屏蔽保护的工作任务结束。

    仅屏蔽 ``to_thread`` 的取消并不充分：调用方可能已经返回，线程却仍持有描述符，
    或仍可能提交写入。必须保留任务、获取其结果，然后才传播取消。
    """
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break  # 在下面获取异常，同时保留取消。
    try:
        result = task.result()
    except BaseException:
        if cancelled:
            raise asyncio.CancelledError from None
        raise
    return result, cancelled


class SqliteStateStore:
    """由调用方管理的本地 SQLite 后端，每次运行使用一个非阻塞获取的租约。

    构造时不执行磁盘 I/O。每次操作均在工作线程中打开并关闭自己的 SQLite 连接。
    线程互斥锁串行保护实例生命周期变更和描述符登记，SQLite 负责跨实例事务串行化。
    锁文件永久保留：删除正在使用的锁文件，会让两个进程对同一运行锁定不同 inode。
    """

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().absolute()
        self._lock_dir = Path(str(self.path) + ".locks")
        self._mutex = threading.Lock()
        self._leases: dict[str, tuple[StateLease, int]] = {}
        self._released: set[StateLease] = set()
        self._closed = False

    async def create(self, run_id: str, state: dict) -> StateLease:
        """创建新运行并获取所有权，不覆盖已有运行。"""
        return await self._invoke(self._create, run_id, state, acquires=True)

    async def acquire(self, run_id: str) -> StateLease:
        """获取已有运行的所有权，并替换已退出所有者留下的失效令牌。"""
        return await self._invoke(self._acquire, run_id, acquires=True)

    async def load(self, lease: StateLease) -> StoredRun:
        """读取一致的状态和账本快照，并验证全部校验和。"""
        return await self._invoke(self._load, lease)

    async def save(self, lease: StateLease, state: dict) -> None:
        """原子替换状态对象，同时保留调用账本。"""
        await self._invoke(self._save, lease, state)

    async def begin_call(self, lease: StateLease, call_id: str, value: dict) -> None:
        """在派发副作用操作前插入不可变的 started 记录。"""
        await self._invoke(self._write_call, lease, call_id, value, False)

    async def finish_call(self, lease: StateLease, call_id: str, value: dict) -> None:
        """用完整且经过验证的响应替换 started 记录，且只允许替换一次。"""
        await self._invoke(self._write_call, lease, call_id, value, True)

    async def release(self, lease: StateLease) -> None:
        """释放所有权；重复释放同一个租约不会产生额外影响。"""
        await self._invoke(self._release, lease, allow_closed=True)

    async def delete(self, run_id: str) -> None:
        """维护操作：拒绝活动租约，在单事务中删除指定运行和调用账本。

        不属于执行端口协议；仅供拥有运行生命周期的宿主显式删除历史。
        保留锁文件 inode，使其他进程仍通过原有 flock 判断所有权。
        """
        await self._invoke(self._delete, run_id)

    async def aclose(self) -> None:
        """释放全部自有描述符，即使其中一次数据库更新失败也如此。"""
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
                # 调用方从未收到此所有权凭证。返回取消前先清理，
                # 即使反复取消也如此。
                try:
                    await _observe_thread(self._perform, self._release, (result,), True)
                except Exception:
                    pass  # _release 始终在 finally 中关闭描述符。
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
                # SQLite 或操作系统错误可能包含路径、SQL 或已存储值。
                raise StateStoreError("storage_error") from None

    @staticmethod
    def _private_directory(path: Path) -> None:
        """以私有权限创建缺失的父目录，并拒绝符号链接形式的锁目录。"""
        if not path.exists():
            if not path.parent.exists():
                SqliteStateStore._private_directory(path.parent)
            try:
                path.mkdir(mode=0o700)
            except FileExistsError:
                pass  # 协作的其他实例可能已并发创建该目录。
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise StateStoreError("storage_error")

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """在修改 pragma、权限或现有表之前先校验数据库结构。"""
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
                # 非空且无版本号的数据库属于其他应用。
                if connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1"
                ).fetchone():
                    # 这些读取分别自动提交：另一个初始化器可能在两次读取之间
                    # 已提交表和版本号。若其结构现已是当前版本，
                    # 则接受该结构，避免误报为
                    # 其他应用的无版本数据库。
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    if version != _SCHEMA_VERSION:
                        raise StateStoreError("schema_mismatch")
            if version == 0:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    # 获取写锁后再次检查：等待期间，其他实例可能
                    # 已完成空数据库的初始化。
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
                # 名称仅来自固定的本地数据库结构常量。
                columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
                if columns != expected:
                    raise StateStoreError("schema_mismatch")
            os.chmod(self.path, 0o600)
            # 在事务外设置 WAL。旁路文件权限继承数据库已有的私有权限，
            # 避免修改进程级 umask。
            # 首次并发打开时，日志模式切换可能立即返回 SQLITE_BUSY，
            # 而不遵守 SQLite 的忙等待超时；仅在同一个有界五秒窗口内
            # 重试这一幂等 pragma。
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
        """在同一快照中完成令牌验证和相应变更。"""
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
        # 先初始化，再打开相邻锁文件。数据库事务成功提交前，
        # 不登记任何锁所有权。
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

    def _delete(self, run_id: str) -> None:
        run_id = _identifier(run_id)
        with self._connection():
            pass  # 与 acquire 相同，须先建立数据库和锁目录。
        descriptor = self._lock_run(run_id)
        try:
            with self._transaction() as connection:
                # 显式按外键顺序删除；事务失败时两张表一起回滚。
                connection.execute("DELETE FROM call_records WHERE run_id=?", (run_id,))
                connection.execute("DELETE FROM run_states WHERE run_id=?", (run_id,))
        finally:
            self._unlock(descriptor)

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
            # 即使存储损坏或不可用，也不能泄漏操作系统锁的所有权。
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
