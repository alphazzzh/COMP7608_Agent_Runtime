"""持续任务和触发器的本地 SQLite 持久化实现。

该模块只提供事务和生命周期边界，不导入 Web、运行时或适配器。每个异步方法把
短事务放入受取消保护的工作线程；SQLite WAL/FULL 同步和 ``BEGIN IMMEDIATE``
保证多个宿主实例竞争领取时只有一个实例能预留额度。数据库文件及父目录始终使用
私有权限，行级规范 JSON 摘要用于发现篡改或不完整写入。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import sqlite3
import stat
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, TypeVar

from ..validation import canonical_json
from .models import MissionCreate, MissionError, MissionHandoff, MissionRecord, TriggerRecord

_SCHEMA_VERSION = 1
_T = TypeVar("_T")
_MISSION_COLUMNS = (
    "mission_id",
    "title",
    "question",
    "profile_id",
    "profile_signature",
    "status",
    "reason",
    "created_at",
    "updated_at",
    "deadline_at",
    "next_wake_at",
    "interval_seconds",
    "stop_on_completed",
    "limits_json",
    "started_runs",
    "completed_runs",
    "used_model_calls",
    "used_data_calls",
    "reserved_model_calls",
    "reserved_data_calls",
    "active_trigger_id",
    "handoff_json",
    "row_hash",
)
_TRIGGER_COLUMNS = (
    "trigger_id",
    "mission_id",
    "event_id",
    "run_id",
    "status",
    "created_at",
    "updated_at",
    "cycle",
    "reserved_model_calls",
    "reserved_data_calls",
    "request_json",
    "handoff_json",
    "run_status",
    "stop_reason",
    "row_hash",
)
_INDEX_SQL = {
    "triggers_pending": "CREATE INDEX triggers_pending ON triggers(status,created_at)",
    "triggers_mission_status": "CREATE INDEX triggers_mission_status ON triggers(mission_id,status)",
    "triggers_one_pending": "CREATE UNIQUE INDEX triggers_one_pending ON triggers(mission_id) WHERE status='pending'",
    "triggers_one_active": "CREATE UNIQUE INDEX triggers_one_active ON triggers(mission_id) WHERE status IN ('claimed','running')",
}


def _canonical(value: Any) -> str:
    """编码有限 JSON；所有异常统一成存储错误而不暴露原始值。"""
    try:
        return canonical_json(value).decode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise MissionError("invalid_json", "载荷必须是有限 JSON。", 400) from None


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _copy(value: Any) -> Any:
    return json.loads(_canonical(value))


def _time(value: float | int | None) -> float:
    """获取有限 UTC epoch 秒；测试可显式注入整数时钟。"""
    if value is None:
        return time.time()
    try:
        valid = type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid:
        raise MissionError("invalid_time", "时间值无效。", 400)
    return float(value)


def _uuid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


async def _observe_thread(function: Callable[..., _T], *args: Any) -> _T:
    """等待线程任务完成后再传播取消，避免取消留下未完成事务。"""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    try:
        result = task.result()
    except BaseException:
        if cancelled:
            raise asyncio.CancelledError from None
        raise
    if cancelled:
        raise asyncio.CancelledError
    return result


class MissionStore:
    """可重启的 Mission/Trigger SQLite 存储，所有公开操作均为异步方法。"""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().absolute()
        self._mutex = threading.Lock()
        self._started = False
        self._closed = False

    async def start(self) -> None:
        """创建私有数据库并校验 schema；构造函数本身不执行磁盘 I/O。"""
        await _observe_thread(self._perform, self._initialize, (), True)

    async def aclose(self) -> None:
        """阻止后续操作；每次操作使用短连接，因此关闭时无资源泄漏。"""
        await _observe_thread(self._perform, self._close, (), True)

    async def create(
        self,
        payload: MissionCreate,
        *,
        profile_id: str | None,
        profile_signature: str,
        now: float | int | None = None,
    ) -> dict[str, Any]:
        """创建任务并冻结 profile 身份；立即运行身份在同一事务内持久化。"""
        payload = MissionCreate.model_validate(
            payload.model_dump(mode="python") if isinstance(payload, MissionCreate) else payload
        )
        if profile_id is not None and (
            type(profile_id) is not str or not profile_id.strip() or len(profile_id) > 100
        ):
            raise MissionError("invalid_profile", "配置身份无效。", 400)
        if (
            type(profile_signature) is not str
            or not profile_signature
            or len(profile_signature) > 512
        ):
            raise MissionError("invalid_profile", "配置签名无效。", 400)
        stamp = _time(now)
        return await self._invoke(
            self._create, payload, profile_id or payload.profile_id, profile_signature, stamp
        )

    async def get(self, mission_id: str) -> dict[str, Any]:
        """读取独立 Mission 副本；未知 ID 只返回固定 not_found。"""
        self._id(mission_id)
        return await self._invoke(self._get, mission_id)

    async def listing(self, limit: int = 100) -> list[dict[str, Any]]:
        """按更新时间倒序列出最多 ``limit`` 条 Mission。"""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise MissionError("invalid_limit", "列表数量无效。", 400)
        return await self._invoke(self._listing, limit)

    async def enqueue(
        self, mission_id: str, event_id: str, now: float | int | None = None
    ) -> dict[str, Any]:
        """以同 Mission 事件去重并持久化 trigger/run 身份。"""
        self._id(mission_id)
        self._event(event_id)
        return await self._invoke(self._enqueue, mission_id, event_id, _time(now))

    async def pending(self, limit: int = 100) -> list[dict[str, Any]]:
        """读取 active 且没有已领取周期的 Mission 待办，供调度器领取。"""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise MissionError("invalid_limit", "列表数量无效。", 400)
        return await self._invoke(self._pending, limit)

    async def open_triggers(self) -> list[dict[str, Any]]:
        """读取 claimed/running trigger，用于重启对账。"""
        return await self._invoke(self._open_triggers)

    async def triggers(self, mission_id: str, limit: int = 100) -> list[dict[str, Any]]:
        """按创建时间倒序读取指定任务的有界触发器历史。"""
        self._id(mission_id)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise MissionError("invalid_limit", "列表数量无效。", 400)
        return await self._invoke(self._triggers, mission_id, limit)

    async def detail(self, mission_id: str, limit: int = 100) -> dict[str, Any]:
        """Read the mission row and its trigger rows from one SQLite snapshot."""
        self._id(mission_id)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise MissionError("invalid_limit", "列表数量无效。", 400)
        return await self._invoke(self._detail, mission_id, limit)

    async def get_trigger(self, trigger_id: str) -> dict[str, Any]:
        self._id(trigger_id)
        return await self._invoke(self._get_trigger, trigger_id)

    async def claim(
        self,
        trigger_id: str,
        *,
        max_model_calls: int,
        max_data_calls: int,
        now: float | int | None = None,
    ) -> dict[str, Any] | None:
        """原子领取 pending trigger 并预留 Mission 与 profile 的较小额度。"""
        self._id(trigger_id)
        self._nonnegative(max_model_calls, "model_calls")
        self._nonnegative(max_data_calls, "data_calls")
        return await self._invoke(
            self._claim, trigger_id, max_model_calls, max_data_calls, _time(now)
        )

    async def mark_running(
        self, trigger_id: str, *, request: dict[str, Any], now: float | int | None = None
    ) -> dict[str, Any]:
        """冻结完整 RunRequest；相同请求重试幂等，不同请求拒绝。"""
        self._id(trigger_id)
        if type(request) is not dict:
            raise MissionError("invalid_request", "运行请求无效。", 400)
        encoded = _canonical(request)
        if len(encoded.encode("utf-8")) > 65_536:
            raise MissionError("invalid_request", "运行请求超过存储大小限制。", 400)
        return await self._invoke(self._mark_running, trigger_id, encoded, _time(now))

    async def settle(
        self,
        trigger_id: str,
        *,
        model_calls: int,
        data_calls: int,
        run_status: str,
        stop_reason: str,
        handoff: MissionHandoff,
        now: float | int | None = None,
    ) -> dict[str, Any]:
        """一次性核对真实调用、释放预留并更新 Mission 周期。"""
        self._id(trigger_id)
        self._nonnegative(model_calls, "model_calls")
        self._nonnegative(data_calls, "data_calls")
        if run_status not in {
            "completed",
            "insufficient_evidence",
            "budget_exceeded",
            "failed",
            "timeout",
        }:
            raise MissionError("invalid_run_status", "运行状态无效。", 400)
        if type(stop_reason) is not str or len(stop_reason) > 2000:
            raise MissionError("invalid_stop_reason", "停止原因无效。", 400)
        handoff = MissionHandoff.model_validate(
            handoff.model_dump(mode="python") if isinstance(handoff, MissionHandoff) else handoff
        )
        encoded_handoff = _canonical(handoff.model_dump(mode="json"))
        return await self._invoke(
            self._settle,
            trigger_id,
            model_calls,
            data_calls,
            run_status,
            stop_reason,
            encoded_handoff,
            _time(now),
        )

    async def block(
        self, trigger_id: str, code: str, now: float | int | None = None
    ) -> dict[str, Any]:
        """以保守策略阻塞任务，保留未确定调用的预留额度。"""
        self._id(trigger_id)
        if type(code) is not str or not code or len(code) > 200:
            raise MissionError("invalid_block_code", "阻塞原因无效。", 400)
        return await self._invoke(self._block, trigger_id, code, _time(now))

    async def set_status(
        self, mission_id: str, status: str, now: float | int | None = None
    ) -> dict[str, Any]:
        """暂停、恢复或结束任务；blocked 不能通过普通 active 恢复。"""
        self._id(mission_id)
        if status not in {"active", "paused", "completed"}:
            raise MissionError("invalid_status", "任务状态无效。", 400)
        return await self._invoke(self._set_status, mission_id, status, _time(now))

    async def schedule_due(self, now: float | int | None = None) -> int:
        """把到期 active Mission 各入队一次；遗漏周期合并而不补跑。"""
        return await self._invoke(self._schedule_due, _time(now))

    @staticmethod
    def _id(value: str) -> None:
        if type(value) is not str or not value or len(value) > 200:
            raise MissionError("invalid_id", "标识无效。", 400)

    @staticmethod
    def _event(value: str) -> None:
        if type(value) is not str or not value or len(value) > 200:
            raise MissionError("invalid_event", "事件标识无效。", 400)
        if value == "initial" or value.startswith(("system:", "schedule:")):
            raise MissionError("reserved_event", "事件标识使用了保留命名空间。", 400)

    @staticmethod
    def _nonnegative(value: int, label: str) -> None:
        if type(value) is not int or value < 0:
            raise MissionError("invalid_usage", f"{label} 用量无效。", 400)

    async def _invoke(self, action: Callable[..., _T], *args: Any) -> _T:
        return await _observe_thread(self._perform, action, args, False)

    def _perform(self, action: Callable[..., _T], args: tuple[Any, ...], lifecycle: bool) -> _T:
        with self._mutex:
            if self._closed and action != self._close:
                raise MissionError("store_closed", "任务存储已关闭。", 409)
            if not self._started and not lifecycle:
                raise MissionError("store_not_started", "任务存储尚未启动。", 409)
            try:
                return action(*args)
            except MissionError:
                raise
            except (sqlite3.Error, OSError, ValueError, UnicodeError, TypeError):
                raise MissionError("storage_error", "本地任务存储无法完成操作。", 500) from None

    @staticmethod
    def _private_parent(path: Path) -> None:
        """逐级以 0700 创建缺失父目录，不修改既有共享祖先目录权限。"""
        parent = path.parent
        if not parent.exists():
            if not parent.parent.exists():
                MissionStore._private_parent(parent)
            try:
                parent.mkdir(mode=0o700)
            except FileExistsError:
                pass
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise MissionError("storage_error", "本地任务存储无法完成操作。", 500)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        self._private_parent(self.path)
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise MissionError("storage_error", "本地任务存储无法完成操作。", 500)
        finally:
            os.close(descriptor)
        os.chmod(self.path, 0o600)
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=5000")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, _SCHEMA_VERSION):
                raise MissionError("schema_mismatch", "任务存储版本不受支持。", 500)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version == 0:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    # 获得写锁后重读，避免两个首次启动实例都初始化同一数据库。
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    tables = {
                        row[0]
                        for row in connection.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                        )
                    }
                    if version == 0 and tables:
                        raise MissionError("schema_mismatch", "任务存储版本不受支持。", 500)
                    if version not in (0, _SCHEMA_VERSION):
                        raise MissionError("schema_mismatch", "任务存储版本不受支持。", 500)
                    schema = """CREATE TABLE missions (
                            mission_id TEXT PRIMARY KEY, title TEXT NOT NULL, question TEXT NOT NULL,
                            profile_id TEXT, profile_signature TEXT NOT NULL, status TEXT NOT NULL,
                            reason TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                            deadline_at REAL NOT NULL, next_wake_at REAL, interval_seconds INTEGER,
                            stop_on_completed INTEGER NOT NULL, limits_json TEXT NOT NULL,
                            started_runs INTEGER NOT NULL, completed_runs INTEGER NOT NULL,
                            used_model_calls INTEGER NOT NULL, used_data_calls INTEGER NOT NULL,
                            reserved_model_calls INTEGER NOT NULL, reserved_data_calls INTEGER NOT NULL,
                            active_trigger_id TEXT, handoff_json TEXT NOT NULL, row_hash TEXT NOT NULL
                        );
                        CREATE TABLE triggers (
                            trigger_id TEXT PRIMARY KEY, mission_id TEXT NOT NULL REFERENCES missions(mission_id),
                            event_id TEXT NOT NULL, run_id TEXT NOT NULL, status TEXT NOT NULL,
                            created_at REAL NOT NULL, updated_at REAL NOT NULL, cycle INTEGER NOT NULL,
                            reserved_model_calls INTEGER NOT NULL, reserved_data_calls INTEGER NOT NULL,
                            request_json TEXT, handoff_json TEXT NOT NULL, run_status TEXT, stop_reason TEXT,
                            row_hash TEXT NOT NULL, UNIQUE(mission_id,event_id)
                        );
                        CREATE INDEX triggers_pending ON triggers(status,created_at);
                        CREATE INDEX triggers_mission_status ON triggers(mission_id,status);
                        CREATE UNIQUE INDEX triggers_one_pending ON triggers(mission_id) WHERE status='pending';
                        CREATE UNIQUE INDEX triggers_one_active ON triggers(mission_id) WHERE status IN ('claimed','running');
                        PRAGMA user_version=1;"""
                    if version == 0:
                        # executescript 会隐式提交，逐条 execute 保留初始化事务的原子性。
                        for statement in schema.split(";"):
                            if statement.strip():
                                connection.execute(statement)
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            elif tables != {"missions", "triggers"}:
                raise MissionError("schema_mismatch", "任务存储版本不受支持。", 500)
            for table, expected in (("missions", _MISSION_COLUMNS), ("triggers", _TRIGGER_COLUMNS)):
                columns = tuple(row[1] for row in connection.execute(f"PRAGMA table_info({table})"))
                if columns != expected:
                    raise MissionError("schema_mismatch", "任务存储版本不受支持。", 500)
            indexes = dict(
                connection.execute(
                    "SELECT name,sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
                )
            )

            def normalize(sql):
                return "".join(sql.lower().split())

            if indexes.keys() != _INDEX_SQL.keys() or any(
                normalize(indexes[name] or "") != normalize(expected)
                for name, expected in _INDEX_SQL.items()
            ):
                raise MissionError("schema_mismatch", "任务存储版本不受支持。", 500)
            # UNIQUE(mission_id,event_id) 是事件幂等边界，不能只核对普通列名。
            event_index = [
                row[2]
                for row in connection.execute("PRAGMA index_info('sqlite_autoindex_triggers_2')")
            ]
            if event_index != ["mission_id", "event_id"]:
                raise MissionError("schema_mismatch", "任务存储版本不受支持。", 500)
            deadline = time.monotonic() + 5
            while True:
                try:
                    mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                    break
                except sqlite3.OperationalError as error:
                    code = getattr(error, "sqlite_errorcode", 0) & 0xFF
                    if (
                        code not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
                        or time.monotonic() >= deadline
                    ):
                        raise
                    time.sleep(0.01)
            if str(mode).lower() != "wal":
                raise MissionError("storage_error", "本地任务存储无法完成操作。", 500)
            connection.execute("PRAGMA synchronous=FULL")
            yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        if self._started:
            return
        with self._connection():
            pass
        self._started = True

    def _close(self) -> None:
        """实例锁保证已派发操作结束后才切换关闭状态。"""
        self._closed = True

    @staticmethod
    def _mission_from_row(row: sqlite3.Row | tuple[Any, ...]) -> dict[str, Any]:
        try:
            values = dict(zip(_MISSION_COLUMNS, row, strict=True))
            if (
                values["stop_on_completed"] not in (0, 1)
                or type(values["stop_on_completed"]) is not int
            ):
                raise ValueError
            limits_value = json.loads(values["limits_json"])
            handoff_value = json.loads(values["handoff_json"])
            if (
                _canonical(limits_value) != values["limits_json"]
                or _canonical(handoff_value) != values["handoff_json"]
            ):
                raise ValueError
            record = {
                "mission_id": values["mission_id"],
                "title": values["title"],
                "question": values["question"],
                "profile_id": values["profile_id"],
                "profile_signature": values["profile_signature"],
                "status": values["status"],
                "reason": values["reason"],
                "created_at": float(values["created_at"]),
                "updated_at": float(values["updated_at"]),
                "deadline_at": float(values["deadline_at"]),
                "next_wake_at": None
                if values["next_wake_at"] is None
                else float(values["next_wake_at"]),
                "interval_seconds": values["interval_seconds"],
                "stop_on_completed": bool(values["stop_on_completed"]),
                "limits": limits_value,
                "started_runs": values["started_runs"],
                "completed_runs": values["completed_runs"],
                "used_model_calls": values["used_model_calls"],
                "used_data_calls": values["used_data_calls"],
                "reserved_model_calls": values["reserved_model_calls"],
                "reserved_data_calls": values["reserved_data_calls"],
                "active_trigger_id": values["active_trigger_id"],
                "handoff": handoff_value,
            }
            record = MissionRecord.model_validate(record).model_dump(mode="json")
            if _digest(record) != values["row_hash"]:
                raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500)
            return record
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, MissionError):
            raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500) from None

    @staticmethod
    def _trigger_from_row(row: sqlite3.Row | tuple[Any, ...]) -> dict[str, Any]:
        try:
            values = dict(zip(_TRIGGER_COLUMNS, row, strict=True))
            handoff_value = json.loads(values["handoff_json"])
            request_value = (
                None if values["request_json"] is None else json.loads(values["request_json"])
            )
            if _canonical(handoff_value) != values["handoff_json"] or (
                request_value is not None and _canonical(request_value) != values["request_json"]
            ):
                raise ValueError
            record = {
                "trigger_id": values["trigger_id"],
                "mission_id": values["mission_id"],
                "event_id": values["event_id"],
                "run_id": values["run_id"],
                "status": values["status"],
                "created_at": float(values["created_at"]),
                "updated_at": float(values["updated_at"]),
                "cycle": values["cycle"],
                "reserved_model_calls": values["reserved_model_calls"],
                "reserved_data_calls": values["reserved_data_calls"],
                "request": request_value,
                "handoff": handoff_value,
                "run_status": values["run_status"],
                "stop_reason": values["stop_reason"],
            }
            record = TriggerRecord.model_validate(record).model_dump(mode="json")
            if _digest(record) != values["row_hash"]:
                raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500)
            return record
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, MissionError):
            raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500) from None

    @staticmethod
    def _mission_row(record: dict[str, Any]) -> tuple[Any, ...]:
        limits = _canonical(record["limits"])
        handoff = _canonical(record["handoff"])
        hashed = _digest(record)
        return (
            record["mission_id"],
            record["title"],
            record["question"],
            record["profile_id"],
            record["profile_signature"],
            record["status"],
            record["reason"],
            record["created_at"],
            record["updated_at"],
            record["deadline_at"],
            record["next_wake_at"],
            record["interval_seconds"],
            int(record["stop_on_completed"]),
            limits,
            record["started_runs"],
            record["completed_runs"],
            record["used_model_calls"],
            record["used_data_calls"],
            record["reserved_model_calls"],
            record["reserved_data_calls"],
            record["active_trigger_id"],
            handoff,
            hashed,
        )

    @staticmethod
    def _trigger_row(record: dict[str, Any]) -> tuple[Any, ...]:
        request = None if record["request"] is None else _canonical(record["request"])
        handoff = _canonical(record["handoff"])
        return (
            record["trigger_id"],
            record["mission_id"],
            record["event_id"],
            record["run_id"],
            record["status"],
            record["created_at"],
            record["updated_at"],
            record["cycle"],
            record["reserved_model_calls"],
            record["reserved_data_calls"],
            request,
            handoff,
            record["run_status"],
            record["stop_reason"],
            _digest(record),
        )

    def _save_mission(self, db: sqlite3.Connection, mission: dict[str, Any]) -> None:
        """总是同时写入完整记录与摘要，避免局部更新遗忘校验字段。"""
        values = self._mission_row(mission)
        assignments = ",".join(f"{column}=?" for column in _MISSION_COLUMNS[1:])
        db.execute(
            f"UPDATE missions SET {assignments} WHERE mission_id=?", (*values[1:], values[0])
        )

    def _save_trigger(self, db: sqlite3.Connection, trigger: dict[str, Any]) -> None:
        """固定列名来自本地 schema；全部可变值通过 SQL 参数传递。"""
        values = self._trigger_row(trigger)
        assignments = ",".join(f"{column}=?" for column in _TRIGGER_COLUMNS[1:])
        db.execute(
            f"UPDATE triggers SET {assignments} WHERE trigger_id=?", (*values[1:], values[0])
        )

    def _cancel_pending(
        self, db: sqlite3.Connection, mission_id: str, stamp: float, reason: str
    ) -> None:
        """结束任务时只取消未领取工作，已领取周期仍由结算边界收尾。"""
        rows = db.execute(
            "SELECT * FROM triggers WHERE mission_id=? AND status='pending'", (mission_id,)
        ).fetchall()
        for row in rows:
            trigger = self._trigger_from_row(row)
            trigger.update(status="cancelled", updated_at=stamp, stop_reason=reason)
            self._save_trigger(db, trigger)

    @staticmethod
    def _new_trigger(mission: dict[str, Any], event_id: str, stamp: float) -> dict[str, Any]:
        """触发器身份一经接纳便永久绑定；handoff 在真正领取时再次冻结。"""
        return {
            "trigger_id": _uuid("trigger"),
            "mission_id": mission["mission_id"],
            "event_id": event_id,
            "run_id": _uuid("run"),
            "status": "pending",
            "created_at": stamp,
            "updated_at": stamp,
            "cycle": 0,
            "reserved_model_calls": 0,
            "reserved_data_calls": 0,
            "request": None,
            "handoff": _copy(mission["handoff"]),
            "run_status": None,
            "stop_reason": None,
        }

    def _create(
        self, payload: MissionCreate, profile_id: str | None, profile_signature: str, stamp: float
    ) -> dict[str, Any]:
        mission_id = _uuid("mission")
        limits = payload.limits.model_dump(mode="json")
        record = {
            "mission_id": mission_id,
            "title": payload.title,
            "question": payload.question,
            "profile_id": profile_id,
            "profile_signature": profile_signature,
            "status": "active",
            "reason": None,
            "created_at": stamp,
            "updated_at": stamp,
            "deadline_at": stamp + limits["max_duration_seconds"],
            "next_wake_at": stamp + payload.interval_seconds
            if not payload.start_immediately and payload.interval_seconds is not None
            else None,
            "interval_seconds": payload.interval_seconds,
            "stop_on_completed": payload.stop_on_completed,
            "limits": limits,
            "started_runs": 0,
            "completed_runs": 0,
            "used_model_calls": 0,
            "used_data_calls": 0,
            "reserved_model_calls": 0,
            "reserved_data_calls": 0,
            "active_trigger_id": None,
            "handoff": MissionHandoff().model_dump(mode="json"),
        }
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "INSERT INTO missions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    self._mission_row(record),
                )
                if payload.start_immediately:
                    initial = self._new_trigger(record, "initial", stamp)
                    db.execute(
                        "INSERT INTO triggers VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        self._trigger_row(initial),
                    )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return _copy(record)

    def _get(self, mission_id: str) -> dict[str, Any]:
        with self._connection() as db:
            row = db.execute("SELECT * FROM missions WHERE mission_id=?", (mission_id,)).fetchone()
            if row is None:
                raise MissionError("not_found", "未找到该任务。", 404)
            return _copy(self._mission_from_row(row))

    def _listing(self, limit: int) -> list[dict[str, Any]]:
        with self._connection() as db:
            rows = db.execute(
                "SELECT * FROM missions ORDER BY updated_at DESC, mission_id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [_copy(self._mission_from_row(row)) for row in rows]

    def _enqueue(self, mission_id: str, event_id: str, stamp: float) -> dict[str, Any]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                mission_row = db.execute(
                    "SELECT * FROM missions WHERE mission_id=?", (mission_id,)
                ).fetchone()
                if mission_row is None:
                    raise MissionError("not_found", "未找到该任务。", 404)
                existing = db.execute(
                    "SELECT * FROM triggers WHERE mission_id=? AND event_id=?",
                    (mission_id, event_id),
                ).fetchone()
                if existing is not None:
                    db.commit()
                    return _copy(self._trigger_from_row(existing))
                mission = self._mission_from_row(mission_row)
                if mission["status"] in {"completed", "blocked"}:
                    raise MissionError("mission_closed", "该任务已结束。", 409)
                if stamp >= mission["deadline_at"]:
                    mission.update(
                        status="completed",
                        reason="deadline_reached",
                        updated_at=stamp,
                        next_wake_at=None,
                    )
                    self._save_mission(db, mission)
                    self._cancel_pending(db, mission_id, stamp, "deadline_reached")
                    db.commit()
                    raise MissionError("mission_closed", "该任务已到截止时间。", 409)
                pending_count = db.execute(
                    "SELECT COUNT(*) FROM triggers WHERE mission_id=? AND status='pending'",
                    (mission_id,),
                ).fetchone()[0]
                accepted_count = db.execute(
                    "SELECT COUNT(*) FROM triggers WHERE mission_id=?", (mission_id,)
                ).fetchone()[0]
                if pending_count or accepted_count >= mission["limits"]["max_runs"]:
                    raise MissionError("mission_queue_full", "该任务的待办容量已满。", 429)
                trigger = self._new_trigger(mission, event_id, stamp)
                db.execute(
                    "INSERT INTO triggers VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    self._trigger_row(trigger),
                )
                mission.update(next_wake_at=None, updated_at=stamp)
                self._save_mission(db, mission)
                db.commit()
                return _copy(trigger)
            except BaseException:
                db.rollback()
                raise

    def _pending(self, limit: int) -> list[dict[str, Any]]:
        with self._connection() as db:
            # 在 LIMIT 前排除暂停任务与已有 owner 的任务，避免前面的不可领取记录
            # 永久占满调度窗口。JOIN 同时取得完整 Mission 行，保留双方的摘要核验。
            rows = db.execute(
                "SELECT t.*,m.* FROM triggers AS t JOIN missions AS m "
                "ON m.mission_id=t.mission_id "
                "WHERE t.status='pending' AND m.status='active' AND m.active_trigger_id IS NULL "
                "ORDER BY t.created_at,t.trigger_id LIMIT ?",
                (limit,),
            ).fetchall()
            result = []
            for row in rows:
                self._mission_from_row(row[len(_TRIGGER_COLUMNS) :])
                result.append(_copy(self._trigger_from_row(row[: len(_TRIGGER_COLUMNS)])))
            return result

    def _open_triggers(self) -> list[dict[str, Any]]:
        with self._connection() as db:
            return [
                _copy(self._trigger_from_row(row))
                for row in db.execute(
                    "SELECT * FROM triggers WHERE status IN ('claimed','running') ORDER BY created_at, trigger_id"
                ).fetchall()
            ]

    def _triggers(self, mission_id: str, limit: int) -> list[dict[str, Any]]:
        with self._connection() as db:
            row = db.execute("SELECT * FROM missions WHERE mission_id=?", (mission_id,)).fetchone()
            if row is None:
                raise MissionError("not_found", "未找到该任务。", 404)
            self._mission_from_row(row)
            rows = db.execute(
                "SELECT * FROM triggers WHERE mission_id=? ORDER BY created_at DESC,trigger_id DESC LIMIT ?",
                (mission_id, limit),
            ).fetchall()
            return [_copy(self._trigger_from_row(row)) for row in rows]

    def _detail(self, mission_id: str, limit: int) -> dict[str, Any]:
        """Return a consistent mission/trigger pair while one read transaction is open."""
        with self._connection() as db:
            db.execute("BEGIN")
            try:
                mission_row = db.execute(
                    "SELECT * FROM missions WHERE mission_id=?", (mission_id,)
                ).fetchone()
                if mission_row is None:
                    raise MissionError("not_found", "未找到该任务。", 404)
                mission = self._mission_from_row(mission_row)
                trigger_rows = db.execute(
                    "SELECT * FROM triggers WHERE mission_id=? "
                    "ORDER BY created_at DESC,trigger_id DESC LIMIT ?",
                    (mission_id, limit),
                ).fetchall()
                triggers = [self._trigger_from_row(row) for row in trigger_rows]
                db.commit()
                return {"mission": _copy(mission), "triggers": _copy(triggers)}
            except BaseException:
                db.rollback()
                raise

    def _get_trigger(self, trigger_id: str) -> dict[str, Any]:
        with self._connection() as db:
            row = db.execute("SELECT * FROM triggers WHERE trigger_id=?", (trigger_id,)).fetchone()
            if row is None:
                raise MissionError("not_found", "未找到该触发器。", 404)
            return _copy(self._trigger_from_row(row))

    def _claim(
        self, trigger_id: str, profile_model: int, profile_data: int, stamp: float
    ) -> dict[str, Any] | None:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT * FROM triggers WHERE trigger_id=?", (trigger_id,)
                ).fetchone()
                if row is None:
                    raise MissionError("not_found", "未找到该触发器。", 404)
                trigger = self._trigger_from_row(row)
                if trigger["status"] != "pending":
                    db.commit()
                    return None
                mission_row = db.execute(
                    "SELECT * FROM missions WHERE mission_id=?", (trigger["mission_id"],)
                ).fetchone()
                if mission_row is None:
                    raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500)
                mission = self._mission_from_row(mission_row)
                if mission["status"] != "active":
                    db.commit()
                    return None
                if profile_model == 0 or profile_data == 0:
                    db.commit()
                    return None
                if stamp >= mission["deadline_at"]:
                    mission["status"], mission["reason"], mission["updated_at"] = (
                        "completed",
                        "deadline_reached",
                        stamp,
                    )
                    mission["next_wake_at"] = None
                    self._save_mission(db, mission)
                    self._cancel_pending(db, mission["mission_id"], stamp, "deadline_reached")
                    db.commit()
                    return None
                if mission["active_trigger_id"] is not None:
                    db.commit()
                    return None
                if mission["started_runs"] >= mission["limits"]["max_runs"]:
                    db.commit()
                    return None
                remaining_model = (
                    mission["limits"]["max_model_calls"]
                    - mission["used_model_calls"]
                    - mission["reserved_model_calls"]
                )
                remaining_data = (
                    mission["limits"]["max_data_calls"]
                    - mission["used_data_calls"]
                    - mission["reserved_data_calls"]
                )
                reserve_model, reserve_data = (
                    min(profile_model, max(0, remaining_model)),
                    min(profile_data, max(0, remaining_data)),
                )
                if reserve_model <= 0 or reserve_data <= 0:
                    mission["status"], mission["reason"], mission["updated_at"] = (
                        "blocked",
                        "quota_exhausted",
                        stamp,
                    )
                    mission["next_wake_at"] = None
                    self._save_mission(db, mission)
                    self._cancel_pending(db, mission["mission_id"], stamp, "quota_exhausted")
                    db.commit()
                    return None
                cycle = mission["started_runs"] + 1
                trigger.update(
                    status="claimed",
                    updated_at=stamp,
                    cycle=cycle,
                    reserved_model_calls=reserve_model,
                    reserved_data_calls=reserve_data,
                    handoff=_copy(mission["handoff"]),
                )
                mission.update(
                    started_runs=cycle,
                    reserved_model_calls=mission["reserved_model_calls"] + reserve_model,
                    reserved_data_calls=mission["reserved_data_calls"] + reserve_data,
                    active_trigger_id=trigger_id,
                    updated_at=stamp,
                )
                self._save_trigger(db, trigger)
                self._save_mission(db, mission)
                db.commit()
                return _copy(trigger)
            except BaseException:
                db.rollback()
                raise

    def _mark_running(self, trigger_id: str, request_json: str, stamp: float) -> dict[str, Any]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT * FROM triggers WHERE trigger_id=?", (trigger_id,)
                ).fetchone()
                if row is None:
                    raise MissionError("not_found", "未找到该触发器。", 404)
                trigger = self._trigger_from_row(row)
                if trigger["status"] == "running":
                    if _canonical(trigger["request"]) == request_json:
                        db.commit()
                        return _copy(trigger)
                    raise MissionError("request_conflict", "该触发器已有不同的运行请求。", 409)
                if trigger["status"] != "claimed":
                    raise MissionError("trigger_not_claimed", "触发器未处于可运行状态。", 409)
                trigger.update(status="running", updated_at=stamp, request=json.loads(request_json))
                self._save_trigger(db, trigger)
                db.commit()
                return _copy(trigger)
            except BaseException:
                db.rollback()
                raise

    def _settle(
        self,
        trigger_id: str,
        model_calls: int,
        data_calls: int,
        run_status: str,
        stop_reason: str,
        handoff_json: str,
        stamp: float,
    ) -> dict[str, Any]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT * FROM triggers WHERE trigger_id=?", (trigger_id,)
                ).fetchone()
                if row is None:
                    raise MissionError("not_found", "未找到该触发器。", 404)
                trigger = self._trigger_from_row(row)
                mission_row = db.execute(
                    "SELECT * FROM missions WHERE mission_id=?", (trigger["mission_id"],)
                ).fetchone()
                if mission_row is None:
                    raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500)
                mission = self._mission_from_row(mission_row)
                if trigger["status"] in {"completed", "blocked", "cancelled"}:
                    db.commit()
                    return _copy(mission)
                if trigger["status"] not in {"claimed", "running"}:
                    raise MissionError("trigger_not_claimed", "触发器未处于可结算状态。", 409)
                if (
                    model_calls > trigger["reserved_model_calls"]
                    or data_calls > trigger["reserved_data_calls"]
                ):
                    raise MissionError("usage_exceeds_reservation", "实际用量超过预留额度。", 409)
                trigger.update(
                    status="blocked"
                    if run_status in {"failed", "timeout", "budget_exceeded"}
                    else "completed",
                    updated_at=stamp,
                    run_status=run_status,
                    stop_reason=stop_reason,
                )
                mission.update(
                    reserved_model_calls=mission["reserved_model_calls"]
                    - trigger["reserved_model_calls"],
                    reserved_data_calls=mission["reserved_data_calls"]
                    - trigger["reserved_data_calls"],
                    used_model_calls=mission["used_model_calls"] + model_calls,
                    used_data_calls=mission["used_data_calls"] + data_calls,
                    completed_runs=mission["completed_runs"] + 1,
                    active_trigger_id=None,
                    handoff=json.loads(handoff_json),
                    updated_at=stamp,
                )
                if (
                    mission["status"] in {"active", "paused"}
                    and run_status == "completed"
                    and mission["stop_on_completed"]
                ):
                    # 暂停只禁止派发。进行中的周期成功后，仍须执行完成政策。
                    mission.update(status="completed", reason="completed", next_wake_at=None)
                elif mission["status"] == "active":
                    # 单轮预算耗尽同样停止，避免固定延迟重复派发必然失败的计划。
                    if run_status in {"failed", "timeout", "budget_exceeded"}:
                        mission["status"], mission["reason"], mission["next_wake_at"] = (
                            "blocked",
                            run_status,
                            None,
                        )
                    elif mission["completed_runs"] >= mission["limits"]["max_runs"]:
                        mission["status"], mission["reason"], mission["next_wake_at"] = (
                            "completed",
                            "max_runs_reached",
                            None,
                        )
                    elif stamp >= mission["deadline_at"]:
                        mission["status"], mission["reason"], mission["next_wake_at"] = (
                            "completed",
                            "deadline_reached",
                            None,
                        )
                    elif (
                        mission["used_model_calls"] >= mission["limits"]["max_model_calls"]
                        or mission["used_data_calls"] >= mission["limits"]["max_data_calls"]
                    ):
                        mission["status"], mission["reason"], mission["next_wake_at"] = (
                            "blocked",
                            "quota_exhausted",
                            None,
                        )
                    else:
                        mission["next_wake_at"] = (
                            stamp + mission["interval_seconds"]
                            if mission["interval_seconds"] is not None
                            else None
                        )
                else:
                    mission["next_wake_at"] = None
                self._save_trigger(db, trigger)
                self._save_mission(db, mission)
                if mission["status"] in {"completed", "blocked"}:
                    self._cancel_pending(
                        db, mission["mission_id"], stamp, mission["reason"] or mission["status"]
                    )
                db.commit()
                return _copy(mission)
            except BaseException:
                db.rollback()
                raise

    def _block(self, trigger_id: str, code: str, stamp: float) -> dict[str, Any]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT * FROM triggers WHERE trigger_id=?", (trigger_id,)
                ).fetchone()
                if row is None:
                    raise MissionError("not_found", "未找到该触发器。", 404)
                trigger = self._trigger_from_row(row)
                mission_row = db.execute(
                    "SELECT * FROM missions WHERE mission_id=?", (trigger["mission_id"],)
                ).fetchone()
                if mission_row is None:
                    raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500)
                mission = self._mission_from_row(mission_row)
                if trigger["status"] in {"blocked", "completed", "cancelled"}:
                    db.commit()
                    return _copy(mission)
                trigger.update(status="blocked", updated_at=stamp, stop_reason=code)
                mission.update(
                    status=mission["status"]
                    if mission["status"] in {"paused", "completed"}
                    else "blocked",
                    # 人工结束或截止时间的原因与本轮阻塞原因分别保留。
                    reason=mission["reason"] if mission["status"] == "completed" else code,
                    updated_at=stamp,
                    next_wake_at=None,
                )
                self._save_trigger(db, trigger)
                self._save_mission(db, mission)
                self._cancel_pending(db, mission["mission_id"], stamp, code)
                db.commit()
                return _copy(mission)
            except BaseException:
                db.rollback()
                raise

    def _set_status(self, mission_id: str, status: str, stamp: float) -> dict[str, Any]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT * FROM missions WHERE mission_id=?", (mission_id,)
                ).fetchone()
                if row is None:
                    raise MissionError("not_found", "未找到该任务。", 404)
                mission = self._mission_from_row(row)
                if mission["status"] == "blocked" and status != "completed":
                    raise MissionError("blocked_resume_forbidden", "阻塞任务不能普通恢复。", 409)
                if mission["status"] == "completed" and status != "completed":
                    raise MissionError("mission_closed", "该任务已结束。", 409)
                if status == "active" and mission["active_trigger_id"] is not None:
                    # block 会保留人工 paused 意图，因此恢复权限还必须检查持久 owner。
                    # 拒绝前不写任何字段，未知结果的原因、预留与原 run 身份保持不变。
                    owner_row = db.execute(
                        "SELECT * FROM triggers WHERE trigger_id=?",
                        (mission["active_trigger_id"],),
                    ).fetchone()
                    if owner_row is None:
                        raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500)
                    owner = self._trigger_from_row(owner_row)
                    if owner["mission_id"] != mission_id:
                        raise MissionError("corrupt_state", "任务存储完整性校验失败。", 500)
                    if owner["status"] == "blocked":
                        raise MissionError(
                            "blocked_resume_forbidden", "阻塞任务不能普通恢复。", 409
                        )
                if mission["status"] == status:
                    db.commit()
                    return _copy(mission)
                mission["status"], mission["updated_at"] = status, stamp
                mission["reason"] = (
                    None if status != "completed" else (mission["reason"] or "completed")
                )
                if status != "active":
                    mission["next_wake_at"] = None
                elif mission["next_wake_at"] is None and mission["interval_seconds"] is not None:
                    mission["next_wake_at"] = stamp
                self._save_mission(db, mission)
                if status == "completed":
                    self._cancel_pending(db, mission_id, stamp, "completed")
                db.commit()
                return _copy(mission)
            except BaseException:
                db.rollback()
                raise

    def _schedule_due(self, stamp: float) -> int:
        count = 0
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                rows = db.execute(
                    "SELECT * FROM missions WHERE status='active' AND "
                    "(deadline_at<=? OR (next_wake_at IS NOT NULL AND next_wake_at<=?)) "
                    "ORDER BY next_wake_at,mission_id",
                    (stamp, stamp),
                ).fetchall()
                for row in rows:
                    mission = self._mission_from_row(row)
                    if stamp >= mission["deadline_at"]:
                        mission.update(
                            status="completed",
                            reason="deadline_reached",
                            next_wake_at=None,
                            updated_at=stamp,
                        )
                        self._save_mission(db, mission)
                        self._cancel_pending(db, mission["mission_id"], stamp, "deadline_reached")
                        continue
                    open_count = db.execute(
                        "SELECT COUNT(*) FROM triggers WHERE mission_id=? AND status IN ('pending','claimed','running')",
                        (mission["mission_id"],),
                    ).fetchone()[0]
                    accepted_count = db.execute(
                        "SELECT COUNT(*) FROM triggers WHERE mission_id=?", (mission["mission_id"],)
                    ).fetchone()[0]
                    if open_count or accepted_count >= mission["limits"]["max_runs"]:
                        if accepted_count >= mission["limits"]["max_runs"] and not open_count:
                            mission["status"], mission["reason"], mission["next_wake_at"] = (
                                "completed",
                                "max_runs_reached",
                                None,
                            )
                            mission["updated_at"] = stamp
                            self._save_mission(db, mission)
                        continue
                    event = f"system:schedule:{mission['mission_id']}:{mission['next_wake_at']:.6f}"
                    trigger = self._new_trigger(mission, event, stamp)
                    db.execute(
                        "INSERT INTO triggers VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        self._trigger_row(trigger),
                    )
                    mission["next_wake_at"], mission["updated_at"] = None, stamp
                    self._save_mission(db, mission)
                    count += 1
                db.commit()
                return count
            except BaseException:
                db.rollback()
                raise
