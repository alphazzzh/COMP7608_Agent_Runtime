"""通过异步文件系统派发，为每次运行提供持久化 JSONL 审计存储。

每个接收器实例独占一个新建的运行目录或执行尝试分段。``append`` 返回前事件已提交；
工件先持久化，其引用才能出现在事件中。恢复分段保留原运行标识，并拥有独立的事件
序列及工件存储；日志本身是审计轨迹，不是可恢复执行的检查点。

文件系统调用在事件循环工作线程池中执行。取消 await 会迅速停止等待，但 Python
无法中断已经执行的文件系统调用。线程锁将残留工作与后续清理串行化。
本模块不保留长期打开的文件描述符，也不创建后台 asyncio 写入任务。
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
    """拒绝路径穿越，不静默改写不透明 ID。"""
    if not isinstance(value, str) or not _SAFE_COMPONENT.fullmatch(value):
        raise ValueError(f"{label} must be a safe opaque filename component")
    return value


def _sync_directory(path: Path) -> None:
    """在支持 fsync 的本地文件系统上持久化目录项。"""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class JsonlTraceSink:
    """独占写入一次运行或执行尝试，不覆盖已有审计数据。

    每次运行或执行尝试须创建新实例。``create_run`` 独占预留 ``root/run_id``；
    ``create_attempt`` 独占预留 ``root/run_id/attempts/attempt_id`` 叶目录。
    两者均使用原始运行 ID 校验事件归属。
    工件、运行和尝试 ID 必须是安全的 ASCII 文件名组成部分，且长度至多 200 个字符。
    配置的根目录属于可信存储，接收器存活期间不允许外部修改。
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
        """预留新的运行目录；已有相同 ID 时原子失败。"""
        _safe_component(run_id, "run_id")
        await asyncio.to_thread(self._locked, self._create_run_sync, run_id)

    async def create_attempt(self, run_id: str, attempt_id: str) -> None:
        """在保留稳定运行归属的同时，预留新的审计分段。

        创建任何目录前先验证两个 ID。绝不以写入方式打开之前的尝试；每个分段从序号一开始，
        并拥有自己的不可变工件，与普通新运行接收器一致。
        """
        _safe_component(run_id, "run_id")
        _safe_component(attempt_id, "attempt_id")
        await asyncio.to_thread(self._locked, self._create_attempt_sync, run_id, attempt_id)

    async def append(self, event: TraceEvent) -> None:
        """检查运行归属和序号后提交单个事件。"""
        await asyncio.to_thread(self._locked, self._append_sync, event)

    async def put_artifact(self, ref: str, json_value: JsonValue) -> None:
        """在任何引用事件写入前，持久化不可变 JSON 工件。"""
        _safe_component(ref, "artifact ref")
        # 触碰磁盘前先验证并序列化：无效 JSON 绝不能留下
        # 部分写入或已预留的工件文件。同时固定调用方可变输入，
        # 确保排队的磁盘操作整个生命周期内使用同一份内容。
        encoded = canonical_json(json_value)
        await asyncio.to_thread(self._locked, self._put_artifact_sync, ref, encoded)

    async def close(self) -> str:
        """结束审计日志并返回其绝对路径；可安全重复调用。"""
        return await asyncio.to_thread(self._locked, self._close_sync)

    def _locked(self, operation: Callable[..., _T], *args: object) -> _T:
        # asyncio.Lock 无法保护 await 被取消后仍在运行的磁盘操作。
        # 因此在线程工作函数内部持有锁。
        with self._lock:
            return operation(*args)

    def _create_run_sync(self, run_id: str) -> None:
        if self._run_id is not None:
            raise RuntimeError("sink already owns a run; create a new sink instance")
        self.root.mkdir(parents=True, exist_ok=True)
        run_dir = self.root / run_id
        # mkdir 提供跨实例和跨进程的独占所有权。
        # 初始化失败时保留预留目录，便于事后检查。
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
        # 父目录可能已包含之前的记录，仅独占预留叶目录；
        # 不截断任何已有日志或工件。
        attempt_dir.mkdir(mode=0o700, exist_ok=False)
        self._initialize_directory(run_id, attempt_dir, (attempts_dir, owner_dir, self.root))

    def _initialize_directory(
        self, run_id: str, run_dir: Path, parent_dirs: tuple[Path, ...]
    ) -> None:
        """在对外暴露日志前，持久化新预留的日志及其祖先目录。"""
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
            # 写入或 fsync 失败时，提交结果可能未知。拒绝后续所有写入，
            # 避免产生重复或不完整的 JSONL 行。
            self._write_failed = True
            raise
        self._last_seq = event.seq

    def _put_artifact_sync(self, ref: str, encoded: bytes) -> None:
        self._require_run()
        assert self._artifacts_dir is not None
        destination = self._artifacts_dir / f"{ref}.json"
        # 独占打开不会触碰已有字节。所有接收器操作均持有存储锁，
        # 因此任何事件都不能观察到未完整写入的工件。
        # 先提交目录项，再将引用标记为可用。
        try:
            with destination.open("xb") as output:
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            _sync_directory(self._artifacts_dir)
        except FileExistsError:
            # 这是安全的验证失败：已有工件未被修改，
            # 因此仍可写入其他新工件和事件。
            raise
        except OSError:
            self._write_failed = True
            raise
        self._artifacts.add(ref)

    def _close_sync(self) -> str:
        self._require_run(writable=False)
        assert self._events_path is not None
        if not self._closed:
            # 每次 append 都已执行 fsync 并关闭描述符；此处再次 fsync
            # 也用于确认收尾时日志仍可访问。
            with self._events_path.open("rb") as output:
                os.fsync(output.fileno())
            self._closed = True
        return str(self._events_path)
