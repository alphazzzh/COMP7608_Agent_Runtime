"""为外部评估报告提供 Web 工作区内的安全私有存储。

独立的报告契约来自 evaluation.models，本模块不运行基准测试或推断未知指标。
文件操作通过网页存储工作线程执行；目录描述符、防符号链接打开和原子替换
限制访问范围，取消时等待待完成的写入收尾。
"""

from __future__ import annotations

import heapq
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4

from pydantic import Field, field_validator

from ..evaluation.models import EvaluationImport, _aware_iso
from ..validation import canonical_json
from .storage import WebError, disk, utc_now

_IDENTITY = re.compile(r"evaluation-[0-9a-f]{32}\Z")
_MAX_RECORD_BYTES = 262_144
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
_TIMESTAMP = Annotated[str, Field(max_length=64)]


class _EvaluationReport(EvaluationImport):
    """重新校验已保存记录，包括由服务端管理且不可变的来源记录。"""

    evaluation_id: str
    imported_at: _TIMESTAMP
    origin: Literal["imported"]

    @field_validator("evaluation_id")
    @classmethod
    def _safe_identity(cls, value: str) -> str:
        if not _IDENTITY.fullmatch(value):
            raise ValueError("invalid evaluation identity")
        return value

    @field_validator("imported_at")
    @classmethod
    def _import_date(cls, value: str) -> str:
        return _aware_iso(value)


class EvaluationRepository:
    """在单一私有数据根目录内，创建并读取经校验的完整报告。

    所属网页仓库负责数据目录的进程级锁。本独立仓库无需后台任务或长期保留的句柄：
    并发创建使用不同临时文件并原子发布；读取和扫描会打开新的描述符，
    并在相应工作线程内关闭。
    """

    def __init__(self, root):
        # 绝对路径规范化只处理路径文本；只有 start() 调度受跟踪的
        # 磁盘工作线程后，才会查询文件系统。
        self.root = Path(root).expanduser().absolute()

    @staticmethod
    def _unavailable() -> WebError:
        return WebError("evaluations_unavailable", "评估报告无法读取或保存。", 503)

    async def start(self) -> None:
        """创建私有目录，并拒绝被替换为符号链接的目录。"""
        await disk(self._start)

    def _start(self) -> None:
        try:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            root_fd = os.open(self.root, _DIRECTORY_FLAGS)
            try:
                os.fchmod(root_fd, 0o700)
                try:
                    os.mkdir("evaluations", mode=0o700, dir_fd=root_fd)
                except FileExistsError:
                    pass
                directory = os.open("evaluations", _DIRECTORY_FLAGS, dir_fd=root_fd)
                try:
                    os.fchmod(directory, 0o700)
                    os.fsync(directory)
                finally:
                    os.close(directory)
                os.fsync(root_fd)
            finally:
                os.close(root_fd)
        except OSError:
            raise self._unavailable() from None

    @contextmanager
    def _directory(self) -> Iterator[int]:
        """将全部报告操作锚定到不跟随符号链接的目录描述符。"""
        root_fd = os.open(self.root, _DIRECTORY_FLAGS)
        try:
            directory = os.open("evaluations", _DIRECTORY_FLAGS, dir_fd=root_fd)
            try:
                yield directory
            finally:
                os.close(directory)
        finally:
            os.close(root_fd)

    async def create(self, report: EvaluationImport) -> dict:
        """先冻结经校验的调用方数据，再将不可变字节交给磁盘工作线程。"""
        # 即使模型由调用方构造或修改，也要重新校验。通过普通数据复制，
        # 同时使所有嵌套结果和指标与原对象相互独立。
        imported = EvaluationImport.model_validate(report.model_dump(mode="python"))
        value = _EvaluationReport.model_validate(
            {
                **imported.model_dump(mode="python"),
                "evaluation_id": f"evaluation-{uuid4().hex}",
                "imported_at": utc_now(),
                "origin": "imported",
            }
        ).model_dump(mode="json")
        encoded = canonical_json(value)
        # 创建成功后产生的文件必须始终能被 _load 接受，即使未来模型变化
        # 扩大了允许序列化的载荷范围，也应满足此约束。
        if len(encoded) > _MAX_RECORD_BYTES:
            raise WebError("evaluation_too_large", "评估报告超过保存上限。", 413)
        await disk(self._save, value["evaluation_id"], encoded)
        return value

    def _save(self, identity: str, encoded: bytes) -> None:
        temporary = f".evaluation-{uuid4().hex}.tmp"
        try:
            with self._directory() as directory:
                # O_EXCL 与 0600 权限确保从首个字节起文件就是私有的，
                # 避免先受进程 umask 影响、再 chmod 的权限暴露窗口。
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(temporary, flags, 0o600, dir_fd=directory)
                try:
                    with os.fdopen(descriptor, "wb") as output:
                        output.write(encoded)
                        output.flush()
                        os.fsync(output.fileno())
                    os.replace(
                        temporary,
                        f"{identity}.json",
                        src_dir_fd=directory,
                        dst_dir_fd=directory,
                    )
                    os.fsync(directory)
                finally:
                    try:
                        os.unlink(temporary, dir_fd=directory)
                    except FileNotFoundError:
                        pass
        except OSError:
            raise self._unavailable() from None

    async def get(self, evaluation_id: str) -> dict:
        """返回一份经校验的完整报告，不接受任意路径。"""
        if not isinstance(evaluation_id, str) or not _IDENTITY.fullmatch(evaluation_id):
            raise WebError("not_found", "未找到该评估报告。", 404)
        return await disk(self._get, evaluation_id)

    def _get(self, identity: str) -> dict:
        try:
            with self._directory() as directory:
                return self._load(directory, identity)
        except FileNotFoundError:
            raise WebError("not_found", "未找到该评估报告。", 404) from None
        except (OSError, ValueError, TypeError):
            raise self._unavailable() from None

    @staticmethod
    def _load(directory: int, identity: str) -> dict:
        # 非阻塞打开避免因文件被替换成 FIFO 而挂起；仅解析大小受限的
        # 普通文件。第二次字节限制检查还可防止文件在 fstat 后增长，
        # 从而避免无界读入内存。
        flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(f"{identity}.json", flags, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as source:
            metadata = os.fstat(source.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_RECORD_BYTES:
                raise ValueError("invalid report file")
            raw = source.read(_MAX_RECORD_BYTES + 1)
        if len(raw) > _MAX_RECORD_BYTES:
            raise ValueError("oversize report")
        # Pydantic 的 JSON 解码器限制嵌套深度，将过深的损坏输入作为
        # 校验错误拒绝，而不向外泄露 RecursionError。
        report = _EvaluationReport.model_validate_json(raw)
        if report.evaluation_id != identity:
            raise ValueError("evaluation ownership mismatch")
        return report.model_dump(mode="json")

    async def listing(self) -> list[dict]:
        """最多返回 100 份完整报告，按导入时间从新到旧排列。"""
        return await disk(self._listing)

    def _listing(self) -> list[dict]:
        try:
            with self._directory() as directory:

                def records():
                    # 内存中只保留当前最新的 100 份报告。校验与时间戳比较
                    # 仍全部在此工作线程中完成。
                    with os.scandir(directory) as entries:
                        for entry in entries:
                            if entry.name.endswith(".json") and _IDENTITY.fullmatch(
                                entry.name[:-5]
                            ):
                                yield self._load(directory, entry.name[:-5])

                return heapq.nlargest(
                    100,
                    records(),
                    key=lambda value: (
                        datetime.fromisoformat(value["imported_at"]),
                        value["evaluation_id"],
                    ),
                )
        except (OSError, ValueError, TypeError):
            raise self._unavailable() from None
