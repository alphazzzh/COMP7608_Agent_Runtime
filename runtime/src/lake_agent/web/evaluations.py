"""Strict, private storage for caller-imported evaluation summaries.

Reports describe externally performed evaluations: this module never executes a
benchmark or infers an unknown metric. All filesystem work runs through the web
storage worker, whose cancellation boundary drains pending writes. Directory
descriptors and no-follow opens keep report identifiers inside the private
``evaluations`` folder; atomic replacement exposes only complete JSON records.
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
from typing import Annotated, Literal, Self
from uuid import uuid4

from pydantic import Field, field_validator, model_validator

from ..models import Mode, Model
from ..validation import canonical_json
from .storage import WebError, disk, utc_now

_IDENTITY = re.compile(r"evaluation-[0-9a-f]{32}\Z")
_MAX_RECORD_BYTES = 262_144
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
_UNIT = Annotated[float, Field(ge=0, le=1)]
_NONNEGATIVE = Annotated[float, Field(ge=0)]
_LABEL = Annotated[str, Field(min_length=1, max_length=200)]
_TIMESTAMP = Annotated[str, Field(max_length=64)]


def _aware_iso(value: str) -> str:
    """Preserve the submitted ISO spelling while requiring a real UTC offset."""
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("evaluation timestamps require an explicit timezone")
    return value


class _EvaluationModel(Model):
    """Keep imported text serializable as UTF-8 as well as strictly typed."""

    @field_validator("*")
    @classmethod
    def _utf8_text(cls, value):
        if isinstance(value, str):
            value.encode("utf-8")
        return value


class EvaluationMetrics(_EvaluationModel):
    """Nullable reported metrics; absence conveys no measured value."""

    accuracy: _UNIT | None = None
    retrieval_precision: _UNIT | None = None
    retrieval_recall: _UNIT | None = None
    retrieval_f1: _UNIT | None = None
    grounding: _UNIT | None = None
    completion_rate: _UNIT | None = None
    tool_calls_mean: _NONNEGATIVE | None = None
    latency_ms_mean: _NONNEGATIVE | None = None
    token_total: _NONNEGATIVE | None = None
    cost_usd: _NONNEGATIVE | None = None


class EvaluationRow(_EvaluationModel):
    """One permitted execution mode and its independently declared task count."""

    mode: Mode
    task_count: Annotated[int, Field(ge=1, le=1_000_000)]
    metrics: EvaluationMetrics = Field(default_factory=EvaluationMetrics)


class EvaluationImport(_EvaluationModel):
    """Caller-supplied assessment metadata, excluding server-owned provenance."""

    title: Annotated[str, Field(min_length=1, max_length=160)]
    dataset_id: _LABEL
    task_set_id: _LABEL
    model_id: _LABEL
    evaluator: _LABEL
    evaluated_at: _TIMESTAMP
    notes: Annotated[str, Field(max_length=2000)] = ""
    results: Annotated[list[EvaluationRow], Field(min_length=1, max_length=3)]

    @field_validator("evaluated_at")
    @classmethod
    def _evaluation_date(cls, value: str) -> str:
        return _aware_iso(value)

    @model_validator(mode="after")
    def _unique_modes(self) -> Self:
        if len({row.mode for row in self.results}) != len(self.results):
            raise ValueError("each evaluation mode may appear only once")
        return self


class _EvaluationReport(EvaluationImport):
    """Revalidate saved records, including immutable server-owned provenance."""

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
    """Create and read validated full reports within one private data root.

    The owning web repository provides the process-level data-directory lock.
    This independent repository needs no background tasks or retained handles:
    concurrent creates use distinct temporary files and atomic publication, and
    reads/scans open fresh descriptors that are closed inside their worker.
    """

    def __init__(self, root):
        # Absolute-path normalization is lexical; no filesystem lookup occurs
        # until start() schedules its observed disk worker.
        self.root = Path(root).expanduser().absolute()

    @staticmethod
    def _unavailable() -> WebError:
        return WebError("evaluations_unavailable", "评估报告无法读取或保存。", 503)

    async def start(self) -> None:
        """Create private directories and reject a substituted symlink folder."""
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
        """Anchor all report operations to a non-symlink directory descriptor."""
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
        """Freeze validated caller data before handing immutable bytes to disk."""
        # Revalidate even caller-constructed/mutated model instances. Copying
        # through their plain data also detaches every nested result and metric.
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
        # Successful creation must always produce a file accepted by _load,
        # even if a future model change expands the allowed serialized payload.
        if len(encoded) > _MAX_RECORD_BYTES:
            raise WebError("evaluation_too_large", "评估报告超过保存上限。", 413)
        await disk(self._save, value["evaluation_id"], encoded)
        return value

    def _save(self, identity: str, encoded: bytes) -> None:
        temporary = f".evaluation-{uuid4().hex}.tmp"
        try:
            with self._directory() as directory:
                # O_EXCL and mode 0600 make the first byte private; no chmod
                # interval exposes a newly created file through the process umask.
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
        """Return one validated full report without accepting arbitrary paths."""
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
        # Nonblocking open avoids hanging on a replaced FIFO; only regular,
        # bounded files are parsed. The second byte bound also handles growth
        # after fstat without an unbounded read into memory.
        flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(f"{identity}.json", flags, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as source:
            metadata = os.fstat(source.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_RECORD_BYTES:
                raise ValueError("invalid report file")
            raw = source.read(_MAX_RECORD_BYTES + 1)
        if len(raw) > _MAX_RECORD_BYTES:
            raise ValueError("oversize report")
        # Pydantic's bounded-depth JSON decoder rejects deeply nested corrupt
        # input as a validation error instead of leaking a RecursionError.
        report = _EvaluationReport.model_validate_json(raw)
        if report.evaluation_id != identity:
            raise ValueError("evaluation ownership mismatch")
        return report.model_dump(mode="json")

    async def listing(self) -> list[dict]:
        """Return at most 100 complete reports, newest import first."""
        return await disk(self._listing)

    def _listing(self) -> list[dict]:
        try:
            with self._directory() as directory:

                def records():
                    # Keep only the current top 100 in memory. Validation and
                    # timestamp comparisons still happen wholly in this worker.
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
