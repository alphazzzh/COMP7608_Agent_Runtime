"""存储私有的展示元数据，并生成已提交审计数据的有界视图。

核心 SQLite 状态仍是唯一的恢复依据。本仓库只保存界面元数据，
读取经校验的审计事件，且只沿用属于所请求运行和尝试的引用。
所有文件系统操作均由受跟踪的工作线程执行。
进程锁禁止多个网页服务实例同时占有同一个数据目录。
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
import shutil
import stat
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path

from .. import models as m
from ..validation import canonical_json

_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}\Z")
_RUN = re.compile(r"run-[0-9a-f]{32}\Z")


def _failure_preview(value):
    """生成有界展示副本，不改变原始审计或把全部模型输出塞进任务顶栏。

    JSONL 与清单已有文件大小限制；这里额外限制节点、深度和文本总量，
    防止一份合法但很大的诊断在每次轮询时重复占用页面和响应空间。
    """
    remaining = {"nodes": 128, "characters": 12_000}

    def preview(item, depth=0):
        if remaining["nodes"] <= 0 or remaining["characters"] <= 0 or depth > 5:
            return "…（展示已截断，完整内容见原始记录）"
        remaining["nodes"] -= 1
        if isinstance(item, str):
            limit = min(4000, remaining["characters"])
            remaining["characters"] -= min(len(item), limit)
            return item[:limit] + ("…" if len(item) > limit else "")
        if isinstance(item, list):
            values = [preview(entry, depth + 1) for entry in item[:16]]
            if len(item) > 16:
                values.append(f"…另有 {len(item) - 16} 项，见原始记录")
            return values
        if isinstance(item, dict):
            return {
                str(key)[:100]: preview(entry, depth + 1) for key, entry in islice(item.items(), 16)
            }
        return item

    return preview(value)


def _display_failure(record, events):
    """从已提交事件重建展示反馈；成功终态不再发布历史失败为当前错误。

    新控制器显式记录 rejected/resolved，拒绝与成功的原始调用事件仍保留。
    旧历史没有反馈事件时，以同阶段调用成功作为清除条件。展示层不裁定
    核心能否恢复、是否接受答案，也不向恢复日志写入派生状态。
    """
    result = record.get("result") or {}
    if result.get("status", record.get("status")) == "completed":
        return None
    pending = {}
    for index, event in enumerate(events):
        kind, error = event["event_type"], event.get("error")
        phase = event["detail"].get("phase")
        if kind == "content.feedback.resolved":
            pending.pop(phase, None)
        elif kind == "content.output.rejected" and error:
            pending[phase] = (event, True, index)
        elif kind.endswith(".call.failed") and error:
            pending[kind.removesuffix(".call.failed")] = (event, False, index)
        elif kind.endswith(".call.completed"):
            phase = kind.removesuffix(".call.completed")
            if phase in pending and not pending[phase][1]:
                pending.pop(phase)
    saved = result.get("failure")
    if isinstance(saved, dict):
        failure = {key: saved.get(key) for key in ("phase", "call_id", "error")}
        linked = next(
            (event for event in reversed(events) if event.get("call_id") == failure["call_id"]),
            None,
        )
    elif pending:
        linked = max(pending.values(), key=lambda value: value[2])[0]
        failure = {
            "phase": linked["detail"].get("phase")
            if linked["event_type"] == "content.output.rejected"
            else linked["event_type"].removesuffix(".call.failed"),
            "call_id": linked.get("call_id"),
            "error": linked["error"],
        }
    else:
        return None
    # 先裁剪模型诊断，再附关联身份；大诊断不能挤掉定位原始调用所需的字段。
    failure = _failure_preview(failure)
    if linked:
        failure.update(attempt_id=linked["attempt_id"], event_seq=linked["seq"])
    return failure


class WebError(Exception):
    """使用固定的公开错误码与消息，不泄露路径、存储内容或原始异常。"""

    def __init__(self, code, message, status=409):
        self.code, self.message, self.status = code, message, status
        super().__init__(message)


def utc_now():
    return datetime.now(UTC).isoformat()


async def disk(method, *args):
    """收到取消时等待文件系统任务收尾，避免写入超出资源的持有期。"""
    job = asyncio.create_task(asyncio.to_thread(method, *args))
    try:
        return await asyncio.shield(job)
    except asyncio.CancelledError:
        while not job.done():
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        # 获取工作线程可能抛出的异常，但不让它替换原取消异常。
        if job.done() and not job.cancelled():
            job.exception()
        raise


class Repository:
    """仅管理界面清单、有序轨迹分段及其引用的工件。"""

    def __init__(self, root):
        self.root = Path(root).expanduser().resolve()
        self.manifests = self.root / "metadata"
        self.traces = self.root / "traces"
        self.deletions = self.root / ".deletions"
        self.descriptor = None

    def _inside(self, path, parent):
        resolved = path.resolve()
        if not resolved.is_relative_to(parent.resolve()):
            raise WebError("not_found", "未找到该记录。", 404)
        return resolved

    def _manifest(self, run_id):
        if not _RUN.fullmatch(run_id):
            raise WebError("not_found", "未找到该运行。", 404)
        return self._inside(self.manifests / f"{run_id}.json", self.manifests)

    async def start(self):
        await disk(self._start)

    def _start(self):
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(self.root / "web.lock", flags, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(fd)
            raise WebError("directory_busy", "该工作区目录已由另一个服务使用。") from None
        self.descriptor = fd
        self.manifests.mkdir(mode=0o700, exist_ok=True)
        self.traces.mkdir(mode=0o700, exist_ok=True)
        self.deletions.mkdir(mode=0o700, exist_ok=True)
        # 删除标记的父目录也必须持久化，避免突然断电后只保留 SQLite 删除。
        self._sync_directory(self.root)

    async def close(self):
        await disk(self._close)

    def _close(self):
        if self.descriptor is not None:
            descriptor, self.descriptor = self.descriptor, None
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    async def save(self, record):
        # 调度磁盘工作线程前，先冻结可变的嵌套数据。
        encoded = canonical_json(record)
        await disk(self._save, record["run_id"], encoded)

    def _save(self, run_id, encoded):
        target = self._manifest(run_id)
        temporary = target.with_suffix(".tmp")
        with temporary.open("wb") as output:
            os.chmod(temporary, 0o600)
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, target)
        fd = os.open(self.manifests, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    async def load(self, run_id):
        return await disk(self._load, run_id)

    def _load(self, run_id):
        if self._deleting(run_id):
            raise WebError("not_found", "未找到该运行。", 404)
        path = self._manifest(run_id)
        try:
            if path.stat().st_size > 512_000:
                raise ValueError("manifest size")
            value = json.loads(path.read_text())
            if value["run_id"] != run_id or not isinstance(value["attempts"], list):
                raise ValueError("manifest ownership")
            return value
        except FileNotFoundError:
            raise WebError("not_found", "未找到该运行。", 404) from None
        except (ValueError, KeyError, TypeError):
            raise WebError("history_unavailable", "运行记录无法读取。", 503) from None

    async def listing(self, *, limit=100):
        """限制浏览器历史记录数量，同时允许启动过程核对并修正全部记录。"""
        return await disk(self._listing, limit)

    def _listing(self, limit):
        records = []
        for path in self.manifests.glob("run-*.json"):
            try:
                records.append(self._load(path.stem))
            except WebError as error:
                if error.code != "not_found":
                    raise
                # 删除意图立即隐藏记录；并发列表读取也不能重新发布它。
        return sorted(records, key=lambda value: value["created_at"], reverse=True)[:limit]

    @staticmethod
    def _real_path(path, *, directory):
        """删除只使用真实目录和普通文件，绝不解析符号链接到其他目标。"""
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            return False
        if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
            raise WebError("delete_failed", "任务路径未通过删除校验，未删除外部文件。", 503)
        return True

    @staticmethod
    def _sync_directory(path):
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _delete_marker(self, run_id):
        if not isinstance(run_id, str) or not _RUN.fullmatch(run_id):
            raise WebError("not_found", "未找到该运行。", 404)
        return self.deletions / f"{run_id}.delete"

    def _deleting(self, run_id):
        marker = self._delete_marker(run_id)
        if not self._real_path(self.deletions, directory=True):
            return False
        return self._real_path(marker, directory=False)

    async def deleting(self, run_id):
        return await disk(self._deleting, run_id)

    async def pending_deletions(self):
        """重启后先完成已提交删除意图，再开放历史与恢复入口。"""
        return await disk(self._pending_deletions)

    def _pending_deletions(self):
        if not self._real_path(self.deletions, directory=True):
            return []
        return [
            path.stem for path in self.deletions.glob("run-*.delete") if self._deleting(path.stem)
        ]

    def _deletion_paths(self, run_id):
        self._delete_marker(run_id)
        for directory in (self.root, self.manifests, self.traces, self.deletions):
            if not self._real_path(directory, directory=True):
                raise WebError("delete_failed", "任务存储目录不可用，删除尚未完成。", 503)
        manifest = self.manifests / f"{run_id}.json"
        temporary = manifest.with_suffix(".tmp")
        traces = self.traces / run_id
        self._real_path(manifest, directory=False)
        self._real_path(temporary, directory=False)
        self._real_path(traces, directory=True)
        return manifest, temporary, traces

    async def prepare_delete(self, run_id):
        """落盘零字节删除标记；其文件名即唯一待删除身份，不包含用户数据。"""
        return await disk(self._prepare_delete, run_id)

    def _prepare_delete(self, run_id):
        self._deletion_paths(run_id)
        if self._deleting(run_id):
            return False
        self._load(run_id)
        marker = self._delete_marker(run_id)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(marker, flags, 0o600)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._sync_directory(self.deletions)
        return True

    async def cancel_delete(self, run_id):
        """仅在 SQLite 明确拒绝活动租约且尚未删除任何数据时撤销新意图。"""
        await disk(self._cancel_delete, run_id)

    def _cancel_delete(self, run_id):
        if self._deleting(run_id):
            self._delete_marker(run_id).unlink()
            self._sync_directory(self.deletions)

    async def finish_delete(self, run_id):
        """SQLite 已提交后清理任务文件；失败保留标记，允许重启或重试完成。"""
        await disk(self._finish_delete, run_id)

    def _finish_delete(self, run_id):
        if not self._deleting(run_id):
            raise WebError("delete_failed", "缺少已提交删除意图，未删除任务文件。", 503)
        manifest, temporary, traces = self._deletion_paths(run_id)
        if traces.exists():
            # POSIX 的 fd 版本 rmtree 不跟随树内符号链接，只移除链接本身。
            shutil.rmtree(traces)
        manifest.unlink(missing_ok=True)
        temporary.unlink(missing_ok=True)
        self._sync_directory(self.traces)
        self._sync_directory(self.manifests)
        self._delete_marker(run_id).unlink()
        self._sync_directory(self.deletions)

    def _attempts(self, record):
        # 纳入进程突然退出前刚刚预留的分段，即使界面清单回调
        # 尚未来得及提交该分段的身份信息。
        ids = [value["attempt_id"] for value in record["attempts"]]
        folder = self._inside(self.traces / record["run_id"] / "attempts", self.traces)
        if folder.exists():
            for path in sorted(
                folder.glob("attempt-*"), key=lambda value: value.stat().st_mtime_ns
            ):
                if path.name not in ids and _COMPONENT.fullmatch(path.name):
                    ids.append(path.name)
        return ids

    def _events(self, run_id, attempt_id):
        if not _COMPONENT.fullmatch(attempt_id):
            raise WebError("not_found", "未找到该执行记录。", 404)
        folder = self._inside(
            self.traces / run_id / "attempts" / attempt_id, self.traces / run_id / "attempts"
        )
        path = self._inside(folder / "events.jsonl", folder)
        if not path.exists():
            return []
        if path.stat().st_size > 2_000_000:
            raise WebError("history_unavailable", "审计记录超过展示上限。", 503)
        raw = path.read_bytes()
        lines = raw.splitlines()
        if raw and not raw.endswith(b"\n"):
            lines = lines[:-1]  # 只有仍在写入的末行可以不完整。
        events = []
        for line in lines:
            try:
                event = m.TraceEvent.model_validate_json(line)
                if event.run_id != run_id or event.seq != len(events) + 1:
                    raise ValueError("event ownership or sequence")
                events.append({**event.model_dump(mode="json"), "attempt_id": attempt_id})
            except ValueError:
                raise WebError("history_unavailable", "审计记录未通过校验。", 503) from None
        return events

    def _artifact(self, run_id, attempt_id, ref):
        if not _COMPONENT.fullmatch(ref):
            raise WebError("not_found", "未找到该调用数据。", 404)
        attempt = self._inside(
            self.traces / run_id / "attempts" / attempt_id,
            self.traces / run_id / "attempts",
        )
        folder = self._inside(attempt / "artifacts", attempt)
        path = self._inside(folder / f"{ref}.json", folder)
        try:
            if path.stat().st_size > 256_000:
                raise ValueError("artifact size")
            return json.loads(path.read_bytes())
        except FileNotFoundError:
            raise WebError("not_found", "未找到该调用数据。", 404) from None
        except ValueError:
            raise WebError("history_unavailable", "调用数据无法读取。", 503) from None

    async def artifact(self, run_id, attempt_id, ref):
        return await disk(self._referenced_artifact, run_id, attempt_id, ref)

    def _referenced_artifact(self, run_id, attempt_id, ref):
        record = self._load(run_id)
        if attempt_id not in self._attempts(record):
            raise WebError("not_found", "未找到该执行记录。", 404)
        if not any(event["data_ref"] == ref for event in self._events(run_id, attempt_id)):
            raise WebError("not_found", "未找到该调用数据。", 404)
        return self._artifact(run_id, attempt_id, ref)

    async def view(self, run_id):
        return await disk(self._view, run_id)

    def _view(self, run_id):
        record = self._load(run_id)
        events, attempts, evidence, sources = [], [], {}, {}
        latest = []
        for identity in self._attempts(record):
            latest = self._events(run_id, identity)
            attempts.append({"attempt_id": identity, "event_count": len(latest)})
            events.extend(latest)
            if len(events) > 5000:
                raise WebError("history_unavailable", "运行记录超过展示上限。", 503)
            for event in latest:
                ref = event["data_ref"]
                if ref and event["event_type"] == "evidence.added":
                    value = m.EvidenceRecord.model_validate(self._artifact(run_id, identity, ref))
                    evidence.setdefault(value.evidence_id, value.model_dump(mode="json"))
                elif ref and event["event_type"] == "catalog.call.completed":
                    value = m.CatalogResult.model_validate(self._artifact(run_id, identity, ref))
                    sources.update(
                        {
                            source.source_id: source.model_dump(mode="json")
                            for source in value.sources
                        }
                    )
        result = record.get("result")
        counts = result["call_counts"] if result else {}
        if not result:
            for event in latest:
                counts = event["detail"].get("call_counts", counts)
        started = [event for event in latest if event["event_type"] == "tool.call.started"]
        metrics = {
            "tools": counts.get("tools", len(started)),
            "steps": counts.get("steps", len({event["step_id"] for event in started})),
            "model_calls": counts.get("model_calls", 0),
            "evidence": len(evidence),
            "attempts": len(attempts),
            "duration_ms": result["duration_ms"]
            if result
            else max(
                0,
                int(
                    (
                        datetime.now(UTC) - datetime.fromisoformat(record["created_at"])
                    ).total_seconds()
                    * 1000
                ),
            ),
        }
        if result:
            # 浏览器和导出使用方需要的是逻辑审计位置，
            # 不应得到操作人员本地文件系统的绝对路径。
            result["trace_ref"] = (
                f"{run_id}/attempts/{attempts[-1]['attempt_id']}/events.jsonl" if attempts else None
            )
        return {
            **record,
            "attempts": attempts,
            "events": events,
            "evidence": list(evidence.values()),
            "sources": list(sources.values()),
            "metrics": metrics,
            "failure": _display_failure(record, events),
        }
