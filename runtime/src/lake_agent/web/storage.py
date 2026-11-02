"""Private presentation metadata and a bounded projection of committed audit data.

The core SQLite state remains the sole recovery source. This repository stores
only UI metadata, reads validated audit events, and follows references belonging
to the requested run/attempt. All filesystem operations run in observed workers.
A process lock rejects multiple web owners of one data directory.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

from .. import models as m
from ..validation import canonical_json

_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}\Z")
_RUN = re.compile(r"run-[0-9a-f]{32}\Z")


class WebError(Exception):
    """Fixed public code/message; no paths, stored data or raw exceptions leak."""

    def __init__(self, code, message, status=409):
        self.code, self.message, self.status = code, message, status
        super().__init__(message)


def utc_now():
    return datetime.now(UTC).isoformat()


async def disk(method, *args):
    """Drain filesystem work on cancellation so writes cannot outlive ownership."""
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
        # Observe a possible worker exception without replacing cancellation.
        if job.done() and not job.cancelled():
            job.exception()
        raise


class Repository:
    """UI manifests, ordered trace segments and referenced artifacts only."""

    def __init__(self, root):
        self.root = Path(root).expanduser().resolve()
        self.manifests = self.root / "metadata"
        self.traces = self.root / "traces"
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
            raise WebError("directory_busy", "该演示目录已由另一个服务使用。") from None
        self.descriptor = fd
        self.manifests.mkdir(mode=0o700, exist_ok=True)
        self.traces.mkdir(mode=0o700, exist_ok=True)

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
        # Freeze mutable nested data before scheduling a disk worker.
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
        """Cap browser history while allowing startup to reconcile every record."""
        return await disk(self._listing, limit)

    def _listing(self, limit):
        records = [self._load(path.stem) for path in self.manifests.glob("run-*.json")]
        return sorted(records, key=lambda value: value["created_at"], reverse=True)[:limit]

    def _attempts(self, record):
        # Include a segment reserved immediately before an abrupt process exit,
        # even if the UI manifest callback had not yet committed its identity.
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
            lines = lines[:-1]  # Only an in-progress trailing row may be incomplete.
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
            # Browser/export consumers need a logical audit location, never the
            # operator's absolute local filesystem path.
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
        }
