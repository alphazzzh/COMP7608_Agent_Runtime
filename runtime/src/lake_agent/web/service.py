"""Own web jobs and presentation state, delegating every execution to the core.

One service owns one local directory. Jobs use fresh adapters and audit sinks,
share only the caller-managed SQLite store, and are drained before shutdown.
The manager does not execute tools, alter core budgets or resolve unknown calls.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from .. import models as m
from ..runtime import resume, run
from ..state import SqliteStateStore, StateStoreError
from ..trace import JsonlTraceSink
from .evaluations import EvaluationRepository
from .profiles import ProfileRegistry
from .storage import Repository, WebError, utc_now

_LOG = logging.getLogger(__name__)


def summary(record):
    """Public index fields; internal recovery flags stay out of history cards."""
    names = (
        "run_id",
        "scenario",
        "question",
        "mode",
        "status",
        "created_at",
        "updated_at",
        "error",
        "result",
        "profile_id",
        "profile",
    )
    value = {name: record.get(name) for name in names}
    if value["result"] is not None:
        # Detach the public result before replacing the private absolute path;
        # callers must not mutate the manifest owned by an executing job.
        attempts = record.get("attempts", [])
        value["result"] = {
            **value["result"],
            "trace_ref": (
                f"{record['run_id']}/attempts/{attempts[-1]['attempt_id']}/events.jsonl"
                if attempts
                else None
            ),
        }
    return value


class PresentationTrace(JsonlTraceSink):
    """Record the exact core attempt identity and optionally stop after evidence."""

    def __init__(self, root, on_attempt, *, pause=False):
        super().__init__(root)
        self.on_attempt = on_attempt
        self.pause = pause
        self.paused = False

    async def create_attempt(self, run_id, attempt_id):
        await super().create_attempt(run_id, attempt_id)
        await self.on_attempt(attempt_id)

    async def append(self, event):
        await super().append(event)
        if self.pause and not self.paused and event.event_type == "evidence.added":
            # The core worker committed its validated reply before this event.
            # Cancellation propagates through the public run boundary, which
            # closes its trace and releases its lease before our job settles.
            self.paused = True
            raise asyncio.CancelledError


class DemoService:
    """Bound active jobs and serialize admission without a queued work backlog."""

    def __init__(self, data_dir, max_active_runs=2, *, profiles=None):
        if type(max_active_runs) is not int or max_active_runs < 1:
            raise ValueError("max_active_runs must be a positive integer")
        self.repository = Repository(data_dir)
        self.evaluations = EvaluationRepository(self.repository.root)
        self.profiles = ProfileRegistry(profiles)
        self.state_store = SqliteStateStore(self.repository.root / "state.sqlite3")
        self.max_active_runs = max_active_runs
        self.tasks = {}
        self.lock = asyncio.Lock()
        self.closing = False

    async def start(self):
        await self.repository.start()
        await self.evaluations.start()
        # A departed process cannot leave an active asyncio job. This is only
        # presentation reconciliation; no core run is resumed automatically.
        for record in await self.repository.listing(limit=None):
            if record["status"] in {"running", "queued"}:
                record.update(status="interrupted", updated_at=utc_now())
                record["has_state"] = await self._has_state(record["run_id"])
                await self.repository.save(record)

    def active(self, run_id):
        task = self.tasks.get(run_id)
        return task is not None and not task.done()

    def _admit(self):
        if self.closing:
            raise WebError("service_stopping", "服务正在停止。", 503)
        if sum(not task.done() for task in self.tasks.values()) >= self.max_active_runs:
            raise WebError("capacity_reached", "已有运行进行中，请稍后再试。", 429)

    def _spawn(self, record, *, recovering):
        run_id = record["run_id"]
        task = asyncio.create_task(
            self._execute(record, recovering=recovering), name=f"web-demo:{run_id}"
        )
        self.tasks[run_id] = task

        def observed(done):
            if self.tasks.get(run_id) is done:
                self.tasks.pop(run_id, None)
            if not done.cancelled():
                error = done.exception()
                if error is not None:
                    _LOG.error("Demo job could not settle: %s", type(error).__name__)

        task.add_done_callback(observed)

    async def _schedule(self, record, *, recovering):
        """An abandoned HTTP admission must settle its persisted UI record."""
        try:
            await self.repository.save(record)
        except asyncio.CancelledError:
            # save drains its worker before propagating. No job was spawned,
            # so retain the interrupted presentation without claiming activity.
            record.update(status="interrupted", updated_at=utc_now())
            await self.repository.save(record)
            raise
        self._spawn(record, recovering=recovering)

    async def create(self, question, scenario, mode, *, profile_id=None):
        async with self.lock:
            self._admit()
            profile = self.profiles.runnable(profile_id or f"demo-{scenario or 'standard'}")
            legacy_scenario = profile.metadata.profile_id.removeprefix("demo-")
            if scenario is None and legacy_scenario in {"standard", "parallel", "recovery"}:
                scenario = legacy_scenario
            now = utc_now()
            record = {
                "run_id": f"run-{uuid4().hex}",
                "question": question,
                "scenario": scenario,
                "profile_id": profile.metadata.profile_id,
                "profile": profile.public(),
                "profile_signature": profile.signature(),
                "mode": mode,
                "status": "queued",
                "created_at": now,
                "updated_at": now,
                "result": None,
                "error": None,
                "attempts": [],
                "has_state": False,
            }
            await self._schedule(record, recovering=False)
            return summary(record)

    async def restart(self, run_id):
        async with self.lock:
            record = await self.repository.load(run_id)
            if self.active(run_id):
                raise WebError("already_running", "该任务仍在运行。")
            if record["result"] is not None or record["status"] not in {"paused", "interrupted"}:
                raise WebError("not_resumable", "该运行当前不支持继续恢复。")
            if not record.get("has_state"):
                raise WebError("not_resumable", "该运行尚未建立可恢复状态。")
            if len(record["attempts"]) >= 20:
                raise WebError("attempt_limit", "该演示已达到执行尝试上限。", 429)
            self.profiles.for_record(record)
            self._admit()
            record.update(status="queued", error=None, updated_at=utc_now())
            await self._schedule(record, recovering=True)
            return summary(record)

    async def _has_state(self, run_id):
        try:
            lease = await self.state_store.acquire(run_id)
        except StateStoreError as error:
            if error.code == "run_not_found":
                return False
            raise
        await self.state_store.release(lease)
        return True

    async def _execute(self, record, *, recovering):
        run_id = record["run_id"]
        core_settled = False

        async def attempt_started(attempt_id):
            record["attempts"].append({"attempt_id": attempt_id, "started_at": utc_now()})
            record["has_state"] = True
            await self.repository.save(record)

        sink = PresentationTrace(
            self.repository.traces,
            attempt_started,
        )
        try:
            profile = self.profiles.for_record(record)
            sink.pause = profile.pause_after_evidence and not recovering
            record.update(status="running", updated_at=utc_now())
            await self.repository.save(record)
            # The context owns all per-attempt adapter resources. Neither a
            # browser request nor the shared job manager closes sibling ports.
            async with profile.ports_factory() as bound:
                ports = dict(
                    data=bound.data,
                    content=bound.content,
                    planner=bound.planner,
                    trace=sink,
                    state_store=self.state_store,
                )
                if recovering:
                    result = await resume(run_id, **ports)
                else:
                    request = m.RunRequest(
                        run_id=run_id,
                        task_id=f"web:{profile.metadata.dataset_id}",
                        question=record["question"],
                        mode=record["mode"],
                        config=profile.run_config.model_copy(deep=True),
                    )
                    result = await run(request, **ports)
                # The journal decides whether a returned result is terminal:
                # audit failures can return a RunResult while deliberately
                # leaving the durable run open for safe reconstruction. Do not
                # turn those attempts into permanent web terminal outcomes.
                lease = await self.state_store.acquire(run_id)
                try:
                    saved = await self.state_store.load(lease)
                finally:
                    await self.state_store.release(lease)
                terminal = saved.state["result"]
                record.update(result=terminal, has_state=True)
                if terminal is None:
                    record.update(
                        status="interrupted",
                        error={
                            "code": result.stop_reason,
                            "message": "本次执行的审计未完成；可尝试恢复，安全性仍由内核检查。",
                        },
                    )
                else:
                    record.update(status=terminal["status"])
                # Save before adapter teardown. A cleanup failure must preserve
                # both a terminal answer and a nonterminal recovery projection.
                core_settled = True
                await self.repository.save(record)
        except asyncio.CancelledError:
            if core_settled:
                record["error"] = {
                    "code": "adapter_cleanup_cancelled",
                    "message": "运行状态已保存；执行模块的资源清理被中断。",
                }
            else:
                record.update(status="paused" if sink.paused else "interrupted")
                record["has_state"] = await self._has_state(run_id)
        except Exception as error:
            if core_settled:
                _LOG.exception("Adapter cleanup failed after core settlement")
                record["error"] = {
                    "code": "adapter_cleanup_failed",
                    "message": "运行状态已保存；执行模块的资源清理发生错误。",
                }
            elif isinstance(error, StateStoreError):
                record.update(
                    status="blocked",
                    error={"code": error.code, "message": self._recovery_message(error.code)},
                )
            elif isinstance(error, WebError):
                record.update(
                    status="blocked", error={"code": error.code, "message": error.message}
                )
            else:
                _LOG.exception("Workspace job failed")
                record.update(
                    status="failed",
                    error={
                        "code": "run_failed",
                        "message": "运行未完成，请查看已保存的执行记录。",
                    },
                )
        finally:
            record["updated_at"] = utc_now()
            await self.repository.save(record)

    @staticmethod
    def _recovery_message(code):
        return {
            "recovery_outcome_unknown": "存在结果未确认的调用，已停止自动恢复，避免重复执行。",
            "recovery_deadline_expired": "原运行期限已到；恢复不会重置时间预算。",
            "recovery_version_mismatch": "运行时版本或源码已改变，不能恢复旧版本运行。",
            "recovery_contract_mismatch": "数据工具契约已改变，不能继续原运行。",
        }.get(code, "持久状态未通过恢复检查，已停止继续调用。")

    async def view(self, run_id):
        view = await self.repository.view(run_id)
        active = self.active(run_id)
        if not active and view["status"] in {"queued", "running"}:
            # Disk projection yields the loop. The job may settle after its
            # manifest was read, so refresh before publishing inactive status.
            view = await self.repository.view(run_id)
            active = self.active(run_id)
        can_resume = (
            not active
            and view["status"] in {"paused", "interrupted"}
            and view.get("has_state", False)
            and len(view["attempts"]) < 20
        )
        resume_blocked_reason = None
        if can_resume:
            try:
                self.profiles.for_record(view)
            except WebError as error:
                can_resume = False
                resume_blocked_reason = error.message
        view.pop("has_state", None)
        view.pop("profile_signature", None)
        return {
            **view,
            "active": active,
            "can_resume": can_resume,
            "resume_blocked_reason": resume_blocked_reason,
        }

    async def listing(self):
        return [summary(record) for record in await self.repository.listing()]

    async def cancel(self, run_id):
        async with self.lock:
            await self.repository.load(run_id)
            task = self.tasks.get(run_id)
            if task is None or task.done():
                raise WebError("not_running", "该运行已经停止。")
            if not task.cancelling():
                task.cancel()
            await asyncio.shield(asyncio.gather(task, return_exceptions=True))
            await self._settle_unstarted(run_id)
            return summary(await self.repository.load(run_id))

    async def _settle_unstarted(self, run_id):
        # A task cancelled before its coroutine's first instruction cannot run
        # its own finally. Reconcile it after observing task completion.
        record = await self.repository.load(run_id)
        if record["status"] in {"queued", "running"}:
            record.update(
                status="interrupted", updated_at=utc_now(), has_state=await self._has_state(run_id)
            )
            await self.repository.save(record)

    async def close(self):
        # Admission, cancellation and the shutdown snapshot share one boundary.
        # Drain outside the lock: job settlement does not need admission rights.
        async with self.lock:
            self.closing = True
            jobs = list(self.tasks.items())
        try:
            for _, task in jobs:
                if not task.done() and not task.cancelling():
                    task.cancel()
            if jobs:
                await asyncio.gather(*(task for _, task in jobs), return_exceptions=True)
            for run_id, _ in jobs:
                await self._settle_unstarted(run_id)
        finally:
            try:
                await self.state_store.aclose()
            finally:
                await self.repository.close()
