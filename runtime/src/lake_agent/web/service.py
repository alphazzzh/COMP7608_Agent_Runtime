"""管理网页任务与展示状态，将全部执行工作委托给核心运行时。

一个服务管理一个本地目录。每个任务使用新的适配器与审计接收端，
只共享由调用方管理的 SQLite 存储；关闭前会等待所有任务收尾。
任务管理器不执行工具、不修改核心预算，也不裁定结果未知的调用。
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
    """返回公开索引字段；历史卡片不包含内部恢复标志。"""
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
        # 替换私有绝对路径前，先复制公开结果；
        # 调用方不得修改正在执行的任务所持有的清单。
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
    """记录核心实际采用的尝试身份，并可选择在获得证据后停止。"""

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
            # 此事件发生前，核心工作任务已提交经校验的响应。
            # 取消信号经公开运行接口传播；该接口会先关闭轨迹并释放租约，
            # 随后本服务的任务才结束。
            self.paused = True
            raise asyncio.CancelledError


class DemoService:
    """限制活动任务数量，并串行处理准入，不建立待执行任务积压队列。"""

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
        # 已退出的进程不可能留下活动的 asyncio 任务。此处只核对并修正
        # 展示状态，不自动恢复任何核心运行。
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
        """HTTP 准入请求被取消时，也必须妥善更新已持久化的界面记录。"""
        try:
            await self.repository.save(record)
        except asyncio.CancelledError:
            # save 会先等待其工作线程结束，再传播取消。此时尚未创建任务，
            # 因此保留中断的展示记录，不将其标记为活动状态。
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
            # 上下文持有本次尝试的全部适配器资源。浏览器请求和共享任务管理器
            # 均不负责关闭其他端口。
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
                # 返回结果是否为终态，由持久化日志决定：审计失败时可能返回
                # RunResult，同时有意将持久化运行保留为未结束状态，
                # 以便安全重建。不能将这些尝试
                # 转换为永久的网页终态结果。
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
                # 在拆除适配器前保存。即使清理失败，也必须同时保留
                # 终态答案和非终态的恢复视图。
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
            # 读取磁盘视图会让出事件循环。清单读取后，任务可能已经结束，
            # 因此发布非活动状态前须刷新记录。
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
        # 任务若在协程首条指令执行前被取消，就无法执行自身的 finally。
        # 等确认任务结束后，再核对并修正记录。
        record = await self.repository.load(run_id)
        if record["status"] in {"queued", "running"}:
            record.update(
                status="interrupted", updated_at=utc_now(), has_state=await self._has_state(run_id)
            )
            await self.repository.save(record)

    async def close(self):
        # 任务准入、取消和关闭快照共用同一个同步边界。
        # 在锁外等待任务收尾，因为任务结束不需要准入权限。
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
