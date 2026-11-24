"""管理网页任务与展示状态，将全部执行工作委托给核心运行时。

一个服务管理一个本地目录。每个任务使用新的适配器与审计接收端，
只共享由调用方管理的 SQLite 存储；关闭前会等待所有任务收尾。
任务管理器不执行工具、不修改核心预算，也不裁定结果未知的调用。
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from uuid import uuid4

from .. import models as m
from ..runtime import resume, run
from ..state import SqliteStateStore, StateStoreError
from ..trace import JsonlTraceSink
from ..validation import canonical_json
from .evaluation_storage import EvaluationRepository
from .profiles import ProfileRegistry
from .storage import Repository, WebError, utc_now

_LOG = logging.getLogger(__name__)


def summary(record):
    """返回公开索引字段；历史卡片不包含内部恢复标志。"""
    names = (
        "run_id",
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
    if record.get("mission_id"):
        value["mission_id"] = record["mission_id"]
        value["mission_trigger_id"] = record["mission_trigger_id"]
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


def _mission_manifest(
    request,
    *,
    mission_id,
    trigger_id,
    profile_id,
    profile_signature,
    deadline_at,
    snapshot=None,
    profile=None,
):
    """重建可丢失的展示投影；缺失旧公开配置时留空，不冒充当前配置。"""
    now = utc_now()
    return {
        "run_id": request.run_id,
        "question": request.question,
        "profile_id": profile_id,
        "profile": profile,
        "profile_signature": profile_signature,
        "mode": request.mode,
        "status": "queued",
        "created_at": snapshot["started_at"] if snapshot else now,
        "updated_at": now,
        "result": None,
        "error": None,
        "attempts": [
            {"attempt_id": identity, "started_at": snapshot["started_at"]}
            for identity in (snapshot or {}).get("attempts", [])
        ],
        "has_state": snapshot is not None,
        "mission_id": mission_id,
        "mission_trigger_id": trigger_id,
        "mission_request": request.model_dump(mode="json"),
        "mission_deadline_at": deadline_at,
    }


class PresentationTrace(JsonlTraceSink):
    """记录核心实际采用的尝试身份，不改变运行的执行与取消行为。"""

    def __init__(self, root, on_attempt):
        super().__init__(root)
        self.on_attempt = on_attempt

    async def create_attempt(self, run_id, attempt_id):
        await super().create_attempt(run_id, attempt_id)
        await self.on_attempt(attempt_id)


class RunService:
    """限制活动任务数量，并串行处理准入，不建立待执行任务积压队列。"""

    def __init__(self, data_dir, max_active_runs=2, *, profiles=None, enable_evaluations=False):
        if type(max_active_runs) is not int or max_active_runs < 1:
            raise ValueError("max_active_runs must be a positive integer")
        if type(enable_evaluations) is not bool:
            raise ValueError("enable_evaluations must be a boolean")
        self.repository = Repository(data_dir)
        # 报告能力由可信宿主显式开启，正式工作区不初始化评估目录。
        self.evaluations = (
            EvaluationRepository(self.repository.root) if enable_evaluations else None
        )
        self.profiles = ProfileRegistry(profiles)
        self.state_store = SqliteStateStore(self.repository.root / "state.sqlite3")
        self.max_active_runs = max_active_runs
        self.tasks = {}
        self.lock = asyncio.Lock()
        self.closing = False

    async def start(self):
        await self.repository.start()
        for run_id in await self.repository.pending_deletions():
            await self._delete_owned(run_id)
        if self.evaluations is not None:
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

    def capacity_available(self):
        """提供调度提示；实际提交仍在准入锁内再次核对容量。"""
        return (
            not self.closing
            and sum(not task.done() for task in self.tasks.values()) < self.max_active_runs
        )

    def _spawn(self, record, *, recovering):
        run_id = record["run_id"]
        execute = (
            self._execute_with_deadline
            if record.get("mission_deadline_at") is not None
            else self._execute
        )
        task = asyncio.create_task(execute(record, recovering=recovering), name=f"web-run:{run_id}")
        self.tasks[run_id] = task

        def observed(done):
            if self.tasks.get(run_id) is done:
                self.tasks.pop(run_id, None)
            if not done.cancelled():
                error = done.exception()
                if error is not None:
                    _LOG.error("Workspace job could not settle: %s", type(error).__name__)

        task.add_done_callback(observed)

    async def _execute_with_deadline(self, record, *, recovering):
        """冻结请求可能在停机后首次派发；外层仍遵守Mission不可变的总期限。"""
        timeout = asyncio.timeout(max(0, record["mission_deadline_at"] - time.time()))
        try:
            async with timeout:
                await self._execute(record, recovering=recovering)
        except TimeoutError:
            await self._settle_unstarted(record["run_id"])
        if timeout.expired():
            record = await self.repository.load(record["run_id"])
            record["error"] = {
                "code": "mission_deadline_reached",
                "message": "持续任务总期限已到，已停止本次执行并等待资源清理。",
            }
            await self.repository.save(record)

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

    async def create(self, question, mode="full_agent", *, profile_id=None):
        """按已注册配置准入新任务，不构造默认端口或推测配置身份。"""
        async with self.lock:
            self._admit()
            profile = self.profiles.runnable(profile_id)
            now = utc_now()
            record = {
                "run_id": f"run-{uuid4().hex}",
                "question": question,
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
            if record.get("mission_id"):
                raise WebError("mission_owned", "请通过持续任务管理所属运行。")
            if self.active(run_id):
                raise WebError("already_running", "该任务仍在运行。")
            if record["result"] is not None or record["status"] != "interrupted":
                raise WebError("not_resumable", "该运行当前不支持继续恢复。")
            if not record.get("has_state"):
                raise WebError("not_resumable", "该运行尚未建立可恢复状态。")
            if len(record["attempts"]) >= 20:
                raise WebError("attempt_limit", "该运行已达到执行尝试上限。", 429)
            self.profiles.for_record(record)
            self._admit()
            record.update(status="queued", error=None, updated_at=utc_now())
            await self._schedule(record, recovering=True)
            return summary(record)

    async def core_snapshot(self, run_id):
        """读取已释放执行租约的核心事实；展示清单不充当预算结算依据。

        调用方须先排除活动任务。读取过去终态不执行端口，也不刷新原期限。
        缺失状态与损坏状态严格区分，后者不能退回首次执行。
        """
        try:
            lease = await self.state_store.acquire(run_id)
        except StateStoreError as error:
            if error.code == "run_not_found":
                return None
            raise
        try:
            saved = await self.state_store.load(lease)
            request = m.RunRequest.model_validate(saved.state["request"])
            if request.run_id != run_id:
                raise StateStoreError("recovery_state_invalid")
            terminal = saved.state.get("result")
            if terminal is not None:
                parsed = m.RunResult.model_validate(terminal)
                if parsed.run_id != run_id or parsed.task_id != request.task_id:
                    raise StateStoreError("recovery_state_invalid")
            return saved.state
        except (KeyError, ValueError, TypeError):
            raise StateStoreError("recovery_state_invalid") from None
        finally:
            await self.state_store.release(lease)

    async def submit_mission(
        self, request, *, mission_id, trigger_id, profile_id, profile_signature, deadline_at=None
    ):
        """按持久触发记录中的既定身份幂等派发；相同身份不得更换请求。

        此入口仅供可信调度宿主使用，HTTP 不接受 RunRequest 或预算配置。
        核心有状态时只走严格恢复；没有状态才首次执行，未知结果不会换号重做。
        """
        request = m.RunRequest.model_validate(request.model_dump(mode="json"))
        if not request.run_id or request.mode != "full_agent":
            raise WebError("mission_run_policy", "持续任务的运行身份或模式不正确。")
        if deadline_at is not None and (
            type(deadline_at) not in (int, float) or not math.isfinite(deadline_at)
        ):
            raise WebError("mission_run_policy", "持续任务截止时间不正确。")
        frozen = request.model_dump(mode="json")
        async with self.lock:
            try:
                record = await self.repository.load(request.run_id)
            except WebError as error:
                if error.code != "not_found":
                    raise
                record = None
            if record is not None and (
                record.get("mission_id") != mission_id
                or record.get("mission_trigger_id") != trigger_id
                or record.get("mission_request") != frozen
                or record.get("profile_id") != profile_id
                or record.get("profile_signature") != profile_signature
                or record.get("mission_deadline_at") != deadline_at
            ):
                raise WebError("mission_run_conflict", "该执行身份已绑定另一份持久请求。")
            if record is not None and self.active(request.run_id):
                return summary(record)
            profile = self.profiles.runnable(profile_id)
            if profile.signature() != profile_signature:
                raise WebError("profile_changed", "执行配置已改变，持续任务已停止派发。")
            expected = profile.run_config.model_dump(mode="json")
            actual = request.config.model_dump(mode="json")
            for field in ("max_model_calls", "max_data_calls", "max_wall_time_ms"):
                if actual[field] > expected[field]:
                    raise WebError("mission_run_policy", "持续任务不能提高执行配置的预算。")
                expected[field] = actual[field]
            if canonical_json(expected) != canonical_json(actual):
                raise WebError("mission_run_policy", "持续任务不能修改原执行配置的策略。")
            snapshot = await self.core_snapshot(request.run_id)
            if snapshot is not None and snapshot["request"] != frozen:
                raise WebError("mission_run_conflict", "核心状态与持久请求不匹配。")
            if record is None:
                record = _mission_manifest(
                    request,
                    mission_id=mission_id,
                    trigger_id=trigger_id,
                    profile_id=profile_id,
                    profile_signature=profile_signature,
                    deadline_at=deadline_at,
                    snapshot=snapshot,
                    profile=profile.public(),
                )
            if snapshot is not None and snapshot.get("result") is not None:
                record.update(
                    result=snapshot["result"], status=snapshot["result"]["status"], has_state=True
                )
                await self.repository.save(record)
                return summary(record)
            if snapshot is None and record["attempts"]:
                raise WebError("mission_state_missing", "既有运行的核心状态缺失，不能重新执行。")
            if len(record["attempts"]) >= 20:
                raise WebError("attempt_limit", "该运行已达到执行尝试上限。", 429)
            if deadline_at is not None and time.time() >= deadline_at:
                raise WebError("mission_deadline_reached", "持续任务的原截止时间已到。")
            self._admit()
            record.update(status="queued", error=None, updated_at=utc_now())
            await self._schedule(record, recovering=snapshot is not None)
            return summary(record)

    async def project_mission_terminal(self, trigger, mission, snapshot):
        """只投影已核对的核心终态，不打开适配器、不要求当前配置可执行。

        先保存网页投影再结算Mission，保证任一写入窗口重启后均能重试；
        展示记录丢失不丢答案，旧配置缺失也不影响读取历史核心事实。
        """
        request = m.RunRequest.model_validate(snapshot["request"])
        result = m.RunResult.model_validate(snapshot["result"])
        if (
            request.run_id != trigger["run_id"]
            or result.run_id != request.run_id
            or result.task_id != request.task_id
            or snapshot["request"] != trigger["request"]
        ):
            raise WebError("mission_run_conflict", "终态与持续任务请求不匹配。")
        async with self.lock:
            if self.active(request.run_id):
                raise WebError("already_running", "运行仍在收尾。")
            try:
                record = await self.repository.load(request.run_id)
            except WebError as error:
                if error.code != "not_found":
                    raise
                record = _mission_manifest(
                    request,
                    mission_id=mission["mission_id"],
                    trigger_id=trigger["trigger_id"],
                    profile_id=mission["profile_id"],
                    profile_signature=mission["profile_signature"],
                    deadline_at=mission["deadline_at"],
                    snapshot=snapshot,
                )
            if any(
                (record.get(key) != value)
                for key, value in {
                    "mission_id": mission["mission_id"],
                    "mission_trigger_id": trigger["trigger_id"],
                    "mission_request": trigger["request"],
                    "profile_id": mission["profile_id"],
                    "profile_signature": mission["profile_signature"],
                    "mission_deadline_at": mission["deadline_at"],
                }.items()
            ):
                raise WebError("mission_run_conflict", "网页投影与持续任务身份不匹配。")
            record.update(
                result=result.model_dump(mode="json"),
                status=result.status,
                has_state=True,
                updated_at=utc_now(),
            )
            await self.repository.save(record)
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
                    if record.get("mission_id"):
                        request = m.RunRequest.model_validate(record["mission_request"])
                    else:
                        request = m.RunRequest(
                            run_id=run_id,
                            # 每次提问拥有独立审计身份；恢复分支继续使用持久化原请求。
                            task_id=f"web:{run_id}",
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
                record.update(status="interrupted")
                record["has_state"] = await self._has_state(run_id)
        except Exception as error:
            if core_settled:
                code = (
                    "state_store"
                    if isinstance(error, StateStoreError)
                    else "adapter_cleanup_failed"
                )
                _LOG.error("Adapter cleanup failed run_id=%s code=%s", run_id, code)
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
                _LOG.error("Workspace job failed run_id=%s code=run_failed", run_id)
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
            "recovery_engine_mismatch": "规划器执行协议已改变，不能继续原运行。",
            "recovery_contract_mismatch": "数据工具契约已改变，不能继续原运行。",
        }.get(code, "持久状态未通过恢复检查，已停止继续调用。")

    async def view(self, run_id):
        """返回与后台任务活动状态一致的运行快照。"""
        was_active = self.active(run_id)
        view = await self.repository.view(run_id)
        active = self.active(run_id)
        if not active and (was_active or view["status"] in {"queued", "running"}):
            # 核心终态可在资源清理前写入。任务在读盘期间结束时，
            # 刷新记录以取得清理后的时间戳与错误状态。
            view = await self.repository.view(run_id)
            active = self.active(run_id)
        can_resume = (
            not active
            and view["status"] == "interrupted"
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
        view.pop("mission_request", None)
        view.pop("mission_deadline_at", None)
        if view.get("mission_id"):
            can_resume = False
            resume_blocked_reason = "请通过持续任务管理所属运行。"
        return {
            **view,
            "active": active,
            "can_resume": can_resume,
            "resume_blocked_reason": resume_blocked_reason,
        }

    async def listing(self):
        return [summary(record) for record in await self.repository.listing()]

    @staticmethod
    async def _await_shielded(coro):
        """等待受屏蔽的收尾任务完成后再传播取消，避免提前释放锁。"""
        job = asyncio.create_task(coro)
        cancelled = False
        while not job.done():
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        try:
            result = job.result()
        except BaseException:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def delete(self, run_id):
        """串行删除已停止的任务；HTTP 取消也须等清理结束后才交还所有权。"""
        async with self.lock:
            if self.closing:
                raise WebError("service_stopping", "服务正在停止。", 503)
            if not await self.repository.deleting(run_id):
                owned = await self.repository.load(run_id)
                if owned.get("mission_id"):
                    raise WebError("mission_owned", "持续任务关联运行需保留用于对账和审计。")
            if self.active(run_id):
                raise WebError("run_active", "任务仍在运行，请先停止任务再删除。")
            try:
                await self._await_shielded(self._delete_owned(run_id))
            finally:
                self.tasks.pop(run_id, None)

    async def _delete_owned(self, run_id):
        """持久意图 → SQLite 单事务 → 文件清理，任何中断均可安全继续。"""
        prepared = False
        try:
            prepared = await self.repository.prepare_delete(run_id)
            await self.state_store.delete(run_id)
            await self.repository.finish_delete(run_id)
        except StateStoreError as error:
            if error.code == "lease_conflict":
                if prepared:
                    try:
                        await self.repository.cancel_delete(run_id)
                    except OSError:
                        raise WebError(
                            "delete_failed", "删除意图尚未撤销，请重试或重启服务继续清理。", 503
                        ) from None
                raise WebError("run_active", "任务存在活动执行租约，请先停止任务再删除。") from None
            raise WebError(
                "delete_failed", "删除尚未完成；可重试删除或重启服务继续清理。", 503
            ) from None
        except OSError:
            raise WebError(
                "delete_failed", "删除尚未完成；可重试删除或重启服务继续清理。", 503
            ) from None

    async def cancel(self, run_id, *, prepare=None):
        """在准入锁内取消；宿主可先校验归属，再对已接纳的取消持久化状态。"""
        async with self.lock:
            record = await self.repository.load(run_id)
            task = self.tasks.get(run_id)
            if task is None or task.done():
                raise WebError("not_running", "该运行已经停止。")
            on_accepted = await prepare(record) if prepare is not None else None
            # 校验过程中运行可以正常结束。最终检查与发送取消之间不能 await。
            if self.tasks.get(run_id) is not task or task.done():
                raise WebError("not_running", "该运行已经停止。")
            if not task.cancelling():
                task.cancel()
            # HTTP 客户端断开后，也必须在释放准入锁前等暂停和收尾写入完成。
            return await self._await_shielded(self._finish_cancellation(run_id, task, on_accepted))

    async def _finish_cancellation(self, run_id, task, on_accepted):
        """即使暂停写入失败，也等待执行退出并结算未开始的运行。"""
        try:
            if on_accepted is not None:
                await on_accepted()
        finally:
            await asyncio.gather(task, return_exceptions=True)
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
