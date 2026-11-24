"""单宿主持续任务调度：持久触发、保守预留及核心终态对账。

本层不执行工具，不推测未知调用是否成功，也不刷新旧 run 的预算。周期调度
只领取持久待办；进程内 Task 是执行载体，SQLite 才保存待办和身份所有权。
"""

from __future__ import annotations

import asyncio
import logging
import math
import time

from pydantic import ValidationError

from .. import models as m
from ..state import StateStoreError
from ..web.storage import WebError
from .handoff import continuation, from_snapshot
from .models import MissionCreate, MissionError
from .store import MissionStore

_LOG = logging.getLogger(__name__)


def _public(record):
    """存储与公开视图分离，不发布签名或内部完整执行配置。"""
    return {
        key: value for key, value in record.items() if key not in {"profile_signature", "request"}
    }


class MissionService:
    """复用 RunService 的受控执行；可由 start 后台驱动或显式调用 tick。"""

    def __init__(self, runs, *, clock=time.time, poll_interval=1.0):
        if not math.isfinite(poll_interval) or not 0.01 <= poll_interval <= 60:
            raise ValueError("poll_interval must be between 0.01 and 60 seconds")
        self.runs = runs
        self.clock = clock
        self.poll_interval = poll_interval
        self.store = MissionStore(runs.repository.root / "missions.sqlite3")
        self.lock = asyncio.Lock()
        self.wakeup = asyncio.Event()
        self.task = None
        self.closing = False
        self.last_error = None

    def _live(self):
        if self.closing or self.runs.closing:
            raise MissionError("service_stopping", "服务正在停止。", 503)
        if self.last_error is not None:
            raise MissionError(
                "mission_scheduler_failed", "持续任务调度已停止，请检查记录后重启。", 503
            )

    async def start(self):
        """只在 RunService 取得工作区独占锁之后启动一个调度宿主。"""
        if self.task is not None:
            return
        await self.store.start()
        self.task = asyncio.create_task(self._loop(), name="mission-scheduler")

    async def _loop(self):
        while not self.closing:
            self.wakeup.clear()
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # 不能吞掉磁盘/完整性错误并继续创建新 run；日志只记录稳定分类。
                code = "state_store" if isinstance(error, StateStoreError) else "scheduler_error"
                _LOG.error("Mission scheduler stopped code=%s", code)
                self.last_error = {
                    "code": "mission_scheduler_failed",
                    "message": "持久调度发生错误，已停止派发。",
                }
                return
            if self.closing:
                return
            try:
                await asyncio.wait_for(self.wakeup.wait(), timeout=self.poll_interval)
            except TimeoutError:
                pass

    async def create(self, payload):
        """先在一个事务中保存目标和首次触发，HTTP取消不丢已接受的任务。"""
        payload = MissionCreate.model_validate(payload.model_dump(mode="json"))
        async with self.lock:
            self._live()
            profile = self.runs.profiles.runnable(payload.profile_id)
            record = await self.store.create(
                payload,
                profile_id=profile.metadata.profile_id,
                profile_signature=profile.signature(),
                now=self.clock(),
            )
            self.wakeup.set()
            return _public(record)

    async def listing(self):
        return [_public(record) for record in await self.store.listing()]

    async def view(self, mission_id):
        snapshot = await self.store.detail(mission_id)
        record = _public(snapshot["mission"])
        record["triggers"] = [_public(trigger) for trigger in snapshot["triggers"]]
        return record

    async def _status(self, mission_id, status):
        async with self.lock:
            self._live()
            record = await self.store.set_status(mission_id, status, now=self.clock())
            self.wakeup.set()
            return _public(record)

    async def pause(self, mission_id):
        """阻止后续领取；已开始的周期继续遵守原取消和清理协议。"""
        return await self._status(mission_id, "paused")

    async def resume(self, mission_id):
        """只恢复人工暂停；blocked 不能借此规避未知结果或预算。"""
        return await self._status(mission_id, "active")

    async def complete(self, mission_id):
        """明确结束调度；已有执行安全收尾，完成状态不宣称答案质量。"""
        return await self._status(mission_id, "completed")

    async def wake(self, mission_id, event_id):
        async with self.lock:
            self._live()
            trigger = await self.store.enqueue(mission_id, event_id, now=self.clock())
            self.wakeup.set()
            return _public(trigger)

    async def cancel_run(self, run_id):
        """验证当前触发器，且仅在运行接纳取消后暂停所属目标。"""

        async def prepare(run):
            # 只依赖清单及持久触发器，不读取可能损坏的审计展示。
            mission_id = run.get("mission_id")
            if mission_id is None:
                return None
            mission = await self.store.get(mission_id)
            trigger_id = mission["active_trigger_id"]
            if trigger_id is None or trigger_id != run.get("mission_trigger_id"):
                raise WebError("not_running", "该运行已不再是持续任务的当前周期。")
            trigger = await self.store.get_trigger(trigger_id)
            if (
                trigger["status"] != "running"
                or trigger["mission_id"] != mission_id
                or trigger["run_id"] != run_id
            ):
                raise WebError("mission_run_conflict", "运行与持续任务的当前触发记录不匹配。")
            if mission["status"] != "active":
                return None

            async def pause_owner():
                await self.store.set_status(mission_id, "paused", now=self.clock())

            return pause_owner

        # 接纳、暂停和收尾全在调度锁内，任务不能在这期间领取下一周期。
        async with self.lock:
            return await self.runs.cancel(run_id, prepare=prepare)

    def health(self):
        return {
            "running": self.task is not None and not self.task.done() and not self.closing,
            "error": self.last_error,
        }

    async def tick(self):
        """一次有界调度，先结算/对账旧身份，再在剩余并发容量内领取待办。"""
        async with self.lock:
            if self.closing:
                return
            now = self.clock()
            await self.store.schedule_due(now=now)
            for trigger in await self.store.open_triggers():
                await self._reconcile(trigger)
            for trigger in await self.store.pending():
                mission = await self.store.get(trigger["mission_id"])
                if mission["status"] != "active":
                    continue
                if self.clock() >= mission["deadline_at"]:
                    await self.store.claim(
                        trigger["trigger_id"], max_model_calls=1, max_data_calls=1, now=self.clock()
                    )
                    continue
                if not self.runs.capacity_available():
                    break
                try:
                    profile = self.runs.profiles.runnable(mission["profile_id"])
                    if profile.signature() != mission["profile_signature"]:
                        raise WebError("profile_changed", "执行配置已改变。")
                except WebError as error:
                    await self.store.block(trigger["trigger_id"], error.code, now=self.clock())
                    continue
                claimed = await self.store.claim(
                    trigger["trigger_id"],
                    max_model_calls=profile.run_config.max_model_calls,
                    max_data_calls=profile.run_config.max_data_calls,
                    now=self.clock(),
                )
                if claimed is not None:
                    await self._reconcile(claimed)

    async def _reconcile(self, trigger):
        """不以网页状态替代核心事实；所有异常身份保持原预留并明确阻塞。"""
        if self.runs.active(trigger["run_id"]):
            return
        try:
            mission = await self.store.get(trigger["mission_id"])
            snapshot = await self.runs.core_snapshot(trigger["run_id"])
            if snapshot is not None:
                if trigger["request"] != snapshot["request"]:
                    raise WebError("mission_run_conflict", "核心请求与触发记录不匹配。")
                if snapshot.get("result") is not None:
                    result = m.RunResult.model_validate(snapshot["result"])
                    await self.runs.project_mission_terminal(trigger, mission, snapshot)
                    await self.store.settle(
                        trigger["trigger_id"],
                        model_calls=result.call_counts.model_calls,
                        data_calls=result.call_counts.catalog + result.call_counts.tools,
                        run_status=result.status,
                        stop_reason=result.stop_reason,
                        handoff=from_snapshot(snapshot),
                        now=self.clock(),
                    )
                    return
            # 结束后没有执行者和核心终态时，显式保留未结算周期及其预留。
            if mission["status"] == "completed":
                await self.store.block(
                    trigger["trigger_id"], "mission_run_unresolved", now=self.clock()
                )
                return
            # 暂停仍可以由用户恢复原周期，不擅自改变触发器状态。
            if mission["status"] != "active":
                return
            if self.clock() >= mission["deadline_at"]:
                raise WebError("mission_deadline_reached", "持续任务的原截止时间已到。")
            try:
                manifest = await self.runs.repository.load(trigger["run_id"])
            except WebError as error:
                if error.code != "not_found":
                    raise
                manifest = None
            if manifest is not None and manifest["status"] in {"blocked", "failed"}:
                code = (manifest.get("error") or {}).get("code")
                if code in {"storage_error", "store_closed", "schema_mismatch", "corrupt_state"}:
                    raise StateStoreError(code)
                raise WebError(code or "mission_run_failed", "既有运行已停止自动执行。")
            if not self.runs.capacity_available():
                return
            if trigger["request"] is None:
                profile = self.runs.profiles.runnable(mission["profile_id"])
                if profile.signature() != mission["profile_signature"]:
                    raise WebError("profile_changed", "执行配置已改变。")
                config = profile.run_config.model_copy(
                    update={
                        "max_model_calls": trigger["reserved_model_calls"],
                        "max_data_calls": trigger["reserved_data_calls"],
                        "max_wall_time_ms": min(
                            profile.run_config.max_wall_time_ms,
                            max(1, int((mission["deadline_at"] - self.clock()) * 1000)),
                        ),
                    },
                    deep=True,
                )
                request = m.RunRequest(
                    run_id=trigger["run_id"],
                    task_id=f"mission:{mission['mission_id']}",
                    question=mission["question"],
                    config=config,
                    continuation=continuation(
                        mission["mission_id"], trigger["cycle"], trigger["handoff"]
                    ),
                )
                trigger = await self.store.mark_running(
                    trigger["trigger_id"], request=request.model_dump(mode="json"), now=self.clock()
                )
            else:
                request = m.RunRequest.model_validate(trigger["request"])
            await self.runs.submit_mission(
                request,
                mission_id=mission["mission_id"],
                trigger_id=trigger["trigger_id"],
                profile_id=mission["profile_id"],
                profile_signature=mission["profile_signature"],
                deadline_at=mission["deadline_at"],
            )
        except StateStoreError as error:
            # 本地状态库不可用时不能把一个触发器伪装成业务失败后继续
            # 派发其它任务；否则本轮可能在持久事实不完整时扩大副作用。
            if error.code in {"storage_error", "store_closed", "schema_mismatch", "corrupt_state"}:
                # 核心运行日志可能损坏，而 Mission SQLite 仍然可写。
                # 尽力标出当前 owner 并保留预留，随后仍向调度器传播原故障。
                try:
                    await self.store.block(trigger["trigger_id"], error.code, now=self.clock())
                except Exception:
                    _LOG.error("Mission trigger block failed code=state_store")
                raise
            if error.code == "lease_conflict":
                return
            await self.store.block(trigger["trigger_id"], error.code, now=self.clock())
        except WebError as error:
            if error.status >= 500:
                raise
            if error.code in {"capacity_reached", "service_stopping", "lease_conflict"}:
                return
            await self.store.block(trigger["trigger_id"], error.code, now=self.clock())
        except (ValidationError, ValueError, KeyError, TypeError):
            await self.store.block(
                trigger["trigger_id"], "mission_request_invalid", now=self.clock()
            )

    async def close(self):
        """先停止调度再释放本层存储；RunService资源仍由创建它的宿主管理。"""
        async with self.lock:
            self.closing = True
            self.wakeup.set()
        try:
            if self.task is not None:
                await self.task
        finally:
            await self.store.aclose()
