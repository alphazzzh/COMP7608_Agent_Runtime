"""由控制器管理有界工具批次，并隔离 I/O 工作任务。

只有 DataPort.invoke 在子任务中运行。准入、预算、意图与结果工件、观察记录、证据
和步骤状态转移均留在串行协调器中。一个批次须全部结束后才允许重试或下游工作；
实际完成时序不能决定语义提交顺序。

混入类复用控制器现有的审计、来源追踪和截止时间辅助方法，不管理事件循环或适配器
资源、不发起模型调用，也不从幂等性推断并行安全性。子任务取消依赖协作；
在返回运行收尾流程前，必须等待全部自有任务结束并获取其结果或异常。
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import random

from pydantic import ValidationError

from . import models as m
from .errors import FATAL_TOOL_CODES, RunStopped
from .execution import ToolAttempt, invoke_tool_attempt, select_ready_wave
from .validation import canonical_json


def _meta(request):
    return dict(
        operation_id=request.operation_id,
        call_id=request.call_id,
        attempt=request.attempt,
        step_id=request.step_id,
    )


class ToolExecutionMixin:
    """为已接受的 ToolPlan 管理单次运行内的工具状态并进行串行协调。"""

    def _init_tool_execution(self):
        self.pending_tool_calls: dict[str, dict] = {}
        self.active_tool_calls: dict[str, dict] = {}
        self.tool_attempts: dict[str, ToolAttempt] = {}
        self.started_tool_steps: set[str] = set()
        self.counted_tool_retries: set[str] = set()
        self.tool_step_terminal: dict[str, str] = {}

    async def _tool_step_state(self, step_id, status):
        """记录已提交的步骤终态，防止后续预算失败撤销这些终态。"""
        await self._event("step.state", step_id=step_id, status=status)
        if status != "running":
            self.tool_step_terminal[step_id] = status

    def _sync_tool_dispatches(self):
        """即使批次被取消，也要结算实际派发计数。

        工作任务仅修改各自的尝试记录。协调器对每个逻辑步骤和实际重试各核算一次。
        已提交意图会占用一次实际调用预留，但其本身不代表逻辑工具调用已经发生。
        """
        for call_id, attempt in self.tool_attempts.items():
            request = attempt.request
            if attempt.dispatched:
                if request.step_id not in self.started_tool_steps:
                    self.started_tool_steps.add(request.step_id)
                    self.counts.steps += 1
                if request.attempt > 1 and call_id not in self.counted_tool_retries:
                    self.counted_tool_retries.add(call_id)
                    self.counts.retries += 1
            if call_id in self.active_tool_calls:
                outcome = attempt.outcome
                self.active_tool_calls[call_id].update(
                    dispatched=attempt.dispatched,
                    outcome="returned" if outcome and outcome.response is not None else "unknown",
                )

    def _call_snapshots(self):
        """以复数调用标识字段为准，单数形式字段继续保留以兼容已有使用方。"""
        self._sync_tool_dispatches()
        pending = ([self.pending_call] if self.pending_call else []) + list(
            self.pending_tool_calls.values()
        )
        active = ([self.active_call] if self.active_call else []) + list(
            self.active_tool_calls.values()
        )
        return dict(
            pending_call=pending[0] if len(pending) == 1 else None,
            active_call=active[0] if len(active) == 1 else None,
            pending_calls=pending,
            active_calls=active,
        )

    async def _prepare_tool_attempt(self, step, operation_id, number):
        """在任何适配器 I/O 前预留一次实际调用尝试并提交意图。"""
        self._time()
        if self.counts.catalog + self.counts.tools >= self.cfg.max_data_calls:
            raise RunStopped("budget_exceeded", "data_call_budget")
        deadline = min(self.deadline, self.loop.time() + self.cfg.per_call_timeout_ms / 1000)
        request = m.ToolRequest(
            run_id=self.run_id,
            task_id=self.req.task_id,
            operation_id=operation_id,
            call_id=self._id("call"),
            attempt=number,
            timeout_ms=max(1, math.ceil((deadline - self.loop.time()) * 1000)),
            max_output_bytes=self.cfg.max_output_bytes_per_call,
            step_id=step.step_id,
            catalog_snapshot_id=self.snapshot,
            tool_name=step.tool_name,
            args=step.args,
        )
        ref = await self._artifact(request)
        meta = _meta(request)
        self.counts.tools += 1
        self.pending_tool_calls[request.call_id] = {"phase": "tool", **meta}
        try:
            await self._event("tool.call.started", data_ref=ref, **meta)
        except asyncio.CancelledError:
            # 追加被打断时，提交结果可能未知。保留实际调用预留，
            # 但此时尚未派发任何工作任务。
            raise
        except RunStopped:
            self.counts.tools -= 1
            self.pending_tool_calls.pop(request.call_id)
            raise
        self.pending_tool_calls[request.call_id]["intent_committed"] = True
        if self.loop.time() >= deadline:
            global_timeout = self.loop.time() >= self.deadline
            await self._event(
                "tool.call.failed",
                status="timeout",
                terminal=global_timeout,
                duration_ms=0,
                error=m.ErrorInfo(code="timeout", message="Deadline expired before dispatch."),
                detail={
                    "code": "time_budget" if global_timeout else "timeout",
                    "outcome": "not_dispatched",
                },
                **meta,
            )
            self.pending_tool_calls.pop(request.call_id)
            raise RunStopped("timeout", "time_budget" if global_timeout else "call_timeout")
        attempt = ToolAttempt(request=request, deadline=deadline)
        self.tool_attempts[request.call_id] = attempt
        return attempt

    async def _gather_tool_attempts(self, entries):
        """启动数量受限的尝试，并在每条退出路径上等待所有子任务结束。"""
        attempts, tasks = [], []
        try:
            if self.recovery is not None:
                await self.recovery.prepare_wave()
            for step, operation_id, number in entries:
                if number == 1:
                    await self._tool_step_state(step.step_id, "running")
                attempt = await self._prepare_tool_attempt(step, operation_id, number)
                attempts.append(attempt)
            # 整批准入具有原子性：所有选中意图持久化提交前，不启动任何适配器。
            # 准备失败时不派发任何调用。
            if self.recovery is not None:
                await self.recovery.admit_wave(attempts)
            for attempt in attempts:
                request = attempt.request
                self.pending_tool_calls.pop(request.call_id)
                self.active_tool_calls[request.call_id] = {
                    "phase": "tool",
                    **_meta(request),
                    "outcome": "unknown",
                    "dispatched": False,
                }
                tasks.append(
                    asyncio.create_task(
                        invoke_tool_attempt(
                            attempt,
                            self._invoke_tool_port,
                            max_output_bytes=self.cfg.max_output_bytes_per_call,
                        )
                    )
                )
            await asyncio.gather(*tasks)
            return attempts
        finally:
            # 准备失败时尚未派发工作任务。批次一旦启动，
            # 所有退出路径都要在追踪收尾前等待全部子任务结束，
            # 包括清理期间发生第二次取消的情况。
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                cleanup = asyncio.gather(*tasks, return_exceptions=True)
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await cleanup
                    raise
            self._sync_tool_dispatches()

    async def _settle_tool_attempt(self, attempt):
        """提交已捕获的响应或失败，返回策略判断所需数据，不派发调用。"""
        outcome, request = attempt.outcome, attempt.request
        assert outcome is not None
        meta = _meta(request)
        self._sync_tool_dispatches()
        if outcome.kind == "response":
            result = outcome.response
            ref = await self._artifact(result)
            await self._event(
                "tool.call.completed" if result.status == "ok" else "tool.call.failed",
                status=result.status,
                duration_ms=outcome.duration_ms,
                data_ref=ref,
                error=result.error,
                **meta,
            )
            await self._observe(request, result, ref)
            self.active_tool_calls.pop(request.call_id)
            return result, result.error, None
        if outcome.kind == "timeout":
            error = m.ErrorInfo(
                code="timeout", message="Cooperative call deadline exceeded", retryable=True
            )
            reason = RunStopped("timeout", "call_timeout")
            status, detail = (
                "timeout",
                {
                    "code": "timeout",
                    "outcome": "unknown" if attempt.dispatched else "not_dispatched",
                },
            )
        elif outcome.kind == "invalid_response":
            error = m.ErrorInfo(
                code="invalid_response", message="Adapter returned an invalid response."
            )
            reason = RunStopped("failed", "invalid_module_response")
            status, detail = "invalid_response", {"error_type": outcome.error_type}
        else:
            error = m.ErrorInfo(code="adapter_error", message="Adapter call failed.")
            reason = RunStopped("failed", "tool_error")
            status, detail = "error", {"error_type": outcome.error_type}
        await self._event(
            "tool.call.failed",
            status=status,
            duration_ms=outcome.duration_ms,
            error=error,
            detail=detail,
            **meta,
        )
        self.active_tool_calls.pop(request.call_id)
        return None, error, reason

    async def _apply_tool_evidence(self, step, result):
        """在将显式证据纳入运行状态前验证其来源关系。"""
        try:
            raw = result.data.get("evidence", [])
            if not isinstance(raw, list) or (
                step.tool_name == "extract" and "evidence" not in result.data
            ):
                raise ValueError("tool evidence must be an explicit array")
            incoming = [m.EvidencePayload.model_validate(item) for item in raw]
        except (ValidationError, ValueError, TypeError) as exc:
            raise RunStopped("failed", "invalid_module_response") from exc
        if any(
            item.source_id not in step.source_ids or item.source_id not in self.sources
            for item in incoming
        ):
            raise RunStopped("failed", "invalid_module_response")
        for item in incoming:
            record = m.EvidenceRecord(
                **item.model_dump(), evidence_id=self._id("ev"), call_id=result.call_id
            )
            await self._write(
                lambda r=record: self.trace.put_artifact(r.evidence_id, r.model_dump(mode="json"))
            )
            self.evidence.append(record)
            await self._event("evidence.added", data_ref=record.evidence_id, step_id=step.step_id)
        self.visited.update(step.source_ids)

    def _tool_failure_policy(self, step, attempt, result, error, hard_stop):
        """优先重试安全的实际调用尝试；可恢复错误交由新的规划决策处理。"""
        if (
            self.specs[step.tool_name].idempotent
            and error.retryable
            and attempt.request.attempt <= self.cfg.max_retries_per_data_operation
        ):
            return "retry", None
        if (
            result is not None
            and error.recoverable
            and error.code not in FATAL_TOOL_CODES
            and self.req.mode != "fixed_retrieval"
        ):
            return "recover", None
        if hard_stop is not None:
            return "stop", hard_stop
        if error.code == "timeout":
            return "stop", RunStopped("timeout", "call_timeout")
        if error.code == "token_budget_exceeded":
            return "stop", RunStopped("budget_exceeded", "token_budget")
        reason = (
            "retry_exhausted"
            if self.specs[step.tool_name].idempotent and error.retryable
            else "tool_error"
        )
        return "stop", RunStopped("failed", reason)

    async def _execute_tool_wave(self, wave):
        """先结算已启动的同批任务，再决定重试、重新规划或停止。"""
        entries = [(step, self._id("op"), 1) for step in wave]
        succeeded, failed = set(), set()
        while entries:
            # 重试与首次尝试竞争同一个实际调用预算。
            # 仅允许完整且预算足够的子批次准入；后续重试
            # 不得使此前已提交的意图搁置而无法派发。
            remaining = self.cfg.max_data_calls - self.counts.catalog - self.counts.tools
            if remaining <= 0:
                raise RunStopped("budget_exceeded", "data_call_budget")
            width = min(self.cfg.max_parallel_tool_calls, remaining)
            entries, deferred = entries[:width], entries[width:]
            attempts = await self._gather_tool_attempts(entries)
            # 全局终止可能打断正常结果持久化。将所有已缓冲结果
            # 保留在复数调用快照中供审计，
            # 绝不能把结果未知或已缓冲的调用误报为未执行。
            self._time()
            retry, final_stops, recover = [], [], []
            for (step, operation_id, number), attempt in zip(entries, attempts, strict=True):
                result, error, hard_stop = await self._settle_tool_attempt(attempt)
                if result is not None and result.status == "ok":
                    try:
                        await self._apply_tool_evidence(step, result)
                    except RunStopped as stop:
                        if self.trace_broken:
                            raise
                        error, hard_stop = (
                            m.ErrorInfo(
                                code="invalid_response",
                                message="Tool evidence violates provenance or schema.",
                            ),
                            stop,
                        )
                    else:
                        await self._tool_step_state(step.step_id, "succeeded")
                        succeeded.add(step.step_id)
                        continue
                policy, stop = self._tool_failure_policy(step, attempt, result, error, hard_stop)
                if policy == "retry":
                    retry.append((step, operation_id, number + 1, attempt))
                    continue
                await self._tool_step_state(step.step_id, "failed")
                failed.add(step.step_id)
                if policy == "recover":
                    recover.append((step, error))
                else:
                    final_stops.append(stop)
            if final_stops or recover:
                # 同批任务失败后，重试其他操作会成为本批停止决策后的
                # 新增派发。它们最后一次明确错误保持已提交状态，
                # 这些逻辑步骤也标记为失败。
                for step, _, _, _ in retry:
                    failed.add(step.step_id)
                    await self._tool_step_state(step.step_id, "failed")
                for step, _, _ in deferred:
                    failed.add(step.step_id)
                    await self._tool_step_state(step.step_id, "failed")
                for step, error in recover:
                    await self._event("tool.replan_requested", step_id=step.step_id, error=error)
                return succeeded, failed, final_stops[0] if final_stops else None
            entries = list(deferred)
            for step, operation_id, number, previous in retry:
                delay = random.uniform(0, min(0.05 * 2 ** min(number - 2, 6), 1.0))
                await self._event(
                    "data.retry",
                    detail={
                        "delay_ms": delay * 1000,
                        "error_code": previous.outcome.response.error.code
                        if previous.outcome.response
                        else "timeout",
                    },
                    **_meta(previous.request),
                )
                try:
                    async with asyncio.timeout_at(self.deadline):
                        if self.recovery is None or not self.recovery.rebuilding:
                            await asyncio.sleep(delay)
                except TimeoutError as exc:
                    raise RunStopped("timeout", "time_budget") from exc
                entries.append((step, operation_id, number))
        return succeeded, failed, None

    async def _close_unstarted_tool_steps(self, steps, failed):
        """将受阻的后继步骤及无关的跳过步骤明确标记为未执行。"""
        failed = set(failed)
        for step in steps:
            if step.step_id in self.tool_step_terminal:
                continue
            blocked = bool(set(step.depends_on) & failed)
            if blocked:
                failed.add(step.step_id)
            await self._tool_step_state(step.step_id, "blocked" if blocked else "skipped")

    async def _dispatch_tool_plan(self, steps):
        """在能力、资源和预算约束内，按稳定顺序选择就绪批次。"""
        pending, succeeded = list(steps), set()
        try:
            while pending:
                self._time()
                remaining = self.cfg.max_data_calls - self.counts.catalog - self.counts.tools
                if remaining <= 0:
                    raise RunStopped("budget_exceeded", "data_call_budget")
                wave = select_ready_wave(
                    pending, succeeded, self.specs, min(self.cfg.max_parallel_tool_calls, remaining)
                )
                if not wave:
                    raise RunStopped("failed", "invalid_plan")
                # 派发前检查整个选中批次。内部实际重试
                # 不再重复增加这些逻辑操作计数。
                signatures = dict(self.action_counts)
                for step in wave:
                    signature = hashlib.sha256(
                        canonical_json([step.subtask_id, step.tool_name, step.args])
                    ).hexdigest()
                    if signatures.get(signature, 0) >= self.cfg.max_identical_tool_calls:
                        await self._event(
                            "tool.loop_detected",
                            step_id=step.step_id,
                            detail={"limit": self.cfg.max_identical_tool_calls},
                        )
                        await self._close_unstarted_tool_steps(pending, {step.step_id})
                        pending = []
                        raise RunStopped("failed", "repeated_tool_call")
                    signatures[signature] = signatures.get(signature, 0) + 1
                self.action_counts = signatures
                done, failed, stop = await self._execute_tool_wave(wave)
                succeeded.update(done)
                wave_ids = {step.step_id for step in wave}
                pending = [step for step in pending if step.step_id not in wave_ids]
                if failed:
                    await self._close_unstarted_tool_steps(pending, failed)
                    pending = []
                    if stop:
                        raise stop
                    return
        except RunStopped:
            if pending and self.loop.time() < self.deadline and not self.trace_broken:
                # 后续重试的准备或预算失败，必须保留此前成功提交的结果。
                # 未结算尝试仍出现在复数调用快照中，
                # 不得视为不存在。
                started = {
                    attempt.request.step_id
                    for attempt in self.tool_attempts.values()
                    if attempt.dispatched
                }
                failed = {
                    step.step_id
                    for step in pending
                    if step.step_id in started and step.step_id not in self.tool_step_terminal
                }
                for step_id in sorted(failed):
                    await self._tool_step_state(step_id, "failed")
                await self._close_unstarted_tool_steps(
                    [step for step in pending if step.step_id not in failed], failed
                )
            raise
