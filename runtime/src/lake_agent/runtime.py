"""通过有界工具批次、单次运行预算和来源追踪进行异步协调。

适配器负责全部解析、检索和生成。本模块授权请求、验证响应，并串行提交状态转移。
仅明确支持安全并发的数据工具等待可以重叠，模型和内容调用保持串行。
本模块不执行模型生成的代码、不读取评估标签，也不解释来源定位符。
"""

from __future__ import annotations

import asyncio
import math
import random
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from . import models as m
from .engine.content_feedback import feedback_protocol, reference_error
from .engine.context import read_json_page
from .engine.errors import FATAL_TOOL_CODES as _FATAL_TOOL_CODES
from .engine.errors import RunStopped as _Stop
from .engine.tool_runtime import ToolExecutionMixin
from .engine.verification_state import VerificationReceipt, verification_fingerprint
from .ports import AgentPlannerPort, ContentPort, DataPort, TraceSink
from .validation import (
    canonical_json,
    catalog_digest,
    source_ids_for,
    validate_dag,
)

if TYPE_CHECKING:
    from .state import StateStore


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _utc() -> str:
    return datetime.now(UTC).isoformat()


class _Controller(ToolExecutionMixin):
    """每次运行独占一个控制器实例，即使调用方共享适配器实例也如此。"""

    def __init__(self, request, data, content, planner, trace, *, recovery=None):
        self.req, self.data, self.content, self.planner, self.trace = (
            request,
            data,
            content,
            planner,
            trace,
        )
        self.recovery = recovery
        self.cfg = request.config
        self.run_id = request.run_id or self._id("run")
        self.loop = asyncio.get_running_loop()
        self.start = self.loop.time()
        self.deadline = self.start + self.cfg.max_wall_time_ms / 1000
        self.started_at = _utc()
        self.seq = 0
        self.counts = m.CallCounts()
        self.model_calls_reserved = 0
        self.pending_call: m.JsonObject | None = None
        self.active_call: m.JsonObject | None = None
        self.usage = {"input_tokens": 0, "output_tokens": 0, "cost": None, "currency": None}
        self.usage_unknown = False
        self.trace_open = False
        self.trace_broken = False
        self.answer = None
        # 仅控制器持有完整受检状态；图与模型上下文的预览不能授权完成。
        self.verification_receipt: VerificationReceipt | None = None
        # 反馈是确定性运行状态：恢复时从已知内容响应重建，不依赖模型会话内存。
        self.content_feedback: dict[str, m.ContentFeedback] = {}
        self.failure: m.RunFailure | None = None
        self.sources = {}
        self.evidence = []
        self.visited = set()
        self.step_ids = set()
        self.subtasks = []
        self.completed_subtasks = []
        self.snapshot = None
        self.handle = None
        self.specs = {}
        self.observations: list[dict] = []
        self.observations_by_id: dict[str, dict] = {}
        self.accepted_steps: dict[str, m.ToolStep] = {}
        self.action_counts: dict[str, int] = {}
        self._init_tool_execution()
        if self.recovery is not None:
            self.recovery.attach(self)

    def _id(self, prefix):
        """仅在启用持久化执行时使用单次运行内稳定的标识符序列。"""
        return self.recovery.identifier(prefix) if self.recovery is not None else _id(prefix)

    async def _invoke_port(self, phase, method, request, response_type):
        """在适配器实现之外完成持久化调用准入。"""
        if self.recovery is not None:
            return await self.recovery.invoke(phase, method, request, response_type)
        return await method(request)

    async def _invoke_tool_port(self, request):
        """由隔离工作任务在同批其他任务结算前保存每个已知响应。"""
        return await self._invoke_port("tool", self.data.invoke, request, m.ToolResult)

    async def _observe(self, request, response, artifact_ref):
        """在完整审计工件提交后，为有效工具响应建立索引。

        观察记录包含失败尝试，但不分配证据 ID。来源授权依据已接受的计划，不采信工具文本。
        本地 JSON 分页读取器只可访问本次运行的索引。
        """
        step = self.accepted_steps[request.step_id]
        observation = dict(
            observation_id=self._id("obs"),
            call_id=request.call_id,
            operation_id=request.operation_id,
            attempt=request.attempt,
            step_id=step.step_id,
            subtask_id=step.subtask_id,
            tool_name=step.tool_name,
            source_ids=list(step.source_ids),
            args=step.args,
            status=response.status,
            artifact_ref=artifact_ref,
            result=response.model_dump(mode="json"),
        )
        await self._event(
            "tool.observed",
            step_id=step.step_id,
            call_id=request.call_id,
            data_ref=artifact_ref,
            detail={"observation_id": observation["observation_id"]},
        )
        self.observations.append(observation)
        self.observations_by_id[observation["observation_id"]] = observation

    async def _read_observation(self, decision, *, max_page_bytes=None):
        """仅读取已知结果，绝不将 ID 解释为文件系统路径。"""
        self._time()
        if self.counts.observation_reads >= self.cfg.max_observation_reads:
            raise _Stop("budget_exceeded", "observation_read_budget")
        observation = self.observations_by_id.get(decision.observation_id)
        if observation is None:
            raise _Stop("failed", "invalid_observation_read")
        # 除页面封装外，还需为稳定的观察 ID 预留空间。
        # 上下文构造器会完整保留请求的页面。
        overhead = len(canonical_json({"observation_id": decision.observation_id})) + 1
        history_overhead = (
            len(
                canonical_json(
                    {"previous_pages": [], "omitted_pages": self.cfg.max_observation_reads}
                )
            )
            + 1
        )
        page_budget = min(
            self.cfg.planner_preview_bytes,
            self.cfg.max_planner_context_bytes // 2 - history_overhead,
        )
        if max_page_bytes is not None:
            page_budget = min(page_budget, max_page_bytes)
        try:
            page = read_json_page(
                observation["result"],
                pointer=decision.pointer,
                offset=decision.offset,
                max_chars=decision.max_chars,
                max_bytes=page_budget - overhead,
            )
        except ValueError as exc:
            raise _Stop("failed", "invalid_observation_read") from exc
        page["observation_id"] = decision.observation_id
        self.counts.observation_reads += 1
        ref = await self._artifact(page)
        await self._event(
            "observation.read", data_ref=ref, detail={"observation_id": decision.observation_id}
        )
        return page

    def _time(self):
        if self.loop.time() >= self.deadline:
            raise _Stop("timeout", "time_budget")

    def _remaining(self):
        return m.BudgetRemaining(
            wall_time_ms=max(0, int((self.deadline - self.loop.time()) * 1000)),
            model_calls=max(0, self.cfg.max_model_calls - self.model_calls_reserved),
            data_calls=max(0, self.cfg.max_data_calls - self.counts.catalog - self.counts.tools),
            plan_steps=max(0, self.cfg.max_plan_steps - self.counts.steps),
            subtasks=max(0, self.cfg.max_subtasks - len(self.subtasks)),
        )

    async def _write(self, factory: Callable, *, terminal=False):
        """普通写入共用运行截止时间，终态结算使用有界宽限时间。"""
        deadline = self.deadline
        if terminal:
            deadline = self.loop.time() + self.cfg.cancel_cleanup_timeout_ms / 1000
        try:
            async with asyncio.timeout_at(deadline):
                return await factory()
        except Exception as exc:
            self.trace_broken = True
            raise _Stop("failed", "trace_write_failed") from exc

    async def _artifact(self, value):
        ref = self._id("artifact")
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        await self._write(lambda: self.trace.put_artifact(ref, value))
        return ref

    async def _event(self, kind, *, terminal=False, **fields):
        self.seq += 1
        if self.recovery is not None:
            fields = self.recovery.annotate(kind, fields)
        event = m.TraceEvent(
            run_id=self.run_id,
            task_id=self.req.task_id,
            seq=self.seq,
            event_type=kind,
            timestamp_utc=_utc(),
            **fields,
        )
        await self._write(lambda: self.trace.append(event), terminal=terminal)
        if self.recovery is not None:
            await self.recovery.progress()

    def _add_usage(self, usage):
        for name in ("input_tokens", "output_tokens"):
            old, new = self.usage[name], getattr(usage, name)
            self.usage[name] = None if old is None or new is None else old + new
        if usage.cost is not None:
            if usage.provider_requests and usage.currency is None:
                self.usage["cost"] = None
                self.usage_unknown = True
            elif self.usage["currency"] not in (None, usage.currency):
                self.usage["cost"] = None
                self.usage_unknown = True
            elif not self.usage_unknown:
                self.usage["cost"] = (self.usage["cost"] or 0) + usage.cost
                self.usage["currency"] = usage.currency
        elif usage.provider_requests:
            self.usage_unknown = True
            self.usage["cost"] = None

    async def _call(
        self,
        phase,
        method,
        request_type,
        fields,
        response_type,
        *,
        provider=False,
        data_kind=None,
        idempotent=False,
        step_id=None,
        content_errors=False,
    ):
        """先持久化调用意图再派发，仅允许幂等数据操作重试。

        在 await 前预留模型调用配额。有效响应须明确报告 provider_requests=0/1；
        超时或响应封装损坏时保留配额，因为控制器无法确认是否实际发起了网络请求。
        """
        operation_id = self._id("op")
        for attempt in range(1, self.cfg.max_retries_per_data_operation + 2):
            self._time()
            if data_kind and self.counts.catalog + self.counts.tools >= self.cfg.max_data_calls:
                raise _Stop("budget_exceeded", "data_call_budget")
            if provider and self.model_calls_reserved >= self.cfg.max_model_calls:
                raise _Stop("budget_exceeded", "model_call_budget")
            if step_id and attempt == 1 and self.counts.steps >= self.cfg.max_plan_steps:
                raise _Stop("budget_exceeded", "step_budget")
            call_deadline = min(
                self.deadline, self.loop.time() + self.cfg.per_call_timeout_ms / 1000
            )
            context = dict(
                run_id=self.run_id,
                task_id=self.req.task_id,
                operation_id=operation_id,
                call_id=self._id("call"),
                attempt=attempt,
                timeout_ms=max(1, math.ceil((call_deadline - self.loop.time()) * 1000)),
                max_output_bytes=self.cfg.max_output_bytes_per_call,
            )
            if provider:
                context.update(
                    model_id=self.cfg.model_id,
                    model_params=self.cfg.model_params,
                    prompt_version=getattr(
                        self.cfg.prompt_versions,
                        "planner" if phase.startswith("planner") else "content",
                    ),
                    token_budget=m.TokenBudget(),
                )
            # Pydantic 会保留已经实例化的嵌套模型。
            # 每个端口请求都要与原对象分离，防止规划器或内容适配器修改
            # 控制器信任的能力、来源或原始证据。
            req = request_type(**context, **fields).model_copy(deep=True)
            if self.recovery is not None:
                req = await self.recovery.prepare_serial(phase, req)
                # 重新绑定新的 ContentPort 可能耗时；新增调用只能使用
                # 原运行剩余的派发时间窗口。
                call_deadline = min(
                    self.deadline, self.loop.time() + self.cfg.per_call_timeout_ms / 1000
                )
                req = req.model_copy(
                    update={
                        "timeout_ms": max(1, math.ceil((call_deadline - self.loop.time()) * 1000))
                    }
                )
            request_ref = await self._artifact(req)
            meta = dict(
                operation_id=operation_id, call_id=req.call_id, attempt=attempt, step_id=step_id
            )
            # 等待开始事件提交前先预留预算。调用方取消若与线程中的
            # 追加操作发生竞态，提交结果可能未知；此时保留实际调用和模型
            # 配额，并在 run.cancelled 中暴露 ID。
            # 普通写入失败则回退预留，因为之后不可能发起端口调用。
            if data_kind:
                setattr(self.counts, data_kind, getattr(self.counts, data_kind) + 1)
                if step_id and attempt == 1:
                    self.counts.steps += 1
            if provider:
                self.model_calls_reserved += 1
            self.pending_call = {"phase": phase, **meta}
            try:
                await self._event(f"{phase}.call.started", data_ref=request_ref, **meta)
            except asyncio.CancelledError:
                if step_id and attempt == 1:
                    self.counts.steps -= 1
                raise
            except _Stop:
                if data_kind:
                    setattr(self.counts, data_kind, getattr(self.counts, data_kind) - 1)
                    if step_id and attempt == 1:
                        self.counts.steps -= 1
                if provider:
                    self.model_calls_reserved -= 1
                self.pending_call = None
                raise
            self.pending_call = None
            # 缓慢的追踪写入可能耗尽派发窗口。已持久化的 call.started
            # 尝试仍占用预算，但不得再进入端口。
            if self.loop.time() >= call_deadline:
                if step_id and attempt == 1:
                    # call.started 已提交后虽预留了实际尝试，
                    # 但逻辑调用尚未开始。
                    self.counts.steps -= 1
                if self.loop.time() >= self.deadline:
                    await self._event(
                        f"{phase}.call.failed",
                        status="timeout",
                        terminal=True,
                        duration_ms=0,
                        error=m.ErrorInfo(
                            code="time_budget",
                            message="Run deadline expired before dispatch.",
                            retryable=True,
                        ),
                        detail={"code": "time_budget", "outcome": "not_dispatched"},
                        **meta,
                    )
                    raise _Stop("timeout", "time_budget")
                await self._event(
                    f"{phase}.call.failed",
                    status="timeout",
                    duration_ms=0,
                    error=m.ErrorInfo(
                        code="timeout",
                        message="Call deadline expired before dispatch.",
                        retryable=True,
                    ),
                    detail={"code": "timeout", "outcome": "not_dispatched"},
                    **meta,
                )
                raise _Stop("timeout", "call_timeout")
            begun = self.loop.time()
            error = None
            response = None
            provider_usage_settled = False
            cancelled = False
            try:
                if not data_kind:
                    count_field = "planner" if phase.startswith("planner") else "content"
                    setattr(self.counts, count_field, getattr(self.counts, count_field) + 1)
                if data_kind and attempt > 1:
                    self.counts.retries += 1
                # 已提交开始事件，与当前正在等待适配器响应或持久化结果的调用不同。
                # 调用方取消导致最终结果无法确定时，
                # 须保留后者的身份。
                self.active_call = {"phase": phase, **meta, "outcome": "unknown"}
                owner = asyncio.current_task()
                cancelled_before = owner.cancelling() if owner is not None else 0
                try:
                    async with asyncio.timeout_at(call_deadline):
                        raw = await self._invoke_port(phase, method, req, response_type)
                finally:
                    # timeout_at 退出时会移除自身的取消请求。
                    # 新增的待处理请求属于调用方，包括适配器吞掉取消后
                    # 返回响应或抛出异常的情况。
                    if owner is not None and owner.cancelling() > cancelled_before:
                        raise asyncio.CancelledError
                # timeout_at 依赖协作：适配器可能吞掉其注入的取消，
                # 但迟到响应仍不得被接受。
                if self.loop.time() >= call_deadline:
                    raise TimeoutError
                # 即使已是模型实例也要重新验证：model_copy(update=...) 会绕过 Pydantic。
                raw = raw.model_dump(mode="json") if isinstance(raw, BaseModel) else raw
                if len(canonical_json(raw)) > self.cfg.max_output_bytes_per_call:
                    raise ValueError("response exceeds max_output_bytes")
                response = response_type.model_validate(raw)
                if response.call_id != req.call_id:
                    raise ValueError("response call_id mismatch")
                self.active_call["outcome"] = "returned"
                if hasattr(response, "usage"):
                    if provider:
                        self.counts.model_calls += response.usage.provider_requests
                        # 有效的零请求报告证明此配额未用于提供方请求。
                        # 已完成的实际调用继续占用预留，
                        # 未使用的预留则释放。
                        self.model_calls_reserved -= 1 - response.usage.provider_requests
                        provider_usage_settled = True
                    self._add_usage(response.usage)
                    if not provider and response.usage.provider_requests != 0:
                        # 即使因违反本地端口契约而失败，也要在用量核算中
                        # 保留已声明的实际派发。
                        self.counts.model_calls += response.usage.provider_requests
                        raise ValueError("local method must not make provider requests")
                ref = await self._artifact(response)
                if response.status == "error":
                    self.failure = m.RunFailure(
                        phase=phase, call_id=req.call_id, error=response.error
                    )
                await self._event(
                    f"{phase}.call.{'completed' if response.status == 'ok' else 'failed'}",
                    status=response.status,
                    data_ref=ref,
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=response.error if response.status == "error" else None,
                    usage=getattr(response, "usage", None),
                    **meta,
                )
                if isinstance(response, m.ToolResult):
                    await self._observe(req, response, ref)
                if response.status == "ok":
                    if (
                        not content_errors
                        and self.failure is not None
                        and self.failure.phase == phase
                    ):
                        self.failure = None
                    return response
                error = response.error
            except asyncio.CancelledError:
                cancelled = True
                if provider and not provider_usage_settled:
                    # 控制器未收到有效用量报告；对预留尝试计数一次，
                    # 与超时和错误的核算方式一致。
                    # 已返回的本地响应仍须保持为已知零用量。
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                raise
            except _Stop:
                raise
            except TimeoutError as exc:
                if provider:
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                error = m.ErrorInfo(
                    code="timeout", message="Cooperative call deadline exceeded", retryable=True
                )
                self.failure = m.RunFailure(phase=phase, call_id=req.call_id, error=error)
                time_budget = self.loop.time() >= self.deadline
                await self._event(
                    f"{phase}.call.failed",
                    status="timeout",
                    terminal=time_budget,
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=error,
                    detail={
                        "code": "time_budget" if time_budget else "timeout",
                        "outcome": "unknown",
                    },
                    **meta,
                )
                if time_budget:
                    raise _Stop("timeout", "time_budget") from exc
            except (ValidationError, ValueError, TypeError) as exc:
                if provider:
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                error = m.ErrorInfo(
                    code="invalid_response",
                    message="Adapter returned an invalid response.",
                    retryable=False,
                )
                self.failure = m.RunFailure(phase=phase, call_id=req.call_id, error=error)
                await self._event(
                    f"{phase}.call.failed",
                    status="invalid_response",
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=error,
                    detail={"error_type": type(exc).__name__},
                    **meta,
                )
                raise _Stop("failed", "invalid_module_response") from exc
            except Exception as exc:
                if provider:
                    self.counts.model_calls += 1
                    self._add_usage(m.Usage(provider_requests=1))
                error = m.ErrorInfo(
                    code="adapter_error", message="Adapter call failed.", retryable=False
                )
                self.failure = m.RunFailure(phase=phase, call_id=req.call_id, error=error)
                await self._event(
                    f"{phase}.call.failed",
                    status="error",
                    duration_ms=int((self.loop.time() - begun) * 1000),
                    error=error,
                    detail={"error_type": type(exc).__name__},
                    **meta,
                )
                # 异常没有可靠的可重试契约，绝不盲目重放工具调用。
                raise _Stop("failed", "tool_error" if data_kind else "module_error") from exc
            finally:
                if not cancelled:
                    self.active_call = None
            # 模型输出修正是一轮新的自主决策，不是 HTTP 重试。只有显式支持反馈的
            # 内容端口和确定的输出校验错误可以进入该通路；超时/鉴权/契约错误仍停止。
            if (
                content_errors
                and feedback_protocol(self.content) is not None
                and isinstance(response, m.ModuleResult)
                and error.code
                in {"invalid_model_citations", "invalid_model_sources", "invalid_model_output"}
            ):
                return response
            if not (
                data_kind
                and idempotent
                and error.retryable
                and attempt <= self.cfg.max_retries_per_data_operation
            ):
                if (
                    isinstance(response, m.ToolResult)
                    and error.recoverable
                    and error.code not in _FATAL_TOOL_CODES
                ):
                    # 重新规划是一次新的、受预算约束的决策。
                    # 它不是传输重试，也不重放结果未知的副作用。
                    return response
                if error.code == "timeout":
                    raise _Stop("timeout", "call_timeout")
                if error.code == "token_budget_exceeded":
                    raise _Stop("budget_exceeded", "token_budget")
                reason = (
                    "retry_exhausted"
                    if data_kind and idempotent and error.retryable
                    else ("tool_error" if data_kind else "module_error")
                )
                raise _Stop("failed", reason)
            # 使用带上限的全抖动退避；不创建新任务，取消可直接打断休眠。
            delay = random.uniform(0, min(0.05 * 2 ** min(attempt - 1, 6), 1.0))
            await self._event(
                "data.retry", detail={"delay_ms": delay * 1000, "error_code": error.code}, **meta
            )
            async with asyncio.timeout_at(self.deadline):
                try:
                    if self.recovery is None or not self.recovery.rebuilding:
                        await asyncio.sleep(delay)
                except TimeoutError as exc:
                    raise _Stop("timeout", "time_budget") from exc
        raise AssertionError("retry loop did not settle")

    async def _catalog(self):
        """分页加载并验证固定目录快照，再向内容端口注册目录。"""
        cursor, cursors = None, set()
        snapshot = self.cfg.dataset_snapshot
        while True:
            result = await self._call(
                "catalog",
                self.data.catalog,
                m.CatalogRequest,
                dict(snapshot_id=snapshot, cursor=cursor, page_size=self.cfg.catalog_page_size),
                m.CatalogResult,
                data_kind="catalog",
                idempotent=True,
            )
            if snapshot is not None and result.catalog_snapshot_id != snapshot:
                raise _Stop("failed", "catalog_snapshot_mismatch")
            snapshot = result.catalog_snapshot_id
            if not snapshot or len(result.sources) > self.cfg.catalog_page_size:
                raise _Stop("failed", "invalid_module_response")
            for source in result.sources:
                if source.source_id in self.sources:
                    raise _Stop("failed", "invalid_module_response")
                self.sources[source.source_id] = source
            if (
                len(self.sources) > self.cfg.max_catalog_sources
                or len(canonical_json(list(self.sources.values()))) > self.cfg.max_catalog_bytes
            ):
                raise _Stop("budget_exceeded", "catalog_limit")
            if not result.has_more:
                break
            if not result.next_cursor or result.next_cursor in cursors:
                raise _Stop("failed", "invalid_module_response")
            cursors.add(result.next_cursor)
            cursor = result.next_cursor
        self.snapshot = snapshot
        sources = sorted(self.sources.values(), key=lambda source: source.source_id)
        digest = catalog_digest(sources)
        reply = await self._call(
            "content.prepare_catalog",
            self.content.prepare_catalog,
            m.CatalogRegistrationRequest,
            dict(catalog_snapshot_id=snapshot, catalog_digest=digest, sources=sources),
            m.ModuleResult[m.CatalogHandle],
        )
        if reply.data.catalog_snapshot_id != snapshot or reply.data.catalog_digest != digest:
            raise _Stop("failed", "catalog_snapshot_mismatch")
        self.handle = reply.data.handle

    async def _search(self, subtask, focus=None, *, source_ids=None):
        """检索当前子任务的候选来源，并验证数量、归属和过滤范围。"""
        query = focus.query if focus else subtask.search_query
        subtask = subtask.model_copy(update={"search_query": query})
        source_filter = (
            source_ids
            if source_ids is not None
            else focus.source_ids
            if focus and focus.source_ids
            else None
        )
        reply = await self._call(
            "content.search",
            self.content.search,
            m.SearchRequest,
            dict(
                question=self.req.question,
                subtask=subtask,
                catalog_snapshot_id=self.snapshot,
                catalog_handle=self.handle,
                visited_source_ids=sorted(self.visited),
                source_id_filter=source_filter,
                limit=self.cfg.candidate_top_k,
            ),
            m.ModuleResult[m.SearchResponse],
        )
        candidates = reply.data.candidates
        ids = [c.source_id for c in candidates]
        if (
            len(ids) > self.cfg.candidate_top_k
            or len(set(ids)) != len(ids)
            or not set(ids) <= self.sources.keys()
            or (source_filter is not None and not set(ids) <= set(source_filter))
        ):
            raise _Stop("failed", "invalid_module_response")
        return sorted(candidates, key=lambda item: (-item.score, item.source_id))

    async def _execute_plan(self, plan, subtask, candidates, *, authorized_sources=None):
        """完整验证计划和来源授权后，再登记步骤并派发工具工作。"""
        if len(plan.steps) > self.cfg.max_plan_steps - self.counts.steps:
            raise _Stop("budget_exceeded", "step_budget")
        allowed = {c.source_id for c in candidates} | {e.source_id for e in self.evidence}
        # inspect 可以建立有用的结构信息，而不返回证据。
        # 检索适配器可能省略已访问来源，因此已接受观察记录的来源授权
        # 须独立于下一次检索的候选列表保留。
        allowed.update(source for item in self.observations for source in item["source_ids"])
        if authorized_sources is not None:
            # 自主决策可直接选择已冻结目录来源，不必先制造检索候选；
            # 权限来自可信调度器，仍不能越过目录边界。
            if not set(authorized_sources) <= self.sources.keys():
                raise _Stop("failed", "invalid_plan")
            allowed.update(authorized_sources)
        try:
            steps = validate_dag(plan.steps, "step_id")
            ids = {step.step_id for step in steps}
            if ids & self.step_ids:
                raise ValueError("step IDs reused across plans")
            for step in steps:
                if step.subtask_id != subtask.subtask_id or step.tool_name not in self.specs:
                    raise ValueError("unknown tool or wrong subtask")
                sources = source_ids_for(self.specs[step.tool_name], step.args)
                if sources != set(step.source_ids) or not sources <= allowed:
                    raise ValueError("source authorization failed")
                if len(step.source_ids) != len(set(step.source_ids)):
                    raise ValueError("duplicate source IDs")
        except (ValueError, ValidationError) as exc:
            raise _Stop("failed", "invalid_plan") from exc
        # 提交任何 ID 或执行任何步骤前，先验证整个计划。
        self.step_ids.update(ids)
        self.accepted_steps.update({step.step_id: step for step in steps})
        ref = await self._artifact(plan)
        await self._event("plan.accepted", data_ref=ref)
        await self._dispatch_tool_plan(steps)

    async def _reject_content(self, phase, call_id, error):
        """保留未接纳输出的反馈，只向声明支持修正的端口继续下一轮。"""
        self.failure = m.RunFailure(phase=f"content.{phase}", call_id=call_id, error=error)
        feedback = m.ContentFeedback(phase=phase, call_id=call_id, error=error)
        ref = await self._artifact(feedback)
        await self._event(
            "content.output.rejected",
            call_id=call_id,
            error=error,
            status="rejected",
            data_ref=ref,
            detail={"phase": f"content.{phase}"},
        )
        if feedback_protocol(self.content) is None:
            raise _Stop("failed", "invalid_module_response")
        self.content_feedback[phase] = feedback

    async def _clear_content_feedback(self, phase, call_id):
        """成功且通过控制器校验后清除同阶段反馈，保留历史拒绝事件。"""
        if self.content_feedback.pop(phase, None) is not None:
            await self._event(
                "content.feedback.resolved",
                call_id=call_id,
                detail={"phase": f"content.{phase}"},
            )
        if self.failure is not None and self.failure.phase == f"content.{phase}":
            self.failure = None
            if self.content_feedback:
                # 不同阶段的修正可交错，清掉最新错误后仍保留另一个尚未解决的反馈。
                remaining = next(reversed(self.content_feedback.values()))
                self.failure = m.RunFailure(
                    phase=f"content.{remaining.phase}",
                    call_id=remaining.call_id,
                    error=remaining.error,
                )

    def _feedback_fields(self, phase, fields):
        """旧端口收到原请求，只有显式协商的修正请求新增反馈字段。"""
        feedback = self.content_feedback.get(phase)
        if feedback is not None and feedback_protocol(self.content) is not None:
            return {**fields, "feedback": feedback}
        return fields

    def _verification_fingerprint(self):
        """以实际业务值计算身份，不以调用次数或工具观察的变化使核验失效。"""
        return verification_fingerprint(
            question=self.req.question,
            answer=self.answer,
            evidence=self.evidence,
            mode=self.req.mode,
            require_verification=self.cfg.require_verification,
        )

    def _verification_status(self):
        """过期只表示不再适用，不能当成对当前答案的否定核验。"""
        if self.verification_receipt is None:
            return "missing"
        if self.answer is None:
            return "stale"
        return (
            "current"
            if self.verification_receipt.input_fingerprint == self._verification_fingerprint()
            else "stale"
        )

    async def _compose(self, *, revision_instructions=None):
        """生成后校验引用，未接纳的内容不覆盖上一次有效答案。"""
        revision = None
        if self.answer is not None:
            receipt = self.verification_receipt
            revision = m.AnswerRevision(
                previous_answer=self.answer,
                verification=receipt.result if receipt is not None else None,
                verification_is_current=self._verification_status() == "current",
                instructions=revision_instructions,
            )
        fields = self._feedback_fields(
            "compose",
            dict(
                question=self.req.question,
                completed_subtasks=self.completed_subtasks,
                evidence=self.evidence,
                revision=revision,
            ),
        )
        reply = await self._call(
            "content.compose",
            self.content.compose,
            m.ComposeFeedbackRequest if "feedback" in fields else m.ComposeRequest,
            fields,
            m.ModuleResult[m.AnswerRecord],
            provider=True,
            content_errors=True,
        )
        error = (
            reply.error
            if reply.status == "error"
            else reference_error(
                "compose",
                reply.data,
                allowed_evidence_ids=[e.evidence_id for e in self.evidence],
                allowed_source_ids=list(self.sources),
            )
        )
        if error is not None:
            await self._reject_content("compose", reply.call_id, error)
            return False
        await self._clear_content_feedback("compose", reply.call_id)
        if self.answer != reply.data:
            # 旧答案的核验错误不属于新答案。图层也会清除旧答案的核验判定。
            await self._clear_content_feedback("verify", reply.call_id)
            self.verification_receipt = None
        self.answer = reply.data
        return True

    async def _verify(self):
        """核验输出格式与引用身份；合法的不支持判定照常交模型决策。"""
        # 在派发前捕获完整实际输入；_call会隔离给适配器的请求，避免其修改控制状态。
        checked_fingerprint = self._verification_fingerprint()
        fields = self._feedback_fields(
            "verify",
            dict(
                question=self.req.question,
                answer=self.answer,
                evidence=self.evidence,
            ),
        )
        reply = await self._call(
            "content.verify",
            self.content.verify,
            m.VerifyFeedbackRequest if "feedback" in fields else m.VerifyRequest,
            fields,
            m.ModuleResult[m.Verification],
            provider=True,
            content_errors=True,
        )
        error = (
            reply.error
            if reply.status == "error"
            else reference_error(
                "verify",
                reply.data,
                allowed_evidence_ids=[e.evidence_id for e in self.evidence],
                allowed_source_ids=list(self.sources),
            )
        )
        if error is not None:
            await self._reject_content("verify", reply.call_id, error)
            return None
        await self._clear_content_feedback("verify", reply.call_id)
        self.verification_receipt = VerificationReceipt(
            input_fingerprint=checked_fingerprint,
            result=reply.data,
            call_id=reply.call_id,
        )
        return reply.data

    async def _workflow(self):
        """将路由交给私有图，I/O 授权仍保留在控制器中。"""
        from .engine.graph_runner import execute_workflow

        return await execute_workflow(self)

    async def _cancel_cleanup(self):
        """在有限清理窗口内记录取消状态并释放追踪接收器。"""
        self._sync_tool_dispatches()
        if not self.trace_open:
            return
        cleanup_deadline = self.loop.time() + self.cfg.cancel_cleanup_timeout_ms / 1000
        if not self.trace_broken:
            try:
                async with asyncio.timeout_at(cleanup_deadline):
                    await self._event(
                        "run.cancelled",
                        status="cancelled",
                        terminal=True,
                        detail={
                            "call_counts": self.counts.model_dump(),
                            # 取消可能打断提交 call.started 的 await。
                            # 保留预留并暴露调用标识，
                            # 便于审计方识别尚未确认提交的
                            # 开始事件。
                            # 此路径下尚未向适配器派发调用。
                            **self._call_snapshots(),
                        },
                    )
            except Exception:
                pass
        # 关闭与追加终态事件相互独立，防止取消记录写入失败
        # 导致接收器无法释放。
        try:
            async with asyncio.timeout_at(cleanup_deadline):
                await self.trace.close()
        except Exception:
            pass

    async def _propagate_cancellation(self):
        """限定追踪清理时长，并在返回取消前等待清理任务结束。"""
        cleanup = asyncio.create_task(self._cancel_cleanup())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            cleanup.cancel()
            try:
                await cleanup
            except asyncio.CancelledError:
                pass

    async def execute(self):
        """执行一次控制器运行，统一处理终态审计、关闭和调用方取消。"""
        trace_ref = None
        try:
            try:
                # 在线程中创建目录时，取消可能发生于目录已预留之后。
                # 在 await 前先标记为需要清理；
                # 普通初始化失败会在下面清除此标记。
                self.trace_open = True
                async with asyncio.timeout_at(self.deadline):
                    if self.recovery is not None:
                        await self.trace.create_attempt(self.run_id, self.recovery.attempt_id)
                    else:
                        await self.trace.create_run(self.run_id)
            except Exception as exc:
                self.trace_open = False
                raise _Stop("failed", "trace_sink_unavailable") from exc
            ref = await self._artifact(self.req)
            resumed = self.recovery is not None and self.recovery.resuming
            await self._event("run.resumed" if resumed else "run.started", data_ref=ref)
            reason = await self._workflow()
            status = "completed"
        except asyncio.CancelledError:
            # 屏蔽短暂清理期间的取消；即使再次取消，也保留原始取消，
            # 并确保清理任务的结果或异常得到获取。
            await self._propagate_cancellation()
            raise
        except _Stop as stop:
            status, reason = stop.status, stop.reason
        except TimeoutError:
            status, reason = "timeout", "time_budget"
        except Exception:
            status, reason = "failed", "module_error"
        try:
            if self.trace_open:
                if not self.trace_broken:
                    try:
                        await self._event(
                            "run.completed" if status == "completed" else "run.stopped",
                            terminal=True,
                            status=status,
                            detail={
                                "stop_reason": reason,
                                **(
                                    {"failure": self.failure.model_dump(mode="json")}
                                    if self.failure is not None
                                    else {}
                                ),
                                "call_counts": self.counts.model_dump(),
                                **self._call_snapshots(),
                            },
                        )
                    except _Stop:
                        status, reason = "failed", "trace_write_failed"
                try:
                    async with asyncio.timeout(self.cfg.cancel_cleanup_timeout_ms / 1000):
                        trace_ref = await self.trace.close()
                except Exception:
                    status, reason, trace_ref = "failed", "trace_close_failed", None
        except asyncio.CancelledError:
            # 最终事件和关闭操作的 await 同样可以被调用方取消。
            await self._propagate_cancellation()
            raise
        return m.RunResult(
            run_id=self.run_id,
            task_id=self.req.task_id,
            mode=self.req.mode,
            status=status,
            stop_reason=reason,
            failure=self.failure if status != "completed" else None,
            answer=self.answer,
            usage=m.AggregateUsage(**self.usage, provider_requests=self.counts.model_calls),
            call_counts=self.counts,
            started_at=self.started_at,
            ended_at=_utc(),
            duration_ms=int((self.loop.time() - self.start) * 1000),
            versions=m.Versions(
                dataset_snapshot=self.snapshot,
                model_id=self.cfg.model_id,
                model_params=self.cfg.model_params,
                prompt_versions=self.cfg.prompt_versions,
                code_version=self.cfg.code_version,
                config_version=self.cfg.config_version,
            ),
            trace_ref=trace_ref,
        )


async def run(
    request: m.RunRequest,
    *,
    data: DataPort,
    content: ContentPort,
    planner: AgentPlannerPort,
    trace: TraceSink,
    state_store: StateStore | None = None,
) -> m.RunResult:
    """在调用方事件循环中执行一次，始终向上传播调用方取消。

    每次运行都须使用新的 TraceSink。只有可重入的适配器才可共享。
    请求字典可先通过 RunRequest.model_validate 验证。
    """
    request = m.RunRequest.model_validate(request.model_dump(mode="json"))
    if state_store is not None:
        from .engine.recovery import durable_run

        return await durable_run(
            request,
            data=data,
            content=content,
            planner=planner,
            trace=trace,
            state_store=state_store,
        )
    return await _Controller(request, data, content, planner, trace).execute()


async def resume(
    run_id: str,
    *,
    data: DataPort,
    content: ContentPort,
    planner: AgentPlannerPort,
    trace: TraceSink,
    state_store: StateStore,
) -> m.RunResult:
    """使用新的端口实例，根据已知响应重建持久化运行。

    原始截止时间和预算仍然有效。状态不兼容或存在未确定的外部结果时，
    在派发前抛出 StateStoreError。调用方负责存储生命周期，每次执行须使用新的追踪接收器。
    """
    from .engine.recovery import durable_run

    return await durable_run(
        None,
        run_id=run_id,
        data=data,
        content=content,
        planner=planner,
        trace=trace,
        state_store=state_store,
    )
