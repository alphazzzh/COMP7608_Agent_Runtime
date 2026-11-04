"""根据经过验证的持久化适配器调用日志重建单个控制器。

此可选协调器借鉴 dsh-superagent-review-2026-11-01.md 中记录的持久化意图及执行尝试
分离模式，不执行参考代码，也不依赖 DeepSeek Harness。重建通过现有控制器消费已知
响应，继续以控制器的授权、证据和预算规则为准。结果未知绝不构成调用适配器的授权。

恢复条件刻意从严：运行时实现和可信工具契约必须一致，原始有限截止时间须包含停机
时长，并且只能有一个本地所有者。每次执行写入独立审计分段。这不支持任意协程恢复、
跨版本迁移，也不保证外部 I/O 恰好执行一次。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sys
import time
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from .. import __version__
from .. import models as m
from ..state import StateLease, StateStore, StateStoreError
from ..validation import canonical_json, catalog_digest, validate_tool_specs
from .agent_protocol import validate_agent_planner
from .content_feedback import feedback_protocol
from .errors import RunStopped

_JOURNAL_VERSION = 1
_RECOVERABLE_STOPS = {
    "trace_sink_unavailable",
    "trace_write_failed",
    "trace_close_failed",
    "state_store_failed",
}


class _RunState(m.Model):
    """独立验证元数据，不仅依赖 SQLite 的 JSON 和哈希保证。"""

    journal_version: int
    runtime_version: m.NonEmpty
    implementation_digest: m.NonEmpty
    planner_engine: Literal["agent-loop-v1"]
    content_feedback_protocol: Literal["content-feedback-v1"] | None = None
    tool_digest: m.NonEmpty
    request: m.RunRequest
    seed: str = Field(pattern=r"^[a-f0-9]{32}$")
    started_epoch: float
    started_at: m.NonEmpty
    deadline_epoch: float
    attempts: list[m.NonEmpty]
    result: m.RunResult | None
    progress: m.JsonObject


class _CallRecord(m.Model):
    """封闭的记录封装；后续按调用阶段验证请求和响应。"""

    status: Literal["started", "completed"]
    phase: m.NonEmpty
    request: m.JsonObject
    auxiliary: bool
    response: m.JsonObject | None = None


def _request_type(phase, request=None):
    """仅解析预定义端口；扩展内容请求必须显式带有反馈字段。

    普通请求继续使用原始模型，不能因新增纠错能力而向旧适配器注入空字段。
    这里只选择严格模型，运行的能力和反馈归属还须在日志预检中校验。
    """
    if isinstance(request, dict) and "feedback" in request:
        extended = {
            "content.compose": m.ComposeFeedbackRequest,
            "content.verify": m.VerifyFeedbackRequest,
        }.get(phase)
        if extended is not None:
            return extended
    return {
        "catalog": m.CatalogRequest,
        "content.prepare_catalog": m.CatalogRegistrationRequest,
        "content.search": m.SearchRequest,
        "content.compose": m.ComposeRequest,
        "content.verify": m.VerifyRequest,
        "planner.decide": m.AgentDecisionRequest,
        "tool": m.ToolRequest,
    }.get(phase)


def _implementation_digest() -> str:
    """即使包版本相同，也拒绝源码或图调度器发生变化的状态。"""
    digest = hashlib.sha256()
    # 冻结的锁文件保证安装可复现；元数据也防范应用在未更新
    # 此包时修改运行环境。
    # 这些包提供图执行器、任务与上下文处理、通道，
    # 以及用于隔离私有工作流输入的追踪上下文。
    for package in ("langgraph", "langchain-core", "langgraph-checkpoint", "langsmith"):
        digest.update(package.encode())
        digest.update(b"\0")
        digest.update(metadata.version(package).encode())
        digest.update(b"\0")
    root = Path(__file__).parent.parent
    # 公共契约、控制器、存储与审计仍在根目录；内部图和真实模型适配器已归档。
    # 用相对路径区分同名文件。报告/UI 不参与执行，不应使恢复身份失效。
    sources = [*root.glob("*.py")]
    for directory in ("engine", "adapters"):
        sources.extend((root / directory).rglob("*.py"))
    for path in sorted(sources):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _tool_digest(data) -> str:
    """对经过验证并与原对象分离的本地能力求哈希，不允许适配器 I/O。"""
    try:
        specs = validate_tool_specs(data.tool_specs())
        value = [specs[name].model_dump(mode="json") for name in sorted(specs)]
        return hashlib.sha256(canonical_json(value)).hexdigest()
    except Exception as exc:
        raise StateStoreError("recovery_contract_mismatch") from exc


def _response_type(phase):
    """使用封闭响应注册表，防止加载任意持久化类型。"""
    return {
        "catalog": m.CatalogResult,
        "content.prepare_catalog": m.ModuleResult[m.CatalogHandle],
        "content.search": m.ModuleResult[m.SearchResponse],
        "content.compose": m.ModuleResult[m.AnswerRecord],
        "content.verify": m.ModuleResult[m.Verification],
        "planner.decide": m.ModuleResult[m.AgentAction],
        "tool": m.ToolResult,
    }.get(phase)


class RecoverySession:
    """负责持久化准入和响应缓存，不负责工具调度策略。"""

    def __init__(self, store: StateStore, lease: StateLease, state, calls, *, resuming):
        self.store, self.lease = store, lease
        self.state, self.calls = state, calls
        self.resuming = resuming
        self.rebuilding = resuming
        self.counter = 0
        self.controller = None
        self.failure = None
        self.prepared = set()
        self.cached = {key for key, value in calls.items() if not value.get("auxiliary", False)}
        self.unseen = set(self.cached)
        self.rebind_call = None
        self.handles = set()
        # 辅助注册是每次恢复时真实发生的本地调用，
        # 与重放原始逻辑 prepare_catalog 调用分别计算。
        self.extra_content_calls = sum(bool(v.get("auxiliary")) for v in calls.values())
        self.attempt_id = f"attempt-{uuid4().hex}"
        self._inspect_journal()

    def _inspect_journal(self):
        """在允许任何适配器访问之前验证完整日志。"""
        for key, entry in self.calls.items():
            try:
                envelope = _CallRecord.model_validate(entry)
                phase = envelope.phase
                request_kind = _request_type(phase, envelope.request)
                if request_kind is None:
                    raise ValueError("unknown request phase")
                parsed = request_kind.model_validate(envelope.request)
                if "feedback" in envelope.request:
                    declared = self.state.get("content_feedback_protocol")
                    if (
                        declared != m.CONTENT_FEEDBACK_PROTOCOL
                        or parsed.feedback.phase != phase.removeprefix("content.")
                    ):
                        raise ValueError("content feedback capability or phase")
                    # 完整内容由确定性重放重新生成并比较；此处先拒绝悬空、跨阶段
                    # 或自引用反馈，不能把任意附加文字当作已发生的模型失败。
                    previous = self.calls.get(parsed.feedback.call_id)
                    if (
                        not isinstance(previous, dict)
                        or previous.get("phase") != phase
                        or parsed.feedback.call_id == parsed.call_id
                    ):
                        raise ValueError("content feedback ownership")
                request = parsed.model_dump(mode="json")
                if (
                    parsed.call_id != key
                    or parsed.run_id != self.lease.run_id
                    or parsed.task_id != self.state["request"]["task_id"]
                ):
                    raise ValueError("call ownership")
                if envelope.auxiliary and phase != "content.prepare_catalog":
                    raise ValueError("unknown auxiliary call")
                if entry["status"] != "completed":
                    raise StateStoreError("recovery_outcome_unknown")
                kind = _response_type(phase)
                if kind is None:
                    raise ValueError("unknown phase")
                reply = kind.model_validate(entry["response"])
                if (
                    reply.call_id != key
                    or len(canonical_json(entry["response"])) > parsed.max_output_bytes
                ):
                    raise ValueError("reply ownership or size")
                if phase == "content.prepare_catalog" and reply.status == "ok":
                    sources = [m.SourceRecord.model_validate(v) for v in request["sources"]]
                    if (
                        reply.data.catalog_snapshot_id != request["catalog_snapshot_id"]
                        or reply.data.catalog_digest != request["catalog_digest"]
                        or catalog_digest(sources) != request["catalog_digest"]
                        or reply.usage.provider_requests != 0
                    ):
                        raise ValueError("catalog registration")
                    self.handles.add(reply.data.handle)
                    if not entry.get("auxiliary", False):
                        self.rebind_call = entry
            except StateStoreError:
                raise
            except (KeyError, TypeError, ValueError, ValidationError) as exc:
                raise StateStoreError("recovery_state_invalid") from exc

    def attach(self, controller):
        """重建使用有界本地时间窗口，新增 I/O 仍以原始 UTC 截止时间为准。"""
        self.controller = controller
        elapsed = max(0, time.time() - self.state["started_epoch"])
        controller.start = controller.loop.time() - elapsed
        controller.started_at = self.state["started_at"]
        controller.counts.content = self.extra_content_calls
        for entry in self.calls.values():
            if entry.get("auxiliary"):
                reply = m.ModuleResult[m.CatalogHandle].model_validate(entry["response"])
                controller._add_usage(reply.usage)
        if not self.rebuilding:
            self._set_original_deadline()

    def identifier(self, prefix):
        """工件、观察记录和证据标识共用一个序列。"""
        self.counter += 1
        value = f"{self.state['seed']}:{self.counter}:{prefix}".encode()
        return f"{prefix}-{hashlib.sha256(value).hexdigest()[:32]}"

    def _set_original_deadline(self):
        remaining = self.state["deadline_epoch"] - time.time()
        self.controller.deadline = self.controller.loop.time() + max(0, remaining)

    def annotate(self, kind, fields):
        """在新审计分段中明确标记重建调用属于历史。"""
        if fields.get("call_id") in self.cached:
            fields["detail"] = {**fields.get("detail", {}), "replayed": True}
        return fields

    def _normalized(self, request):
        value = request.model_dump(mode="json") if isinstance(request, BaseModel) else request
        # 先通过 JSON 往返转换生成独立副本，再规范化嵌套字段。
        value = json.loads(canonical_json(value))
        value.pop("timeout_ms", None)
        if "budget_remaining" in value:
            value["budget_remaining"].pop("wall_time_ms", None)
        if "catalog_handle" in value:
            if value["catalog_handle"] not in self.handles:
                raise StateStoreError("recovery_catalog_mismatch")
            # 只有对这个确切的已存储快照进行有效注册后返回的句柄
            # 才能进入别名集合；任意其他值都不能匹配。
            value["catalog_handle"] = {
                "registered_catalog": self.rebind_call["request"]["catalog_digest"]
            }
        return canonical_json(value)

    def _matches(self, phase, request):
        entry = self.calls.get(request.call_id)
        if entry is None:
            return False
        if entry.get("auxiliary") or phase != entry["phase"]:
            raise StateStoreError("recovery_request_mismatch")
        if self._normalized(request) != self._normalized(entry["request"]):
            raise StateStoreError("recovery_request_mismatch")
        return True

    async def _guard_match(self, phase, request):
        """防止历史不匹配被误归为普通适配器错误或重试错误。"""
        try:
            return self._matches(phase, request)
        except StateStoreError as exc:
            self.failure = exc.code
            raise RunStopped("failed", "state_store_failed") from exc

    async def _stored(self, method, *args):
        """持久化边界失败后冻结所有后续派发。"""
        try:
            return await method(*args)
        except StateStoreError as exc:
            self.failure = exc.code
            raise RunStopped("failed", "state_store_failed") from exc

    async def progress(self):
        """持久化可检查的进度，重建仍以响应为依据。"""
        if self.failure:
            raise RunStopped("failed", "state_store_failed")
        c = self.controller
        self.state["progress"] = {
            "call_counts": c.counts.model_dump(mode="json"),
            "model_calls_reserved": c.model_calls_reserved,
            "usage": c.usage,
            "snapshot": c.snapshot,
            "completed_subtasks": [v.subtask_id for v in c.completed_subtasks],
            "evidence_ids": [v.evidence_id for v in c.evidence],
            "observation_ids": list(c.observations_by_id),
            "pending": c._call_snapshots(),
        }
        await self._stored(self.store.save, self.lease, self.state)

    async def _activate(self, permitted=()):
        """仅在此前每个调用均匹配后，从历史重放切换到新增工作。"""
        if self.failure:
            raise RunStopped("failed", "state_store_failed")
        if self.unseen - set(permitted):
            raise StateStoreError("recovery_request_mismatch")
        if not self.rebuilding:
            return
        self.rebuilding = False
        self._set_original_deadline()
        self.controller._time()
        if self.rebind_call is not None:
            await self._register_catalog()

    async def _register_catalog(self):
        """仅重建本地 ContentPort 索引，并单独计入一次调用。"""
        c = self.controller
        old = self.rebind_call["request"]
        if c.snapshot != old["catalog_snapshot_id"]:
            raise StateStoreError("recovery_catalog_mismatch")
        request = m.CatalogRegistrationRequest.model_validate(
            {
                **old,
                "call_id": f"rebind-{uuid4().hex}",
                "operation_id": f"rebind-op-{uuid4().hex}",
                "timeout_ms": max(
                    1,
                    min(
                        self.state["request"]["config"]["per_call_timeout_ms"],
                        int((c.deadline - c.loop.time()) * 1000),
                    ),
                ),
            }
        )
        # 独立 ID 防止辅助工作改变控制器的历史序列。
        # 辅助工作仍会产生普通的成对审计记录。
        ref = f"rebind-artifact-{uuid4().hex}"
        await c._write(lambda: c.trace.put_artifact(ref, request.model_dump(mode="json")))
        c.counts.content += 1
        await c._event(
            "recovery.prepare_catalog.call.started",
            call_id=request.call_id,
            operation_id=request.operation_id,
            attempt=1,
            data_ref=ref,
        )
        reply = await self._dispatch(
            "content.prepare_catalog",
            c.content.prepare_catalog,
            request,
            m.ModuleResult[m.CatalogHandle],
            auxiliary=True,
        )
        # 重新绑定仍须遵守本地端口契约，但契约违规不能抹去
        # 已声明的真实提供方派发，也不能将未知用量改为零。
        c._add_usage(reply.usage)
        c.counts.model_calls += reply.usage.provider_requests
        c.model_calls_reserved += reply.usage.provider_requests
        valid = (
            reply.status == "ok"
            and reply.usage.provider_requests == 0
            and reply.data.catalog_snapshot_id == old["catalog_snapshot_id"]
            and reply.data.catalog_digest == old["catalog_digest"]
        )
        ref = f"rebind-artifact-{uuid4().hex}"
        await c._write(lambda: c.trace.put_artifact(ref, reply.model_dump(mode="json")))
        await c._event(
            "recovery.prepare_catalog.call.completed"
            if valid
            else "recovery.prepare_catalog.call.failed",
            call_id=request.call_id,
            operation_id=request.operation_id,
            attempt=1,
            data_ref=ref,
            usage=reply.usage,
            status=reply.status if valid else "invalid_response",
        )
        if not valid:
            raise StateStoreError("recovery_catalog_mismatch")
        self.handles.add(reply.data.handle)
        c.handle = reply.data.handle

    async def prepare_serial(self, phase, request):
        """在提交新增意图前检查缓存请求，或切换到新增工作。"""
        if await self._guard_match(phase, request):
            return request
        await self._stored(self._activate)
        updates = {}
        if isinstance(request, m.SearchRequest):
            updates["catalog_handle"] = self.controller.handle
        if hasattr(request, "budget_remaining"):
            # 字段在可选的目录重新注册前已经组装完毕。
            # 只刷新墙钟时间；其他预算由调用方预留。
            updates["budget_remaining"] = request.budget_remaining.model_copy(
                update={"wall_time_ms": self.controller._remaining().wall_time_ms}
            )
        return request.model_copy(update=updates)

    async def prepare_wave(self):
        """在构造新一批工具调用的时间窗口前，重新绑定其执行前沿。

        持久化准入会在启动任何工作任务前记录全部意图，因此可恢复的历史批次必定全部有缓存。
        部分准入的批次存在结果未知的意图，已在预检中被拒绝。此处没有尚未重放的调用，
        因此切换到新增工作不会跳过历史，也不会扰乱确定性 ID。
        """
        if self.rebuilding and not self.unseen:
            await self._stored(self._activate)

    async def admit_wave(self, attempts):
        """在启动批次内任何工作任务之前，持久化全部新增调用意图。"""
        known, new = set(), []
        for attempt in attempts:
            request = attempt.request
            if await self._guard_match("tool", request):
                known.add(request.call_id)
            else:
                new.append(request)
        if new:
            await self._stored(self._activate, known)
            for request in new:
                await self._begin("tool", request)
        for attempt in attempts:
            if attempt.request.call_id not in known:
                attempt.deadline = min(attempt.deadline, self.controller.deadline)

    async def _begin(self, phase, request, *, auxiliary=False):
        value = {
            "status": "started",
            "phase": phase,
            "request": request.model_dump(mode="json"),
            "auxiliary": auxiliary,
        }
        await self._stored(self.store.begin_call, self.lease, request.call_id, value)
        self.calls[request.call_id] = value
        self.prepared.add(request.call_id)

    async def _dispatch(self, phase, method, request, response_type, *, auxiliary=False):
        if self.failure:
            raise RunStopped("failed", "state_store_failed")
        self.controller._time()
        if request.call_id not in self.prepared:
            await self._begin(phase, request, auxiliary=auxiliary)
        # 存储需要 await；提交意图时可能超过原始截止时间。
        # 仅使用 timeout_at(past_deadline) 仍可能进入协程代码。
        self.controller._time()
        deadline = min(
            self.controller.deadline, self.controller.loop.time() + request.timeout_ms / 1000
        )
        owner = asyncio.current_task()
        cancelled_before = owner.cancelling() if owner is not None else 0
        try:
            async with asyncio.timeout_at(deadline):
                raw = await method(request.model_copy(deep=True))
            if self.controller.loop.time() >= deadline:
                raise TimeoutError
            raw = raw.model_dump(mode="json") if isinstance(raw, BaseModel) else raw
            if len(canonical_json(raw)) > request.max_output_bytes:
                raise ValueError("response exceeds max_output_bytes")
            response = response_type.model_validate(raw)
            if response.call_id != request.call_id:
                raise ValueError("response call_id mismatch")
            value = {
                **self.calls[request.call_id],
                "status": "completed",
                "response": response.model_dump(mode="json"),
            }
            # 每个工作任务都立即提交自己经过验证的响应。其他同批任务缓慢
            # 或后续追踪失败，都不应抹去已知外部结果。
            await self._stored(self.store.finish_call, self.lease, request.call_id, value)
            self.calls[request.call_id] = value
            if phase == "content.prepare_catalog" and response.status == "ok":
                self.handles.add(response.data.handle)
                if not auxiliary:
                    self.rebind_call = value
            return response
        finally:
            # 对无效返回和存储错误也须保留调用方的取消意图。
            # 有效响应在执行到这里前已经提交。
            if owner is not None and owner.cancelling() > cancelled_before:
                raise asyncio.CancelledError

    async def invoke(self, phase, method, request, response_type):
        if request.call_id in self.cached:
            if not await self._guard_match(phase, request):
                self.failure = "recovery_request_mismatch"
                raise RunStopped("failed", "state_store_failed")
            self.unseen.discard(request.call_id)
            return response_type.model_validate(self.calls[request.call_id]["response"])
        return await self._dispatch(phase, method, request, response_type)


async def durable_run(request, *, data, content, planner, trace, state_store, run_id=None):
    """在重建、最终持久化和取消处理全过程持有同一个租约。"""
    # 局部导入使现有运行时无需加载可选 POSIX 存储后端，
    # 同时避免 runtime/recovery 循环导入。
    from ..runtime import _Controller

    if not callable(getattr(trace, "create_attempt", None)):
        raise StateStoreError("recovery_trace_unsupported")
    try:
        validate_agent_planner(planner)
    except RunStopped as exc:
        raise StateStoreError("recovery_engine_mismatch") from exc
    resuming = run_id is not None
    if resuming:
        lease = await state_store.acquire(run_id)
    else:
        request = m.RunRequest.model_validate(request.model_dump(mode="json"))
        run_id = request.run_id or f"run-{uuid4().hex}"
        request = request.model_copy(update={"run_id": run_id})
        now = time.time()
        state = {
            "journal_version": _JOURNAL_VERSION,
            "runtime_version": __version__,
            "implementation_digest": _implementation_digest(),
            "planner_engine": m.AGENT_PROTOCOL,
            "content_feedback_protocol": feedback_protocol(content),
            "tool_digest": _tool_digest(data),
            "request": request.model_dump(mode="json"),
            "seed": uuid4().hex,
            "started_epoch": now,
            "started_at": datetime.fromtimestamp(now, UTC).isoformat(),
            "deadline_epoch": now + request.config.max_wall_time_ms / 1000,
            "attempts": [],
            "result": None,
            "progress": {},
        }
        lease = await state_store.create(run_id, state)
    try:
        loaded = await state_store.load(lease)
        state = loaded.state
        try:
            parsed_state = _RunState.model_validate(state)
            if (
                type(state["started_epoch"]) is not float
                or type(state["deadline_epoch"]) is not float
                or state["started_epoch"] <= 0
                or not math.isclose(
                    state["deadline_epoch"] - state["started_epoch"],
                    parsed_state.request.config.max_wall_time_ms / 1000,
                    rel_tol=0,
                    abs_tol=0.00001,
                )
                or not math.isclose(
                    datetime.fromisoformat(state["started_at"]).timestamp(),
                    state["started_epoch"],
                    rel_tol=0,
                    abs_tol=0.000001,
                )
            ):
                raise ValueError("invalid original deadline")
            if (
                state["journal_version"] != _JOURNAL_VERSION
                or state["runtime_version"] != __version__
                or state["implementation_digest"] != _implementation_digest()
            ):
                raise StateStoreError("recovery_version_mismatch")
            request = m.RunRequest.model_validate(state["request"])
            if request.run_id != run_id:
                raise ValueError("run ownership")
            # 相同源码也可能接收到能力不同的新规划器。必须在返回缓存终态、创建
            # 审计分段或重建任何 A/C 端口之前，确认原始控制流身份没有改变。
            if parsed_state.planner_engine != m.AGENT_PROTOCOL:
                raise StateStoreError("recovery_engine_mismatch")
            # 同一源码中也可传入不同能力的内容端口；必须在缓存终态或异步端口
            # 准入前拒绝变化，避免把扩展请求交给旧实现或改变重放后的反馈策略。
            if parsed_state.content_feedback_protocol != feedback_protocol(content):
                raise StateStoreError("recovery_contract_mismatch")
            if parsed_state.result is not None:
                if (
                    parsed_state.result.run_id != run_id
                    or parsed_state.result.task_id != request.task_id
                ):
                    raise ValueError("terminal result ownership")
                return parsed_state.result
            if state["tool_digest"] != _tool_digest(data):
                raise StateStoreError("recovery_contract_mismatch")
            session = RecoverySession(state_store, lease, state, loaded.calls, resuming=resuming)
            if resuming and time.time() >= state["deadline_epoch"]:
                raise StateStoreError("recovery_deadline_expired")
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise StateStoreError("recovery_state_invalid") from exc
        state["attempts"].append(session.attempt_id)
        await state_store.save(lease, state)
        controller = _Controller(request, data, content, planner, trace, recovery=session)
        result = await controller.execute()
        if session.failure:
            raise StateStoreError(session.failure)
        if session.unseen:
            raise StateStoreError("recovery_request_mismatch")
        # 审计失败只能通过独立调用日志恢复。
        # 正常业务停止（包括提供方 403）是已持久化的终态结果，
        # 不能成为反复消耗新模型预算的理由。
        if result.stop_reason not in _RECOVERABLE_STOPS:
            state["result"] = result.model_dump(mode="json")
        await state_store.save(lease, state)
        return result
    finally:
        # 所有路径都必须清理，但清理期间的存储失败不能覆盖
        # 已经在传播的调用方取消或主要恢复错误。
        # 即使持久化租约令牌更新失败，SQLite 后端也会等待
        # 磁盘工作线程结束并关闭操作系统锁。
        primary = sys.exception()
        try:
            await state_store.release(lease)
        except Exception:
            if primary is None:
                raise
