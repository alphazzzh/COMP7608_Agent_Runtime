"""持续任务 API 的严格输入模型与固定公开错误。

模型只描述可持久化的用户载荷。严格模式拒绝隐式类型转换，交接内容另以
规范 UTF-8 JSON 字节数限制，避免把大对象或不受信任的执行策略写入任务库。
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Model(BaseModel):
    """公共严格 Pydantic 配置，禁止未知字段和非有限 JSON 数值。"""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


MissionStatus = Literal["active", "paused", "completed", "blocked"]
TriggerStatus = Literal["pending", "claimed", "running", "completed", "blocked", "cancelled"]
RunStatus = Literal["completed", "insufficient_evidence", "budget_exceeded", "failed", "timeout"]


class MissionLimits(_Model):
    """一个 Mission 的累计硬上限；字段必须是有限范围内的正整数。"""

    max_runs: Annotated[int, Field(strict=True, gt=0, le=100)] = 10
    max_model_calls: Annotated[int, Field(strict=True, gt=0, le=10_000)] = 120
    max_data_calls: Annotated[int, Field(strict=True, gt=0, le=10_000)] = 120
    max_duration_seconds: Annotated[int, Field(strict=True, gt=0, le=604_800)] = 86_400


class MissionCreate(_Model):
    """创建任务时允许的字段；profile 身份由宿主签名参数最终确认。"""

    title: Annotated[str, Field(strict=True, min_length=1, max_length=200)]
    question: Annotated[str, Field(strict=True, min_length=1, max_length=2_000)]
    profile_id: Annotated[str, Field(strict=True, min_length=1, max_length=100)] | None = None
    interval_seconds: Annotated[int, Field(strict=True, gt=0, le=604_800)] | None = None
    start_immediately: bool = True
    stop_on_completed: bool = True
    limits: MissionLimits = Field(default_factory=MissionLimits)

    @field_validator("title", "question", "profile_id")
    @classmethod
    def clean_text(cls, value: str | None) -> str | None:
        """沿用 Web 输入边界：去掉两端空白，并拒绝纯空白目标。"""
        if value is not None:
            value = value.strip()
            if not value:
                raise ValueError("text must not be blank")
            try:
                value.encode("utf-8")
            except UnicodeError:
                raise ValueError("text must be valid UTF-8") from None
        return value


class MissionHandoff(_Model):
    """跨周期传递的有界进展摘要，不承载证据记录或执行权限。"""

    summary: Annotated[str, Field(strict=True, max_length=2_000)] = ""
    pending_items: list[Annotated[str, Field(strict=True, min_length=1, max_length=256)]] = Field(
        default_factory=list
    )
    previous_run_id: Annotated[str, Field(strict=True, min_length=1, max_length=200)] | None = None
    artifact_refs: list[Annotated[str, Field(strict=True, min_length=1, max_length=200)]] = Field(
        default_factory=list
    )
    truncated: bool = False

    @field_validator("pending_items")
    @classmethod
    def bounded_pending(cls, value: list[str]) -> list[str]:
        """限制待办数量，保持交接显示有界。"""
        if len(value) > 8:
            raise ValueError("pending_items exceed 8 entries")
        return value

    @field_validator("artifact_refs")
    @classmethod
    def bounded_artifacts(cls, value: list[str]) -> list[str]:
        """限制工件引用数量，引用本身仍由宿主另行解析和授权。"""
        if len(value) > 8:
            raise ValueError("artifact_refs exceed 8 entries")
        return value

    @model_validator(mode="after")
    def bounded_json(self) -> MissionHandoff:
        """按规范 UTF-8 JSON 的真实字节数限制整个交接对象。"""
        try:
            encoded = json.dumps(
                self.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except (UnicodeError, ValueError):
            raise ValueError("handoff must be valid UTF-8 JSON") from None
        if len(encoded) > 4096:
            raise ValueError("handoff exceeds the 4096-byte limit")
        return self


class MissionRecord(_Model):
    """Mission 持久记录的完整形状；只用于存储读写核验，API 返回普通字典。"""

    mission_id: Annotated[str, Field(min_length=1, max_length=200)]
    title: Annotated[str, Field(min_length=1, max_length=200)]
    question: Annotated[str, Field(min_length=1, max_length=2000)]
    profile_id: Annotated[str, Field(min_length=1, max_length=100)] | None
    profile_signature: Annotated[str, Field(min_length=1, max_length=512)]
    status: MissionStatus
    reason: Annotated[str, Field(max_length=2000)] | None
    created_at: float
    updated_at: float
    deadline_at: float
    next_wake_at: float | None
    interval_seconds: Annotated[int, Field(gt=0, le=604800)] | None
    stop_on_completed: bool
    limits: MissionLimits
    started_runs: Annotated[int, Field(ge=0, le=100)]
    completed_runs: Annotated[int, Field(ge=0, le=100)]
    used_model_calls: Annotated[int, Field(ge=0, le=10000)]
    used_data_calls: Annotated[int, Field(ge=0, le=10000)]
    reserved_model_calls: Annotated[int, Field(ge=0, le=10000)]
    reserved_data_calls: Annotated[int, Field(ge=0, le=10000)]
    active_trigger_id: Annotated[str, Field(min_length=1, max_length=200)] | None
    handoff: MissionHandoff

    @model_validator(mode="after")
    def consistent_counters(self) -> MissionRecord:
        """行摘要通过后仍验证预算关系，避免错误迁移成为可派发任务。"""
        if not self.completed_runs <= self.started_runs <= self.limits.max_runs:
            raise ValueError("inconsistent mission cycles")
        if self.used_model_calls + self.reserved_model_calls > self.limits.max_model_calls:
            raise ValueError("inconsistent model budget")
        if self.used_data_calls + self.reserved_data_calls > self.limits.max_data_calls:
            raise ValueError("inconsistent data budget")
        if self.deadline_at < self.created_at:
            raise ValueError("inconsistent deadline")
        if self.active_trigger_id is None and (
            self.reserved_model_calls or self.reserved_data_calls
        ):
            raise ValueError("reservation has no owner")
        return self


class TriggerRecord(_Model):
    """Trigger 持久记录的完整形状；run 身份在入队时便永久固定。"""

    trigger_id: Annotated[str, Field(min_length=1, max_length=200)]
    mission_id: Annotated[str, Field(min_length=1, max_length=200)]
    event_id: Annotated[str, Field(min_length=1, max_length=200)]
    run_id: Annotated[str, Field(min_length=1, max_length=200)]
    status: TriggerStatus
    created_at: float
    updated_at: float
    cycle: Annotated[int, Field(ge=0, le=100)]
    reserved_model_calls: Annotated[int, Field(ge=0, le=10000)]
    reserved_data_calls: Annotated[int, Field(ge=0, le=10000)]
    request: dict[str, Any] | None
    handoff: MissionHandoff
    run_status: RunStatus | None
    stop_reason: Annotated[str, Field(max_length=2000)] | None

    @model_validator(mode="after")
    def consistent_phase(self) -> TriggerRecord:
        """领取前不能附带额度，running 必须已有冻结请求。"""
        if self.status == "pending" and (
            self.cycle
            or self.reserved_model_calls
            or self.reserved_data_calls
            or self.request is not None
        ):
            raise ValueError("pending trigger contains execution state")
        if self.status in {"claimed", "running", "completed"} and self.cycle == 0:
            raise ValueError("trigger has not been claimed")
        if self.status == "running" and self.request is None:
            raise ValueError("running trigger has no frozen request")
        return self


class MissionError(Exception):
    """固定安全错误码、消息和 HTTP 状态，不回显 SQL、路径或原始异常。"""

    def __init__(self, code: str, message: str = "任务操作无法完成。", status: int = 409):
        self.code = code
        self.message = message
        self.status = status
        super().__init__(message)
