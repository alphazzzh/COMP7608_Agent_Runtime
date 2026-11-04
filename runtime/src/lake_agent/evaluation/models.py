"""评估报告的独立数据契约，不执行实验，也不依赖网页应用。

评估脚本与 Web 导入入口共享这些严格模型。固定检索仅是报告中的对照标签；
生产 RunMode 仍只支持自主 Agent 的两种核验策略，不从报告模式推导执行能力。
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import Field, field_validator, model_validator

from ..models import Model

_UNIT = Annotated[float, Field(ge=0, le=1)]
_NONNEGATIVE = Annotated[float, Field(ge=0)]
_LABEL = Annotated[str, Field(min_length=1, max_length=200)]
_TIMESTAMP = Annotated[str, Field(max_length=64)]
# 报告可以比较评估侧固定策略；该标签绝不能扩展生产 RunRequest 的执行模式。
EvaluationMode = Literal["fixed_retrieval", "no_verifier", "full_agent"]


def _aware_iso(value: str) -> str:
    """要求时间具有真实的 UTC 偏移，同时保留提交时的 ISO 字符串写法。"""
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("evaluation timestamps require an explicit timezone")
    return value


class _EvaluationModel(Model):
    """严格检查导入文本的类型，并确保其可序列化为 UTF-8。"""

    @field_validator("*")
    @classmethod
    def _utf8_text(cls, value):
        if isinstance(value, str):
            value.encode("utf-8")
        return value


class EvaluationMetrics(_EvaluationModel):
    """可为空的上报指标；缺失表示没有提供测量值。"""

    accuracy: _UNIT | None = None
    numerical_accuracy: _UNIT | None = None
    structured_accuracy: _UNIT | None = None
    retrieval_precision: _UNIT | None = None
    retrieval_recall: _UNIT | None = None
    retrieval_f1: _UNIT | None = None
    grounding: _UNIT | None = None
    completion_rate: _UNIT | None = None
    valid_tool_call_rate: _UNIT | None = None
    unnecessary_tool_call_rate: _UNIT | None = None
    retry_recovery_rate: _UNIT | None = None
    tool_calls_mean: _NONNEGATIVE | None = None
    # 访问次数与实际数据传输量、外部数据服务费用是不同测量；没有可靠上报时保留空值。
    data_calls_mean: _NONNEGATIVE | None = None
    external_data_bytes_total: _NONNEGATIVE | None = None
    external_data_cost_usd: _NONNEGATIVE | None = None
    latency_ms_mean: _NONNEGATIVE | None = None
    token_total: _NONNEGATIVE | None = None
    provider_requests_total: _NONNEGATIVE | None = None
    input_tokens_total: _NONNEGATIVE | None = None
    output_tokens_total: _NONNEGATIVE | None = None
    cost_usd: _NONNEGATIVE | None = None


class EvaluationRow(_EvaluationModel):
    """一个允许的执行模式，及为该模式独立声明的任务数量。"""

    mode: EvaluationMode
    task_count: Annotated[int, Field(ge=1, le=1_000_000)]
    metrics: EvaluationMetrics = Field(default_factory=EvaluationMetrics)


class EvaluationImport(_EvaluationModel):
    """调用方提供的评估元数据，不含服务端管理的来源记录。"""

    title: Annotated[str, Field(min_length=1, max_length=160)]
    dataset_id: _LABEL
    task_set_id: _LABEL
    model_id: _LABEL
    evaluator: _LABEL
    evaluated_at: _TIMESTAMP
    notes: Annotated[str, Field(max_length=2000)] = ""
    results: Annotated[list[EvaluationRow], Field(min_length=1, max_length=3)]

    @field_validator("evaluated_at")
    @classmethod
    def _evaluation_date(cls, value: str) -> str:
        return _aware_iso(value)

    @model_validator(mode="after")
    def _unique_modes(self) -> Self:
        if len({row.mode for row in self.results}) != len(self.results):
            raise ValueError("each evaluation mode may appear only once")
        return self
