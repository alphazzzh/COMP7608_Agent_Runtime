"""异构数据池异步运行时使用的严格 JSON 边界模型。

所有公开载荷均可序列化为 JSON。请求有意将上下文字段展平，使适配器能够通过
一次 ``model_validate`` 调用校验普通的输入 JSON。依赖运行状态的检查，
包括数据源授权、预算和快照身份，由 validation.py 和控制器负责。
"""

from __future__ import annotations

import json
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_serializer,
    model_validator,
)

JsonObject = dict[str, JsonValue]
NonEmpty = Annotated[str, Field(min_length=1)]
PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeInt = Annotated[int, Field(ge=0)]
# 修订目标是待解释的业务文本，不是执行策略；完整请求仍另受UTF-8字节预算约束。
RevisionInstructions = Annotated[str, Field(min_length=1, max_length=4000)]
AGENT_PROTOCOL = "agent-loop-v1"
CONTENT_FEEDBACK_PROTOCOL = "content-feedback-v1"
RunMode = Literal["no_verifier", "full_agent"]
# 保留旧类型别名供宿主导入；评估报告的对照组不属于可执行运行模式。
Mode = RunMode
Status = Literal["completed", "insufficient_evidence", "budget_exceeded", "timeout", "failed"]
T = TypeVar("T")


class Model(BaseModel):
    """拒绝未知字段、隐式类型转换以及非有限的 JSON 数值。"""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class PromptVersions(Model):
    planner: str | None = "planner-v1.1-exploration"
    content: str | None = "content-v0.1"


class RunConfig(Model):
    """可配置的运行限制；实际模型身份由宿主或服务端配置提供。"""

    max_wall_time_ms: PositiveInt = 120_000
    per_call_timeout_ms: PositiveInt = 20_000
    cancel_cleanup_timeout_ms: PositiveInt = 3_000
    max_subtasks: PositiveInt = 8
    max_plan_steps: PositiveInt = 8
    # 自由决策循环独立限次，避免本地动作或零请求适配器无限循环。
    max_agent_turns: PositiveInt = 32
    require_verification: bool = False
    max_data_calls: PositiveInt = 12
    max_model_calls: PositiveInt = 12
    max_retries_per_data_operation: NonNegativeInt = 1
    candidate_top_k: PositiveInt = 5
    max_output_bytes_per_call: PositiveInt = 65_536
    catalog_page_size: PositiveInt = 100
    max_catalog_sources: PositiveInt = 2_000
    max_catalog_bytes: PositiveInt = 2_000_000
    # 以下限制按工作上下文的 UTF-8 JSON 字节计算，不是服务商的 token 限制。
    max_planner_context_bytes: Annotated[int, Field(ge=1024)] = 24_000
    planner_preview_bytes: Annotated[int, Field(ge=256)] = 2_000
    max_observation_reads: NonNegativeInt = 8
    max_identical_tool_calls: PositiveInt = 2
    # 适配器还必须单独声明工具可并发；已有工具默认继续串行执行。
    max_parallel_tool_calls: Annotated[int, Field(ge=1, le=32)] = 4
    max_input_tokens: PositiveInt | None = None
    max_output_tokens: PositiveInt | None = None
    dataset_snapshot: NonEmpty | None = None
    model_id: NonEmpty = "unconfigured"
    model_params: JsonObject = Field(default_factory=lambda: {"temperature": 0})
    prompt_versions: PromptVersions = Field(default_factory=PromptVersions)
    code_version: str | None = None
    config_version: NonEmpty = "dev-v1"

    @model_validator(mode="after")
    def check_context_limits(self) -> RunConfig:
        """为来源信息、省略数量和请求读取的页面预留上下文空间。"""
        if self.planner_preview_bytes > self.max_planner_context_bytes // 2:
            raise ValueError("planner_preview_bytes exceeds half the context budget")
        return self


class RunRequest(Model):
    schema_version: Literal["1.0"] = "1.0"
    task_id: NonEmpty
    question: NonEmpty
    mode: RunMode = "full_agent"
    run_id: NonEmpty | None = None
    config: RunConfig = Field(default_factory=RunConfig)


class ErrorInfo(Model):
    """适配器错误；重放操作与修正输入后重新规划属于不同权限。

    只有可信数据适配器，才能声明一个已明确失败的操作可安全地修正输入后再执行。
    结果未知的操作和运行时校验错误均不允许走这条纠错恢复路径。
    """

    code: NonEmpty
    message: NonEmpty
    retryable: bool = False
    recoverable: bool = False
    # 可选诊断只保存模型输出的有限差异，不容纳 HTTP 请求、凭据或提供方封装。
    details: JsonObject | None = None

    @field_validator("details")
    @classmethod
    def bounded_details(cls, value: JsonObject | None) -> JsonObject | None:
        """把可持久化诊断限制为 8 KB 有限 UTF-8 JSON，避免错误放大日志。"""
        if value is not None:
            try:
                encoded = json.dumps(
                    value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
                ).encode("utf-8")
            except (ValueError, TypeError, UnicodeError, RecursionError):
                raise ValueError("diagnostics must be finite UTF-8 JSON") from None
            if len(encoded) > 8000:
                raise ValueError("diagnostics exceed the 8000-byte limit")
        return value

    @model_serializer(mode="wrap")
    def omit_absent_details(self, handler):
        """旧适配器和已保存错误在未使用扩展时保留原来的字段集合。"""
        result = handler(self)
        if self.details is None:
            result.pop("details", None)
        return result


class ContentFeedback(Model):
    """上一已知失败的内容调用；由控制器传递，不包含网络层状态。"""

    phase: Literal["compose", "verify"]
    call_id: NonEmpty
    error: ErrorInfo


class RunFailure(Model):
    """最近尚未解决的具体调用错误，用于终态说明和可追溯定位。"""

    phase: NonEmpty
    call_id: NonEmpty
    error: ErrorInfo


class Usage(Model):
    """记录一次适配器调用的用量，明确区分请求派发次数与实际测得的用量。"""

    input_tokens: NonNegativeInt | None = None
    output_tokens: NonNegativeInt | None = None
    cost: Annotated[float, Field(ge=0)] | None = None
    currency: NonEmpty | None = None
    provider_requests: Annotated[int, Field(ge=0, le=1)] = 1

    @classmethod
    def local(cls) -> Usage:
        """为本地操作记录零服务商请求和零 token 用量；费用保持未知。"""
        return cls(input_tokens=0, output_tokens=0, provider_requests=0)


class AggregateUsage(Usage):
    """运行级别的汇总用量可以包含多个实际服务商请求。"""

    provider_requests: NonNegativeInt = 0


class TokenBudget(Model):
    remaining_input_tokens: NonNegativeInt | None = None
    remaining_output_tokens: NonNegativeInt | None = None
    max_output_tokens_this_call: NonNegativeInt | None = None

    @model_validator(mode="after")
    def check_output_limit(self) -> TokenBudget:
        if (
            self.remaining_output_tokens is not None
            and self.max_output_tokens_this_call is not None
            and self.max_output_tokens_this_call > self.remaining_output_tokens
        ):
            raise ValueError("per-call output limit exceeds remaining output budget")
        return self


class CallContext(Model):
    schema_version: Literal["1.0"] = "1.0"
    run_id: NonEmpty
    task_id: NonEmpty
    operation_id: NonEmpty
    call_id: NonEmpty
    attempt: PositiveInt = 1
    timeout_ms: PositiveInt
    max_output_bytes: PositiveInt


class ModelCallContext(CallContext):
    model_id: NonEmpty
    model_params: JsonObject = Field(default_factory=dict)
    prompt_version: str | None = None
    token_budget: TokenBudget = Field(default_factory=TokenBudget)


class SourceRecord(Model):
    source_id: NonEmpty
    name: NonEmpty
    format: NonEmpty
    summary: str
    metadata: JsonObject = Field(default_factory=dict)
    content_hash: str | None = None
    version: str | None = None


class ToolSpec(Model):
    """可信工具能力声明；并发安全与重放安全相互独立。

    静态资源键描述适配器管理的共享连接或引擎。
    规划器不得通过工具参数或步骤元数据覆盖这些声明。
    """

    name: NonEmpty
    description: NonEmpty
    input_schema: JsonObject
    schema_version: NonEmpty = "1.0"
    idempotent: bool
    uses_sources: bool
    source_id_paths: list[str] = Field(default_factory=list)
    parallel_safe: bool = False
    exclusive_resource_keys: list[NonEmpty] = Field(default_factory=list)


class CatalogRequest(CallContext):
    snapshot_id: NonEmpty | None = None
    page_size: PositiveInt
    cursor: NonEmpty | None = None


class CatalogResult(Model):
    schema_version: Literal["1.0"] = "1.0"
    call_id: NonEmpty
    status: Literal["ok", "error"] = "ok"
    catalog_snapshot_id: NonEmpty | None = None
    sources: list[SourceRecord] = Field(default_factory=list)
    next_cursor: NonEmpty | None = None
    has_more: bool = False
    error: ErrorInfo | None = None
    duration_ms: NonNegativeInt = 0

    @model_validator(mode="after")
    def check_status(self) -> CatalogResult:
        if self.status == "ok":
            if self.error is not None or self.catalog_snapshot_id is None:
                raise ValueError("successful catalog requires snapshot and no error")
            if self.has_more != (self.next_cursor is not None):
                raise ValueError("has_more must match next_cursor presence")
        elif self.error is None or self.sources or self.next_cursor is not None or self.has_more:
            raise ValueError("failed catalog requires error and no sources or cursor")
        return self


class EvidencePayload(Model):
    source_id: NonEmpty
    content_type: NonEmpty
    payload: JsonValue
    locator: JsonObject
    warnings: list[str] = Field(default_factory=list)


class EvidenceRecord(EvidencePayload):
    evidence_id: NonEmpty
    call_id: NonEmpty


class ToolRequest(CallContext):
    step_id: NonEmpty
    catalog_snapshot_id: NonEmpty
    tool_name: NonEmpty
    args: JsonObject


class ToolResult(Model):
    schema_version: Literal["1.0"] = "1.0"
    call_id: NonEmpty
    status: Literal["ok", "error"] = "ok"
    data: JsonObject | None = None
    error: ErrorInfo | None = None
    duration_ms: NonNegativeInt = 0
    warnings: list[str] = Field(default_factory=list)
    truncated: bool = False

    @model_validator(mode="after")
    def check_status(self) -> ToolResult:
        if self.status == "ok" and (self.data is None or self.error is not None):
            raise ValueError("successful tool result requires data and no error")
        if self.status == "error" and (self.data is not None or self.error is None):
            raise ValueError("failed tool result requires error and null data")
        return self


class ModuleResult(Model, Generic[T]):
    schema_version: Literal["1.0"] = "1.0"
    call_id: NonEmpty
    status: Literal["ok", "error"] = "ok"
    data: T | None = None
    error: ErrorInfo | None = None
    usage: Usage = Field(default_factory=Usage)
    duration_ms: NonNegativeInt = 0

    @model_validator(mode="after")
    def check_status(self) -> ModuleResult[T]:
        if self.status == "ok" and (self.data is None or self.error is not None):
            raise ValueError("successful module result requires data and no error")
        if self.status == "error" and (self.data is not None or self.error is None):
            raise ValueError("failed module result requires error and null data")
        if self.usage.provider_requests > 1:
            raise ValueError("one module call may dispatch at most one provider request")
        return self


class CatalogRegistrationRequest(CallContext):
    catalog_snapshot_id: NonEmpty
    catalog_digest: NonEmpty
    sources: list[SourceRecord]


class CatalogHandle(Model):
    catalog_snapshot_id: NonEmpty
    catalog_digest: NonEmpty
    handle: NonEmpty


class Subtask(Model):
    subtask_id: NonEmpty
    objective: NonEmpty
    search_query: NonEmpty
    required_evidence: list[str] = Field(default_factory=list)
    depends_on: list[NonEmpty] = Field(default_factory=list)


class Candidate(Model):
    source_id: NonEmpty
    score: Annotated[float, Field(ge=0, le=1)]
    match_reason: str


class SearchRequest(CallContext):
    question: NonEmpty
    subtask: Subtask
    catalog_snapshot_id: NonEmpty
    catalog_handle: NonEmpty
    visited_source_ids: list[NonEmpty] = Field(default_factory=list)
    source_id_filter: list[NonEmpty] | None = None
    limit: PositiveInt


class SearchResponse(Model):
    candidates: list[Candidate] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_sources(self) -> SearchResponse:
        if len({item.source_id for item in self.candidates}) != len(self.candidates):
            raise ValueError("search candidates contain duplicate source IDs")
        return self


class AnswerRecord(Model):
    text: NonEmpty
    citations: list[NonEmpty] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    structured_result: JsonValue = None


class VerifyRequest(ModelCallContext):
    question: NonEmpty
    answer: AnswerRecord
    evidence: list[EvidenceRecord] = Field(default_factory=list)


class VerifyFeedbackRequest(VerifyRequest):
    """显式支持反馈的内容端口收到的核验请求；基础 VerifyRequest 不变。"""

    feedback: ContentFeedback


class Focus(Model):
    query: NonEmpty
    subtask_id: NonEmpty | None = None
    source_ids: list[NonEmpty] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)


class Recommendation(Model):
    action: Literal["none", "compose_again", "search_more", "query_again", "stop"]
    focus: Focus | None = None

    @model_validator(mode="after")
    def check_focus(self) -> Recommendation:
        if self.action in {"search_more", "query_again"}:
            if self.focus is None:
                raise ValueError("search/query action requires focus")
            if self.focus.subtask_id is None:
                raise ValueError("search/query feedback requires subtask_id")
            if self.action == "query_again" and not self.focus.source_ids:
                raise ValueError("query_again requires source IDs")
        elif self.focus is not None:
            raise ValueError("terminal/recompose actions require null focus")
        return self


class VerificationIssue(Model):
    type: NonEmpty
    evidence_ids: list[NonEmpty] = Field(default_factory=list)
    detail: NonEmpty


class Verification(Model):
    verdict: Literal["supported", "insufficient", "conflicting", "invalid_citation"]
    issues: list[VerificationIssue] = Field(default_factory=list)
    recommendation: Recommendation

    @model_validator(mode="after")
    def check_action(self) -> Verification:
        allowed = {
            "supported": {"none"},
            "insufficient": {"search_more", "query_again", "stop", "none"},
            "conflicting": {"search_more", "query_again", "stop", "none"},
            "invalid_citation": {"compose_again", "stop"},
        }
        if self.recommendation.action not in allowed[self.verdict]:
            raise ValueError("verification verdict and recommendation disagree")
        return self


class AnswerRevision(Model):
    """明确传递旧答案、相关核验及修改目标，不赋予内容端口任何运行权限。

    verification_is_current由控制器按完整可信输入计算；False表示历史意见，
    不能把意见过期理解为当前答案已经错误。首次生成不需要本对象。
    """

    previous_answer: AnswerRecord
    verification: Verification | None = None
    verification_is_current: bool = False
    instructions: RevisionInstructions | None = None

    @model_validator(mode="after")
    def require_current_verification(self) -> AnswerRevision:
        """没有核验对象时不能声称它适用于当前状态。"""
        if self.verification_is_current and self.verification is None:
            raise ValueError("current verification requires a verification result")
        return self


class ComposeRequest(ModelCallContext):
    """生成或修订答案；revision显式携带业务反馈，不改写原问题或证据。"""

    question: NonEmpty
    completed_subtasks: list[Subtask] = Field(default_factory=list)
    evidence: list[EvidenceRecord] = Field(default_factory=list)
    revision: AnswerRevision | None = None

    @model_serializer(mode="wrap")
    def omit_absent_revision(self, handler):
        """首次生成保留旧线格式；扩展纠错子类同样省略未使用的修订字段。"""
        result = handler(self)
        if self.revision is None:
            result.pop("revision", None)
        return result


class ComposeFeedbackRequest(ComposeRequest):
    """只有声明 content-feedback-v1 的内容端口才会收到的生成请求。"""

    feedback: ContentFeedback


class BudgetRemaining(Model):
    wall_time_ms: NonNegativeInt
    model_calls: NonNegativeInt
    data_calls: NonNegativeInt
    plan_steps: NonNegativeInt
    subtasks: NonNegativeInt


class AgentDecisionRequest(ModelCallContext):
    """一次自由决策的公开输入；运行身份和服务商配置仍由调用上下文携带。"""

    question: NonEmpty
    catalog_snapshot_id: NonEmpty
    tool_specs: list[ToolSpec]
    budget_remaining: BudgetRemaining
    context: JsonObject = Field(default_factory=dict)
    remaining_turns: NonNegativeInt
    require_verification: bool = False


class ToolStep(Model):
    step_id: NonEmpty
    subtask_id: NonEmpty
    tool_name: NonEmpty
    source_ids: list[NonEmpty]
    args: JsonObject
    depends_on: list[NonEmpty] = Field(default_factory=list)
    purpose: NonEmpty


class ToolPlan(Model):
    kind: Literal["actions"] = "actions"
    steps: Annotated[list[ToolStep], Field(min_length=1)]


class ReadObservationDecision(Model):
    """请求读取已观测结果的一页内容，不能用于请求文件系统路径。

    指针语法由运行内的读取器检查；格式错误时会明确返回 invalid_observation_read，
    且不会打开任何资源。偏移量按所选值的规范 JSON 中的 Unicode 字符计数。
    """

    kind: Literal["read_observation"] = "read_observation"
    observation_id: NonEmpty
    pointer: str = ""
    offset: NonNegativeInt = 0
    max_chars: Annotated[int, Field(ge=1, le=4096)] = 2000


class AgentSearchDecision(Model):
    """按问题相关查询检索目录，可进一步限制到已注册的数据源集合。"""

    kind: Literal["search"] = "search"
    query: NonEmpty
    source_ids: list[NonEmpty] | None = None


class AgentToolStep(Model):
    """自由循环中的工具步骤，不强制绑定控制器预先拆分的子任务。"""

    step_id: NonEmpty
    tool_name: NonEmpty
    source_ids: list[NonEmpty]
    args: JsonObject
    depends_on: list[NonEmpty] = Field(default_factory=list)
    purpose: NonEmpty


class AgentToolsDecision(Model):
    """提出至少一个工具步骤；授权、依赖与预算由控制器最终检查。"""

    kind: Literal["tools"] = "tools"
    steps: Annotated[list[AgentToolStep], Field(min_length=1)]


class AgentReadCatalogDecision(Model):
    """读取冻结的完整注册目录的一页；候选检索结果不等同于授权边界。"""

    kind: Literal["read_catalog"] = "read_catalog"
    offset: NonNegativeInt = 0
    limit: Annotated[int, Field(ge=1, le=100)] = 20


class AgentPlanDecision(Model):
    """可选地记录或调整子任务；空计划允许直接处理不需要拆解的问题。"""

    kind: Literal["plan"] = "plan"
    subtasks: list[Subtask] = Field(default_factory=list)
    completed_subtask_ids: list[NonEmpty] = Field(default_factory=list)


class AgentComposeDecision(Model):
    """提出生成或定向修订；修改要求不能改变控制器的预算与验收政策。"""

    kind: Literal["compose"] = "compose"
    revision_instructions: RevisionInstructions | None = None

    @model_serializer(mode="wrap")
    def omit_absent_revision_instructions(self, handler):
        """无定向要求的旧compose动作继续只序列化kind。"""
        result = handler(self)
        if self.revision_instructions is None:
            result.pop("revision_instructions", None)
        return result


class AgentVerifyDecision(Model):
    """请求内容适配器校验当前答案与可用证据。"""

    kind: Literal["verify"] = "verify"


class AgentFinishDecision(Model):
    """请求提交已组织的答案；完成条件由控制器而非模型声明决定。"""

    kind: Literal["finish"] = "finish"


class AgentStopDecision(Model):
    """在不能继续推进时明确停止，并保留可审计的原因。"""

    kind: Literal["stop"] = "stop"
    reason: NonEmpty


AgentAction = Annotated[
    AgentSearchDecision
    | AgentToolsDecision
    | ReadObservationDecision
    | AgentReadCatalogDecision
    | AgentPlanDecision
    | AgentComposeDecision
    | AgentVerifyDecision
    | AgentFinishDecision
    | AgentStopDecision,
    Field(discriminator="kind"),
]


class CallCounts(Model):
    planner: NonNegativeInt = 0
    content: NonNegativeInt = 0
    model_calls: NonNegativeInt = 0
    catalog: NonNegativeInt = 0
    tools: NonNegativeInt = 0
    retries: NonNegativeInt = 0
    steps: NonNegativeInt = 0
    observation_reads: NonNegativeInt = 0


class Versions(Model):
    dataset_snapshot: str | None = None
    model_id: NonEmpty
    model_params: JsonObject
    prompt_versions: PromptVersions
    code_version: str | None = None
    config_version: NonEmpty


class RunResult(Model):
    schema_version: Literal["1.0"] = "1.0"
    run_id: NonEmpty
    task_id: NonEmpty
    mode: RunMode
    status: Status
    stop_reason: NonEmpty
    answer: AnswerRecord | None = None
    usage: AggregateUsage = Field(default_factory=AggregateUsage.local)
    call_counts: CallCounts = Field(default_factory=CallCounts)
    started_at: NonEmpty
    ended_at: NonEmpty
    duration_ms: NonNegativeInt
    versions: Versions
    trace_ref: str | None = None
    failure: RunFailure | None = None

    @model_serializer(mode="wrap")
    def omit_absent_failure(self, handler):
        """普通旧结果不增加 null 字段，成功和历史报告保持原来的线形状。"""
        result = handler(self)
        if self.failure is None:
            result.pop("failure", None)
        return result

    @model_validator(mode="after")
    def require_completed_answer(self) -> RunResult:
        if self.status == "completed" and self.answer is None:
            raise ValueError("completed runs require a valid answer")
        return self


class TraceEvent(Model):
    """精简的审计记录；大体积载荷须先写入工件。"""

    schema_version: Literal["1.0"] = "1.0"
    run_id: NonEmpty
    task_id: NonEmpty
    seq: PositiveInt
    event_type: NonEmpty
    timestamp_utc: NonEmpty
    step_id: str | None = None
    subtask_id: str | None = None
    operation_id: str | None = None
    call_id: str | None = None
    attempt: PositiveInt | None = None
    status: str | None = None
    duration_ms: NonNegativeInt | None = None
    usage: Usage | None = None
    error: ErrorInfo | None = None
    data_ref: str | None = None
    detail: JsonObject = Field(default_factory=dict)
