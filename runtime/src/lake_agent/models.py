"""Strict JSON boundary models for the asynchronous data-lake runtime.

Every public payload is JSON serializable.  Context fields are intentionally
flattened on requests so adapters can validate plain incoming JSON with one
``model_validate`` call.  Runtime-dependent checks (authorized sources, budgets
and snapshot identity) live in validation.py and the controller.
"""

from __future__ import annotations

from typing import Annotated, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

JsonObject = dict[str, JsonValue]
NonEmpty = Annotated[str, Field(min_length=1)]
PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeInt = Annotated[int, Field(ge=0)]
Mode = Literal["fixed_retrieval", "no_verifier", "full_agent"]
Status = Literal["completed", "insufficient_evidence", "budget_exceeded", "timeout", "failed"]
T = TypeVar("T")


class Model(BaseModel):
    """Reject unknown fields, coercion and non-finite JSON numbers."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class PromptVersions(Model):
    planner: str | None = "planner-v0.2-observations"
    content: str | None = "content-v0.1"


class RunConfig(Model):
    """Editable run limits; defaults are suitable for the offline example."""

    max_wall_time_ms: PositiveInt = 120_000
    per_call_timeout_ms: PositiveInt = 20_000
    cancel_cleanup_timeout_ms: PositiveInt = 3_000
    max_subtasks: PositiveInt = 8
    max_plan_steps: PositiveInt = 8
    max_data_calls: PositiveInt = 12
    max_model_calls: PositiveInt = 12
    max_retries_per_data_operation: NonNegativeInt = 1
    max_feedback_rounds: NonNegativeInt = 2
    candidate_top_k: PositiveInt = 5
    max_output_bytes_per_call: PositiveInt = 65_536
    catalog_page_size: PositiveInt = 100
    max_catalog_sources: PositiveInt = 2_000
    max_catalog_bytes: PositiveInt = 2_000_000
    fixed_extract_max_chars: PositiveInt = 12_000
    # These are UTF-8 JSON working-context limits, not provider token limits.
    max_planner_context_bytes: Annotated[int, Field(ge=1024)] = 24_000
    planner_preview_bytes: Annotated[int, Field(ge=256)] = 2_000
    max_observation_reads: NonNegativeInt = 8
    max_identical_tool_calls: PositiveInt = 2
    # The adapter must separately opt tools into concurrency. Old tools remain serial.
    max_parallel_tool_calls: Annotated[int, Field(ge=1, le=32)] = 4
    max_input_tokens: PositiveInt | None = None
    max_output_tokens: PositiveInt | None = None
    dataset_snapshot: NonEmpty | None = None
    model_id: NonEmpty = "offline-demo"
    model_params: JsonObject = Field(default_factory=lambda: {"temperature": 0})
    prompt_versions: PromptVersions = Field(default_factory=PromptVersions)
    code_version: str | None = None
    config_version: NonEmpty = "dev-v1"

    @model_validator(mode="after")
    def check_context_limits(self) -> RunConfig:
        """Leave room for provenance, omission counts and a requested page."""
        if self.planner_preview_bytes > self.max_planner_context_bytes // 2:
            raise ValueError("planner_preview_bytes exceeds half the context budget")
        return self


class RunRequest(Model):
    schema_version: Literal["1.0"] = "1.0"
    task_id: NonEmpty
    question: NonEmpty
    mode: Mode = "full_agent"
    run_id: NonEmpty | None = None
    config: RunConfig = Field(default_factory=RunConfig)


class ErrorInfo(Model):
    """An adapter error; replay and corrected-input planning are separate rights.

    Only a trusted data adapter may declare an explicitly failed operation safe
    to correct. Unknown outcomes and runtime validation errors never recover.
    """

    code: NonEmpty
    message: NonEmpty
    retryable: bool = False
    recoverable: bool = False


class Usage(Model):
    """Usage for one adapter call; dispatch and measured quantities are explicit."""

    input_tokens: NonNegativeInt | None = None
    output_tokens: NonNegativeInt | None = None
    cost: Annotated[float, Field(ge=0)] | None = None
    currency: NonEmpty | None = None
    provider_requests: Annotated[int, Field(ge=0, le=1)] = 1

    @classmethod
    def local(cls) -> Usage:
        """A local operation is known to have issued zero provider requests."""
        return cls(input_tokens=0, output_tokens=0, provider_requests=0)


class AggregateUsage(Usage):
    """Run-level totals may include more than one actual provider request."""

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
    """Trusted capabilities; safe concurrency and safe replay are independent.

    Static resource keys describe adapter-owned shared connections or engines.
    The planner cannot override these claims in tool arguments or step metadata.
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


class ComposeRequest(ModelCallContext):
    question: NonEmpty
    completed_subtasks: list[Subtask] = Field(default_factory=list)
    evidence: list[EvidenceRecord] = Field(default_factory=list)


class AnswerRecord(Model):
    text: NonEmpty
    citations: list[NonEmpty] = Field(default_factory=list)
    unresolved_questions: list[str] = Field(default_factory=list)
    structured_result: JsonValue = None


class VerifyRequest(ModelCallContext):
    question: NonEmpty
    answer: AnswerRecord
    evidence: list[EvidenceRecord] = Field(default_factory=list)


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


class BudgetRemaining(Model):
    wall_time_ms: NonNegativeInt
    model_calls: NonNegativeInt
    data_calls: NonNegativeInt
    plan_steps: NonNegativeInt
    subtasks: NonNegativeInt


class PlannerRequest(ModelCallContext):
    question: NonEmpty
    catalog_snapshot_id: NonEmpty
    tool_specs: list[ToolSpec]
    budget_remaining: BudgetRemaining


class TaskPlan(Model):
    subtasks: Annotated[list[Subtask], Field(min_length=1)]


class ToolPlanningRequest(ModelCallContext):
    question: NonEmpty
    subtask: Subtask
    candidates: list[Candidate]
    evidence_summary: list[EvidenceRecord | JsonObject] = Field(default_factory=list)
    tool_observations: list[JsonObject] = Field(default_factory=list)
    context_status: JsonObject = Field(default_factory=dict)
    observation_read: JsonObject | None = None
    verifier_focus: Focus | None = None
    tool_specs: list[ToolSpec]
    budget_remaining: BudgetRemaining


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


class FinishDecision(Model):
    kind: Literal["finish"] = "finish"
    reason: Literal["evidence_complete", "insufficient_evidence", "not_applicable"]


class ReadObservationDecision(Model):
    """Request a page from a previously observed result, never a filesystem path.

    Pointer syntax is checked by the run-local reader so malformed requests get
    an explicit invalid_observation_read result without opening any resource.
    Offsets count Unicode characters in the selected value's canonical JSON.
    """

    kind: Literal["read_observation"] = "read_observation"
    observation_id: NonEmpty
    pointer: str = ""
    offset: NonNegativeInt = 0
    max_chars: Annotated[int, Field(ge=1, le=4096)] = 2000


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
    mode: Mode
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

    @model_validator(mode="after")
    def require_completed_answer(self) -> RunResult:
        if self.status == "completed" and self.answer is None:
            raise ValueError("completed runs require a valid answer")
        return self


class TraceEvent(Model):
    """Small audit record; bulky payloads are written as artifacts first."""

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
