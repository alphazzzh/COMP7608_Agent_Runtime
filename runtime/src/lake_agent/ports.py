"""Structural async integration ports; implementations keep ownership of resources.

The controller awaits these methods and never starts an event loop internally.
Adapters must be reentrant across different runs, propagate cancellation and
disable implicit provider retries.  tool_specs is the sole synchronous method
because it returns static local metadata and must perform no data access.

Within a run, invoke may overlap only for trusted parallel_safe tools with
disjoint source and exclusive-resource claims. Declaring idempotence grants
retry eligibility, not concurrency. The controller still owns budgets and all
semantic commits; adapter methods must not mutate request objects as a way to
change control policy or send unreported provider requests.
"""

from typing import Protocol

from .models import (
    AnswerRecord,
    CatalogHandle,
    CatalogRegistrationRequest,
    CatalogRequest,
    CatalogResult,
    ComposeRequest,
    FinishDecision,
    JsonValue,
    ModuleResult,
    PlannerRequest,
    ReadObservationDecision,
    SearchRequest,
    SearchResponse,
    TaskPlan,
    ToolPlan,
    ToolPlanningRequest,
    ToolRequest,
    ToolResult,
    ToolSpec,
    TraceEvent,
    Verification,
    VerifyRequest,
)


class DataPort(Protocol):
    async def catalog(self, request: CatalogRequest) -> CatalogResult: ...
    def tool_specs(self) -> list[ToolSpec]: ...
    async def invoke(self, request: ToolRequest) -> ToolResult: ...


class ContentPort(Protocol):
    async def prepare_catalog(
        self, request: CatalogRegistrationRequest
    ) -> ModuleResult[CatalogHandle]: ...
    async def search(self, request: SearchRequest) -> ModuleResult[SearchResponse]: ...
    async def compose(self, request: ComposeRequest) -> ModuleResult[AnswerRecord]: ...
    async def verify(self, request: VerifyRequest) -> ModuleResult[Verification]: ...


class PlannerPort(Protocol):
    async def decompose(self, request: PlannerRequest) -> ModuleResult[TaskPlan]: ...
    async def select_tools(
        self, request: ToolPlanningRequest
    ) -> ModuleResult[ToolPlan | FinishDecision | ReadObservationDecision]: ...


class TraceSink(Protocol):
    async def create_run(self, run_id: str) -> None: ...
    async def append(self, event: TraceEvent) -> None: ...
    async def put_artifact(self, ref: str, json_value: JsonValue) -> None: ...
    async def close(self) -> str: ...


class AttemptTraceSink(TraceSink, Protocol):
    """Optional audit sink that isolates recovery attempts under one run ID.

    Each execution uses a fresh sink and a fresh attempt ID. Events retain the
    original run ID, while sequences and artifacts belong only to that attempt.
    Ordinary runs can continue to use the base TraceSink protocol unchanged.
    """

    async def create_attempt(self, run_id: str, attempt_id: str) -> None:
        """Exclusively create an attempt segment; never overwrite a prior one."""
        ...
