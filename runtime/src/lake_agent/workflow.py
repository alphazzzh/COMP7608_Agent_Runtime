"""Run-local LangGraph orchestration over the runtime's existing safety primitives.

The graph owns only control flow: the bound controller still authorizes every
adapter call, accounts for budgets, allocates semantic IDs, commits evidence and
writes the audit journal. State contains transient routing/planning values and
is deliberately neither checkpointed nor shared between invocations. Durable
recovery continues to replay the controller's single known-response journal.

Each named node represents a real stage of the original workflow. In particular,
observation reads return directly to planning, execution returns to retrieval,
and feedback revisits an accepted subtask rather than constructing another plan.
No graph diagnostic is added to the public trace or its stable identifier stream.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, TypedDict

from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tracers.context import tracing_v2_callback_var
from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from . import models as m
from .context import build_planner_context
from .errors import RunStopped as _Stop
from .validation import canonical_json, validate_dag, validate_tool_specs

if TYPE_CHECKING:
    from .runtime import _Controller


class _NodeCancelled(Exception):
    """Carry deliberate node cancellation through LangGraph's task scheduler.

    A trace/adapter may raise CancelledError without cancelling the graph task
    (the Web pause hook does this after saving evidence). LangGraph treats a
    cancelled child as settled and may return an incomplete state instead of
    propagating it. A private ordinary exception forces graph unwind; the owned
    invocation restores CancelledError only after all graph children are gone.
    """


class WorkflowState(TypedDict, total=False):
    """Transient values belonging to exactly one bound controller invocation.

    Controller-owned evidence, usage, accepted plans and audit history are not
    duplicated here. Nodes replace each routing field serially; no reducers or
    parallel writes are needed. Read pages live only through a contiguous read
    phase of the active subtask, matching the planner's bounded context contract.
    """

    next_node: str
    subtask_cursor: int
    failed_subtask_ids: set[str]
    active_subtask: m.Subtask
    candidates: list[m.Candidate]
    decision: m.ToolPlan | m.FinishDecision | m.ReadObservationDecision
    read_result: dict | None
    read_pages: list[dict]
    feedback_focus: m.Focus | None
    feedback_round: int
    feedback_action: str
    in_feedback: bool
    terminal_reason: str


def graph_step_limit(config: m.RunConfig) -> int:
    """Bound transitions by finite control budgets, never provider requests.

    A normal cycle consumes a subtask, feedback round, tool step or observation
    read. Eight transitions per unit conservatively cover routing, completion,
    compose and verification; the constant covers initialization and the final
    over-budget attempt. Local ModuleResult replies release model reservations,
    so max_model_calls cannot bound the number of graph transitions.
    """
    return 32 + 8 * (
        config.max_subtasks
        + config.max_feedback_rounds
        + config.max_plan_steps
        + config.max_observation_reads
    )


@contextmanager
def _local_graph_context() -> Iterator[None]:
    """Keep this graph independent of ambient LangSmith/Runnable tracing.

    In the supported Core version, callback configuration first checks the
    tracing_v2 callback ContextVar, then LangSmith's enabled context/environment.
    Clearing only the latter would still attach an outer LangChainTracer. Also
    clear inherited Runnable configuration so outer callbacks/checkpoint options
    cannot enter this private invocation. Tokens restore the caller's exact
    values; process environment and concurrent task contexts are never changed.
    """
    tracer_token = tracing_v2_callback_var.set(None)
    config_token = var_child_runnable_config.set(None)
    try:
        with tracing_context(enabled=False, parent=False):
            yield
    finally:
        var_child_runnable_config.reset(config_token)
        tracing_v2_callback_var.reset(tracer_token)


def build_workflow(controller: _Controller):
    """Compile a fresh, controller-bound graph without retries/cache/checkpoints."""
    c = controller

    async def initialize(state: WorkflowState):
        if c.cfg.max_input_tokens is not None or c.cfg.max_output_tokens is not None:
            raise _Stop("failed", "unsupported_token_budget")
        try:
            c.specs = validate_tool_specs(c.data.tool_specs())
        except Exception as exc:
            raise _Stop("failed", "invalid_tool_contract") from exc
        return {
            "subtask_cursor": 0,
            "failed_subtask_ids": set(),
            "feedback_round": 0,
            "feedback_focus": None,
            "in_feedback": False,
            "read_result": None,
            "read_pages": [],
        }

    async def catalog(state: WorkflowState):
        await c._catalog()
        return {"next_node": "fixed_search" if c.req.mode == "fixed_retrieval" else "decompose"}

    async def decompose(state: WorkflowState):
        reply = await c._call(
            "planner.decompose",
            c.planner.decompose,
            m.PlannerRequest,
            dict(
                question=c.req.question,
                catalog_snapshot_id=c.snapshot,
                tool_specs=list(c.specs.values()),
                budget_remaining=c._remaining(),
            ),
            m.ModuleResult[m.TaskPlan],
            provider=True,
        )
        try:
            if len(reply.data.subtasks) > c.cfg.max_subtasks:
                raise ValueError("too many subtasks")
            c.subtasks = validate_dag(reply.data.subtasks, "subtask_id")
        except ValueError as exc:
            raise _Stop("failed", "invalid_plan") from exc
        ref = await c._artifact(reply.data)
        await c._event("task_plan.accepted", data_ref=ref)
        return {}

    async def next_subtask(state: WorkflowState):
        cursor = state["subtask_cursor"]
        if cursor >= len(c.subtasks):
            if not c.evidence:
                raise _Stop("insufficient_evidence", "planner_finished_no_evidence")
            return {"next_node": "compose"}
        subtask = c.subtasks[cursor]
        failed = state["failed_subtask_ids"]
        if set(subtask.depends_on) & failed:
            await c._event(
                "subtask.state", status="blocked", detail={"subtask_id": subtask.subtask_id}
            )
            return {
                "failed_subtask_ids": failed | {subtask.subtask_id},
                "subtask_cursor": cursor + 1,
                "next_node": "next_subtask",
            }
        await c._event("subtask.state", status="running", detail={"subtask_id": subtask.subtask_id})
        return {
            "active_subtask": subtask,
            "read_result": None,
            "read_pages": [],
            "next_node": "search",
        }

    async def search(state: WorkflowState):
        candidates = await c._search(state["active_subtask"], state["feedback_focus"])
        return {"candidates": candidates}

    async def select_tools(state: WorkflowState):
        c._time()
        subtask = state["active_subtask"]
        context = build_planner_context(
            c.observations,
            c.evidence,
            subtask_id=subtask.subtask_id,
            max_bytes=c.cfg.max_planner_context_bytes,
            preview_bytes=c.cfg.planner_preview_bytes,
            read_result=state["read_result"],
        )
        context["context_status"]["remaining_observation_reads"] = max(
            0, c.cfg.max_observation_reads - c.counts.observation_reads
        )
        if len(canonical_json(context)) > c.cfg.max_planner_context_bytes:
            del context["context_status"]["remaining_observation_reads"]
        result = await c._call(
            "planner.select_tools",
            c.planner.select_tools,
            m.ToolPlanningRequest,
            dict(
                question=c.req.question,
                subtask=subtask,
                candidates=state["candidates"],
                **context,
                verifier_focus=state["feedback_focus"],
                tool_specs=list(c.specs.values()),
                budget_remaining=c._remaining(),
            ),
            m.ModuleResult[m.ToolPlan | m.FinishDecision | m.ReadObservationDecision],
            provider=True,
        )
        decision = result.data
        destination = (
            "read_observation"
            if isinstance(decision, m.ReadObservationDecision)
            else "finish_subtask"
            if isinstance(decision, m.FinishDecision)
            else "execute_plan"
        )
        return {"decision": decision, "next_node": destination}

    async def read_observation(state: WorkflowState):
        page = await c._read_observation(state["decision"])
        pages = state["read_pages"]
        result = dict(page, previous_pages=[], omitted_pages=len(pages))
        # Preserve recent whole pages up to half the planner-context byte cap.
        # This exactly retains the original contiguous-read history semantics.
        for previous in reversed(pages):
            result["previous_pages"].insert(0, previous)
            result["omitted_pages"] -= 1
            if len(canonical_json(result)) > c.cfg.max_planner_context_bytes // 2:
                result["previous_pages"].pop(0)
                result["omitted_pages"] += 1
                break
        # State is private and graph nodes commit serially. Append in place to
        # avoid copying the complete page history for every bounded page read.
        pages.append(page)
        return {"read_result": result, "read_pages": pages}

    async def execute_plan(state: WorkflowState):
        await c._execute_plan(state["decision"], state["active_subtask"], state["candidates"])
        return {"read_result": None, "read_pages": []}

    async def finish_subtask(state: WorkflowState):
        subtask = state["active_subtask"]
        reason = state["decision"].reason
        await c._event(
            "subtask.state",
            status=(
                "succeeded"
                if reason == "evidence_complete"
                else "skipped"
                if reason == "not_applicable"
                else "failed"
            ),
            detail={"subtask_id": subtask.subtask_id, "reason": reason},
        )
        if state["in_feedback"]:
            # Revisiting a task replaces its completion state. Rebuild in the
            # accepted DAG order, preserving the original no-duplicate behavior.
            completed = {item.subtask_id for item in c.completed_subtasks}
            if reason == "evidence_complete":
                completed.add(subtask.subtask_id)
            else:
                completed.discard(subtask.subtask_id)
            c.completed_subtasks = [item for item in c.subtasks if item.subtask_id in completed]
            return {"next_node": "compose"}
        failed = state["failed_subtask_ids"]
        if reason == "evidence_complete":
            c.completed_subtasks.append(subtask)
        else:
            # Skipped prerequisites block descendants just like failed ones.
            failed = failed | {subtask.subtask_id}
        return {
            "failed_subtask_ids": failed,
            "subtask_cursor": state["subtask_cursor"] + 1,
            "next_node": "next_subtask",
        }

    async def compose(state: WorkflowState):
        await c._compose()
        if c.req.mode == "fixed_retrieval":
            return {"terminal_reason": "fixed_retrieval_completed", "next_node": END}
        if c.req.mode == "no_verifier":
            return {"terminal_reason": "planner_finished", "next_node": END}
        return {"next_node": "verify"}

    async def verify(state: WorkflowState):
        reply = await c._call(
            "content.verify",
            c.content.verify,
            m.VerifyRequest,
            dict(question=c.req.question, answer=c.answer, evidence=c.evidence),
            m.ModuleResult[m.Verification],
            provider=True,
        )
        verification = reply.data
        if any(
            not set(issue.evidence_ids) <= {e.evidence_id for e in c.evidence}
            for issue in verification.issues
        ):
            raise _Stop("failed", "invalid_module_response")
        recommendation = verification.recommendation
        focus = recommendation.focus
        if focus and not set(focus.source_ids) <= c.sources.keys():
            raise _Stop("failed", "invalid_module_response")
        ref = await c._artifact(verification)
        await c._event("verification", data_ref=ref, status=verification.verdict)
        if verification.verdict == "supported":
            return {"terminal_reason": "verified_supported", "next_node": END}
        if recommendation.action in ("stop", "none"):
            raise _Stop(
                "insufficient_evidence",
                "verifier_stop" if recommendation.action == "stop" else "no_further_action",
            )
        return {
            "feedback_focus": focus,
            "feedback_action": recommendation.action,
            "next_node": "feedback",
        }

    async def feedback(state: WorkflowState):
        if state["feedback_round"] >= c.cfg.max_feedback_rounds:
            raise _Stop("insufficient_evidence", "feedback_exhausted")
        round_number = state["feedback_round"] + 1
        action = state["feedback_action"]
        await c._event("feedback.accepted", detail={"round": round_number, "action": action})
        update = {"feedback_round": round_number, "in_feedback": True}
        if action == "compose_again":
            return {**update, "next_node": "compose"}
        focus = state["feedback_focus"]
        if focus is None:
            raise _Stop("failed", "invalid_module_response")
        target = next((item for item in c.subtasks if item.subtask_id == focus.subtask_id), None)
        if target is None:
            raise _Stop("failed", "invalid_module_response")
        await c._event("subtask.state", status="running", detail={"subtask_id": target.subtask_id})
        return {
            **update,
            "active_subtask": target,
            "read_result": None,
            "read_pages": [],
            "next_node": "search",
        }

    async def fixed_search(state: WorkflowState):
        subtask = m.Subtask(
            subtask_id="fixed", objective=c.req.question, search_query=c.req.question
        )
        candidates = await c._search(subtask)
        if not candidates:
            raise _Stop("insufficient_evidence", "no_candidates")
        if "extract" not in c.specs:
            raise _Stop("failed", "invalid_tool_contract")
        return {"active_subtask": subtask, "candidates": candidates}

    async def fixed_execute(state: WorkflowState):
        steps = [
            m.ToolStep(
                step_id=c._id("step"),
                subtask_id="fixed",
                tool_name="extract",
                source_ids=[candidate.source_id],
                args={
                    "source_id": candidate.source_id,
                    "query": c.req.question,
                    "max_chars": c.cfg.fixed_extract_max_chars,
                },
                purpose="Read fixed top-k evidence",
            )
            for candidate in state["candidates"]
        ]
        await c._execute_plan(m.ToolPlan(steps=steps), state["active_subtask"], state["candidates"])
        if not c.evidence:
            raise _Stop("insufficient_evidence", "no_evidence")
        c.completed_subtasks = [state["active_subtask"]]
        return {}

    # Explicit edges make the actual execution stages inspectable. Routing
    # callbacks read only per-run state and never authorize or dispatch work.
    graph = StateGraph(WorkflowState)
    nodes = {
        "initialize": initialize,
        "catalog": catalog,
        "decompose": decompose,
        "next_subtask": next_subtask,
        "search": search,
        "select_tools": select_tools,
        "read_observation": read_observation,
        "execute_plan": execute_plan,
        "finish_subtask": finish_subtask,
        "compose": compose,
        "verify": verify,
        "feedback": feedback,
        "fixed_search": fixed_search,
        "fixed_execute": fixed_execute,
    }

    def cancellation_bridge(node):
        async def guarded(state: WorkflowState):
            try:
                return await node(state)
            except asyncio.CancelledError as exc:
                raise _NodeCancelled() from exc

        return guarded

    for name, node in nodes.items():
        graph.add_node(name, cancellation_bridge(node))
    for source, target in (
        (START, "initialize"),
        ("initialize", "catalog"),
        ("decompose", "next_subtask"),
        ("search", "select_tools"),
        ("read_observation", "select_tools"),
        ("execute_plan", "search"),
        ("fixed_search", "fixed_execute"),
        ("fixed_execute", "compose"),
    ):
        graph.add_edge(source, target)
    for name, destinations in (
        ("catalog", ("fixed_search", "decompose")),
        ("next_subtask", ("next_subtask", "search", "compose")),
        ("select_tools", ("read_observation", "finish_subtask", "execute_plan")),
        ("finish_subtask", ("compose", "next_subtask")),
        ("compose", (END, "verify")),
        ("verify", (END, "feedback")),
        ("feedback", ("compose", "search")),
    ):
        graph.add_conditional_edges(name, lambda state: state["next_node"], list(destinations))
    with _local_graph_context():
        return graph.compile()


async def execute_workflow(controller: _Controller) -> str:
    """Run and fully settle the private graph before controller cleanup begins.

    LangGraph's AsyncBackgroundExecutor.__aexit__ cancels its node tasks and
    awaits asyncio.wait(tasks). A second cancellation of that exit would abort
    the wait and leave a live node writing to an already-closed trace or released
    recovery lease. Own one task, shield the caller's await, forward cancellation
    exactly once, and shield-drain through any subsequent caller cancellations.
    """

    async def invoke():
        with _local_graph_context():
            graph = build_workflow(controller)
            try:
                result = await graph.ainvoke(
                    {},
                    config={
                        "recursion_limit": graph_step_limit(controller.cfg),
                        "callbacks": [],
                        "max_concurrency": 1,
                    },
                )
            except _NodeCancelled as exc:
                raise asyncio.CancelledError() from exc
            return result["terminal_reason"]

    task = asyncio.create_task(invoke())
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        task.cancel()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Further cancellation belongs to the outer caller only. Do not
                # forward it into graph exit/node settlement a second time.
                continue
            except Exception:
                break
        # Observe a raced failure, but preserve the caller's cancellation.
        if not task.cancelled():
            task.exception()
        raise
