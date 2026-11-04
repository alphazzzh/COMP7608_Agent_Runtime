"""模型逐步选择行动的 LangGraph；不预设检索、任务拆分或核验顺序。

图只提供可选择的操作环境，所有外部调用和工具执行仍经过同一个控制器。
可选计划用于模型跟踪任务，不作为工具执行的强制前置流程。
A/C 请求沿用既有严格模型；内部单运行工具 scope 不要求模型虚构子任务。
"""

from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from .. import models as m
from ..validation import canonical_json, validate_dag, validate_tool_specs
from .agent_context import build_agent_context, catalog_page, context_page_budget
from .errors import RunStopped


class AgentState(TypedDict, total=False):
    """仅保存确定性的路由状态；恢复从已知响应逐步重建，不开启图检查点。"""

    turns: int
    decision: m.AgentAction
    next_node: str
    terminal_reason: str
    candidates: list[m.Candidate]
    read_result: dict | None
    read_pages: list[dict]
    read_count: int
    catalog_page: dict | None
    verification: m.Verification | None
    last_action: dict | None


def build_agent_workflow(controller, guard):
    """编译按动作路由的图；guard 使用统一执行器的取消传播桥。"""
    c = controller
    scope = m.Subtask(subtask_id="agent", objective=c.req.question, search_query=c.req.question)

    async def initialize(state):
        if c.req.mode == "no_verifier" and c.cfg.require_verification:
            raise RunStopped("failed", "invalid_run_policy")
        if c.cfg.max_input_tokens is not None or c.cfg.max_output_tokens is not None:
            raise RunStopped("failed", "unsupported_token_budget")
        try:
            c.specs = validate_tool_specs(c.data.tool_specs())
        except Exception as exc:
            raise RunStopped("failed", "invalid_tool_contract") from exc
        await c._catalog()
        return {
            "turns": 0,
            "candidates": [],
            "read_result": None,
            "read_pages": [],
            "read_count": 0,
            "catalog_page": None,
            "verification": None,
            "last_action": None,
        }

    async def decide(state):
        c._time()
        if state["turns"] >= c.cfg.max_agent_turns:
            raise RunStopped("budget_exceeded", "agent_turn_budget")
        reply = await c._call(
            "planner.decide",
            c.planner.decide,
            m.AgentDecisionRequest,
            dict(
                question=c.req.question,
                catalog_snapshot_id=c.snapshot,
                tool_specs=list(c.specs.values()),
                budget_remaining=c._remaining(),
                context=build_agent_context(c, state),
                remaining_turns=c.cfg.max_agent_turns - state["turns"],
                require_verification=c.cfg.require_verification,
            ),
            m.ModuleResult[m.AgentAction],
            provider=True,
        )
        decision = reply.data
        ref = await c._artifact(decision)
        await c._event(
            "agent.decision.accepted",
            data_ref=ref,
            detail={"kind": decision.kind, "turn": state["turns"] + 1},
        )
        destination = (
            "read" if decision.kind in {"read_observation", "read_catalog"} else decision.kind
        )
        return {"turns": state["turns"] + 1, "decision": decision, "next_node": destination}

    def accepted(kind, **extra):
        return {"last_action": {"kind": kind, "status": "ok", **extra}}

    async def reject(kind, code, *, reason=None):
        detail = {"kind": kind, "code": code}
        if reason is not None:
            detail["reason"] = reason
        await c._event("agent.action.rejected", status="rejected", detail=detail)
        return {
            "last_action": {"status": "rejected", **detail},
            "next_node": "decide",
        }

    async def search(state):
        action = state["decision"]
        if action.source_ids is not None:
            if (
                len(set(action.source_ids)) != len(action.source_ids)
                or not set(action.source_ids) <= c.sources.keys()
            ):
                raise RunStopped("failed", "invalid_plan")
        focus = m.Focus(query=action.query, source_ids=action.source_ids or [])
        candidates = await c._search(scope, focus, source_ids=action.source_ids)
        return {
            **accepted("search", count=len(candidates)),
            "candidates": candidates,
            "read_result": None,
            "read_pages": [],
            "read_count": 0,
            "catalog_page": None,
        }

    async def tools(state):
        decision = state["decision"]
        plan = m.ToolPlan(
            steps=[
                m.ToolStep(subtask_id=scope.subtask_id, **step.model_dump())
                for step in decision.steps
            ]
        )
        await c._execute_plan(plan, scope, [], authorized_sources=set(c.sources))
        return {
            **accepted("tools", steps=len(plan.steps)),
            "read_result": None,
            "read_pages": [],
            "read_count": 0,
            "catalog_page": None,
        }

    async def read(state):
        action = state["decision"]
        if action.kind == "read_observation":
            page = await c._read_observation(
                action, max_page_bytes=context_page_budget(c, state, observation=True)
            )
            result = dict(page, previous_pages=[], omitted_pages=state["read_count"])
            # 决策适配器无会话内存，保留能完整装入配额的最近页面；
            # 历史本身也有界，不能在控制器内累积所有已读大结果。
            for previous in reversed(state["read_pages"]):
                result["previous_pages"].insert(0, previous)
                result["omitted_pages"] -= 1
                if len(canonical_json(result)) > c.cfg.max_planner_context_bytes // 2:
                    result["previous_pages"].pop(0)
                    result["omitted_pages"] += 1
                    break
            return {
                **accepted(action.kind),
                "read_result": result,
                "catalog_page": None,
                "read_pages": [*result["previous_pages"], page],
                "read_count": state["read_count"] + 1,
            }
        page = catalog_page(
            c,
            action.offset,
            action.limit,
            max_bytes=context_page_budget(c, state, observation=False),
        )
        ref = await c._artifact(page)
        await c._event(
            "catalog.read",
            data_ref=ref,
            detail={"offset": action.offset, "count": len(page["sources"])},
        )
        return {
            **accepted(action.kind),
            "catalog_page": page,
            "read_result": None,
            "read_pages": [],
            "read_count": 0,
        }

    async def plan(state):
        action = state["decision"]
        try:
            if len(action.subtasks) > c.cfg.max_subtasks:
                raise ValueError("subtask budget")
            tasks = validate_dag(action.subtasks, "subtask_id")
            completed = set(action.completed_subtask_ids)
            if len(completed) != len(action.completed_subtask_ids) or not completed <= {
                s.subtask_id for s in tasks
            }:
                raise ValueError("unknown completed task")
            if any(not set(s.depends_on) <= completed for s in tasks if s.subtask_id in completed):
                raise ValueError("unfinished dependency")
        except ValueError as exc:
            raise RunStopped("failed", "invalid_plan") from exc
        c.subtasks = tasks
        c.completed_subtasks = [s for s in tasks if s.subtask_id in completed]
        ref = await c._artifact(action)
        await c._event("task_plan.updated", data_ref=ref, detail={"count": len(tasks)})
        return accepted("plan")

    async def compose(state):
        instructions = state["decision"].revision_instructions
        if instructions is not None and c.answer is None:
            return await reject(
                "compose", "revision_answer_required", reason="Compose an initial answer first."
            )
        previous = c.answer
        if not await c._compose(revision_instructions=instructions):
            return {
                "last_action": {
                    "kind": "compose",
                    "status": "rejected",
                    "error": c.content_feedback["compose"].error.model_dump(mode="json"),
                }
            }
        # 完全相同的答案不能通过一次无变化生成洗掉已失败的核验。
        verification = state.get("verification") if c.answer == previous else None
        return {**accepted("compose"), "verification": verification}

    async def verify(state):
        if c.answer is None:
            return await reject("verify", "answer_required")
        if c.req.mode == "no_verifier":
            return await reject("verify", "verification_disabled")
        value = await c._verify()
        if value is None:
            return {
                "last_action": {
                    "kind": "verify",
                    "status": "rejected",
                    "error": c.content_feedback["verify"].error.model_dump(mode="json"),
                }
            }
        ref = await c._artifact(value)
        await c._event(
            "verification",
            call_id=c.verification_receipt.call_id,
            data_ref=ref,
            status=value.verdict,
            detail={"input_fingerprint": c.verification_receipt.input_fingerprint},
        )
        # recommendation 是下一轮模型的观察，不再强制跳回某个已固定子任务。
        return {**accepted("verify", verdict=value.verdict), "verification": value}

    async def finish(state):
        if c.content_feedback:
            return await reject("finish", "unresolved_content_failure")
        if c.answer is None:
            return await reject("finish", "answer_required")
        freshness = c._verification_status()
        if c.cfg.require_verification and freshness == "stale":
            return await reject(
                "finish",
                "verification_stale",
                reason="Answer or evidence changed. Verify current inputs or revise the answer.",
            )
        # 过期的历史负面意见也不能冒充对当前状态的判定；可选政策允许未核验完成。
        verification = c.verification_receipt.result if freshness == "current" else None
        if c.cfg.require_verification and (
            verification is None or verification.verdict != "supported"
        ):
            return await reject(
                "finish",
                "verification_required",
                reason="A current supported verification is required; verify or revise the answer.",
            )
        if verification is not None and verification.verdict != "supported":
            raise RunStopped("insufficient_evidence", "agent_verification_failed")
        return {"terminal_reason": "agent_finished", "next_node": END}

    async def stop(state):
        await c._event("agent.stopped", detail={"reason": state["decision"].reason})
        raise RunStopped("insufficient_evidence", "agent_stopped")

    graph = StateGraph(AgentState)
    nodes = {
        "initialize": initialize,
        "decide": decide,
        "search": search,
        "tools": tools,
        "read": read,
        "plan": plan,
        "compose": compose,
        "verify": verify,
        "finish": finish,
        "stop": stop,
    }
    for name, method in nodes.items():
        graph.add_node(name, guard(method))
    graph.add_edge(START, "initialize")
    graph.add_edge("initialize", "decide")
    graph.add_conditional_edges(
        "decide",
        lambda state: state["next_node"],
        ["search", "tools", "read", "plan", "compose", "verify", "finish", "stop"],
    )
    for name in ("search", "tools", "read", "plan", "compose", "verify"):
        graph.add_edge(name, "decide")
    graph.add_conditional_edges("finish", lambda state: state["next_node"], ["decide", END])
    return graph.compile()
