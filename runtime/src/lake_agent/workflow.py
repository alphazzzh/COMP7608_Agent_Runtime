"""基于运行时现有安全机制，为单次运行组织 LangGraph 工作流。

图只负责控制流：绑定的控制器仍负责授权每次适配器调用、核算预算、分配语义 ID、
提交证据并写入审计日志。状态仅包含临时路由和规划值，不创建检查点，也不在调用间共享。
持久化恢复仍通过控制器唯一的已知响应日志重放历史。

每个命名节点对应原工作流的实际阶段。读取观察结果后直接返回规划，执行后返回检索，
反馈则重新访问已接受的子任务，不另建子任务计划。
图诊断信息不会进入公开追踪或其稳定的标识符序列。
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, TypedDict

from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tracers.context import run_collector_var, tracing_v2_callback_var
from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from . import models as m
from .context import build_planner_context
from .errors import RunStopped as _Stop
from .validation import canonical_json, validate_dag, validate_tool_specs

if TYPE_CHECKING:
    from .runtime import _Controller


class _NodeCancelled(Exception):
    """通过 LangGraph 任务调度器传递节点主动引发的取消。

    追踪或适配器可能抛出 CancelledError，却没有取消图任务；Web 暂停钩子在保存证据后
    就会这样做。LangGraph 将被取消的子任务视为已结束，可能返回不完整状态而不传播取消。
    这里用私有普通异常强制退出图；所属调用待所有图子任务结束后才还原 CancelledError。
    """


class WorkflowState(TypedDict, total=False):
    """仅属于一次绑定控制器调用的临时状态。

    控制器持有的证据、用量、已接受计划和审计历史不在此重复保存。节点串行替换路由字段，
    无需归约器或并行写入。读取的页面只在当前子任务的连续读取阶段保留，符合规划器的
    有界上下文约定。
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
    """根据有限的控制预算约束转移次数，不以提供方请求数作为上限。

    正常循环会消耗一个子任务、反馈轮次、工具步骤或观察读取配额。每单位预留八次转移，
    保守覆盖路由、完成、答案合成和验证；常数项覆盖初始化和最后一次超预算尝试。
    本地 ModuleResult 响应会释放模型调用预留，因此 max_model_calls 不能约束图转移次数。
    """
    return 32 + 8 * (
        config.max_subtasks
        + config.max_feedback_rounds
        + config.max_plan_steps
        + config.max_observation_reads
    )


@contextmanager
def _local_graph_context() -> Iterator[None]:
    """隔离当前图与外层 LangSmith/Runnable 追踪环境。

    在支持的 Core 版本中，回调配置先检查 tracing_v2 回调 ContextVar，再检查 LangSmith
    的启用上下文或环境；仅清除后者仍会附加外层 LangChainTracer。即使禁用追踪，Core
    也会通过独立的 run_collector ContextVar 注入 collect_runs()，因此也需清除该收集器，
    防止私有图输入输出进入调用方的收集结果。清除继承的 Runnable 配置，防止外层回调和
    检查点选项进入本次调用。令牌用于精确恢复调用方原值，不修改进程环境或并发任务上下文。
    """
    tracer_token = tracing_v2_callback_var.set(None)
    collector_token = run_collector_var.set(None)
    config_token = var_child_runnable_config.set(None)
    try:
        with tracing_context(enabled=False, parent=False):
            yield
    finally:
        var_child_runnable_config.reset(config_token)
        run_collector_var.reset(collector_token)
        tracing_v2_callback_var.reset(tracer_token)


def build_workflow(controller: _Controller):
    """编译一个新建且绑定控制器的图，不启用重试、缓存或检查点。"""
    c = controller

    async def initialize(state: WorkflowState):
        """验证工具契约并初始化本次图调用的临时路由状态。"""
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
        """注册来源目录，并按运行模式选择固定检索或任务分解。"""
        await c._catalog()
        return {"next_node": "fixed_search" if c.req.mode == "fixed_retrieval" else "decompose"}

    async def decompose(state: WorkflowState):
        """请求并验证子任务 DAG，提交已接受计划的审计记录。"""
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
        """按已接受顺序推进子任务，并阻塞依赖失败任务的后继。"""
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
        """围绕当前子任务和可选验证焦点检索候选来源。"""
        candidates = await c._search(state["active_subtask"], state["feedback_focus"])
        return {"candidates": candidates}

    async def select_tools(state: WorkflowState):
        """构造有界上下文，并将规划决策路由到读取、完成或执行节点。"""
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
        """读取已知观察结果，并在上下文预算内保留连续读取的完整页面。"""
        page = await c._read_observation(state["decision"])
        pages = state["read_pages"]
        result = dict(page, previous_pages=[], omitted_pages=len(pages))
        # 在规划器上下文字节上限的一半内保留最近的完整页面。
        # 这样精确保留原有连续读取历史的语义。
        for previous in reversed(pages):
            result["previous_pages"].insert(0, previous)
            result["omitted_pages"] -= 1
            if len(canonical_json(result)) > c.cfg.max_planner_context_bytes // 2:
                result["previous_pages"].pop(0)
                result["omitted_pages"] += 1
                break
        # 状态私有且图节点串行提交；原地追加，避免每次有界分页读取
        # 都复制完整页面历史。
        pages.append(page)
        return {"read_result": result, "read_pages": pages}

    async def execute_plan(state: WorkflowState):
        """通过控制器验证并执行计划，随后清空连续读取上下文。"""
        await c._execute_plan(state["decision"], state["active_subtask"], state["candidates"])
        return {"read_result": None, "read_pages": []}

    async def finish_subtask(state: WorkflowState):
        """提交子任务终态，并更新普通推进或反馈重访的完成状态。"""
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
            # 重新访问任务会替换其完成状态。按已接受 DAG 的顺序重建，
            # 保留原有的不重复行为。
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
            # 跳过的前置任务与失败任务一样，会阻塞后继任务。
            failed = failed | {subtask.subtask_id}
        return {
            "failed_subtask_ids": failed,
            "subtask_cursor": state["subtask_cursor"] + 1,
            "next_node": "next_subtask",
        }

    async def compose(state: WorkflowState):
        """合成答案，并根据运行模式结束或进入验证。"""
        await c._compose()
        if c.req.mode == "fixed_retrieval":
            return {"terminal_reason": "fixed_retrieval_completed", "next_node": END}
        if c.req.mode == "no_verifier":
            return {"terminal_reason": "planner_finished", "next_node": END}
        return {"next_node": "verify"}

    async def verify(state: WorkflowState):
        """验证答案与证据引用，并确定结束或反馈路径。"""
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
        """消耗一轮反馈配额，重新合成或重访已接受的目标子任务。"""
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
        """为固定检索模式获取候选来源，并检查提取工具是否可用。"""
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
        """为固定候选构造提取步骤，通过控制器执行并要求产生证据。"""
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

    # 显式边便于检查实际执行阶段。路由回调只读取本次运行状态，
    # 绝不授权或派发工作。
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
        """将节点取消转换为可强制图调度器退出的私有异常。"""

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
    """运行私有图，并在控制器开始清理前等待图内所有任务结束。

    LangGraph 的 AsyncBackgroundExecutor.__aexit__ 会取消节点任务，再等待
    asyncio.wait(tasks)。若退出过程再次被取消，等待会中止，存活节点可能继续写入
    已关闭的追踪或使用已释放的恢复租约。这里持有一个任务，屏蔽调用方等待时的取消，
    只向图转发一次取消，并在调用方后续取消期间持续屏蔽取消、等待图任务结束。
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
                # 后续取消仅属于外层调用方，不再向图退出和节点收尾过程
                # 重复转发取消。
                continue
            except Exception:
                break
        # 获取竞态中发生的失败，但保留调用方取消。
        if not task.cancelled():
            task.exception()
        raise
