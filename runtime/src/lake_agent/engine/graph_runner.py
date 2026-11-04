"""基于运行时现有安全机制，为单次运行组织 LangGraph 工作流。

图只负责控制流：绑定的控制器仍负责授权每次适配器调用、核算预算、分配语义 ID、
提交证据并写入审计日志。状态仅包含临时路由和规划值，不创建检查点，也不在调用间共享。
持久化恢复仍通过控制器唯一的已知响应日志重放历史。

本模块只负责模型驱动图的编译和共享执行机制，不再包含固定流程或协议回退。
具体动作节点在 agent_workflow.py，所有动作仍受同一个控制器约束。
图诊断信息不会进入公开追踪或其稳定的标识符序列。
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tracers.context import run_collector_var, tracing_v2_callback_var
from langsmith import tracing_context

from .. import models as m
from .agent_protocol import validate_agent_planner
from .agent_workflow import build_agent_workflow

if TYPE_CHECKING:
    from ..runtime import _Controller


class _NodeCancelled(Exception):
    """通过 LangGraph 任务调度器传递节点主动引发的取消。

    追踪或适配器可能抛出 CancelledError，却没有取消图任务；Web 暂停钩子在保存证据后
    就会这样做。LangGraph 将被取消的子任务视为已结束，可能返回不完整状态而不传播取消。
    这里用私有普通异常强制退出图；所属调用待所有图子任务结束后才还原 CancelledError。
    """


def graph_step_limit(config: m.RunConfig) -> int:
    """根据有限的控制预算约束转移次数，不以提供方请求数作为上限。

    每轮决策至少消耗一轮 max_agent_turns。每轮最多经过决策与动作节点，
    预留额外转移覆盖初始化、完成和最后一次超预算尝试。模型调用数不是图步数：
    本地 ModuleResult 可上报零请求，但仍必须受独立的决策轮数限制。
    """
    return 8 + 3 * config.max_agent_turns


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


def cancellation_bridge(node):
    """将节点取消转换为可强制图调度器退出的私有异常。"""

    async def guarded(state):
        try:
            return await node(state)
        except asyncio.CancelledError as exc:
            raise _NodeCancelled() from exc

    return guarded


def build_workflow(controller: _Controller):
    """只编译模型驱动图；旧规划器会在任何外部调用之前被拒绝。"""
    validate_agent_planner(controller.planner)
    with _local_graph_context():
        return build_agent_workflow(controller, cancellation_bridge)


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
