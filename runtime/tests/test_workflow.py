"""真实 LangGraph 路由与公开运行时行为的离线迁移验收关卡。

每个结构测试均在内部解析新的工作流模块，避免其初始失败阻断现有公开 run 回归测试的收集。
适配器仍使用本地测试替身，不涉及模型提供方或网络访问。
"""

from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import suppress

import pytest

from lake_agent import models as m
from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.runtime import _Controller, run


class MemoryTrace:
    """记录真实提交，并暴露接收器关闭之后发生的任何写入。"""

    def __init__(self):
        self.events = []
        self.artifacts = {}
        self.closed = False
        self.late_writes = []
        self.active_at_close = None
        self.data = None

    async def create_run(self, run_id):
        self.run_id = run_id

    async def put_artifact(self, ref, value):
        if self.closed:
            self.late_writes.append((ref, value))
        self.artifacts[ref] = value

    async def append(self, event):
        if self.closed:
            self.late_writes.append(event)
        self.events.append(event)

    async def close(self):
        self.active_at_close = set(self.data.active) if self.data is not None else set()
        self.closed = True
        return "memory://workflow"


class RecordingContent(DemoContent):
    """检查最终消费者的输入，不依赖有界的规划器预览。"""

    def __init__(self):
        super().__init__()
        self.searches = []
        self.compositions = []
        self.verifications = []

    async def search(self, request):
        self.searches.append(request)
        return await super().search(request)

    async def compose(self, request):
        self.compositions.append(request)
        return await super().compose(request)

    async def verify(self, request):
        self.verifications.append(request)
        return await super().verify(request)


def local_reply(request, data):
    """本地模块释放其临时预留的模型请求额度。"""
    return m.ModuleResult(call_id=request.call_id, data=data, usage=m.Usage.local())


def request(*, mode="full_agent", run_id=None, question="Annual revenue?", **limits):
    """使用显式有限额度，防止图执行限制掩盖失控的工作量。"""
    return m.RunRequest(
        task_id="workflow-regression",
        run_id=run_id,
        question=question,
        mode=mode,
        config=m.RunConfig(max_model_calls=1, **limits),
    )


async def wait_for_gate(task, gate):
    """暴露运行时早期故障，并始终获取临时等待任务的最终结果。"""
    waiter = asyncio.create_task(gate.wait())
    try:
        done, _ = await asyncio.wait({task, waiter}, timeout=3, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
            pytest.fail("run ended before the adapter gate")
        assert waiter in done, "run did not reach the adapter gate"
    finally:
        waiter.cancel()
        with suppress(asyncio.CancelledError):
            await waiter


async def test_workflow_compiles_real_nodes_for_agent_and_fixed_routes():
    """仅用一个节点包装旧的单体工作流，不构成真正的迁移。"""
    workflow = importlib.import_module("lake_agent.workflow")
    controller = _Controller(
        request(),
        DemoData(),
        DemoContent(),
        DemoPlanner(),
        None,
    )
    graph = workflow.build_workflow(controller)
    compiled = importlib.import_module("langgraph.graph.state").CompiledStateGraph
    assert isinstance(graph, compiled)
    assert {
        "catalog",
        "decompose",
        "next_subtask",
        "search",
        "select_tools",
        "read_observation",
        "execute_plan",
        "finish_subtask",
        "compose",
        "verify",
        "feedback",
        "fixed_search",
        "fixed_execute",
    } <= graph.get_graph().nodes.keys()
    assert callable(workflow.execute_workflow)
    assert callable(workflow.graph_step_limit)


@pytest.mark.parametrize("mode", ["fixed_retrieval", "no_verifier", "full_agent"])
async def test_all_modes_keep_evidence_and_terminal_semantics(mode):
    """固定模式绕过规划；no-verifier 模式仅绕过验证。"""
    get_config = importlib.import_module("langgraph.config").get_config

    class NodeTrace(MemoryTrace):
        """在已提交的 I/O 意图边界读取真实节点上下文。"""

        def __init__(self):
            super().__init__()
            self.call_nodes = []

        async def append(self, event):
            if event.event_type.endswith(".call.started"):
                self.call_nodes.append(get_config()["metadata"]["langgraph_node"])
            await super().append(event)

    trace, content = NodeTrace(), RecordingContent()
    result = await run(
        request(mode=mode), data=DemoData(), content=content, planner=DemoPlanner(), trace=trace
    )
    assert (result.status, result.stop_reason) == {
        "fixed_retrieval": ("completed", "fixed_retrieval_completed"),
        "no_verifier": ("completed", "planner_finished"),
        "full_agent": ("completed", "verified_supported"),
    }[mode]
    assert result.answer.citations == [content.compositions[0].evidence[0].evidence_id]
    assert result.call_counts.tools == (2 if mode == "fixed_retrieval" else 1)
    assert result.call_counts.planner == (0 if mode == "fixed_retrieval" else 3)
    assert len(content.verifications) == (1 if mode == "full_agent" else 0)
    # 这些是观测到的图执行上下文，不是源代码名称或伪造的回调计数器。
    # 仅用单个节点包装旧循环的实现无法通过此关卡。
    assert trace.call_nodes == (
        ["catalog", "catalog", "fixed_search", "fixed_execute", "fixed_execute", "compose"]
        if mode == "fixed_retrieval"
        else [
            "catalog",
            "catalog",
            "decompose",
            "search",
            "select_tools",
            "execute_plan",
            "search",
            "select_tools",
            "compose",
        ]
        + (["verify"] if mode == "full_agent" else [])
    )
    assert result.call_counts.model_calls == result.usage.provider_requests == 0
    assert trace.events[-1].event_type == "run.completed"
    assert trace.closed and not trace.late_writes


async def test_ambient_tracing_is_disabled_inside_adapters_and_restored(monkeypatch):
    """环境启用的 LangSmith 或显式 LangChain 追踪均不能收到运行输入。"""
    langsmith = importlib.import_module("langsmith")
    callbacks = importlib.import_module("langchain_core.callbacks")
    tracer_context = importlib.import_module("langchain_core.tracers.context")
    tracer_module = importlib.import_module("langchain_core.tracers.langchain")
    attached = []
    observed = []

    class LocalTracer(callbacks.BaseCallbackHandler):
        """即使自动启用机制失效，也确保追踪回归测试完全在本地执行。"""

        def __init__(self, **kwargs):
            super().__init__()

        def copy_with_metadata_defaults(self, **kwargs):
            return self

        def on_chain_start(self, serialized, inputs, **kwargs):
            attached.append(inputs)

    class InspectTracingData(DemoData):
        async def catalog(self, req):
            observed.append(
                (
                    langsmith.get_tracing_context()["enabled"],
                    tracer_context.tracing_v2_callback_var.get(),
                )
            )
            return await super().catalog(req)

    # 仅替换遥测接收器的构造函数。真实回调配置、
    # 图执行以及适配器上下文传播仍在测试范围内。
    monkeypatch.setattr(tracer_module, "LangChainTracer", LocalTracer)
    ambient = LocalTracer()
    token = tracer_context.tracing_v2_callback_var.set(ambient)
    try:
        with langsmith.tracing_context(enabled=True, tags=["outer-offline-context"]):
            result = await run(
                request(),
                data=InspectTracingData(),
                content=DemoContent(),
                planner=DemoPlanner(),
                trace=MemoryTrace(),
            )
            assert result.status == "completed"
            assert langsmith.get_tracing_context()["enabled"] is True
            assert tracer_context.tracing_v2_callback_var.get() is ambient
            assert langsmith.get_tracing_context()["tags"] == ["outer-offline-context"]
    finally:
        tracer_context.tracing_v2_callback_var.reset(token)
    assert observed == [(False, None)]
    assert attached == []


async def test_outer_collect_runs_cannot_collect_graph_and_remains_usable():
    """内置本地收集器只保留调用方显式创建的外层运行。"""
    langsmith = importlib.import_module("langsmith")
    tracer_context = importlib.import_module("langchain_core.tracers.context")
    runnable = importlib.import_module("langchain_core.runnables").RunnableLambda

    async def echo(value):
        return value

    # collect_runs 本身在本地执行。显式禁用 LangSmith，
    # 避免环境设置在测试期间创建遥测客户端。
    with langsmith.tracing_context(enabled=False, parent=False):
        with tracer_context.collect_runs() as collector:
            before = runnable(echo, name="outer-before")
            assert await before.ainvoke({"owner": "before"}) == {"owner": "before"}
            assert [item.name for item in collector.traced_runs] == ["outer-before"]
            result = await run(
                request(),
                data=DemoData(),
                content=DemoContent(),
                planner=DemoPlanner(),
                trace=MemoryTrace(),
            )
            assert result.status == "completed"
            assert tracer_context.run_collector_var.get() is collector
            after = runnable(echo, name="outer-after")
            assert await after.ainvoke({"owner": "after"}) == {"owner": "after"}
            # 断言所收集的载荷，既能证明输入隔离，也能证明真实收集功能已恢复，
            # 而不仅是验证 ContextVar 身份相同。
            assert [(item.name, item.inputs) for item in collector.traced_runs] == [
                ("outer-before", {"owner": "before"}),
                ("outer-after", {"owner": "after"}),
            ]


@pytest.mark.parametrize("mode", ["no_verifier", "full_agent"])
async def test_local_action_loop_exceeds_default_graph_recursion_limit(mode):
    """九轮搜索、规划与执行需要超过 25 个真实图步骤。"""
    rounds = 9

    class DistinctData(DemoData):
        async def invoke(self, req):
            reply = await super().invoke(req)
            item = reply.data["evidence"][0]
            # 不同参数、定位符与载荷代表真实且相互独立的动作，
            # 并非重复请求同一个会被去重的读取。
            item["payload"] = req.args["query"]
            item["locator"] = {"query": req.args["query"]}
            return reply

    class MultiRoundPlanner(DemoPlanner):
        async def select_tools(self, req):
            completed = len(req.evidence_summary)
            if completed == rounds:
                return local_reply(req, m.FinishDecision(reason="evidence_complete"))
            return local_reply(
                req,
                m.ToolPlan(
                    steps=[
                        m.ToolStep(
                            step_id=f"round-{completed}",
                            subtask_id=req.subtask.subtask_id,
                            tool_name="extract",
                            source_ids=["demo-annual"],
                            args={
                                "source_id": "demo-annual",
                                "query": f"Independent round {completed}",
                                "max_chars": 4000,
                            },
                            purpose="Read a distinct authorized synthetic fact",
                        )
                    ]
                ),
            )

    trace, content = MemoryTrace(), RecordingContent()
    result = await run(
        request(mode=mode, max_plan_steps=rounds, max_data_calls=rounds + 1),
        data=DistinctData(),
        content=content,
        planner=MultiRoundPlanner(),
        trace=trace,
    )
    assert result.status == "completed", result
    assert [item.payload for item in content.compositions[0].evidence] == [
        f"Independent round {index}" for index in range(rounds)
    ]
    assert result.call_counts.steps == result.call_counts.tools == rounds
    assert result.call_counts.planner == rounds + 2
    assert result.call_counts.catalog == 1
    assert result.call_counts.model_calls == result.usage.provider_requests == 0
    assert len(content.searches) == rounds + 1
    assert trace.closed and not trace.late_writes


async def test_local_observation_loop_exceeds_default_graph_recursion_limit():
    """读取节点直接返回规划阶段，不再进行搜索或数据调用。"""

    class PagingPlanner(DemoPlanner):
        def __init__(self):
            self.pages = []

        async def select_tools(self, req):
            if not req.tool_observations:
                return await super().select_tools(req)
            offset = 0
            if req.observation_read is not None:
                page = req.observation_read
                self.pages.append(page)
                if not page["truncated"]:
                    return local_reply(req, m.FinishDecision(reason="evidence_complete"))
                offset = page["next_offset"]
            return local_reply(
                req,
                m.ReadObservationDecision(
                    observation_id=req.tool_observations[-1]["observation_id"],
                    pointer="/data/evidence/0/payload",
                    offset=offset,
                    max_chars=4,
                ),
            )

    planner, content, trace = PagingPlanner(), RecordingContent(), MemoryTrace()
    result = await run(
        request(max_plan_steps=1, max_data_calls=2, max_observation_reads=32),
        data=DemoData(),
        content=content,
        planner=planner,
        trace=trace,
    )
    assert result.status == "completed", result
    assert len(planner.pages) > 12  # 至少 26 次读取与规划的图状态转换。
    assert json.loads("".join(page["text"] for page in planner.pages)) == (
        "Synthetic fixture: 2024 annual revenue was 120 million HKD."
    )
    assert result.call_counts.observation_reads == len(planner.pages)
    assert result.call_counts.tools == result.call_counts.steps == 1
    assert result.call_counts.catalog == 1
    assert result.call_counts.model_calls == 0
    assert len(content.searches) == 2
    assert trace.closed and not trace.late_writes


async def test_concurrent_runs_keep_routing_evidence_and_completion_isolated():
    """共享可重入端口不能导致图调用状态共享。"""
    started = {name: asyncio.Event() for name in ("left", "right")}
    releases = {name: asyncio.Event() for name in ("left", "right")}

    class GatedData(DemoData):
        async def invoke(self, req):
            started[req.run_id].set()
            await releases[req.run_id].wait()
            reply = await super().invoke(req)
            reply.data["evidence"][0]["payload"] = f"Evidence owned by {req.run_id}"
            return reply

    data, content, planner = GatedData(), RecordingContent(), DemoPlanner()
    traces = {name: MemoryTrace() for name in started}
    tasks = {
        name: asyncio.create_task(
            run(
                request(run_id=name, question=f"Question for {name}"),
                data=data,
                content=content,
                planner=planner,
                trace=traces[name],
            )
        )
        for name in started
    }
    try:
        for name in started:
            await wait_for_gate(tasks[name], started[name])
        releases["left"].set()
        left = await asyncio.wait_for(asyncio.shield(tasks["left"]), timeout=3)
        assert left.status == "completed"
        assert not tasks["right"].done()
        assert not traces["right"].closed
        releases["right"].set()
        right = await asyncio.wait_for(asyncio.shield(tasks["right"]), timeout=3)
        assert right.status == "completed"
        by_run = {req.run_id: req for req in content.compositions}
        assert set(by_run) == {"left", "right"}
        for name, result in (("left", left), ("right", right)):
            evidence = by_run[name].evidence
            assert [item.payload for item in evidence] == [f"Evidence owned by {name}"]
            assert result.answer.citations == [evidence[0].evidence_id]
            assert result.call_counts.tools == result.call_counts.steps == 1
            assert all(event.run_id == name for event in traces[name].events)
            assert traces[name].closed and not traces[name].late_writes
        assert left.answer.citations != right.answer.citations
    finally:
        for gate in releases.values():
            gate.set()
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)


@pytest.mark.parametrize("phase", ["catalog", "tool"])
async def test_repeated_caller_cancellation_drains_adapter_before_trace_close(phase):
    """调用方第二次取消不能中断图所拥有的清理任务。"""
    entered, cleanup_entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class CleanupData(DemoData):
        def __init__(self):
            super().__init__()
            self.active = set()
            self.children = set()
            self.cleanup_cancellations = 0

        async def held(self, req):
            self.active.add(req.call_id)
            self.children.add(asyncio.current_task())
            entered.set()
            try:
                await asyncio.Future()
            finally:
                cleanup_entered.set()
                # 即使有缺陷的所有者反复将取消传入此清理等待，
                # 该清理也保持有限且可协作结束。
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        self.cleanup_cancellations += 1
                self.active.remove(req.call_id)

        async def catalog(self, req):
            if phase == "catalog":
                await self.held(req)
            return await super().catalog(req)

        async def invoke(self, req):
            if phase == "tool":
                await self.held(req)
            return await super().invoke(req)

    data, trace = CleanupData(), MemoryTrace()
    trace.data = data
    task = asyncio.create_task(
        run(request(), data=data, content=DemoContent(), planner=DemoPlanner(), trace=trace)
    )
    try:
        await wait_for_gate(task, entered)
        task.cancel()
        await wait_for_gate(task, cleanup_entered)
        for _ in range(2):
            task.cancel()
            # 就绪队列中的事件使取消请求得以送达；
            # 清理过程的交错顺序不依赖经过时间的假设。
            checkpoint = asyncio.Event()
            asyncio.get_running_loop().call_soon(checkpoint.set)
            await checkpoint.wait()
        assert data.cleanup_cancellations == 0
        assert data.active and not task.done()
        assert not trace.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=3)
        assert not data.active
        assert all(child.done() for child in data.children)
        assert trace.closed and trace.active_at_close == set()
        assert trace.events[-1].event_type == "run.cancelled"
        assert len([event for event in trace.events if event.event_type == "run.cancelled"]) == 1
        assert not trace.late_writes
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
