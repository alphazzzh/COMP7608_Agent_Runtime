"""通过公开 run 入口验收有界且可审计的工具计划批次。

测试替身仅使用相互独立的合成数据源与本地端口。显式事件关卡验证并行重叠、
逆序完成、等待任务结束和取消；超时仅用于限制缺陷实现的等待，不衡量性能。
轨迹事件与产物内容单独验证串行提交边界，将其与原始适配器工作任务区分。
所有测试均不调用控制器私有方法。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, suppress

import pytest

from lake_agent import models as m
from lake_agent.demo import DemoContent, DemoPlanner
from lake_agent.runtime import run


class MemoryTrace:
    """记录已提交的事件和产物，并在关闭轨迹时检查适配器。"""

    def __init__(self):
        self.events = []
        self.artifacts = {}
        self.closed = False
        self.active_at_close = None
        self.data = None

    async def create_run(self, run_id):
        self.run_id = run_id

    async def put_artifact(self, ref, value):
        self.artifacts[ref] = value

    async def append(self, event):
        self.events.append(event)

    async def close(self):
        self.active_at_close = set(self.data.active) if self.data is not None else set()
        self.closed = True
        return "memory://parallel-runtime"


class SourceData:
    """只读数据源测试替身，显式控制原始工作任务的生命周期。"""

    def __init__(self, *, held=(), errors=None, before_invoke=None):
        self.sources = [
            m.SourceRecord(
                source_id=f"source-{index}",
                name=f"local-{index}.txt",
                format="text",
                summary=f"Independent synthetic source {index}",
            )
            for index in range(1, 6)
        ]
        self.held = set(held)
        self.errors = errors or {}
        self.before_invoke: Callable[[m.ToolRequest], Awaitable[None]] | None = before_invoke
        self.started = {}
        self.releases = {}
        self.finished = {}
        self.invocations = []
        self.active = set()
        self.children = set()
        self.cancelled = set()
        self.returned = []
        self.peak = 0

    def event(self, collection, step_id):
        """复用每个步骤的稳定事件关卡，包括跨重试尝试的复用。"""
        return collection.setdefault(step_id, asyncio.Event())

    def tool_specs(self):
        specs = []
        for name, safe, keys, uses_sources in (
            ("read", True, [], True),
            ("locked", True, ["shared-database"], True),
            ("locked_peer", True, ["shared-database"], True),
            ("unsafe", False, [], True),
            ("global", True, [], False),
        ):
            properties = {"label": {"type": "string"}}
            if uses_sources:
                properties["source_id"] = {"type": "string"}
            specs.append(
                m.ToolSpec(
                    name=name,
                    description=f"Synthetic local {name} fixture",
                    input_schema={
                        "type": "object",
                        "properties": properties,
                        "required": list(properties),
                        "additionalProperties": False,
                    },
                    idempotent=True,
                    uses_sources=uses_sources,
                    source_id_paths=["/source_id"] if uses_sources else [],
                    parallel_safe=safe,
                    exclusive_resource_keys=keys,
                )
            )
        return specs

    async def catalog(self, request):
        return m.CatalogResult(
            call_id=request.call_id,
            catalog_snapshot_id="parallel-fixture-v1",
            sources=self.sources,
        )

    async def invoke(self, request):
        """只有此适配器函数体并发执行；控制器状态在其外部管理。"""
        self.invocations.append(request)
        self.active.add(request.call_id)
        self.children.add(asyncio.current_task())
        self.peak = max(self.peak, len(self.active))
        self.event(self.started, request.step_id).set()
        try:
            if self.before_invoke is not None:
                await self.before_invoke(request)
            if request.step_id in self.held:
                await self.event(self.releases, request.step_id).wait()
            error = self.errors.get((request.step_id, request.attempt))
            if error is not None:
                result = m.ToolResult(call_id=request.call_id, status="error", error=error)
            else:
                source_id = request.args.get("source_id")
                evidence = (
                    [
                        {
                            "source_id": source_id,
                            "content_type": "text",
                            "payload": request.args["label"],
                            "locator": {"line": 1},
                        }
                    ]
                    if source_id is not None
                    else []
                )
                result = m.ToolResult(call_id=request.call_id, data={"evidence": evidence})
            self.returned.append(request.step_id)
            return result
        except asyncio.CancelledError:
            self.cancelled.add(request.call_id)
            raise
        finally:
            self.active.remove(request.call_id)
            self.event(self.finished, request.step_id).set()


class RecordingContent(DemoContent):
    """独立于规划上下文预览，保留完整的 compose 输入。"""

    def __init__(self):
        super().__init__()
        self.compositions = []

    async def compose(self, request):
        self.compositions.append(request)
        return await super().compose(request)


class PlanOnce(DemoPlanner):
    """先提供一个可接受的计划，再使用控制器观测完成任务。"""

    def __init__(self, steps):
        self.steps = steps
        self.requests = []

    async def select_tools(self, request):
        self.requests.append(request)
        decision = (
            m.ToolPlan(steps=self.steps)
            if len(self.requests) == 1
            else m.FinishDecision(
                reason="evidence_complete" if request.evidence_summary else "insufficient_evidence"
            )
        )
        return m.ModuleResult(call_id=request.call_id, data=decision, usage=m.Usage.local())


def step(step_id, source="source-1", *, tool="read", depends_on=()):
    """为动作分配稳定身份，以及彼此不同且已授权的参数签名。"""
    args = {"label": step_id}
    if source is not None:
        args["source_id"] = source
    return m.ToolStep(
        step_id=step_id,
        subtask_id="annual-revenue",
        tool_name=tool,
        source_ids=[source] if source is not None else [],
        args=args,
        depends_on=list(depends_on),
        purpose="Read a synthetic source for concurrency acceptance",
    )


async def execute(steps, *, data=None, trace=None, content=None, planner=None, **config):
    """使用确定性的本地端口驱动受支持的公开入口。"""
    data = data if data is not None else SourceData()
    trace = trace if trace is not None else MemoryTrace()
    trace.data = data
    return await run(
        m.RunRequest(
            task_id="parallel-acceptance",
            question="What do the local sources say?",
            mode="no_verifier",
            config=m.RunConfig(**config),
        ),
        data=data,
        content=content if content is not None else RecordingContent(),
        planner=planner if planner is not None else PlanOnce(steps),
        trace=trace,
    )


@asynccontextmanager
async def running(steps, **kwargs):
    """确保关卡断言失败时，不会遗留仍在运行的主任务或子任务。"""
    task = asyncio.create_task(execute(steps, **kwargs))
    try:
        yield task
    finally:
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def wait_event(event):
    """为回归测试设置等待上限，不依赖适配器的完成速度。"""
    async with asyncio.timeout(2):
        await event.wait()


async def finish(task):
    async with asyncio.timeout(2):
        return await task


def event_index(trace, event_type, step_id):
    return next(
        index
        for index, event in enumerate(trace.events)
        if event.event_type == event_type and event.step_id == step_id
    )


def terminal_states(trace):
    return {
        event.step_id: event.status
        for event in trace.events
        if event.event_type == "step.state" and event.status != "running"
    }


def test_parallel_configuration_and_safe_metadata_contract():
    """并发始终受到显式限制，现有工具默认串行执行。"""
    assert m.RunConfig().max_parallel_tool_calls == 4
    spec = m.ToolSpec(
        name="read",
        description="Local source reader",
        input_schema={"type": "object"},
        idempotent=True,
        uses_sources=False,
    )
    assert spec.parallel_safe is False
    assert spec.exclusive_resource_keys == []
    for limit in (1, 32):
        assert m.RunConfig(max_parallel_tool_calls=limit).max_parallel_tool_calls == limit
    for limit in (0, 33, True, "2", 2.0):
        with pytest.raises(ValueError):
            m.RunConfig(max_parallel_tool_calls=limit)


async def test_safe_disjoint_sources_overlap_and_respect_configured_cap():
    """两个受阻的工作任务必须同时存在，之后才允许启动第三个任务所在的批次。"""
    data = SourceData(held={"a", "b", "c"})
    steps = [step("a"), step("b", "source-2"), step("c", "source-3")]
    async with running(steps, data=data, max_parallel_tool_calls=2) as task:
        await wait_event(data.event(data.started, "a"))
        await wait_event(data.event(data.started, "b"))
        assert len(data.active) == data.peak == 2
        assert not data.event(data.started, "c").is_set()
        # 整个批次必须全部结束：同批单个任务返回后，不能提前腾出名额。
        data.event(data.releases, "b").set()
        await wait_event(data.event(data.finished, "b"))
        assert not data.event(data.started, "c").is_set()
        data.event(data.releases, "a").set()
        await wait_event(data.event(data.started, "c"))
        data.event(data.releases, "c").set()
        result = await finish(task)
    assert result.status == "completed"
    assert data.peak == 2
    assert result.call_counts.steps == result.call_counts.tools == 3


async def test_reverse_worker_completion_commits_evidence_in_selected_plan_order():
    """适配器返回顺序不能改变轨迹、观测或 compose 输入的顺序。"""
    data, trace, content = SourceData(held={"first", "second"}), MemoryTrace(), RecordingContent()
    steps = [step("first"), step("second", "source-2")]
    planner = PlanOnce(steps)
    async with running(steps, data=data, trace=trace, content=content, planner=planner) as task:
        await wait_event(data.event(data.started, "first"))
        await wait_event(data.event(data.started, "second"))
        data.event(data.releases, "second").set()
        await wait_event(data.event(data.finished, "second"))
        data.event(data.releases, "first").set()
        result = await finish(task)
    assert result.status == "completed"
    assert data.returned == ["second", "first"]
    assert [item.payload for item in content.compositions[0].evidence] == ["first", "second"]
    assert [
        event.step_id for event in trace.events if event.event_type == "tool.call.completed"
    ] == ["first", "second"]
    assert [event.step_id for event in trace.events if event.event_type == "evidence.added"] == [
        "first",
        "second",
    ]
    assert [event.step_id for event in trace.events if event.event_type == "tool.observed"] == [
        "first",
        "second",
    ]
    # 工作上下文刻意按从新到旧的顺序提供已提交观测。
    assert [item["step_id"] for item in planner.requests[-1].tool_observations] == [
        "second",
        "first",
    ]


async def test_dependency_waits_for_parent_audit_evidence_and_entire_wave_drain():
    trace = MemoryTrace()

    async def check_child(request):
        if request.step_id == "child":
            # 派发之前，必须先提交产物与证据事件。
            for parent in ("parent", "sibling"):
                event = next(
                    item
                    for item in trace.events
                    if item.event_type == "evidence.added" and item.step_id == parent
                )
                assert trace.artifacts[event.data_ref]["payload"] == parent
                assert event_index(trace, "tool.call.completed", parent) < event_index(
                    trace, "tool.call.started", "child"
                )
                assert terminal_states(trace)[parent] == "succeeded"

    data = SourceData(held={"parent", "sibling"}, before_invoke=check_child)
    steps = [
        step("parent"),
        step("child", "source-2", depends_on=["parent"]),
        step("sibling", "source-3"),
    ]
    async with running(steps, data=data, trace=trace, max_parallel_tool_calls=2) as task:
        await wait_event(data.event(data.started, "parent"))
        await wait_event(data.event(data.started, "sibling"))
        data.event(data.releases, "parent").set()
        await wait_event(data.event(data.finished, "parent"))
        assert not data.event(data.started, "child").is_set()
        data.event(data.releases, "sibling").set()
        result = await finish(task)
    assert result.status == "completed"
    assert terminal_states(trace)["child"] == "succeeded"


@pytest.mark.parametrize(
    "steps",
    [
        [step("a"), step("b")],
        [step("a", tool="locked"), step("b", "source-2", tool="locked_peer")],
        [step("a"), step("barrier", "source-2", tool="unsafe"), step("b", "source-3")],
        [step("a"), step("barrier", None, tool="global"), step("b", "source-3")],
    ],
    ids=["same-source", "exclusive-key", "unsafe-barrier", "source-less-barrier"],
)
async def test_conflicting_and_global_barrier_actions_serialize(steps):
    trace, data = MemoryTrace(), SourceData()
    result = await execute(steps, data=data, trace=trace, max_parallel_tool_calls=4)
    assert result.status == "completed"
    assert data.peak == 1
    for earlier, later in zip(steps, steps[1:], strict=False):
        assert event_index(trace, "tool.call.completed", earlier.step_id) < event_index(
            trace, "tool.call.started", later.step_id
        )


async def test_parallel_limit_one_preserves_serial_dispatch():
    steps = [step("a"), step("b", "source-2"), step("c", "source-3")]
    trace = MemoryTrace()
    result = await execute(steps, trace=trace, max_parallel_tool_calls=1)
    assert result.status == "completed"
    for earlier, later in zip(steps, steps[1:], strict=False):
        assert event_index(trace, "tool.call.completed", earlier.step_id) < event_index(
            trace, "tool.call.started", later.step_id
        )


async def test_complete_plan_authorization_precedes_any_wave_dispatch():
    data = SourceData()
    result = await execute([step("valid"), step("foreign", "not-in-catalog")], data=data)
    assert (result.status, result.stop_reason) == ("failed", "invalid_plan")
    assert data.invocations == []
    assert result.call_counts.steps == result.call_counts.tools == 0


async def test_parallel_budget_reservations_never_overshoot_physical_data_cap():
    data = SourceData(held={"a", "b"})
    trace = MemoryTrace()
    steps = [step("a"), step("b", "source-2"), step("c", "source-3")]
    # 一次目录请求加两次工具请求恰好耗尽此上限。
    async with running(
        steps, data=data, trace=trace, max_parallel_tool_calls=3, max_data_calls=3
    ) as task:
        await wait_event(data.event(data.started, "a"))
        await wait_event(data.event(data.started, "b"))
        assert not data.event(data.started, "c").is_set()
        data.event(data.releases, "a").set()
        data.event(data.releases, "b").set()
        result = await finish(task)
    assert (result.status, result.stop_reason) == ("budget_exceeded", "data_call_budget")
    assert result.call_counts.catalog + result.call_counts.tools == 3
    assert [request.step_id for request in data.invocations] == ["a", "b"]
    assert terminal_states(trace)["a"] == terminal_states(trace)["b"] == "succeeded"


async def test_parallel_retry_reuses_logical_operation_and_counts_physical_attempts():
    data = SourceData(
        held={"a", "b"},
        errors={
            ("a", 1): m.ErrorInfo(
                code="unavailable", message="Transient local error", retryable=True
            )
        },
    )
    steps = [step("a"), step("b", "source-2")]
    async with running(steps, data=data, max_parallel_tool_calls=2, max_data_calls=4) as task:
        await wait_event(data.event(data.started, "a"))
        await wait_event(data.event(data.started, "b"))
        data.event(data.releases, "a").set()
        data.event(data.releases, "b").set()
        result = await finish(task)
    assert result.status == "completed"
    attempts = [request for request in data.invocations if request.step_id == "a"]
    assert len(attempts) == 2
    assert attempts[0].operation_id == attempts[1].operation_id
    assert attempts[0].call_id != attempts[1].call_id
    assert [request.attempt for request in attempts] == [1, 2]
    assert result.call_counts.steps == 2
    assert result.call_counts.tools == 3
    assert result.call_counts.retries == 1
    assert result.call_counts.catalog + result.call_counts.tools == 4


async def test_retry_budget_exhaustion_still_commits_successful_started_sibling():
    """因实际请求次数上限而被拒绝的重试，不能丢弃同批另一工作任务的回复。"""
    data = SourceData(
        held={"a", "b"},
        errors={("a", 1): m.ErrorInfo(code="unavailable", message="Retry wanted", retryable=True)},
    )
    trace = MemoryTrace()
    steps = [step("a"), step("b", "source-2"), step("fresh", "source-3")]
    async with running(
        steps, data=data, trace=trace, max_parallel_tool_calls=2, max_data_calls=3
    ) as task:
        await wait_event(data.event(data.started, "a"))
        await wait_event(data.event(data.started, "b"))
        data.event(data.releases, "a").set()
        await wait_event(data.event(data.finished, "a"))
        assert not task.done()
        data.event(data.releases, "b").set()
        result = await finish(task)
    assert (result.status, result.stop_reason) == ("budget_exceeded", "data_call_budget")
    assert result.call_counts.catalog + result.call_counts.tools == 3
    assert result.call_counts.steps == 2 and result.call_counts.retries == 0
    assert [request.step_id for request in data.invocations] == ["a", "b"]
    assert terminal_states(trace)["b"] == "succeeded"
    assert terminal_states(trace)["fresh"] == "skipped"
    evidence = [event for event in trace.events if event.event_type == "evidence.added"]
    assert [trace.artifacts[event.data_ref]["payload"] for event in evidence] == ["b"]


@pytest.mark.parametrize("recoverable", [True, False], ids=["recoverable", "fatal"])
async def test_failed_sibling_drains_started_success_and_blocks_unstarted_plan(recoverable):
    data = SourceData(
        held={"failed", "success"},
        errors={
            ("failed", 1): m.ErrorInfo(
                code="bad_query", message="Synthetic failed query", recoverable=recoverable
            )
        },
    )
    trace, content = MemoryTrace(), RecordingContent()
    steps = [
        step("failed"),
        step("success", "source-2"),
        step("child", "source-3", depends_on=["failed"]),
        step("grandchild", "source-4", depends_on=["child"]),
        step("unstarted", "source-5"),
    ]
    async with running(
        steps, data=data, trace=trace, content=content, max_parallel_tool_calls=2
    ) as task:
        await wait_event(data.event(data.started, "failed"))
        await wait_event(data.event(data.started, "success"))
        data.event(data.releases, "failed").set()
        await wait_event(data.event(data.finished, "failed"))
        assert not data.event(data.started, "unstarted").is_set()
        assert not task.done()
        data.event(data.releases, "success").set()
        result = await finish(task)
    states = terminal_states(trace)
    assert states["failed"] == "failed"
    assert states["success"] == "succeeded"
    assert states["child"] == states["grandchild"] == "blocked"
    assert states["unstarted"] == "skipped"
    assert [request.step_id for request in data.invocations] == ["failed", "success"]
    evidence_events = [event for event in trace.events if event.event_type == "evidence.added"]
    assert len(evidence_events) == 1
    assert trace.artifacts[evidence_events[0].data_ref]["payload"] == "success"
    assert result.call_counts.steps == result.call_counts.tools == 2
    if recoverable:
        assert result.status == "completed"
        assert [item.payload for item in content.compositions[0].evidence] == ["success"]
    else:
        assert (result.status, result.stop_reason) == ("failed", "tool_error")
    assert trace.closed and trace.active_at_close == set()


async def test_cancelling_parallel_wave_observes_every_child_before_trace_close():
    data, trace = SourceData(held={"a", "b"}), MemoryTrace()
    steps = [step("a"), step("b", "source-2"), step("unstarted", "source-3")]
    async with running(steps, data=data, trace=trace, max_parallel_tool_calls=2) as task:
        await wait_event(data.event(data.started, "a"))
        await wait_event(data.event(data.started, "b"))
        active_requests = list(data.invocations)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await finish(task)
    assert len(active_requests) == 2
    assert data.cancelled == {request.call_id for request in active_requests}
    assert data.active == set()
    assert all(child.done() for child in data.children)
    assert trace.closed and trace.active_at_close == set()
    cancelled = [event for event in trace.events if event.event_type == "run.cancelled"]
    assert len(cancelled) == 1
    detail = cancelled[0].detail
    assert {item["call_id"] for item in detail["active_calls"]} == {
        request.call_id for request in active_requests
    }
    assert {item["operation_id"] for item in detail["active_calls"]} == {
        request.operation_id for request in active_requests
    }
    assert all(item["phase"] == "tool" for item in detail["active_calls"])
    assert detail["pending_calls"] == []
    assert detail["call_counts"]["tools"] == detail["call_counts"]["steps"] == 2
    assert not data.event(data.started, "unstarted").is_set()


async def test_cancellation_during_second_wave_intent_preserves_all_pending_identities():
    """批次的原子准备过程应暴露已提交及提交状态不确定的调用意图预留。"""
    entered = asyncio.Event()

    class HeldSecondIntent(MemoryTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "tool.call.started" and event.step_id == "b":
                entered.set()
                await asyncio.Future()

    trace, data = HeldSecondIntent(), SourceData()
    steps = [step("a"), step("b", "source-2")]
    async with running(steps, data=data, trace=trace, max_parallel_tool_calls=2) as task:
        await wait_event(entered)
        assert data.invocations == []
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await finish(task)
    assert data.invocations == []
    assert trace.closed and trace.active_at_close == set()
    cancelled = next(event for event in trace.events if event.event_type == "run.cancelled")
    intents = [event for event in trace.events if event.event_type == "tool.call.started"]
    pending = cancelled.detail["pending_calls"]
    assert len(pending) == 2
    assert {item["call_id"] for item in pending} == {event.call_id for event in intents}
    assert {item["operation_id"] for item in pending} == {event.operation_id for event in intents}
    assert {item["step_id"] for item in pending} == {"a", "b"}
    by_step = {item["step_id"]: item for item in pending}
    assert by_step["a"]["intent_committed"] is True
    assert not by_step["b"].get("intent_committed", False)
    assert all(item["phase"] == "tool" for item in pending)
    assert cancelled.detail["active_calls"] == []
    assert cancelled.detail["pending_call"] is None
    assert cancelled.detail["call_counts"]["tools"] == 2
    assert cancelled.detail["call_counts"]["steps"] == 0


async def test_competing_retries_use_final_slot_in_stable_order_without_discarding_it():
    """原子准备过程不得为无法派发的重试预留额度。"""
    error = m.ErrorInfo(code="unavailable", message="Transient", retryable=True)
    data = SourceData(errors={("a", 1): error, ("b", 1): error})
    trace = MemoryTrace()
    result = await execute(
        [step("a"), step("b", "source-2")],
        data=data,
        trace=trace,
        max_parallel_tool_calls=2,
        max_data_calls=4,
    )
    assert (result.status, result.stop_reason) == ("budget_exceeded", "data_call_budget")
    assert [(item.step_id, item.attempt) for item in data.invocations] == [
        ("a", 1),
        ("b", 1),
        ("a", 2),
    ]
    assert result.call_counts.tools == 3
    assert result.call_counts.retries == 1
    assert terminal_states(trace) == {"a": "succeeded", "b": "failed"}
    assert [
        trace.artifacts[e.data_ref]["payload"]
        for e in trace.events
        if e.event_type == "evidence.added"
    ] == ["a"]
    assert trace.events[-1].detail["pending_calls"] == []


async def test_planner_request_mutation_cannot_grant_adapter_parallel_permission():
    """规划器拥有的请求记录不能改写可信的并发能力声明。"""

    class UnsafeData(SourceData):
        def tool_specs(self):
            specs = super().tool_specs()
            for spec in specs:
                spec.parallel_safe = False
            return specs

    class MutatingPlanner(PlanOnce):
        async def decompose(self, request):
            for spec in request.tool_specs:
                spec.parallel_safe = True
                spec.exclusive_resource_keys.clear()
            return await super().decompose(request)

        async def select_tools(self, request):
            for spec in request.tool_specs:
                spec.parallel_safe = True
                spec.exclusive_resource_keys.clear()
            return await super().select_tools(request)

    steps = [step("a"), step("b", "source-2")]
    trace = MemoryTrace()
    result = await execute(steps, data=UnsafeData(), planner=MutatingPlanner(steps), trace=trace)
    assert result.status == "completed"
    assert event_index(trace, "evidence.added", "a") < event_index(trace, "tool.call.started", "b")


async def test_cancellation_during_first_result_write_preserves_all_buffered_replies():
    """同批任务已经返回的结果，即使尚未轮到按序提交，也仍属于已知结果。"""
    entered = asyncio.Event()

    class HeldResultTrace(MemoryTrace):
        async def put_artifact(self, ref, value):
            await super().put_artifact(ref, value)
            if (
                isinstance(value, dict)
                and value.get("data", {}).get("evidence", [{}])[0].get("payload") == "a"
            ):
                entered.set()
                await asyncio.Future()

    trace, data = HeldResultTrace(), SourceData()
    async with running([step("a"), step("b", "source-2")], trace=trace, data=data) as task:
        await wait_event(entered)
        assert data.active == set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await finish(task)
    detail = trace.events[-1].detail
    assert trace.events[-1].event_type == "run.cancelled"
    assert {item["step_id"] for item in detail["active_calls"]} == {"a", "b"}
    assert all(
        item["outcome"] == "returned" and item["dispatched"] for item in detail["active_calls"]
    )
    assert detail["call_counts"]["steps"] == 2
    assert trace.closed and all(task.done() for task in data.children)


async def test_parallel_observation_write_failure_stops_new_dispatch_and_drains_workers():
    """审计故障不能泄漏任务，也不能放行另一次数据源读取。"""

    class BrokenObservationTrace(MemoryTrace):
        async def append(self, event):
            if event.event_type == "tool.observed":
                raise OSError("synthetic audit failure")
            await super().append(event)

    data, trace = SourceData(), BrokenObservationTrace()
    result = await execute(
        [step("a"), step("b", "source-2"), step("fresh", "source-3")],
        data=data,
        trace=trace,
        max_parallel_tool_calls=2,
    )
    assert (result.status, result.stop_reason) == ("failed", "trace_write_failed")
    assert [request.step_id for request in data.invocations] == ["a", "b"]
    assert result.call_counts.tools == result.call_counts.steps == 2
    assert trace.closed and not data.active
    assert all(task.done() for task in data.children)


async def test_fatal_sibling_prevents_new_retry_after_all_started_results_settle():
    """批次作出致命故障决策后，同批可重试的任务不得再次派发。"""
    data = SourceData(
        errors={
            ("a", 1): m.ErrorInfo(code="busy", message="Transient", retryable=True),
            ("b", 1): m.ErrorInfo(code="unauthorized", message="Denied", recoverable=True),
        }
    )
    trace = MemoryTrace()
    result = await execute(
        [step("a"), step("b", "source-2"), step("fresh", "source-3")],
        data=data,
        trace=trace,
        max_parallel_tool_calls=2,
    )
    assert (result.status, result.stop_reason) == ("failed", "tool_error")
    assert [request.step_id for request in data.invocations] == ["a", "b"]
    assert result.call_counts.retries == 0
    assert terminal_states(trace) == {"a": "failed", "b": "failed", "fresh": "skipped"}
    assert len([e for e in trace.events if e.event_type == "tool.observed"]) == 2
