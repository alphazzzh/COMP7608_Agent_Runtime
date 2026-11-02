"""Offline migration gates for real LangGraph routing and public runtime behavior.

Resolve the new workflow module inside each structural test so its initial RED
does not prevent the existing public-run regressions from being collected.
Adapters remain local fixtures; no model provider or network access is involved.
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
    """Record real commits and expose any write after the sink was closed."""

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
    """Inspect final consumer inputs rather than bounded planner previews."""

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
    """Local modules release their provisional model-request reservation."""
    return m.ModuleResult(call_id=request.call_id, data=data, usage=m.Usage.local())


def request(*, mode="full_agent", run_id=None, question="Annual revenue?", **limits):
    """Use explicit finite allowances so graph limits cannot hide runaway work."""
    return m.RunRequest(
        task_id="workflow-regression",
        run_id=run_id,
        question=question,
        mode=mode,
        config=m.RunConfig(max_model_calls=1, **limits),
    )


async def wait_for_gate(task, gate):
    """Surface early runtime failures and always observe the temporary waiter."""
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
    """A wrapper node around the old monolithic workflow is not a migration."""
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
    """Fixed mode bypasses planning; no-verifier bypasses only verification."""
    get_config = importlib.import_module("langgraph.config").get_config

    class NodeTrace(MemoryTrace):
        """Read actual node context at committed I/O intent boundaries."""

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
    # These are observed graph execution contexts, not source names or mocked
    # callback counters. A one-node wrapper of the old loop fails this gate.
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
    """Ambient LangSmith or explicit LangChain tracing cannot receive run inputs."""
    langsmith = importlib.import_module("langsmith")
    callbacks = importlib.import_module("langchain_core.callbacks")
    tracer_context = importlib.import_module("langchain_core.tracers.context")
    tracer_module = importlib.import_module("langchain_core.tracers.langchain")
    attached = []
    observed = []

    class LocalTracer(callbacks.BaseCallbackHandler):
        """Keep a tracing regression entirely local, even if auto-enable breaks."""

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

    # Replace only the telemetry sink constructor. Real callback configuration,
    # graph execution and adapter context propagation remain under test.
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
    """A built-in local collector keeps only the caller's explicit outer runs."""
    langsmith = importlib.import_module("langsmith")
    tracer_context = importlib.import_module("langchain_core.tracers.context")
    runnable = importlib.import_module("langchain_core.runnables").RunnableLambda

    async def echo(value):
        return value

    # collect_runs is itself local. Explicitly disable LangSmith so ambient
    # environment settings cannot create a telemetry client during this test.
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
            # Asserting collected payloads proves both input isolation and
            # restoration of actual collection, beyond ContextVar identity.
            assert [(item.name, item.inputs) for item in collector.traced_runs] == [
                ("outer-before", {"owner": "before"}),
                ("outer-after", {"owner": "after"}),
            ]


@pytest.mark.parametrize("mode", ["no_verifier", "full_agent"])
async def test_local_action_loop_exceeds_default_graph_recursion_limit(mode):
    """Nine search/plan/execute rounds require more than 25 genuine graph steps."""
    rounds = 9

    class DistinctData(DemoData):
        async def invoke(self, req):
            reply = await super().invoke(req)
            item = reply.data["evidence"][0]
            # Distinct arguments, locators and payloads are real independent
            # actions, rather than repeatedly requesting one deduplicated read.
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
    """Read nodes return directly to planning without another search or data call."""

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
    assert len(planner.pages) > 12  # At least 26 read/plan graph transitions.
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
    """Shared reentrant ports cannot imply shared graph invocation state."""
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
    """A second caller cancel must not interrupt the graph's owned cleanup task."""
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
                # Remain finite and cooperative even when a broken owner
                # forwards repeated cancellation into this cleanup await.
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
            # A ready-queue event lets the cancellation request be delivered;
            # no elapsed-time assumption controls the cleanup interleaving.
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
