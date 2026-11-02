"""控制器截止时间与清理边界的独立回归测试。"""

from __future__ import annotations

import asyncio

import pytest

from lake_agent import models as m
from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.models import RunConfig, RunRequest
from lake_agent.runtime import run


class MemoryTrace:
    """用于隔离控制器调度行为的协作式记录接收器。"""

    def __init__(self):
        self.events = []
        self.closed = False

    async def create_run(self, run_id):
        self.run_id = run_id

    async def put_artifact(self, ref, value):
        pass

    async def append(self, event):
        self.events.append(event)

    async def close(self):
        self.closed = True
        return "memory://trace"


def request(**config):
    return RunRequest(
        task_id="review-test",
        question="What was annual revenue?",
        mode="fixed_retrieval",
        config=RunConfig(**config),
    )


async def test_expired_per_call_window_never_dispatches_adapter():
    class SlowIntent(MemoryTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "catalog.call.started":
                await asyncio.sleep(0.03)

    class CountingData(DemoData):
        def __init__(self):
            super().__init__()
            self.catalog_calls = 0

        async def catalog(self, req):
            self.catalog_calls += 1
            return await super().catalog(req)

    data = CountingData()
    result = await run(
        request(per_call_timeout_ms=5, max_retries_per_data_operation=0),
        data=data,
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=SlowIntent(),
    )
    assert data.catalog_calls == 0
    assert (result.status, result.stop_reason) == ("timeout", "call_timeout")


async def test_unstarted_tool_attempt_does_not_count_as_logical_step():
    class SlowToolIntent(MemoryTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "tool.call.started":
                await asyncio.sleep(0.03)

    class CountingData(DemoData):
        def __init__(self):
            super().__init__()
            self.invoke_calls = 0

        async def invoke(self, req):
            self.invoke_calls += 1
            return await super().invoke(req)

    data = CountingData()
    result = await run(
        request(per_call_timeout_ms=5, max_retries_per_data_operation=0),
        data=data,
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=SlowToolIntent(),
    )
    assert data.invoke_calls == 0
    assert result.call_counts.steps == 0
    assert result.call_counts.tools == 1


async def test_cancellation_during_terminal_event_closes_trace():
    entered = asyncio.Event()

    class SlowTerminal(MemoryTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "run.completed":
                entered.set()
                await asyncio.Future()

    trace = SlowTerminal()
    task = asyncio.create_task(
        run(
            request(cancel_cleanup_timeout_ms=100),
            data=DemoData(),
            content=DemoContent(),
            planner=DemoPlanner(),
            trace=trace,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert trace.closed
    assert trace.events[-1].event_type == "run.cancelled"
    assert trace.events[-1].detail["pending_call"] is None


async def test_late_adapter_return_cannot_be_accepted_after_call_deadline():
    class IgnoresFirstCancel(DemoData):
        async def catalog(self, req):
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                # Python 取消采用协作机制，适配器可以抑制取消，
                # 但迟到的结果仍不能将该调用变为成功。
                return await super().catalog(req)

    result = await run(
        request(per_call_timeout_ms=5, max_retries_per_data_operation=0),
        data=IgnoresFirstCancel(),
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=MemoryTrace(),
    )
    assert (result.status, result.stop_reason) == ("timeout", "call_timeout")


async def test_unstarted_provider_attempt_is_not_reported_as_model_call():
    class SlowPlannerIntent(MemoryTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "planner.decompose.call.started":
                await asyncio.sleep(0.03)

    class CountingPlanner(DemoPlanner):
        def __init__(self):
            self.decompose_calls = 0

        async def decompose(self, req):
            self.decompose_calls += 1
            return await super().decompose(req)

    planner = CountingPlanner()
    result = await run(
        RunRequest(
            task_id="review-test",
            question="What was annual revenue?",
            mode="no_verifier",
            config=RunConfig(per_call_timeout_ms=5, max_retries_per_data_operation=0),
        ),
        data=DemoData(),
        content=DemoContent(),
        planner=planner,
        trace=SlowPlannerIntent(),
    )
    assert planner.decompose_calls == 0
    assert result.call_counts.model_calls == 0
    assert result.call_counts.planner == 0


async def test_failed_cancel_event_does_not_skip_trace_close():
    entered = asyncio.Event()

    class BrokenCancelEvent(MemoryTrace):
        async def append(self, event):
            if event.event_type == "catalog.call.started":
                entered.set()
            if event.event_type == "run.cancelled":
                raise OSError("simulated trace append failure")
            await super().append(event)

    trace = BrokenCancelEvent()

    class SlowData(DemoData):
        async def catalog(self, req):
            await asyncio.Future()

    task = asyncio.create_task(
        run(request(), data=SlowData(), content=DemoContent(), planner=DemoPlanner(), trace=trace)
    )
    async with asyncio.timeout(1):
        await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert trace.closed


async def test_cancellation_reports_call_started_commit_race():
    entered = asyncio.Event()

    class SlowStartedEvent(MemoryTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "catalog.call.started":
                entered.set()
                await asyncio.Future()

    class SlowData(DemoData):
        def __init__(self):
            super().__init__()
            self.catalog_calls = 0

        async def catalog(self, req):
            self.catalog_calls += 1
            await asyncio.Future()

    trace, data = SlowStartedEvent(), SlowData()
    task = asyncio.create_task(
        run(
            request(),
            data=data,
            content=DemoContent(),
            planner=DemoPlanner(),
            trace=trace,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    cancelled = [event for event in trace.events if event.event_type == "run.cancelled"]
    assert len(cancelled) == 1
    pending = cancelled[0].detail["pending_call"]
    assert pending["phase"] == "catalog"
    assert pending["operation_id"]
    assert pending["call_id"]
    assert cancelled[0].detail["call_counts"]["catalog"] == 1
    assert data.catalog_calls == 0
    assert trace.closed


@pytest.mark.parametrize("mode", ["fixed_retrieval", "no_verifier", "full_agent"])
async def test_zero_provider_usage_releases_reserved_model_budget(mode):
    result = await run(
        RunRequest(
            task_id="local-budget",
            question="What was annual revenue?",
            mode=mode,
            config=RunConfig(max_model_calls=1),
        ),
        data=DemoData(),
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=MemoryTrace(),
    )
    assert result.status == "completed"
    assert result.call_counts.model_calls == 0
    assert result.usage.provider_requests == 0


async def test_actual_provider_usage_keeps_model_budget_consumed():
    class ProviderPlanner(DemoPlanner):
        async def decompose(self, req):
            reply = await super().decompose(req)
            return reply.model_copy(update={"usage": m.Usage(provider_requests=1)})

    result = await run(
        RunRequest(
            task_id="provider-budget",
            question="What was annual revenue?",
            config=RunConfig(max_model_calls=1),
        ),
        data=DemoData(),
        content=DemoContent(),
        planner=ProviderPlanner(),
        trace=MemoryTrace(),
    )
    assert (result.status, result.stop_reason) == ("budget_exceeded", "model_call_budget")
    assert result.call_counts.model_calls == 1
    assert result.call_counts.planner == 1
    assert result.call_counts.tools == 0


@pytest.mark.parametrize("cancel", [True, False])
async def test_retry_start_interruption_preserves_already_executed_logical_step(cancel):
    entered = asyncio.Event()

    class InterruptedRetry(MemoryTrace):
        async def append(self, event):
            if event.event_type == "tool.call.started" and event.attempt == 2:
                if not cancel:
                    raise OSError("retry start write failed")
                await super().append(event)
                entered.set()
                await asyncio.Future()
            else:
                await super().append(event)

    class RetryData(DemoData):
        async def invoke(self, req):
            return m.ToolResult(
                call_id=req.call_id,
                status="error",
                error=m.ErrorInfo(code="busy", message="Try later", retryable=True),
            )

    trace = InterruptedRetry()
    task = asyncio.create_task(
        run(
            request(candidate_top_k=1),
            data=RetryData(),
            content=DemoContent(),
            planner=DemoPlanner(),
            trace=trace,
        )
    )
    if cancel:
        async with asyncio.timeout(1):
            await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        terminal = trace.events[-1]
        assert terminal.event_type == "run.cancelled"
        assert terminal.detail["call_counts"]["steps"] == 1
        assert terminal.detail["call_counts"]["tools"] == 2
        assert terminal.detail["pending_call"]["attempt"] == 2
    else:
        result = await task
        assert result.stop_reason == "trace_write_failed"
        assert result.call_counts.steps == 1
        assert result.call_counts.tools == 1
    assert trace.closed


async def test_global_timeout_records_matching_call_failure_before_run_end():
    class SlowData(DemoData):
        async def catalog(self, req):
            await asyncio.sleep(1)

    trace = MemoryTrace()
    result = await run(
        request(max_wall_time_ms=20, per_call_timeout_ms=1000),
        data=SlowData(),
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=trace,
    )
    types = [event.event_type for event in trace.events]
    assert result.stop_reason == "time_budget"
    assert "catalog.call.started" in types
    assert "catalog.call.failed" in types
    assert types.index("catalog.call.failed") < types.index("run.stopped")


async def test_global_deadline_during_intent_write_stops_before_dispatch():
    class SlowIntent(MemoryTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "catalog.call.started":
                await asyncio.sleep(0.03)

    class CountingData(DemoData):
        def __init__(self):
            super().__init__()
            self.catalog_calls = 0

        async def catalog(self, req):
            self.catalog_calls += 1
            return await super().catalog(req)

    data, trace = CountingData(), SlowIntent()
    result = await run(
        request(max_wall_time_ms=10, per_call_timeout_ms=1000),
        data=data,
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=trace,
    )
    assert result.stop_reason == "trace_write_failed"
    assert data.catalog_calls == 0


async def test_not_applicable_subtask_does_not_satisfy_dependent_subtask():
    class DependentPlanner:
        def __init__(self):
            self.selected = []

        async def decompose(self, req):
            return m.ModuleResult(
                call_id=req.call_id,
                data=m.TaskPlan(
                    subtasks=[
                        m.Subtask(
                            subtask_id="primary",
                            objective="Check a required fact",
                            search_query="required fact",
                        ),
                        m.Subtask(
                            subtask_id="dependent",
                            objective="Use the primary fact",
                            search_query="primary fact",
                            depends_on=["primary"],
                        ),
                    ]
                ),
                usage=m.Usage.local(),
            )

        async def select_tools(self, req):
            self.selected.append(req.subtask.subtask_id)
            return m.ModuleResult(
                call_id=req.call_id,
                data=m.FinishDecision(reason="not_applicable"),
                usage=m.Usage.local(),
            )

    planner = DependentPlanner()
    await run(
        RunRequest(task_id="review-test", question="Question?", mode="no_verifier"),
        data=DemoData(),
        content=DemoContent(),
        planner=planner,
        trace=MemoryTrace(),
    )
    assert planner.selected == ["primary"]


async def test_local_method_provider_violation_is_counted_before_failure():
    class InvalidContent(DemoContent):
        async def prepare_catalog(self, req):
            result = await super().prepare_catalog(req)
            payload = result.model_dump(mode="json")
            payload["usage"] = m.Usage(provider_requests=1).model_dump(mode="json")
            return m.ModuleResult[m.CatalogHandle].model_validate(payload)

    result = await run(
        request(),
        data=DemoData(),
        content=InvalidContent(),
        planner=DemoPlanner(),
        trace=MemoryTrace(),
    )
    assert result.status == "failed"
    assert result.stop_reason == "invalid_module_response"
    assert result.call_counts.model_calls == 1
    assert result.usage.provider_requests == 1


async def test_failed_tool_event_contains_error_and_duration():
    class FailingData(DemoData):
        async def invoke(self, req):
            return m.ToolResult(
                call_id=req.call_id,
                status="error",
                error=m.ErrorInfo(
                    code="upstream_unavailable",
                    message="temporary backend failure",
                    retryable=False,
                ),
                duration_ms=2,
            )

    trace = MemoryTrace()
    result = await run(
        request(max_retries_per_data_operation=0),
        data=FailingData(),
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=trace,
    )
    assert (result.status, result.stop_reason) == ("failed", "tool_error")
    failed = next(event for event in trace.events if event.event_type == "tool.call.failed")
    assert failed.status == "error"
    assert failed.error.code == "upstream_unavailable"
    assert failed.error.message == "temporary backend failure"
    assert failed.duration_ms is not None


async def test_timeout_event_contains_stable_error_and_duration():
    class SlowData(DemoData):
        async def catalog(self, req):
            await asyncio.sleep(1)

    trace = MemoryTrace()
    result = await run(
        request(per_call_timeout_ms=5, max_retries_per_data_operation=0),
        data=SlowData(),
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=trace,
    )
    assert (result.status, result.stop_reason) == ("timeout", "call_timeout")
    failed = next(event for event in trace.events if event.event_type == "catalog.call.failed")
    assert failed.status == "timeout"
    assert failed.error.code == "timeout"
    assert failed.duration_ms is not None


async def test_adapter_exception_event_uses_stable_error_without_raw_message():
    class BrokenData(DemoData):
        async def catalog(self, req):
            raise RuntimeError("secret endpoint https://private.example/key=token")

    trace = MemoryTrace()
    result = await run(
        request(),
        data=BrokenData(),
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=trace,
    )
    assert (result.status, result.stop_reason) == ("failed", "tool_error")
    failed = next(event for event in trace.events if event.event_type == "catalog.call.failed")
    assert failed.status == "error"
    assert failed.error.code == "adapter_error"
    assert failed.error.message == "Adapter call failed."
    assert failed.duration_ms is not None
    assert "private.example" not in failed.model_dump_json()


async def test_verifier_feedback_targets_declared_subtask():
    class MultiPlanner:
        def __init__(self):
            self.selections = []

        async def decompose(self, req):
            return m.ModuleResult(
                call_id=req.call_id,
                data=m.TaskPlan(
                    subtasks=[
                        m.Subtask(
                            subtask_id="s1", objective="First fact", search_query="first fact"
                        ),
                        m.Subtask(
                            subtask_id="s2", objective="Second fact", search_query="second fact"
                        ),
                    ]
                ),
                usage=m.Usage.local(),
            )

        async def select_tools(self, req):
            self.selections.append(
                (
                    req.subtask.subtask_id,
                    req.verifier_focus.subtask_id if req.verifier_focus else None,
                )
            )
            if req.verifier_focus is not None:
                return m.ModuleResult(
                    call_id=req.call_id,
                    data=m.FinishDecision(reason="evidence_complete"),
                    usage=m.Usage.local(),
                )
            if req.subtask.subtask_id not in {item[0] for item in self.selections[:-1]}:
                candidate = req.candidates[0]
                return m.ModuleResult(
                    call_id=req.call_id,
                    data=m.ToolPlan(
                        steps=[
                            m.ToolStep(
                                step_id=f"{req.subtask.subtask_id}-step",
                                subtask_id=req.subtask.subtask_id,
                                tool_name="extract",
                                source_ids=[candidate.source_id],
                                args={
                                    "source_id": candidate.source_id,
                                    "query": req.subtask.search_query,
                                    "max_chars": 100,
                                },
                                purpose="Read evidence",
                            )
                        ]
                    ),
                    usage=m.Usage.local(),
                )
            return m.ModuleResult(
                call_id=req.call_id,
                data=m.FinishDecision(reason="evidence_complete"),
                usage=m.Usage.local(),
            )

    class FeedbackContent(DemoContent):
        def __init__(self):
            super().__init__()
            self.verify_calls = 0
            self.feedback_searches = []

        async def search(self, req):
            if req.subtask.search_query == "follow-up":
                self.feedback_searches.append(req.subtask.subtask_id)
            return await super().search(req)

        async def verify(self, req):
            self.verify_calls += 1
            if self.verify_calls == 1:
                verification = m.Verification(
                    verdict="insufficient",
                    issues=[],
                    recommendation=m.Recommendation(
                        action="search_more",
                        focus=m.Focus(subtask_id="s2", query="follow-up"),
                    ),
                )
            else:
                verification = m.Verification(
                    verdict="supported",
                    issues=[],
                    recommendation=m.Recommendation(action="none"),
                )
            return m.ModuleResult(call_id=req.call_id, data=verification, usage=m.Usage.local())

    planner, content = MultiPlanner(), FeedbackContent()
    result = await run(
        RunRequest(
            task_id="feedback-target",
            question="Which facts are supported?",
            mode="full_agent",
            config=RunConfig(max_feedback_rounds=1),
        ),
        data=DemoData(),
        content=content,
        planner=planner,
        trace=MemoryTrace(),
    )
    assert (result.status, result.stop_reason) == ("completed", "verified_supported")
    assert content.feedback_searches == ["s2"]
    assert ("s2", "s2") in planner.selections


@pytest.mark.parametrize(
    "initial_reason,feedback_reason,initial_completed,feedback_completed",
    [
        (
            "insufficient_evidence",
            "evidence_complete",
            ["leading", "trailing"],
            ["leading", "target", "trailing"],
        ),
        (
            "evidence_complete",
            "insufficient_evidence",
            ["leading", "target", "trailing"],
            ["leading", "trailing"],
        ),
        (
            "evidence_complete",
            "not_applicable",
            ["leading", "target", "trailing"],
            ["leading", "trailing"],
        ),
        (
            "evidence_complete",
            "evidence_complete",
            ["leading", "target", "trailing"],
            ["leading", "target", "trailing"],
        ),
    ],
)
async def test_feedback_reconciles_completed_subtasks(
    initial_reason, feedback_reason, initial_completed, feedback_completed
):
    """每轮反馈后，compose 均按有向无环图顺序接收当前成功状态。"""

    class ReplanningFixture(DemoPlanner):
        def __init__(self):
            self.selections = {}

        async def decompose(self, req):
            # 输入顺序不同于执行顺序：必须先运行 leading，
            # 再运行 target 和 trailing。恢复成功的 target 应处于中间。
            return m.ModuleResult(
                call_id=req.call_id,
                data=m.TaskPlan(
                    subtasks=[
                        m.Subtask(
                            subtask_id="target",
                            objective="Target fact",
                            search_query="target",
                            depends_on=["leading"],
                        ),
                        m.Subtask(
                            subtask_id="trailing",
                            objective="Trailing fact",
                            search_query="tail",
                            depends_on=["leading"],
                        ),
                        m.Subtask(
                            subtask_id="leading",
                            objective="Prerequisite",
                            search_query="lead",
                        ),
                    ]
                ),
                usage=m.Usage.local(),
            )

        async def select_tools(self, req):
            subtask_id = req.subtask.subtask_id
            feedback = req.verifier_focus is not None
            key = (subtask_id, feedback)
            self.selections[key] = self.selections.get(key, 0) + 1
            reason = (
                (feedback_reason if feedback else initial_reason)
                if subtask_id == "target"
                else "evidence_complete"
            )
            if reason != "evidence_complete" or self.selections[key] > 1:
                decision = m.FinishDecision(reason=reason)
            else:
                source_id = req.candidates[0].source_id
                decision = m.ToolPlan(
                    steps=[
                        m.ToolStep(
                            step_id=req.call_id,
                            subtask_id=subtask_id,
                            tool_name="extract",
                            source_ids=[source_id],
                            args={"source_id": source_id, "query": "fact", "max_chars": 100},
                            purpose="Read a synthetic fact for the current subtask pass",
                        )
                    ]
                )
            return m.ModuleResult(call_id=req.call_id, data=decision, usage=m.Usage.local())

    class RecordingContent(DemoContent):
        def __init__(self):
            super().__init__()
            self.composed_subtasks = []
            self.verify_calls = 0

        async def compose(self, req):
            self.composed_subtasks.append([item.subtask_id for item in req.completed_subtasks])
            return await super().compose(req)

        async def verify(self, req):
            self.verify_calls += 1
            if self.verify_calls == 1:
                return m.ModuleResult(
                    call_id=req.call_id,
                    data=m.Verification(
                        verdict="insufficient",
                        recommendation=m.Recommendation(
                            action="search_more",
                            focus=m.Focus(subtask_id="target", query="follow-up target fact"),
                        ),
                    ),
                    usage=m.Usage.local(),
                )
            return await super().verify(req)

    content = RecordingContent()
    result = await run(
        RunRequest(
            task_id="feedback-completed-subtasks",
            question="Which facts are supported?",
            mode="full_agent",
            config=RunConfig(max_feedback_rounds=1),
        ),
        data=DemoData(),
        content=content,
        planner=ReplanningFixture(),
        trace=MemoryTrace(),
    )
    assert (result.status, result.stop_reason) == ("completed", "verified_supported")
    assert content.composed_subtasks == [initial_completed, feedback_completed]
    assert len(content.composed_subtasks[-1]) == len(set(content.composed_subtasks[-1]))
