"""真实控制器的授权、严格 JSON、预算与端口异常回归。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from lake_agent import models as m
from lake_agent.evaluation.models import EvaluationImport
from lake_agent.models import RunConfig, RunRequest
from lake_agent.runtime import run
from lake_agent.web.app import create_app
from lake_agent.web.evaluation_storage import EvaluationRepository
from lake_agent.web.profiles import ExecutionPorts, ExecutionProfile, ProfileMetadata
from lake_agent.web.storage import WebError
from tests.support.ports import DemoContent, DemoData, DemoPlanner


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
        mode="full_agent",
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
            if event.event_type == "planner.decide.call.started":
                await asyncio.sleep(0.03)

    class CountingPlanner(DemoPlanner):
        def __init__(self):
            self.decide_calls = 0

        async def decide(self, req):
            self.decide_calls += 1
            return await super().decide(req)

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
    assert planner.decide_calls == 0
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


@pytest.mark.parametrize("mode", ["no_verifier", "full_agent"])
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
        async def decide(self, req):
            reply = await super().decide(req)
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


@pytest.mark.parametrize("cleanup", ["error", "cancel"])
async def test_adapter_cleanup_preserves_already_committed_terminal_result(tmp_path, cleanup):
    """后置清理不能抹掉已完成的答案，也不能声称该任务仍可恢复。"""
    cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def owned_ports():
        # 每次尝试都获得真实且全新的本地适配器。测试关卡只作用于清理阶段，
        # 此时 run() 已将结果持久化并返回。
        try:
            yield ExecutionPorts(data=DemoData(), content=DemoContent(), planner=DemoPlanner())
        finally:
            cleanup_entered.set()
            if cleanup == "error":
                raise RuntimeError("test adapter cleanup failed")
            await cleanup_release.wait()

    profile = ExecutionProfile(
        metadata=ProfileMetadata(
            profile_id="review-project",
            title="Review project",
            description="Offline adapter teardown regression.",
            kind="project",
            dataset_id="review-data",
            dataset_name="Review data",
            revision="review-v1",
            modules={
                name: {"status": "connected", "label": "Local review adapter"}
                for name in ("data", "planner", "content")
            },
        ),
        ports_factory=owned_ports,
    )
    app = create_app(data_dir=tmp_path, profiles=(profile,))
    service = app.state.run_service
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            response = await client.post(
                "/api/runs",
                json={
                    "question": "What was 2024 annual revenue?",
                    "profile_id": "review-project",
                },
            )
            assert response.status_code == 202, response.text
            run_id = response.json()["run_id"]
            try:
                async with asyncio.timeout(5):
                    await cleanup_entered.wait()
                # 触发取消前先验证持久化结果，
                # 从而区分后置清理与尚未完成的任务。
                lease = await service.state_store.acquire(run_id)
                try:
                    saved = await service.state_store.load(lease)
                    assert saved.state["result"]["status"] == "completed"
                finally:
                    await service.state_store.release(lease)
                if cleanup == "cancel":
                    cancelled = await client.post(f"/api/runs/{run_id}/cancel")
                    assert cancelled.status_code == 202, cancelled.text
                async with asyncio.timeout(5):
                    while True:
                        view = (await client.get(f"/api/runs/{run_id}")).json()
                        if not view["active"]:
                            break
                        await asyncio.sleep(0.01)
                assert view["result"] is not None, view
                assert view["result"]["status"] == view["status"] == "completed"
                assert "120 million HKD" in view["result"]["answer"]["text"]
                assert view["can_resume"] is False
            finally:
                cleanup_release.set()


async def test_repository_does_not_publish_a_report_larger_than_its_read_limit(tmp_path):
    """直接导入不能创建表面有效、却会破坏所有后续读取的记录。"""
    repository = EvaluationRepository(tmp_path)
    await repository.start()
    # Python 接受任意长的小数秒。HTTP 大小限制独立于
    # 公开仓库关于创建可读文件的契约。
    value = {
        "title": "Oversize timestamp",
        "dataset_id": "review-data",
        "task_set_id": "review-tasks",
        "model_id": "offline-demo",
        "evaluator": "Review author",
        "evaluated_at": "2026-11-02T08:30:00." + "1" * 300_000 + "+08:00",
        "results": [{"mode": "full_agent", "task_count": 1}],
    }
    # 严格导入模型或仓库均可拒绝该大小；
    # 两者都不能发布无法读取的文件后仍报告创建成功。
    with pytest.raises((ValueError, WebError)):
        await repository.create(EvaluationImport.model_validate(value))
    assert await repository.listing() == []
    assert list((tmp_path / "evaluations").iterdir()) == []
