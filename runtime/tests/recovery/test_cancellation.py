"""取消的用量结算、清理与工作台触发的真实核心恢复。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import httpx
import pytest

from lake_agent.models import RunRequest, Usage
from lake_agent.runtime import run
from lake_agent.web import service as service_module
from lake_agent.web.app import create_app
from lake_agent.web.profiles import ExecutionPorts
from tests.support.ports import DemoContent, DemoData, DemoPlanner
from tests.support.profiles import fixture_profiles
from tests.support.web_ports import demo_ports


class CancellationTrace:
    """使用内存保存已提交轨迹，并设置由事件驱动的显式取消关卡。"""

    def __init__(self, entered, *, block_return=False):
        self.entered = entered
        self.block_return = block_return
        self.events = []
        self.closed = False

    async def create_run(self, run_id):
        self.run_id = run_id

    async def put_artifact(self, ref, value):
        pass

    async def append(self, event):
        self.events.append(event)
        if self.block_return and event.event_type == "planner.decide.call.completed":
            self.entered.set()
            await asyncio.Future()

    async def close(self):
        self.closed = True
        return "memory-trace"


@pytest.mark.parametrize("reported_requests", [None, 0, 1])
async def test_cancelled_provider_usage_settles_once(reported_requests):
    """调用结果未知时，仅保守计入一次提供方请求；保留已知的本地或模型用量。"""
    entered = asyncio.Event()

    class Planner(DemoPlanner):
        async def decide(self, request):
            if reported_requests is None:
                entered.set()
                await asyncio.Future()
            reply = await super().decide(request)
            usage = (
                Usage.local()
                if reported_requests == 0
                else Usage(provider_requests=1, input_tokens=7, output_tokens=3)
            )
            return reply.model_copy(update={"usage": usage})

    trace = CancellationTrace(entered, block_return=reported_requests is not None)
    task = asyncio.create_task(
        run(
            RunRequest(task_id="cancel-accounting", question="Synthetic annual revenue?"),
            data=DemoData(),
            content=DemoContent(),
            planner=Planner(),
            trace=trace,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    event = trace.events[-1]
    assert event.event_type == "run.cancelled"
    expected = 1 if reported_requests is None else reported_requests
    assert event.detail["call_counts"]["model_calls"] == expected
    active = event.detail["active_call"]
    assert active["phase"] == "planner.decide"
    assert active["call_id"]
    assert active["outcome"] == ("unknown" if reported_requests is None else "returned")
    assert trace.closed


class MemoryTrace:
    """使取消断言不依赖文件系统操作的时序。"""

    def __init__(self):
        self.events = []
        self.closed = False

    async def create_run(self, run_id):
        pass

    async def put_artifact(self, ref, value):
        pass

    async def append(self, event):
        self.events.append(event)

    async def close(self):
        self.closed = True
        return "memory-trace"


@pytest.mark.parametrize("adapter_finish", ["return", "raise"])
@pytest.mark.parametrize("phase", ["catalog", "planner.decide"])
async def test_ordinary_run_propagates_cancellation_swallowed_by_serial_adapter(
    adapter_finish, phase
):
    """被吞掉的调用方取消信号不能转变为结果，也不能触发后续派发。"""
    entered = asyncio.Event()
    calls = []

    async def interrupted_adapter():
        """接收一次取消信号，然后返回或以另一个异常替换该信号。"""
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            # 即使适配器错误地吞掉取消信号后仍会结束，控制器也必须在其返回和
            # 抛出异常这两条路径中，保留来自调用方的取消信号。
            if adapter_finish == "raise":
                raise RuntimeError("adapter replaced cancellation") from None

    class SuppressingData(DemoData):
        async def catalog(self, request):
            calls.append("catalog")
            if phase == "catalog":
                await interrupted_adapter()
            return await super().catalog(request)

        async def invoke(self, request):
            calls.append("tool")
            return await super().invoke(request)

    class RecordingContent(DemoContent):
        async def prepare_catalog(self, request):
            calls.append("content.prepare_catalog")
            return await super().prepare_catalog(request)

    class RecordingPlanner(DemoPlanner):
        async def decide(self, request):
            calls.append("planner.decide")
            if phase == "planner.decide":
                await interrupted_adapter()
            return await super().decide(request)

    trace = MemoryTrace()
    task = asyncio.create_task(
        run(
            RunRequest(task_id="final-review-cancellation", question="Annual revenue?"),
            data=SuppressingData(),
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=trace,
        )
    )
    try:
        async with asyncio.timeout(3):
            await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            async with asyncio.timeout(3):
                await task
        assert calls == (
            ["catalog"]
            if phase == "catalog"
            else ["catalog", "content.prepare_catalog", "planner.decide"]
        )
        assert trace.events[-1].event_type == "run.cancelled"
        # 被中断的模型阶段没有通过校验的用量报告，因此保守地计入恰好一次
        # 模型请求；数据阶段则不计入模型请求。
        assert trace.events[-1].detail["call_counts"]["model_calls"] == (
            0 if phase == "catalog" else 1
        )
        assert trace.closed is True
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@asynccontextmanager
async def running_workspace(data_dir, *, profiles=None):
    """管理真实应用的生命周期及其持久化本地资源。"""
    app = create_app(data_dir=data_dir, profiles=profiles)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield client, app.state.run_service


async def stopped_view(client, run_id):
    """观察任务最终状态的落定，不假定具体调度顺序。"""
    async with asyncio.timeout(5):
        while True:
            response = await client.get(f"/api/runs/{run_id}")
            assert response.status_code == 200, response.text
            view = response.json()
            if not view["active"]:
                return view
            await asyncio.sleep(0.01)


async def assert_web_resume_completes(client, run_id):
    """状态已知且尚未终结的核心调用日志，经 Web 桥接后仍应允许恢复。"""
    view = await stopped_view(client, run_id)
    assert view["can_resume"] is True, view
    response = await client.post(f"/api/runs/{run_id}/resume")
    assert response.status_code == 202, response.text
    completed = await stopped_view(client, run_id)
    assert completed["status"] == "completed", completed
    assert completed["result"]["status"] == "completed"
    assert completed["can_resume"] is False
    assert len(completed["attempts"]) == 2


@pytest.mark.parametrize("restart_service", [False, True])
@pytest.mark.parametrize("adapter_cleanup", ["normal", "error", "cancel"])
async def test_web_preserves_core_recovery_after_known_trace_failure(
    tmp_path, monkeypatch, restart_service, adapter_cleanup
):
    """可恢复的审计故障不能转变为 Web 层永久终结的结果。"""
    failed = False
    original_trace = service_module.PresentationTrace
    cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()
    opened = 0

    @asynccontextmanager
    async def owned_ports():
        nonlocal opened
        opened += 1
        first_attempt = opened == 1
        data, content, planner = demo_ports("standard")
        try:
            yield ExecutionPorts(data=data, content=content, planner=planner)
        finally:
            # 仅在核心返回可恢复的轨迹故障后，破坏第一次尝试的清理过程。
            # 新恢复尝试拥有全新的端口。
            if first_attempt:
                cleanup_entered.set()
                if adapter_cleanup == "error":
                    raise RuntimeError("offline adapter cleanup fault")
                if adapter_cleanup == "cancel":
                    await cleanup_release.wait()

    profile = replace(fixture_profiles()[0], ports_factory=owned_ports)

    class FailOneCommittedEvent(original_trace):
        async def append(self, event):
            nonlocal failed
            await super().append(event)
            if not failed and event.event_type == "evidence.added":
                # 响应与此事件都已提交。该故障发生在可恢复的审计边界，
                # 不属于 I/O 结果未知的情况。
                failed = True
                raise OSError("offline final-review trace fault")

    monkeypatch.setattr(service_module, "PresentationTrace", FailOneCommittedEvent)
    async with running_workspace(tmp_path, profiles=(profile,)) as (client, service):
        response = await client.post(
            "/api/runs",
            json={"question": "What was 2024 annual revenue?", "profile_id": "test-standard"},
        )
        assert response.status_code == 202, response.text
        run_id = response.json()["run_id"]
        if adapter_cleanup == "cancel":
            try:
                async with asyncio.timeout(5):
                    await cleanup_entered.wait()
                cancelled = await client.post(f"/api/runs/{run_id}/cancel")
                assert cancelled.status_code == 202, cancelled.text
            finally:
                cleanup_release.set()
        await stopped_view(client, run_id)
        assert failed is True
        lease = await service.state_store.acquire(run_id)
        try:
            saved = await service.state_store.load(lease)
            assert saved.state["result"] is None
            assert saved.calls
            assert all(entry["status"] == "completed" for entry in saved.calls.values())
        finally:
            await service.state_store.release(lease)
        if not restart_service:
            await assert_web_resume_completes(client, run_id)

    if restart_service:
        async with running_workspace(tmp_path, profiles=(profile,)) as (client, _):
            await assert_web_resume_completes(client, run_id)
