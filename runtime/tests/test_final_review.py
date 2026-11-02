"""取消与 Web 恢复准入边界的最终审查回归测试。

测试经过公开运行接口与真实的本地 ASGI、SQLite 和 JSONL 路径。
仅在适配器取消关卡或已提交的单个轨迹事件处中断；
所有数据和规划均使用离线测试替身，不向外部提供方发送请求。
"""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import httpx
import pytest

from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.models import RunRequest
from lake_agent.runtime import run
from lake_agent.web import service as service_module
from lake_agent.web.app import create_app
from lake_agent.web.fixtures import demo_ports
from lake_agent.web.profiles import ExecutionPorts, default_profiles


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
@pytest.mark.parametrize("phase", ["catalog", "planner.decompose"])
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
        async def decompose(self, request):
            calls.append("planner.decompose")
            if phase == "planner.decompose":
                await interrupted_adapter()
            return await super().decompose(request)

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
            else ["catalog", "content.prepare_catalog", "planner.decompose"]
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
            yield client, app.state.demo_service


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

    profile = replace(default_profiles()[1], ports_factory=owned_ports)

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
            json={"question": "What was 2024 annual revenue?", "profile_id": "demo-standard"},
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
