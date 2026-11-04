"""内存 ASGI 生命周期、任务轮询及工具关卡，不启动监听服务。

只使用显式测试端口、临时目录和内存传输；测试场景不代表真实模型质量。
"""

from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager
from typing import get_args

import httpx
import pytest

from lake_agent import models as m
from tests.support.ports import DemoData
from tests.support.profiles import fixture_profiles


def make_app(data_dir, *, max_active_runs=2):
    """通过公开工厂构造测试工作台，并显式注入可信测试配置。"""
    try:
        module = importlib.import_module("lake_agent.web.app")
    except ModuleNotFoundError as exc:
        if exc.name in {"lake_agent.web", "lake_agent.web.app"}:
            pytest.fail("The optional web application factory has not been implemented")
        raise
    return module.create_app(
        data_dir=data_dir,
        max_active_runs=max_active_runs,
        profiles=fixture_profiles(),
        allowed_modes=get_args(m.Mode),
        enable_evaluations=True,
    )


@asynccontextmanager
async def running_client(data_dir, *, max_active_runs=2):
    """HTTPX 不会自行触发生命周期，因此在此始终显式进入并等待其结束。"""
    app = make_app(data_dir, max_active_runs=max_active_runs)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield client


async def create_run(client, *, scenario="standard", question=None, mode="full_agent"):
    """提交合成问题，仅返回已受理运行的身份标识。"""
    response = await client.post(
        "/api/runs",
        json={
            "question": question or "What was 2024 annual revenue?",
            "profile_id": f"test-{scenario}",
            "mode": mode,
        },
    )
    assert response.status_code == 202, response.text
    summary = response.json()
    assert summary["run_id"]
    return summary["run_id"]


async def wait_stopped(client, run_id):
    """限制轮询时长，并在进度停止时显示最终 HTTP 视图。"""
    async with asyncio.timeout(8):
        while True:
            response = await client.get(f"/api/runs/{run_id}")
            assert response.status_code == 200, response.text
            view = response.json()
            if not view["active"]:
                return view
            await asyncio.sleep(0.01)


class InvokeGate:
    """阻塞真实的本地提取器，以观察调度与取消。"""

    def __init__(self, monkeypatch, *, expected=1):
        self.requests = []
        self.expected = expected
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.peak = 0
        original = DemoData.invoke

        async def invoke(data, request):
            self.requests.append(request)
            self.active += 1
            self.peak = max(self.peak, self.active)
            if len(self.requests) >= self.expected:
                self.entered.set()
            try:
                await self.release.wait()
                return await original(data, request)
            finally:
                self.active -= 1

        monkeypatch.setattr(DemoData, "invoke", invoke)

    async def wait_entered(self):
        """工具派发串行执行或缺失时，必须在有限等待内使测试失败。"""
        async with asyncio.timeout(4):
            await self.entered.wait()
