"""显式注入网页测试端口和故障，产品注册表不携带测试配置。

中断标志属于当前任务的 ContextVar；并发运行不能相互触发故障。
故障只在第一次证据已持久化后注入，恢复使用原本的真实 SQLite 日志。
"""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar

from lake_agent.models import RunConfig
from lake_agent.web.profiles import (
    ExecutionPorts,
    ExecutionProfile,
    ModuleConnections,
    ModuleState,
    ProfileMetadata,
)

from .web_ports import SCENARIOS, demo_ports

interrupt_after_evidence = ContextVar("test_interrupt_after_evidence", default=False)


def fixture_profiles():
    """每项配置都由测试显式注册，模拟真实模块工厂的资源边界。"""
    profiles = []
    for scenario in SCENARIOS:
        identity = scenario["id"]

        @asynccontextmanager
        async def ports(selected=identity):
            token = interrupt_after_evidence.set(selected == "recovery")
            try:
                data, content, planner = demo_ports(selected)
                yield ExecutionPorts(data=data, content=content, planner=planner)
            finally:
                interrupt_after_evidence.reset(token)

        data, _, _ = demo_ports(identity)
        connected = ModuleState(status="connected", label="显式注入的离线测试端口")
        profiles.append(
            ExecutionProfile(
                metadata=ProfileMetadata(
                    profile_id=f"test-{identity}",
                    title=f"测试配置 · {scenario['title']}",
                    description=scenario["description"],
                    kind="project",
                    dataset_id="test-revenue",
                    dataset_name="离线测试资料",
                    revision="test-v1",
                    modules=ModuleConnections(data=connected, content=connected, planner=connected),
                    suggested_questions=[scenario["question"]],
                ),
                sources=tuple(data.sources),
                ports_factory=ports,
                run_config=RunConfig(max_wall_time_ms=600_000, model_id="test-model"),
            )
        )
    return profiles


def interrupting_trace(base):
    """将故障注入保留在测试中，不向生产 trace 增加暂停开关。"""

    class InterruptAfterEvidence(base):
        interrupted_runs = set()

        async def append(self, event):
            await super().append(event)
            if (
                interrupt_after_evidence.get()
                and event.run_id not in self.interrupted_runs
                and event.event_type == "evidence.added"
            ):
                self.interrupted_runs.add(event.run_id)
                raise asyncio.CancelledError

    return InterruptAfterEvidence
