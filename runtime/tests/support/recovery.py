"""持久恢复共用记录端口和故障关卡；保留真实 SQLite 与审计行为。

只使用显式测试端口、临时目录和内存传输；测试场景不代表真实模型质量。
"""

from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import pytest

from lake_agent import models as m
from lake_agent.trace import JsonlTraceSink
from tests.support.ports import DemoContent, DemoData


def recovery_api():
    """从公开模块获取真实恢复入口与存储错误，不替换生产实现。"""
    runtime = importlib.import_module("lake_agent.runtime")
    state = importlib.import_module("lake_agent.state")
    return runtime.resume, state.SqliteStateStore, state.StateStoreError


def request(**config):
    """使用严格的数据调用额度，使重复的目录或工具派发可被观察。"""
    limits = {"max_data_calls": 2, "max_model_calls": 8, "require_verification": True, **config}
    return m.RunRequest(
        run_id="recoverable-run",
        task_id="durable-offline-fixture",
        question="What was 2024 annual revenue?",
        config=m.RunConfig(**limits),
    )


class RecordingData(DemoData):
    """统计真实数据调用，并通过事件暴露结果未知或并行执行状态。"""

    def __init__(self, *, hold_tool=False, parallel=False, contract_revision=1):
        super().__init__()
        self.catalog_requests = []
        self.tool_requests = []
        self.returned_sources = []
        self.entered = asyncio.Event()
        self.quarter_returned = asyncio.Event()
        self.hold_tool = hold_tool
        self.parallel = parallel
        self.contract_revision = contract_revision

    def tool_specs(self):
        specs = super().tool_specs()
        return [
            spec.model_copy(
                update={
                    "parallel_safe": self.parallel,
                    "description": f"Recovery fixture contract v{self.contract_revision}",
                }
            )
            for spec in specs
        ]

    async def catalog(self, req):
        self.catalog_requests.append(req)
        return await super().catalog(req)

    async def invoke(self, req):
        self.tool_requests.append(req)
        self.entered.set()
        if self.hold_tool:
            await asyncio.Future()
        # 第二个数据源必须先完成，以检查每个已缓冲原始回复都能恢复，
        # 且不受串行审计提交顺序影响。
        if self.parallel and req.args["source_id"] == "demo-annual":
            await self.quarter_returned.wait()
        reply = await super().invoke(req)
        self.returned_sources.append(req.args["source_id"])
        if req.args["source_id"] == "demo-quarter":
            self.quarter_returned.set()
        return reply


class RecordingContent(DemoContent):
    """使用每个实例独立的句柄，并在最终消费者处验证来源信息。"""

    def __init__(self):
        super().__init__()
        self.instance_id = uuid4().hex
        self.preparations = []
        self.searches = []
        self.compositions = []
        self.verifications = []

    async def prepare_catalog(self, req):
        self.preparations.append(req)
        reply = await super().prepare_catalog(req)
        transient = f"{reply.data.handle}-{self.instance_id}"
        self.catalogs[transient] = self.catalogs.pop(reply.data.handle)
        return reply.model_copy(
            update={"data": reply.data.model_copy(update={"handle": transient})}
        )

    async def search(self, req):
        self.searches.append(req)
        return await super().search(req)

    async def compose(self, req):
        self.compositions.append(req)
        # compose/verify 也可以访问本地目录索引。即使跳过缓存中的搜索，
        # 也必须已经对新实例执行重新绑定。
        assert self.catalogs, "recovery did not bind the fresh content instance"
        return await super().compose(req)

    async def verify(self, req):
        self.verifications.append(req)
        return await super().verify(req)


class RecordingPlanner:
    """按当前请求中的观察选择动作，跨进程不依赖本地调用次数。

    所有动作由测试端口提出，用真实运行控制器执行；本夹具不模拟旧规划接口。
    付费用量用于验证恢复不会重置或重复结算模型请求。
    """

    agent_protocol = "agent-loop-v1"

    def __init__(self, *, two_tools=False):
        self.requests = []
        self.two_tools = two_tools

    @staticmethod
    def paid(reply):
        return reply.model_copy(
            update={"usage": m.Usage(provider_requests=1, input_tokens=7, output_tokens=3)}
        )

    async def decide(self, req):
        self.requests.append(req)
        context = req.context
        if context["has_answer"]:
            decision = (
                m.AgentVerifyDecision()
                if req.require_verification and context["verification_verdict"] is None
                else m.AgentFinishDecision()
            )
        elif context["evidence_summary"]:
            decision = m.AgentComposeDecision()
        elif context["candidates"]:
            candidates = context["candidates"] if self.two_tools else context["candidates"][:1]
            decision = m.AgentToolsDecision(
                steps=[
                    m.AgentToolStep(
                        step_id=f"extract-{candidate['source_id']}",
                        tool_name="extract",
                        source_ids=[candidate["source_id"]],
                        args={
                            "source_id": candidate["source_id"],
                            "query": req.question,
                            "max_chars": 4000,
                        },
                        purpose="Read independently recoverable synthetic evidence",
                    )
                    for candidate in candidates
                ]
            )
        else:
            decision = m.AgentSearchDecision(query=req.question)
        return self.paid(m.ModuleResult(call_id=req.call_id, data=decision, usage=m.Usage.local()))


class InterruptingTrace(JsonlTraceSink):
    """提交指定的公开事件，再取消后续审计过程或使其失败。"""

    def __init__(self, root, *, event_type="evidence.added", fail=False):
        super().__init__(root)
        self.event_type = event_type
        self.fail = fail
        self.entered = asyncio.Event()
        self.interrupted_event = None

    async def append(self, event):
        await super().append(event)
        if event.event_type == self.event_type and self.interrupted_event is None:
            self.interrupted_event = event
            self.entered.set()
            if self.fail:
                raise OSError("controlled audit continuation failure")
            await asyncio.Future()


async def cancel_at_gate(task, entered):
    """始终等待所有者任务结束，包括缺陷实现发生挂起时。"""
    gate = asyncio.create_task(entered.wait())
    try:
        done, _ = await asyncio.wait({task, gate}, timeout=3, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
            pytest.fail("run terminated before reaching the interruption gate")
        assert gate in done, "run did not reach the interruption gate"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
    finally:
        gate.cancel()
        with suppress(asyncio.CancelledError):
            await gate
        if not task.done():
            task.cancel()
        # 即使执行在抵达事件关卡前失败，也要获取其异常。
        # try 块中的原始故障仍是有用的诊断信息。
        with suppress(asyncio.CancelledError, Exception):
            await task


def trace_events(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()]


def assert_no_port_calls(data, content, planner):
    """元数据发现属于本地操作；拒绝恢复时不得运行任何异步适配器方法。"""
    assert data.catalog_requests == data.tool_requests == []
    assert content.preparations == content.searches == content.compositions == []
    assert content.verifications == []
    assert planner.requests == []


class UnavailableAttemptTrace(JsonlTraceSink):
    """在创建审计轨迹前失败；其他状态与轨迹存储仍使用真实实现。"""

    async def create_attempt(self, run_id, attempt_id):
        raise OSError("temporary trace admission failure")


class FrontierTrace(JsonlTraceSink):
    """暂停新执行产生的事件，同时允许历史事件重放。"""

    def __init__(self, root, event_type):
        super().__init__(root)
        self.event_type = event_type
        self.entered = asyncio.Event()
        self.interrupted_event = None

    async def append(self, event):
        await super().append(event)
        if (
            event.event_type == self.event_type
            and not event.detail.get("replayed")
            and self.interrupted_event is None
        ):
            self.interrupted_event = event
            self.entered.set()
            await asyncio.Future()
