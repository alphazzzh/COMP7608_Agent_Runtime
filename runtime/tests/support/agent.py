"""自主控制器共用测试端口、动作及内存审计；不构成生产策略。

只使用显式测试端口、临时目录和内存传输；测试场景不代表真实模型质量。
"""

from __future__ import annotations

import json
from copy import deepcopy

import pytest

from lake_agent import models as m
from lake_agent.runtime import run
from tests.support.ports import DemoContent, DemoData


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


class ActionPlanner:
    """显式声明新协议，旧 decompose/select_tools 一旦被调用即失败。"""

    agent_protocol = "agent-loop-v1"

    def __init__(self, actions):
        self.actions = iter(actions)
        self.requests = []

    async def decide(self, request):
        self.requests.append(request.model_copy(deep=True))
        action = next(self.actions)
        if callable(action):
            action = action(request)
        return m.ModuleResult(
            call_id=request.call_id, status="ok", data=deepcopy(action), usage=m.Usage.local()
        )

    async def decompose(self, request):
        pytest.fail("Autonomous flow must not force task decomposition")

    async def select_tools(self, request):
        pytest.fail("Autonomous flow must not use the legacy subtask loop")


class FlexibleContent(DemoContent):
    """允许无正文证据的目录说明，并记录模型实际选择的内容操作。"""

    def __init__(self, verdicts=()):
        super().__init__()
        self.searches, self.compositions, self.verifications = [], [], []
        self.verdicts = iter(verdicts)

    async def search(self, request):
        self.searches.append(request.model_copy(deep=True))
        return await super().search(request)

    async def compose(self, request):
        self.compositions.append(request.model_copy(deep=True))
        return m.ModuleResult(
            call_id=request.call_id,
            status="ok",
            usage=m.Usage.local(),
            data=m.AnswerRecord(
                text=f"Answer {len(self.compositions)}: {request.question}",
                citations=[e.evidence_id for e in request.evidence],
            ),
        )

    async def verify(self, request):
        self.verifications.append(request.model_copy(deep=True))
        verdict = next(self.verdicts, "supported")
        return m.ModuleResult(
            call_id=request.call_id,
            status="ok",
            usage=m.Usage.local(),
            data=m.Verification(verdict=verdict, recommendation=m.Recommendation(action="none")),
        )


def tool_action(source="demo-annual", step="read-1"):
    """只使用已注册工具与来源，不需要虚构一个模型子任务。"""
    return {
        "kind": "tools",
        "steps": [
            {
                "step_id": step,
                "tool_name": "extract",
                "source_ids": [source],
                "args": {"source_id": source, "query": "revenue", "max_chars": 1000},
                "purpose": "Read selected source",
            }
        ],
    }


async def execute(actions, *, content=None, **config):
    planner, trace = ActionPlanner(actions), MemoryTrace()
    content = content or FlexibleContent()
    result = await run(
        m.RunRequest(
            task_id="agent-test", question="目前有哪些资料？", config=m.RunConfig(**config)
        ),
        data=DemoData(),
        content=content,
        planner=planner,
        trace=trace,
    )
    return result, planner, content, trace


class StatelessRevisionContent(FlexibleContent):
    """修订是对明确错误差异的确定转换，不取决于第几次调用。"""

    async def compose(self, request):
        self.compositions.append(request.model_copy(deep=True))
        revision = getattr(request, "revision", None)
        text = "Revenue was 120 million USD."
        if revision is not None and revision.verification is not None:
            difference = json.loads(revision.verification.issues[0].detail)
            text = revision.previous_answer.text.replace(difference["find"], difference["replace"])
            if revision.instructions:
                text += json.loads(revision.instructions)["suffix"]
        return m.ModuleResult(
            call_id=request.call_id,
            data=m.AnswerRecord(text=text),
            usage=m.Usage.local(),
        )

    async def verify(self, request):
        self.verifications.append(request.model_copy(deep=True))
        if "USD" in request.answer.text:
            value = m.Verification(
                verdict="conflicting",
                issues=[
                    m.VerificationIssue(
                        type="currency",
                        detail=json.dumps({"find": "USD", "replace": "HKD"}),
                    )
                ],
                recommendation=m.Recommendation(action="none"),
            )
        else:
            value = m.Verification(
                verdict="supported", recommendation=m.Recommendation(action="none")
            )
        return m.ModuleResult(call_id=request.call_id, data=value, usage=m.Usage.local())


class RevenueData(DemoData):
    """有同意/冲突收入记录，也有明确不产生正文证据的结构探测。"""

    def __init__(self, *, padding=""):
        super().__init__()
        self.padding = padding
        self.catalog_requests, self.tool_requests = [], []

    async def catalog(self, request):
        self.catalog_requests.append(request.model_copy(deep=True))
        return await super().catalog(request)

    async def invoke(self, request):
        self.tool_requests.append(request.model_copy(deep=True))
        if request.args["query"] == "recoverable-error":
            return m.ToolResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(
                    code="fixture_unavailable",
                    message="This optional read is unavailable.",
                    recoverable=True,
                ),
            )
        evidence = []
        if request.args["query"] != "inspect-only":
            amount = 80 if request.args["query"] == "conflict" else 120
            evidence.append(
                {
                    "source_id": request.args["source_id"],
                    "content_type": "json",
                    "payload": {"notes": self.padding, "revenue_million_hkd": amount},
                    "locator": {"type": "test_record", "key": request.args["query"]},
                }
            )
        return m.ToolResult(call_id=request.call_id, data={"evidence": evidence})


class EvidenceContent(DemoContent):
    """核验取决于所有真实输入记录，不依赖第几次调用。"""

    def __init__(self):
        super().__init__()
        self.compositions, self.verifications = [], []

    async def compose(self, request):
        self.compositions.append(request.model_copy(deep=True))
        first = request.evidence[0] if request.evidence else None
        amount = first.payload["revenue_million_hkd"] if first else None
        answer = m.AnswerRecord(
            text=f"Revenue was {amount} million HKD."
            if first
            else "No revenue evidence is available.",
            citations=[first.evidence_id] if first else [],
        )
        return m.ModuleResult(call_id=request.call_id, data=answer, usage=m.Usage.local())

    async def verify(self, request):
        self.verifications.append(request.model_copy(deep=True))
        amounts = {entry.payload["revenue_million_hkd"] for entry in request.evidence}
        conflicting = len(amounts) > 1
        issues = (
            [
                m.VerificationIssue(
                    type="conflicting_values",
                    evidence_ids=[entry.evidence_id for entry in request.evidence],
                    detail="The registered records report both 120 and 80 million HKD.",
                )
            ]
            if conflicting
            else []
        )
        return m.ModuleResult(
            call_id=request.call_id,
            data=m.Verification(
                verdict="conflicting" if conflicting else "supported",
                issues=issues,
                recommendation=m.Recommendation(action="none"),
            ),
            usage=m.Usage.local(),
        )


def revenue_tool(step, query="revenue"):
    """沿真实注册schema提议动作，来源授权由控制器检查。"""
    action = tool_action(step=step)
    action["steps"][0]["args"]["query"] = query
    return action
