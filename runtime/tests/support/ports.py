"""仅供离线测试使用的合成端口，不属于产品入口或真实模块实现。

这些测试替身有意不做数据解析、网络调用或模型推理，
只用于明确展示真实数据、检索与生成端口的接入位置。
所有测试替身均可重入：运行状态存放于请求 ID 或各运行独有的句柄中。
"""

from __future__ import annotations

from lake_agent import models as m

EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "source_id": {"type": "string"},
        "query": {"type": "string"},
        "max_chars": {"type": "integer", "minimum": 1},
    },
    "required": ["source_id", "query", "max_chars"],
    "additionalProperties": False,
}


class DemoData:
    """提供两个合成数据源、一个冻结目录和一个只读提取工具。"""

    def __init__(self, page_size=100):
        self.page_size = page_size
        self.sources = [
            m.SourceRecord(
                source_id="demo-annual",
                name="synthetic-report.txt",
                format="text",
                summary="Synthetic annual revenue example",
                metadata={"year": 2024},
            ),
            m.SourceRecord(
                source_id="demo-quarter",
                name="synthetic-quarter.txt",
                format="text",
                summary="Synthetic quarterly context",
                metadata={"year": 2024},
            ),
        ]

    def tool_specs(self):
        return [
            m.ToolSpec(
                name="extract",
                description="Extract a synthetic fixture excerpt",
                input_schema=EXTRACT_SCHEMA,
                idempotent=True,
                uses_sources=True,
                source_id_paths=["/source_id"],
            )
        ]

    async def catalog(self, request):
        index = int(request.cursor or 0)
        size = min(request.page_size, self.page_size)
        page = self.sources[index : index + size]
        end = index + len(page)
        return m.CatalogResult(
            call_id=request.call_id,
            status="ok",
            catalog_snapshot_id="demo-v1",
            sources=page,
            next_cursor=str(end) if end < len(self.sources) else None,
            has_more=end < len(self.sources),
            duration_ms=0,
        )

    async def invoke(self, request):
        source_id = request.args["source_id"]
        text = (
            "Synthetic fixture: 2024 annual revenue was 120 million HKD."
            if source_id == "demo-annual"
            else "Synthetic fixture: reporting year was 2024."
        )
        max_chars = request.args["max_chars"]
        return m.ToolResult(
            call_id=request.call_id,
            status="ok",
            data={
                "evidence": [
                    {
                        "source_id": source_id,
                        "content_type": "text",
                        "payload": text[:max_chars],
                        "locator": {"type": "text_line", "line": 1},
                        "warnings": ["Synthetic demonstration data"],
                    }
                ]
            },
            duration_ms=0,
            # 此标志反映是否实际截断，并覆盖恰好达到长度边界的情况。
            truncated=len(text) > max_chars,
        )


def _reply(request, payload):
    """本地测试替身明确上报已知为零的服务商用量。"""
    return m.ModuleResult(
        call_id=request.call_id, status="ok", data=payload, usage=m.Usage.local(), duration_ms=0
    )


class DemoContent:
    """用于控制器测试的最小本地行为，不实现检索算法。"""

    def __init__(self):
        self.catalogs = {}

    async def prepare_catalog(self, request):
        handle = f"catalog-{request.run_id}"
        self.catalogs[handle] = (
            request.run_id,
            request.catalog_snapshot_id,
            [s.source_id for s in request.sources],
        )
        return _reply(
            request,
            m.CatalogHandle(
                catalog_snapshot_id=request.catalog_snapshot_id,
                catalog_digest=request.catalog_digest,
                handle=handle,
            ),
        )

    async def search(self, request):
        owner, snapshot, sources = self.catalogs[request.catalog_handle]
        if owner != request.run_id or snapshot != request.catalog_snapshot_id:
            raise ValueError("catalog handle belongs to another run or snapshot")
        allowed = (
            sources
            if request.source_id_filter is None
            else [s for s in sources if s in request.source_id_filter]
        )
        return _reply(
            request,
            m.SearchResponse(
                candidates=[
                    m.Candidate(
                        source_id=s, score=1.0 - i * 0.1, match_reason="Synthetic fixture ordering"
                    )
                    for i, s in enumerate(allowed[: request.limit])
                ]
            ),
        )

    async def compose(self, request):
        return _reply(
            request,
            m.AnswerRecord(
                text="Offline demonstration only: synthetic 2024 revenue was 120 million HKD.",
                citations=[request.evidence[0].evidence_id],
                unresolved_questions=[],
            ),
        )

    async def verify(self, request):
        return _reply(
            request,
            m.Verification(
                verdict="supported",
                issues=[],
                recommendation=m.Recommendation(action="none", focus=None),
            ),
        )


class DemoPlanner:
    """无会话状态的自主动作测试替身；仅用于离线控制器回归。"""

    agent_protocol = m.AGENT_PROTOCOL

    async def decide(self, request):
        context = request.context
        last = context.get("last_action") or {}
        if context["has_answer"]:
            if context["verification_verdict"] is None and last.get("kind") != "verify":
                action = m.AgentVerifyDecision()
            else:
                action = m.AgentFinishDecision()
        elif context["evidence_summary"]:
            action = m.AgentComposeDecision()
        elif context["candidates"]:
            source = context["candidates"][0]["source_id"]
            action = m.AgentToolsDecision(
                steps=[
                    m.AgentToolStep(
                        step_id=f"step-{request.call_id}",
                        tool_name="extract",
                        source_ids=[source],
                        args={"source_id": source, "query": request.question, "max_chars": 4000},
                        purpose="Read the synthetic source excerpt",
                    )
                ]
            )
        elif last.get("kind") == "search":
            action = m.AgentStopDecision(reason="No candidate sources")
        else:
            action = m.AgentSearchDecision(query=request.question)
        return _reply(request, action)
