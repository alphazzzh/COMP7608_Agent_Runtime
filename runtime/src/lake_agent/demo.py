"""Offline fixtures and CLI for independent integration, never a benchmark result.

The fixtures intentionally perform no parsing, network calls or model inference.
They show precisely where real data, retrieval and generation ports connect.
All fixtures are reentrant: run state lives in request IDs or per-run handles.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from . import models as m

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
    """Two synthetic sources, a frozen catalog and one read-only extract tool."""

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
            # The flag describes actual clipping, including the exact-length boundary.
            truncated=len(text) > max_chars,
        )


def _reply(request, payload):
    """A local fixture reports known zero provider usage explicitly."""
    return m.ModuleResult(
        call_id=request.call_id, status="ok", data=payload, usage=m.Usage.local(), duration_ms=0
    )


class DemoContent:
    """Minimal local behavior for controller testing, not a retrieval algorithm."""

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
    """Deterministic plan fixture. The genuine model adapter is JsonPlanner."""

    async def decompose(self, request):
        return _reply(
            request,
            m.TaskPlan(
                subtasks=[
                    m.Subtask(
                        subtask_id="annual-revenue",
                        objective=request.question,
                        search_query=request.question,
                    )
                ]
            ),
        )

    async def select_tools(self, request):
        available = [
            candidate
            for candidate in request.candidates
            if candidate.source_id
            not in {
                item["source_id"] if isinstance(item, dict) else item.source_id
                for item in request.evidence_summary
            }
        ]
        if request.evidence_summary:
            decision = m.FinishDecision(reason="evidence_complete")
        elif not available:
            decision = m.FinishDecision(reason="insufficient_evidence")
        else:
            candidate = available[0]
            decision = m.ToolPlan(
                steps=[
                    m.ToolStep(
                        step_id=f"step-{request.call_id}",
                        subtask_id=request.subtask.subtask_id,
                        tool_name="extract",
                        source_ids=[candidate.source_id],
                        args={
                            "source_id": candidate.source_id,
                            "query": request.subtask.search_query,
                            "max_chars": 4000,
                        },
                        purpose="Read the synthetic source excerpt",
                    )
                ]
            )
        return _reply(request, decision)


async def _demo(mode: str, output: Path):
    from .runtime import run
    from .trace import JsonlTraceSink

    request = m.RunRequest(
        task_id="synthetic-demo",
        question="What was 2024 annual revenue?",
        mode=mode,
        config=m.RunConfig(),
    )
    result = await run(
        request,
        data=DemoData(),
        content=DemoContent(),
        planner=DemoPlanner(),
        trace=JsonlTraceSink(output),
    )
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))
    return 0 if result.status == "completed" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=["fixed_retrieval", "no_verifier", "full_agent"], default="full_agent"
    )
    parser.add_argument("--output", type=Path, default=Path("runs"))
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_demo(args.mode, args.output)))


if __name__ == "__main__":
    main()
