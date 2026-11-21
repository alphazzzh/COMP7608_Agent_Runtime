"""真实本地数据端口与 Runtime 的离线回归测试。"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from lake_agent import models as m
from lake_agent.adapters import data_adapter
from lake_agent.runtime import run
from lake_agent.validation import canonical_json
from lake_agent.web.configuration import _import_factory
from tests.support.agent import ActionPlanner, FlexibleContent, MemoryTrace

FACTORY = "lake_agent.adapters.data_adapter:create_port"


@pytest.fixture
def dataset(tmp_path):
    """准备多字节文本和有明确聚合结果的表格。"""
    (tmp_path / "content.txt").write_text("营收：" + "甲" * 400, encoding="utf-8")
    (tmp_path / "sales.csv").write_text(
        "amount,label\n" + "".join(f"{i},数据条目{i}\n" for i in range(200)), encoding="utf-8"
    )
    return tmp_path


def call_context(budget):
    """构造可直接由 Runtime 传入的调用身份和预算。"""
    return {
        "call_id": "call-0123456789abcdef0123456789abcdef",
        "timeout_ms": 5000,
        "max_output_bytes": budget,
        "operation_id": "op-1",
        "run_id": "run-1",
        "task_id": "task-1",
    }


async def invoke(port, args, budget, *, tool="extract", timeout_ms=20_000):
    """校验适配器实际回复的封套及其规范 JSON 大小。"""
    context = call_context(budget) | {"timeout_ms": timeout_ms}
    raw = await port.invoke(
        m.ToolRequest(
            **context,
            step_id="step-1",
            catalog_snapshot_id="local-snapshot-v1",
            tool_name=tool,
            args=args,
        )
    )
    assert len(canonical_json(raw)) <= budget
    return m.ToolResult.model_validate(raw)


@pytest.mark.parametrize(("max_chars", "budget"), [(15, 1200), (1000, 500)])
async def test_text_truncation_preserves_evidence(dataset, max_chars, budget):
    async with data_adapter.create_port(options={"base_dir": str(dataset)}) as port:
        result = await invoke(
            port, {"source_id": "content.txt", "query": "营收", "max_chars": max_chars}, budget
        )

    assert result.status == "ok" and result.truncated
    (evidence,) = result.data["evidence"]
    assert evidence["source_id"] == "content.txt"
    assert evidence["locator"]["line_number"] == 1
    assert evidence["payload"].startswith("营收")


async def test_small_byte_budget_returns_bounded_error(dataset):
    async with data_adapter.create_port(options={"base_dir": str(dataset)}) as port:
        result = await invoke(
            port, {"source_id": "content.txt", "query": "营收", "max_chars": 1000}, 150
        )
    assert result.status == "error" and result.error.code == "response_too_large"


@pytest.mark.parametrize("budget", [800, 65_536])
async def test_query_returns_whole_rows_with_truncation_flag(dataset, budget):
    async with data_adapter.create_port(options={"base_dir": str(dataset)}) as port:
        result = await invoke(
            port,
            {"source_id": "sales.csv", "sql": "SELECT * FROM data ORDER BY amount"},
            budget,
            tool="query",
        )
    assert result.status == "ok" and result.truncated
    (evidence,) = result.data["evidence"]
    rows = evidence["payload"]["rows"]
    assert 0 < len(rows) <= 100
    assert rows[-1][0] == len(rows) - 1
    assert all(len(row) == 2 for row in rows)


async def test_read_only_aggregate_reaches_runtime_and_preserves_citations(dataset):
    factory = _import_factory(FACTORY)
    sql = (
        "WITH totals AS (SELECT count(*) AS n, sum(amount) AS total FROM data) SELECT * FROM totals"
    )
    action = {
        "kind": "tools",
        "steps": [
            {
                "step_id": "read-total",
                "tool_name": "query",
                "source_ids": ["sales.csv"],
                "args": {"source_id": "sales.csv", "sql": sql},
                "purpose": "Aggregate source",
            }
        ],
    }
    planner = ActionPlanner([action, {"kind": "compose"}, {"kind": "verify"}, {"kind": "finish"}])
    content = FlexibleContent()
    async with factory(options={"base_dir": str(dataset)}, llm=None) as port:
        result = await run(
            m.RunRequest(
                task_id="table-aggregate",
                question="What are the count and total?",
                config=m.RunConfig(require_verification=True),
            ),
            data=port,
            content=content,
            planner=planner,
            trace=MemoryTrace(),
        )
    assert result.status == "completed"
    (evidence,) = content.compositions[0].evidence
    assert evidence.source_id == "sales.csv"
    assert result.answer.citations == [evidence.evidence_id]
    assert evidence.payload == {"columns": ["n", "total"], "rows": [[200, 19900]]}


async def test_query_timeout_stops_and_reaps_worker(dataset, monkeypatch):
    """阻塞一个普通查询的子进程，验证适配器主动终止并回收它。"""
    launch = asyncio.create_subprocess_exec
    workers = []

    async def delayed_worker(*args, **kwargs):
        process = await launch(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
        workers.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_worker)
    async with data_adapter.create_port(options={"base_dir": str(dataset)}) as port:
        result = await invoke(
            port,
            {"source_id": "sales.csv", "sql": "SELECT count(*) FROM data"},
            1500,
            tool="query",
            timeout_ms=100,
        )
    assert result.error.code == "timeout"
    assert len(workers) == 1 and workers[0].returncode is not None


async def test_catalog_paginates_within_response_budget(dataset: Path):
    for index in range(5):
        (dataset / f"report-{index}.txt").touch()
    expected = await asyncio.to_thread(lambda: sorted(file.name for file in dataset.iterdir()))
    ids = []
    cursor = snapshot = None
    async with data_adapter.create_port(options={"base_dir": str(dataset)}) as port:
        while True:
            raw = await port.catalog(
                m.CatalogRequest(
                    **call_context(400), cursor=cursor, snapshot_id=snapshot, page_size=100
                )
            )
            assert len(canonical_json(raw)) <= 400
            page = m.CatalogResult.model_validate(raw)
            assert page.status == "ok"
            ids.extend(source.source_id for source in page.sources)
            snapshot, cursor = page.catalog_snapshot_id, page.next_cursor
            if cursor is None:
                break
    assert ids == expected


async def test_query_cancellation_reaps_started_worker(dataset, monkeypatch):
    """调用方取消时也要等待工作进程退出。"""
    launch = asyncio.create_subprocess_exec
    started = asyncio.Event()
    workers = []

    async def delayed_worker(*args, **kwargs):
        process = await launch(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
        workers.append(process)
        started.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_worker)
    async with data_adapter.create_port(options={"base_dir": str(dataset)}) as port:
        call = asyncio.create_task(
            invoke(
                port,
                {"source_id": "sales.csv", "sql": "SELECT count(*) FROM data"},
                1500,
                tool="query",
            )
        )
        try:
            await asyncio.wait_for(started.wait(), timeout=2)
        finally:
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
    assert len(workers) == 1 and workers[0].returncode is not None


async def test_query_rejects_unregistered_table_before_engine(dataset, monkeypatch):
    async def no_worker(*args, **kwargs):
        pytest.fail("query outside the supported table scope reached DuckDB")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", no_worker)
    async with data_adapter.create_port(options={"base_dir": str(dataset)}) as port:
        result = await invoke(
            port,
            {"source_id": "sales.csv", "sql": "SELECT amount FROM inventory"},
            1500,
            tool="query",
        )
    assert result.status == "error" and result.error.code == "invalid_query"
