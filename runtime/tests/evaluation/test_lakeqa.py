"""验证真实 LakeQA 格式转换和固定组在本地数据端口取证。"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from lake_agent import models as m
from lake_agent.adapters.data_adapter import create_port
from tests.ablation_runtime import _tasks
from tests.support.lakeqa import build_step, main


def test_lakeqa_cli_maps_labels_and_preserves_numeric_precision(tmp_path):
    source = tmp_path / "lakeqa"
    (source / "chain").mkdir(parents=True)
    (source / "chain" / "task_1.json").write_text(
        '{"question_id":"q-1","question":"What was the annual revenue?",'
        '"answer":13.330000000000000001,'
        '"nodes":{"1":{"source":"reports/year.txt","fact":"Private gold"},'
        '"2":{"source":"reports/year.txt"}},'
        '"datasets_used":["reports/year.txt","distractor.txt"]}',
        encoding="utf-8",
    )
    (source / "chain" / "task_2.json").write_text(
        json.dumps(
            {
                "question_id": "q-2",
                "question": "What is the company name?",
                "answer": "Example Ltd",
                "nodes": {"1": {"source": "reports/other.txt"}},
                "relevant_source_ids": [],
            }
        ),
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.json"
    assert main([str(source), str(manifest)]) == 0
    rows = json.loads(manifest.read_text(encoding="utf-8"), parse_float=Decimal)
    assert rows == [
        {
            "task_id": "q-1",
            "question": "What was the annual revenue?",
            "expected_answers": ["13.330000000000000001"],
            "numeric_answer": Decimal("13.330000000000000001"),
            "relevant_source_ids": ["reports/year.txt"],
        },
        {
            "task_id": "q-2",
            "question": "What is the company name?",
            "expected_answers": ["Example Ltd"],
            "relevant_source_ids": [],
        },
    ]
    tasks = _tasks(manifest)
    assert tasks[0].relevant_source_ids == frozenset({"reports/year.txt"})
    assert tasks[1].relevant_source_ids == frozenset()
    with pytest.raises(SystemExit):
        main([str(source), str(manifest)])


def test_lakeqa_cli_rejects_duplicate_task_ids_without_output(tmp_path):
    raw = {"question_id": "q-1", "question": "What is the result?", "answer": "Example"}
    for index in range(2):
        (tmp_path / f"task_{index}.json").write_text(json.dumps(raw), encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    with pytest.raises(SystemExit):
        main([str(tmp_path), str(manifest)])
    assert not manifest.exists()


async def test_fixed_retrieval_reads_real_data_using_question_term(tmp_path):
    source_id = "annual-revenue.txt"
    (tmp_path / source_id).write_text(
        "Annual revenue in 2024 was HKD 120 million.", encoding="utf-8"
    )
    candidate = m.Candidate(source_id=source_id, score=0.8, match_reason="catalog match")
    async with create_port(options={"base_dir": str(tmp_path)}) as data:
        step = build_step(candidate, data.tool_specs(), "What was the 2024 annual revenue?", 0)
        reply = await data.invoke(
            m.ToolRequest(
                call_id="call-1",
                operation_id="operation-1",
                run_id="run-1",
                task_id="task-1",
                step_id=step.step_id,
                tool_name=step.tool_name,
                args=step.args,
                catalog_snapshot_id="local-snapshot-v1",
                timeout_ms=20_000,
                max_output_bytes=65_536,
            )
        )
    assert step.source_ids == [source_id]
    assert step.args["query"] == "revenue"
    assert reply["data"]["evidence"][0]["payload"] == (
        "Annual revenue in 2024 was HKD 120 million."
    )
