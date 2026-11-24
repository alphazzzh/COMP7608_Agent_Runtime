"""持续任务交接的真实持久计划、截断标记和 UTF-8 总量边界。

使用离线控制器和临时 SQLite 核验已接受计划的输出；历史证据身份只作为
不应跨轮携带的反例，不启动监听服务或真实模型请求。
"""

from __future__ import annotations

import json

import pytest

from lake_agent import models as m
from lake_agent.missions.handoff import continuation, from_snapshot
from lake_agent.runtime import run
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink
from tests.support.agent import ActionPlanner, FlexibleContent
from tests.support.ports import DemoData


def snapshot(*, answer=None, tasks=(), completed=(), status="insufficient_evidence"):
    """构造独立、有效的核心终态；不复用待测摘要的生成过程。"""
    result = m.RunResult(
        run_id="run-old",
        task_id="task-old",
        mode="full_agent",
        status=status,
        stop_reason="agent_stopped",
        answer=answer,
        started_at="2026-11-17T00:00:00+00:00",
        ended_at="2026-11-17T00:00:01+00:00",
        duration_ms=1000,
        versions=m.Versions(
            model_id="offline",
            model_params={},
            prompt_versions=m.PromptVersions(),
            config_version="test-v1",
        ),
    )
    return {
        "result": result.model_dump(mode="json"),
        "progress": {
            "subtasks": list(tasks),
            "completed_subtasks": list(completed),
            "evidence_ids": ["evidence-old"],
        },
    }


async def test_real_persisted_accepted_plan_supplies_only_unfinished_objectives(tmp_path):
    """删除 progress 中的已接受计划会丢失可交接待办，本例必须捕捉该遗漏。"""
    tasks = [
        {"subtask_id": "read", "objective": "读取当期收入", "search_query": "revenue"},
        {
            "subtask_id": "compare",
            "objective": "对比去年收入并复核单位",
            "search_query": "previous revenue",
            "depends_on": ["read"],
        },
    ]
    request = m.RunRequest(run_id="accepted-plan", task_id="mission-test", question="收入变化？")
    planner = ActionPlanner(
        [
            {"kind": "plan", "subtasks": tasks, "completed_subtask_ids": ["read"]},
            {"kind": "stop", "reason": "等待缺失的去年资料"},
        ]
    )
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    try:
        result = await run(
            request,
            data=DemoData(),
            content=FlexibleContent(),
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
            state_store=store,
        )
        assert result.status == "insufficient_evidence"
        lease = await store.acquire("accepted-plan")
        try:
            saved = await store.load(lease)
        finally:
            await store.release(lease)
        handoff = from_snapshot(saved.state)
        assert handoff.pending_items == ["对比去年收入并复核单位"]
        assert handoff.previous_run_id == "accepted-plan"
        assert handoff.truncated is False
        frozen = continuation("mission-real", 2, handoff.model_dump(mode="json"))
        assert frozen.pending_items == ["对比去年收入并复核单位"]
        assert frozen.artifact_refs == ["run:accepted-plan"]
        assert "evidence_ids" not in frozen.model_dump()
    finally:
        await store.aclose()


def test_shortening_a_single_pending_objective_marks_handoff_truncated():
    """待办字符数截断也必须显式标记，不能只检查摘要或条目数量。"""
    objective = "核" * 300
    handoff = from_snapshot(
        snapshot(tasks=[{"subtask_id": "check", "objective": objective, "search_query": "q"}])
    )
    assert handoff.pending_items == ["核" * 256]
    assert handoff.truncated is True


@pytest.mark.parametrize("text", ["核😀" * 2000, '\\"\n\t' * 2000])
def test_large_answer_and_pending_are_bounded_as_complete_utf8_json(text):
    """多字节与 JSON 转义均按完整编码计量，缩减不能损坏 Unicode。"""
    tasks = [
        {"subtask_id": f"check-{i}", "objective": "😀" * 256, "search_query": "q"} for i in range(8)
    ]
    handoff = from_snapshot(snapshot(answer=m.AnswerRecord(text=text), tasks=tasks))
    encoded = json.dumps(
        handoff.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert len(encoded) <= 4096
    assert handoff.truncated is True
    assert handoff.previous_run_id == "run-old"
    assert handoff.artifact_refs == ["run:run-old"]
    assert all(item == "😀" * 256 for item in handoff.pending_items)
    assert "evidence-old" not in encoded.decode("utf-8")


def test_continuation_identity_overhead_is_included_in_total_limit():
    """合法交接接近上限时，加入长身份应再缩减正文，不能产生超限请求。"""
    fields = {
        "summary": "",
        "pending_items": [],
        "previous_run_id": "run-old",
        "artifact_refs": ["run:run-old"],
        "truncated": False,
    }
    base_size = len(
        json.dumps(fields, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    )
    chinese, remainder = divmod(4096 - base_size, 3)
    fields["summary"] = "续" * chinese + "x" * remainder
    frozen = continuation("任" * 100, 100, fields)
    encoded = json.dumps(
        frozen.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert len(encoded) <= 4096
    assert frozen.mission_id == "任" * 100
    assert frozen.cycle == 100
    assert frozen.previous_run_id == "run-old"
    assert frozen.summary != fields["summary"]
    assert frozen.truncated is True
    assert fields["truncated"] is False


def test_pending_plan_is_not_replaced_by_generic_stop_hint():
    """已完成的计划项不进入新轮次；无未完成项时才回退到停止提示。"""
    task = {"subtask_id": "done", "objective": "已完成的工作", "search_query": "q"}
    handoff = from_snapshot(snapshot(tasks=[task], completed=["done"]))
    assert handoff.pending_items == ["原目标尚未完成；根据上轮停止原因重新检查缺失的数据和证据。"]
    assert handoff.truncated is False
