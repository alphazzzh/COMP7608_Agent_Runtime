"""持续任务交接的严格边界、真实控制器上下文与 SQLite 中断恢复。

所有场景使用本地测试端口与临时存储，不启动服务或请求真实模型。
历史交接只是规划线索，不能替代本轮 question、证据或观察身份。
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from lake_agent import models as m
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink
from lake_agent.validation import canonical_json
from tests.support.agent import ActionPlanner, FlexibleContent, MemoryTrace
from tests.support.ports import DemoData
from tests.support.recovery import (
    InterruptingTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
)


def continuation_fields(**updates):
    """返回独立的历史线索；引用故意属于上一轮，不能变成本轮证据。"""
    return {
        "mission_id": "mission-1",
        "cycle": 2,
        "previous_run_id": "previous-run",
        "summary": "上一轮发现收入候选，仍需本轮重新核验。",
        "pending_items": ["重新读取收入来源"],
        "artifact_refs": ["previous-run/artifacts/evidence-old.json"],
        "truncated": False,
        **updates,
    }


def test_plain_request_keeps_old_serialized_shape():
    """未提供或显式清空交接都保留旧字段集合，含嵌套 JSON 导出。"""
    expected = {"schema_version", "task_id", "question", "mode", "run_id", "config"}
    for fields in ({}, {"continuation": None}):
        request = m.RunRequest(task_id="ordinary", question="原始问题", **fields)
        assert set(request.model_dump()) == expected
        assert set(request.model_dump(mode="json")) == expected
        assert set(json.loads(request.model_dump_json())) == expected
        assert request.continuation is None


def test_continuation_round_trips_exact_fields_and_default_marker():
    """严格请求保留交接身份及列表，仅截断标记可省略并默认为假。"""
    fields = continuation_fields(previous_run_id=None)
    fields.pop("truncated")
    request = m.RunRequest(task_id="mission", question="问题", continuation=fields)
    assert isinstance(request.continuation, m.RunContinuation)
    expected = {**fields, "truncated": False}
    assert request.model_dump()["continuation"] == expected
    assert json.loads(request.model_dump_json())["continuation"] == expected
    assert m.RunRequest.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize(
    "updates",
    [
        {"mission_id": ""},
        {"mission_id": "m" * 101},
        {"mission_id": 1},
        {"mission_id": b"mission"},
        {"cycle": 0},
        {"cycle": -1},
        {"cycle": True},
        {"cycle": "2"},
        {"cycle": 2.0},
        {"cycle": float("nan")},
        {"cycle": float("inf")},
        {"previous_run_id": 7},
        {"summary": 1},
        {"summary": "s" * 2001},
        {"summary": "\ud800"},
        {"pending_items": ["p"] * 9},
        {"pending_items": ["p" * 257]},
        {"pending_items": ("p",)},
        {"pending_items": [7]},
        {"artifact_refs": ["a"] * 9},
        {"artifact_refs": ["a" * 201]},
        {"artifact_refs": "artifact"},
        {"artifact_refs": [float("nan")]},
        {"truncated": 1},
        {"truncated": "false"},
        {"evidence": [{"evidence_id": "old"}]},
    ],
)
def test_continuation_rejects_coercion_nonfinite_and_unbounded_fields(updates):
    """非严格类型、未知证据字段和逐字段越界均在运行前拒绝。"""
    with pytest.raises(ValidationError):
        m.RunContinuation.model_validate(continuation_fields(**updates))


def test_continuation_accepts_inclusive_character_and_item_limits():
    """字符与条目上限包含边界；总量预算另行限制其组合。"""
    fields = continuation_fields(
        mission_id="m" * 100,
        summary="s" * 2000,
        pending_items=["p"] * 8,
        artifact_refs=["a"] * 8,
    )
    accepted = m.RunRequest(task_id="mission", question="q", continuation=fields)
    assert accepted.continuation.model_dump() == fields
    for updates in ({"pending_items": ["p" * 256]}, {"artifact_refs": ["a" * 200]}):
        fields = continuation_fields(**updates)
        assert m.RunContinuation.model_validate(fields).model_dump() == fields


def test_continuation_counts_full_utf8_json_at_4096_byte_boundary():
    """按含身份与标记的规范 UTF-8 JSON 限额，不能按字符数或 ASCII 转义计数。"""
    fields = continuation_fields(
        mission_id="任" * 100,
        previous_run_id="上一轮",
        summary="",
        pending_items=[],
        artifact_refs=[],
    )
    empty_bytes = len(
        json.dumps(
            fields, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    )
    chinese, remainder = divmod(4096 - empty_bytes, 3)
    fields["summary"] = "续" * chinese + "x" * remainder
    assert len(fields["summary"]) <= 2000
    accepted = m.RunContinuation.model_validate(fields)
    assert len(canonical_json(accepted)) == 4096
    with pytest.raises(ValidationError):
        m.RunContinuation.model_validate({**fields, "summary": fields["summary"] + "x"})
    # 即使正文不变，身份增长也必须参与完整交接的总量校验。
    with pytest.raises(ValidationError):
        m.RunContinuation.model_validate({**fields, "previous_run_id": "上一轮x"})


async def test_real_run_passes_frozen_continuation_without_importing_evidence():
    """真实控制器交付冻结线索，不改问题、不导入证据且每次均服从上下文预算。"""
    expected = continuation_fields()
    request = m.RunRequest(
        task_id="mission",
        question="本轮原始问题？",
        continuation=expected,
        config=m.RunConfig(max_planner_context_bytes=1600, planner_preview_bytes=512),
    )

    def change_host_state(_):
        # 宿主在第一次派发后产生新摘要，不得改变已冻结的本轮输入。
        request.continuation.summary = "宿主已经生成更新的任务摘要"
        request.continuation.pending_items.append("下一轮的新待办")
        return {"kind": "compose"}

    planner = ActionPlanner([change_host_state, {"kind": "finish"}])
    content, trace = FlexibleContent(), MemoryTrace()
    result = await run(request, data=DemoData(), content=content, planner=planner, trace=trace)
    assert result.status == "completed", result
    assert result.answer.citations == []
    assert len(planner.requests) == 2
    for received in planner.requests:
        assert received.question == "本轮原始问题？"
        assert received.context["continuation"] == expected
        assert received.context["evidence_summary"] == []
        assert received.context["tool_observations"] == []
        assert len(canonical_json(received.context)) <= 1600
    assert content.compositions[0].question == "本轮原始问题？"
    assert content.compositions[0].evidence == []
    assert result.call_counts.tools == 0


async def test_plain_real_run_does_not_add_a_continuation_context_key():
    """普通运行不凭空增加规划上下文字段，保留既有控制器协议。"""
    planner = ActionPlanner([{"kind": "stop", "reason": "无需继续"}])
    await run(
        m.RunRequest(task_id="ordinary", question="问题"),
        data=DemoData(),
        content=FlexibleContent(),
        planner=planner,
        trace=MemoryTrace(),
    )
    assert len(planner.requests) == 1
    assert "continuation" not in planner.requests[0].context


async def test_continuation_budget_failure_happens_before_planner_dispatch():
    """不可丢失交接装不进原上下文限额时，不能后注入或额外派发模型。"""
    planner = ActionPlanner([{"kind": "stop", "reason": "不应派发"}])
    result = await run(
        m.RunRequest(
            task_id="mission",
            question="问题",
            continuation=continuation_fields(summary="续" * 300),
            config=m.RunConfig(max_planner_context_bytes=1024, planner_preview_bytes=256),
        ),
        data=DemoData(),
        content=FlexibleContent(),
        planner=planner,
        trace=MemoryTrace(),
    )
    assert (result.status, result.stop_reason) == ("budget_exceeded", "planner_context_limit")
    assert result.call_counts.planner == 0
    assert planner.requests == []


async def test_sqlite_resume_reuses_frozen_continuation_and_real_planner_journal(tmp_path):
    """中断后重开真实 SQLite，仅凭保存的请求与调用日志恢复，不读取宿主新摘要。"""
    expected = continuation_fields()
    request = m.RunRequest(
        run_id="continuation-run",
        task_id="mission",
        question="What was 2024 annual revenue?",
        continuation=expected,
        config=m.RunConfig(max_data_calls=2, max_model_calls=8, require_verification=True),
    )
    original_data, original_content = RecordingData(), RecordingContent()
    original_planner = RecordingPlanner()
    database, trace_root = tmp_path / "state.db", tmp_path / "traces"
    store = SqliteStateStore(database)
    try:
        failed = await run(
            request,
            data=original_data,
            content=original_content,
            planner=original_planner,
            trace=InterruptingTrace(
                trace_root, event_type="planner.decide.call.completed", fail=True
            ),
            state_store=store,
        )
        assert failed.stop_reason == "trace_write_failed"
        assert len(original_planner.requests) == 1
        lease = await store.acquire(request.run_id)
        try:
            saved = await store.load(lease)
        finally:
            await store.release(lease)
        assert saved.state["request"]["continuation"] == expected
        journal = [value for value in saved.calls.values() if value["phase"] == "planner.decide"]
        assert len(journal) == 1
        assert journal[0]["request"] == original_planner.requests[0].model_dump(mode="json")
        assert journal[0]["request"]["context"]["continuation"] == expected
    finally:
        await store.aclose()

    # 仅修改宿主对象并关闭旧实例；resume 只接收既有 run_id。
    request.continuation.summary = "新的宿主摘要不属于已领取的这一轮"
    request.continuation.artifact_refs.clear()
    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    reopened = SqliteStateStore(database)
    try:
        result = await resume(
            "continuation-run",
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
            state_store=reopened,
        )
        assert result.status == "completed", result
        assert len(original_planner.requests) + len(planner.requests) == 5
        assert original_planner.requests[0].call_id not in {
            received.call_id for received in planner.requests
        }
        assert all(received.context["continuation"] == expected for received in planner.requests)
        assert all(received.question == request.question for received in planner.requests)
        assert data.catalog_requests == []
        lease = await reopened.acquire("continuation-run")
        try:
            after = await reopened.load(lease)
        finally:
            await reopened.release(lease)
        assert after.state["request"]["continuation"] == expected
        planner_calls = [
            value for value in after.calls.values() if value["phase"] == "planner.decide"
        ]
        assert len(planner_calls) == 5
        assert all(
            value["request"]["context"]["continuation"] == expected for value in planner_calls
        )
    finally:
        await reopened.aclose()
