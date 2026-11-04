"""全新端口实例下的已知响应重放、未知结果拒绝与累计预算。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from lake_agent import models as m
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink
from tests.support.recovery import (
    InterruptingTrace,
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    cancel_at_gate,
    recovery_api,
    request,
    trace_events,
)


@pytest.mark.parametrize("event_type", ["tool.observed", "evidence.added"])
async def test_resume_reconstructs_known_replies_without_redispatch(tmp_path, event_type):
    """控制器内存丢失后，不能重复已经成功的外部工具调用。"""
    resume, Store, _ = recovery_api()
    state_path, trace_root = tmp_path / "state.db", tmp_path / "traces"
    old_data, old_content, old_planner = RecordingData(), RecordingContent(), RecordingPlanner()
    interrupted = InterruptingTrace(trace_root, event_type=event_type)
    store = Store(state_path)
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=old_data,
                content=old_content,
                planner=old_planner,
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
    finally:
        await store.aclose()
    original_paths = sorted(trace_root.rglob("events.jsonl"))
    assert len(original_paths) == 1
    original_bytes = original_paths[0].read_bytes()
    assert len(old_data.catalog_requests) == len(old_data.tool_requests) == 1
    assert len(old_planner.requests) == 2

    data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
    restarted = Store(state_path)
    try:
        result = await resume(
            "recoverable-run",
            state_store=restarted,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(trace_root),
        )
    finally:
        await restarted.aclose()
    assert (result.run_id, result.status, result.stop_reason) == (
        "recoverable-run",
        "completed",
        "agent_finished",
    )
    assert data.catalog_requests == data.tool_requests == []
    assert len(planner.requests) == 3
    assert len(content.preparations) == 1  # 显式重新绑定本地临时句柄。
    assert content.searches == []  # 初始检索已缓存，无需强制再次搜索。
    assert len(content.compositions) == len(content.verifications) == 1
    assert (result.call_counts.catalog, result.call_counts.tools, result.call_counts.steps) == (
        1,
        1,
        1,
    )
    assert (result.call_counts.planner, result.call_counts.model_calls) == (5, 5)
    assert result.call_counts.content == 5  # prepare/search/compose/verify，加上一次重新绑定。
    assert (result.usage.input_tokens, result.usage.output_tokens) == (35, 15)
    evidence = content.compositions[0].evidence
    assert len(evidence) == 1
    assert evidence[0].call_id == old_data.tool_requests[0].call_id
    assert result.answer.citations == [evidence[0].evidence_id]
    if event_type == "evidence.added":
        assert interrupted.interrupted_event.data_ref == evidence[0].evidence_id

    assert original_paths[0].read_bytes() == original_bytes
    assert Path(result.trace_ref) != original_paths[0]
    assert len(list(trace_root.rglob("events.jsonl"))) == 2
    events = trace_events(result.trace_ref)
    assert events[0]["event_type"] == "run.resumed"
    assert events[-1]["event_type"] == "run.completed"
    assert all(event["run_id"] == result.run_id for event in events)
    assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
    cached = [
        event
        for event in events
        if event["event_type"].endswith(".call.completed") and event["detail"].get("replayed")
    ]
    assert {event["event_type"] for event in cached} >= {
        "catalog.call.completed",
        "content.prepare_catalog.call.completed",
        "planner.decide.call.completed",
        "content.search.call.completed",
        "tool.call.completed",
    }
    assert {
        event["call_id"] for event in cached if event["event_type"] == "tool.call.completed"
    } == {old_data.tool_requests[0].call_id}
    for event in events:
        if event["data_ref"]:
            assert (
                Path(result.trace_ref).parent / "artifacts" / f"{event['data_ref']}.json"
            ).exists()


async def test_resume_refuses_unknown_tool_outcome_even_when_idempotent(tmp_path):
    """幂等性允许正常重试，但无法判定已丢失的调用结果。"""
    resume, Store, StateStoreError = recovery_api()
    store = Store(tmp_path / "state.db")
    first = RecordingData(hold_tool=True)
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=first,
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=JsonlTraceSink(tmp_path / "traces"),
                state_store=store,
            )
        )
        await cancel_at_gate(task, first.entered)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        with pytest.raises(StateStoreError) as unknown:
            await resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        assert unknown.value.code == "recovery_outcome_unknown"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


@pytest.mark.parametrize("data_limit", [1, 2])
async def test_resume_returns_stored_terminal_result_without_calls(tmp_path, data_limit):
    """成功运行与正常因预算停止的运行都属于终结状态快照。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request().model_copy(update={"config": m.RunConfig(max_data_calls=data_limit)})
    try:
        result = await run(
            req,
            data=RecordingData(),
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=JsonlTraceSink(tmp_path / "traces"),
            state_store=store,
        )
        assert result.status == ("completed" if data_limit == 2 else "budget_exceeded")
        for _ in range(2):
            data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
            recovered = await resume(
                result.run_id,
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
            assert recovered.model_dump(mode="json") == result.model_dump(mode="json")
            assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


async def test_resume_rejects_changed_trusted_tool_contract_before_calls(tmp_path):
    """缓存计划不能静默继承已经变化的工具能力元数据。"""
    resume, Store, StateStoreError = recovery_api()
    store = Store(tmp_path / "state.db")
    interrupted = InterruptingTrace(tmp_path / "traces")
    try:
        task = asyncio.create_task(
            run(
                request(),
                data=RecordingData(),
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        data, content, planner = (
            RecordingData(contract_revision=2),
            RecordingContent(),
            RecordingPlanner(),
        )
        with pytest.raises(StateStoreError) as changed:
            await resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        assert changed.value.code == "recovery_contract_mismatch"
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


async def test_resume_preserves_original_deadline_during_downtime(tmp_path):
    """重启进程不能重新获得一份完整的总运行时间额度。"""
    resume, Store, StateStoreError = recovery_api()
    store = Store(tmp_path / "state.db")
    interrupted = InterruptingTrace(tmp_path / "traces")
    try:
        task = asyncio.create_task(
            run(
                request(max_wall_time_ms=1000),
                data=RecordingData(),
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        await asyncio.sleep(1.05)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        try:
            result = await resume(
                "recoverable-run",
                state_store=store,
                data=data,
                content=content,
                planner=planner,
                trace=JsonlTraceSink(tmp_path / "traces"),
            )
        except StateStoreError as expired:
            assert expired.code == "recovery_deadline_expired"
        else:
            assert (result.status, result.stop_reason) == ("timeout", "time_budget")
        assert_no_port_calls(data, content, planner)
    finally:
        await store.aclose()


async def test_resume_keeps_consumed_model_budget(tmp_path):
    """重放已付费的回复时，仍须计入原始运行的模型额度。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    interrupted = InterruptingTrace(tmp_path / "traces")
    req = request().model_copy(update={"config": m.RunConfig(max_model_calls=3, max_data_calls=2)})
    try:
        task = asyncio.create_task(
            run(
                req,
                data=RecordingData(),
                content=RecordingContent(),
                planner=RecordingPlanner(),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        result = await resume(
            req.run_id,
            state_store=store,
            data=data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert (result.status, result.stop_reason) == ("budget_exceeded", "model_call_budget")
        assert result.call_counts.model_calls == result.call_counts.planner == 3
        assert (result.call_counts.catalog, result.call_counts.tools) == (1, 1)
        assert data.catalog_requests == data.tool_requests == []
        assert len(planner.requests) == 1
        assert len(content.preparations) == 1
        assert content.compositions == content.verifications == []
    finally:
        await store.aclose()


async def test_audit_failure_does_not_repeat_already_returned_tool(tmp_path):
    """必须先持久化回复，再进行可能失败的观测审计写入。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    data = RecordingData()
    interrupted = InterruptingTrace(tmp_path / "traces", event_type="tool.observed", fail=True)
    try:
        stopped = await run(
            request(),
            data=data,
            content=RecordingContent(),
            planner=RecordingPlanner(),
            trace=interrupted,
            state_store=store,
        )
        assert stopped.stop_reason == "trace_write_failed"
        assert len(data.tool_requests) == 1
        fresh_data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        result = await resume(
            stopped.run_id,
            state_store=store,
            data=fresh_data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "completed"
        assert fresh_data.catalog_requests == fresh_data.tool_requests == []
        assert content.compositions[0].evidence[0].call_id == data.tool_requests[0].call_id
        assert result.call_counts.tools == 1
    finally:
        await store.aclose()


async def test_parallel_reverse_completion_reuses_every_known_reply(tmp_path):
    """首次串行审计期间崩溃，不能抹掉同批已返回任务的回复。"""
    resume, Store, _ = recovery_api()
    store = Store(tmp_path / "state.db")
    req = request().model_copy(
        update={
            "config": m.RunConfig(max_data_calls=3, max_model_calls=8, require_verification=True)
        }
    )
    data = RecordingData(parallel=True)
    interrupted = InterruptingTrace(tmp_path / "traces", event_type="tool.observed")
    try:
        task = asyncio.create_task(
            run(
                req,
                data=data,
                content=RecordingContent(),
                planner=RecordingPlanner(two_tools=True),
                trace=interrupted,
                state_store=store,
            )
        )
        await cancel_at_gate(task, interrupted.entered)
        assert data.returned_sources == ["demo-quarter", "demo-annual"]
        fresh_data = RecordingData(parallel=True)
        content, planner = RecordingContent(), RecordingPlanner(two_tools=True)
        result = await resume(
            req.run_id,
            state_store=store,
            data=fresh_data,
            content=content,
            planner=planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
        assert result.status == "completed"
        assert fresh_data.catalog_requests == fresh_data.tool_requests == []
        assert (result.call_counts.catalog, result.call_counts.tools, result.call_counts.steps) == (
            1,
            2,
            2,
        )
        evidence = content.compositions[0].evidence
        assert {item.source_id for item in evidence} == {"demo-annual", "demo-quarter"}
        assert {item.call_id for item in evidence} == {req.call_id for req in data.tool_requests}
        assert len(evidence) == 2
    finally:
        await store.aclose()
