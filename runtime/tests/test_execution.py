"""原始调用尝试与稳定就绪批次选择的离线回归测试。

此处的适配器均为本地协程。事件关卡在不访问网络的情况下触发取消；
选择器的预期结果使用手动排序且相互独立的步骤。
"""

from __future__ import annotations

import asyncio

import pytest

from lake_agent import models as m
from lake_agent.execution import ToolAttempt, invoke_tool_attempt, select_ready_wave


def attempt(*, timeout=1.0):
    """构造一个具有授权请求形式的请求，不依赖控制器或审计接收器。"""
    return ToolAttempt(
        request=m.ToolRequest(
            run_id="run-execution",
            task_id="task-execution",
            operation_id="op-execution",
            call_id="call-execution",
            step_id="step-execution",
            timeout_ms=1000,
            max_output_bytes=4096,
            catalog_snapshot_id="snapshot-execution",
            tool_name="extract",
            args={"source_id": "source-a"},
        ),
        deadline=asyncio.get_running_loop().time() + timeout,
    )


def spec(name="extract", *, parallel_safe=True, uses_sources=True, keys=()):
    """为测试工具注册可信的安全能力与共享资源声明。"""
    return m.ToolSpec(
        name=name,
        description="Local execution fixture",
        input_schema={"type": "object"},
        idempotent=True,
        parallel_safe=parallel_safe,
        uses_sources=uses_sources,
        source_id_paths=["/source_id"] if uses_sources else [],
        exclusive_resource_keys=list(keys),
    )


def step(name, source="source-a", *, tool="extract", depends_on=()):
    """创建身份稳定的计划条目；选择器接收的是已经通过校验的计划。"""
    return m.ToolStep(
        step_id=name,
        subtask_id="subtask-execution",
        tool_name=tool,
        source_ids=[source] if source is not None else [],
        args={"source_id": source} if source is not None else {},
        depends_on=list(depends_on),
        purpose="Read an independent fixture source",
    )


@pytest.mark.parametrize("failed", [False, True])
async def test_valid_tool_reply_is_returned_without_controller_policy(failed):
    current = attempt()

    async def invoke(request):
        assert current.dispatched
        assert request is current.request
        if failed:
            return m.ToolResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(code="bad_query", message="Adjust input", recoverable=True),
            )
        return m.ToolResult(call_id=request.call_id, data={"value": 7})

    outcome = await invoke_tool_attempt(current, invoke, max_output_bytes=4096)

    assert outcome.kind == "response"
    assert outcome.response.status == ("error" if failed else "ok")
    assert outcome.duration_ms >= 0
    assert current.outcome is outcome
    assert current.started_at is not None


async def test_expired_deadline_never_dispatches_the_adapter():
    current = attempt(timeout=-1)

    async def forbidden_dispatch(request):
        raise AssertionError("Expired attempts must not reach the adapter")

    outcome = await invoke_tool_attempt(current, forbidden_dispatch, max_output_bytes=4096)

    assert outcome.kind == "timeout"
    assert outcome.duration_ms == 0
    assert not current.dispatched
    assert current.outcome is outcome


async def test_running_attempt_times_out_and_observes_adapter_cancellation():
    current = attempt(timeout=0.01)
    cancelled = asyncio.Event()

    async def blocked(request):
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    outcome = await invoke_tool_attempt(current, blocked, max_output_bytes=4096)

    assert outcome.kind == "timeout"
    assert current.dispatched
    assert cancelled.is_set()
    assert current.outcome is outcome


async def test_adapter_suppressing_deadline_cancellation_cannot_return_late_success():
    current = attempt(timeout=0.01)
    cancelled = asyncio.Event()

    async def late_reply(request):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
        return m.ToolResult(call_id=request.call_id, data={"late": True})

    outcome = await invoke_tool_attempt(current, late_reply, max_output_bytes=4096)

    assert cancelled.is_set()
    assert outcome.kind == "timeout"
    assert outcome.response is None
    assert current.outcome is outcome


async def test_caller_cancellation_propagates_without_inventing_an_outcome():
    current = attempt()
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def blocked(request):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    task = asyncio.create_task(invoke_tool_attempt(current, blocked, max_output_bytes=4096))
    async with asyncio.timeout(1):
        await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert cancelled.is_set()
    assert current.dispatched
    assert current.outcome is None


async def test_adapter_cannot_turn_suppressed_caller_cancellation_into_success():
    current = attempt()
    entered = asyncio.Event()

    async def suppress_cancel(request):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            return m.ToolResult(call_id=request.call_id, data={"late": True})

    task = asyncio.create_task(invoke_tool_attempt(current, suppress_cancel, max_output_bytes=4096))
    async with asyncio.timeout(1):
        await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert current.dispatched
    assert current.outcome is None


@pytest.mark.parametrize(
    "invalid",
    [
        {"call_id": "another-call", "data": {"value": 7}},
        {"call_id": "call-execution", "data": {"value": float("nan")}},
        {"call_id": "call-execution", "data": {"value": 7}, "duration_ms": "1"},
        m.ToolResult(call_id="call-execution", data={"value": 7}).model_copy(
            update={"call_id": "another-call"}
        ),
    ],
)
async def test_invalid_json_envelopes_and_call_ids_are_rejected(invalid):
    current = attempt()

    async def malformed(request):
        return invalid

    outcome = await invoke_tool_attempt(current, malformed, max_output_bytes=4096)

    assert outcome.kind == "invalid_response"
    assert outcome.response is None
    assert outcome.error_type in {"ValueError", "ValidationError"}
    assert current.outcome is outcome


async def test_output_limit_counts_serialized_utf8_bytes():
    current = attempt()

    async def oversized(request):
        return m.ToolResult(call_id=request.call_id, data={"text": "界" * 100})

    outcome = await invoke_tool_attempt(current, oversized, max_output_bytes=250)

    assert outcome.kind == "invalid_response"
    assert outcome.error_type == "ValueError"
    assert current.outcome is outcome


async def test_adapter_exception_captures_only_its_type():
    current = attempt()

    async def broken(request):
        raise RuntimeError("PRIVATE ADAPTER MESSAGE")

    outcome = await invoke_tool_attempt(current, broken, max_output_bytes=4096)

    assert outcome.kind == "exception"
    assert outcome.error_type == "RuntimeError"
    assert "PRIVATE" not in repr(outcome)
    assert current.outcome is outcome


def test_ready_wave_respects_dependency_success_and_the_parallel_limit():
    first = step("first", "source-a")
    child = step("child", "source-b", depends_on=["first"])
    third = step("third", "source-c")
    pending, succeeded = [first, child, third], set()
    specs = {"extract": spec()}

    assert select_ready_wave(pending, succeeded, specs, 2) == [first, third]
    assert select_ready_wave(pending, succeeded, specs, 1) == [first]
    assert select_ready_wave([child, third], {"first"}, specs, 2) == [child, third]
    assert pending == [first, child, third]
    assert succeeded == set()


def test_ready_wave_stops_at_first_source_conflict():
    first = step("first", "source-a")
    conflict = step("conflict", "source-a")
    later = step("later", "source-b")

    assert select_ready_wave([first, conflict, later], set(), {"extract": spec()}, 4) == [first]


def test_ready_wave_stops_at_first_shared_resource_conflict():
    first = step("first", "source-a", tool="query")
    conflict = step("conflict", "source-b", tool="inspect")
    later = step("later", "source-c", tool="extract")
    specs = {
        "query": spec("query", keys=["shared-db"]),
        "inspect": spec("inspect", keys=["shared-db"]),
        "extract": spec(),
    }

    assert select_ready_wave([first, conflict, later], set(), specs, 4) == [first]


def test_different_resources_and_sources_can_share_a_wave():
    first = step("first", "source-a", tool="query")
    second = step("second", "source-b", tool="inspect")
    specs = {
        "query": spec("query", keys=["database-a"]),
        "inspect": spec("inspect", keys=["database-b"]),
    }

    assert select_ready_wave([first, second], set(), specs, 4) == [first, second]


@pytest.mark.parametrize("source_less", [False, True])
def test_unsafe_or_source_less_entry_is_a_serial_barrier(source_less):
    first = step("first", "source-a")
    barrier = step("barrier", None if source_less else "source-b", tool="barrier")
    later = step("later", "source-c")
    specs = {
        "extract": spec(),
        "barrier": spec("barrier", parallel_safe=source_less, uses_sources=not source_less),
    }

    assert select_ready_wave([first, barrier, later], set(), specs, 4) == [first]
    assert select_ready_wave([barrier, later], set(), specs, 4) == [barrier]


def test_unready_unsafe_barrier_prevents_later_work_from_overtaking_it():
    first = step("first", "source-a")
    barrier = step("barrier", "source-b", tool="unsafe", depends_on=["first"])
    later = step("later", "source-c")
    specs = {"extract": spec(), "unsafe": spec("unsafe", parallel_safe=False)}

    assert select_ready_wave([first, barrier, later], set(), specs, 4) == [first]
    assert select_ready_wave([barrier, later], set(), specs, 4) == []


@pytest.mark.parametrize("limit", [0, -1])
def test_no_slots_produces_no_dispatch(limit):
    assert select_ready_wave([step("first")], set(), {"extract": spec()}, limit) == []
