"""控制器验收测试覆盖可观测行为、预算与取消。"""

import asyncio
import json
from pathlib import Path

import pytest

from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.models import RunConfig, RunRequest
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink


def request(mode="full_agent", **config):
    return RunRequest(
        task_id="task-1", question="What was 2024 revenue?", mode=mode, config=RunConfig(**config)
    )


async def execute(tmp_path, req=None, *, data=None, content=None, planner=None, trace=None):
    return await run(
        req or request(),
        data=data or DemoData(),
        content=content or DemoContent(),
        planner=planner or DemoPlanner(),
        trace=trace or JsonlTraceSink(tmp_path),
    )


@pytest.mark.parametrize(
    "mode,reason,models",
    [
        ("fixed_retrieval", "fixed_retrieval_completed", 0),
        ("no_verifier", "planner_finished", 0),
        ("full_agent", "verified_supported", 0),
    ],
)
async def test_three_modes_persist_cited_answer(tmp_path, mode, reason, models):
    result = await execute(tmp_path, request(mode))
    assert result.status == "completed"
    assert result.stop_reason == reason
    assert result.answer.citations
    assert result.call_counts.model_calls == models  # 离线桩从不调用提供方。
    trace_text = await asyncio.to_thread(Path(result.trace_ref).read_text)
    events = [json.loads(line) for line in trace_text.splitlines()]
    assert [e["seq"] for e in events] == list(range(1, len(events) + 1))
    assert events[-1]["event_type"] == "run.completed"
    for event in events:
        if event.get("data_ref"):
            assert (tmp_path / result.run_id / "artifacts" / f"{event['data_ref']}.json").exists()


async def test_catalog_is_complete_before_registration(tmp_path):
    data = DemoData(page_size=1)
    result = await execute(tmp_path, data=data)
    assert result.status == "completed"
    assert result.call_counts.catalog == 2


async def test_catalog_limit_stops_before_content(tmp_path):
    result = await execute(tmp_path, request(max_catalog_sources=1))
    assert (result.status, result.stop_reason) == ("budget_exceeded", "catalog_limit")
    assert result.call_counts.content == 0


async def test_catalog_snapshot_mismatch(tmp_path):
    result = await execute(tmp_path, request(dataset_snapshot="absent"))
    assert result.stop_reason == "catalog_snapshot_mismatch"


async def test_catalog_pages_must_share_the_same_implicit_snapshot(tmp_path):
    class ChangingSnapshot(DemoData):
        async def catalog(self, req):
            result = await super().catalog(req)
            if req.cursor is not None:
                result = result.model_copy(update={"catalog_snapshot_id": "demo-v2"})
            return result

    result = await execute(tmp_path, data=ChangingSnapshot(page_size=1))
    assert (result.status, result.stop_reason) == ("failed", "catalog_snapshot_mismatch")
    assert result.call_counts.catalog == 2


async def test_data_budget_includes_catalog(tmp_path):
    result = await execute(tmp_path, request(max_data_calls=1))
    assert result.stop_reason == "data_call_budget"
    assert result.call_counts.catalog == 1
    assert result.call_counts.tools == 0


async def test_empty_search_is_successful_but_insufficient(tmp_path):
    class Empty(DemoContent):
        async def search(self, req):
            reply = await super().search(req)
            return reply.model_copy(
                update={"data": reply.data.model_copy(update={"candidates": []})}
            )

    result = await execute(tmp_path, request("fixed_retrieval"), content=Empty())
    assert result.stop_reason == "no_candidates"
    assert result.answer is None


async def test_retry_keeps_operation_and_changes_call_id(tmp_path):
    from lake_agent.models import ErrorInfo

    class Flaky(DemoData):
        attempts = []

        async def invoke(self, req):
            self.attempts.append(req)
            reply = await super().invoke(req)
            if len(self.attempts) == 1:
                return reply.model_copy(
                    update={
                        "status": "error",
                        "data": None,
                        "error": ErrorInfo(code="unavailable", message="temporary", retryable=True),
                    }
                )
            return reply

    data = Flaky()
    result = await execute(tmp_path, request("fixed_retrieval"), data=data)
    assert result.status == "completed"
    a, b = data.attempts[:2]
    assert a.operation_id == b.operation_id
    assert a.call_id != b.call_id
    assert [a.attempt, b.attempt] == [1, 2]
    assert result.call_counts.retries == 1


async def test_invalid_citation_never_becomes_answer(tmp_path):
    class Bad(DemoContent):
        async def compose(self, req):
            reply = await super().compose(req)
            return reply.model_copy(
                update={"data": reply.data.model_copy(update={"citations": ["fake"]})}
            )

    result = await execute(tmp_path, content=Bad())
    assert result.stop_reason == "invalid_module_response"
    assert result.answer is None


async def test_verifier_failure_preserves_answer_without_success(tmp_path):
    class Broken(DemoContent):
        async def verify(self, req):
            raise RuntimeError("failed verification")

    result = await execute(tmp_path, content=Broken())
    assert result.status == "failed"
    assert result.answer is not None


async def test_per_call_timeout(tmp_path):
    class Slow(DemoData):
        async def catalog(self, req):
            await asyncio.sleep(10)

    result = await execute(
        tmp_path, request(per_call_timeout_ms=10, max_retries_per_data_operation=0), data=Slow()
    )
    assert (result.status, result.stop_reason) == ("timeout", "call_timeout")


async def test_global_timeout(tmp_path):
    class Slow(DemoData):
        async def catalog(self, req):
            await asyncio.sleep(10)

    result = await execute(tmp_path, request(max_wall_time_ms=20), data=Slow())
    assert (result.status, result.stop_reason) == ("timeout", "time_budget")


async def test_cancel_propagates_and_no_adapter_task_left(tmp_path):
    entered, cancelled = asyncio.Event(), asyncio.Event()

    class Slow(DemoData):
        async def catalog(self, req):
            entered.set()
            try:
                await asyncio.sleep(10)
            finally:
                cancelled.set()

    task = asyncio.create_task(execute(tmp_path, data=Slow()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    events = [
        json.loads(line) for line in next(tmp_path.glob("*/events.jsonl")).read_text().splitlines()
    ]
    assert events[-1]["event_type"] == "run.cancelled"


async def test_duplicate_run_never_overwrites_trace(tmp_path):
    req = request().model_copy(update={"run_id": "same"})
    first = await execute(tmp_path, req)
    original = await asyncio.to_thread(Path(first.trace_ref).read_text)
    second = await execute(tmp_path, req)
    assert second.stop_reason == "trace_sink_unavailable"
    assert second.call_counts.catalog == 0
    assert await asyncio.to_thread(Path(first.trace_ref).read_text) == original


async def test_token_cap_rejected_before_external_calls(tmp_path):
    result = await execute(tmp_path, request(max_input_tokens=1000))
    assert result.status == "failed"
    assert result.stop_reason == "unsupported_token_budget"
    assert result.call_counts.catalog == 0


async def test_independent_concurrent_runs(tmp_path):
    data, content, planner = DemoData(), DemoContent(), DemoPlanner()
    results = await asyncio.gather(
        *[execute(tmp_path, data=data, content=content, planner=planner) for _ in range(3)]
    )
    assert all(r.status == "completed" for r in results)
    assert len({r.run_id for r in results}) == 3
    assert len({r.answer.citations[0] for r in results}) == 3
