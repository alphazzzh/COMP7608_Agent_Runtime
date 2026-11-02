"""Regression tests for provider accounting at caller-cancellation boundaries.

An interrupted adapter has unknown provider usage; a valid returned response
settles its declared usage exactly once even if later trace I/O is cancelled.
"""

import asyncio

import pytest

from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.models import RunRequest, Usage
from lake_agent.runtime import run


class CancellationTrace:
    """In-memory committed trace with explicit event-driven cancellation gates."""

    def __init__(self, entered, *, block_return=False):
        self.entered = entered
        self.block_return = block_return
        self.events = []
        self.closed = False

    async def create_run(self, run_id):
        self.run_id = run_id

    async def put_artifact(self, ref, value):
        pass

    async def append(self, event):
        self.events.append(event)
        if self.block_return and event.event_type == "planner.decompose.call.completed":
            self.entered.set()
            await asyncio.Future()

    async def close(self):
        self.closed = True
        return "memory-trace"


@pytest.mark.parametrize("reported_requests", [None, 0, 1])
async def test_cancelled_provider_usage_settles_once(reported_requests):
    """Unknown dispatch is charged once; known local/model usage is preserved."""
    entered = asyncio.Event()

    class Planner(DemoPlanner):
        async def decompose(self, request):
            if reported_requests is None:
                entered.set()
                await asyncio.Future()
            reply = await super().decompose(request)
            usage = (
                Usage.local()
                if reported_requests == 0
                else Usage(provider_requests=1, input_tokens=7, output_tokens=3)
            )
            return reply.model_copy(update={"usage": usage})

    trace = CancellationTrace(entered, block_return=reported_requests is not None)
    task = asyncio.create_task(
        run(
            RunRequest(task_id="cancel-accounting", question="Synthetic annual revenue?"),
            data=DemoData(),
            content=DemoContent(),
            planner=Planner(),
            trace=trace,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    event = trace.events[-1]
    assert event.event_type == "run.cancelled"
    expected = 1 if reported_requests is None else reported_requests
    assert event.detail["call_counts"]["model_calls"] == expected
    active = event.detail["active_call"]
    assert active["phase"] == "planner.decompose"
    assert active["call_id"]
    assert active["outcome"] == ("unknown" if reported_requests is None else "returned")
    assert trace.closed
