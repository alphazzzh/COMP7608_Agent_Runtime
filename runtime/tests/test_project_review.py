"""Independent regressions for project adapter cleanup after durable completion.

The real offline core, SQLite journal, trace and HTTP view remain in the path.
Only the context owner's final cleanup fails or waits: a terminal core outcome
must remain available when that later resource lifecycle step is interrupted.
"""

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.web.app import create_app
from lake_agent.web.evaluations import EvaluationImport, EvaluationRepository
from lake_agent.web.profiles import ExecutionPorts, ExecutionProfile, ProfileMetadata
from lake_agent.web.storage import WebError


@pytest.mark.parametrize("cleanup", ["error", "cancel"])
async def test_adapter_cleanup_preserves_already_committed_terminal_result(tmp_path, cleanup):
    """Late cleanup cannot erase a completed answer or advertise resumable work."""
    cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def owned_ports():
        # Every attempt receives genuine fresh local adapters. The test gate
        # belongs only to teardown, after run() has durably returned its result.
        try:
            yield ExecutionPorts(data=DemoData(), content=DemoContent(), planner=DemoPlanner())
        finally:
            cleanup_entered.set()
            if cleanup == "error":
                raise RuntimeError("test adapter cleanup failed")
            await cleanup_release.wait()

    profile = ExecutionProfile(
        metadata=ProfileMetadata(
            profile_id="review-project",
            title="Review project",
            description="Offline adapter teardown regression.",
            kind="project",
            dataset_id="review-data",
            dataset_name="Review data",
            revision="review-v1",
            modules={
                name: {"status": "connected", "label": "Local review adapter"}
                for name in ("data", "planner", "content")
            },
        ),
        ports_factory=owned_ports,
    )
    app = create_app(data_dir=tmp_path, profiles=(profile,))
    service = app.state.demo_service
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            response = await client.post(
                "/api/runs",
                json={
                    "question": "What was 2024 annual revenue?",
                    "profile_id": "review-project",
                },
            )
            assert response.status_code == 202, response.text
            run_id = response.json()["run_id"]
            try:
                async with asyncio.timeout(5):
                    await cleanup_entered.wait()
                # Verify the durable outcome before triggering cancellation;
                # this distinguishes late cleanup from an unfinished task.
                lease = await service.state_store.acquire(run_id)
                try:
                    saved = await service.state_store.load(lease)
                    assert saved.state["result"]["status"] == "completed"
                finally:
                    await service.state_store.release(lease)
                if cleanup == "cancel":
                    cancelled = await client.post(f"/api/runs/{run_id}/cancel")
                    assert cancelled.status_code == 202, cancelled.text
                async with asyncio.timeout(5):
                    while True:
                        view = (await client.get(f"/api/runs/{run_id}")).json()
                        if not view["active"]:
                            break
                        await asyncio.sleep(0.01)
                assert view["result"] is not None, view
                assert view["result"]["status"] == view["status"] == "completed"
                assert "120 million HKD" in view["result"]["answer"]["text"]
                assert view["can_resume"] is False
            finally:
                cleanup_release.set()


async def test_repository_does_not_publish_a_report_larger_than_its_read_limit(tmp_path):
    """A direct import cannot create a valid record that poisons all later reads."""
    repository = EvaluationRepository(tmp_path)
    await repository.start()
    # Python accepts arbitrarily long fractional seconds. The HTTP limit is
    # independent of the public repository's promise to create readable files.
    value = {
        "title": "Oversize timestamp",
        "dataset_id": "review-data",
        "task_set_id": "review-tasks",
        "model_id": "offline-demo",
        "evaluator": "Review author",
        "evaluated_at": "2026-11-02T08:30:00." + "1" * 300_000 + "+08:00",
        "results": [{"mode": "full_agent", "task_count": 1}],
    }
    # Either the strict import model or the repository may reject the size;
    # neither may publish an unreadable file and report a successful create.
    with pytest.raises((ValueError, WebError)):
        await repository.create(EvaluationImport.model_validate(value))
    assert await repository.listing() == []
    assert list((tmp_path / "evaluations").iterdir()) == []
