"""Review regressions for ownership changes at web job lifecycle boundaries.

All assertions use the HTTP view of a real local run. Test-owned gates expose
the interval after an old task drains and before admission finishes; the core
runtime, journal, trace and repository operations remain in the execution path.
"""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest

pytest.importorskip("fastapi")

from test_web import InvokeGate, create_run, make_app, running_client, wait_stopped


async def test_old_cancel_cannot_overwrite_a_new_resume(tmp_path, monkeypatch):
    """A cancelled generation cannot settle metadata owned by its successor."""
    gate = InvokeGate(monkeypatch)
    app = make_app(tmp_path)
    service = app.state.demo_service
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            run_id = await create_run(client)
            await gate.wait_entered()
            settle_entered, allow_settle = asyncio.Event(), asyncio.Event()
            resume_entered, allow_resume = asyncio.Event(), asyncio.Event()
            original_settle = service._settle_unstarted
            original_acquire = service.state_store.acquire
            settle_held = False
            acquire_held = False

            async def held_settle(identity):
                nonlocal settle_held
                if not settle_held:
                    settle_held = True
                    settle_entered.set()
                    await allow_settle.wait()
                return await original_settle(identity)

            async def held_acquire(identity):
                nonlocal acquire_held
                if not acquire_held:
                    acquire_held = True
                    resume_entered.set()
                    await allow_resume.wait()
                return await original_acquire(identity)

            monkeypatch.setattr(service, "_settle_unstarted", held_settle)
            cancelled = asyncio.create_task(client.post(f"/api/runs/{run_id}/cancel"))
            resumed = None
            try:
                async with asyncio.timeout(4):
                    await settle_entered.wait()
                # The original runtime has already drained and released its
                # lease. Hold the real resume at its next acquisition boundary.
                monkeypatch.setattr(service.state_store, "acquire", held_acquire)
                cancel_serializes_admission = service.lock.locked()
                resumed = asyncio.create_task(client.post(f"/api/runs/{run_id}/resume"))
                if not cancel_serializes_admission:
                    # On the defective manager the successor can start while
                    # the old cancellation remains inside its settlement gate.
                    async with asyncio.timeout(4):
                        await resume_entered.wait()
                allow_settle.set()
                assert (await cancelled).status_code == 202
                assert (await resumed).status_code == 202
                async with asyncio.timeout(4):
                    await resume_entered.wait()
                view = (await client.get(f"/api/runs/{run_id}")).json()
                assert view["active"] is True
                assert view["status"] == "running", view
            finally:
                allow_settle.set()
                allow_resume.set()
                gate.release.set()
                await asyncio.gather(
                    *(task for task in (cancelled, resumed) if task is not None),
                    return_exceptions=True,
                )


async def test_shutdown_cannot_finish_before_an_admitted_create_spawns(tmp_path, monkeypatch):
    """An admission in progress is included in shutdown's owned job snapshot."""
    app = make_app(tmp_path)
    service = app.state.demo_service
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            save_entered, allow_save = asyncio.Event(), asyncio.Event()
            close_entered = asyncio.Event()
            original_save = service.repository.save
            held = False

            async def held_save(record):
                nonlocal held
                if not held and record["status"] == "queued":
                    held = True
                    save_entered.set()
                    await allow_save.wait()
                return await original_save(record)

            async def closing():
                close_entered.set()
                await service.close()

            monkeypatch.setattr(service.repository, "save", held_save)
            creation = asyncio.create_task(client.post("/api/runs", json={"question": "revenue?"}))
            shutdown = None
            try:
                async with asyncio.timeout(4):
                    await save_entered.wait()
                shutdown = asyncio.create_task(closing())
                async with asyncio.timeout(4):
                    await close_entered.wait()
                if service.closing:
                    # Without coordinated admission, shutdown sees no spawned
                    # jobs and closes the store while this POST is still held.
                    async with asyncio.timeout(4):
                        await shutdown
                allow_save.set()
                response = await creation
                assert response.status_code == 202, response.text
                await shutdown
                view = await wait_stopped(client, response.json()["run_id"])
                assert view["status"] == "interrupted", view
                assert view["active"] is view["can_resume"] is False
                assert view["error"] is None
            finally:
                allow_save.set()
                await asyncio.gather(
                    *(task for task in (creation, shutdown) if task is not None),
                    return_exceptions=True,
                )


async def test_history_uses_the_same_logical_trace_location_as_detail(tmp_path):
    """History cards cannot expose the operator's absolute filesystem path."""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        view = await wait_stopped(client, run_id)
        rows = (await client.get("/api/runs")).json()["runs"]
        location = rows[0]["result"]["trace_ref"]
        assert location == view["result"]["trace_ref"]
        assert not location.startswith("/")
        assert str(tmp_path) not in json.dumps(rows)


async def test_restart_reconciles_runs_older_than_the_history_display_limit(tmp_path):
    """The newest-100 display limit cannot hide stale active ownership forever."""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        await wait_stopped(client, run_id)

    manifest = tmp_path / "metadata" / f"{run_id}.json"
    record = json.loads(manifest.read_text())
    # Seed independent completed presentation records, avoiding 100 unnecessary
    # controller executions. The original remains backed by real core state.
    for index in range(100):
        identity = f"run-{uuid4().hex}"
        newer = {
            **record,
            "run_id": identity,
            "created_at": f"2026-11-02T00:00:{index // 2:02d}.{index:06d}+00:00",
            "attempts": [],
            "has_state": False,
            "result": {**record["result"], "run_id": identity, "trace_ref": None},
        }
        (manifest.parent / f"{identity}.json").write_text(json.dumps(newer))
    record.update(status="running", result=None, created_at="2000-01-01T00:00:00+00:00")
    manifest.write_text(json.dumps(record))

    async with running_client(tmp_path) as client:
        rows = (await client.get("/api/runs")).json()["runs"]
        assert len(rows) == 100
        assert run_id not in {row["run_id"] for row in rows}
        view = (await client.get(f"/api/runs/{run_id}")).json()
        assert view["status"] == "interrupted", view
        assert view["active"] is False
        assert view["can_resume"] is True
