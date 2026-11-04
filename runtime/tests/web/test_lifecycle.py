"""任务删除同步清理 SQLite，以及并发任务和工作台关闭的资源边界。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from uuid import uuid4

import httpx
import pytest

from lake_agent.state import SqliteStateStore, StateStoreError
from lake_agent.web.storage import WebError
from tests.support.web import InvokeGate, create_run, make_app, running_client, wait_stopped


def state_counts(root, run_id):
    """直接检查两张持久表，避免以 HTTP 隐藏记录代替真正删除。"""
    with sqlite3.connect(root / "state.sqlite3") as connection:
        return tuple(
            connection.execute(
                f"SELECT count(*) FROM {table} WHERE run_id=?", (run_id,)
            ).fetchone()[0]
            for table in ("run_states", "call_records")
        )


async def test_delete_removes_selected_task_files_sqlite_and_public_citations(tmp_path):
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        view = await wait_stopped(client, run_id)
        other_id = await create_run(client, question="A separate question")
        other = await wait_stopped(client, other_id)
        other_counts = state_counts(tmp_path, other_id)
        event = next(item for item in view["events"] if item["data_ref"])
        artifact = f"/api/runs/{run_id}/artifacts/{event['attempt_id']}/{event['data_ref']}"
        lock = (
            tmp_path
            / "state.sqlite3.locks"
            / (hashlib.sha256(run_id.encode()).hexdigest() + ".lock")
        )
        inode = lock.stat().st_ino
        assert state_counts(tmp_path, run_id)[0] == 1
        assert state_counts(tmp_path, run_id)[1] > 0

        response = await client.delete(f"/api/runs/{run_id}")
        assert response.status_code == 204, response.text
        assert response.content == b""
        assert state_counts(tmp_path, run_id) == (0, 0)
        assert not (tmp_path / "metadata" / f"{run_id}.json").exists()
        assert not (tmp_path / "traces" / run_id).exists()
        assert lock.stat().st_ino == inode
        for path in (
            f"/api/runs/{run_id}",
            artifact,
            f"/api/runs/{run_id}/export",
            f"/api/runs/{run_id}/report",
        ):
            assert (await client.get(path)).status_code == 404
        assert (await client.post(f"/api/runs/{run_id}/resume")).status_code == 404
        assert (await client.delete(f"/api/runs/{run_id}")).status_code == 404
        assert [item["run_id"] for item in (await client.get("/api/runs")).json()["runs"]] == [
            other_id
        ]
        assert (await client.get(f"/api/runs/{other_id}")).json()["result"] == other["result"]
        assert state_counts(tmp_path, other_id) == other_counts

    store = SqliteStateStore(tmp_path / "state.sqlite3")
    try:
        with pytest.raises(StateStoreError) as caught:
            await store.acquire(run_id)
        assert caught.value.code == "run_not_found"
    finally:
        await store.aclose()


async def test_missing_task_delete_is_404_and_foreign_origin_is_rejected(tmp_path):
    async with running_client(tmp_path) as client:
        path = "/api/runs/run-" + "0" * 32
        assert (await client.delete(path)).status_code == 404
        assert (await client.delete("/api/runs/not-a-run")).status_code == 404
        assert (
            await client.delete(path, headers={"origin": "https://foreign.example"})
        ).status_code == 403


async def test_state_delete_refuses_owned_run_and_preserves_lock_inode(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    rival = SqliteStateStore(tmp_path / "state.sqlite3")
    lease = await store.create("selected", {"task": "selected"})
    await store.begin_call(lease, "call", {"status": "started"})
    lock = tmp_path / "state.sqlite3.locks" / (hashlib.sha256(b"selected").hexdigest() + ".lock")
    inode = lock.stat().st_ino
    try:
        for backend in (store, rival):
            with pytest.raises(StateStoreError) as caught:
                await backend.delete("selected")
            assert caught.value.code == "lease_conflict"
        assert state_counts(tmp_path, "selected") == (1, 1)
        await store.release(lease)
        await rival.delete("selected")
        assert state_counts(tmp_path, "selected") == (0, 0)
        assert lock.stat().st_ino == inode
        # 中断后的删除可重复完成，不需要重建已删除的状态。
        await rival.delete("selected")
    finally:
        await rival.aclose()
        await store.aclose()


async def test_state_delete_rolls_back_call_cleanup_if_state_removal_fails(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    lease = await store.create("selected", {})
    await store.begin_call(lease, "call", {"status": "started"})
    await store.release(lease)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "CREATE TRIGGER reject_removal BEFORE DELETE ON run_states "
            "BEGIN SELECT RAISE(ABORT, 'controlled deletion failure'); END"
        )
    try:
        with pytest.raises(StateStoreError) as caught:
            await store.delete("selected")
        assert caught.value.code == "storage_error"
        assert state_counts(tmp_path, "selected") == (1, 1)
    finally:
        await store.aclose()


async def test_active_task_must_stop_before_it_can_be_deleted(tmp_path, monkeypatch):
    gate = InvokeGate(monkeypatch)
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        await gate.wait_entered()
        try:
            response = await client.delete(f"/api/runs/{run_id}")
            assert response.status_code == 409, response.text
            assert response.json()["detail"]["code"] == "run_active"
            assert (tmp_path / "metadata" / f"{run_id}.json").exists()
            assert (await client.post(f"/api/runs/{run_id}/cancel")).status_code == 202
            assert (await client.delete(f"/api/runs/{run_id}")).status_code == 204
            assert state_counts(tmp_path, run_id) == (0, 0)
        finally:
            gate.release.set()


async def test_external_execution_lease_prevents_delete_without_hiding_task(tmp_path):
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        original = await wait_stopped(client, run_id)
        rival = SqliteStateStore(tmp_path / "state.sqlite3")
        lease = await rival.acquire(run_id)
        try:
            response = await client.delete(f"/api/runs/{run_id}")
            assert response.status_code == 409, response.text
            assert response.json()["detail"]["code"] == "run_active"
            assert (await client.get(f"/api/runs/{run_id}")).json()["result"] == original["result"]
            assert not list((tmp_path / ".deletions").iterdir())
            assert state_counts(tmp_path, run_id)[0] == 1
        finally:
            await rival.release(lease)
            await rival.aclose()
        assert (await client.delete(f"/api/runs/{run_id}")).status_code == 204


@pytest.mark.parametrize("nested", [False, True], ids=["run-root-link", "nested-link"])
async def test_delete_never_traverses_external_symlink_targets(tmp_path, nested):
    external = tmp_path / "external"
    external.mkdir()
    sentinel = external / "must-survive.txt"
    sentinel.write_text("external data")
    async with running_client(tmp_path / "workspace") as client:
        root = tmp_path / "workspace"
        run_id = await create_run(client)
        await wait_stopped(client, run_id)
        traces = root / "traces" / run_id
        if nested:
            (traces / "external-link").symlink_to(external, target_is_directory=True)
        else:
            traces.rename(root / "original-selected-traces")
            traces.symlink_to(external, target_is_directory=True)
        response = await client.delete(f"/api/runs/{run_id}")
        assert response.status_code == (204 if nested else 503), response.text
        assert sentinel.read_text() == "external data"
        if not nested:
            assert state_counts(root, run_id)[0] == 1
            assert (root / "metadata" / f"{run_id}.json").exists()


@pytest.mark.parametrize("failure", ["sqlite", "files"])
async def test_pending_delete_is_hidden_and_completed_after_service_restart(
    tmp_path, monkeypatch, failure
):
    app = make_app(tmp_path)
    service = app.state.run_service

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            run_id = await create_run(client)
            await wait_stopped(client, run_id)
            with monkeypatch.context() as fault:
                if failure == "sqlite":

                    async def refuse(_run_id):
                        raise StateStoreError("storage_error")

                    fault.setattr(service.state_store, "delete", refuse)
                else:

                    def refuse(_run_id):
                        raise OSError("controlled cleanup failure")

                    fault.setattr(service.repository, "_finish_delete", refuse)
                response = await client.delete(f"/api/runs/{run_id}")
                assert response.status_code == 503, response.text
            assert (await client.get(f"/api/runs/{run_id}")).status_code == 404
            assert (await client.get("/api/runs")).json() == {"runs": []}
            assert (tmp_path / ".deletions" / f"{run_id}.delete").is_file()
            assert state_counts(tmp_path, run_id)[0] == (1 if failure == "sqlite" else 0)
    async with running_client(tmp_path) as client:
        assert (await client.get(f"/api/runs/{run_id}")).status_code == 404
        assert state_counts(tmp_path, run_id) == (0, 0)
        assert not (tmp_path / "traces" / run_id).exists()
        assert not (tmp_path / "metadata" / f"{run_id}.json").exists()
        assert not list((tmp_path / ".deletions").iterdir())


async def test_delete_cancellation_waits_for_cleanup_and_serializes_resume(tmp_path, monkeypatch):
    app = make_app(tmp_path)
    service = app.state.run_service

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            run_id = await create_run(client, scenario="recovery")
            assert (await wait_stopped(client, run_id))["can_resume"] is True
            entered, release = asyncio.Event(), asyncio.Event()
            finish = service.repository.finish_delete

            async def held_finish(identity):
                entered.set()
                await release.wait()
                await finish(identity)

            monkeypatch.setattr(service.repository, "finish_delete", held_finish)
            deletion = asyncio.create_task(service.delete(run_id))
            resumed = None
            try:
                async with asyncio.timeout(3):
                    await entered.wait()
                deletion.cancel()
                await asyncio.sleep(0)
                deletion.cancel()
                resumed = asyncio.create_task(service.restart(run_id))
                await asyncio.sleep(0)
                assert not deletion.done()
                assert not resumed.done()
                assert service.lock.locked()
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await deletion
                with pytest.raises(WebError) as caught:
                    await resumed
                assert caught.value.status == 404
                assert state_counts(tmp_path, run_id) == (0, 0)
                assert not (tmp_path / "traces" / run_id).exists()
            finally:
                release.set()
                await asyncio.gather(
                    *(task for task in (deletion, resumed) if task is not None),
                    return_exceptions=True,
                )


async def test_resume_admitted_before_delete_is_treated_as_active(tmp_path, monkeypatch):
    app = make_app(tmp_path)
    service = app.state.run_service
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            run_id = await create_run(client, scenario="recovery")
            assert (await wait_stopped(client, run_id))["can_resume"] is True
            entered, release = asyncio.Event(), asyncio.Event()
            acquire = service.state_store.acquire

            async def held_acquire(identity):
                entered.set()
                await release.wait()
                return await acquire(identity)

            monkeypatch.setattr(service.state_store, "acquire", held_acquire)
            await service.restart(run_id)
            try:
                async with asyncio.timeout(3):
                    await entered.wait()
                response = await client.delete(f"/api/runs/{run_id}")
                assert response.status_code == 409, response.text
                assert response.json()["detail"]["code"] == "run_active"
                assert not list((tmp_path / ".deletions").iterdir())
            finally:
                release.set()
            assert (await wait_stopped(client, run_id))["status"] == "completed"


pytest.importorskip("fastapi")


async def test_old_cancel_cannot_overwrite_a_new_resume(tmp_path, monkeypatch):
    """被取消的旧代任务不能落定其后继任务拥有的元数据。"""
    gate = InvokeGate(monkeypatch)
    app = make_app(tmp_path)
    service = app.state.run_service
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
                # 原始运行任务已经结束并释放租约。
                # 在下一次获取租约的边界阻塞真实 resume 流程。
                monkeypatch.setattr(service.state_store, "acquire", held_acquire)
                cancel_serializes_admission = service.lock.locked()
                resumed = asyncio.create_task(client.post(f"/api/runs/{run_id}/resume"))
                if not cancel_serializes_admission:
                    # 在有缺陷的管理器中，后继任务可能已经启动，
                    # 而旧任务的取消仍停留在最终状态落定关卡内。
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
    """正在进行的准入必须包含在关闭服务时拥有的任务快照内。"""
    app = make_app(tmp_path)
    service = app.state.run_service
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
            creation = asyncio.create_task(
                client.post(
                    "/api/runs", json={"question": "revenue?", "profile_id": "test-standard"}
                )
            )
            shutdown = None
            try:
                async with asyncio.timeout(4):
                    await save_entered.wait()
                shutdown = asyncio.create_task(closing())
                async with asyncio.timeout(4):
                    await close_entered.wait()
                if service.closing:
                    # 若准入缺少协调，关闭流程会看不到任何已派生任务，
                    # 从而在此 POST 仍被阻塞时就关闭存储。
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
    """历史卡片不能暴露操作者的绝对文件系统路径。"""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        view = await wait_stopped(client, run_id)
        rows = (await client.get("/api/runs")).json()["runs"]
        location = rows[0]["result"]["trace_ref"]
        assert location == view["result"]["trace_ref"]
        assert not location.startswith("/")
        assert str(tmp_path) not in json.dumps(rows)


async def test_restart_reconciles_runs_older_than_the_history_display_limit(tmp_path):
    """仅显示最新 100 条记录的限制，不能永远隐藏过期的活跃所有权。"""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        await wait_stopped(client, run_id)

    manifest = tmp_path / "metadata" / f"{run_id}.json"
    record = json.loads(manifest.read_text())
    # 预先写入独立且已完成的展示记录，避免 100 次无必要的控制器执行。
    # 原始运行仍以真实核心状态为依据。
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
