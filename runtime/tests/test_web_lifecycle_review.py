"""Web 任务生命周期边界上所有权变更的审查回归测试。

所有断言均使用真实本地运行的 HTTP 视图。测试拥有的事件关卡暴露旧任务结束之后、
准入完成之前的间隙；核心运行时、调用日志、轨迹和仓库操作仍参与实际执行。
"""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest

pytest.importorskip("fastapi")

from test_web import InvokeGate, create_run, make_app, running_client, wait_stopped


async def test_old_cancel_cannot_overwrite_a_new_resume(tmp_path, monkeypatch):
    """被取消的旧代任务不能落定其后继任务拥有的元数据。"""
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
