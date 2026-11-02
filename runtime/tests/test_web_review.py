"""Web 任务与引用文件边界的具体审查回归测试。

测试替身使用真实的本地服务与磁盘仓库。事件关卡只中断准入，
或在执行结束后替换测试拥有的产物目录。
"""

import asyncio
import json
import threading

import pytest

pytest.importorskip("fastapi")

from test_web import create_run, running_client, wait_stopped


async def test_cancelled_http_admission_does_not_leave_queued_run(tmp_path, monkeypatch):
    """POST 连接中断后，不能使运行一直被错误标记为排队中。"""
    from lake_agent.web.storage import Repository

    entered, release = threading.Event(), threading.Event()
    original = Repository._save
    held = False

    def gated_save(repository, run_id, encoded):
        nonlocal held
        if not held and json.loads(encoded)["status"] == "queued":
            held = True
            entered.set()
            assert release.wait(3)
        return original(repository, run_id, encoded)

    monkeypatch.setattr(Repository, "_save", gated_save)
    async with running_client(tmp_path) as client:
        request = asyncio.create_task(client.post("/api/runs", json={"question": "revenue?"}))
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            request.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await request
            rows = (await client.get("/api/runs")).json()["runs"]
            assert len(rows) == 1
            view = (await client.get(f"/api/runs/{rows[0]['run_id']}")).json()
            assert view["status"] == "interrupted"
            assert view["active"] is view["can_resume"] is False
        finally:
            release.set()
            if not request.done():
                request.cancel()
            await asyncio.gather(request, return_exceptions=True)


async def test_referenced_artifact_symlink_cannot_escape_attempt(tmp_path):
    """即使被引用的文件名有效，也必须仍处于其所属尝试的目录内。"""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        view = await wait_stopped(client, run_id)
        event = next(value for value in view["events"] if value["data_ref"])
        attempt = event["attempt_id"]
        ref = event["data_ref"]
        artifacts = tmp_path / "traces" / run_id / "attempts" / attempt / "artifacts"
        artifacts.rename(artifacts.with_name("artifacts-backup"))
        outside = tmp_path / "outside-attempt"
        outside.mkdir()
        (outside / f"{ref}.json").write_text('{"foreign":"must not be returned"}')
        artifacts.symlink_to(outside, target_is_directory=True)
        response = await client.get(f"/api/runs/{run_id}/artifacts/{attempt}/{ref}")
        assert response.status_code == 404
        assert "foreign" not in response.text
