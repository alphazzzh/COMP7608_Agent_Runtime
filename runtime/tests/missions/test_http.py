"""持续任务的内存 HTTP 验收，使用离线端口和临时 SQLite，不监听端口。"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from lake_agent.web.app import create_app
from tests.support.profiles import fixture_profiles
from tests.support.web import InvokeGate


@asynccontextmanager
async def client_for(path, *, modes=("full_agent",)):
    """启动完整应用生命周期；请求只通过进程内 ASGI 传输。"""
    app = create_app(path, profiles=fixture_profiles(), allowed_modes=modes)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield app, client


def mission_payload(**changes):
    """默认手动触发，便于分别验证 HTTP 操作与调度。"""
    return {
        "title": "收入追踪",
        "question": "What was 2024 annual revenue?",
        "profile_id": "test-standard",
        "start_immediately": False,
        "stop_on_completed": False,
        **changes,
    }


async def test_http_mission_lifecycle_and_private_fields(tmp_path):
    """创建、暂停、恢复、重复唤醒和结束均调用真实持久服务。"""
    async with client_for(tmp_path) as (app, client):
        info = (await client.get("/api/info")).json()
        assert info["features"]["missions"] is True
        response = await client.post("/api/missions", json=mission_payload())
        assert response.status_code == 201, response.text
        mission = response.json()
        identity = mission["mission_id"]
        assert "profile_signature" not in mission
        listing = (await client.get("/api/missions")).json()
        assert listing["missions"][0]["mission_id"] == identity
        assert listing["scheduler"]["running"] is True
        for action, status in (("pause", "paused"), ("resume", "active")):
            changed = await client.post(f"/api/missions/{identity}/{action}")
            assert changed.status_code == 200, changed.text
            assert changed.json()["status"] == status
        for _ in range(2):
            wake = await client.post(f"/api/missions/{identity}/wake", json={"event_id": "same"})
            assert wake.status_code == 202, wake.text
            if _ == 0:
                accepted = wake.json()
            assert wake.json()["trigger_id"] == accepted["trigger_id"]
        async with asyncio.timeout(5):
            while True:
                await app.state.mission_service.tick()
                detail = (await client.get(f"/api/missions/{identity}")).json()
                if detail["completed_runs"] == 1:
                    break
                await asyncio.sleep(0.005)
        assert len(detail["triggers"]) == 1
        assert "profile_signature" not in detail
        assert "request" not in detail["triggers"][0]
        run_id = accepted["run_id"]
        view = (await client.get(f"/api/runs/{run_id}")).json()
        assert view["mission_id"] == identity
        assert view["can_resume"] is False
        assert not {"mission_request", "mission_deadline_at", "profile_signature"} & view.keys()
        assert (await client.delete(f"/api/runs/{run_id}")).status_code == 409
        assert (await client.post(f"/api/runs/{run_id}/resume")).status_code == 409
        finished = await client.post(f"/api/missions/{identity}/complete")
        assert finished.status_code == 200
        assert finished.json()["status"] == "completed"
        # 已接受的事件身份即使任务结束也保持幂等。
        assert (
            await client.post(f"/api/missions/{identity}/wake", json={"event_id": "same"})
        ).json()["run_id"] == run_id
        assert (
            await client.post(f"/api/missions/{identity}/wake", json={"event_id": "new"})
        ).status_code == 409
    assert app.state.mission_service.health()["running"] is False


@pytest.mark.parametrize(
    "change",
    [
        {"question": " "},
        {"limits": {"max_runs": True}},
        {"limits": {"max_runs": 101}},
        {"interval_seconds": 0},
        {"api_key": "do-not-echo"},
        {"mode": "no_verifier"},
    ],
)
async def test_invalid_mission_input_is_safe_and_creates_nothing(tmp_path, change):
    """严格字段和范围错误不能变成可执行记录，也不回显调用者输入。"""
    async with client_for(tmp_path) as (_, client):
        response = await client.post("/api/missions", json=mission_payload(**change))
        assert response.status_code == 422
        assert response.json()["detail"]["code"] == "invalid_mission"
        assert "do-not-echo" not in response.text
        assert (await client.get("/api/missions")).json()["missions"] == []


async def test_mission_body_origin_and_unknown_identity_boundaries(tmp_path):
    """分块大输入、恶意Origin、无效JSON及非法event均在持久派发前拒绝。"""
    async with client_for(tmp_path) as (_, client):
        malformed = await client.post("/api/missions", content="{")
        assert malformed.status_code == 422

        async def oversized():
            yield b" " * 20000
            yield b" " * 20000

        response = await client.post("/api/missions", content=oversized())
        assert response.status_code == 413
        forbidden = await client.post(
            "/api/missions", json=mission_payload(), headers={"Origin": "https://evil.example"}
        )
        assert forbidden.status_code == 403
        assert (await client.get("/api/missions/missing")).status_code == 404
        for action in ("pause", "resume", "complete", "wake"):
            response = await client.post(f"/api/missions/missing/{action}", json={"event_id": "x"})
            assert response.status_code == 404, response.text
        identity = (await client.post("/api/missions", json=mission_payload())).json()["mission_id"]
        for body in (
            {"event_id": " "},
            {"event_id": "x" * 201},
            {"event_id": 1},
            {"event_id": "x", "secret": "do-not-echo"},
        ):
            rejected = await client.post(f"/api/missions/{identity}/wake", json=body)
            assert rejected.status_code == 422
            assert "do-not-echo" not in rejected.text


async def test_disabled_full_agent_never_auto_dispatches_persisted_missions(tmp_path):
    """实验宿主关闭完整Agent时，重启也不能从持久队列绕过模式策略。"""
    async with client_for(tmp_path) as (_, client):
        identity = (await client.post("/api/missions", json=mission_payload())).json()["mission_id"]
    async with client_for(tmp_path, modes=("no_verifier",)) as (app, client):
        assert (await client.get("/api/info")).json()["features"]["missions"] is False
        assert app.state.mission_service.health()["running"] is False
        assert (await client.post("/api/missions", json=mission_payload())).status_code == 422
        for action in ("resume", "wake"):
            response = await client.post(
                f"/api/missions/{identity}/{action}", json={"event_id": "new"}
            )
            assert response.status_code == 422
        assert (await client.post(f"/api/missions/{identity}/pause")).status_code == 200
        assert (await client.get("/api/runs")).json()["runs"] == []


async def test_cancelling_a_mission_run_pauses_parent_before_cleanup(tmp_path, monkeypatch):
    """用户取消周期必须先停止新领取，不能由下一次tick自动恢复相同run。"""
    gate = InvokeGate(monkeypatch)
    async with client_for(tmp_path) as (app, client):
        created = await client.post("/api/missions", json=mission_payload(start_immediately=True))
        assert created.status_code == 201
        identity = created.json()["mission_id"]
        await gate.wait_entered()
        detail = (await client.get(f"/api/missions/{identity}")).json()
        run_id = detail["triggers"][0]["run_id"]
        stopped = await client.post(f"/api/runs/{run_id}/cancel")
        assert stopped.status_code == 202
        await app.state.mission_service.tick()
        paused = (await client.get(f"/api/missions/{identity}")).json()
        assert paused["status"] == "paused"
        assert len(gate.requests) == 1
        assert paused["triggers"][0]["run_id"] == run_id
        gate.release.set()


@pytest.mark.parametrize("mission_owned", [False, True])
async def test_cancel_does_not_depend_on_readable_audit_view(tmp_path, monkeypatch, mission_owned):
    """展示审计损坏时也必须能停止实际执行，普通运行不能被新功能拖累。"""
    gate = InvokeGate(monkeypatch)
    async with client_for(tmp_path) as (app, client):
        if mission_owned:
            created = await client.post(
                "/api/missions", json=mission_payload(start_immediately=True)
            )
            identity = created.json()["mission_id"]
            await gate.wait_entered()
            mission = (await client.get(f"/api/missions/{identity}")).json()
            run_id = mission["triggers"][0]["run_id"]
        else:
            created = await client.post(
                "/api/runs",
                json={"question": "What was 2024 annual revenue?", "profile_id": "test-standard"},
            )
            run_id = created.json()["run_id"]
            await gate.wait_entered()
        logs = list(tmp_path.rglob("events.jsonl"))
        assert len(logs) == 1
        with logs[0].open("a") as output:
            output.write("{broken-audit\n")
        assert (await client.get(f"/api/runs/{run_id}")).status_code == 503
        stopped = await client.post(f"/api/runs/{run_id}/cancel")
        assert stopped.status_code == 202, stopped.text
        assert app.state.run_service.active(run_id) is False
        if mission_owned:
            assert (await client.get(f"/api/missions/{identity}")).json()["status"] == "paused"
        gate.release.set()
