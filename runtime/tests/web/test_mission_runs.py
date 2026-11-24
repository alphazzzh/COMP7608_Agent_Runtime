"""持续任务对既定 run 身份的真实持久提交，不启动服务或调用模型。"""

from __future__ import annotations

import asyncio
import time
from uuid import uuid4

import pytest

from lake_agent import models as m
from lake_agent.web.service import RunService
from lake_agent.web.storage import WebError
from tests.support.ports import DemoData
from tests.support.profiles import fixture_profiles
from tests.support.web import InvokeGate


def mission_request(service, **config_updates):
    """使用手工指定的稳定身份和服务端配置，不读取实现生成的期望值。"""
    profile = service.profiles.get("test-standard")
    return m.RunRequest(
        run_id=f"run-{uuid4().hex}",
        task_id="mission:mission-test",
        question="What was 2024 annual revenue?",
        config=profile.run_config.model_copy(update=config_updates),
    )


async def submit(service, request, **options):
    """只调用产品内部入口，缺少实现时产生明确的功能失败。"""
    method = getattr(service, "submit_mission", None)
    assert callable(method), "RunService must accept a preallocated durable mission run"
    return await method(
        request,
        mission_id="mission-test",
        trigger_id="trigger-test",
        profile_id="test-standard",
        profile_signature=service.profiles.get("test-standard").signature(),
        **options,
    )


async def stopped(service, run_id):
    """等待实际任务和资源清理结束，避免只断言创建响应。"""
    async with asyncio.timeout(5):
        task = service.tasks.get(run_id)
        if task is not None:
            await asyncio.shield(task)
    return await service.view(run_id)


async def test_same_mission_identity_never_dispatches_an_active_or_completed_run_twice(
    tmp_path, monkeypatch
):
    """重复提交同一已派发身份不能增加工具调用或恢复 attempt。"""
    gate = InvokeGate(monkeypatch)
    service = RunService(tmp_path, profiles=fixture_profiles())
    await service.start()
    try:
        request = mission_request(service)
        first = await submit(service, request)
        await gate.wait_entered()
        second = await submit(service, request)
        assert first["run_id"] == second["run_id"] == request.run_id
        assert len(gate.requests) == 1
        gate.release.set()
        result = await stopped(service, request.run_id)
        assert result["status"] == "completed"
        third = await submit(service, request)
        assert third["status"] == "completed"
        assert len(gate.requests) == 1
        assert len((await service.view(request.run_id))["attempts"]) == 1
        assert "mission_request" not in result
    finally:
        gate.release.set()
        await service.close()


async def test_mission_identity_rejects_changed_frozen_request(tmp_path):
    """相同 run ID 不能被换成另一个问题，哪怕旧 run 已有正常终态。"""
    service = RunService(tmp_path, profiles=fixture_profiles())
    await service.start()
    try:
        request = mission_request(service)
        await submit(service, request)
        await stopped(service, request.run_id)
        with pytest.raises(WebError) as error:
            await submit(service, request.model_copy(update={"question": "different question"}))
        assert error.value.code == "mission_run_conflict"
    finally:
        await service.close()


async def test_lost_manifest_rebuilds_from_core_terminal_without_reexecuting(tmp_path, monkeypatch):
    """完成后丢失展示投影时，应从真实核心终态修复而非再次调用适配器。"""
    calls = []
    original = DemoData.invoke

    async def observed(data, request):
        calls.append(request.call_id)
        return await original(data, request)

    monkeypatch.setattr(DemoData, "invoke", observed)
    service = RunService(tmp_path, profiles=fixture_profiles())
    await service.start()
    try:
        request = mission_request(service)
        await submit(service, request)
        before = await stopped(service, request.run_id)
        (service.repository.manifests / f"{request.run_id}.json").unlink()
        after = await submit(service, request)
        assert after["result"]["answer"] == before["result"]["answer"]
        assert len(calls) == 1
        snapshot = await service.core_snapshot(request.run_id)
        assert snapshot["result"]["status"] == "completed"
        assert snapshot["request"] == request.model_dump(mode="json")
    finally:
        await service.close()


@pytest.mark.parametrize(
    "updates",
    [
        {"max_model_calls": 13},
        {"max_data_calls": 13},
        {"max_wall_time_ms": 600_001},
        {"require_verification": True},
        {"model_id": "another-model"},
    ],
)
async def test_internal_mission_submission_cannot_raise_or_change_profile_policy(tmp_path, updates):
    """长期任务只允许收紧预算，不能修改原执行配置的模型或完成政策。"""
    service = RunService(tmp_path, profiles=fixture_profiles())
    await service.start()
    try:
        with pytest.raises(WebError) as error:
            await submit(service, mission_request(service, **updates))
        assert error.value.code == "mission_run_policy"
        assert await service.listing() == []
    finally:
        await service.close()


async def test_mission_run_cannot_bypass_parent_through_ordinary_resume_or_delete(tmp_path):
    """普通运行接口不能删除或擅自恢复被长期任务拥有的执行记录。"""
    service = RunService(tmp_path, profiles=fixture_profiles())
    await service.start()
    try:
        request = mission_request(service)
        await submit(service, request)
        await stopped(service, request.run_id)
        for action in (service.restart, service.delete):
            with pytest.raises(WebError) as error:
                await action(request.run_id)
            assert error.value.code == "mission_owned"
        assert (await service.core_snapshot(request.run_id))["result"]["status"] == "completed"
    finally:
        await service.close()


async def test_mission_absolute_deadline_cancels_a_previously_frozen_long_request(
    tmp_path, monkeypatch
):
    """领取后停机延迟不能让原始相对run预算延长Mission总期限。"""
    gate = InvokeGate(monkeypatch)
    service = RunService(tmp_path, profiles=fixture_profiles())
    await service.start()
    try:
        request = mission_request(service)
        await submit(service, request, deadline_at=time.time() + 0.3)
        await gate.wait_entered()
        view = await stopped(service, request.run_id)
        assert view["status"] == "interrupted"
        assert view["error"]["code"] == "mission_deadline_reached"
        snapshot = await service.core_snapshot(request.run_id)
        assert snapshot["result"] is None
        assert len(gate.requests) == 1
    finally:
        gate.release.set()
        await service.close()
