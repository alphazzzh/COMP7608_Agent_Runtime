"""持续任务的真实执行、崩溃窗口和保守恢复，不使用外部模型或监听端口。"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import time
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from lake_agent import models as m
from lake_agent.state import StateStoreError
from lake_agent.web.service import RunService
from lake_agent.web.storage import WebError
from tests.support.ports import DemoData
from tests.support.profiles import fixture_profiles
from tests.support.web import InvokeGate


def mission_api():
    """缺少功能时给出明确 RED，之后始终执行产品类而非自制调度替身。"""
    assert importlib.util.find_spec("lake_agent.missions.service"), (
        "Persistent MissionService missing"
    )
    service = importlib.import_module("lake_agent.missions.service")
    models = importlib.import_module("lake_agent.missions.models")
    return service.MissionService, models


@asynccontextmanager
async def host(path, *, profiles=None, clock=time.time, max_active_runs=2):
    """手工驱动产品 tick，测试不靠后台定时器竞争决定期望值。"""
    Service, models = mission_api()
    runs = RunService(
        path, profiles=profiles or fixture_profiles(), max_active_runs=max_active_runs
    )
    await runs.start()
    missions = Service(runs, clock=clock)
    await missions.store.start()
    try:
        yield runs, missions, models
    finally:
        await missions.close()
        await runs.close()


async def wait_for(missions, identity, predicate):
    """等待真正的核心/事务结算，所有循环都有独立时间上界。"""
    async with asyncio.timeout(8):
        while True:
            await missions.tick()
            view = await missions.view(identity)
            if predicate(view):
                return view
            await asyncio.sleep(0.005)


def payload(models, **updates):
    """使用独立手工期望的离线目标和小额度。"""
    fields = {
        "title": "持续检查报告",
        "question": "What was 2024 annual revenue?",
        "profile_id": "test-standard",
        "stop_on_completed": False,
        "limits": {"max_runs": 2, "max_model_calls": 12, "max_data_calls": 6},
    }
    fields.update(updates)
    return models.MissionCreate.model_validate(fields)


async def test_two_manual_cycles_use_real_runs_and_frozen_previous_progress(tmp_path):
    """第二轮收到第一轮线索，累计目录/工具调用按4次计而非把预留当实耗。"""
    async with host(tmp_path) as (runs, missions, models):
        mission = await missions.create(payload(models))
        first = await wait_for(
            missions, mission["mission_id"], lambda view: view["completed_runs"] == 1
        )
        first_run = first["triggers"][0]["run_id"]
        assert first["used_model_calls"] == 0
        assert first["used_data_calls"] == 2
        assert first["reserved_data_calls"] == 0
        assert first["status"] == "active"
        await missions.wake(mission["mission_id"], "manual-second")
        finished = await wait_for(
            missions, mission["mission_id"], lambda view: view["status"] == "completed"
        )
        assert finished["started_runs"] == finished["completed_runs"] == 2
        assert finished["used_data_calls"] == 4
        assert finished["reserved_model_calls"] == finished["reserved_data_calls"] == 0
        assert "profile_signature" not in finished
        second_run = next(item["run_id"] for item in finished["triggers"] if item["cycle"] == 2)
        assert first_run != second_run
        snapshot = await runs.core_snapshot(second_run)
        original = snapshot["request"]
        assert original["question"] == "What was 2024 annual revenue?"
        assert original["continuation"]["previous_run_id"] == first_run
        assert "120 million HKD" in original["continuation"]["summary"]
        assert original["config"]["max_data_calls"] == 4
        assert all("request" not in item for item in finished["triggers"])


async def test_global_capacity_keeps_trigger_pending_without_reserving(tmp_path, monkeypatch):
    """满额时不能丢失已接受事件，也不能提前占住一个不执行周期的额度。"""
    gate = InvokeGate(monkeypatch)
    async with host(tmp_path, max_active_runs=1) as (runs, missions, models):
        await runs.create("What was 2024 annual revenue?", profile_id="test-standard")
        await gate.wait_entered()
        mission = await missions.create(payload(models))
        await missions.tick()
        waiting = await missions.view(mission["mission_id"])
        assert waiting["triggers"][0]["status"] == "pending"
        assert waiting["started_runs"] == 0
        assert waiting["reserved_data_calls"] == 0
        gate.release.set()
        done = await wait_for(
            missions, mission["mission_id"], lambda value: value["completed_runs"] == 1
        )
        assert done["used_data_calls"] == 2
        assert len(gate.requests) == 2


async def test_restart_after_claim_uses_original_trigger_and_reserved_budget(tmp_path):
    """预留已提交但请求还未派发时重启，只补执行原周期而不二次预留。"""
    async with host(tmp_path) as (_, missions, models):
        mission = await missions.create(payload(models))
        trigger = (await missions.store.pending())[0]
        await missions.store.claim(trigger["trigger_id"], max_model_calls=12, max_data_calls=12)
        identity, trigger_id, run_id = (
            mission["mission_id"],
            trigger["trigger_id"],
            trigger["run_id"],
        )
    async with host(tmp_path) as (_, missions, _):
        after = await wait_for(missions, identity, lambda value: value["completed_runs"] == 1)
        assert after["started_runs"] == 1
        assert after["used_data_calls"] == 2
        assert after["triggers"][0]["trigger_id"] == trigger_id
        assert after["triggers"][0]["run_id"] == run_id


@pytest.mark.parametrize("finish_mission", [False, True])
async def test_restart_after_core_terminal_only_settles_without_reexecuting(
    tmp_path, monkeypatch, finish_mission
):
    """核心已完成但Mission尚未结算的崩溃窗口，不能再次调用工具。"""
    calls = []
    original = DemoData.invoke

    async def observed(data, request):
        calls.append(request.call_id)
        return await original(data, request)

    monkeypatch.setattr(DemoData, "invoke", observed)
    async with host(tmp_path) as (runs, missions, models):
        mission = await missions.create(payload(models))
        await missions.tick()
        async with asyncio.timeout(5):
            await asyncio.gather(*list(runs.tasks.values()))
        assert (await missions.view(mission["mission_id"]))["completed_runs"] == 0
        if finish_mission:
            await missions.complete(mission["mission_id"])
    async with host(tmp_path) as (_, missions, _):
        after = await wait_for(
            missions, mission["mission_id"], lambda value: value["completed_runs"] == 1
        )
        for _ in range(3):
            await missions.tick()
        assert after["started_runs"] == 1
        assert after["used_data_calls"] == 2 and after["reserved_data_calls"] == 0
        assert (await missions.view(mission["mission_id"])) == after
        assert after["status"] == ("completed" if finish_mission else "active")
        assert len(calls) == 1


async def test_unknown_tool_outcome_blocks_without_refund_or_replay(tmp_path, monkeypatch):
    """取消正在执行的工具产生unknown；不能换run身份或把预留退回继续轮询。"""
    gate = InvokeGate(monkeypatch)
    async with host(tmp_path) as (runs, missions, models):
        mission = await missions.create(payload(models))
        await missions.tick()
        await gate.wait_entered()
        current = await missions.view(mission["mission_id"])
        await runs.cancel(current["triggers"][0]["run_id"])
        blocked = await wait_for(
            missions, mission["mission_id"], lambda value: value["status"] == "blocked"
        )
        assert blocked["reason"] == "recovery_outcome_unknown"
        assert blocked["reserved_data_calls"] == 6
        assert blocked["used_data_calls"] == 0
        assert blocked["started_runs"] == 1
        for _ in range(3):
            await missions.tick()
        assert len(gate.requests) == 1
        with pytest.raises(models.MissionError):
            await missions.resume(mission["mission_id"])
        with pytest.raises(models.MissionError):
            await missions.wake(mission["mission_id"], "unsafe-new-identity")


async def test_pause_holds_pending_and_resume_keeps_original_event_identity(tmp_path):
    """暂停不消费pending事件，继续后仍执行原触发且只执行一次。"""
    async with host(tmp_path) as (_, missions, models):
        mission = await missions.create(payload(models))
        initial = (await missions.view(mission["mission_id"]))["triggers"][0]
        await missions.pause(mission["mission_id"])
        await missions.tick()
        paused = await missions.view(mission["mission_id"])
        assert paused["status"] == "paused"
        assert paused["started_runs"] == 0
        await missions.resume(mission["mission_id"])
        after = await wait_for(
            missions, mission["mission_id"], lambda value: value["completed_runs"] == 1
        )
        assert after["triggers"][0]["trigger_id"] == initial["trigger_id"]
        assert after["started_runs"] == 1


async def test_paused_success_honors_stop_on_completed_before_resume(tmp_path, monkeypatch):
    """运行期间暂停不能让成功后的 stop_on_completed 被恢复绕过。"""
    gate = InvokeGate(monkeypatch)
    async with host(tmp_path) as (_, missions, models):
        mission = await missions.create(payload(models, stop_on_completed=True))
        await missions.tick()
        await gate.wait_entered()
        await missions.pause(mission["mission_id"])
        pending = await missions.wake(mission["mission_id"], "before-success")
        gate.release.set()

        finished = await wait_for(
            missions, mission["mission_id"], lambda value: value["completed_runs"] == 1
        )
        assert finished["status"] == "completed"
        assert finished["reason"] == "completed"
        assert finished["started_runs"] == 1
        assert (await missions.store.get_trigger(pending["trigger_id"]))["status"] == "cancelled"
        with pytest.raises(models.MissionError) as closed:
            await missions.resume(mission["mission_id"])
        assert closed.value.code == "mission_closed"
        await missions.tick()
        assert (await missions.view(mission["mission_id"]))["started_runs"] == 1


@pytest.mark.parametrize("freeze_request", [False, True])
async def test_completed_mission_with_open_unknown_run_is_explicitly_blocked(
    tmp_path, freeze_request
):
    """结束父任务时不得静默遗留无核心终态的已运行触发器或退还预留。"""
    async with host(tmp_path) as (_, missions, models):
        mission = await missions.create(payload(models))
        trigger = (await missions.store.pending())[0]
        claimed = await missions.store.claim(
            trigger["trigger_id"], max_model_calls=12, max_data_calls=12
        )
        if freeze_request:
            request = m.RunRequest(
                run_id=claimed["run_id"],
                task_id=f"mission:{mission['mission_id']}",
                question=mission["question"],
            )
            await missions.store.mark_running(
                trigger["trigger_id"], request=request.model_dump(mode="json")
            )
        await missions.complete(mission["mission_id"])

    async with host(tmp_path) as (_, missions, _):
        for _ in range(2):
            await missions.tick()
        view = await missions.view(mission["mission_id"])
        unresolved = view["triggers"][0]
        assert view["status"] == "completed"
        assert view["reason"] == "completed"
        assert unresolved["status"] == "blocked"
        assert unresolved["stop_reason"] == "mission_run_unresolved"
        for field in ("reserved_data_calls", "reserved_model_calls"):
            assert view[field] == unresolved[field] == claimed[field] > 0
        assert view["completed_runs"] == view["used_data_calls"] == 0
        assert view["active_trigger_id"] == unresolved["trigger_id"]


async def test_cancelling_historical_mission_run_does_not_pause_active_mission(tmp_path):
    """历史 run 的 not_running 拒绝发生前，不应改变当前 Mission 状态。"""
    async with host(tmp_path) as (_, missions, models):
        mission = await missions.create(payload(models, stop_on_completed=False))
        finished = await wait_for(
            missions, mission["mission_id"], lambda value: value["completed_runs"] == 1
        )
        old_run_id = finished["triggers"][0]["run_id"]
        assert finished["status"] == "active"

        with pytest.raises(WebError) as not_running:
            await missions.cancel_run(old_run_id)
        assert not_running.value.code == "not_running"
        still_active = await missions.view(mission["mission_id"])
        assert still_active["status"] == "active"
        await missions.wake(mission["mission_id"], "after-historical-cancel")
        assert (await missions.view(mission["mission_id"]))["status"] == "active"


async def test_run_finishing_during_cancel_admission_does_not_pause_mission(tmp_path, monkeypatch):
    """在读取当前触发器时正常收尾的运行不接受取消，父任务也不变。"""
    gate = InvokeGate(monkeypatch)
    async with host(tmp_path) as (runs, missions, models):
        mission = await missions.create(payload(models))
        await missions.tick()
        await gate.wait_entered()
        run_id, task = next(iter(runs.tasks.items()))
        load_mission = missions.store.get

        async def get_after_completion(mission_id):
            gate.release.set()
            async with asyncio.timeout(5):
                await asyncio.shield(task)
            return await load_mission(mission_id)

        with monkeypatch.context() as patch:
            patch.setattr(missions.store, "get", get_after_completion)
            with pytest.raises(WebError) as stop:
                await missions.cancel_run(run_id)
        assert stop.value.code == "not_running"
        settled = await wait_for(
            missions, mission["mission_id"], lambda view: view["completed_runs"] == 1
        )
        assert settled["status"] == "active"


async def test_disconnected_cancel_keeps_scheduler_locked_until_parent_is_paused(
    tmp_path, monkeypatch
):
    """已接纳的取消即使请求断开，也先完成暂停和清理，调度才能继续。"""
    gate = InvokeGate(monkeypatch)
    async with host(tmp_path) as (runs, missions, models):
        mission = await missions.create(payload(models))
        await missions.tick()
        await gate.wait_entered()
        run_id = next(iter(runs.tasks))
        pending = await missions.wake(mission["mission_id"], "during-cancel")
        entered, release = asyncio.Event(), asyncio.Event()
        set_status = missions.store.set_status

        async def gated_pause(*args, **kwargs):
            entered.set()
            await release.wait()
            return await set_status(*args, **kwargs)

        monkeypatch.setattr(missions.store, "set_status", gated_pause)
        cancellation = asyncio.create_task(missions.cancel_run(run_id))
        tick = None
        try:
            async with asyncio.timeout(4):
                await entered.wait()
                cancellation.cancel()
                tick = asyncio.create_task(missions.tick())
                await asyncio.sleep(0)
                assert not cancellation.done() and not tick.done()
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await cancellation
                await tick
            assert (await missions.view(mission["mission_id"]))["status"] == "paused"
            assert (await missions.store.get_trigger(pending["trigger_id"]))["status"] == "pending"
            assert not runs.active(run_id)
            assert len(gate.requests) == 1
        finally:
            release.set()
            await asyncio.gather(
                cancellation, *([tick] if tick is not None else []), return_exceptions=True
            )


async def test_expired_pending_mission_never_calls_runtime(tmp_path):
    """挂起期间超过Mission截止时间的待办不应启动一个拥有新期限的run。"""
    now = [time.time()]
    async with host(tmp_path, clock=lambda: now[0]) as (runs, missions, models):
        mission = await missions.create(payload(models, limits={"max_duration_seconds": 1}))
        now[0] += 2
        await missions.tick()
        expired = await missions.view(mission["mission_id"])
        assert expired["status"] == "completed"
        assert expired["reason"] == "deadline_reached"
        assert expired["started_runs"] == 0
        assert await runs.listing() == []


@pytest.mark.parametrize(
    "code", ["storage_error", "store_closed", "schema_mismatch", "corrupt_state"]
)
async def test_core_storage_failure_stops_dispatch_without_releasing_reservation(
    tmp_path, monkeypatch, code
):
    """核心持久层失效不是单任务失败；本轮不得继续派发其他目标。"""
    async with host(tmp_path) as (runs, missions, models):
        first = await missions.create(payload(models))
        await missions.create(payload(models))
        pending = await missions.store.pending()
        owned = next(item for item in pending if item["mission_id"] == first["mission_id"])
        await missions.store.claim(owned["trigger_id"], max_model_calls=4, max_data_calls=4)

        async def unavailable(run_id):
            raise StateStoreError(code)

        monkeypatch.setattr(runs, "core_snapshot", unavailable)
        with pytest.raises(StateStoreError, match="."):
            await missions.tick()
        assert await runs.listing() == []


async def test_real_core_write_failure_stops_scheduler_before_other_missions_dispatch(
    tmp_path, caplog
):
    """工具调用日志写失败后，blocked 投影不能吞掉宿主级持久故障。"""
    async with host(tmp_path, max_active_runs=1) as (runs, missions, models):
        first = await missions.create(payload(models))
        first_id = first["mission_id"]
        first_run = (await missions.store.triggers(first_id))[0]["run_id"]
        finish_call = runs.state_store.finish_call
        failed = asyncio.Event()

        async def fail_first_finish(lease, call_id, value):
            if lease.run_id == first_run and value["phase"] == "tool" and not failed.is_set():
                failed.set()
                raise StateStoreError("storage_error")
            return await finish_call(lease, call_id, value)

        runs.state_store.finish_call = fail_first_finish
        await missions.start()
        async with asyncio.timeout(3):
            await failed.wait()
            active_task = runs.tasks.get(first_run)
            if active_task is not None:
                await asyncio.gather(active_task, return_exceptions=True)

        second = await missions.create(payload(models))
        await missions.wakeup.wait()
        async with asyncio.timeout(3):
            await missions.task

        first_record = await missions.view(first_id)
        second_record = await missions.view(second["mission_id"])
        assert missions.health()["running"] is False
        assert missions.health()["error"]["code"] == "mission_scheduler_failed"
        assert first_record["status"] == "blocked"
        assert first_record["reserved_model_calls"] == 12
        assert first_record["reserved_data_calls"] == 6
        assert first_record["active_trigger_id"] is not None
        assert second_record["started_runs"] == 0
        assert second_record["triggers"][0]["status"] == "pending"
        # 后台宿主把同一故障转换为安全健康状态，并停止后续循环。
        await missions.start()
        async with asyncio.timeout(2):
            await missions.task
        assert missions.health()["running"] is False
        assert missions.health()["error"]["code"] == "mission_scheduler_failed"
        # 已领取周期的 blocked run 保留在历史中，供恢复与审计对账。
        listing = await runs.listing()
        assert len(listing) == 1
        assert listing[0]["run_id"] == first_run
        assert listing[0]["mission_id"] == first_id
        assert listing[0]["status"] == "blocked"


@pytest.mark.parametrize("projection", ["missing", "stale"])
async def test_core_terminal_repairs_web_projection_even_without_current_profile(
    tmp_path, monkeypatch, projection
):
    """核心完成后的网页保存窗口可恢复，历史结果不依赖当前执行配置仍存在。"""
    calls = []
    original = DemoData.invoke

    async def observed(data, request):
        calls.append(request.call_id)
        return await original(data, request)

    monkeypatch.setattr(DemoData, "invoke", observed)
    async with host(tmp_path) as (runs, missions, models):
        mission = await missions.create(payload(models))
        await missions.tick()
        trigger = (await missions.view(mission["mission_id"]))["triggers"][0]
        run_id = trigger["run_id"]
        async with asyncio.timeout(5):
            await asyncio.gather(*list(runs.tasks.values()))
        if projection == "missing":
            (runs.repository.manifests / f"{run_id}.json").unlink()
        else:
            stale = await runs.repository.load(run_id)
            stale.update(status="running", result=None)
            await runs.repository.save(stale)
    async with host(tmp_path) as (runs, missions, _):
        runs.profiles.entries.clear()
        await missions.tick()
        settled = await missions.view(mission["mission_id"])
        assert settled["completed_runs"] == 1
        view = await runs.view(run_id)
        assert view["status"] == "completed"
        assert "120 million HKD" in view["result"]["answer"]["text"]
        assert view["mission_id"] == mission["mission_id"]
        assert len(view["attempts"]) == 1
        assert len(view["evidence"]) == 1
        assert len(calls) == 1


async def test_single_run_budget_exceeded_blocks_instead_of_repeating_same_failure(tmp_path):
    """单轮耗尽预算应保留真实终态并停止调度，不能花完Mission额度反复空转。"""
    profiles = fixture_profiles()
    profiles[0] = replace(
        profiles[0], run_config=profiles[0].run_config.model_copy(update={"max_data_calls": 1})
    )
    async with host(tmp_path, profiles=profiles) as (runs, missions, models):
        mission = await missions.create(payload(models, interval_seconds=1))
        finished = await wait_for(
            missions, mission["mission_id"], lambda item: item["completed_runs"] == 1
        )
        assert finished["used_data_calls"] == 1
        assert finished["triggers"][0]["run_status"] == "budget_exceeded"
        assert finished["status"] == "blocked"
        assert finished["reason"] == "budget_exceeded"
        assert finished["next_wake_at"] is None
        assert len(await runs.listing()) == 1
