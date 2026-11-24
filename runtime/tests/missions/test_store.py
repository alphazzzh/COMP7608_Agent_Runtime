"""以真实临时 SQLite 验证持续任务的身份、预留与一次性结算。"""

from __future__ import annotations

import asyncio
import importlib
import os
import sqlite3
import threading
from contextlib import contextmanager

import pytest


def api():
    """在测试体内导入，让缺少实现表现为可观察的测试失败。"""
    models = importlib.import_module("lake_agent.missions.models")
    store = importlib.import_module("lake_agent.missions.store")
    return models, store.MissionStore


async def create(store, models, **changes):
    """建立可复现的手动任务，不依赖真实时钟或任何外部适配器。"""
    payload = {"title": "跟进", "question": "研究什么？", "start_immediately": False}
    payload.update(changes)
    return await store.create(
        models.MissionCreate(**payload),
        profile_id="local",
        profile_signature="sha256:fixture",
        now=100,
    )


async def test_duplicate_event_keeps_permanent_identity_even_after_completion(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models)
        trigger = await store.enqueue(mission["mission_id"], "event-1", now=101)
        assert await store.enqueue(mission["mission_id"], "event-1", now=102) == trigger
        assert trigger["run_id"].startswith("run-")
        assert trigger["status"] == "pending"
        claimed = await store.claim(
            trigger["trigger_id"], max_model_calls=12, max_data_calls=12, now=103
        )
        assert claimed["cycle"] == 1
        await store.mark_running(trigger["trigger_id"], request={"question": "研究什么？"}, now=104)
        await store.settle(
            trigger["trigger_id"],
            model_calls=2,
            data_calls=1,
            run_status="completed",
            stop_reason="done",
            handoff=models.MissionHandoff(summary="已完成"),
            now=105,
        )
        duplicate = await store.enqueue(mission["mission_id"], "event-1", now=106)
        assert duplicate["trigger_id"] == trigger["trigger_id"]
        assert duplicate["run_id"] == trigger["run_id"]
        assert duplicate["status"] == "completed"
    finally:
        await store.aclose()


async def test_user_events_cannot_claim_system_trigger_namespace(tmp_path):
    """旧内部身份及系统命名空间不可由手动事件伪造。"""
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models)
        for event_id in ("initial", "schedule:legacy", "system:initial"):
            with pytest.raises(models.MissionError) as rejected:
                await store.enqueue(mission["mission_id"], event_id, now=101)
            assert rejected.value.code == "reserved_event"
        created = await create(
            store,
            models,
            interval_seconds=1,
            start_immediately=False,
        )
        assert await store.schedule_due(now=102) == 1
        scheduled = (await store.triggers(created["mission_id"]))[0]
        assert scheduled["event_id"].startswith("system:schedule:")
        manual = await create(store, models)
        assert await store.enqueue(manual["mission_id"], "user-event", now=103)
    finally:
        await store.aclose()


async def test_mission_detail_reads_mission_and_triggers_from_one_snapshot(tmp_path, monkeypatch):
    """详情的任务计数、当前 owner 与触发状态必须来自同一读快照。"""
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models)
        trigger = await store.enqueue(mission["mission_id"], "first", now=101)
        await store.claim(trigger["trigger_id"], max_model_calls=4, max_data_calls=4, now=102)

        connection = store._connection
        snapshot_started = threading.Event()
        allow_snapshot = threading.Event()

        class Cursor:
            def __init__(self, cursor, pause=False):
                self.cursor = cursor
                self.pause = pause

            def fetchone(self):
                row = self.cursor.fetchone()
                if self.pause:
                    snapshot_started.set()
                    assert allow_snapshot.wait(2)
                return row

            def __getattr__(self, name):
                return getattr(self.cursor, name)

        class Connection:
            def __init__(self, db):
                self.db = db
                self.paused = False

            def execute(self, sql, *args):
                cursor = self.db.execute(sql, *args)
                pause = not self.paused and sql.startswith(
                    "SELECT * FROM missions WHERE mission_id="
                )
                self.paused |= pause
                return Cursor(cursor, pause)

            def __getattr__(self, name):
                return getattr(self.db, name)

        @contextmanager
        def pause_after_mission_read():
            with connection() as db:
                yield Connection(db)

        monkeypatch.setattr(store, "_connection", pause_after_mission_read)
        other = Store(store.path)
        await other.start()
        pending = asyncio.create_task(store.detail(mission["mission_id"]))
        assert await asyncio.to_thread(snapshot_started.wait, 2)
        await other.settle(
            trigger["trigger_id"],
            model_calls=1,
            data_calls=1,
            run_status="completed",
            stop_reason="done",
            handoff=models.MissionHandoff(summary="已完成"),
            now=103,
        )
        allow_snapshot.set()
        view = await pending
        assert view["mission"]["status"] == "active"
        assert view["mission"]["active_trigger_id"] == trigger["trigger_id"]
        assert view["mission"]["reserved_model_calls"] == 4
        assert view["triggers"][0]["status"] == "claimed"
        await other.aclose()
    finally:
        await store.aclose()


async def test_two_instances_claim_only_once_and_reserve_remaining_budget(tmp_path):
    models, Store = api()
    path = tmp_path / "missions.sqlite3"
    first, second = Store(path), Store(path)
    await asyncio.gather(first.start(), second.start())
    try:
        mission = await create(
            first, models, limits=models.MissionLimits(max_model_calls=5, max_data_calls=3)
        )
        trigger = await first.enqueue(mission["mission_id"], "event", now=101)
        results = await asyncio.gather(
            *[
                store.claim(trigger["trigger_id"], max_model_calls=12, max_data_calls=12, now=102)
                for store in (first, second)
            ]
        )
        assert sum(result is not None for result in results) == 1
        saved = await second.get(mission["mission_id"])
        assert saved["started_runs"] == 1
        assert saved["reserved_model_calls"] == 5
        assert saved["reserved_data_calls"] == 3
        assert len(await second.open_triggers()) == 1
    finally:
        await asyncio.gather(first.aclose(), second.aclose())


async def test_settle_is_once_and_handoff_is_detached(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models, stop_on_completed=False, interval_seconds=20)
        trigger = await store.enqueue(mission["mission_id"], "event", now=101)
        await store.claim(trigger["trigger_id"], max_model_calls=12, max_data_calls=12, now=102)
        handoff = models.MissionHandoff(summary="继续", pending_items=["核对来源"])
        kwargs = dict(
            model_calls=3,
            data_calls=2,
            run_status="insufficient_evidence",
            stop_reason="more",
            handoff=handoff,
        )
        first = await store.settle(trigger["trigger_id"], **kwargs, now=110)
        second = await store.settle(trigger["trigger_id"], **kwargs, now=120)
        assert first == second
        assert first["used_model_calls"] == 3
        assert first["reserved_model_calls"] == 0
        assert first["completed_runs"] == 1
        assert first["next_wake_at"] == 130.0
        first["handoff"]["pending_items"].clear()
        assert (await store.get(mission["mission_id"]))["handoff"]["pending_items"] == ["核对来源"]
    finally:
        await store.aclose()


def test_models_reject_implicit_coercion_and_large_utf8_handoff():
    models, _ = api()
    from pydantic import ValidationError

    for value in (True, "10", 0, 101):
        with pytest.raises(ValidationError):
            models.MissionLimits(max_runs=value)
    with pytest.raises(ValidationError):
        models.MissionCreate(title="x", question="q", interval_seconds=True)
    with pytest.raises(ValidationError):
        models.MissionHandoff(summary="中" * 1500)
    with pytest.raises(ValidationError):
        models.MissionHandoff(pending_items=["x"] * 9)


async def test_queue_allows_one_pending_beside_active_and_counts_accepted_runs(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(
            store, models, limits=models.MissionLimits(max_runs=2), stop_on_completed=False
        )
        first = await store.enqueue(mission["mission_id"], "one", now=101)
        await store.claim(first["trigger_id"], max_model_calls=10, max_data_calls=10, now=102)
        second = await store.enqueue(mission["mission_id"], "two", now=103)
        with pytest.raises(models.MissionError) as full:
            await store.enqueue(mission["mission_id"], "three", now=104)
        assert (full.value.code, full.value.status) == ("mission_queue_full", 429)
        assert (
            await store.claim(second["trigger_id"], max_model_calls=10, max_data_calls=10, now=105)
            is None
        )
        assert len(await store.triggers(mission["mission_id"])) == 2
        await store.settle(
            first["trigger_id"],
            model_calls=1,
            data_calls=0,
            run_status="insufficient_evidence",
            stop_reason="next",
            handoff=models.MissionHandoff(),
            now=106,
        )
        assert (
            await store.claim(second["trigger_id"], max_model_calls=10, max_data_calls=10, now=107)
        )["cycle"] == 2
        await store.settle(
            second["trigger_id"],
            model_calls=1,
            data_calls=0,
            run_status="insufficient_evidence",
            stop_reason="next",
            handoff=models.MissionHandoff(),
            now=108,
        )
        assert (await store.get(mission["mission_id"]))["reason"] == "max_runs_reached"
    finally:
        await store.aclose()


async def test_fixed_delay_coalesces_missed_wakes_and_delays_first_cycle(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models, interval_seconds=20, stop_on_completed=False)
        assert mission["next_wake_at"] == 120.0
        assert await store.schedule_due(now=119) == 0
        assert await store.schedule_due(now=200) == 1
        assert await store.schedule_due(now=300) == 0
        trigger = (await store.pending())[0]
        await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=3, now=301)
        await store.settle(
            trigger["trigger_id"],
            model_calls=1,
            data_calls=1,
            run_status="insufficient_evidence",
            stop_reason="missing_evidence",
            handoff=models.MissionHandoff(),
            now=310,
        )
        assert (await store.get(mission["mission_id"]))["next_wake_at"] == 330.0
        assert await store.schedule_due(now=329) == 0
        assert await store.schedule_due(now=1000) == 1
        assert len(await store.triggers(mission["mission_id"])) == 2
    finally:
        await store.aclose()


async def test_immediate_creation_persists_initial_identity_in_same_transaction(tmp_path):
    models, Store = api()
    path = tmp_path / "missions.sqlite3"
    store = Store(path)
    await store.start()
    mission = await create(store, models, start_immediately=True)
    initial = await store.pending()
    assert len(initial) == 1
    assert initial[0]["event_id"] == "initial"
    assert mission["next_wake_at"] is None
    await store.aclose()
    reopened = Store(path)
    await reopened.start()
    try:
        assert await reopened.pending() == initial
        assert len(await reopened.triggers(mission["mission_id"])) == 1
    finally:
        await reopened.aclose()


@pytest.mark.parametrize("intent", ["paused", "completed"])
async def test_user_status_survives_settlement_and_unknown_outcome(tmp_path, intent):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        for block in (False, True):
            mission = await create(store, models)
            trigger = await store.enqueue(mission["mission_id"], "one", now=101)
            await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=3, now=102)
            await store.set_status(mission["mission_id"], intent, now=103)
            if block:
                result = await store.block(trigger["trigger_id"], "unknown_outcome", now=104)
                assert result["reserved_model_calls"] == 3
            else:
                result = await store.settle(
                    trigger["trigger_id"],
                    model_calls=1,
                    data_calls=1,
                    run_status="failed",
                    stop_reason="failed",
                    handoff=models.MissionHandoff(),
                    now=104,
                )
                assert result["reserved_model_calls"] == 0
            assert result["status"] == intent
    finally:
        await store.aclose()


async def test_deadline_rejects_new_claim_and_scheduler_never_enqueues_expired_task(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        limits = models.MissionLimits(max_duration_seconds=10)
        due = await create(store, models, limits=limits, start_immediately=True)
        pending_mission = await create(store, models, limits=limits)
        trigger = await store.enqueue(pending_mission["mission_id"], "event", now=101)
        assert (
            await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=3, now=110)
            is None
        )
        assert (await store.get(pending_mission["mission_id"]))["reason"] == "deadline_reached"
        assert (await store.get_trigger(trigger["trigger_id"]))["status"] == "cancelled"
        assert await store.schedule_due(now=110) == 0
        assert (await store.get(due["mission_id"]))["status"] == "completed"
    finally:
        await store.aclose()


async def test_pause_stops_claim_and_complete_cancels_only_pending(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models)
        trigger = await store.enqueue(mission["mission_id"], "one", now=101)
        await store.set_status(mission["mission_id"], "paused", now=102)
        saved = await store.get_trigger(trigger["trigger_id"])
        assert (
            await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=3, now=103)
            is None
        )
        assert await store.get_trigger(trigger["trigger_id"]) == saved
        await store.set_status(mission["mission_id"], "active", now=104)
        await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=3, now=105)
        second = await store.enqueue(mission["mission_id"], "two", now=106)
        await store.set_status(mission["mission_id"], "completed", now=107)
        assert (await store.get_trigger(second["trigger_id"]))["status"] == "cancelled"
        assert (await store.get_trigger(trigger["trigger_id"]))["status"] == "claimed"
    finally:
        await store.aclose()


async def test_frozen_request_conflict_restart_and_actual_usage_bounds(tmp_path):
    models, Store = api()
    path = tmp_path / "missions.sqlite3"
    store = Store(path)
    await store.start()
    mission = await create(store, models)
    trigger = await store.enqueue(mission["mission_id"], "event", now=101)
    await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=2, now=102)
    request = {"question": "研究什么？", "config": {"max_model_calls": 3}}
    first = await store.mark_running(trigger["trigger_id"], request=request, now=103)
    request["config"]["max_model_calls"] = 100
    assert first["request"]["config"]["max_model_calls"] == 3
    assert (
        await store.mark_running(trigger["trigger_id"], request=first["request"], now=104) == first
    )
    with pytest.raises(models.MissionError) as conflict:
        await store.mark_running(trigger["trigger_id"], request=request, now=105)
    assert conflict.value.code == "request_conflict"
    with pytest.raises(models.MissionError) as excess:
        await store.settle(
            trigger["trigger_id"],
            model_calls=4,
            data_calls=0,
            run_status="completed",
            stop_reason="done",
            handoff=models.MissionHandoff(),
            now=106,
        )
    assert excess.value.code == "usage_exceeds_reservation"
    await store.aclose()
    reopened = Store(path)
    await reopened.start()
    try:
        assert await reopened.get_trigger(trigger["trigger_id"]) == first
        assert (await reopened.get(mission["mission_id"]))["reserved_model_calls"] == 3
        assert os.stat(path).st_mode & 0o777 == 0o600
        assert os.stat(path.parent).st_mode & 0o777 == 0o700
    finally:
        await reopened.aclose()


async def test_unknown_outcome_preserves_reservation_and_cannot_be_resumed(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models)
        trigger = await store.enqueue(mission["mission_id"], "event", now=101)
        await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=2, now=102)
        blocked = await store.block(trigger["trigger_id"], "unknown_outcome", now=103)
        assert blocked["status"] == "blocked"
        assert blocked["reserved_model_calls"] == 3
        assert blocked["active_trigger_id"] == trigger["trigger_id"]
        with pytest.raises(models.MissionError):
            await store.set_status(mission["mission_id"], "active", now=104)
        with pytest.raises(models.MissionError):
            await store.set_status(mission["mission_id"], "paused", now=104)
        assert await store.block(trigger["trigger_id"], "different", now=105) == blocked
    finally:
        await store.aclose()


async def test_zero_profile_capacity_does_not_mutate_pending_trigger(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models)
        trigger = await store.enqueue(mission["mission_id"], "event", now=101)
        assert (
            await store.claim(trigger["trigger_id"], max_model_calls=0, max_data_calls=3, now=102)
            is None
        )
        assert await store.get_trigger(trigger["trigger_id"]) == trigger
    finally:
        await store.aclose()


async def test_schema_and_digest_corruption_return_safe_errors(tmp_path):
    models, Store = api()
    path = tmp_path / "missions.sqlite3"
    store = Store(path)
    await store.start()
    mission = await create(store, models)
    with sqlite3.connect(path) as db:
        db.execute(
            "UPDATE missions SET title='secret SQL' WHERE mission_id=?", (mission["mission_id"],)
        )
    try:
        with pytest.raises(models.MissionError) as corrupt:
            await store.get(mission["mission_id"])
        assert corrupt.value.code == "corrupt_state"
        assert str(path) not in str(corrupt.value)
        assert "secret SQL" not in str(corrupt.value)
    finally:
        await store.aclose()
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=99")
    reopened = Store(path)
    with pytest.raises(models.MissionError) as version:
        await reopened.start()
    assert version.value.code == "schema_mismatch"
    await reopened.aclose()


@pytest.mark.parametrize(
    "column,value", [("handoff_json", "{"), ("limits_json", "[]"), ("updated_at", "not-time")]
)
async def test_malformed_persisted_values_are_corruption_errors(tmp_path, column, value):
    models, Store = api()
    path = tmp_path / "missions.sqlite3"
    store = Store(path)
    await store.start()
    mission = await create(store, models)
    with sqlite3.connect(path) as db:
        # 列名来自本测试固定参数，值必须经参数绑定。
        db.execute(
            f"UPDATE missions SET {column}=? WHERE mission_id=?", (value, mission["mission_id"])
        )
    try:
        with pytest.raises(models.MissionError) as error:
            await store.get(mission["mission_id"])
        assert error.value.code == "corrupt_state"
    finally:
        await store.aclose()


async def test_enqueue_at_deadline_does_not_accept_new_identity(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models, limits=models.MissionLimits(max_duration_seconds=10))
        with pytest.raises(models.MissionError) as error:
            await store.enqueue(mission["mission_id"], "late", now=110)
        assert error.value.code == "mission_closed"
        assert await store.triggers(mission["mission_id"]) == []
        assert (await store.get(mission["mission_id"]))["reason"] == "deadline_reached"
    finally:
        await store.aclose()


async def test_persisted_request_is_bounded_strict_json_and_handoff_frozen_on_claim(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models, stop_on_completed=False)
        first = await store.enqueue(mission["mission_id"], "first", now=101)
        await store.claim(first["trigger_id"], max_model_calls=3, max_data_calls=3, now=102)
        second = await store.enqueue(mission["mission_id"], "second", now=103)
        for invalid in ({"x": (1, 2)}, {1: "bad"}, {"x": float("nan")}, {"x": "x" * 65_536}):
            with pytest.raises(models.MissionError):
                await store.mark_running(first["trigger_id"], request=invalid, now=104)
        await store.mark_running(first["trigger_id"], request={"large": "中" * 8000}, now=105)
        handoff = models.MissionHandoff(summary="新摘要")
        await store.settle(
            first["trigger_id"],
            model_calls=1,
            data_calls=1,
            run_status="insufficient_evidence",
            stop_reason="next",
            handoff=handoff,
            now=106,
        )
        claimed = await store.claim(
            second["trigger_id"], max_model_calls=3, max_data_calls=3, now=107
        )
        assert claimed["handoff"]["summary"] == "新摘要"
        handoff.summary = "污染"
        assert (await store.get_trigger(second["trigger_id"]))["handoff"]["summary"] == "新摘要"
    finally:
        await store.aclose()


def test_public_input_strips_blank_text_and_checks_model_copy_bypass():
    models, _ = api()
    from pydantic import ValidationError

    assert models.MissionCreate(title=" 标题 ", question=" 目标 ").question == "目标"
    for kwargs in (
        {"title": "  ", "question": "q"},
        {"title": "x", "question": "q" * 2001},
        {"title": "x", "question": "q", "profile_id": "x" * 101},
    ):
        with pytest.raises(ValidationError):
            models.MissionCreate(**kwargs)
    with pytest.raises(ValidationError):
        models.MissionHandoff(summary="\ud800")


async def test_repeated_cancel_waits_for_transaction_and_close_drains_threads(
    tmp_path, monkeypatch
):
    _, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    entered, release = threading.Event(), threading.Event()
    original = store._listing

    def gated(limit):
        entered.set()
        assert release.wait(5)
        return original(limit)

    monkeypatch.setattr(store, "_listing", gated)
    task = asyncio.create_task(store.listing())
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    closing = asyncio.create_task(store.aclose())
    await asyncio.sleep(0.01)
    assert not task.done()
    assert not closing.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await closing
    with pytest.raises(Exception) as error:
        await store.listing()
    assert error.value.code == "store_closed"


async def test_exhausted_quota_blocks_once_and_keeps_accepted_pending_cancelled(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(
            store,
            models,
            stop_on_completed=False,
            limits=models.MissionLimits(max_model_calls=2, max_data_calls=3),
        )
        first = await store.enqueue(mission["mission_id"], "first", now=101)
        await store.claim(first["trigger_id"], max_model_calls=8, max_data_calls=8, now=102)
        second = await store.enqueue(mission["mission_id"], "second", now=103)
        result = await store.settle(
            first["trigger_id"],
            model_calls=2,
            data_calls=1,
            run_status="insufficient_evidence",
            stop_reason="more",
            handoff=models.MissionHandoff(),
            now=104,
        )
        assert (result["status"], result["reason"]) == ("blocked", "quota_exhausted")
        assert result["used_model_calls"] == 2
        assert result["reserved_model_calls"] == 0
        assert (await store.get_trigger(second["trigger_id"]))["status"] == "cancelled"
        assert (
            await store.claim(second["trigger_id"], max_model_calls=1, max_data_calls=1, now=105)
            is None
        )
    finally:
        await store.aclose()


async def test_schema_index_corruption_is_rejected_and_nested_parents_are_private(tmp_path):
    models, Store = api()
    path = tmp_path / "nested" / "private" / "missions.sqlite3"
    store = Store(path)
    await store.start()
    try:
        assert os.stat(path.parent).st_mode & 0o777 == 0o700
        assert os.stat(path.parent.parent).st_mode & 0o777 == 0o700
        with sqlite3.connect(path) as db:
            db.execute("DROP INDEX triggers_one_active")
        with pytest.raises(models.MissionError) as error:
            await store.listing()
        assert error.value.code == "schema_mismatch"
    finally:
        await store.aclose()


async def test_symlink_database_is_rejected_without_disclosing_path(tmp_path):
    models, Store = api()
    target = tmp_path / "target"
    target.write_bytes(b"secret")
    path = tmp_path / "link.sqlite3"
    path.symlink_to(target)
    store = Store(path)
    try:
        with pytest.raises(models.MissionError) as error:
            await store.start()
        assert error.value.code == "storage_error"
        assert str(path) not in error.value.message
        assert target.read_bytes() == b"secret"
    finally:
        await store.aclose()


async def test_noncanonical_persisted_json_and_boolean_corruption_are_rejected(tmp_path):
    models, Store = api()
    path = tmp_path / "missions.sqlite3"
    store = Store(path)
    await store.start()
    try:
        for column, value in (
            ("stop_on_completed", 2),
            (
                "handoff_json",
                '{"summary":"", "pending_items":[],"previous_run_id":null,"artifact_refs":[],"truncated":false}',
            ),
        ):
            mission = await create(store, models)
            with sqlite3.connect(path) as db:
                db.execute(
                    f"UPDATE missions SET {column}=? WHERE mission_id=?",
                    (value, mission["mission_id"]),
                )
            with pytest.raises(models.MissionError) as error:
                await store.get(mission["mission_id"])
            assert error.value.code == "corrupt_state"
    finally:
        await store.aclose()


async def test_cancelled_sqlite_write_commits_before_cancellation_returns(tmp_path, monkeypatch):
    models, Store = api()
    path = tmp_path / "missions.sqlite3"
    store = Store(path)
    await store.start()
    mission = await create(store, models)
    entered = threading.Event()
    original = store._enqueue

    def observed(*args):
        entered.set()
        return original(*args)

    monkeypatch.setattr(store, "_enqueue", observed)
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    task = asyncio.create_task(store.enqueue(mission["mission_id"], "cancelled-request", now=101))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done()
    finally:
        blocker.rollback()
        blocker.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    trigger = (await store.triggers(mission["mission_id"]))[0]
    assert trigger["event_id"] == "cancelled-request"
    assert await store.enqueue(mission["mission_id"], "cancelled-request", now=102) == trigger
    await store.aclose()


async def test_run_limit_completion_takes_precedence_over_exhausted_quota(tmp_path):
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(
            store,
            models,
            stop_on_completed=False,
            limits=models.MissionLimits(max_runs=1, max_model_calls=1, max_data_calls=1),
        )
        trigger = await store.enqueue(mission["mission_id"], "last", now=101)
        await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=3, now=102)
        result = await store.settle(
            trigger["trigger_id"],
            model_calls=1,
            data_calls=1,
            run_status="insufficient_evidence",
            stop_reason="limit",
            handoff=models.MissionHandoff(),
            now=103,
        )
        assert (result["status"], result["reason"]) == ("completed", "max_runs_reached")
    finally:
        await store.aclose()


async def test_pending_filters_unclaimable_missions_before_limiting_results(tmp_path):
    """大量暂停任务不能占满调度窗口，已有 owner 的额外待办也不能遮挡后续任务。"""
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        for index in range(100):
            paused = await create(store, models, start_immediately=True)
            await store.set_status(paused["mission_id"], "paused", now=101 + index)
        occupied = await create(store, models)
        running = await store.enqueue(occupied["mission_id"], "running", now=201)
        await store.claim(running["trigger_id"], max_model_calls=1, max_data_calls=1, now=202)
        waiting = await store.enqueue(occupied["mission_id"], "waiting", now=203)
        eligible = await create(store, models)
        expected = await store.enqueue(eligible["mission_id"], "eligible", now=204)

        assert await store.pending(limit=100) == [expected]
        assert await store.pending(limit=1) == [expected]
        assert (await store.get_trigger(waiting["trigger_id"]))["status"] == "pending"
    finally:
        await store.aclose()


async def test_resume_rejects_blocked_owner_after_user_pause_without_clearing_intent(tmp_path):
    """人工暂停保留下来时，blocked owner 仍必须阻止普通恢复与换身份派发。"""
    models, Store = api()
    store = Store(tmp_path / "missions.sqlite3")
    await store.start()
    try:
        mission = await create(store, models)
        trigger = await store.enqueue(mission["mission_id"], "original", now=101)
        await store.claim(trigger["trigger_id"], max_model_calls=3, max_data_calls=2, now=102)
        await store.set_status(mission["mission_id"], "paused", now=103)
        blocked = await store.block(trigger["trigger_id"], "unknown_outcome", now=104)
        saved_trigger = await store.get_trigger(trigger["trigger_id"])

        assert blocked["status"] == "paused"
        assert blocked["reason"] == "unknown_outcome"
        assert saved_trigger["status"] == "blocked"
        with pytest.raises(models.MissionError) as error:
            await store.set_status(mission["mission_id"], "active", now=105)
        assert (error.value.code, error.value.status) == ("blocked_resume_forbidden", 409)
        assert await store.get(mission["mission_id"]) == blocked
        assert await store.get_trigger(trigger["trigger_id"]) == saved_trigger
        assert blocked["reserved_model_calls"] == 3
        assert blocked["reserved_data_calls"] == 2
        assert blocked["active_trigger_id"] == trigger["trigger_id"]
        assert len(await store.triggers(mission["mission_id"])) == 1
    finally:
        await store.aclose()
