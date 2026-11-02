"""基于真实 SQLite 与 POSIX 文件锁的持久化状态存储契约测试。"""

import asyncio
import hashlib
import importlib
import sqlite3
import sys
import threading
from pathlib import Path

import pytest


def state_api():
    """在测试函数体内导入，使缺少实现表现为测试失败。"""
    return importlib.import_module("lake_agent.state")


async def test_roundtrip_is_detached_and_survives_store_restart(tmp_path):
    api = state_api()
    path = tmp_path / "state.sqlite3"
    store = api.SqliteStateStore(path)
    original = {"phase": "plan", "nested": {"values": [1, 2]}}
    lease = await store.create("run/a", original)
    try:
        original["nested"]["values"].append(3)
        loaded = await store.load(lease)
        assert loaded.state == {"phase": "plan", "nested": {"values": [1, 2]}}
        assert loaded.calls == {}
        loaded.state["nested"]["values"].clear()
        assert (await store.load(lease)).state["nested"]["values"] == [1, 2]
        await store.save(lease, {"phase": "done"})
    finally:
        await store.aclose()
    restarted = api.SqliteStateStore(path)
    try:
        next_lease = await restarted.acquire("run/a")
        assert (await restarted.load(next_lease)).state == {"phase": "done"}
    finally:
        await restarted.aclose()


async def test_call_ledger_is_immutable_until_single_completion(tmp_path):
    api = state_api()
    store = api.SqliteStateStore(tmp_path / "state.db")
    lease = await store.create("run", {})
    started = {"status": "started", "signature": {"tool": "read"}}
    completed = {"status": "completed", "signature": {"tool": "read"}, "raw": [1]}
    try:
        await store.begin_call(lease, "call", started)
        started["signature"]["tool"] = "changed"
        assert (await store.load(lease)).calls["call"] == {
            "status": "started",
            "signature": {"tool": "read"},
        }
        with pytest.raises(api.StateStoreError) as duplicate:
            await store.begin_call(lease, "call", {"status": "started"})
        assert duplicate.value.code == "call_exists"
        with pytest.raises(api.StateStoreError) as missing:
            await store.finish_call(lease, "absent", completed)
        assert missing.value.code == "call_not_found"
        await store.finish_call(lease, "call", completed)
        completed["raw"].clear()
        loaded = await store.load(lease)
        assert loaded.calls["call"]["raw"] == [1]
        loaded.calls["call"]["raw"].clear()
        assert (await store.load(lease)).calls["call"]["raw"] == [1]
        with pytest.raises(api.StateStoreError) as repeated:
            await store.finish_call(lease, "call", {"status": "completed"})
        assert repeated.value.code == "call_completed"
    finally:
        await store.aclose()


async def test_one_owner_across_instances_and_stale_token_is_rejected(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    first, second = api.SqliteStateStore(path), api.SqliteStateStore(path)
    try:
        old = await first.create("run", {"v": 1})
        with pytest.raises(api.StateStoreError) as conflict:
            await second.acquire("run")
        assert conflict.value.code == "lease_conflict"
        forged = api.StateLease("run", "not-the-token")
        with pytest.raises(api.StateStoreError) as wrong:
            await first.save(forged, {"v": 2})
        assert wrong.value.code == "lease_invalid"
        await first.release(old)
        await first.release(old)
        current = await second.acquire("run")
        with pytest.raises(api.StateStoreError) as stale:
            await first.save(old, {"v": 3})
        assert stale.value.code == "lease_invalid"
        await first.release(old)  # 重复执行旧租约的 release，不能解锁新所有者的租约。
        with pytest.raises(api.StateStoreError):
            await first.acquire("run")
        assert (await second.load(current)).state == {"v": 1}
    finally:
        await first.aclose()
        await second.aclose()


async def test_duplicate_runs_and_absent_runs_are_rejected(tmp_path):
    api = state_api()
    store = api.SqliteStateStore(tmp_path / "state.db")
    try:
        lease = await store.create("run", {})
        await store.release(lease)
        with pytest.raises(api.StateStoreError) as duplicate:
            await store.create("run", {"overwrite": True})
        assert duplicate.value.code == "run_exists"
        with pytest.raises(api.StateStoreError) as absent:
            await store.acquire("absent")
        assert absent.value.code == "run_not_found"
    finally:
        await store.aclose()


@pytest.mark.parametrize(
    "value", [{"n": float("nan")}, {"n": float("inf")}, {1: "key"}, {"tuple": (1, 2)}, {"set": {1}}]
)
async def test_non_json_values_never_replace_valid_state(tmp_path, value):
    api = state_api()
    store = api.SqliteStateStore(tmp_path / "state.db")
    lease = await store.create("run", {"safe": True})
    try:
        with pytest.raises(api.StateStoreError) as invalid:
            await store.save(lease, value)
        assert invalid.value.code == "invalid_json"
        assert (await store.load(lease)).state == {"safe": True}
    finally:
        await store.aclose()


async def test_invalid_call_status_does_not_advance_ledger(tmp_path):
    api = state_api()
    store = api.SqliteStateStore(tmp_path / "state.db")
    lease = await store.create("run", {})
    try:
        with pytest.raises(api.StateStoreError) as bad_start:
            await store.begin_call(lease, "call", {"status": "completed"})
        assert bad_start.value.code == "invalid_record"
        await store.begin_call(lease, "call", {"status": "started"})
        with pytest.raises(api.StateStoreError):
            await store.finish_call(lease, "call", {"status": "started"})
        assert (await store.load(lease)).calls == {"call": {"status": "started"}}
    finally:
        await store.aclose()


@pytest.mark.parametrize("version", [0, 1, 200])
async def test_unsupported_schema_is_not_mutated(tmp_path, version):
    api = state_api()
    path = tmp_path / "future.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE future_data (value TEXT)")
        connection.execute("INSERT INTO future_data VALUES ('preserve-me')")
        connection.execute(f"PRAGMA user_version={version}")
    original = path.read_bytes()
    store = api.SqliteStateStore(path)
    try:
        with pytest.raises(api.StateStoreError) as mismatch:
            await store.acquire("run")
        assert mismatch.value.code == "schema_mismatch"
        assert path.read_bytes() == original
        assert not await asyncio.to_thread(Path(str(path) + "-wal").exists)
    finally:
        await store.aclose()


@pytest.mark.parametrize("target", ["state", "call"])
async def test_integrity_failure_is_reported_before_returning_content(tmp_path, target):
    api = state_api()
    path = tmp_path / "state.db"
    store = api.SqliteStateStore(path)
    lease = await store.create("run", {"safe": True})
    try:
        await store.begin_call(lease, "call", {"status": "started"})
        with sqlite3.connect(path) as connection:
            if target == "state":
                connection.execute("UPDATE run_states SET state_json='{}'")
            else:
                connection.execute("UPDATE call_records SET value_json='{}'")
        with pytest.raises(api.StateStoreError) as corrupt:
            await store.load(lease)
        assert corrupt.value.code == "corrupt_state"
    finally:
        await store.aclose()


async def test_cross_process_owner_crash_releases_advisory_lock(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    owner = api.SqliteStateStore(path)
    lease = await owner.create("run", {})
    await owner.release(lease)
    program = """
import asyncio, sys
from lake_agent.state import SqliteStateStore
async def main():
    store = SqliteStateStore(sys.argv[1])
    await store.acquire('run')
    print('owned', flush=True)
    await asyncio.Future()
asyncio.run(main())
"""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        program,
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert await asyncio.wait_for(child.stdout.readline(), timeout=5) == b"owned\n"
        with pytest.raises(api.StateStoreError) as conflict:
            await owner.acquire("run")
        assert conflict.value.code == "lease_conflict"
        child.kill()
        await asyncio.wait_for(child.wait(), timeout=5)
        recovered = await owner.acquire("run")
        assert (await owner.load(recovered)).state == {}
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
        await owner.aclose()


async def test_private_files_and_idempotent_close_release_ownership(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    store = api.SqliteStateStore(path)
    lease = await store.create("../path/does/not/escape", {})
    assert (await asyncio.to_thread(path.stat)).st_mode & 0o777 == 0o600
    lock_dir = Path(str(path) + ".locks")
    assert (await asyncio.to_thread(lock_dir.stat)).st_mode & 0o777 == 0o700
    modes = await asyncio.to_thread(lambda: [p.stat().st_mode & 0o777 for p in lock_dir.iterdir()])
    assert all(mode == 0o600 for mode in modes)
    await store.aclose()
    await store.aclose()
    await store.release(lease)
    with pytest.raises(api.StateStoreError) as closed:
        await store.load(lease)
    assert closed.value.code == "store_closed"
    second = api.SqliteStateStore(path)
    try:
        await second.acquire(lease.run_id)
    finally:
        await second.aclose()


async def test_cancelled_save_waits_for_sqlite_transaction_before_return(tmp_path):
    """取消后不得遗留与新租约所有者竞争的后台写入任务。"""
    api = state_api()
    path = tmp_path / "state.db"
    store = api.SqliteStateStore(path)
    lease = await store.create("run", {"v": 1})
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    task = asyncio.create_task(store.save(lease, {"v": 2}))
    try:
        # 已持有的 SQLite 写锁为真实工作线程提供确定性的同步屏障。
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        blocker.execute("ROLLBACK")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        await store.release(lease)
        recovered = await store.acquire("run")
        assert (await store.load(recovered)).state == {"v": 2}
    finally:
        if blocker.in_transaction:
            blocker.execute("ROLLBACK")
        blocker.close()
        await store.aclose()


async def test_cancelled_acquire_does_not_leak_an_unreturned_lease(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    store = api.SqliteStateStore(path)
    initial = await store.create("run", {})
    await store.release(initial)
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    task = asyncio.create_task(store.acquire("run"))
    second = api.SqliteStateStore(path)
    try:
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        blocker.execute("ROLLBACK")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        lease = await second.acquire("run")
        assert (await second.load(lease)).state == {}
    finally:
        if blocker.in_transaction:
            blocker.execute("ROLLBACK")
        blocker.close()
        await second.aclose()
        await store.aclose()


async def test_first_initialization_can_race_across_store_instances(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    stores = [api.SqliteStateStore(path) for _ in range(8)]
    try:
        leases = await asyncio.gather(
            *(store.create(f"run-{index}", {"index": index}) for index, store in enumerate(stores))
        )
        loaded = await asyncio.gather(
            *(store.load(lease) for store, lease in zip(stores, leases, strict=True))
        )
        assert [item.state for item in loaded] == [{"index": index} for index in range(8)]
    finally:
        await asyncio.gather(*(store.aclose() for store in stores))


async def test_repeated_cancellation_still_drains_worker_and_releases_created_lease(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    store = api.SqliteStateStore(path)
    initial = await store.create("initial", {})
    await store.release(initial)
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    task = asyncio.create_task(store.create("new", {"durable": True}))
    second = api.SqliteStateStore(path)
    try:
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        blocker.execute("ROLLBACK")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
        recovered = await second.acquire("new")
        assert (await second.load(recovered)).state == {"durable": True}
    finally:
        if blocker.in_transaction:
            blocker.execute("ROLLBACK")
        blocker.close()
        await second.aclose()
        await store.aclose()


@pytest.mark.parametrize("bad_token", [[], None, 4])
async def test_malformed_leases_are_rejected_without_leaking_type_errors(tmp_path, bad_token):
    api = state_api()
    store = api.SqliteStateStore(tmp_path / "state.db")
    lease = await store.create("run", {})
    try:
        malformed = api.StateLease("run", bad_token)
        for operation in (store.load, store.release):
            with pytest.raises(api.StateStoreError) as invalid:
                await operation(malformed)
            assert invalid.value.code == "lease_invalid"
        assert (await store.load(lease)).state == {}
    finally:
        await store.aclose()


async def test_failed_release_still_closes_the_os_lock(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    first, second = api.SqliteStateStore(path), api.SqliteStateStore(path)
    lease = await first.create("run", {})
    try:
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE run_states SET lease_token='external-token'")
        with pytest.raises(api.StateStoreError) as stale:
            await first.release(lease)
        assert stale.value.code == "lease_invalid"
        acquired = await second.acquire("run")
        assert (await second.load(acquired)).state == {}
    finally:
        await first.aclose()
        await second.aclose()


async def test_deeply_malformed_persisted_json_is_a_sanitized_corruption_error(tmp_path):
    api = state_api()
    path = tmp_path / "state.db"
    store = api.SqliteStateStore(path)
    lease = await store.create("run", {})
    # 校验和有效，不能使无法解码的载荷变成有效状态。
    text = '{"value":' * 10000 + "null" + "}" * 10000
    digest = hashlib.sha256(text.encode()).hexdigest()
    try:
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE run_states SET state_json=?,state_hash=?", (text, digest))
        with pytest.raises(api.StateStoreError) as error:
            await store.load(lease)
        assert error.value.code == "corrupt_state"
        assert "value" not in str(error.value)
    finally:
        await store.aclose()


async def test_concurrent_initializer_between_schema_reads_is_accepted(tmp_path, monkeypatch):
    """在第一次真实连接的两次模式预检读取之间将其暂停。"""
    api = state_api()
    path = tmp_path / "state.db"
    first, second = api.SqliteStateStore(path), api.SqliteStateStore(path)
    entered, proceed = threading.Event(), threading.Event()
    original_connect = sqlite3.connect
    first_connection = True

    def gated_connect(*args, **kwargs):
        nonlocal first_connection
        connection = original_connect(*args, **kwargs)
        if first_connection:
            first_connection = False

            def gate(sql):
                if sql.startswith("SELECT 1 FROM sqlite_master"):
                    entered.set()
                    proceed.wait(timeout=5)

            connection.set_trace_callback(gate)
        return connection

    monkeypatch.setattr(sqlite3, "connect", gated_connect)
    task = asyncio.create_task(first.create("first", {}))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        await second.create("second", {})
        proceed.set()
        lease = await asyncio.wait_for(task, timeout=5)
        assert (await first.load(lease)).state == {}
    finally:
        proceed.set()
        await asyncio.gather(task, return_exceptions=True)
        await first.aclose()
        await second.aclose()


async def test_wal_initialization_retries_transient_sqlite_lock(tmp_path):
    """SQLite 日志模式切换可能不遵守常规的繁忙超时设置。"""
    api = state_api()
    path = tmp_path / "state.db"
    initial = api.SqliteStateStore(path)
    await initial.create("run", {})
    await initial.aclose()
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("PRAGMA journal_mode=DELETE")
    blocker.execute("BEGIN")
    blocker.execute("SELECT * FROM run_states").fetchall()
    store = api.SqliteStateStore(path)
    task = asyncio.create_task(store.acquire("run"))
    try:
        await asyncio.sleep(0.05)
        assert not task.done()
        blocker.execute("ROLLBACK")
        lease = await asyncio.wait_for(task, timeout=5)
        assert (await store.load(lease)).state == {}
    finally:
        if blocker.in_transaction:
            blocker.execute("ROLLBACK")
        blocker.close()
        await asyncio.gather(task, return_exceptions=True)
        await store.aclose()
