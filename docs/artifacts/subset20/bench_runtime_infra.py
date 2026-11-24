"""Zero-API-cost microbenchmark for Agent Runtime infrastructure guarantees.

Measures four production runtime mechanisms without external LLM calls:
1. Safe parallel tool scheduling & exclusive-resource mutual exclusion
2. SQLite durable crash recovery (zero redispatch) & fail-closed unknown-outcome guard
3. Persistent Missions multi-cycle scheduling, hard budget cutoff, and bounded RunContinuation
4. Read-only SQLGlot AST sandbox interception & 16 MiB file-size guard
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path

from pydantic import ValidationError

from lake_agent import models as m
from lake_agent.adapters._data_query import normalize_query
from lake_agent.adapters.data_adapter import LocalDataPort
from lake_agent.missions import models as mm
from lake_agent.missions.store import MissionStore
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink
from tests.support.agent import ActionPlanner, FlexibleContent, MemoryTrace, tool_action
from tests.support.ports import DemoData

ROOT = Path(__file__).resolve().parent
OUT_JSON = ROOT / "runtime-infra-benchmark.json"


async def bench_parallel() -> dict:
    """Compare serial vs parallel_safe=True vs exclusive_resource_keys mutual exclusion."""
    delay_s = 0.08
    source_ids = [f"demo-slot-{i}" for i in range(4)]
    steps = [
        {
            "step_id": f"s-{i}",
            "tool_name": "delayed_read",
            "source_ids": [source_ids[i]],
            "args": {"source_id": source_ids[i], "slot": i},
            "depends_on": [],
            "purpose": f"Read slot {i}",
        }
        for i in range(4)
    ]

    async def measure(parallel_safe: bool, exclusive_keys: list[str]) -> dict:
        active = 0
        max_active = 0

        class DelayedData(DemoData):
            async def catalog(self, request):
                return m.CatalogResult(
                    call_id=request.call_id,
                    catalog_snapshot_id="snap-par",
                    sources=[
                        m.SourceRecord(
                            source_id=sid, name=f"{sid}.csv", format="csv", summary=f"Slot {sid}"
                        )
                        for sid in source_ids
                    ],
                )

            def tool_specs(self):
                return [
                    m.ToolSpec(
                        name="delayed_read",
                        description="Simulated I/O tool with fixed latency.",
                        idempotent=True,
                        uses_sources=True,
                        source_id_paths=["/source_id"],
                        parallel_safe=parallel_safe,
                        exclusive_resource_keys=exclusive_keys,
                        input_schema={
                            "type": "object",
                            "properties": {
                                "source_id": {"type": "string", "minLength": 1},
                                "slot": {"type": "integer"},
                            },
                            "required": ["source_id", "slot"],
                            "additionalProperties": False,
                        },
                    )
                ]

            async def invoke(self, request):
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                try:
                    await asyncio.sleep(delay_s)
                    slot = request.args["slot"]
                    return m.ToolResult(
                        call_id=request.call_id,
                        data={
                            "evidence": [
                                {
                                    "source_id": request.args["source_id"],
                                    "content_type": "text",
                                    "payload": f"slot-{slot}",
                                    "locator": {"slot": slot},
                                }
                            ]
                        },
                    )
                finally:
                    active -= 1

        content = FlexibleContent()
        planner = ActionPlanner([{"kind": "tools", "steps": steps}, {"kind": "compose"}, {"kind": "finish"}])
        t0 = time.perf_counter()
        res = await run(
            m.RunRequest(
                task_id=f"bench-par-{parallel_safe}-{bool(exclusive_keys)}",
                question="Read four independent slots.",
                config=m.RunConfig(max_parallel_tool_calls=4),
            ),
            data=DelayedData(),
            content=content,
            planner=planner,
            trace=MemoryTrace(),
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        ev_order = [ev.payload for ev in content.compositions[0].evidence]
        return {
            "status": res.status,
            "elapsed_ms": round(elapsed_ms, 1),
            "max_concurrent_observed": max_active,
            "deterministic_evidence_order": ev_order == [f"slot-{i}" for i in range(4)],
        }

    serial = await measure(parallel_safe=False, exclusive_keys=[])
    parallel = await measure(parallel_safe=True, exclusive_keys=[])
    mutex = await measure(parallel_safe=True, exclusive_keys=["duckdb-shared"])
    return {
        "steps_count": 4,
        "simulated_io_ms_per_step": int(delay_s * 1000),
        "serial": serial,
        "parallel_safe": parallel,
        "exclusive_mutex": mutex,
        "parallel_speedup": round(serial["elapsed_ms"] / parallel["elapsed_ms"], 2),
    }


async def bench_recovery() -> dict:
    """Measure SQLite crash recovery (zero redispatch) and fail-closed unknown-outcome guard."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "state.sqlite3"
        store = SqliteStateStore(db)
        try:
            # Scenario A: interrupt cleanly right after evidence.added (between committed calls), then resume()
            class CountingData(DemoData):
                def __init__(self):
                    super().__init__()
                    self.catalog_calls = 0
                    self.tool_calls = 0

                async def catalog(self, request):
                    self.catalog_calls += 1
                    return await super().catalog(request)

                async def invoke(self, request):
                    self.tool_calls += 1
                    return await super().invoke(request)

            class CrashAfterEvidenceTrace(JsonlTraceSink):
                async def emit(self, event):
                    await super().emit(event)
                    if event.event_type == "evidence.added":
                        raise asyncio.CancelledError

            tool_step = tool_action()
            data1 = CountingData()
            try:
                await run(
                    m.RunRequest(run_id="run-recover-ok", task_id="t-rec-1", question="Recover cleanly"),
                    data=data1,
                    content=FlexibleContent(),
                    planner=ActionPlanner([tool_step, {"kind": "compose"}, {"kind": "finish"}]),
                    trace=CrashAfterEvidenceTrace(Path(tmp) / "trace-ok-1"),
                    state_store=store,
                )
            except asyncio.CancelledError:
                pass

            # Resume after process restart with fresh CountingData
            data2 = CountingData()
            content2 = FlexibleContent()
            t0 = time.perf_counter()
            resumed = await resume(
                "run-recover-ok",
                data=data2,
                content=content2,
                planner=ActionPlanner([tool_step, {"kind": "compose"}, {"kind": "finish"}]),
                trace=JsonlTraceSink(Path(tmp) / "trace-ok-2"),
                state_store=store,
            )
            resume_ms = round((time.perf_counter() - t0) * 1000, 1)

            # Scenario B: interrupt mid-flight during an active tool call (unknown outcome -> fail-closed)
            class HangingToolData(CountingData):
                async def invoke(self, request):
                    self.tool_calls += 1
                    raise asyncio.CancelledError

            data3 = HangingToolData()
            try:
                await run(
                    m.RunRequest(run_id="run-recover-unknown", task_id="t-rec-2", question="In-flight crash"),
                    data=data3,
                    content=FlexibleContent(),
                    planner=ActionPlanner([tool_step, {"kind": "compose"}, {"kind": "finish"}]),
                    trace=JsonlTraceSink(Path(tmp) / "trace-unk-1"),
                    state_store=store,
                )
            except asyncio.CancelledError:
                pass

            data4 = CountingData()
            fail_closed_code = None
            try:
                await resume(
                    "run-recover-unknown",
                    data=data4,
                    content=FlexibleContent(),
                    planner=ActionPlanner([tool_step, {"kind": "compose"}, {"kind": "finish"}]),
                    trace=JsonlTraceSink(Path(tmp) / "trace-unk-2"),
                    state_store=store,
                )
            except Exception as exc:
                fail_closed_code = getattr(exc, "code", type(exc).__name__)

            return {
                "clean_crash_recovery": {
                    "initial_catalog_calls": data1.catalog_calls,
                    "initial_tool_calls": data1.tool_calls,
                    "resumed_status": resumed.status,
                    "resumed_new_catalog_calls": data2.catalog_calls,
                    "resumed_new_tool_calls": data2.tool_calls,
                    "evidence_preserved": bool(content2.compositions and content2.compositions[0].evidence),
                    "resume_ms": resume_ms,
                },
                "inflight_unknown_fail_closed": {
                    "fail_closed_triggered": fail_closed_code is not None,
                    "error_code": fail_closed_code,
                    "resumed_new_tool_calls": data4.tool_calls,
                },
            }
        finally:
            await store.aclose()


async def bench_missions() -> dict:
    """Measure Persistent Missions multi-cycle handoff, hard budget cutoff, and bounded continuation."""
    with tempfile.TemporaryDirectory() as tmp:
        store = MissionStore(Path(tmp) / "missions.sqlite3")
        await store.start()
        try:
            now = 1_700_000_000.0
            mission = await store.create(
                mm.MissionCreate(
                    title="Quarterly lake monitor",
                    question="Track revenue changes across quarters.",
                    profile_id="lakeqa-mini",
                    interval_seconds=60,
                    start_immediately=False,
                    stop_on_completed=False,
                    limits=mm.MissionLimits(
                        max_runs=3,
                        max_model_calls=10,
                        max_data_calls=15,
                        max_duration_seconds=3600,
                    ),
                ),
                profile_id="lakeqa-mini",
                profile_signature="sig-v1",
                now=now,
            )
            mid = mission["mission_id"]

            cycles_completed = 0
            continuation_bytes = []
            prev_run_id = None
            for cycle in range(1, 4):
                trig = await store.enqueue(mid, f"evt-{cycle}", now=now + cycle)
                claimed = await store.claim(
                    trig["trigger_id"], max_model_calls=3, max_data_calls=4, now=now + cycle
                )
                assert claimed is not None
                cont = m.RunContinuation(
                    mission_id=mid,
                    cycle=cycle,
                    previous_run_id=prev_run_id,
                    summary=f"Cycle {cycle - 1} summary with verified leads.",
                    pending_items=[f"Check quarter Q{cycle}"],
                    artifact_refs=[f"art-{cycle - 1}"] if prev_run_id else [],
                )
                cont_len = len(json.dumps(cont.model_dump(), separators=(",", ":")).encode("utf-8"))
                continuation_bytes.append(cont_len)
                req = m.RunRequest(
                    run_id=claimed["run_id"],
                    task_id=f"mission-{cycle}",
                    question="Check quarterly revenue.",
                    continuation=cont,
                )
                await store.mark_running(
                    trig["trigger_id"], request=req.model_dump(mode="json"), now=now + cycle
                )
                res = await run(
                    req,
                    data=DemoData(),
                    content=FlexibleContent(),
                    planner=ActionPlanner([{"kind": "compose"}, {"kind": "finish"}]),
                    trace=MemoryTrace(),
                )
                assert res.status == "completed"
                await store.settle(
                    trig["trigger_id"],
                    model_calls=2,
                    data_calls=1,
                    run_status=res.status,
                    stop_reason=res.stop_reason,
                    handoff=mm.MissionHandoff(
                        summary=f"Completed cycle {cycle}",
                        pending_items=[],
                        artifact_refs=[],
                    ),
                    now=now + cycle + 1,
                )
                prev_run_id = res.run_id
                cycles_completed += 1

            # Verify 4th cycle is strictly blocked by max_runs=3 (auto-closed at limit)
            fourth_rejected_code = None
            try:
                trig4 = await store.enqueue(mid, "evt-4", now=now + 10)
                claimed4 = await store.claim(
                    trig4["trigger_id"], max_model_calls=1, max_data_calls=1, now=now + 10
                )
                if claimed4 is None:
                    fourth_rejected_code = "claim_denied"
            except mm.MissionError as exc:
                fourth_rejected_code = exc.code
            final_state = await store.get(mid)

            # Verify oversized continuation (> 4096 bytes) is rejected
            oversized_rejected = False
            try:
                m.RunContinuation(
                    mission_id=mid,
                    cycle=4,
                    previous_run_id=prev_run_id,
                    summary="x" * 2000,
                    pending_items=["p" * 256 for _ in range(8)],
                    artifact_refs=["a" * 200 for _ in range(8)],
                )
            except ValidationError:
                oversized_rejected = True

            return {
                "cycles_completed": cycles_completed,
                "max_runs_limit": 3,
                "mission_status_at_limit": final_state.get("status"),
                "fourth_cycle_rejected_code": fourth_rejected_code,
                "budget_overruns": 0 if fourth_rejected_code is not None else 1,
                "used_runs": final_state.get("used_runs", final_state.get("completed_runs", cycles_completed)),
                "used_model_calls": final_state.get("used_model_calls"),
                "used_data_calls": final_state.get("used_data_calls"),
                "continuation_bytes_max": max(continuation_bytes),
                "continuation_limit_bytes": 4096,
                "oversized_continuation_rejected": oversized_rejected,
            }
        finally:
            await store.aclose()


async def bench_sql_sandbox() -> dict:
    """Measure read-only SQLGlot AST sandbox rejection rate, latency, and 16 MiB file guard."""
    adversarial = [
        ("external_file_read", "SELECT * FROM read_csv_auto('/etc/passwd')"),
        ("unauthorized_table", "SELECT * FROM users"),
        ("schema_prefix", "SELECT * FROM main.data"),
        (
            "recursive_cte",
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5) SELECT * FROM t",
        ),
        ("ddl_drop", "DROP TABLE data"),
        ("dml_delete", "DELETE FROM data"),
        ("system_func", "SELECT getenv('HOME') FROM data"),
        ("multi_statement", "SELECT * FROM data; SELECT 1"),
    ]
    allowed = [
        ("basic_select", "SELECT * FROM data LIMIT 5"),
        (
            "lakeqa_agg",
            'SELECT substr("Draw Date",7,4) AS yr, AVG("Bonus #"), SQRT(AVG("Bonus #")) FROM data GROUP BY 1',
        ),
        ("non_recursive_cte", "WITH sub AS (SELECT COUNT(*) AS n FROM data) SELECT n FROM sub"),
    ]

    t0 = time.perf_counter()
    adv_blocked = sum(normalize_query(sql) is None for _, sql in adversarial)
    adv_ms = (time.perf_counter() - t0) * 1000 / len(adversarial)

    ok_passed = sum(normalize_query(sql) is not None for _, sql in allowed)

    # Verify >16 MiB file is rejected cleanly with source_too_large
    with tempfile.TemporaryDirectory() as tmp:
        big_csv = Path(tmp) / "big.csv"
        big_csv.write_bytes(b"a,b\n" + b"1,2\n" * (4 * 1024 * 1024 + 100))
        port = LocalDataPort(tmp)
        req = m.ToolRequest(
            run_id="r-sql",
            task_id="t-sql",
            operation_id="op-1",
            call_id="call-1",
            timeout_ms=5000,
            max_output_bytes=8192,
            step_id="s-1",
            catalog_snapshot_id="local-snapshot-v1",
            tool_name="query",
            args={"source_id": "big.csv", "sql": "SELECT * FROM data LIMIT 1"},
        )
        reply = await port.invoke(req)
        oversized_code = (reply.get("error") or {}).get("code")

    return {
        "adversarial_total": len(adversarial),
        "adversarial_blocked": adv_blocked,
        "adversarial_block_rate_pct": round(adv_blocked / len(adversarial) * 100, 1),
        "avg_rejection_latency_ms": round(adv_ms, 3),
        "legitimate_total": len(allowed),
        "legitimate_allowed": ok_passed,
        "oversized_file_guard_code": oversized_code,
    }


async def main() -> None:
    report = {
        "parallel_scheduling": await bench_parallel(),
        "durable_recovery": await bench_recovery(),
        "persistent_missions": await bench_missions(),
        "sql_sandbox": await bench_sql_sandbox(),
    }
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
