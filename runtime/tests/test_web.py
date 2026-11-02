"""Offline web acceptance checks across HTTP, the real harness, SQLite and JSONL.

Every application owns a temporary data directory and runs its explicit ASGI
lifespan. Adapter gates only delay real local fixture calls; they never replace
controller, checkpoint, trace or report behavior with fabricated responses.
"""

from __future__ import annotations

import asyncio
import html
import importlib
from contextlib import asynccontextmanager

import httpx
import pytest

from lake_agent import models as m
from lake_agent.demo import DemoData

pytest.importorskip("fastapi", reason="web acceptance tests require the optional web extra")


def make_app(data_dir, *, max_active_runs=2):
    """Resolve the new public factory during execution, retaining useful REDs."""
    try:
        module = importlib.import_module("lake_agent.web.app")
    except ModuleNotFoundError as exc:
        if exc.name in {"lake_agent.web", "lake_agent.web.app"}:
            pytest.fail("The optional web application factory has not been implemented")
        raise
    return module.create_app(data_dir=data_dir, max_active_runs=max_active_runs)


@asynccontextmanager
async def running_client(data_dir, *, max_active_runs=2):
    """HTTPX does not dispatch lifespan itself, so always open and drain it here."""
    app = make_app(data_dir, max_active_runs=max_active_runs)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield client


async def create_run(client, *, scenario="standard", question=None, mode="full_agent"):
    """Submit a synthetic question and return only an accepted run identity."""
    response = await client.post(
        "/api/runs",
        json={
            "question": question or "What was 2024 annual revenue?",
            "scenario": scenario,
            "mode": mode,
        },
    )
    assert response.status_code == 202, response.text
    summary = response.json()
    assert summary["run_id"]
    return summary["run_id"]


async def wait_stopped(client, run_id):
    """Bound polling and expose the final HTTP view when progress stops."""
    async with asyncio.timeout(8):
        while True:
            response = await client.get(f"/api/runs/{run_id}")
            assert response.status_code == 200, response.text
            view = response.json()
            if not view["active"]:
                return view
            await asyncio.sleep(0.01)


async def test_standard_run_executes_harness_and_exposes_evidence(tmp_path):
    """A cosmetic success response cannot replace actual trace and provenance."""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        view = await wait_stopped(client, run_id)

    assert view["status"] == "completed"
    assert view["result"]["status"] == "completed"
    assert "120 million HKD" in view["result"]["answer"]["text"]
    assert view["metrics"]["tools"] == 1
    assert len(view["evidence"]) == 1
    assert view["evidence"][0]["source_id"] == "demo-annual"
    assert view["result"]["answer"]["citations"] == [view["evidence"][0]["evidence_id"]]
    assert {event["event_type"] for event in view["events"]} >= {
        "run.started",
        "tool.call.completed",
        "evidence.added",
        "run.completed",
    }
    assert view["active"] is view["can_resume"] is False
    expected_metrics = {
        "tools": 1,
        "steps": 1,
        "model_calls": 0,
        "evidence": 1,
        "attempts": 1,
        "duration_ms": view["result"]["duration_ms"],
    }
    assert {key: view["metrics"][key] for key in expected_metrics} == expected_metrics
    assert len(view["attempts"]) == 1
    m.RunResult.model_validate(view["result"])
    for evidence in view["evidence"]:
        m.EvidenceRecord.model_validate(evidence)
    assert {m.SourceRecord.model_validate(source).source_id for source in view["sources"]} >= {
        "demo-annual"
    }
    for event in view["events"]:
        assert event["attempt_id"]
        assert event["run_id"] == run_id
        m.TraceEvent.model_validate(
            {key: value for key, value in event.items() if key != "attempt_id"}
        )


async def test_info_describes_offline_scenarios_and_actual_modes_work(tmp_path):
    """The offline promise and mode selector must match executable behavior."""
    async with running_client(tmp_path) as client:
        info = await client.get("/api/info")
        assert info.status_code == 200
        payload = info.json()
        assert payload["demo_only"] is True
        assert payload["version"]
        assert {item["id"] for item in payload["scenarios"]} == {"standard", "parallel", "recovery"}
        for scenario in payload["scenarios"]:
            assert all(scenario[field] for field in ("title", "description", "question"))
        assert payload["capabilities"]
        for capability in payload["capabilities"]:
            assert all(capability[field] for field in ("name", "status", "description"))
        for mode in ("full_agent", "no_verifier", "fixed_retrieval"):
            run_id = await create_run(client, mode=mode)
            view = await wait_stopped(client, run_id)
            assert view["status"] == "completed", view
            assert view["result"]["mode"] == mode
            assert view["metrics"]["model_calls"] == 0


class InvokeGate:
    """Hold the real local extractor to observe scheduling and cancellation."""

    def __init__(self, monkeypatch, *, expected=1):
        self.requests = []
        self.expected = expected
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.peak = 0
        original = DemoData.invoke

        async def invoke(data, request):
            self.requests.append(request)
            self.active += 1
            self.peak = max(self.peak, self.active)
            if len(self.requests) >= self.expected:
                self.entered.set()
            try:
                await self.release.wait()
                return await original(data, request)
            finally:
                self.active -= 1

        monkeypatch.setattr(DemoData, "invoke", invoke)

    async def wait_entered(self):
        """A serial or missing tool dispatch fails within a bounded interval."""
        async with asyncio.timeout(4):
            await self.entered.wait()


async def test_parallel_scenario_overlaps_real_disjoint_tool_calls(tmp_path, monkeypatch):
    """Two tools must enter before either response is released to the harness."""
    gate = InvokeGate(monkeypatch, expected=2)
    async with running_client(tmp_path) as client:
        run_id = await create_run(client, scenario="parallel")
        await gate.wait_entered()
        assert gate.active == gate.peak == 2
        assert {request.args["source_id"] for request in gate.requests} == {
            "demo-annual",
            "demo-quarter",
        }
        gate.release.set()
        view = await wait_stopped(client, run_id)
    assert view["status"] == "completed"
    assert view["metrics"]["tools"] == view["metrics"]["steps"] == 2
    assert {item["source_id"] for item in view["evidence"]} == {"demo-annual", "demo-quarter"}
    assert gate.active == 0


async def test_safe_recovery_keeps_provenance_budget_and_history_after_restart(tmp_path):
    """Persisted success is replayed with stable IDs, never sent to tools again."""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client, scenario="recovery")
        paused = await wait_stopped(client, run_id)
        assert paused["status"] == "paused", paused
        assert paused["can_resume"] is True
        assert len(paused["evidence"]) == 1
        original_evidence = paused["evidence"][0]
        original_call = original_evidence["call_id"]
        initial_logs = {path: path.read_bytes() for path in tmp_path.rglob("events.jsonl")}
        assert initial_logs

    async with running_client(tmp_path) as client:
        history = await client.get("/api/runs")
        assert history.status_code == 200
        assert history.json()["runs"][0]["run_id"] == run_id
        restored = (await client.get(f"/api/runs/{run_id}")).json()
        assert restored["status"] == "paused"
        assert restored["evidence"] == paused["evidence"]
        response = await client.post(f"/api/runs/{run_id}/resume")
        assert response.status_code == 202, response.text
        completed = await wait_stopped(client, run_id)
        assert completed["status"] == "completed", completed
        assert completed["can_resume"] is False
        assert completed["evidence"] == [original_evidence]
        assert completed["metrics"]["tools"] == completed["metrics"]["steps"] == 1
        assert completed["result"]["call_counts"]["tools"] == 1
        assert completed["metrics"]["attempts"] == len(completed["attempts"]) == 2
        tool_events = [
            event for event in completed["events"] if event["event_type"] == "tool.call.completed"
        ]
        assert {event["call_id"] for event in tool_events} == {original_call}
        assert len([event for event in tool_events if not event["detail"].get("replayed")]) == 1
        assert any(event["detail"].get("replayed") for event in tool_events)
        attempt_ids = list(dict.fromkeys(event["attempt_id"] for event in completed["events"]))
        assert len(attempt_ids) == 2
        for attempt_id in attempt_ids:
            events = [event for event in completed["events"] if event["attempt_id"] == attempt_id]
            assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
        terminal = await client.post(f"/api/runs/{run_id}/resume")
        assert terminal.status_code == 409
        assert_error_detail(terminal)
        assert (await client.get(f"/api/runs/{run_id}")).json()["result"] == completed["result"]

    for path, original_bytes in initial_logs.items():
        assert path.read_bytes() == original_bytes


def assert_error_detail(response):
    """Public errors stay structured and contain no raw Python exception dumps."""
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert isinstance(detail["code"], str) and detail["code"]
    assert isinstance(detail["message"], str) and detail["message"]
    assert "Traceback" not in detail["message"]


@pytest.mark.parametrize(
    "change",
    [
        {"question": ""},
        {"question": "x" * 2001},
        {"question": None},
        {"question": 12},
        {"scenario": "external-service"},
        {"mode": "paid-model"},
    ],
)
async def test_invalid_run_requests_are_rejected_without_creating_history(tmp_path, change):
    """Malformed public input cannot create an executable or persisted run."""
    payload = {"question": "Revenue?", "scenario": "standard", "mode": "full_agent", **change}
    async with running_client(tmp_path) as client:
        response = await client.post("/api/runs", json=payload)
        assert response.status_code == 422
        assert_error_detail(response)
        assert (await client.get("/api/runs")).json() == {"runs": []}


async def test_malformed_json_and_unknown_ids_return_safe_http_errors(tmp_path):
    """Invalid JSON and opaque path components must not escape as server errors."""
    async with running_client(tmp_path) as client:
        malformed = await client.post(
            "/api/runs", content="{", headers={"Content-Type": "application/json"}
        )
        assert malformed.status_code == 422
        assert_error_detail(malformed)
        for suffix in ("", "/export", "/report", "/artifacts/initial/missing"):
            response = await client.get(f"/api/runs/no-such-run{suffix}")
            assert response.status_code == 404
            assert_error_detail(response)
        for action in ("resume", "cancel"):
            response = await client.post(f"/api/runs/no-such-run/{action}")
            assert response.status_code == 404
            assert_error_detail(response)
        traversal = await client.get("/api/runs/..%2F..%2Fprivate")
        assert traversal.status_code == 404


@pytest.mark.parametrize(
    "origin", ["https://evil.example", "http://testserver:8001", "null", "not-an-origin"]
)
async def test_foreign_or_invalid_origins_cannot_create_runs(tmp_path, origin):
    """An unrelated website cannot launch work on the local demo service."""
    async with running_client(tmp_path) as client:
        response = await client.post(
            "/api/runs",
            json={"question": "Revenue?", "scenario": "standard"},
            headers={"Origin": origin},
        )
        assert response.status_code == 403
        assert_error_detail(response)
        assert "access-control-allow-origin" not in response.headers
        assert (await client.get("/api/runs")).json() == {"runs": []}
        same_origin = await client.post(
            "/api/runs",
            json={"question": "Revenue?", "scenario": "standard"},
            headers={"Origin": "http://testserver"},
        )
        assert same_origin.status_code == 202
        run_id = same_origin.json()["run_id"]
        view = await wait_stopped(client, run_id)
        assert view["result"]["mode"] == "full_agent"
        for action in ("cancel", "resume"):
            forbidden = await client.post(
                f"/api/runs/{run_id}/{action}", headers={"Origin": origin}
            )
            assert forbidden.status_code == 403
            assert_error_detail(forbidden)


async def test_report_escapes_untrusted_content_and_export_preserves_json(tmp_path):
    """Shared reports render input as text while the JSON export remains lossless."""
    question = "<script>alert(1)</script><img src=x onerror=alert(2)> & revenue?"
    async with running_client(tmp_path) as client:
        run_id = await create_run(client, question=question)
        view = await wait_stopped(client, run_id)
        exported = await client.get(f"/api/runs/{run_id}/export")
        assert exported.status_code == 200
        assert "attachment" in exported.headers["content-disposition"]
        assert exported.json() == view
        assert exported.json()["question"] == question
        report = await client.get(f"/api/runs/{run_id}/report")
        assert report.status_code == 200
        assert "text/html" in report.headers["content-type"]
        assert "attachment" in report.headers["content-disposition"]
        assert html.escape(question) in report.text
        assert question not in report.text
        assert "<script" not in report.text.lower()
        assert 'src="http' not in report.text.lower()
        assert 'href="http' not in report.text.lower()


async def test_artifacts_require_same_run_attempt_and_recorded_reference(tmp_path):
    """A recorded reference grants one scoped read, never directory access."""
    async with running_client(tmp_path) as client:
        first_id = await create_run(client)
        first = await wait_stopped(client, first_id)
        event = next(
            event
            for event in first["events"]
            if event["event_type"] == "tool.call.completed" and event["data_ref"]
        )
        attempt_id, ref = event["attempt_id"], event["data_ref"]
        route = f"/api/runs/{first_id}/artifacts/{attempt_id}"
        artifact = await client.get(f"{route}/{ref}")
        assert artifact.status_code == 200
        assert artifact.json()["call_id"] == event["call_id"]
        # Plant an adjacent unreferenced JSON file: filename validity alone is
        # insufficient authority to expose storage through the API.
        files = list(tmp_path.rglob(f"{ref}.json"))
        assert len(files) == 1
        (files[0].parent / "unreferenced-test.json").write_text('{"private":true}')
        second_id = await create_run(client)
        await wait_stopped(client, second_id)
        history = (await client.get("/api/runs")).json()["runs"]
        assert [summary["run_id"] for summary in history] == [second_id, first_id]
        for path in (
            f"{route}/unreferenced-test",
            f"{route}/..%2F..%2Fprivate",
            f"/api/runs/{first_id}/artifacts/missing-attempt/{ref}",
            f"/api/runs/{second_id}/artifacts/{attempt_id}/{ref}",
        ):
            response = await client.get(path)
            assert response.status_code == 404, (path, response.text)


async def test_capacity_rejects_extra_run_and_frees_slot_after_cancel(tmp_path, monkeypatch):
    """Admission counts active owner tasks and cancellation returns their slots."""
    gate = InvokeGate(monkeypatch, expected=2)
    async with running_client(tmp_path, max_active_runs=2) as client:
        first_id = await create_run(client)
        second_id = await create_run(client)
        await gate.wait_entered()
        active_resume = await client.post(f"/api/runs/{first_id}/resume")
        assert active_resume.status_code == 409
        assert_error_detail(active_resume)
        rejected = await client.post(
            "/api/runs", json={"question": "Overflow?", "scenario": "standard"}
        )
        assert rejected.status_code == 429
        assert_error_detail(rejected)
        assert rejected.json()["detail"]["code"] == "capacity_reached"
        assert len((await client.get("/api/runs")).json()["runs"]) == 2
        cancelled = await client.post(f"/api/runs/{first_id}/cancel")
        assert cancelled.status_code == 202
        stopped = await wait_stopped(client, first_id)
        assert stopped["status"] == "interrupted"
        assert gate.active == 1
        third_id = await create_run(client)
        gate.release.set()
        assert (await wait_stopped(client, second_id))["status"] == "completed"
        assert (await wait_stopped(client, third_id))["status"] == "completed"
    assert gate.active == 0


async def test_manual_cancel_does_not_bypass_unknown_outcome_recovery(tmp_path, monkeypatch):
    """Cancelling after dispatch leaves unknown outcome refusal with no replay."""
    gate = InvokeGate(monkeypatch)
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        await gate.wait_entered()
        cancelled = await client.post(f"/api/runs/{run_id}/cancel")
        assert cancelled.status_code == 202
        stopped = await wait_stopped(client, run_id)
        assert stopped["status"] == "interrupted"
        assert gate.active == 0
        resumed = await client.post(f"/api/runs/{run_id}/resume")
        assert resumed.status_code == 202
        refused = await wait_stopped(client, run_id)
        assert refused["status"] == "blocked"
        assert refused["error"]["code"] == "recovery_outcome_unknown"
        assert refused["error"]["message"]
        assert refused["can_resume"] is False
        assert len(gate.requests) == 1


async def test_shutdown_drains_owned_tasks_and_retains_interrupted_history(tmp_path, monkeypatch):
    """Closing the service must observe cancellation before releasing its store."""
    gate = InvokeGate(monkeypatch)
    baseline = asyncio.all_tasks()
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        await gate.wait_entered()
        assert gate.active == 1
    await asyncio.sleep(0)
    assert gate.active == 0
    leaked = [task for task in asyncio.all_tasks() - baseline if not task.done()]
    assert leaked == [], f"Lifespan leaked tasks: {leaked}"
    async with running_client(tmp_path) as client:
        view = (await client.get(f"/api/runs/{run_id}")).json()
        assert view["status"] == "interrupted"
        assert view["active"] is False
        assert len(gate.requests) == 1
