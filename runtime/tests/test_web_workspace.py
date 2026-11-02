"""Offline acceptance checks for explicitly registered project workspaces.

These tests drive the actual ASGI application, controller, durable checkpoints
and trace projection. Local demo ports provide deterministic outputs while a
real asynchronous context manager records adapter ownership and cleanup.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
import pytest

from lake_agent import models as m
from lake_agent.demo import DemoContent, DemoData, DemoPlanner

pytest.importorskip("fastapi", reason="workspace acceptance requires the optional web extra")

_PRIVATE_CREDENTIAL = "workspace-test-credential-do-not-publish"


class OwnedDemoData(DemoData):
    """Exercise genuine local extraction while exposing context-owned lifetime."""

    def __init__(self):
        super().__init__()
        self.closed = False
        self.invocations = 0
        self.credential = _PRIVATE_CREDENTIAL

    async def invoke(self, request):
        assert self.closed is False, "The service reused an adapter after its context exited"
        self.invocations += 1
        return await super().invoke(request)


class PortOwner:
    """A trusted factory that allocates fresh offline ports and releases them."""

    def __init__(self):
        self.opened = []
        self.closed = []

    @asynccontextmanager
    async def ports(self):
        module = importlib.import_module("lake_agent.web.profiles")
        data = OwnedDemoData()
        ports = module.ExecutionPorts(data=data, content=DemoContent(), planner=DemoPlanner())
        self.opened.append(ports)
        try:
            yield ports
        finally:
            data.closed = True
            self.closed.append(ports)


def registered_profile(owner, *, revision="test-v1", sources=None, config=None, pause=False):
    """Register only offline objects; connected describes this trusted injection."""
    module = importlib.import_module("lake_agent.web.profiles")
    metadata = module.ProfileMetadata(
        profile_id="project-test",
        title="Registered local project",
        description="Explicit offline adapters for workspace acceptance.",
        kind="project",
        dataset_id="registered-test-data",
        dataset_name="Registered test data",
        revision=revision,
        modules={
            name: module.ModuleState(status="connected", label=f"Local {name} adapter")
            for name in ("data", "planner", "content")
        },
        suggested_questions=["What was 2024 annual revenue?"],
    )
    return module.ExecutionProfile(
        metadata=metadata,
        sources=tuple(DemoData().sources if sources is None else sources),
        ports_factory=owner.ports,
        run_config=config or m.RunConfig(max_wall_time_ms=600_000, model_id="registered-offline"),
        pause_after_evidence=pause,
    )


def assert_public_payload(value):
    """Credentials and callable/recovery implementation details are never public."""
    encoded = json.dumps(value)
    assert _PRIVATE_CREDENTIAL not in encoded
    for private in ("profile_signature", "ports_factory", "credentials"):
        assert private not in encoded


async def wait_stopped(client, run_id):
    """Bound waiting on real persisted execution rather than assuming scheduling."""
    async with asyncio.timeout(8):
        while True:
            response = await client.get(f"/api/runs/{run_id}")
            assert response.status_code == 200, response.text
            view = response.json()
            if not view["active"]:
                return view
            await asyncio.sleep(0.01)


async def submit_profile_run(client, profile_id="project-test"):
    """Use the public configuration selector and retain its saved identity."""
    response = await client.post(
        "/api/runs",
        json={"question": "What was 2024 annual revenue?", "profile_id": profile_id},
    )
    assert response.status_code == 202, response.text
    return response.json()["run_id"]


@asynccontextmanager
async def workspace_client(data_dir, **kwargs):
    """Open the real app lifespan so SQLite, jobs and resources are drained."""
    module = importlib.import_module("lake_agent.web.app")
    app = module.create_app(data_dir=data_dir, **kwargs)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield client


async def test_default_profile_catalog_marks_project_unavailable(tmp_path):
    """Missing real modules cannot silently run a demo as the project data pool."""
    async with workspace_client(tmp_path) as client:
        response = await client.get("/api/profiles")
        assert response.status_code == 200, response.text
        profiles = {item["profile_id"]: item for item in response.json()["profiles"]}
        assert set(profiles) == {
            "project-default",
            "demo-standard",
            "demo-parallel",
            "demo-recovery",
        }
        project = profiles["project-default"]
        assert project["kind"] == "project"
        assert project["ready"] is False
        assert project["unavailable_reason"]
        assert set(project["modules"]) == {"data", "planner", "content"}
        assert any(value["status"] == "unavailable" for value in project["modules"].values())
        for profile_id in ("demo-standard", "demo-parallel", "demo-recovery"):
            assert profiles[profile_id]["ready"] is True
            assert profiles[profile_id]["kind"] == "demo"
        info = (await client.get("/api/info")).json()
        assert info["application"] == "project-workspace"
        assert info["profiles"] == list(profiles.values())
        refused = await client.post(
            "/api/runs", json={"question": "Revenue?", "profile_id": "project-default"}
        )
        assert refused.status_code == 409, refused.text
        assert refused.json()["detail"]["message"]
        assert (await client.get("/api/runs")).json() == {"runs": []}


async def test_registered_profile_directory_is_public_and_does_not_open_ports(tmp_path):
    """Catalog reads expose registration metadata without constructing clients."""
    owner = PortOwner()
    profile = registered_profile(owner)
    async with workspace_client(tmp_path, profiles=(profile,)) as client:
        response = await client.get("/api/profiles")
        assert response.status_code == 200, response.text
        catalog = response.json()["profiles"]
        assert len(catalog) == 1
        public = catalog[0]
        assert public["profile_id"] == "project-test"
        assert public["ready"] is True
        assert public["source_count"] == 2
        assert public["model_id"] == "registered-offline"
        assert public["max_wall_time_ms"] == 600_000
        assert public["unavailable_reason"] is None
        response = await client.get("/api/profiles/project-test/sources")
        assert response.status_code == 200, response.text
        directory = response.json()
        assert directory["profile"] == public
        assert [source["source_id"] for source in directory["sources"]] == [
            "demo-annual",
            "demo-quarter",
        ]
        assert directory["sources"][0]["name"] == "synthetic-report.txt"
        for source in directory["sources"]:
            m.SourceRecord.model_validate(source)
        assert_public_payload({"catalog": catalog, "directory": directory})
        assert owner.opened == owner.closed == []


async def test_unknown_profile_and_conflicting_selectors_do_not_create_history(tmp_path):
    """A typo or ambiguous legacy selector cannot launch an unintended adapter."""
    async with workspace_client(tmp_path) as client:
        missing = await client.get("/api/profiles/no-such-profile/sources")
        assert missing.status_code == 404
        for payload, status in (
            ({"profile_id": "no-such-profile"}, 404),
            ({"profile_id": "demo-standard", "scenario": "standard"}, 422),
            ({"profile_id": "demo-standard", "ports_factory": "external.callable"}, 422),
        ):
            response = await client.post("/api/runs", json={"question": "Revenue?", **payload})
            assert response.status_code == status, response.text
            assert response.json()["detail"]["message"]
        assert (await client.get("/api/runs")).json() == {"runs": []}


async def test_registered_profile_uses_fresh_context_for_runs_and_resume(tmp_path):
    """Run and recovery own separate clients; all exits close the prior context."""
    owner = PortOwner()
    profile = registered_profile(owner, pause=True)
    async with workspace_client(tmp_path, profiles=(profile,)) as client:
        first_id = await submit_profile_run(client)
        paused = await wait_stopped(client, first_id)
        assert paused["status"] == "paused", paused
        assert paused["can_resume"] is True
        assert paused["profile_id"] == "project-test"
        assert paused["profile"]["title"] == "Registered local project"
        assert paused["profile"]["model_id"] == "registered-offline"
        assert owner.closed == owner.opened
        assert len(owner.opened) == 1
        assert owner.opened[0].data.closed is True

        second_id = await submit_profile_run(client)
        second = await wait_stopped(client, second_id)
        assert second["status"] == "paused", second
        assert len(owner.opened) == 2
        assert owner.opened[0].data is not owner.opened[1].data
        assert owner.closed == owner.opened

        resumed = await client.post(f"/api/runs/{first_id}/resume")
        assert resumed.status_code == 202, resumed.text
        completed = await wait_stopped(client, first_id)
        assert completed["status"] == "completed", completed
        assert completed["result"]["status"] == "completed"
        assert "120 million HKD" in completed["result"]["answer"]["text"]
        assert completed["evidence"] == paused["evidence"]
        assert completed["metrics"]["attempts"] == 2
        assert completed["metrics"]["model_calls"] == 0
        assert len(owner.opened) == 3
        assert owner.opened[2].data is not owner.opened[0].data
        assert owner.closed == owner.opened
        assert [ports.data.invocations for ports in owner.opened] == [1, 1, 0]
        assert all(ports.data.closed for ports in owner.opened)
        history = (await client.get("/api/runs")).json()
        assert all(item["profile_id"] == "project-test" for item in history["runs"])
        assert_public_payload({"paused": paused, "completed": completed, "history": history})


@pytest.mark.parametrize("change", ["missing", "revision", "sources", "run_config"])
async def test_recovery_refuses_missing_or_changed_registration_before_factory(tmp_path, change):
    """Changing a saved execution contract cannot create clients or mutate history."""
    first_owner = PortOwner()
    original = registered_profile(first_owner, pause=True)
    async with workspace_client(tmp_path, profiles=(original,)) as client:
        run_id = await submit_profile_run(client)
        paused = await wait_stopped(client, run_id)
        assert paused["status"] == "paused"
        original_evidence = paused["evidence"]

    replacement_owner = PortOwner()
    kwargs = {"pause": True}
    if change == "revision":
        kwargs["revision"] = "test-v2"
    elif change == "sources":
        kwargs["sources"] = DemoData().sources[:1]
    elif change == "run_config":
        kwargs["config"] = m.RunConfig(max_wall_time_ms=500_000, model_id="registered-offline")
    replacement = registered_profile(replacement_owner, **kwargs)
    profiles = () if change == "missing" else (replacement,)
    async with workspace_client(tmp_path, profiles=profiles) as client:
        detail = await client.get(f"/api/runs/{run_id}")
        assert detail.status_code == 200
        before = detail.json()
        assert before["can_resume"] is False
        assert before["resume_blocked_reason"]
        assert before["profile"]["revision"] == "test-v1"
        refused = await client.post(f"/api/runs/{run_id}/resume")
        assert refused.status_code == 409, refused.text
        assert refused.json()["detail"]["message"]
        after = (await client.get(f"/api/runs/{run_id}")).json()
        assert after["status"] == "paused"
        assert after["evidence"] == original_evidence
        assert len(after["attempts"]) == 1
        assert replacement_owner.opened == replacement_owner.closed == []
        assert_public_payload(after)


async def test_legacy_default_and_scenario_selectors_resolve_to_demo_profiles(tmp_path):
    """Existing clients keep their offline default and explicit recovery mapping."""
    async with workspace_client(tmp_path) as client:
        for payload, expected in (
            ({}, "demo-standard"),
            ({"scenario": "standard"}, "demo-standard"),
            ({"scenario": "parallel"}, "demo-parallel"),
            ({"scenario": "recovery"}, "demo-recovery"),
        ):
            response = await client.post("/api/runs", json={"question": "Revenue?", **payload})
            assert response.status_code == 202, response.text
            accepted = response.json()
            assert accepted["profile_id"] == expected
            view = await wait_stopped(client, accepted["run_id"])
            assert view["scenario"] == expected.removeprefix("demo-")
            assert view["profile"]["profile_id"] == expected
            assert view["status"] == ("paused" if expected == "demo-recovery" else "completed")


def test_duplicate_registration_ids_are_rejected_before_startup(tmp_path):
    """A duplicated public selector must never choose a factory by ordering."""
    profile = registered_profile(PortOwner())
    module = importlib.import_module("lake_agent.web.app")
    with pytest.raises(ValueError):
        module.create_app(data_dir=tmp_path, profiles=(profile, profile))


async def test_profile_run_config_budget_is_applied_and_preserves_terminal_status(tmp_path):
    """A registered data-call budget must reach the core and stay visible to users."""
    owner = PortOwner()
    profile = registered_profile(
        owner,
        config=m.RunConfig(
            max_wall_time_ms=600_000, max_data_calls=1, model_id="registered-offline"
        ),
    )
    async with workspace_client(tmp_path, profiles=(profile,)) as client:
        run_id = await submit_profile_run(client)
        stopped = await wait_stopped(client, run_id)
        assert stopped["status"] == stopped["result"]["status"] == "budget_exceeded"
        assert stopped["metrics"]["tools"] == 0
        assert stopped["can_resume"] is False
        assert len(owner.opened) == 1
        assert owner.closed == owner.opened


async def test_registered_project_report_uses_saved_profile_identity(tmp_path):
    """A project export must identify its registered pool rather than label all data demo."""
    owner = PortOwner()
    profile = registered_profile(owner)
    async with workspace_client(tmp_path, profiles=(profile,)) as client:
        run_id = await submit_profile_run(client)
        completed = await wait_stopped(client, run_id)
        assert completed["status"] == "completed"
        response = await client.get(f"/api/runs/{run_id}/report")
        assert response.status_code == 200, response.text
        assert "Registered local project" in response.text
        assert "Registered test data" in response.text
        assert "本地合成fixture" not in response.text
        assert "演示答案" not in response.text
        assert "合成数据 · 离线规则演示" not in response.text


def evaluation_payload():
    """Imported unknowns stay null; this fixture makes no quality-score claim."""
    return {
        "title": "User supplied offline report",
        "dataset_id": "registered-test-data",
        "task_set_id": "workspace-acceptance-v1",
        "model_id": "registered-offline",
        "evaluator": "Test report author",
        "evaluated_at": "2026-11-02T12:00:00+08:00",
        "notes": "All quality metrics are unknown.",
        "results": [{"mode": "full_agent", "task_count": 2, "metrics": {"accuracy": None}}],
    }


async def test_evaluation_template_and_import_preserve_unknowns_after_restart(tmp_path):
    """A downloaded template is separate from persisted, explicitly imported reports."""
    async with workspace_client(tmp_path) as client:
        assert (await client.get("/api/evaluations")).json() == {"evaluations": []}
        template = await client.get("/api/evaluations/template")
        assert template.status_code == 200, template.text
        assert "attachment" in template.headers["content-disposition"]
        for row in template.json()["results"]:
            assert all(value is None for value in row["metrics"].values())
        assert (await client.get("/api/evaluations")).json() == {"evaluations": []}
        response = await client.post("/api/evaluations", json=evaluation_payload())
        assert response.status_code == 201, response.text
        imported = response.json()
        identity = imported["evaluation_id"]
        assert identity.startswith("evaluation-")
        assert imported["origin"] == "imported"
        assert datetime.fromisoformat(imported["imported_at"]).tzinfo is not None
        assert imported["dataset_id"] == "registered-test-data"
        assert imported["results"][0]["task_count"] == 2
        assert all(value is None for value in imported["results"][0]["metrics"].values())
        downloaded = await client.get(f"/api/evaluations/{identity}")
        assert downloaded.json() == imported
        # Embedded browsers may ignore an anchor's download attribute. The
        # server must also identify this JSON response as a named attachment.
        assert downloaded.headers.get("content-disposition") == (
            f'attachment; filename="{identity}.json"'
        )
        assert (await client.get("/api/evaluations")).json() == {"evaluations": [imported]}

    async with workspace_client(tmp_path) as client:
        assert (await client.get(f"/api/evaluations/{identity}")).json() == imported
        assert (await client.get("/api/evaluations")).json() == {"evaluations": [imported]}
        assert (await client.get("/api/evaluations/no-such-report")).status_code == 404


async def test_evaluation_rejects_duplicate_modes_oversize_and_foreign_origin(tmp_path):
    """Invalid imports cannot create a report, including a streamed oversized body."""
    payload = evaluation_payload()
    payload["results"] *= 2

    async def oversized_chunks():
        # No Content-Length is sent: the HTTP boundary must count received bytes.
        yield b" " * 65_536
        yield b" " * 65_536
        yield b"x"

    async with workspace_client(tmp_path) as client:
        duplicate = await client.post("/api/evaluations", json=payload)
        assert duplicate.status_code == 422, duplicate.text
        assert duplicate.json()["detail"]["message"]
        oversized = await client.post(
            "/api/evaluations",
            content=oversized_chunks(),
            headers={"Content-Type": "application/json"},
        )
        assert oversized.status_code == 413, oversized.text
        forbidden = await client.post(
            "/api/evaluations",
            json=evaluation_payload(),
            headers={"Origin": "https://foreign.example"},
        )
        assert forbidden.status_code == 403, forbidden.text
        assert (await client.get("/api/evaluations")).json() == {"evaluations": []}
