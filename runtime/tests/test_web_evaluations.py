"""Validate imported evaluation summaries and their private local persistence.

These tests exercise the public repository/model boundary without web routes or
model calls. Real files verify atomic visibility, permissions, safe identifiers,
and cancellation drainage; malformed reports never become benchmark results.
"""

import asyncio
import importlib
import json
import math
import os
import re
import stat
import threading
from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lake_agent.web.storage import WebError


def evaluations():
    """Import inside tests so the initial missing feature is a test failure."""
    return importlib.import_module("lake_agent.web.evaluations")


def payload():
    """A caller-authored report deliberately leaves all quality metrics unknown."""
    return {
        "title": "Verified task set",
        "dataset_id": "course-pool-v1",
        "task_set_id": "tasks-2026-11",
        "model_id": "registered-model",
        "evaluator": "Human evaluator",
        "evaluated_at": "2026-11-02T08:30:00+08:00",
        "results": [{"mode": "full_agent", "task_count": 12}],
    }


def test_report_preserves_unknown_metrics_and_allows_different_task_counts():
    module = evaluations()
    value = payload()
    value["results"].append(
        {"mode": "fixed_retrieval", "task_count": 4, "metrics": {"accuracy": 0.5}}
    )
    report = module.EvaluationImport.model_validate(value).model_dump(mode="json")
    assert report["notes"] == ""
    assert report["results"][0]["metrics"] == {
        "accuracy": None,
        "retrieval_precision": None,
        "retrieval_recall": None,
        "retrieval_f1": None,
        "grounding": None,
        "completion_rate": None,
        "tool_calls_mean": None,
        "latency_ms_mean": None,
        "token_total": None,
        "cost_usd": None,
    }
    assert report["results"][1]["metrics"]["accuracy"] == 0.5
    assert report["evaluated_at"] == value["evaluated_at"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("title", ""),
        ("title", "a" * 161),
        ("title", "\ud800"),
        ("dataset_id", ""),
        ("dataset_id", "a" * 201),
        ("task_set_id", ""),
        ("model_id", "a" * 201),
        ("evaluator", ""),
        ("notes", "a" * 2001),
        ("evaluated_at", "2026-11-02"),
        ("evaluated_at", "2026-11-02T08:30:00"),
        ("evaluated_at", "yesterday"),
        ("evaluated_at", 1793589000),
        ("results", []),
        ("results", [{"mode": "invalid", "task_count": 12}]),
        ("results", [{"mode": "full_agent", "task_count": "12"}]),
        ("results", [{"mode": "full_agent", "task_count": True}]),
        ("results", [{"mode": "full_agent", "task_count": 0}]),
        ("results", [{"mode": "full_agent", "task_count": 1_000_001}]),
        ("origin", "claimed-benchmark"),
    ],
)
def test_report_rejects_invalid_or_untrusted_fields(field, value):
    report = payload()
    report[field] = value
    with pytest.raises(ValidationError):
        evaluations().EvaluationImport.model_validate(report)


@pytest.mark.parametrize("level", ["report", "row", "metrics"])
def test_extra_fields_are_rejected_at_every_level(level):
    report = payload()
    target = report
    if level == "row":
        target = report["results"][0]
    elif level == "metrics":
        target = report["results"][0].setdefault("metrics", {})
    target["unknown"] = "must not persist"
    with pytest.raises(ValidationError):
        evaluations().EvaluationImport.model_validate(report)


def test_duplicate_modes_are_rejected():
    report = payload()
    report["results"] *= 2
    with pytest.raises(ValidationError):
        evaluations().EvaluationImport.model_validate(report)


async def test_copied_model_cannot_admit_an_overlong_timestamp(tmp_path):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()
    report = module.EvaluationImport.model_validate(payload()).model_copy(
        update={"evaluated_at": "2026-11-02T08:30:00." + "1" * 39 + "+08:00"}
    )
    # model_copy intentionally skips validation; create must still reject it
    # before publishing anything, including an atomically complete bad record.
    with pytest.raises(ValidationError):
        await repository.create(report)
    assert await repository.listing() == []
    assert list((tmp_path / "evaluations").iterdir()) == []


async def test_create_cannot_exceed_the_repository_read_limit(tmp_path, monkeypatch):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()
    # A smaller storage cap makes the write/read invariant observable using a
    # valid public report, independently of future changes to model field bounds.
    monkeypatch.setattr(module, "_MAX_RECORD_BYTES", 64)
    with pytest.raises(WebError) as caught:
        await repository.create(module.EvaluationImport.model_validate(payload()))
    assert caught.value.status == 413
    assert await repository.listing() == []
    assert list((tmp_path / "evaluations").iterdir()) == []


@pytest.mark.parametrize(
    "metric",
    [
        "accuracy",
        "retrieval_precision",
        "retrieval_recall",
        "retrieval_f1",
        "grounding",
        "completion_rate",
    ],
)
@pytest.mark.parametrize("value", [-0.01, 1.01, math.nan, math.inf, -math.inf, True, "0.5"])
def test_quality_metrics_require_finite_unit_interval_numbers(metric, value):
    with pytest.raises(ValidationError):
        evaluations().EvaluationMetrics.model_validate({metric: value})


@pytest.mark.parametrize(
    "metric", ["tool_calls_mean", "latency_ms_mean", "token_total", "cost_usd"]
)
@pytest.mark.parametrize("value", [-1, math.nan, math.inf, True, "1"])
def test_usage_metrics_require_finite_nonnegative_numbers(metric, value):
    with pytest.raises(ValidationError):
        evaluations().EvaluationMetrics.model_validate({metric: value})


async def test_create_round_trips_private_report_across_restarts(tmp_path):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()
    report = module.EvaluationImport.model_validate(payload())
    created = await repository.create(report)
    assert re.fullmatch(r"evaluation-[0-9a-f]{32}", created["evaluation_id"])
    assert created["origin"] == "imported"
    assert datetime.fromisoformat(created["imported_at"]).utcoffset() is not None
    assert created["title"] == report.title
    assert "evaluation_id" not in report.model_dump()
    path = tmp_path / "evaluations" / f"{created['evaluation_id']}.json"
    assert json.loads(path.read_text()) == created
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700
    restarted = module.EvaluationRepository(tmp_path)
    await restarted.start()
    assert await restarted.get(created["evaluation_id"]) == created
    assert await restarted.listing() == [created]


@pytest.mark.parametrize("identity", ["../outside", "evaluation-../outside", "a", "", None])
async def test_get_rejects_unsafe_or_missing_identifiers(tmp_path, identity):
    repository = evaluations().EvaluationRepository(tmp_path)
    await repository.start()
    with pytest.raises(WebError) as caught:
        await repository.get(identity)
    assert caught.value.status == 404
    assert caught.value.code == "not_found"
    with pytest.raises(WebError) as caught:
        await repository.get(f"evaluation-{uuid4().hex}")
    assert caught.value.status == 404


@pytest.mark.parametrize(
    "damage",
    [
        "invalid-json",
        "invalid-utf8",
        "nested-json",
        "extra",
        "identity",
        "origin",
        "date",
        "long-import-date",
        "oversize",
    ],
)
async def test_stored_reports_are_fully_revalidated(tmp_path, damage):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()
    created = await repository.create(module.EvaluationImport.model_validate(payload()))
    path = tmp_path / "evaluations" / f"{created['evaluation_id']}.json"
    if damage == "invalid-json":
        path.write_text("{")
    elif damage == "invalid-utf8":
        path.write_bytes(b"\xff")
    elif damage == "oversize":
        path.write_bytes(b"x" * 262_145)
    elif damage == "nested-json":
        path.write_text("[" * 20_000 + "null" + "]" * 20_000)
    elif damage == "long-import-date":
        created["imported_at"] = "2026-11-02T08:30:00." + "1" * 39 + "+08:00"
        path.write_text(json.dumps(created))
    else:
        key = {
            "extra": "invented",
            "identity": "evaluation_id",
            "origin": "origin",
            "date": "imported_at",
        }[damage]
        created[key] = "untrusted"
        path.write_text(json.dumps(created))
    for operation in (
        lambda: repository.get(created["evaluation_id"] if damage != "identity" else path.stem),
        repository.listing,
    ):
        with pytest.raises(WebError) as caught:
            await operation()
        assert caught.value.status == 503
        assert str(tmp_path) not in caught.value.message


async def test_listing_returns_only_the_latest_hundred_reports(tmp_path):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()
    report = module.EvaluationImport.model_validate(payload())
    seed = await repository.create(report)
    seed["imported_at"] = "2030-01-01T00:00:00+00:00"
    (tmp_path / "evaluations" / f"{seed['evaluation_id']}.json").write_text(json.dumps(seed))
    identities = []
    for index in range(105):
        identity = f"evaluation-{uuid4().hex}"
        identities.append(identity)
        value = {
            **seed,
            "evaluation_id": identity,
            "imported_at": f"2026-11-02T00:00:{index // 2:02d}.{index:06d}+00:00",
        }
        (tmp_path / "evaluations" / f"{identity}.json").write_text(json.dumps(value))
    # A symlink or unrelated filename must not cause a scan outside the folder.
    (tmp_path / "evaluations" / "notes.json").write_text("not a report")
    rows = await repository.listing()
    assert len(rows) == 100
    assert [value["evaluation_id"] for value in rows] == [
        seed["evaluation_id"],
        *reversed(identities[-99:]),
    ]


async def test_repository_rejects_symlinked_report_and_directory(tmp_path):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path / "private")
    await repository.start()
    outside = tmp_path / "outside.json"
    outside.write_text("private outside contents")
    identity = f"evaluation-{uuid4().hex}"
    (tmp_path / "private" / "evaluations" / f"{identity}.json").symlink_to(outside)
    with pytest.raises(WebError):
        await repository.get(identity)
    assert outside.read_text() == "private outside contents"
    other = tmp_path / "other"
    other.mkdir()
    linked_root = tmp_path / "linked"
    linked_root.mkdir()
    (linked_root / "evaluations").symlink_to(other, target_is_directory=True)
    with pytest.raises(WebError):
        await module.EvaluationRepository(linked_root).start()
    assert list(other.iterdir()) == []


async def test_concurrent_creates_have_distinct_complete_private_files(tmp_path):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()
    report = module.EvaluationImport.model_validate(payload())
    created = await asyncio.gather(*(repository.create(report) for _ in range(20)))
    assert len({value["evaluation_id"] for value in created}) == 20
    assert len(await repository.listing()) == 20
    for value in created:
        assert await repository.get(value["evaluation_id"]) == value
    assert len(list((tmp_path / "evaluations").iterdir())) == 20


async def test_failed_atomic_save_leaves_no_partial_report(tmp_path, monkeypatch):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()

    def fail_replace(*args, **kwargs):
        raise OSError("private host detail")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(WebError) as caught:
        await repository.create(module.EvaluationImport.model_validate(payload()))
    assert caught.value.status == 503
    assert "private host detail" not in caught.value.message
    assert list((tmp_path / "evaluations").iterdir()) == []


async def test_cancelled_create_drains_the_atomic_worker(tmp_path, monkeypatch):
    module = evaluations()
    repository = module.EvaluationRepository(tmp_path)
    await repository.start()
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    original = os.replace
    worker = None

    def held_replace(*args, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(os, "replace", held_replace)
    try:
        worker = asyncio.create_task(
            repository.create(module.EvaluationImport.model_validate(payload()))
        )
        async with asyncio.timeout(3):
            await entered.wait()
        worker.cancel()
        # A cancelled owner remains alive while its filesystem worker is held.
        await asyncio.sleep(0)
        assert not worker.done()
        assert await repository.listing() == []
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        rows = await repository.listing()
        assert len(rows) == 1
        assert await repository.get(rows[0]["evaluation_id"]) == rows[0]
        assert len(list((tmp_path / "evaluations").iterdir())) == 1
    finally:
        release.set()
        if worker is not None:
            await asyncio.gather(worker, return_exceptions=True)
