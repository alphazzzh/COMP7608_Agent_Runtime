"""Contract tests for bounded planner context and observation paging."""

from __future__ import annotations

import copy

import pytest

from lake_agent.context import build_planner_context, read_json_page
from lake_agent.models import EvidenceRecord
from lake_agent.validation import canonical_json


def observation(observation_id: str, subtask_id: str, **updates):
    """Build a complete planner observation with optional payload overrides."""
    return {
        "observation_id": observation_id,
        "call_id": f"call-{observation_id}",
        "step_id": f"step-{observation_id}",
        "subtask_id": subtask_id,
        "tool_name": "inspect",
        "source_ids": ["source-1"],
        "args": {"source_id": "source-1"},
        "artifact_ref": None,
        "result": {
            "status": "ok",
            "data": {"schema": ["name", "value"]},
            "error": None,
            "warnings": [],
        },
    } | updates


def test_build_planner_context_prioritizes_local_recent_observations_and_projects_evidence():
    observations = [
        observation("foreign-old", "other"),
        observation("local-old", "target"),
        observation("foreign-new", "other"),
        observation("local-new", "target"),
    ]
    evidence = EvidenceRecord(
        evidence_id="evidence-1",
        call_id="private-call",
        source_id="source-1",
        content_type="text",
        payload="Revenue was 120 million HKD.",
        locator={"row": 3},
        warnings=["rounded"],
    )

    context = build_planner_context(
        observations, [evidence], subtask_id="target", max_bytes=4096, preview_bytes=512
    )

    assert set(context) == {
        "evidence_summary",
        "tool_observations",
        "context_status",
        "observation_read",
    }
    assert [item["observation_id"] for item in context["tool_observations"]] == [
        "local-new",
        "local-old",
        "foreign-new",
        "foreign-old",
    ]
    assert context["evidence_summary"] == [
        {
            "evidence_id": "evidence-1",
            "source_id": "source-1",
            "content_type": "text",
            "payload": "Revenue was 120 million HKD.",
            "locator": {"row": 3},
            "warnings": ["rounded"],
        }
    ]
    assert context["observation_read"] is None
    assert context["context_status"]["observations"] == {
        "total": 4,
        "included": 4,
        "omitted": 0,
    }
    assert len(canonical_json(context)) <= 4096


def test_build_planner_context_bounds_payloads_preserves_latest_and_does_not_mutate_inputs():
    huge = "香港" * 3000
    observations = [
        observation("older", "target"),
        observation(
            "latest",
            "target",
            args={"query": huge},
            result={"status": "ok", "data": {"rows": huge}, "error": None, "warnings": [huge]},
            unrecognized_payload={"private": huge},
        ),
    ]
    original = copy.deepcopy(observations)

    context = build_planner_context(
        observations, [], subtask_id="target", max_bytes=1024, preview_bytes=256
    )

    assert len(canonical_json(context)) <= 1024
    assert context["tool_observations"][0]["observation_id"] == "latest"
    latest = context["tool_observations"][0]
    assert latest["args"]["truncated"] is True
    assert latest["args"]["total_bytes"] == len(canonical_json({"query": huge}))
    assert len(latest["args"]["text"].encode("utf-8")) <= 256
    assert latest["result"]["data"]["truncated"] is True
    assert len(latest["result"]["warnings"]["text"].encode("utf-8")) <= 256
    assert context["context_status"]["unknown_fields_omitted"] == 1
    counts = context["context_status"]["observations"]
    assert counts["included"] + counts["omitted"] == 2
    assert observations == original


def test_build_planner_context_keeps_read_result_whole_and_rejects_invalid_limits():
    read_result = {"observation_id": "obs-1", "pointer": "/data", "text": "{"}
    context = build_planner_context(
        [],
        [],
        subtask_id="target",
        max_bytes=1024,
        preview_bytes=256,
        read_result=read_result,
    )
    assert context["observation_read"] == read_result

    for kwargs in (
        {"max_bytes": 1023, "preview_bytes": 256},
        {"max_bytes": 1024, "preview_bytes": 255},
        {"max_bytes": 1024, "preview_bytes": 513},
    ):
        with pytest.raises(ValueError):
            build_planner_context([], [], subtask_id="target", **kwargs)


def test_build_planner_context_rejects_read_result_that_does_not_fit():
    with pytest.raises(ValueError, match="read_result"):
        build_planner_context(
            [],
            [],
            subtask_id="target",
            max_bytes=1024,
            preview_bytes=256,
            read_result={"large": "x" * 5000},
        )


def test_read_json_page_decodes_rfc6901_and_slices_unicode_canonical_json():
    value = {"a/b": {"~key": ["香港", {"done": True}]}}
    encoded = canonical_json("香港").decode("utf-8")

    page = read_json_page(value, pointer="/a~1b/~0key/0", offset=1, max_chars=2, max_bytes=256)

    assert page == {
        "pointer": "/a~1b/~0key/0",
        "offset": 1,
        "text": encoded[1:3],
        "next_offset": 3,
        "total_chars": len(encoded),
        "truncated": True,
    }
    assert len(canonical_json(page)) <= 256


@pytest.mark.parametrize(
    ("value", "pointer"),
    [
        ({"x": 1}, "/missing"),
        ({"x": 1}, "/~2bad"),
        (["zero", "one"], "/01"),
        (["zero", "one"], "/-"),
        (["zero", "one"], "/2"),
        ("root", "/child"),
    ],
)
def test_read_json_page_rejects_invalid_or_unresolvable_pointers(value, pointer):
    with pytest.raises(ValueError):
        read_json_page(value, pointer=pointer, offset=0, max_chars=10, max_bytes=256)


@pytest.mark.parametrize(
    ("offset", "max_chars", "max_bytes"),
    [(-1, 1, 256), (5, 1, 256), (0, 0, 256), (0, 4097, 256), (0, 1, 1)],
)
def test_read_json_page_rejects_invalid_offsets_limits_or_fixed_metadata_overflow(
    offset, max_chars, max_bytes
):
    with pytest.raises(ValueError):
        read_json_page("ab", pointer="", offset=offset, max_chars=max_chars, max_bytes=max_bytes)


def test_read_json_page_shrinks_character_slice_to_fit_encoded_byte_limit():
    page = read_json_page(
        "香港" * 100,
        pointer="",
        offset=0,
        max_chars=100,
        max_bytes=100,
    )

    assert page["truncated"] is True
    assert page["next_offset"] == len(page["text"])
    assert len(page["text"]) < 100
    assert len(canonical_json(page)) <= 100
