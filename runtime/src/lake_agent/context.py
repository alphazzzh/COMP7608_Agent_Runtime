"""Bounded model working context, independent of the complete audit record.

Inspired by result persistence and pre-request budgeting in the supplied Claude
Code reference. Full results remain owned by the controller; these pure
functions return detached JSON previews and local JSON Pointer pages. Limits
measure serialized UTF-8 bytes, never tokenizer or provider usage estimates.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence

from .models import EvidenceRecord, JsonValue
from .validation import canonical_json

_OBSERVATION_KEYS = frozenset(
    {
        "observation_id",
        "call_id",
        "operation_id",
        "attempt",
        "step_id",
        "subtask_id",
        "tool_name",
        "source_ids",
        "args",
        "artifact_ref",
        "result",
        "status",
    }
)


def _preview(value: JsonValue, limit: int) -> JsonValue:
    """Preserve small structured values; mark partial JSON text explicitly."""
    encoded = canonical_json(value)
    if len(encoded) <= limit:
        return json.loads(encoded)
    text = encoded.decode("utf-8")
    result = {"truncated": True, "text": "", "total_bytes": len(encoded)}
    lo, hi = 0, min(len(text), limit)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        result["text"] = text[:mid]
        if len(canonical_json(result)) <= limit:
            lo = mid
        else:
            hi = mid - 1
    result["text"] = text[:lo]
    return result


def _observation(item: dict, limit: int) -> dict:
    """Retain stable provenance without letting bulk data crowd out feedback."""
    result = item["result"]
    projected = {
        key: value
        for key, value in item.items()
        if key in _OBSERVATION_KEYS and key not in {"args", "result"}
    }
    projected["status"] = result.get("status", item.get("status"))
    projected["args"] = _preview(item.get("args", {}), limit)
    projected["result"] = {
        "status": result.get("status"),
        "data": _preview(result.get("data"), limit),
        "error": _preview(result.get("error"), limit),
        "warnings": _preview(result.get("warnings", []), limit),
        "truncated": result.get("truncated", False),
    }
    return json.loads(canonical_json(projected))


def _evidence(item: EvidenceRecord, limit: int) -> dict:
    """A preview has an evidence ID but never replaces the original evidence."""
    return {
        "evidence_id": item.evidence_id,
        "source_id": item.source_id,
        "content_type": item.content_type,
        "payload": _preview(item.payload, limit),
        "locator": _preview(item.locator, limit),
        "warnings": _preview(item.warnings, limit),
    }


def build_planner_context(
    observations: Sequence[dict],
    evidence: Sequence[EvidenceRecord],
    *,
    subtask_id: str,
    max_bytes: int,
    preview_bytes: int,
    read_result: dict | None = None,
) -> dict:
    """Pack observations and evidence into an exact, omission-aware JSON budget.

    The latest observation for this subtask and latest evidence get first
    opportunity to fit, then earlier observations and evidence follow. Within
    each group the order is deterministic. Oversized provenance is omitted as a
    whole rather than fabricating shortened identifiers. Requested read pages
    are kept whole so their offsets remain meaningful.
    """
    if (
        type(max_bytes) is not int
        or max_bytes < 1024
        or type(preview_bytes) is not int
        or preview_bytes < 256
        or preview_bytes > max_bytes // 2
    ):
        raise ValueError("invalid working-context byte limits")
    if read_result is not None and len(canonical_json(read_result)) > max_bytes // 2:
        raise ValueError("read_result exceeds its reserved byte budget")
    context = {
        "evidence_summary": [],
        "tool_observations": [],
        "context_status": {
            "observations": {
                "total": len(observations),
                "included": 0,
                "omitted": len(observations),
            },
            "evidence": {"total": len(evidence), "included": 0, "omitted": len(evidence)},
            "unknown_fields_omitted": sum(
                len(set(item) - _OBSERVATION_KEYS) for item in observations
            ),
        },
        "observation_read": json.loads(canonical_json(read_result)),
    }
    if len(canonical_json(context)) > max_bytes:
        raise ValueError("read_result and context metadata exceed budget")
    ordered = [item for item in reversed(observations) if item["subtask_id"] == subtask_id]
    ordered += [item for item in reversed(observations) if item["subtask_id"] != subtask_id]
    recent_evidence = list(reversed(evidence))
    entries = []
    if ordered:
        entries.append(("tool_observations", "observations", ordered[0], _observation))
    if recent_evidence:
        entries.append(("evidence_summary", "evidence", recent_evidence[0], _evidence))
    entries.extend(
        ("tool_observations", "observations", item, _observation) for item in ordered[1:]
    )
    entries.extend(
        ("evidence_summary", "evidence", item, _evidence) for item in recent_evidence[1:]
    )
    for key, group, item, projector in entries:
        limit = preview_bytes
        while True:
            candidate = projector(item, limit)
            context[key].append(candidate)
            counts = context["context_status"][group]
            counts["included"] += 1
            counts["omitted"] -= 1
            if len(canonical_json(context)) <= max_bytes:
                break
            context[key].pop()
            counts["included"] -= 1
            counts["omitted"] += 1
            if limit == 64:
                break
            limit = max(64, limit // 2)
    return context


def read_json_page(
    value: JsonValue,
    *,
    pointer: str,
    offset: int,
    max_chars: int,
    max_bytes: int,
) -> dict:
    """Page canonical JSON at an RFC 6901 location, bounded after JSON escaping.

    No paths, URLs, files, or object attributes are accessed. The empty pointer
    selects the whole result. Offsets refer to Unicode characters, not bytes;
    clients can concatenate page text and parse JSON once the last page arrives.
    """
    if (
        not isinstance(pointer, str)
        or (pointer and not pointer.startswith("/"))
        or re.search(r"~(?![01])", pointer)
        or type(offset) is not int
        or offset < 0
        or type(max_chars) is not int
        or not 1 <= max_chars <= 4096
        or type(max_bytes) is not int
        or max_bytes <= 0
    ):
        raise ValueError("invalid JSON page parameters")
    selected = value
    for token in pointer.split("/")[1:] if pointer else ():
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(selected, dict) and token in selected:
            selected = selected[token]
        elif isinstance(selected, list) and re.fullmatch(r"0|[1-9][0-9]*", token):
            # Bound the decimal representation before converting untrusted input.
            if len(token) > len(str(len(selected))) or int(token) >= len(selected):
                raise ValueError("JSON pointer array index is out of range")
            selected = selected[int(token)]
        else:
            raise ValueError("JSON pointer does not resolve")
    text = canonical_json(selected).decode("utf-8")
    if offset > len(text):
        raise ValueError("JSON page offset exceeds selected value")

    def page(length: int) -> dict:
        end = offset + length
        return {
            "pointer": pointer,
            "offset": offset,
            "text": text[offset:end],
            "next_offset": end if end < len(text) else None,
            "total_chars": len(text),
            "truncated": end < len(text),
        }

    lo, hi = 0, min(max_chars, len(text) - offset)
    if len(canonical_json(page(hi))) <= max_bytes:
        return page(hi)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if len(canonical_json(page(mid))) <= max_bytes:
            lo = mid
        else:
            hi = mid - 1
    result = page(lo)
    if len(canonical_json(result)) > max_bytes or (lo == 0 and offset < len(text)):
        raise ValueError("JSON page metadata or one character exceeds byte budget")
    return result
