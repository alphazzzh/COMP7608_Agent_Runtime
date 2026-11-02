"""One opt-in live acceptance of the tool-observation harness.

The PlannerPort is the real JsonPlanner and uses the configured remote model.
DataPort and ContentPort are explicitly SYNTHETIC local fixtures: this checks
inspect -> local read_observation -> query -> finish, never retrieval or answer
quality on a real data lake. A fresh unpredictable table name occurs only in
the full inspect result until the controller returns its requested JSON page.

Run from project/runtime after installing the runtime dependencies::

    .venv/bin/python scripts/live_harness_acceptance.py --help
    .venv/bin/python scripts/live_harness_acceptance.py --live

Credentials use live_controller_acceptance.read_credential (MODEL_API_KEY or
the existing authorized macOS Keychain item). Secrets are never printed, put
in model input, or deliberately persisted. There is exactly one run, no outer
retry, no HTTP retry/redirect, and no automatic data-operation retry. A failure
report is still written for ordinary exceptions and user interruption; an OS
kill or unwritable output directory cannot guarantee a report.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
from live_controller_acceptance import read_credential

from lake_agent import models as m
from lake_agent.demo import DemoContent
from lake_agent.planner import JsonPlanner
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink
from lake_agent.validation import canonical_json

RUNTIME_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "https://codex.miaomiaocode.com/v1"
MODEL_ID = "gpt-6.1-sol"
SOURCE_ID = "synthetic-harness-source"
READ_POINTER = "/data/schema/query_table"
SAFE_REF = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}\Z")


class SyntheticData:
    """Expose one read-only source; only inspect reveals its random table name."""

    def __init__(self) -> None:
        self.query_table = "synthetic_annual_" + uuid4().hex
        self.invocations: list[m.ToolRequest] = []

    def tool_specs(self) -> list[m.ToolSpec]:
        """Advertise generic argument shapes without embedding the table value."""
        specs = []
        for name in ("inspect", "query"):
            properties = {"source_id": {"type": "string"}}
            if name == "query":
                properties["table"] = {"type": "string"}
            specs.append(
                m.ToolSpec(
                    name=name,
                    description=(
                        "SYNTHETIC local read-only fixture. Inspect the source schema."
                        if name == "inspect"
                        else "SYNTHETIC local read-only fixture. Query the table from inspect."
                    ),
                    input_schema={
                        "type": "object",
                        "properties": properties,
                        "required": list(properties),
                        "additionalProperties": False,
                    },
                    idempotent=True,
                    uses_sources=True,
                    source_id_paths=["/source_id"],
                )
            )
        return specs

    async def catalog(self, request: m.CatalogRequest) -> m.CatalogResult:
        """Provide no schema/table hints in the catalog or retrieval metadata."""
        return m.CatalogResult(
            call_id=request.call_id,
            catalog_snapshot_id="synthetic-harness-v1",
            sources=[
                m.SourceRecord(
                    source_id=SOURCE_ID,
                    name="SYNTHETIC local annual report",
                    format="synthetic_table",
                    summary="Synthetic annual revenue source for harness acceptance only.",
                )
            ],
        )

    async def invoke(self, request: m.ToolRequest) -> m.ToolResult:
        """Return bulky schema or evidence; a guessed table is safely correctable."""
        self.invocations.append(request)
        if request.tool_name == "inspect":
            return m.ToolResult(
                call_id=request.call_id,
                data={
                    "fixture": "SYNTHETIC; no real data-lake source",
                    "schema": {
                        # canonical_json sorts keys. Padding sorts before
                        # query_table, placing its random value past the 512
                        # byte preview even if serialization order changes.
                        "padding": "Synthetic schema documentation. " * 256,
                        "query_table": self.query_table,
                    },
                },
            )
        if request.tool_name != "query" or request.args.get("table") != self.query_table:
            return m.ToolResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(
                    code="synthetic_unknown_table",
                    message=(
                        "No matching table. This read-only fixture made no change. "
                        "Inspect and read /data/schema/query_table before correcting table."
                    ),
                    retryable=False,
                    recoverable=True,
                ),
            )
        return m.ToolResult(
            call_id=request.call_id,
            data={
                "evidence": [
                    {
                        "source_id": SOURCE_ID,
                        "content_type": "text",
                        "payload": "SYNTHETIC fixture: 2024 annual revenue was 120 million HKD.",
                        "locator": {"table": self.query_table, "row": 1},
                        "warnings": ["SYNTHETIC local fixture; not real retrieval evidence"],
                    }
                ]
            },
        )


class SyntheticContent(DemoContent):
    """Keep catalog/search/verification local and compose only fixture evidence."""

    async def compose(self, request: m.ComposeRequest) -> m.ModuleResult[m.AnswerRecord]:
        # This deliberately simple composer is not a model-quality test. It
        # references the actual evidence ID, preserving controller provenance.
        if not request.evidence:
            raise ValueError("Synthetic composer requires controller-owned evidence.")
        return m.ModuleResult(
            call_id=request.call_id,
            usage=m.Usage.local(),
            data=m.AnswerRecord(
                text=str(request.evidence[0].payload),
                citations=[request.evidence[0].evidence_id],
            ),
        )


class CountingTransport(httpx.AsyncBaseTransport):
    """Count dispatched HTTP attempts, including transport failures, without keys.

    Response hooks alone miss requests that fail before headers arrive. This
    wrapper records only lengths, booleans, phase and status; headers, raw model
    responses and request bodies never enter its report. The independent hard
    cap prevents a runtime regression from dispatching unbounded requests.
    """

    def __init__(self, table: str, max_requests: int) -> None:
        self.delegate = httpx.AsyncHTTPTransport(retries=0)
        self.table = table
        self.max_requests = max_requests
        self.attempts: list[dict] = []
        self.blocked_requests = 0
        self.read_seen = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if len(self.attempts) >= self.max_requests:
            self.blocked_requests += 1
            raise httpx.RequestError("Acceptance HTTP request budget exhausted.", request=request)
        body = json.loads(request.content)
        supplied = json.loads(body["messages"][-1]["content"])
        inputs = supplied["input"]
        page = inputs.get("observation_read") or {}
        current_read = page.get("pointer") == READ_POINTER and self.table in page.get("text", "")
        context = {
            key: inputs[key]
            for key in (
                "evidence_summary",
                "tool_observations",
                "context_status",
                "observation_read",
            )
            if key in inputs
        }
        inspect_previews = [
            item["result"]["data"]
            for item in inputs.get("tool_observations", [])
            if item.get("tool_name") == "inspect"
        ]
        visible = self.table.encode() in request.content
        record = {
            "index": len(self.attempts) + 1,
            "phase": supplied["phase"],
            "model_matches": body.get("model") == MODEL_ID,
            "endpoint_matches": str(request.url) == BASE_URL + "/chat/completions",
            "request_bytes": len(request.content),
            "working_context_bytes": len(canonical_json(context)),
            "table_visible": visible,
            "table_visible_before_read": visible and not (self.read_seen or current_read),
            "inspect_preview_contains_table": any(
                self.table.encode() in canonical_json(item) for item in inspect_previews
            ),
            "inspect_preview_truncated": any(
                isinstance(item, dict) and item.get("truncated") is True
                for item in inspect_previews
            ),
            "requested_table_page_visible": current_read,
            "http_status": None,
            "transport_failed": False,
        }
        self.read_seen = self.read_seen or current_read
        self.attempts.append(record)
        try:
            response = await self.delegate.handle_async_request(request)
        except BaseException:
            # A timeout/cancellation is still a physical HTTP attempt; keep its
            # count but never stringify an exception that may contain headers.
            record["transport_failed"] = True
            raise
        record["http_status"] = response.status_code
        return response

    async def aclose(self) -> None:
        await self.delegate.aclose()


def source_hashes() -> dict[str, str]:
    """Identify the tested code independently of cwd, without reading secrets."""
    paths = sorted((RUNTIME_ROOT / "src" / "lake_agent").glob("*.py"))
    paths.extend(
        RUNTIME_ROOT / relative
        for relative in (
            "scripts/live_harness_acceptance.py",
            "scripts/live_controller_acceptance.py",
            "tests/test_harness_observations.py",
            "pyproject.toml",
            "uv.lock",
        )
    )
    return {
        str(path.relative_to(RUNTIME_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def inspect_trace(path: Path, run_id: str) -> tuple[dict, dict, list[dict]]:
    """Audit complete or partial traces without dropping evidence on failure."""
    events: list[dict] = []
    artifacts: dict[str, dict] = {}
    problems = []
    if path.exists():
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            try:
                events.append(m.TraceEvent.model_validate_json(line).model_dump(mode="json"))
            except Exception:
                problems.append({"code": "invalid_trace_event", "line": number})
    else:
        problems.append({"code": "trace_missing"})
    references = {event["data_ref"] for event in events if event["data_ref"]}
    index = []
    for ref in sorted(references):
        if not SAFE_REF.fullmatch(ref):
            problems.append({"code": "invalid_artifact_ref"})
            continue
        artifact = path.parent / "artifacts" / (ref + ".json")
        try:
            raw = artifact.read_bytes()
            artifacts[ref] = json.loads(raw)
            index.append({"ref": ref, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
        except Exception:
            problems.append({"code": "missing_or_invalid_artifact", "ref": ref})
    starts = [event for event in events if event["event_type"].endswith(".call.started")]
    ends = [
        event
        for event in events
        if event["event_type"].endswith((".call.completed", ".call.failed"))
    ]
    start_counts = Counter(event["call_id"] for event in starts)
    end_counts = Counter(event["call_id"] for event in ends)
    checks = {
        "trace_present_and_valid": bool(events) and not problems,
        "trace_sequence_and_ownership": bool(events)
        and [event["seq"] for event in events] == list(range(1, len(events) + 1))
        and all(event["run_id"] == run_id for event in events),
        "every_call_has_one_start_and_terminal": bool(starts)
        and start_counts == end_counts
        and all(count == 1 for count in start_counts.values())
        and all(
            next(item["seq"] for item in starts if item["call_id"] == event["call_id"])
            < event["seq"]
            for event in ends
        ),
        "all_artifact_refs_resolve": bool(references) and len(artifacts) == len(references),
    }
    audit = {
        "trace_ref": str(path),
        "event_count": len(events),
        "call_starts": len(starts),
        "call_terminals": len(ends),
        "reported_provider_requests_from_events": sum(
            (event["usage"] or {}).get("provider_requests", 0) for event in ends
        ),
        "events": [
            {key: event[key] for key in ("seq", "event_type", "call_id", "attempt", "data_ref")}
            for event in events
        ],
        "artifacts": index,
        "problems": problems,
        "checks": checks,
    }
    return audit, artifacts, events


async def execute(args: argparse.Namespace, report: dict, output: Path) -> None:
    """Make a single budgeted run; the caller always writes the final report."""
    data = SyntheticData()
    transport = CountingTransport(data.query_table, args.max_requests)
    report["http_attempts"] = transport.attempts
    result = None
    report["fixture_table_sha256"] = hashlib.sha256(data.query_table.encode()).hexdigest()
    request = m.RunRequest(
        run_id=report["run_id"],
        task_id="synthetic-live-harness-acceptance",
        mode="full_agent",
        question=(
            "SYNTHETIC harness acceptance only. Determine 2024 annual revenue and currency "
            "using the single synthetic source. Create exactly one subtask. First inspect "
            "the source; its large result contains schema.query_table beyond the preview. "
            "Then use read_observation on that inspect observation, with pointer "
            "/data/schema/query_table, offset 0, max_chars 128. Parse the returned page text "
            "as a JSON string and query that exact table on the same source. Never guess "
            "a table or plan a query before reading the inspect result. Select each action "
            "in a separate turn, because its inputs depend on the preceding result. Once "
            "query supplies revenue evidence, return finish with reason evidence_complete. "
            "The local synthetic ContentPort will compose and verify the final answer."
        ),
        config=m.RunConfig(
            model_id=MODEL_ID,
            model_params={},
            max_wall_time_ms=args.wall_timeout_ms,
            per_call_timeout_ms=args.call_timeout_ms,
            max_model_calls=args.max_requests,
            max_subtasks=1,
            max_plan_steps=3,
            max_data_calls=4,
            max_retries_per_data_operation=0,
            max_feedback_rounds=0,
            max_observation_reads=2,
            max_identical_tool_calls=1,
            candidate_top_k=1,
            max_planner_context_bytes=4096,
            planner_preview_bytes=512,
            prompt_versions=m.PromptVersions(
                planner="planner-v0.2-observations", content="synthetic-harness-v1"
            ),
            config_version="manual-live-harness-v1",
        ),
    )
    report["run_config"] = request.config.model_dump(mode="json")
    try:
        # Credential lookup occurs only after --live and output preparation.
        # Keep the key in memory only; errors below retain only exception type.
        secret = read_credential(BASE_URL)
        async with httpx.AsyncClient(transport=transport, follow_redirects=False) as client:
            async with JsonPlanner(BASE_URL, secret, client=client) as planner:
                result = await run(
                    request,
                    data=data,
                    content=SyntheticContent(),
                    planner=planner,
                    trace=JsonlTraceSink(output / "runs"),
                )
    finally:
        # This also covers credential lookup failing before AsyncClient owns
        # the transport. HTTPX transport closure is safe to repeat.
        await transport.aclose()
        report["http_requests"] = len(transport.attempts)
        report["http_responses"] = sum(
            item["http_status"] is not None for item in transport.attempts
        )
        report["blocked_http_requests"] = transport.blocked_requests
        report["tool_calls"] = len(data.invocations)
        report["tool_names"] = [item.tool_name for item in data.invocations]
        report["pending_asyncio_tasks"] = sum(
            task is not asyncio.current_task() and not task.done() for task in asyncio.all_tasks()
        )
        if result is not None:
            report["result"] = {
                "status": result.status,
                "stop_reason": result.stop_reason,
                "duration_ms": result.duration_ms,
                "usage": result.usage.model_dump(mode="json"),
                "call_counts": result.call_counts.model_dump(mode="json"),
                "citations": result.answer.citations if result.answer else [],
            }


def finalize_checks(report: dict, output: Path) -> None:
    """Derive acceptance from persisted requests/pages/results, including failures."""
    path = output / "runs" / report["run_id"] / "events.jsonl"
    audit, artifacts, events = inspect_trace(path, report["run_id"])
    report["trace"] = audit
    checks = report["checks"]
    checks.update(audit["checks"])
    starts = [event for event in events if event["event_type"].endswith(".call.started")]
    reads = [event for event in events if event["event_type"] == "observation.read"]
    tool_starts = [event for event in events if event["event_type"] == "tool.call.started"]
    tool_requests = [artifacts.get(event["data_ref"], {}) for event in tool_starts]
    inspect_calls = [
        event
        for event, payload in zip(tool_starts, tool_requests, strict=True)
        if payload.get("tool_name") == "inspect"
    ]
    inspect_observations = [
        event
        for event in events
        if event["event_type"] == "tool.observed"
        and any(event["call_id"] == item["call_id"] for item in inspect_calls)
    ]
    inspected_ids = {event["detail"].get("observation_id") for event in inspect_observations}
    table = None
    for event in inspect_observations:
        table = (
            artifacts.get(event["data_ref"], {})
            .get("data", {})
            .get("schema", {})
            .get("query_table")
        )
    correct_reads = (
        [
            event
            for event in reads
            if artifacts.get(event["data_ref"], {}).get("pointer") == READ_POINTER
            and artifacts.get(event["data_ref"], {}).get("observation_id") in inspected_ids
            and artifacts.get(event["data_ref"], {}).get("text") == json.dumps(table)
            and artifacts.get(event["data_ref"], {}).get("offset") == 0
            and artifacts.get(event["data_ref"], {}).get("next_offset") is None
        ]
        if table
        else []
    )
    correct_queries = [
        event
        for event, payload in zip(tool_starts, tool_requests, strict=True)
        if table
        and payload.get("tool_name") == "query"
        and payload.get("args", {}).get("table") == table
    ]
    finishes = [
        event
        for event in events
        if event["event_type"] == "planner.select_tools.call.completed"
        and artifacts.get(event["data_ref"], {}).get("data", {}).get("kind") == "finish"
        and artifacts.get(event["data_ref"], {}).get("data", {}).get("reason")
        == "evidence_complete"
    ]
    evidence = {event["data_ref"] for event in events if event["event_type"] == "evidence.added"}
    result = report.get("result", {})
    counts = result.get("call_counts", {})
    attempts = report["http_attempts"]
    report["observation_reads"] = len(reads)
    report["correct_table_queries"] = len(correct_queries)
    report["reported_provider_requests"] = result.get("usage", {}).get("provider_requests")
    checks.update(
        terminal_completed=result.get("status") == "completed"
        and bool(events)
        and events[-1]["event_type"] == "run.completed",
        required_inspect_read_query_finish_order=bool(
            inspect_calls and correct_reads and correct_queries and finishes
        )
        and inspect_calls[0]["seq"]
        < correct_reads[0]["seq"]
        < correct_queries[0]["seq"]
        < finishes[-1]["seq"],
        inspect_preview_is_truncated=any(item["inspect_preview_truncated"] for item in attempts),
        table_hidden_until_local_read=bool(attempts)
        and not any(item["table_visible_before_read"] for item in attempts)
        and not any(item["inspect_preview_contains_table"] for item in attempts),
        requested_page_reaches_real_planner=any(
            item["requested_table_page_visible"] for item in attempts
        ),
        provider_accounting_matches=bool(attempts)
        and len(attempts)
        == report["reported_provider_requests"]
        == counts.get("model_calls")
        == audit["reported_provider_requests_from_events"],
        local_read_and_tool_accounting=counts.get("observation_reads") == len(reads)
        and counts.get("tools") == report["tool_calls"] == len(tool_starts),
        citations_resolve=bool(result.get("citations")) and set(result["citations"]) <= evidence,
        model_and_endpoint_match=bool(attempts)
        and all(item["model_matches"] and item["endpoint_matches"] for item in attempts),
        working_context_is_byte_bounded=bool(attempts)
        and all(item["working_context_bytes"] <= 4096 for item in attempts),
        within_http_budget=0 < report["http_requests"] <= report["limits"]["max_requests"]
        and report["blocked_http_requests"] == 0,
        no_automatic_retries=counts.get("retries") == 0
        and all(event["attempt"] == 1 for event in starts),
        no_pending_asyncio_tasks=report["pending_asyncio_tasks"] == 0,
        no_unhandled_failure=report["failure"] is None,
    )
    report["passed"] = all(checks.values())


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--live", action="store_true", help="Explicitly authorize one remote run.")
    parser.add_argument("--output", type=Path, default=RUNTIME_ROOT / "runs" / "acceptance")
    parser.add_argument("--max-requests", type=int, choices=range(5, 9), default=8)
    parser.add_argument("--wall-timeout-ms", type=int, default=180_000)
    parser.add_argument("--call-timeout-ms", type=int, default=45_000)
    args = parser.parse_args()
    if not args.live:
        parser.error("No network call made. Pass --live to execute one remote acceptance run.")
    if not 1 <= args.call_timeout_ms <= args.wall_timeout_ms <= 180_000:
        parser.error("Require 1 <= call timeout <= wall timeout <= 180000 milliseconds.")
    output = args.output.expanduser().resolve() / ("live-harness-" + uuid4().hex)
    output.mkdir(parents=True, mode=0o700)
    started = time.monotonic()
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "scope": "Real JsonPlanner; SYNTHETIC local DataPort and ContentPort; harness acceptance only.",
        "base_url": BASE_URL,
        "model_id": MODEL_ID,
        "run_id": "harness-" + uuid4().hex,
        "limits": {
            "attempts": 1,
            "max_requests": args.max_requests,
            "wall_timeout_ms": args.wall_timeout_ms,
            "call_timeout_ms": args.call_timeout_ms,
            "http_retries": 0,
            "data_retries": 0,
        },
        "credential_in_report": False,
        "http_attempts": [],
        "http_requests": 0,
        "http_responses": 0,
        "blocked_http_requests": 0,
        "tool_calls": 0,
        "pending_asyncio_tasks": 0,
        "failure": None,
        "checks": {},
        "passed": False,
        "source_sha256": {},
    }
    try:
        report["source_sha256"] = source_hashes()
        asyncio.run(execute(args, report, output))
    except (Exception, KeyboardInterrupt) as exc:
        # Never print exception messages or tracebacks: imported credential
        # helpers and third-party transports may attach sensitive arguments.
        report["failure"] = {"stage": "execution", "type": type(exc).__name__}
    try:
        finalize_checks(report, output)
    except Exception as exc:
        report["failure"] = {"stage": "report_audit", "type": type(exc).__name__}
        report["passed"] = False
    report["duration_ms"] = int((time.monotonic() - started) * 1000)
    target = output / "report.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {
                "report": str(target),
                "passed": report["passed"],
                "http_requests": report["http_requests"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
