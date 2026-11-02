"""Opt-in live acceptance for the controller with a real PlannerPort.

Data and Content ports deliberately use synthetic local fixtures. This checks
control flow and integration contracts, never real retrieval/answer quality.
Run explicitly from the runtime directory; offline pytest never imports it.
Credentials come from an environment variable or the authorized macOS Keychain
item and are never emitted or stored in reports, traces, or command arguments.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import ctypes.util
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import httpx

from lake_agent import models as m
from lake_agent.demo import DemoContent, DemoData
from lake_agent.planner import JsonPlanner
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink


def read_credential(base_url: str) -> str:
    """Retrieve an explicitly configured secret without ever printing it."""
    if os.environ.get("MODEL_API_KEY"):
        return os.environ["MODEL_API_KEY"]
    security = ctypes.CDLL(ctypes.util.find_library("Security"))
    pointer = ctypes.c_void_p
    length = ctypes.c_uint32()
    password = pointer()
    service = b"COMP7608 data-lake provider testing"
    account = base_url.encode()
    security.SecKeychainFindGenericPassword.argtypes = [
        pointer,
        ctypes.c_uint32,
        ctypes.c_char_p,
        ctypes.c_uint32,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(pointer),
        pointer,
    ]
    security.SecKeychainFindGenericPassword.restype = ctypes.c_int32
    security.SecKeychainItemFreeContent.argtypes = [pointer, pointer]
    status = security.SecKeychainFindGenericPassword(
        None,
        len(service),
        service,
        len(account),
        account,
        ctypes.byref(length),
        ctypes.byref(password),
        None,
    )
    if status != 0:
        raise RuntimeError(f"Credential lookup failed with OS status {status}.")
    try:
        return ctypes.string_at(password, length.value).decode()
    finally:
        security.SecKeychainItemFreeContent(None, password)


class FeedbackFixture(DemoContent):
    """Ask for one real replanning pass; verification itself remains a fixture."""

    def __init__(self):
        super().__init__()
        self.target = None
        self.verify_calls = 0

    async def search(self, request):
        self.target = request.subtask.subtask_id
        return await super().search(request)

    async def verify(self, request):
        self.verify_calls += 1
        if self.verify_calls > 1:
            return await super().verify(request)
        return m.ModuleResult(
            call_id=request.call_id,
            usage=m.Usage.local(),
            data=m.Verification(
                verdict="insufficient",
                recommendation=m.Recommendation(
                    action="search_more",
                    focus=m.Focus(subtask_id=self.target, query="2024 annual revenue HKD"),
                ),
            ),
        )


def inspect_run(result: m.RunResult, root: Path, *, feedback: bool) -> dict:
    """Validate committed events, artifacts, provenance and dispatch accounting."""
    trace = Path(result.trace_ref) if result.trace_ref else None
    if trace is None:
        raise AssertionError("The controller did not return a trace.")
    events = [m.TraceEvent.model_validate_json(line) for line in trace.read_text().splitlines()]
    assert [event.seq for event in events] == list(range(1, len(events) + 1))
    assert all(event.run_id == result.run_id for event in events)
    artifacts = {}
    for event in events:
        if event.data_ref:
            artifact = trace.parent / "artifacts" / f"{event.data_ref}.json"
            artifacts[event.data_ref] = json.loads(artifact.read_text())
    starts = {e.call_id for e in events if e.event_type.endswith(".call.started")}
    ends = [e for e in events if e.event_type.endswith((".call.completed", ".call.failed"))]
    assert len(starts) == len(ends) == len({e.call_id for e in ends})
    assert starts == {e.call_id for e in ends}
    reported = sum(e.usage.provider_requests for e in ends if e.usage)
    assert reported == result.call_counts.model_calls == result.usage.provider_requests
    evidence = {e.data_ref for e in events if e.event_type == "evidence.added"}
    if result.answer:
        assert result.answer.citations and set(result.answer.citations) <= evidence
    for ref in evidence:
        assert artifacts[ref]["call_id"] in starts
        assert artifacts[ref]["source_id"] in {"demo-annual", "demo-quarter"}
    accepted_feedback = sum(e.event_type == "feedback.accepted" for e in events)
    checks = {
        "terminal_completed": result.status == "completed"
        and events[-1].event_type == "run.completed",
        "answer_has_citations": bool(result.answer and result.answer.citations),
        "provider_accounting_matches_events": True,
        "trace_sequence_and_ownership": True,
        "every_call_has_one_terminal_event": True,
        "artifacts_exist_and_citations_resolve": True,
        "feedback_round_expected": accepted_feedback == (1 if feedback else 0),
    }
    return {
        "run_id": result.run_id,
        "mode": result.mode,
        "status": result.status,
        "stop_reason": result.stop_reason,
        "duration_ms": result.duration_ms,
        "call_counts": result.call_counts.model_dump(),
        "usage": result.usage.model_dump(),
        "event_count": len(events),
        "evidence_count": len(evidence),
        "feedback_rounds": accepted_feedback,
        "trace_ref": str(trace.relative_to(root)),
        "checks": checks,
        "passed": all(checks.values()),
    }


async def acceptance(args, secret: str) -> dict:
    started = time.monotonic()
    output = args.output.resolve() / ("live-" + uuid4().hex)
    await asyncio.to_thread(output.mkdir, parents=True)
    observations = []

    async def observe_response(response):
        # Do not retain headers, credential, raw content, or provider error text.
        observations.append({"http_status": response.status_code})

    async with httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(retries=0),
        follow_redirects=False,
        event_hooks={"response": [observe_response]},
    ) as client:
        async with JsonPlanner(args.base_url, secret, client=client) as planner:

            async def scenario(name, mode, *, content=None, feedback=False):
                request = m.RunRequest(
                    run_id=name + "-" + uuid4().hex,
                    task_id="synthetic-acceptance-" + name,
                    question="Find the 2024 annual revenue and currency in the synthetic annual report. The local evidence must support the answer.",
                    mode=mode,
                    config=m.RunConfig(
                        model_id=args.model,
                        model_params={},
                        max_wall_time_ms=180000,
                        per_call_timeout_ms=45000,
                        max_subtasks=1,
                        max_plan_steps=4,
                        max_model_calls=8,
                        max_data_calls=8,
                        max_feedback_rounds=1,
                        prompt_versions=m.PromptVersions(
                            planner="planner-v0.1", content="local-fixture-v1"
                        ),
                        config_version="controller-acceptance-v1",
                    ),
                )
                result = await run(
                    request,
                    data=DemoData(page_size=1),
                    content=content or DemoContent(),
                    planner=planner,
                    trace=JsonlTraceSink(output / "runs"),
                )
                summary = await asyncio.to_thread(inspect_run, result, output, feedback=feedback)
                summary["scenario"] = name
                print(json.dumps(summary, ensure_ascii=False), flush=True)
                return summary

            summaries = []
            summaries.append(await scenario("fixed", "fixed_retrieval"))
            summaries.append(await scenario("no-verifier", "no_verifier"))
            # Sharing a reentrant planner/client and content fixture exercises
            # run-local handles, budgets, citations and independent TraceSinks.
            shared = DemoContent()
            pair = await asyncio.gather(
                scenario("concurrent-a", "full_agent", content=shared),
                scenario("concurrent-b", "full_agent", content=shared),
            )
            summaries.extend(pair)
            concurrent_isolated = (
                len({s["run_id"] for s in pair}) == 2 and len({s["trace_ref"] for s in pair}) == 2
            )
            summaries.append(
                await scenario("feedback", "full_agent", content=FeedbackFixture(), feedback=True)
            )

    # No task created by the runtime/client should remain after closing them.
    pending_tasks = [
        t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
    ]
    reported_requests = sum(s["usage"]["provider_requests"] for s in summaries)
    tracked_requests = len(observations)
    assert reported_requests == tracked_requests
    inputs = await asyncio.to_thread(lambda: list(Path("src/lake_agent").glob("*.py")))
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    report = {
        "created_at": datetime.now().astimezone().isoformat(),
        "scope": "Controller acceptance with real JsonPlanner, synthetic local DataPort and ContentPort; not a real data-lake end-to-end benchmark.",
        "base_url": args.base_url,
        "model_id": args.model,
        "credential_in_report": False,
        "cases": summaries,
        "concurrent_runs_isolated": concurrent_isolated,
        "http_responses": observations,
        "observed_provider_requests": tracked_requests,
        "reported_provider_requests": reported_requests,
        "pending_asyncio_tasks": len(pending_tasks),
        "duration_ms": int((time.monotonic() - started) * 1000),
        "source_sha256": hashes,
        "passed": all(s["passed"] for s in summaries) and concurrent_isolated and not pending_tasks,
    }
    target = output / "report.json"
    await asyncio.to_thread(
        target.write_text,
        json.dumps(report, ensure_ascii=False, indent=2).replace(secret, "[REDACTED]") + "\n",
    )
    print(
        json.dumps(
            {
                "report": str(target),
                "passed": report["passed"],
                "provider_requests": tracked_requests,
                "duration_ms": report["duration_ms"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="https://codex.miaomiaocode.com/v1")
    parser.add_argument("--model", default="gpt-6.1-sol")
    parser.add_argument("--output", type=Path, default=Path("runs/acceptance"))
    args = parser.parse_args()
    secret = read_credential(args.base_url)
    report = asyncio.run(acceptance(args, secret))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
