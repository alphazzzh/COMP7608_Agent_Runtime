"""仅在显式启用后，执行一次安全并行工具调用的在线验收。

只有 JsonPlanner 使用远程模型。DataPort 和 ContentPort 是本地合成测试替身；
本脚本验证控制器行为，不验证检索或答案质量。单个子任务必须生成一个计划，
其中含两个独立的 extract 调用，然后结束。invoke 内的事件同步点要求两个
工作任务同时处于活动状态；串行执行最迟会在五秒后失败。

从 project/runtime 运行 ``.venv/bin/python scripts/live_parallel_acceptance.py
--live``。凭据复用已获授权的环境变量/Keychain 辅助函数，绝不打印或有意持久化。
只执行一次运行，最多发起六次 HTTP 请求，不进行 HTTP 重定向或重试，也不重试数据操作。
普通失败和用户中断仍会生成经脱敏的报告；操作系统强制终止或输出目录不可写时，
无法保证生成报告。离线调用方可以注入 MockTransport。
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
from live_controller_acceptance import read_credential
from live_harness_acceptance import inspect_trace
from live_harness_acceptance import source_hashes as harness_source_hashes

from lake_agent import __version__
from lake_agent import models as m
from lake_agent.demo import EXTRACT_SCHEMA, DemoContent
from lake_agent.planner import JsonPlanner
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink

RUNTIME_ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "https://codex.miaomiaocode.com/v1"
MODEL_ID = "gpt-6.1-sol"
FIXTURE_TIMEOUT_SECONDS = 5


class SyntheticData:
    """两个独立的只读数据源，带有等待时限的重叠执行同步点。"""

    def __init__(self) -> None:
        self.sources = [
            m.SourceRecord(
                source_id="synthetic-parallel-" + uuid4().hex,
                name=f"SYNTHETIC independent source {index}",
                format="text",
                summary=f"Synthetic independent fixture number {index}; local data only.",
                version="synthetic-parallel-v1",
            )
            for index in (1, 2)
        ]
        self.invocations: list[m.ToolRequest] = []
        self.catalog_calls = 0
        self.active: set[str] = set()
        self.children: set[asyncio.Task] = set()
        self.peak_active = 0
        self.gate_timeouts = 0
        self.gate_released = False
        self.gate = asyncio.Event()

    def tool_specs(self) -> list[m.ToolSpec]:
        """仅允许普通 extract 模式在数据源互不冲突时并行执行。"""
        return [
            m.ToolSpec(
                name="extract",
                description="SYNTHETIC local read-only extract from one independent source.",
                input_schema=EXTRACT_SCHEMA,
                idempotent=True,
                uses_sources=True,
                source_id_paths=["/source_id"],
                parallel_safe=True,
                exclusive_resource_keys=[],
            )
        ]

    async def catalog(self, request: m.CatalogRequest) -> m.CatalogResult:
        """在单页冻结的本地目录中公开两个数据源。"""
        self.catalog_calls += 1
        return m.CatalogResult(
            call_id=request.call_id,
            catalog_snapshot_id="synthetic-parallel-v1",
            sources=self.sources,
        )

    async def invoke(self, request: m.ToolRequest) -> m.ToolResult:
        """仅在两个提取工作任务的函数体实际重叠执行后，才允许二者继续。

        首次 await 之前的状态更新在此事件循环上是原子的，无需锁或后台定时任务。
        控制器持有工作任务并等待其收尾；测试替身保留任务引用，仅用于审计其是否已退出。
        """
        self.invocations.append(request)
        self.active.add(request.call_id)
        self.children.add(asyncio.current_task())
        self.peak_active = max(self.peak_active, len(self.active))
        if len(self.active) == 2:
            self.gate_released = True
            self.gate.set()
        try:
            try:
                async with asyncio.timeout(FIXTURE_TIMEOUT_SECONDS):
                    await self.gate.wait()
            except TimeoutError:
                self.gate_timeouts += 1
                return m.ToolResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(
                        code="synthetic_overlap_timeout",
                        message="SYNTHETIC fixture did not observe two concurrent extracts.",
                    ),
                )
            source_id = request.args["source_id"]
            text = f"SYNTHETIC independent fixture evidence from {source_id}."
            max_chars = request.args["max_chars"]
            return m.ToolResult(
                call_id=request.call_id,
                data={
                    "evidence": [
                        {
                            "source_id": source_id,
                            "content_type": "text",
                            "payload": text[:max_chars],
                            "locator": {"type": "synthetic_fixture", "line": 1},
                            "warnings": ["SYNTHETIC local fixture; not real retrieval evidence"],
                        }
                    ]
                },
                truncated=len(text) > max_chars,
            )
        finally:
            self.active.remove(request.call_id)


class SyntheticContent(DemoContent):
    """在本地模拟目录、搜索、答案组织与校验，不进行模型推理。"""

    async def compose(self, request: m.ComposeRequest) -> m.ModuleResult[m.AnswerRecord]:
        """引用控制器实际持有的全部证据，并显著标明答案的适用范围。"""
        if not request.evidence:
            raise ValueError("Synthetic composition requires controller-owned evidence.")
        return m.ModuleResult(
            call_id=request.call_id,
            usage=m.Usage.local(),
            data=m.AnswerRecord(
                text="SYNTHETIC local composition only: "
                + " ".join(str(item.payload) for item in request.evidence),
                citations=[item.evidence_id for item in request.evidence],
            ),
        )

    async def verify(self, request: m.VerifyRequest) -> m.ModuleResult[m.Verification]:
        """只检查合成测试数据的来源，明确不作事实核验。"""
        supported = (
            len(request.evidence) == 2
            and len({item.source_id for item in request.evidence}) == 2
            and set(request.answer.citations) == {item.evidence_id for item in request.evidence}
            and request.answer.text.startswith("SYNTHETIC local composition only:")
            and all(str(item.payload).startswith("SYNTHETIC") for item in request.evidence)
        )
        return m.ModuleResult(
            call_id=request.call_id,
            usage=m.Usage.local(),
            data=m.Verification(
                verdict="supported" if supported else "insufficient",
                recommendation=m.Recommendation(action="none" if supported else "stop"),
            ),
        )


class CountingTransport(httpx.AsyncBaseTransport):
    """统计实际请求次数，不保留请求头、提示词或响应。"""

    def __init__(
        self, max_requests: int, *, delegate: httpx.AsyncBaseTransport | None = None
    ) -> None:
        if not 1 <= max_requests <= 6:
            raise ValueError("Acceptance allows between one and six HTTP requests.")
        self.delegate = delegate if delegate is not None else httpx.AsyncHTTPTransport(retries=0)
        self.max_requests = max_requests
        self.attempts: list[dict] = []
        self.blocked_requests = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """同时记录失败的请求派发与已收到的 HTTP 响应。"""
        if len(self.attempts) >= self.max_requests:
            self.blocked_requests += 1
            raise httpx.RequestError("Acceptance HTTP request budget exhausted.", request=request)
        body = json.loads(request.content)
        supplied = json.loads(body["messages"][-1]["content"])
        record = {
            "index": len(self.attempts) + 1,
            "phase": supplied["phase"],
            "model_matches": body.get("model") == MODEL_ID,
            "endpoint_matches": str(request.url) == BASE_URL + "/chat/completions",
            "request_bytes": len(request.content),
            "http_status": None,
            "transport_failed": False,
        }
        self.attempts.append(record)
        try:
            response = await self.delegate.handle_async_request(request)
        except BaseException:
            # 异常消息可能包含请求头或服务商响应正文。
            record["transport_failed"] = True
            raise
        record["http_status"] = response.status_code
        return response

    async def aclose(self) -> None:
        """即使凭据查询失败，也关闭底层传输器。"""
        await self.delegate.aclose()


def source_hashes() -> dict[str, str]:
    """复用执行框架的源码清单，并纳入本脚本与并行测试源码。"""
    hashes = harness_source_hashes()
    paths = [Path(__file__).resolve(), RUNTIME_ROOT / "tests" / "test_execution.py"]
    paths.extend(sorted((RUNTIME_ROOT / "tests").glob("test_parallel*.py")))
    for path in paths:
        hashes[str(path.relative_to(RUNTIME_ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(hashes.items()))


def new_report(args: argparse.Namespace) -> dict:
    """在访问凭据或网络之前，初始化即使失败也可保留的报告。"""
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "scope": "Real JsonPlanner; SYNTHETIC local DataPort and ContentPort; parallel control only.",
        "base_url": BASE_URL,
        "model_id": MODEL_ID,
        "runtime_version": __version__,
        "package_version": tomllib.loads((RUNTIME_ROOT / "pyproject.toml").read_text())["project"][
            "version"
        ],
        "run_id": "parallel-" + uuid4().hex,
        "limits": {
            "attempts": 1,
            "max_requests": args.max_requests,
            "wall_timeout_ms": args.wall_timeout_ms,
            "call_timeout_ms": args.call_timeout_ms,
            "http_retries": 0,
            "data_retries": 0,
            "data_calls": 3,
            "fixture_timeout_seconds": FIXTURE_TIMEOUT_SECONDS,
        },
        "credential_in_report": False,
        "http_attempts": [],
        "http_requests": 0,
        "http_responses": 0,
        "blocked_http_requests": 0,
        "tool_calls": 0,
        "catalog_calls": 0,
        "peak_active": 0,
        "active_invocations": 0,
        "active_children": 0,
        "pending_asyncio_tasks": 0,
        "gate_released": False,
        "gate_timeouts": 0,
        "failure": None,
        "checks": {},
        "passed": False,
        "source_sha256": {},
    }


async def execute(
    args: argparse.Namespace,
    report: dict,
    output: Path,
    *,
    transport: CountingTransport | None = None,
) -> None:
    """执行一次预算受限的控制器运行；注入离线传输器时保持离线。"""
    data = SyntheticData()
    transport = transport if transport is not None else CountingTransport(args.max_requests)
    report["http_attempts"] = transport.attempts
    report["fixture_source_ids"] = sorted(source.source_id for source in data.sources)
    result = None
    request = m.RunRequest(
        run_id=report["run_id"],
        task_id="synthetic-live-parallel-acceptance",
        question=(
            "SYNTHETIC parallel controller acceptance only. Create exactly ONE subtask to "
            "collect one excerpt from EACH of the TWO independent synthetic candidate sources. "
            "In the first select_tools decision return ONE ToolPlan with exactly TWO extract "
            "steps, one per distinct candidate source. Both steps have empty depends_on; "
            "neither depends on the other. Use the registered extract arguments source_id, "
            "query and max_chars=1000. Copy the current subtask_id exactly. After both excerpts "
            "are present, return finish with reason evidence_complete. Do not split the two "
            "extracts into separate decisions or inspect/read observations. The SYNTHETIC "
            "local ContentPort will compose and verify; no real data-lake claim is assessed."
        ),
        mode="full_agent",
        config=m.RunConfig(
            model_id=MODEL_ID,
            model_params={},
            max_wall_time_ms=args.wall_timeout_ms,
            per_call_timeout_ms=args.call_timeout_ms,
            max_model_calls=args.max_requests,
            max_subtasks=1,
            max_plan_steps=2,
            max_data_calls=3,
            max_parallel_tool_calls=2,
            max_retries_per_data_operation=0,
            max_feedback_rounds=0,
            max_observation_reads=0,
            max_identical_tool_calls=1,
            candidate_top_k=2,
            code_version=__version__,
            config_version="manual-live-parallel-v1",
            prompt_versions=m.PromptVersions(content="synthetic-parallel-v1"),
        ),
    )
    report["run_config"] = request.config.model_dump(mode="json")
    try:
        # 命令行入口会先检查 --live，再调用此已获授权的凭据辅助函数。
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
        await transport.aclose()
        report.update(
            http_requests=len(transport.attempts),
            http_responses=sum(item["http_status"] is not None for item in transport.attempts),
            blocked_http_requests=transport.blocked_requests,
            tool_calls=len(data.invocations),
            catalog_calls=data.catalog_calls,
            peak_active=data.peak_active,
            active_invocations=len(data.active),
            active_children=sum(not task.done() for task in data.children),
            gate_released=data.gate_released,
            gate_timeouts=data.gate_timeouts,
            pending_asyncio_tasks=sum(
                task is not asyncio.current_task() and not task.done()
                for task in asyncio.all_tasks()
            ),
        )
        if result is not None:
            report["result"] = {
                "status": result.status,
                "stop_reason": result.stop_reason,
                "duration_ms": result.duration_ms,
                "usage": result.usage.model_dump(mode="json"),
                "call_counts": result.call_counts.model_dump(mode="json"),
                "versions": result.versions.model_dump(mode="json"),
                "citations": result.answer.citations if result.answer else [],
            }


def finalize_checks(report: dict, output: Path) -> None:
    """审计持久化计划、执行顺序、证据归属及实际请求计数。"""
    audit, artifacts, events = inspect_trace(
        output / "runs" / report["run_id"] / "events.jsonl", report["run_id"]
    )
    report["trace"] = audit
    plans = [
        artifacts.get(event["data_ref"], {})
        for event in events
        if event["event_type"] == "plan.accepted"
    ]
    task_plans = [
        artifacts.get(event["data_ref"], {})
        for event in events
        if event["event_type"] == "task_plan.accepted"
    ]
    steps = plans[0].get("steps", []) if len(plans) == 1 else []
    starts = [event for event in events if event["event_type"] == "tool.call.started"]
    terminals = [
        event
        for event in events
        if event["event_type"] in {"tool.call.completed", "tool.call.failed"}
    ]
    requests = [artifacts.get(event["data_ref"], {}) for event in starts]
    evidence = [
        artifacts.get(event["data_ref"], {})
        for event in events
        if event["event_type"] == "evidence.added"
    ]
    finishes = [
        event
        for event in events
        if event["event_type"] == "planner.select_tools.call.completed"
        and artifacts.get(event["data_ref"], {}).get("data", {}).get("kind") == "finish"
        and artifacts.get(event["data_ref"], {}).get("data", {}).get("reason")
        == "evidence_complete"
    ]
    step_ids = [step.get("step_id") for step in steps]
    call_ids = [event["call_id"] for event in starts]
    owned_sources = {
        request.get("call_id"): request.get("args", {}).get("source_id") for request in requests
    }
    evidence_ids = {item.get("evidence_id") for item in evidence}
    result = report.get("result", {})
    counts = result.get("call_counts", {})
    attempts = report["http_attempts"]
    report["plan_steps"] = steps
    report["evidence_ownership"] = [
        {key: item.get(key) for key in ("evidence_id", "call_id", "source_id")} for item in evidence
    ]
    report["checks"].update(audit["checks"])
    report["checks"].update(
        terminal_completed=result.get("status") == "completed"
        and bool(events)
        and events[-1]["event_type"] == "run.completed",
        one_subtask_one_independent_two_source_plan=len(task_plans) == 1
        and len(task_plans[0].get("subtasks", [])) == 1
        and len(steps) == 2
        and len(set(step_ids)) == 2
        and all(
            step.get("tool_name") == "extract"
            and not step.get("depends_on")
            and step.get("source_ids") == [step.get("args", {}).get("source_id")]
            for step in steps
        )
        and {step.get("args", {}).get("source_id") for step in steps}
        == set(report.get("fixture_source_ids", [])),
        two_tool_intents_before_terminals=len(starts) == len(terminals) == 2
        and max(event["seq"] for event in starts) < min(event["seq"] for event in terminals),
        tool_commits_follow_plan_order=len(steps) == 2
        and [event["step_id"] for event in starts] == step_ids
        and [event["step_id"] for event in terminals] == step_ids
        and [item.get("call_id") for item in evidence] == call_ids,
        evidence_owned_by_invocation=len(evidence) == 2
        and all(
            item.get("source_id") in report.get("fixture_source_ids", [])
            and owned_sources.get(item.get("call_id")) == item.get("source_id")
            for item in evidence
        ),
        actual_two_invocations_overlap=report["peak_active"] == 2
        and report["gate_released"]
        and report["gate_timeouts"] == 0,
        finished_after_both_tools=len(finishes) == 1
        and len(terminals) == 2
        and finishes[0]["seq"] > max(event["seq"] for event in terminals),
        citations_resolve_to_both_sources=len(result.get("citations", [])) == 2
        and set(result.get("citations", [])) == evidence_ids,
        provider_accounting_matches=bool(attempts)
        and len(attempts)
        == result.get("usage", {}).get("provider_requests")
        == counts.get("model_calls")
        == audit["reported_provider_requests_from_events"],
        local_data_accounting=counts.get("catalog") == report["catalog_calls"] == 1
        and counts.get("tools") == report["tool_calls"] == len(starts) == 2
        and counts.get("steps") == 2,
        model_and_endpoint_match=bool(attempts)
        and all(item["model_matches"] and item["endpoint_matches"] for item in attempts),
        within_http_budget=0 < report["http_requests"] <= report["limits"]["max_requests"] <= 6
        and report["blocked_http_requests"] == 0,
        no_automatic_retries=counts.get("retries") == 0
        and all(
            event["attempt"] == 1
            for event in events
            if event["event_type"].endswith(".call.started")
        ),
        no_active_children=report["active_children"] == report["active_invocations"] == 0,
        no_pending_asyncio_tasks=report["pending_asyncio_tasks"] == 0,
        current_version_and_source_hashes=bool(report["source_sha256"])
        and report["source_sha256"] == source_hashes()
        and report["runtime_version"] == report["package_version"] == __version__
        and result.get("versions", {}).get("code_version") == __version__,
        no_unhandled_failure=report["failure"] is None,
    )
    report["passed"] = all(report["checks"].values())


def main() -> None:
    """要求显式启用在线执行，并始终持久化经脱敏的最终报告。"""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--live", action="store_true", help="Explicitly authorize one remote run.")
    parser.add_argument("--output", type=Path, default=RUNTIME_ROOT / "runs" / "acceptance")
    parser.add_argument("--max-requests", type=int, choices=range(1, 7), default=6)
    parser.add_argument("--wall-timeout-ms", type=int, default=120_000)
    parser.add_argument("--call-timeout-ms", type=int, default=45_000)
    args = parser.parse_args()
    if not args.live:
        parser.error("No network call made. Pass --live to execute one remote acceptance run.")
    if not 1 <= args.call_timeout_ms <= min(args.wall_timeout_ms, 45_000):
        parser.error("Require 1 <= call timeout <= min(wall timeout, 45000) milliseconds.")
    if not 1 <= args.wall_timeout_ms <= 120_000:
        parser.error("Require 1 <= wall timeout <= 120000 milliseconds.")
    output = args.output.expanduser().resolve() / ("live-parallel-" + uuid4().hex)
    output.mkdir(parents=True, mode=0o700)
    started = time.monotonic()
    report = new_report(args)
    try:
        report["source_sha256"] = source_hashes()
        asyncio.run(execute(args, report, output))
    except (Exception, KeyboardInterrupt) as exc:
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
                "peak_active": report["peak_active"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
