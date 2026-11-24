"""三组消融的统一预算、审计、真实统计信息流和 CLI 校验。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from lake_agent import models as m
from lake_agent.adapters.planner import JsonPlanner
from lake_agent.evaluation.models import EvaluationImport
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink
from lake_agent.web.evaluation_storage import EvaluationRepository
from lake_agent.web.profiles import ExecutionPorts
from tests import ablation_runtime as cli
from tests.support.ablation import CapturingTrace, FixedRetrievalPlanner, run_ablation
from tests.support.ablation_cases import build_step
from tests.support.ablation_metrics import EvaluationSample, EvaluationTask, aggregate, score_sample
from tests.support.ports import DemoContent, DemoData, _reply


class MeasuredContent(DemoContent):
    """本地可测量模型用量；示例不代表模型质量。"""

    def __init__(self, requests):
        super().__init__()
        self.requests = requests

    async def compose(self, request):
        self.requests.append(request.model_dump(mode="json"))
        return m.ModuleResult(
            call_id=request.call_id,
            data=m.AnswerRecord(text="120", citations=[e.evidence_id for e in request.evidence]),
            usage=m.Usage(
                provider_requests=1, input_tokens=10, output_tokens=2, cost=0.01, currency="USD"
            ),
        )

    async def verify(self, request):
        self.requests.append(request.model_dump(mode="json"))
        return m.ModuleResult(
            call_id=request.call_id,
            data=m.Verification(
                verdict="supported", recommendation=m.Recommendation(action="none")
            ),
            usage=m.Usage(
                provider_requests=1, input_tokens=5, output_tokens=1, cost=0.005, currency="USD"
            ),
        )


def test_task_manifest_rejects_ambiguous_or_nonfinite_annotations():
    for values in (
        {"expected_answers": ()},
        {"numeric_answer": float("inf")},
        {"numeric_tolerance": -1},
        {"structured_answer_json": '{"x":1,"x":2}'},
    ):
        with pytest.raises(ValueError):
            EvaluationTask("task", "question", **values)


def test_python_manifest_rejects_bytes_before_running_any_experiment():
    """json.loads 接受 bytes，但它不属于可导出 JSON 清单的字符串类型。"""
    with pytest.raises(ValueError, match="string"):
        EvaluationTask("task", "question", structured_answer_json=b"120")


async def experiment(
    tmp_path,
    *,
    tasks=None,
    repeats=1,
    data_factory=DemoData,
    content_factory=MeasuredContent,
    unnecessary=None,
    data_usage_provider=None,
    config=None,
    step_builder=build_step,
    resume_samples=(),
    on_sample=None,
):
    calls, content_requests, data_requests, closed = [], [], [], []

    def respond(incoming):
        body = json.loads(incoming.content)
        calls.append(body)
        assert body["model"] == "frozen-model" and body["temperature"] == 0
        request = json.loads(body["messages"][1]["content"])["input"]
        context = request["context"]
        if context["has_answer"]:
            action = (
                {"kind": "verify"}
                if request["require_verification"] and (context["verification_verdict"] is None)
                else {"kind": "finish"}
            )
        elif context["evidence_summary"]:
            action = {"kind": "compose"}
        elif context["candidates"]:
            candidate = m.Candidate.model_validate(context["candidates"][0])
            action = {
                "kind": "tools",
                "steps": [
                    build_step(
                        candidate,
                        [m.ToolSpec.model_validate(spec) for spec in request["tool_specs"]],
                        request["question"],
                        0,
                    ).model_dump(mode="json")
                ],
            }
        else:
            action = {"kind": "search", "query": request["question"]}
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": json.dumps(action)}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 1},
            },
        )

    @asynccontextmanager
    async def ports():
        data = data_factory()
        original = data.invoke

        async def invoke(request):
            data_requests.append(request.model_dump(mode="json"))
            return await original(request)

        data.invoke = invoke
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            async with JsonPlanner(
                "https://offline.invalid/v1", "offline-key", client=client
            ) as planner:
                yield ExecutionPorts(data, content_factory(content_requests), planner)
        closed.append(True)

    tasks = tasks or [
        EvaluationTask(
            "task-1",
            "Annual revenue?",
            expected_answers=("120", "private-gold-never-sent"),
            relevant_source_ids=frozenset({"demo-annual"}),
            numeric_answer=120.0,
            structured_answer_json="120",
        )
    ]
    result = await run_ablation(
        tasks,
        ports_factory=ports,
        step_builder=step_builder,
        config=config
        or m.RunConfig(model_id="frozen-model", max_model_calls=12, candidate_top_k=2),
        dataset_id="synthetic-pool",
        task_set_id="frozen-task-manifest",
        repeats=repeats,
        trace_factory=lambda: JsonlTraceSink(tmp_path / "audit"),
        resume_samples=resume_samples,
        on_sample=on_sample,
        unnecessary=unnecessary,
        **({"data_usage_provider": data_usage_provider} if data_usage_provider else {}),
    )
    return result, calls, content_requests, data_requests, closed


async def test_three_groups_share_runtime_budgets_model_and_audit_without_gold_leak(tmp_path):
    outcome, calls, content, data, closed = await experiment(tmp_path, repeats=2)
    report = outcome.report.model_dump(mode="json")
    assert [row["mode"] for row in report["results"]] == [
        "fixed_retrieval",
        "no_verifier",
        "full_agent",
    ]
    assert [row["task_count"] for row in report["results"]] == [2, 2, 2]
    configurations = deepcopy(outcome.details["configurations"])
    assert [configurations[mode].pop("require_verification") for mode in configurations] == [
        False,
        False,
        True,
    ]
    assert all(value == next(iter(configurations.values())) for value in configurations.values())
    assert len(closed) == 6
    assert "private-gold-never-sent" not in json.dumps([calls, content, data])
    samples = outcome.details["samples"]
    assert samples[0]["mode"] == "fixed_retrieval"
    assert samples[0]["result"]["mode"] == "no_verifier"  # 仅复用运行传输模式
    assert samples[0]["result"]["call_counts"]["model_calls"] == 1
    assert samples[0]["result"]["call_counts"]["tools"] == 2
    for row in report["results"]:
        assert row["metrics"]["accuracy"] == row["metrics"]["completion_rate"] == 1
        assert row["metrics"]["valid_tool_call_rate"] == 1
        assert row["metrics"]["unnecessary_tool_call_rate"] is None
    assert report["results"][0]["metrics"]["retrieval_precision"] == 0.5
    assert report["results"][0]["metrics"]["retrieval_recall"] == 1
    assert report["results"][0]["metrics"]["retrieval_f1"] == pytest.approx(2 / 3)
    assert report["results"][0]["metrics"]["grounding"] == 0.5
    assert report["results"][1]["metrics"]["retrieval_precision"] == 1
    assert report["results"][2]["metrics"]["input_tokens_total"] == 60  # 5*3 + compose10 + verify5
    assert report["results"][2]["metrics"]["provider_requests_total"] == 14
    for sample in samples:
        path = sample["result"]["trace_ref"]
        events = [
            json.loads(line)
            for line in (await asyncio.to_thread(Path(path).read_text)).splitlines()
        ]
        assert events[-1]["event_type"] == "run.completed"
        has_verify = any(e["event_type"] == "content.verify.call.started" for e in events)
        assert has_verify == (sample["mode"] == "full_agent")
    repository = EvaluationRepository(tmp_path / "reports")
    await repository.start()
    saved = await repository.create(EvaluationImport.model_validate(report))
    assert (await repository.get(saved["evaluation_id"]))["results"] == report["results"]


async def test_retry_metrics_and_independent_unnecessary_labels_are_measured(tmp_path):
    class RetryData(DemoData):
        async def invoke(self, request):
            if request.attempt == 1:
                return m.ToolResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(code="busy", message="retry", retryable=True),
                )
            return await super().invoke(request)

    outcome, *_ = await experiment(
        tmp_path,
        data_factory=RetryData,
        unnecessary=lambda request: request["args"]["source_id"] == "demo-quarter",
    )
    rows = outcome.report.results
    assert rows[0].metrics.valid_tool_call_rate == 0.5
    assert rows[0].metrics.unnecessary_tool_call_rate == 0.5
    assert all(row.metrics.retry_recovery_rate == 1 for row in rows)
    assert rows[0].metrics.tool_calls_mean == 4
    assert rows[0].metrics.data_calls_mean == 5


async def test_only_measured_data_attempts_contribute_to_ablation_totals(tmp_path):
    """目录、失败重试和成功尝试分别计量；一个缺测只影响对应指标和模式。"""

    class MeteredData(DemoData):
        def __init__(self):
            super().__init__()
            self.measurements = {}

        async def catalog(self, request):
            self.measurements[request.call_id] = {"bytes_transferred": 30, "cost_usd": 0.03}
            return await super().catalog(request)

        async def invoke(self, request):
            self.measurements[request.call_id] = (
                {"bytes_transferred": 10, "cost_usd": None}
                if request.args["source_id"] == "demo-quarter"
                else {"bytes_transferred": 10, "cost_usd": 0.01}
            )
            if request.attempt == 1:
                return m.ToolResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(code="busy", message="retry", retryable=True),
                )
            return await super().invoke(request)

    def measured(data, run_id):
        assert run_id.startswith("eval-")
        return data.measurements

    outcome, *_ = await experiment(tmp_path, data_factory=MeteredData, data_usage_provider=measured)
    by_mode = {row.mode: row.metrics for row in outcome.report.results}
    assert by_mode["fixed_retrieval"].external_data_bytes_total == 70
    assert by_mode["fixed_retrieval"].external_data_cost_usd is None
    for mode in ("no_verifier", "full_agent"):
        assert by_mode[mode].external_data_bytes_total == 50
        assert by_mode[mode].external_data_cost_usd == pytest.approx(0.05)


async def test_usage_ignores_all_tool_intents_when_wave_stops_before_dispatch():
    """第二条意图超时后，首条待派发意图也不能让已测目录用量变成缺测。"""

    class ParallelData(DemoData):
        def tool_specs(self):
            return [
                spec.model_copy(update={"parallel_safe": True}) for spec in super().tool_specs()
            ]

        async def invoke(self, request):
            pytest.fail("No tool in a partially prepared wave should be dispatched")

    class SlowSecondIntent(CapturingTrace):
        async def append(self, event):
            await super().append(event)
            if event.event_type == "tool.call.started" and event.step_id == "fixed-1":
                await asyncio.sleep(0.35)

    task = EvaluationTask("partial-wave", "Annual revenue?")
    trace = SlowSecondIntent()
    result = await run(
        m.RunRequest(
            task_id=task.task_id,
            question=task.question,
            mode="no_verifier",
            config=m.RunConfig(per_call_timeout_ms=300, max_parallel_tool_calls=2),
        ),
        data=ParallelData(),
        content=DemoContent(),
        planner=FixedRetrievalPlanner(build_step),
        trace=trace,
    )
    assert (result.status, result.stop_reason) == ("timeout", "call_timeout")
    assert result.call_counts.tools == 2 and result.call_counts.steps == 0
    assert trace.events[-1].detail["pending_calls"][0]["step_id"] == "fixed-0"
    catalog_call = next(e.call_id for e in trace.events if e.event_type == "catalog.call.completed")
    sample = score_sample(
        task,
        "fixed_retrieval",
        0,
        result,
        trace.events,
        trace.artifacts,
        data_usage={catalog_call: {"bytes_transferred": 30, "cost_usd": 0.03}},
    )
    assert sample.external_data_bytes_total == 30
    assert sample.external_data_cost_usd == 0.03


async def test_unknown_annotations_and_usage_remain_null(tmp_path):
    class UnknownContent(MeasuredContent):
        async def compose(self, request):
            response = await super().compose(request)
            response.usage = m.Usage(provider_requests=1)
            return response

    outcome, *_ = await experiment(
        tmp_path, tasks=[EvaluationTask("unknown", "question")], content_factory=UnknownContent
    )
    for row in outcome.report.results:
        assert row.metrics.accuracy is None and row.metrics.retrieval_f1 is None
        assert row.metrics.grounding is None and row.metrics.token_total is None
        assert row.metrics.cost_usd is None
        assert row.metrics.external_data_bytes_total is row.metrics.external_data_cost_usd is None


async def test_measured_structured_result_is_scored_before_explanatory_answer_text(tmp_path):
    class StructuredContent(MeasuredContent):
        async def compose(self, request):
            reply = await super().compose(request)
            reply.data.text = "The total is 120."
            reply.data.structured_result = 120.0
            return reply

    outcome, *_ = await experiment(
        tmp_path,
        tasks=[
            EvaluationTask(
                "structured", "question", numeric_answer=120.0, structured_answer_json="120.0"
            )
        ],
        content_factory=StructuredContent,
    )
    assert all(
        row.metrics.numerical_accuracy == row.metrics.structured_accuracy == 1
        for row in outcome.report.results
    )


@pytest.mark.parametrize(
    "gold,tolerance,text,structured,score",
    [
        (9007199254740992, 0, "9007199254740993", None, 0),
        (9007199254740992, 0, "Explanation", 9007199254740993, 0),
        ("9007199254740993.125", 0, "9007199254740993.125", None, 1),
        ("9007199254740993.125", 0, "9007199254740993.126", None, 0),
        ("0", "1.00000000000000000000000000001", "1.00000000000000000000000000002", None, 0),
        ("0.1", "0.2", "0.3", None, 1),
    ],
)
async def test_numeric_manifest_and_scoring_keep_exact_decimal_precision(
    tmp_path, gold, tolerance, text, structured, score
):
    """通过真实清单和三组评分验证大整数及绝对误差边界。"""

    class NumericContent(MeasuredContent):
        async def compose(self, request):
            reply = await super().compose(request)
            reply.data.text = text
            reply.data.structured_result = structured
            return reply

    manifest = tmp_path / "tasks.json"
    manifest.write_text(
        '[{"task_id":"exact","question":"Return a number",'
        f'"numeric_answer":{gold},"numeric_tolerance":{tolerance}}}]'
    )
    outcome, *_ = await experiment(
        tmp_path, tasks=cli._tasks(manifest), content_factory=NumericContent
    )
    assert all(row.metrics.numerical_accuracy == score for row in outcome.report.results)
    manifest.write_text(json.dumps(outcome.details["manifest"]))
    roundtrip = cli._tasks(manifest)[0]
    assert str(roundtrip.numeric_answer) == str(gold)
    assert str(roundtrip.numeric_tolerance) == str(tolerance)


async def test_structured_json_scoring_keeps_decimal_precision(tmp_path):
    class StructuredContent(MeasuredContent):
        async def compose(self, request):
            reply = await super().compose(request)
            reply.data.text = '{"value":0.10000000000000000002}'
            return reply

    outcome, *_ = await experiment(
        tmp_path,
        tasks=[
            EvaluationTask(
                "structured-decimal",
                "Return the exact value",
                structured_answer_json='{"value":0.10000000000000000001}',
            )
        ],
        content_factory=StructuredContent,
    )
    assert all(row.metrics.structured_accuracy == 0 for row in outcome.report.results)


async def test_fixed_baseline_keeps_one_shot_answer_without_any_candidates(tmp_path):
    class EmptyContent(MeasuredContent):
        async def search(self, request):
            return _reply(request, m.SearchResponse(candidates=[]))

    # 测试固定策略本身，避免自主测试规划器持续搜索空结果。
    result = await run(
        m.RunRequest(task_id="empty", question="Unknown", mode="no_verifier"),
        data=DemoData(),
        content=EmptyContent([]),
        planner=FixedRetrievalPlanner(build_step),
        trace=CapturingTrace(),
    )
    assert result.status == "completed" and result.answer.citations == []
    assert result.call_counts.tools == 0 and result.call_counts.model_calls == 1


@pytest.mark.parametrize("candidates", [False, True])
async def test_fixed_baseline_does_not_reenter_stages_after_compose_rejection(candidates):
    """反馈能力开启时，被拒的生成仍只执行一次并保留在评分分母。"""

    class RejectOnceContent(MeasuredContent):
        content_feedback_protocol = m.CONTENT_FEEDBACK_PROTOCOL

        def __init__(self):
            super().__init__([])
            self.searches = 0

        async def search(self, request):
            self.searches += 1
            if not candidates:
                return _reply(request, m.SearchResponse(candidates=[]))
            return await super().search(request)

        async def compose(self, request):
            reply = await super().compose(request)
            if len(self.requests) == 1:
                reply.data.citations = ["not-an-evidence-id"]
            return reply

    task = EvaluationTask("fixed-rejected", "Annual revenue?", expected_answers=("120",))
    content, trace = RejectOnceContent(), CapturingTrace()
    result = await run(
        m.RunRequest(task_id=task.task_id, question=task.question, mode="no_verifier"),
        data=DemoData(),
        content=content,
        planner=FixedRetrievalPlanner(build_step),
        trace=trace,
    )
    assert content.searches == 1 and len(content.requests) == 1
    assert sum(e.event_type == "plan.accepted" for e in trace.events) == int(candidates)
    assert result.status == "insufficient_evidence" and result.answer is None
    sample = score_sample(task, "fixed_retrieval", 0, result, trace.events, trace.artifacts)
    metrics, _ = aggregate([sample])
    assert metrics["accuracy"] == metrics["completion_rate"] == 0


def test_aggregate_requires_actual_samples():
    with pytest.raises(ValueError, match="empty"):
        aggregate([])


@pytest.mark.parametrize("gold", ["NaN", "Infinity", "1e999", '{"x":1,"x":2}'])
def test_structured_gold_rejects_nonfinite_and_duplicate_json(gold):
    with pytest.raises(ValueError):
        EvaluationTask("task", "question", structured_answer_json=gold)


@pytest.mark.parametrize(
    "answer,gold,score",
    [
        ("true", "1", 0),
        ('{"x":false}', '{"x":0}', 0),
        ('{"b":2,"a":1}', '{"a":1,"b":2}', 1),
        ("1.0", "1.00", 1),
        ("1e2", "100.0", 1),
        ("1e999", "1", 0),
    ],
)
async def test_structured_scoring_is_type_sensitive_and_rejects_invalid_output(
    tmp_path, answer, gold, score
):
    class StructuredContent(MeasuredContent):
        async def compose(self, request):
            reply = await super().compose(request)
            reply.data.text = answer
            return reply

    outcome, *_ = await experiment(
        tmp_path,
        tasks=[EvaluationTask("structured", "question", structured_answer_json=gold)],
        content_factory=StructuredContent,
    )
    assert all(row.metrics.structured_accuracy == score for row in outcome.report.results)


async def test_missing_one_annotation_does_not_selectively_average_other_tasks(tmp_path):
    outcome, *_ = await experiment(
        tmp_path,
        tasks=[
            EvaluationTask("known", "question", expected_answers=("120",)),
            EvaluationTask("unknown", "question"),
        ],
    )
    assert all(row.metrics.accuracy is None for row in outcome.report.results)
    assert all(
        value["accuracy"]["known_n"] == 1 for value in outcome.details["variability"].values()
    )


async def test_metric_coverage_excludes_explicitly_inapplicable_tasks_but_keeps_unknowns(tmp_path):
    outcome, *_ = await experiment(
        tmp_path,
        tasks=[
            EvaluationTask("numeric", "Return a number", numeric_answer=120),
            EvaluationTask(
                "text",
                "Return text",
                not_applicable_metrics=frozenset({"numerical_accuracy"}),
            ),
        ],
    )
    rows = {row.mode: row for row in outcome.report.results}
    for row in rows.values():
        assert row.metrics.numerical_accuracy == 1
        assert row.metric_coverage["numerical_accuracy"].model_dump() == {
            "applicable_count": 1,
            "not_applicable_count": 1,
            "unknown_count": 0,
        }
    assert outcome.details["metric_coverage"]["full_agent"]["numerical_accuracy"] == {
        "applicable_count": 1,
        "not_applicable_count": 1,
        "unknown_count": 0,
    }

    unknown_path = tmp_path / "with-unknown"
    unknown_path.mkdir()
    mixed, *_ = await experiment(
        unknown_path,
        tasks=[
            EvaluationTask("numeric", "Return a number", numeric_answer=120),
            EvaluationTask(
                "text",
                "Return text",
                not_applicable_metrics=frozenset({"numerical_accuracy"}),
            ),
            EvaluationTask("unannotated", "Unknown applicability"),
        ],
    )
    for row in mixed.report.results:
        assert row.metrics.numerical_accuracy is None
        assert row.metric_coverage["numerical_accuracy"].unknown_count == 1


def test_report_count_metrics_preserve_large_integers_and_reject_fractions():
    raw = {
        "title": "integer counts",
        "dataset_id": "dataset",
        "task_set_id": "tasks",
        "model_id": "model",
        "evaluator": "test",
        "evaluated_at": "2026-11-19T00:00:00Z",
        "results": [
            {
                "mode": "full_agent",
                "task_count": 1,
                "metrics": {
                    "external_data_bytes_total": 9007199254740993,
                    "token_total": 9007199254740993,
                    "provider_requests_total": 1,
                    "input_tokens_total": 2,
                    "output_tokens_total": 3,
                },
            }
        ],
    }
    metrics = EvaluationImport.model_validate(raw).results[0].metrics
    assert metrics.external_data_bytes_total == 9007199254740993
    assert metrics.token_total == 9007199254740993
    for field in (
        "external_data_bytes_total",
        "token_total",
        "input_tokens_total",
        "output_tokens_total",
        "provider_requests_total",
    ):
        raw["results"][0]["metrics"][field] = 1.5
        with pytest.raises(ValidationError):
            EvaluationImport.model_validate(raw)
        raw["results"][0]["metrics"][field] = 1


async def test_fixed_retrieval_rejects_steps_outside_top_k():
    def choose_other_source(candidate, specs, question, index):
        step = build_step(candidate, specs, question, index)
        step.source_ids = ["not-in-top-k"]
        step.args["source_id"] = "not-in-top-k"
        return step

    planner = FixedRetrievalPlanner(choose_other_source)
    planner.stage = "retrieval"
    request = SimpleNamespace(
        call_id="fixed-planner-call",
        context={
            "last_action": {"kind": "search", "status": "ok", "count": 1},
            "candidates": [
                m.Candidate(source_id="demo-annual", score=1, match_reason="top result").model_dump(
                    mode="json"
                )
            ],
        },
        tool_specs=DemoData().tool_specs(),
        question="Question",
    )
    with pytest.raises(ValueError, match="outside the fixed retrieval candidates"):
        await planner.decide(request)


async def test_rejected_tool_results_do_not_count_as_access_or_recovery(tmp_path):
    class InvalidProvenanceData(DemoData):
        async def invoke(self, request):
            if request.attempt == 1:
                return m.ToolResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(code="busy", message="retry", retryable=True),
                )
            return m.ToolResult(
                call_id=request.call_id,
                data={
                    "evidence": [
                        {
                            "source_id": "demo-quarter",
                            "content_type": "text",
                            "payload": "unrelated source",
                            "locator": {},
                        }
                    ]
                },
            )

    for mode in ("fixed_retrieval", "no_verifier", "full_agent"):
        trace = CapturingTrace()
        ports = ExecutionPorts(
            InvalidProvenanceData(),
            MeasuredContent([]),
            FixedRetrievalPlanner(build_step),
        )
        result = await run(
            m.RunRequest(
                task_id="invalid-evidence",
                question="Question",
                mode="no_verifier" if mode == "fixed_retrieval" else mode,
                config=m.RunConfig(candidate_top_k=1),
            ),
            data=ports.data,
            content=ports.content,
            planner=ports.planner,
            trace=trace,
        )
        sample = score_sample(
            EvaluationTask(
                "invalid-evidence",
                "Question",
                relevant_source_ids=frozenset({"demo-annual"}),
            ),
            mode,
            0,
            result,
            trace.events,
            trace.artifacts,
        )
        assert result.status == "failed"
        assert sample.retrieved_sources == []
        assert sample.visited_sources == ["demo-annual"]
        assert sample.evidence_sources == []
        assert sample.recovered_operations == 0
        assert sample.failure_categories == ["tool_errors"]
        assert result.source_funnel.searched_sources == ["demo-annual"]
        assert result.source_funnel.visited_sources == ["demo-annual"]
        assert result.source_funnel.evidence_sources == []
        assert result.source_funnel.cited_sources == []


async def test_rejected_tool_steps_count_as_invalid_attempts_in_quality_metrics():
    class RepairingPlanner:
        agent_protocol = m.AGENT_PROTOCOL

        def __init__(self, actions):
            self.actions = iter(actions)

        async def decide(self, request):
            return m.ModuleResult(
                call_id=request.call_id,
                data=next(self.actions),
                usage=m.Usage.local(),
            )

    candidate = m.Candidate(source_id="demo-annual", score=1, match_reason="fixture")
    invalid_step = build_step(candidate, DemoData().tool_specs(), "Question", 0)
    invalid_step.args["unexpected"] = "invalid schema field"
    valid_step = build_step(candidate, DemoData().tool_specs(), "Question", 0)
    trace = CapturingTrace()
    task = EvaluationTask("repaired-tool-call", "Question")
    result = await run(
        m.RunRequest(
            task_id=task.task_id,
            question=task.question,
            mode="no_verifier",
            config=m.RunConfig(candidate_top_k=1),
        ),
        data=DemoData(),
        content=MeasuredContent([]),
        planner=RepairingPlanner(
            [
                m.AgentToolsDecision(steps=[invalid_step]),
                m.AgentToolsDecision(steps=[valid_step]),
                m.AgentComposeDecision(),
                m.AgentFinishDecision(),
            ]
        ),
        trace=trace,
    )

    sample = score_sample(
        task,
        "no_verifier",
        0,
        result,
        trace.events,
        trace.artifacts,
        unnecessary=lambda request: False,
    )
    assert result.status == "completed"
    assert sample.valid_calls == 1
    assert sample.tool_attempts == 2
    assert sample.unnecessary_calls is None


async def test_accepted_empty_evidence_remains_a_successful_tool_call():
    class EmptyEvidenceData(DemoData):
        async def invoke(self, request):
            return m.ToolResult(call_id=request.call_id, data={"evidence": []})

    trace = CapturingTrace()
    task = EvaluationTask(
        "empty-evidence", "Question", relevant_source_ids=frozenset({"demo-annual"})
    )
    result = await run(
        m.RunRequest(
            task_id=task.task_id,
            question=task.question,
            mode="no_verifier",
            config=m.RunConfig(candidate_top_k=1),
        ),
        data=EmptyEvidenceData(),
        content=MeasuredContent([]),
        planner=FixedRetrievalPlanner(build_step),
        trace=trace,
    )
    sample = score_sample(task, "fixed_retrieval", 0, result, trace.events, trace.artifacts)
    assert result.status == "completed"
    assert sample.valid_calls == sample.tool_attempts == 1
    assert sample.retrieved_sources == ["demo-annual"]


async def test_catalog_retries_are_included_in_retry_recovery_metric(tmp_path):
    class RetriedCatalogData(DemoData):
        async def catalog(self, request):
            if request.attempt == 1:
                return m.CatalogResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(code="busy", message="retry", retryable=True),
                )
            return await super().catalog(request)

    outcome, *_ = await experiment(tmp_path, data_factory=RetriedCatalogData)
    for row in outcome.report.results:
        assert row.metrics.retry_recovery_rate == 1
    assert all(
        sample["retried_operations"] == sample["recovered_operations"] == 1
        for sample in outcome.details["samples"]
    )


async def test_snapshot_drift_is_rejected_before_export(tmp_path):
    generation = 0

    snapshots = iter(("expected", "mismatched", "expected"))

    class ChangingData(DemoData):
        async def catalog(self, request):
            result = await super().catalog(request)
            result.catalog_snapshot_id = self.snapshot
            return result

    def data_factory():
        nonlocal generation
        generation += 1
        data = ChangingData()
        data.snapshot = next(snapshots)
        return data

    with pytest.raises(ValueError, match="catalog snapshot mismatch"):
        await experiment(
            tmp_path,
            data_factory=data_factory,
            config=m.RunConfig(
                model_id="frozen-model",
                max_model_calls=12,
                candidate_top_k=2,
                dataset_snapshot="expected",
            ),
        )


async def test_invalid_report_metadata_is_rejected_before_opening_ports():
    @asynccontextmanager
    async def forbidden():
        pytest.fail("invalid experiment metadata reached adapter creation")
        yield

    with pytest.raises(ValueError):
        await run_ablation(
            [EvaluationTask("task", "question")],
            ports_factory=forbidden,
            config=m.RunConfig(),
            step_builder=build_step,
            dataset_id="",
            task_set_id="frozen",
        )


def test_manifest_strict_json_types_and_unknown_fields(tmp_path):
    path = tmp_path / "tasks.json"
    for raw in (
        '[{"task_id":"t","task_id":"u","question":"q"}]',
        '[{"task_id":"t","question":"q","numeric_answer":NaN}]',
        '[{"task_id":"t","question":"q","expected_answers":"answer"}]',
        '[{"task_id":"t","question":"q","unexpected":1}]',
    ):
        path.write_text(raw)
        with pytest.raises((ValueError, TypeError)):
            cli._tasks(path)
    path.write_text(
        json.dumps(
            [
                {
                    "task_id": "t",
                    "question": "q",
                    "expected_answers": ["a"],
                    "relevant_source_ids": ["f"],
                }
            ]
        )
    )
    task = cli._tasks(path)[0]
    assert task.expected_answers == ("a",) and task.relevant_source_ids == frozenset({"f"})


def test_manifest_read_is_bounded(tmp_path):
    path = tmp_path / "large.json"
    path.write_bytes(b" " * 2_000_001)
    with pytest.raises(ValueError, match="2 MB"):
        cli._tasks(path)


def test_cli_adapter_error_is_redacted_and_existing_directory_is_preserved(
    tmp_path, monkeypatch, capsys
):
    @asynccontextmanager
    async def ports():
        raise RuntimeError("private-adapter-secret")
        yield

    monkeypatch.setattr(cli, "load_config", lambda path: object())
    monkeypatch.setattr(
        cli,
        "configured_profile",
        lambda config: SimpleNamespace(ports_factory=ports, run_config=m.RunConfig()),
    )
    manifest = tmp_path / "tasks.json"
    manifest.write_text('[{"task_id":"t","question":"q"}]')
    output = tmp_path / "output"
    args = [
        "--config",
        "unused.toml",
        "--manifest",
        str(manifest),
        "--step-builder",
        "tests.support.ablation_cases:build_step",
        "--dataset-id",
        "data",
        "--task-set-id",
        "tasks",
        "--output-dir",
        str(output),
    ]
    with pytest.raises(SystemExit) as failure:
        cli.main(args)
    assert failure.value.code == 2
    assert "private-adapter-secret" not in capsys.readouterr().err
    marker = output / "keep.txt"
    marker.write_text("keep")
    with pytest.raises(SystemExit):
        cli.main(args)
    assert marker.read_text() == "keep" and not (output / "report.json").exists()


def test_help_does_not_load_configuration(monkeypatch):
    monkeypatch.setattr(cli, "load_config", lambda _: pytest.fail("help touched config"))
    with pytest.raises(SystemExit) as result:
        cli.main(["--help"])
    assert result.value.code == 0


async def test_ablation_resume_skips_checkpointed_samples(tmp_path):
    tasks = [
        EvaluationTask("task-a", "Question A", expected_answers=("120",)),
        EvaluationTask("task-b", "Question B", expected_answers=("120",)),
    ]
    checkpoint = []

    def interrupt_after_first(sample):
        checkpoint.append(sample.export())
        raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        await experiment(
            tmp_path / "first",
            tasks=tasks,
            repeats=1,
            on_sample=interrupt_after_first,
        )

    resumed, _, _, data_requests, closed = await experiment(
        tmp_path / "resumed",
        tasks=tasks,
        repeats=1,
        resume_samples=[EvaluationSample.from_export(value) for value in checkpoint],
    )
    assert len(resumed.details["samples"]) == 6
    assert len(closed) == 5
    assert len(data_requests) == 6  # fixed_retrieval 的剩余样本各读取两个候选来源


def test_lakeqa_manifest_aliases_and_hop_fields_are_normalized(tmp_path):
    path = tmp_path / "task_1.json"
    path.write_text(
        json.dumps(
            [
                {
                    "question_id": "q-1",
                    "question": "Question?",
                    "answer": "Answer",
                    "datasets_used": ["source-a"],
                    "reasoning_hops": [{"hop": 1}, {"hop_id": 2}],
                }
            ]
        )
    )
    task = cli._tasks(path)[0]
    assert task.task_id == "q-1"
    assert task.expected_answers == ("Answer",)
    assert task.relevant_source_ids == frozenset({"source-a"})
    assert task.hop_count == 2


def test_lakeqa_manifest_rejects_conflicting_hop_aliases(tmp_path):
    path = tmp_path / "task_1.json"
    path.write_text(
        json.dumps(
            [
                {
                    "question_id": "q-1",
                    "question": "Question?",
                    "answer": "Answer",
                    "reasoning_hops": [{"hop": 1, "hop_id": 2}],
                }
            ]
        )
    )
    with pytest.raises(ValueError, match="hop"):
        cli._tasks(path)
    path.write_text(
        json.dumps(
            [
                {
                    "question_id": "q-2",
                    "question": "Question?",
                    "answer": "Answer",
                    "hop": 1,
                    "hop_id": 2,
                    "reasoning_hops": [{}],
                }
            ]
        )
    )
    with pytest.raises(ValueError, match="hop"):
        cli._tasks(path)


def test_manifest_allows_null_expected_answers_with_answer_alias(tmp_path):
    path = tmp_path / "task_null_alias.json"
    path.write_text(
        json.dumps(
            [
                {
                    "task_id": "t-1",
                    "question": "Question?",
                    "expected_answers": None,
                    "answer": "a",
                }
            ]
        )
    )
    task = cli._tasks(path)[0]
    assert task.expected_answers == ("a",)


def test_accuracy_accepts_scalar_structured_result_with_explanatory_text():
    from tests.support.ablation_metrics import EvaluationTask, score_sample

    def make_result(text: str, structured_result):
        return m.RunResult(
            run_id="r-acc",
            task_id="t-acc",
            mode="full_agent",
            status="completed",
            stop_reason="agent_finished",
            answer=m.AnswerRecord(text=text, structured_result=structured_result),
            started_at="2026-11-21T00:00:00Z",
            ended_at="2026-11-21T00:00:01Z",
            duration_ms=100,
            versions=m.Versions(
                model_id="test-model",
                model_params={},
                prompt_versions=m.PromptVersions(),
                config_version="dev-v1",
            ),
        )

    assert (
        score_sample(
            EvaluationTask(task_id="t-str", question="Which city?", expected_answers=("Brownsville",)),
            "full_agent",
            0,
            make_result("Based on the dataset, the city is Brownsville.", "  brownsville "),
            [],
            {},
        ).accuracy
        == 1.0
    )

    assert (
        score_sample(
            EvaluationTask(task_id="t-num", question="How many?", expected_answers=("113",)),
            "full_agent",
            0,
            make_result("The final count is 113.", 113),
            [],
            {},
        ).accuracy
        == 1.0
    )

    assert (
        score_sample(
            EvaluationTask(task_id="t-bool", question="Flag?", expected_answers=("True",)),
            "full_agent",
            0,
            make_result("The flag is enabled.", True),
            [],
            {},
        ).accuracy
        == 0.0
    )
