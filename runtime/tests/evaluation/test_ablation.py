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

from lake_agent import models as m
from lake_agent.adapters.planner import JsonPlanner
from lake_agent.evaluation.models import EvaluationImport
from lake_agent.trace import JsonlTraceSink
from lake_agent.web.evaluation_storage import EvaluationRepository
from lake_agent.web.profiles import ExecutionPorts
from tests import ablation_runtime as cli
from tests.support.ablation import FixedRetrievalPlanner, run_ablation
from tests.support.ablation_cases import build_step
from tests.support.ablation_metrics import EvaluationTask, aggregate
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
        step_builder=build_step,
        config=m.RunConfig(model_id="frozen-model", max_model_calls=12, candidate_top_k=2),
        dataset_id="synthetic-pool",
        task_set_id="frozen-task-manifest",
        repeats=repeats,
        trace_factory=lambda: JsonlTraceSink(tmp_path / "audit"),
        unnecessary=unnecessary,
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


async def test_fixed_baseline_keeps_one_shot_answer_without_any_candidates(tmp_path):
    class EmptyContent(MeasuredContent):
        async def search(self, request):
            return _reply(request, m.SearchResponse(candidates=[]))

    # 测试固定策略本身，避免自主测试规划器持续搜索空结果。
    from lake_agent.runtime import run
    from tests.support.ablation import CapturingTrace

    result = await run(
        m.RunRequest(task_id="empty", question="Unknown", mode="no_verifier"),
        data=DemoData(),
        content=EmptyContent([]),
        planner=FixedRetrievalPlanner(build_step),
        trace=CapturingTrace(),
    )
    assert result.status == "completed" and result.answer.citations == []
    assert result.call_counts.tools == 0 and result.call_counts.model_calls == 1


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


async def test_snapshot_drift_is_rejected_before_export(tmp_path):
    generation = 0

    class ChangingData(DemoData):
        async def catalog(self, request):
            result = await super().catalog(request)
            result.catalog_snapshot_id = self.snapshot
            return result

    def data_factory():
        nonlocal generation
        generation += 1
        data = ChangingData()
        data.snapshot = f"version-{generation}"
        return data

    with pytest.raises(ValueError, match="snapshot changed"):
        await experiment(tmp_path, data_factory=data_factory)


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
