"""Proposal 评估侧的标注模型和纯统计函数，不向执行端口泄露答案或相关文件。

文件级检索以成功工具实际访问的来源集合计算，另保留搜索候选集合供检查。
有效调用是返回合法 ok ToolResult 的物理尝试；不必要调用必须由独立标注判断，
不能将无证据的 inspect、失败尝试或重复参数一概视为无用。未知值传播为 None。
"""

from __future__ import annotations

import json
import math
import re
import unicodedata
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from statistics import fmean, pstdev
from typing import Literal

from lake_agent import models as m
from lake_agent.validation import canonical_json


@dataclass(frozen=True)
class EvaluationTask:
    """只有 question/task_id 进入执行请求，其余字段仅在运行结束后评分。"""

    task_id: str
    question: str
    expected_answers: tuple[str, ...] | None = None
    relevant_source_ids: frozenset[str] | None = None
    numeric_answer: float | None = None
    numeric_tolerance: float = 0.0
    structured_answer_json: str | None = None

    def __post_init__(self):
        if any(
            type(value) is not str or not value.strip() for value in (self.task_id, self.question)
        ):
            raise ValueError("task_id and question must be nonempty")
        if self.expected_answers is not None and (
            type(self.expected_answers) is not tuple
            or not self.expected_answers
            or any(type(answer) is not str for answer in self.expected_answers)
        ):
            raise ValueError("expected_answers must be a nonempty immutable tuple of strings")
        if self.relevant_source_ids is not None and (
            type(self.relevant_source_ids) is not frozenset
            or any(type(source) is not str or not source for source in self.relevant_source_ids)
        ):
            raise ValueError("relevant_source_ids must be an immutable set of source IDs")
        if (
            type(self.numeric_tolerance) not in (int, float)
            or not math.isfinite(self.numeric_tolerance)
            or self.numeric_tolerance < 0
        ):
            raise ValueError("numeric_tolerance must be finite and nonnegative")
        if self.numeric_answer is not None and (
            type(self.numeric_answer) not in (int, float) or not math.isfinite(self.numeric_answer)
        ):
            raise ValueError("numeric_answer must be finite")
        if self.structured_answer_json is not None:
            if type(self.structured_answer_json) is not str:
                raise ValueError("structured_answer_json must be a string")
            _json(self.structured_answer_json)
        canonical_json(
            {
                "task_id": self.task_id,
                "question": self.question,
                "expected_answers": list(self.expected_answers)
                if self.expected_answers is not None
                else None,
                "relevant_source_ids": sorted(self.relevant_source_ids)
                if self.relevant_source_ids is not None
                else None,
            }
        )


@dataclass
class EvaluationSample:
    """保留结果、集合与尝试级计数，避免只导出不可复核的百分比。"""

    task_id: str
    mode: Literal["fixed_retrieval", "no_verifier", "full_agent"]
    repeat: int
    result: m.RunResult
    accuracy: float | None
    numerical_accuracy: float | None
    structured_accuracy: float | None
    retrieval_precision: float | None
    retrieval_recall: float | None
    retrieval_f1: float | None
    grounding: float | None
    retrieved_sources: list[str] = field(default_factory=list)
    candidate_sources: list[str] = field(default_factory=list)
    cited_sources: list[str] = field(default_factory=list)
    valid_calls: int = 0
    tool_attempts: int = 0
    unnecessary_calls: int | None = None
    retried_operations: int = 0
    recovered_operations: int = 0

    def export(self) -> dict:
        from dataclasses import asdict

        value = asdict(self)
        value["result"] = self.result.model_dump(mode="json")
        return value


def _normalized(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def _json(text: str):
    def reject(value):
        raise ValueError(f"nonfinite JSON: {value}")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(text, parse_constant=reject, object_pairs_hook=unique)
        # parse_constant 不处理 1e999 这类溢出的合法数值 token；统一 JSON 边界还会
        # 检查全部嵌套值、Unicode 和非有限浮点数。
        canonical_json(value)
    except RecursionError:
        raise ValueError("JSON nesting exceeds evaluator capacity") from None
    return value


def score_sample(
    task: EvaluationTask,
    mode: str,
    repeat: int,
    result: m.RunResult,
    events: Sequence[m.TraceEvent],
    artifacts: dict[str, m.JsonValue],
    *,
    unnecessary: Callable[[dict], bool | None] | None = None,
) -> EvaluationSample:
    """只在执行完成后读取 gold；可选判定器接收完整 ToolRequest，不影响决策。"""
    text = result.answer.text if result.answer else None
    accuracy = None
    if task.expected_answers is not None:
        accuracy = float(
            text is not None
            and _normalized(text) in {_normalized(answer) for answer in task.expected_answers}
        )
    numerical = None
    if task.numeric_answer is not None:
        # 仅接受答案本身为一个数，避免从解释性文本中捞到偶然匹配的数字。
        numeric_text = (text or "").strip()
        structured_value = result.answer.structured_result if result.answer else None
        if type(structured_value) in (int, float):
            numeric_text = str(structured_value)
        matched = re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", numeric_text)
        numerical = float(
            bool(matched)
            and math.isclose(
                float(numeric_text), task.numeric_answer, rel_tol=0, abs_tol=task.numeric_tolerance
            )
        )
    structured = None
    if task.structured_answer_json is not None:
        try:
            actual = result.answer.structured_result if result.answer else None
            if actual is None:
                actual = _json(text) if text is not None else None
            structured = float(
                result.answer is not None
                and canonical_json(actual) == canonical_json(_json(task.structured_answer_json))
            )
        except (ValueError, TypeError):
            structured = 0.0

    requests, responses, evidence, steps = {}, {}, {}, {}
    candidates, accessed = set(), set()
    for event in events:
        if event.detail.get("replayed"):
            continue
        payload = artifacts.get(event.data_ref) if event.data_ref else None
        if not isinstance(payload, dict):
            continue
        if event.event_type == "tool.call.started":
            requests[event.call_id] = payload
        elif event.event_type == "plan.accepted":
            steps.update({step["step_id"]: step for step in payload["steps"]})
        elif event.event_type == "tool.call.completed":
            responses[event.call_id] = payload
        elif event.event_type == "content.search.call.completed":
            candidates.update(item["source_id"] for item in payload["data"]["candidates"])
        elif event.event_type == "evidence.added":
            evidence[payload["evidence_id"]] = payload["source_id"]
    for call_id, payload in responses.items():
        # 每次合法成功的工具尝试，无论是否产生 Evidence，都计为有效调用。
        m.ToolResult.model_validate(payload)
        request = requests[call_id]
        # 来源声明来自已接受的计划工件，不能从任意工具参数猜测字段路径。
        accessed.update(steps[request["step_id"]]["source_ids"])
    cited = [evidence[citation] for citation in (result.answer.citations if result.answer else [])]
    relevant = task.relevant_source_ids
    precision = recall = f1 = grounding = None
    if relevant is not None:
        hit = len(accessed & relevant)
        precision = hit / len(accessed) if accessed else 0.0
        recall = hit / len(relevant) if relevant else float(not accessed)
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        cited_files = set(cited)
        grounding = len(cited_files & relevant) / len(cited_files) if cited_files else None
    unnecessary_count = None
    if unnecessary is not None:
        labels = [unnecessary(request) for request in requests.values()]
        if any(label is not None and type(label) is not bool for label in labels):
            raise ValueError("unnecessary-call labels must be bool or None")
        if all(label is not None for label in labels):
            unnecessary_count = sum(labels)
    operations = {}
    for request in requests.values():
        operations.setdefault(request["operation_id"], []).append(request)
    retried = {key for key, group in operations.items() if any(r["attempt"] > 1 for r in group)}
    recovered = {
        request["operation_id"]
        for key, request in requests.items()
        if key in responses and request["attempt"] > 1
    }
    return EvaluationSample(
        task.task_id,
        mode,
        repeat,
        result,
        accuracy,
        numerical,
        structured,
        precision,
        recall,
        f1,
        grounding,
        sorted(accessed),
        sorted(candidates),
        sorted(set(cited)),
        len(responses),
        len(requests),
        unnecessary_count,
        len(retried),
        len(recovered & retried),
    )


def aggregate(samples: Sequence[EvaluationSample]) -> tuple[dict, dict]:
    """质量指标取任务宏平均；调用比率用尝试总数作分母，缺测不选择性忽略。"""
    if not samples:
        raise ValueError("cannot aggregate an empty task set")

    def mean_known(values):
        return fmean(values) if all(value is not None for value in values) else None

    metrics = {
        key: mean_known([getattr(sample, key) for sample in samples])
        for key in (
            "accuracy",
            "numerical_accuracy",
            "structured_accuracy",
            "retrieval_precision",
            "retrieval_recall",
            "retrieval_f1",
            "grounding",
        )
    }
    attempts = sum(sample.tool_attempts for sample in samples)
    retried = sum(sample.retried_operations for sample in samples)
    unnecessary_counts = [sample.unnecessary_calls for sample in samples]
    inputs = [sample.result.usage.input_tokens for sample in samples]
    outputs = [sample.result.usage.output_tokens for sample in samples]
    costs = [sample.result.usage.cost for sample in samples]
    metrics.update(
        completion_rate=fmean(sample.result.status == "completed" for sample in samples),
        valid_tool_call_rate=sum(s.valid_calls for s in samples) / attempts if attempts else None,
        unnecessary_tool_call_rate=(
            sum(unnecessary_counts) / attempts
            if attempts and all(value is not None for value in unnecessary_counts)
            else None
        ),
        retry_recovery_rate=sum(s.recovered_operations for s in samples) / retried
        if retried
        else None,
        tool_calls_mean=fmean(s.result.call_counts.tools for s in samples),
        data_calls_mean=fmean(
            s.result.call_counts.tools + s.result.call_counts.catalog for s in samples
        ),
        latency_ms_mean=fmean(s.result.duration_ms for s in samples),
        input_tokens_total=sum(inputs) if all(v is not None for v in inputs) else None,
        output_tokens_total=sum(outputs) if all(v is not None for v in outputs) else None,
        token_total=sum(inputs) + sum(outputs)
        if all(v is not None for v in inputs + outputs)
        else None,
        provider_requests_total=sum(s.result.usage.provider_requests for s in samples),
        # 现有 DataPort 没有可信传输字节/计费字段，不能从调用数或工具正文猜成本。
        external_data_bytes_total=None,
        external_data_cost_usd=None,
        cost_usd=sum(costs)
        if all(v is not None for v in costs)
        and all(s.result.usage.currency == "USD" for s in samples)
        else None,
    )
    variability = {}
    for key, values in {
        "latency_ms": [s.result.duration_ms for s in samples],
        "tool_calls": [s.result.call_counts.tools for s in samples],
        "accuracy": [s.accuracy for s in samples],
        "retrieval_f1": [s.retrieval_f1 for s in samples],
        "provider_requests": [s.result.usage.provider_requests for s in samples],
        "input_tokens": inputs,
        "output_tokens": outputs,
    }.items():
        known = all(value is not None for value in values)
        variability[key] = {
            "mean": fmean(values) if known else None,
            "population_stddev": pstdev(values) if known else None,
            "n": len(values),
            "known_n": sum(value is not None for value in values),
        }
    return metrics, variability
