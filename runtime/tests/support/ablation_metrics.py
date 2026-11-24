"""Proposal 评估侧的标注模型和纯统计函数，不向执行端口泄露答案或相关文件。

文件级检索以成功工具实际访问的来源集合计算，另保留搜索候选集合供检查。
有效调用是被控制器接纳的 ok ToolResult 尝试；不必要调用必须由独立标注判断，
不能将无证据的 inspect、失败尝试或重复参数一概视为无用。未知值传播为 None。
"""

from __future__ import annotations

import json
import math
import re
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from decimal import MAX_EMAX, MIN_EMIN, ROUND_UP, Context, Decimal, InvalidOperation
from statistics import fmean, pstdev
from typing import Annotated, Literal

from pydantic import Field

from lake_agent import models as m
from lake_agent.validation import canonical_json

_NUMBER = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
_DATA_CALLS_STARTED = {"catalog.call.started", "tool.call.started"}
_DATA_CALLS_FAILED = {"catalog.call.failed", "tool.call.failed"}
_QUALITY_METRICS = (
    "accuracy",
    "numerical_accuracy",
    "structured_accuracy",
    "retrieval_precision",
    "retrieval_recall",
    "retrieval_f1",
    "grounding",
)


def _decimal(value: int | float | str | Decimal) -> Decimal:
    """接受整数、有限小数及清单中的精确数字字符串，拒绝布尔与隐式转换。"""
    if type(value) not in (int, float, str, Decimal) or (
        type(value) is str and not _NUMBER.fullmatch(value)
    ):
        raise ValueError("annotation must be a finite number")
    try:
        number = Decimal(str(value) if type(value) is float else value)
    except InvalidOperation:
        raise ValueError("annotation must be a finite number") from None
    if not number.is_finite():
        raise ValueError("annotation must be a finite number")
    return number


def _manifest_number(value):
    """小数可无损还原时保留 JSON 数字，否则用精确字符串留存标注。"""
    if type(value) is not Decimal:
        return value
    floating = float(value)
    return floating if math.isfinite(floating) and Decimal(str(floating)) == value else str(value)


@dataclass(frozen=True)
class EvaluationTask:
    """只有 question/task_id 进入执行请求，其余字段仅在运行结束后评分。"""

    task_id: str
    question: str
    expected_answers: tuple[str, ...] | None = None
    relevant_source_ids: frozenset[str] | None = None
    numeric_answer: int | float | str | Decimal | None = None
    numeric_tolerance: int | float | str | Decimal = 0.0
    structured_answer_json: str | None = None
    not_applicable_metrics: frozenset[str] = frozenset()
    hop_count: int | None = None

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
        if self.hop_count is not None and (type(self.hop_count) is not int or self.hop_count < 1):
            raise ValueError("hop_count must be a positive integer or null")
        if _decimal(self.numeric_tolerance) < 0:
            raise ValueError("numeric_tolerance must be finite and nonnegative")
        if self.numeric_answer is not None:
            _decimal(self.numeric_answer)
        if self.structured_answer_json is not None:
            if type(self.structured_answer_json) is not str:
                raise ValueError("structured_answer_json must be a string")
            _canonical_exact_json(_json(self.structured_answer_json, exact_decimals=True))
        if type(
            self.not_applicable_metrics
        ) is not frozenset or not self.not_applicable_metrics <= set(_QUALITY_METRICS):
            raise ValueError("not_applicable_metrics must be a set of score metric names")
        if (
            "accuracy" in self.not_applicable_metrics
            and self.expected_answers is not None
            or "numerical_accuracy" in self.not_applicable_metrics
            and self.numeric_answer is not None
            or "structured_accuracy" in self.not_applicable_metrics
            and self.structured_answer_json is not None
            or self.not_applicable_metrics
            & {"retrieval_precision", "retrieval_recall", "retrieval_f1", "grounding"}
            and self.relevant_source_ids is not None
        ):
            raise ValueError("a metric cannot be both applicable and not applicable")
        canonical_json(self.manifest())

    def manifest(self) -> dict:
        """导出独立的、可由原清单入口重新读入的标注对象。"""
        return self.__dict__ | {
            "expected_answers": list(self.expected_answers)
            if self.expected_answers is not None
            else None,
            "relevant_source_ids": sorted(self.relevant_source_ids)
            if self.relevant_source_ids is not None
            else None,
            "numeric_answer": _manifest_number(self.numeric_answer),
            "numeric_tolerance": _manifest_number(self.numeric_tolerance),
            "not_applicable_metrics": sorted(self.not_applicable_metrics),
            "hop_count": self.hop_count,
        }


class DataAttemptUsage(m.Model):
    """可信宿主上报的单次目录或工具访问测量；每一项可独立未知。"""

    bytes_transferred: m.NonNegativeInt | None = None
    cost_usd: Annotated[float, Field(ge=0)] | None = None


DataUsage = Mapping[str, DataAttemptUsage | Mapping[str, m.JsonValue]]


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
    external_data_bytes_total: int | None = None
    external_data_cost_usd: float | None = None
    data_attempt_usage: dict[str, dict] | None = None
    metric_applicability: dict[str, bool | None] = field(default_factory=dict)
    evidence_sources: list[str] = field(default_factory=list)
    searched_sources: list[str] = field(default_factory=list)
    visited_sources: list[str] = field(default_factory=list)
    failure_categories: list[str] = field(default_factory=list)
    verification_verdict: str | None = None

    @classmethod
    def from_export(cls, value: Mapping) -> EvaluationSample:
        """从可信的本地 checkpoint 还原一条已完成样本。"""
        if not isinstance(value, Mapping) or not isinstance(value.get("result"), Mapping):
            raise ValueError("checkpoint sample must contain a result object")
        result = m.RunResult.model_validate(value["result"])
        funnel = (
            value.get("source_funnel") if isinstance(value.get("source_funnel"), Mapping) else {}
        )
        fields = {
            "task_id": value.get("task_id"),
            "mode": value.get("mode"),
            "repeat": value.get("repeat"),
            "result": result,
            "accuracy": value.get("accuracy"),
            "numerical_accuracy": value.get("numerical_accuracy"),
            "structured_accuracy": value.get("structured_accuracy"),
            "retrieval_precision": value.get("retrieval_precision"),
            "retrieval_recall": value.get("retrieval_recall"),
            "retrieval_f1": value.get("retrieval_f1"),
            "grounding": value.get("grounding"),
            "retrieved_sources": value.get("retrieved_sources", []),
            "candidate_sources": value.get("candidate_sources", funnel.get("searched_sources", [])),
            "cited_sources": value.get("cited_sources", funnel.get("cited_sources", [])),
            "valid_calls": value.get("valid_calls", 0),
            "tool_attempts": value.get("tool_attempts", 0),
            "unnecessary_calls": value.get("unnecessary_calls"),
            "retried_operations": value.get("retried_operations", 0),
            "recovered_operations": value.get("recovered_operations", 0),
            "external_data_bytes_total": value.get("external_data_bytes_total"),
            "external_data_cost_usd": value.get("external_data_cost_usd"),
            "data_attempt_usage": value.get("data_attempt_usage"),
            "metric_applicability": value.get("metric_applicability", {}),
            "evidence_sources": value.get("evidence_sources", funnel.get("evidence_sources", [])),
            "searched_sources": value.get("searched_sources", funnel.get("searched_sources", [])),
            "visited_sources": value.get("visited_sources", funnel.get("visited_sources", [])),
            "failure_categories": value.get("failure_categories", []),
            "verification_verdict": value.get("verification_verdict"),
        }
        if (
            not isinstance(fields["task_id"], str)
            or not isinstance(fields["mode"], str)
            or type(fields["repeat"]) is not int
        ):
            raise ValueError("checkpoint sample identity is invalid")
        return cls(**fields)

    def export(self) -> dict:
        value = asdict(self)
        value["result"] = self.result.model_dump(mode="json")
        value["source_funnel"] = {
            "searched_sources": self.searched_sources,
            "visited_sources": self.visited_sources,
            "evidence_sources": self.evidence_sources,
            "cited_sources": self.cited_sources,
        }
        return value


def _normalized(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def _json(text: str | bytes, *, exact_decimals=False):
    """仅清单保留数字词法；结构化答案维持原本的有限 JSON 类型规则。"""

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
        value = json.loads(
            text,
            parse_float=Decimal if exact_decimals else float,
            parse_constant=reject,
            object_pairs_hook=unique,
        )
        # parse_constant 不处理 1e999 这类溢出的合法数值 token；统一 JSON 边界还会
        # 检查全部嵌套值。精确清单的各字段随后由 EvaluationTask 校验。
        if not exact_decimals:
            canonical_json(value)
    except InvalidOperation:
        raise ValueError("JSON decimal exceeds evaluator capacity") from None
    except RecursionError:
        raise ValueError("JSON nesting exceeds evaluator capacity") from None
    return value


def _canonical_decimal(value: Decimal) -> str:
    """按数值语义规范化有限 Decimal，使 1.0/1.00/1e2/100.0 等价且保留完整精度。"""
    if not value.is_finite() or not math.isfinite(float(value)):
        raise ValueError("JSON number must be finite")
    if value == 0:
        return "0.0"
    normalized = value.normalize()
    sign, digits, exponent = normalized.as_tuple()
    prefix = "-" if sign else ""
    return f"{prefix}{''.join(str(digit) for digit in digits)}e{exponent}"


def _canonical_exact_json(value) -> str:
    """比较解析后的 JSON 时保留 Decimal 词法精度，不经 float 或通用 JSON 转换。"""
    if value is None:
        return "null"
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) is str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if type(value) is int:
        return str(value)
    if type(value) is Decimal:
        return _canonical_decimal(value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON number must be finite")
        return _canonical_decimal(Decimal(str(value)))
    if type(value) is list:
        return "[" + ",".join(_canonical_exact_json(item) for item in value) + "]"
    if type(value) is dict and all(type(key) is str for key in value):
        return (
            "{"
            + ",".join(
                _canonical_exact_json(key) + ":" + _canonical_exact_json(value[key])
                for key in sorted(value)
            )
            + "}"
        )
    raise ValueError("value is not JSON")


def _numeric_match(numeric: str, task: EvaluationTask) -> bool:
    """比较精确的十进制绝对误差，容差边界不向下舍入。"""
    if not _NUMBER.fullmatch(numeric):
        return False
    try:
        actual = _decimal(numeric)
    except ValueError:
        return False
    expected, tolerance = _decimal(task.numeric_answer), _decimal(task.numeric_tolerance)
    if not tolerance:
        return actual == expected
    # 保证容差本身可精确表示。将差值向远离零的方向舍入，既不会把超差
    # 误判为通过，也不用按指数跨度展开巨量的零。
    context = Context(
        prec=max(28, len(tolerance.as_tuple().digits)),
        rounding=ROUND_UP,
        Emin=MIN_EMIN,
        Emax=MAX_EMAX,
        traps=[],
    )
    return context.subtract(actual, expected).copy_abs() <= tolerance


def _sum_known(values):
    """任一必要测量缺失时保留空值，避免把未测量当作零。"""
    values = list(values)
    return sum(values) if all(value is not None for value in values) else None


def _measured_data_usage(data_usage: DataUsage | None, attempted: set[str]):
    """只接收对应本次运行的物理尝试；多个 attempt 不按 operation_id 合并。"""
    if data_usage is None:
        return None, None, None
    if not isinstance(data_usage, Mapping) or data_usage.keys() - attempted:
        raise ValueError("data usage must reference only attempted catalog/tool call IDs")
    recorded = {}
    for call_id, value in data_usage.items():
        if isinstance(value, DataAttemptUsage):
            value = value.model_dump()
        elif isinstance(value, Mapping):
            value = dict(value)
        recorded[call_id] = DataAttemptUsage.model_validate(value).model_dump()
    rows = [recorded.get(call_id, {}) for call_id in sorted(attempted)]
    return (
        _sum_known(row.get("bytes_transferred") for row in rows),
        _sum_known(row.get("cost_usd") for row in rows),
        recorded,
    )


def score_sample(
    task: EvaluationTask,
    mode: str,
    repeat: int,
    result: m.RunResult,
    events: Sequence[m.TraceEvent],
    artifacts: dict[str, m.JsonValue],
    *,
    unnecessary: Callable[[dict], bool | None] | None = None,
    data_usage: DataUsage | None = None,
) -> EvaluationSample:
    """只在执行完成后读取 gold；可选判定器接收完整 ToolRequest，不影响决策。"""
    text = result.answer.text if result.answer else None
    structured_value = result.answer.structured_result if result.answer else None
    accuracy = None
    if task.expected_answers is not None:
        expected_normalized = {_normalized(answer) for answer in task.expected_answers}
        structured_match = (
            type(structured_value) in (str, int, float)
            and _normalized(str(structured_value)) in expected_normalized
        )
        text_match = text is not None and _normalized(text) in expected_normalized
        accuracy = float(structured_match or text_match)
    numerical = None
    if task.numeric_answer is not None:
        # 仅接受答案本身为一个数，避免从解释性文本中捞到偶然匹配的数字。
        numeric_text = (text or "").strip()
        if type(structured_value) in (int, float):
            numeric_text = str(structured_value)
        elif type(structured_value) is str and _NUMBER.fullmatch(structured_value.strip()):
            numeric_text = structured_value.strip()
        numerical = float(_numeric_match(numeric_text, task))
    structured = None
    if task.structured_answer_json is not None:
        try:
            actual = result.answer.structured_result if result.answer else None
            if actual is None:
                actual = _json(text, exact_decimals=True) if text is not None else None
            structured = float(
                result.answer is not None
                and _canonical_exact_json(actual)
                == _canonical_exact_json(_json(task.structured_answer_json, exact_decimals=True))
            )
        except (ValueError, TypeError):
            structured = 0.0

    requests, data_requests, responses, evidence, steps = {}, {}, {}, {}, {}
    candidates, accessed, successful_steps = set(), set(), set()
    rejected_tool_intents = 0
    data_intents, not_dispatched = set(), set()
    for event in events:
        if event.detail.get("replayed"):
            continue
        if event.call_id is not None:
            if event.event_type in _DATA_CALLS_STARTED:
                data_intents.add(event.call_id)
            elif (
                event.event_type in _DATA_CALLS_FAILED
                and event.detail.get("outcome") == "not_dispatched"
            ):
                not_dispatched.add(event.call_id)
        if event.event_type in {"run.stopped", "run.cancelled"}:
            # 批次准备中止时，之前已提交的意图可能只有终态快照，没有逐条失败事件。
            for key in ("pending_calls", "active_calls"):
                calls = event.detail.get(key)
                if isinstance(calls, list):
                    not_dispatched.update(
                        call["call_id"]
                        for call in calls
                        if isinstance(call, dict)
                        and isinstance(call.get("call_id"), str)
                        and (key == "pending_calls" or call.get("dispatched") is False)
                    )
        if event.event_type == "step.state" and event.status == "succeeded":
            successful_steps.add(event.step_id)
        if event.event_type == "agent.action.rejected" and event.detail.get("kind") == "tools":
            count = event.detail.get("count", 1)
            if type(count) is int and count > 0:
                rejected_tool_intents += count
        payload = artifacts.get(event.data_ref) if event.data_ref else None
        if not isinstance(payload, dict):
            continue
        if event.event_type in _DATA_CALLS_STARTED:
            data_requests[event.call_id] = payload
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
    accepted_responses = {
        call_id: payload
        for call_id, payload in responses.items()
        if requests[call_id]["step_id"] in successful_steps
    }
    for call_id, payload in accepted_responses.items():
        # 只计控制器接纳为成功步骤的合法 ToolResult。
        m.ToolResult.model_validate(payload)
        request = requests[call_id]
        accessed.update(steps[request["step_id"]]["source_ids"])
    visited = {
        source_id
        for call_id, request in requests.items()
        if call_id not in not_dispatched
        for source_id in steps.get(request.get("step_id"), {}).get("source_ids", [])
    }
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
        labels.extend([None] * rejected_tool_intents)
        if any(label is not None and type(label) is not bool for label in labels):
            raise ValueError("unnecessary-call labels must be bool or None")
        if all(label is not None for label in labels):
            unnecessary_count = sum(labels)
    operation_attempts = {}
    operation_recovered = set()
    for event in events:
        if event.detail.get("replayed") or event.call_id is None:
            continue
        if event.event_type not in (
            _DATA_CALLS_STARTED | {"catalog.call.completed", "tool.call.completed"}
        ):
            continue
        payload = artifacts.get(event.data_ref) if event.data_ref else None
        if not isinstance(payload, dict):
            continue
        if event.event_type in _DATA_CALLS_STARTED:
            request_payload = payload
        elif event.event_type == "catalog.call.completed":
            request_payload = data_requests.get(event.call_id)
        else:
            request_payload = requests.get(event.call_id)
        if not isinstance(request_payload, dict):
            continue
        operation_id = request_payload.get("operation_id")
        attempt_number = request_payload.get("attempt")
        if not isinstance(operation_id, str) or type(attempt_number) is not int:
            continue
        operation_attempts.setdefault(operation_id, set()).add(attempt_number)
        if event.event_type.endswith(".completed") and event.status == "ok":
            if (
                event.event_type == "catalog.call.completed"
                or request_payload.get("step_id") in successful_steps
            ):
                operation_recovered.add(operation_id)
    retried = {operation for operation, attempts in operation_attempts.items() if max(attempts) > 1}
    recovered = retried & operation_recovered
    bytes_total, data_cost, measured_attempts = _measured_data_usage(
        data_usage, data_intents - not_dispatched
    )
    verification_verdicts = [
        event.status
        for event in events
        if event.event_type == "verification" and event.status is not None
    ]
    final_verdict = verification_verdicts[-1] if verification_verdicts else None
    failure_code = result.failure.error.code if result.failure is not None else None
    failure_categories = []
    if result.status != "completed":
        if task.relevant_source_ids and not (set(task.relevant_source_ids) & candidates):
            failure_categories.append("missed_sources")
        if result.failure is not None and (
            result.failure.phase == "tool"
            or failure_code in {"tool_error", "retry_exhausted", "invalid_response"}
        ):
            failure_categories.append("tool_errors")
        if final_verdict == "conflicting" or failure_code == "agent_verification_failed":
            failure_categories.append("evidence_conflicts")
        if final_verdict in {"unsupported", "conflicting", "insufficient"} or (
            result.stop_reason in {"insufficient_evidence", "agent_stopped"}
        ):
            failure_categories.append("unsupported_answers")
        if not failure_categories:
            failure_categories.append("other")
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
        len(accepted_responses),
        len(requests) + rejected_tool_intents,
        unnecessary_count,
        len(retried),
        len(recovered),
        bytes_total,
        data_cost,
        measured_attempts,
        metric_applicability={
            "accuracy": False
            if "accuracy" in task.not_applicable_metrics
            else (True if task.expected_answers is not None else None),
            "numerical_accuracy": False
            if "numerical_accuracy" in task.not_applicable_metrics
            else (True if task.numeric_answer is not None else None),
            "structured_accuracy": False
            if "structured_accuracy" in task.not_applicable_metrics
            else (True if task.structured_answer_json is not None else None),
            **{
                name: False
                if name in task.not_applicable_metrics
                else (True if task.relevant_source_ids is not None else None)
                for name in ("retrieval_precision", "retrieval_recall", "retrieval_f1", "grounding")
            },
        },
        evidence_sources=sorted(set(evidence.values())),
        searched_sources=sorted(candidates),
        visited_sources=sorted(visited),
        failure_categories=failure_categories,
        verification_verdict=final_verdict,
    )


def failure_category_summary(samples: Sequence[EvaluationSample]) -> dict[str, int]:
    """按样本统计失败主因；一个样本的多个独立信号可分别计数。"""
    names = ("missed_sources", "tool_errors", "evidence_conflicts", "unsupported_answers", "other")
    return {name: sum(name in sample.failure_categories for sample in samples) for name in names}


def failure_diagnostic_summary(samples: Sequence[EvaluationSample]) -> dict[str, dict[str, int]]:
    """保留 stop reason、核验 verdict 和最近 failure.code 的原始分类。"""

    def counts(values):
        result = {}
        for value in values:
            if value is not None:
                result[value] = result.get(value, 0) + 1
        return dict(sorted(result.items()))

    return {
        "categories": failure_category_summary(samples),
        "stop_reasons": counts(sample.result.stop_reason for sample in samples),
        "verification_verdicts": counts(sample.verification_verdict for sample in samples),
        "failure_codes": counts(
            sample.result.failure.error.code if sample.result.failure is not None else None
            for sample in samples
        ),
    }


def metric_coverage(samples: Sequence[EvaluationSample]) -> dict[str, dict[str, int]]:
    """为每个质量指标统计已标注、不适用与未知任务数量。"""
    return {
        name: {
            "applicable_count": sum(
                sample.metric_applicability.get(name) is True for sample in samples
            ),
            "not_applicable_count": sum(
                sample.metric_applicability.get(name) is False for sample in samples
            ),
            "unknown_count": sum(
                sample.metric_applicability.get(name) is None for sample in samples
            ),
        }
        for name in _QUALITY_METRICS
    }


def aggregate(samples: Sequence[EvaluationSample]) -> tuple[dict, dict]:
    """质量指标取任务宏平均；调用比率用尝试总数作分母，缺测不选择性忽略。"""
    if not samples:
        raise ValueError("cannot aggregate an empty task set")

    def mean_known(values):
        return fmean(values) if all(value is not None for value in values) else None

    metrics = {
        key: mean_known(
            [
                getattr(sample, key)
                for sample in samples
                if sample.metric_applicability.get(key) is True
            ]
        )
        if all(sample.metric_applicability.get(key) is not None for sample in samples)
        and any(sample.metric_applicability.get(key) is True for sample in samples)
        else None
        for key in _QUALITY_METRICS
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
        external_data_bytes_total=_sum_known(s.external_data_bytes_total for s in samples),
        external_data_cost_usd=_sum_known(s.external_data_cost_usd for s in samples),
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
