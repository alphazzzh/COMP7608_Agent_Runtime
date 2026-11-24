"""只存在于测试/评估侧的 Proposal 三组对照运行器。

固定 Top-k 策略通过本地 agent-loop-v1 决策使用同一个生产控制器：一次搜索、
一次候选工具批次、一次回答，不重规划、不核验、不调用模型规划器。它不会增加
生产模式或修改图。所有组使用新端口与同一基础配置，标签只在运行结束后评分。
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import uuid4

from lake_agent import models as m
from lake_agent.evaluation.models import EvaluationImport
from lake_agent.ports import DataPort, TraceSink
from lake_agent.runtime import run
from lake_agent.validation import canonical_json
from lake_agent.web.profiles import ExecutionPorts

from .ablation_metrics import (
    DataUsage,
    EvaluationSample,
    EvaluationTask,
    aggregate,
    failure_category_summary,
    failure_diagnostic_summary,
    metric_coverage,
    score_sample,
)

MODES = ("fixed_retrieval", "no_verifier", "full_agent")
StepBuilder = Callable[[m.Candidate, list[m.ToolSpec], str, int], m.AgentToolStep]
DataUsageProvider = Callable[[DataPort, str], DataUsage | None]


class CapturingTrace:
    """复制同一 TraceSink 协议的工件和事件，可同时委托真实 JSONL sink。

    评估顺序执行且不使用恢复，因此只需 create_run 而不创建 SQLite 账本。
    生产 run/resume 与恢复机制不受影响。数据在关闭后用于评分。
    """

    def __init__(self, delegate: TraceSink | None = None):
        self.delegate = delegate
        self.events: list[m.TraceEvent] = []
        self.artifacts: dict[str, m.JsonValue] = {}
        self.run_id = None

    async def create_run(self, run_id):
        self.run_id = run_id
        if self.delegate:
            await self.delegate.create_run(run_id)

    async def append(self, event):
        if self.delegate:
            await self.delegate.append(event)
        self.events.append(event.model_copy(deep=True))

    async def put_artifact(self, ref, value):
        if self.delegate:
            await self.delegate.put_artifact(ref, value)
        self.artifacts[ref] = json.loads(canonical_json(value))

    async def close(self):
        return await self.delegate.close() if self.delegate else f"memory://{self.run_id}"


class FixedRetrievalPlanner:
    """每个样本新建的固定策略；生成被拒后结束，不能回到搜索或工具阶段。"""

    agent_protocol = m.AGENT_PROTOCOL

    def __init__(self, step_builder: StepBuilder):
        self.step_builder = step_builder
        self.stage = "search"

    async def decide(self, request):
        """最多一次搜索、一次逻辑工具批次和一次生成；批次内重试由控制器负责。"""
        context = request.context
        last = context.get("last_action") or {}
        if self.stage == "search":
            action = m.AgentSearchDecision(query=request.question)
            self.stage = "retrieval"
        elif self.stage == "retrieval":
            candidates = context.get("candidates")
            if (
                last.get("kind") != "search"
                or last.get("status") != "ok"
                or not isinstance(candidates, list)
                or len(candidates) != last.get("count")
            ):
                # 固定对照不能悄悄改 top-k；上下文不足需明确失败而不是取被截短集合。
                action = m.AgentStopDecision(reason="Fixed candidate list exceeds context budget")
                self.stage = "finish"
            elif candidates:
                candidate_rows = [m.Candidate.model_validate(value) for value in candidates]
                steps = [
                    self.step_builder(candidate, request.tool_specs, request.question, index)
                    for index, candidate in enumerate(candidate_rows)
                ]
                if any(
                    set(step.source_ids) != {candidate.source_id}
                    for step, candidate in zip(steps, candidate_rows, strict=True)
                ):
                    raise ValueError(
                        "fixed retrieval step is outside the fixed retrieval candidates"
                    )
                action = m.AgentToolsDecision(steps=steps)
                self.stage = "compose"
            else:
                action = m.AgentComposeDecision()
                self.stage = "finish"
        elif self.stage == "compose":
            action = m.AgentComposeDecision()
            self.stage = "finish"
        else:
            action = (
                m.AgentFinishDecision()
                if context["has_answer"]
                else m.AgentStopDecision(reason="Fixed baseline did not obtain an accepted answer")
            )
        return m.ModuleResult(call_id=request.call_id, data=action, usage=m.Usage.local())


@dataclass
class AblationResult:
    """可导入工作台的摘要，以及包含配置、逐任务结果和方差的独立实验记录。"""

    report: EvaluationImport
    details: dict[str, Any]


def ablation_identity(
    tasks: Sequence[EvaluationTask], config: m.RunConfig
) -> tuple[str, str, list[dict]]:
    """返回 checkpoint 需要绑定的任务清单和共享运行配置摘要。"""
    base = m.RunConfig.model_validate(config.model_dump(mode="json"))
    manifest = [task.manifest() for task in tasks]
    return (
        sha256(canonical_json(manifest)).hexdigest(),
        sha256(canonical_json(base)).hexdigest(),
        manifest,
    )


def sample_key(sample: EvaluationSample) -> tuple[str, int, str]:
    """返回一条样本的稳定身份，避免恢复时按列表位置误判。"""
    return sample.mode, sample.repeat, sample.task_id


async def run_ablation(
    tasks: Sequence[EvaluationTask],
    *,
    ports_factory: Callable[[], AbstractAsyncContextManager[ExecutionPorts]],
    config: m.RunConfig,
    step_builder: StepBuilder,
    dataset_id: str,
    task_set_id: str,
    repeats: int = 1,
    title: str = "Proposal ablation",
    evaluator: str = "offline ablation runner",
    trace_factory: Callable[[], TraceSink] | None = None,
    unnecessary: Callable[[dict], bool | None] | None = None,
    data_usage_provider: DataUsageProvider | None = None,
    resume_samples: Sequence[EvaluationSample] = (),
    on_sample: Callable[[EvaluationSample], None] | None = None,
) -> AblationResult:
    """顺序运行相同任务集的三组，冻结模型参数/预算，仅切换核验策略与规划器。

    宿主必须提供冻结数据的同一工厂；跨组返回快照 ID 不一致时拒绝汇总。任务、gold、
    数值容差和相关源仅用于结果评分，绝不进入 RunRequest 或 step_builder。
    可选的用量读取器仅在每轮结束后、端口关闭前读取宿主已测量的目录和工具用量。
    """
    if type(repeats) is not int or repeats < 1 or not tasks:
        raise ValueError("tasks must be nonempty and repeats must be a positive integer")
    tasks = tuple(tasks)
    if len({task.task_id for task in tasks}) != len(tasks):
        raise ValueError("task IDs must be unique")
    base = m.RunConfig.model_validate(config.model_dump(mode="json"))
    if base.candidate_top_k > base.max_plan_steps:
        raise ValueError("fixed top-k exceeds the shared tool-step budget")
    if base.max_agent_turns < 4:
        raise ValueError("fixed retrieval requires at least four decision turns")
    # 在任何端口实例创建或付费请求之前验证报告身份和任务数量。
    metadata = EvaluationImport.model_validate(
        {
            "title": title,
            "dataset_id": dataset_id,
            "task_set_id": task_set_id,
            "model_id": base.model_id,
            "evaluator": evaluator,
            "evaluated_at": datetime.now(UTC).isoformat(),
            "results": [{"mode": mode, "task_count": len(tasks) * repeats} for mode in MODES],
        }
    ).model_dump(mode="json")
    task_digest, config_digest, manifest = ablation_identity(tasks, base)
    samples = list(resume_samples)
    expected_keys = {
        (mode, repeat, task.task_id)
        for mode in MODES
        for repeat in range(repeats)
        for task in tasks
    }
    completed_keys = {sample_key(sample) for sample in samples}
    if len(completed_keys) != len(samples) or not completed_keys <= expected_keys:
        raise ValueError("resume samples do not match the task set")
    for sample in samples:
        expected_mode = "no_verifier" if sample.mode == "fixed_retrieval" else sample.mode
        if (
            sample.result.task_id != sample.task_id
            or sample.result.mode != expected_mode
            or sample.result.versions.model_id != base.model_id
            or sample.result.versions.model_params != base.model_params
            or sample.result.versions.prompt_versions != base.prompt_versions
            or (
                base.dataset_snapshot is not None
                and sample.result.versions.dataset_snapshot != base.dataset_snapshot
            )
        ):
            raise ValueError("resume sample does not match its task or run configuration")
    snapshots = {
        sample.result.versions.dataset_snapshot
        for sample in samples
        if sample.result.versions.dataset_snapshot is not None
    }
    configurations = {}
    for mode in MODES:
        policy = base.model_copy(update={"require_verification": mode == "full_agent"}, deep=True)
        configurations[mode] = policy.model_dump(mode="json")
        for repeat in range(repeats):
            for task in tasks:
                key = (mode, repeat, task.task_id)
                if key in completed_keys:
                    continue
                trace = CapturingTrace(trace_factory() if trace_factory else None)
                async with ports_factory() as ports:
                    result = await run(
                        m.RunRequest(
                            task_id=task.task_id,
                            question=task.question,
                            run_id=f"eval-{uuid4().hex}",
                            mode="no_verifier" if mode == "fixed_retrieval" else mode,
                            config=policy.model_copy(deep=True),
                        ),
                        data=ports.data,
                        content=ports.content,
                        planner=FixedRetrievalPlanner(step_builder)
                        if mode == "fixed_retrieval"
                        else ports.planner,
                        trace=trace,
                    )
                    data_usage = (
                        data_usage_provider(ports.data, result.run_id)
                        if data_usage_provider is not None
                        else None
                    )
                    if result.stop_reason == "catalog_snapshot_mismatch":
                        raise ValueError("catalog snapshot mismatch")
                    sample = score_sample(
                        task,
                        mode,
                        repeat,
                        result,
                        trace.events,
                        trace.artifacts,
                        unnecessary=unnecessary,
                        data_usage=data_usage,
                    )
                if result.versions.dataset_snapshot is not None:
                    snapshots.add(result.versions.dataset_snapshot)
                if len(snapshots) > 1:
                    raise ValueError("dataset snapshot changed across ablation runs")
                samples.append(sample)
                completed_keys.add(key)
                if on_sample is not None:
                    on_sample(sample)
    by_mode = {mode: [s for s in samples if s.mode == mode] for mode in MODES}
    rows, variability = [], {}
    for mode in MODES:
        mode_samples = by_mode[mode]
        metrics, variability[mode] = aggregate(mode_samples)
        rows.append(
            {
                "mode": mode,
                "task_count": len(tasks) * repeats,
                "metrics": metrics,
                "metric_coverage": metric_coverage(mode_samples),
            }
        )
    report = EvaluationImport.model_validate(
        {
            **metadata,
            "evaluated_at": datetime.now(UTC).isoformat(),
            "notes": f"task_digest={task_digest}; config_digest={config_digest}; "
            "retrieval=successful tool-accessed files; quality=macro over all runs; "
            "missing annotations/usage=null; fixed baseline uses no_verifier transport only.",
            "results": rows,
        }
    )
    return AblationResult(
        report,
        {
            "task_digest": task_digest,
            "config_digest": config_digest,
            "dataset_snapshot": next(iter(snapshots), None),
            "configurations": configurations,
            "manifest": manifest,
            "samples": [sample.export() for sample in samples],
            "variability": variability,
            "metric_coverage": {mode: metric_coverage(by_mode[mode]) for mode in MODES},
            "failure_categories": {mode: failure_category_summary(by_mode[mode]) for mode in MODES},
            "failure_diagnostics": {
                mode: failure_diagnostic_summary(by_mode[mode]) for mode in MODES
            },
            "metric_definitions": {
                "accuracy": "NFKC + casefold + whitespace normalized exact match",
                "numerical_accuracy": "exact decimal with annotated absolute tolerance",
                "retrieval": "unique registered source_ids from successful accepted tool attempts",
                "grounding": "unique cited files in relevant annotations / unique cited files",
                "valid_tool_call_rate": "controller-accepted ok ToolResults / audited tool-call intents",
                "unnecessary_tool_call_rate": "independent bool labels / audited tool-call intents",
                "retry_recovery_rate": "retried operations with later ok result / retried operations",
                "external_data_usage": "trusted measurements per catalog/tool attempt, including retries",
                "unknown": "null if any required annotation or measured usage is missing",
            },
        },
    )
