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
from lake_agent.ports import TraceSink
from lake_agent.runtime import run
from lake_agent.validation import canonical_json
from lake_agent.web.profiles import ExecutionPorts

from .ablation_metrics import EvaluationTask, aggregate, score_sample

MODES = ("fixed_retrieval", "no_verifier", "full_agent")
StepBuilder = Callable[[m.Candidate, list[m.ToolSpec], str, int], m.AgentToolStep]


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
    """评估固定策略；工具名和参数映射必须由评估宿主提供，核心不预设 extract。"""

    agent_protocol = m.AGENT_PROTOCOL

    def __init__(self, step_builder: StepBuilder):
        self.step_builder = step_builder

    async def decide(self, request):
        context = request.context
        last = context.get("last_action") or {}
        if context["has_answer"]:
            action = m.AgentFinishDecision()
        elif last.get("kind") == "tools":
            action = m.AgentComposeDecision()
        elif last.get("kind") == "search":
            candidates = context.get("candidates")
            if not isinstance(candidates, list) or len(candidates) != last.get("count"):
                # 固定对照不能悄悄改 top-k；上下文不足需明确失败而不是取被截短集合。
                action = m.AgentStopDecision(reason="Fixed candidate list exceeds context budget")
            elif candidates:
                steps = [
                    self.step_builder(
                        m.Candidate.model_validate(value),
                        request.tool_specs,
                        request.question,
                        index,
                    )
                    for index, value in enumerate(candidates)
                ]
                action = m.AgentToolsDecision(steps=steps)
            else:
                action = m.AgentComposeDecision()
        else:
            action = m.AgentSearchDecision(query=request.question)
        return m.ModuleResult(call_id=request.call_id, data=action, usage=m.Usage.local())


@dataclass
class AblationResult:
    """可导入工作台的摘要，以及包含配置、逐任务结果和方差的独立实验记录。"""

    report: EvaluationImport
    details: dict[str, Any]


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
) -> AblationResult:
    """顺序运行相同任务集的三组，冻结模型参数/预算，仅切换核验策略与规划器。

    宿主必须提供冻结数据的同一工厂；跨组返回快照 ID 不一致时拒绝汇总。任务、gold、
    数值容差和相关源仅用于结果评分，绝不进入 RunRequest 或 step_builder。
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
    manifest = [
        task.__dict__
        | {
            "expected_answers": list(task.expected_answers)
            if task.expected_answers is not None
            else None,
            "relevant_source_ids": sorted(task.relevant_source_ids)
            if task.relevant_source_ids is not None
            else None,
        }
        for task in tasks
    ]
    config_digest = sha256(canonical_json(base)).hexdigest()
    task_digest = sha256(canonical_json(manifest)).hexdigest()
    samples = []
    snapshots = set()
    configurations = {}
    for mode in MODES:
        policy = base.model_copy(update={"require_verification": mode == "full_agent"}, deep=True)
        configurations[mode] = policy.model_dump(mode="json")
        for repeat in range(repeats):
            for task in tasks:
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
                if result.versions.dataset_snapshot is not None:
                    snapshots.add(result.versions.dataset_snapshot)
                if len(snapshots) > 1:
                    raise ValueError("dataset snapshot changed across ablation runs")
                samples.append(
                    score_sample(
                        task,
                        mode,
                        repeat,
                        result,
                        trace.events,
                        trace.artifacts,
                        unnecessary=unnecessary,
                    )
                )
    rows, variability = [], {}
    for mode in MODES:
        metrics, variability[mode] = aggregate([s for s in samples if s.mode == mode])
        rows.append({"mode": mode, "task_count": len(tasks) * repeats, "metrics": metrics})
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
            "metric_definitions": {
                "accuracy": "NFKC + casefold + whitespace normalized exact match",
                "retrieval": "unique registered source_ids from successful accepted tool attempts",
                "grounding": "unique cited files in relevant annotations / unique cited files",
                "valid_tool_call_rate": "valid ok ToolResults / audited tool-call intents",
                "unnecessary_tool_call_rate": "independent bool labels / audited tool-call intents",
                "retry_recovery_rate": "retried operations with later ok result / retried operations",
                "unknown": "null if any required annotation or measured usage is missing",
            },
        },
    )
