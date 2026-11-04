"""显式命令行运行 Proposal 三组评估；不属于生产 wheel 或 Web 任务执行模式。

在源码目录使用 python -m tests.ablation_runtime；配置中的真实 A/C 模块和模型由
调用者准备。导入、--help 与离线 pytest 不执行网络或创建文件。运行输出目录必须
不存在，报告和完整 JSONL 审计统一写入该新目录，不覆盖历史 runs。
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
from pathlib import Path

from lake_agent.config import load_config
from lake_agent.trace import JsonlTraceSink
from lake_agent.validation import canonical_json
from lake_agent.web.configuration import configured_profile

from .support.ablation import run_ablation
from .support.ablation_metrics import EvaluationTask, _json


def _tasks(path: Path) -> list[EvaluationTask]:
    """严格读取本地评估标注；JSON 只选择数据，不接受动态执行字段。"""
    with path.open("rb") as source:
        raw = source.read(2_000_001)
    if len(raw) > 2_000_000:
        raise ValueError("task manifest exceeds 2 MB")
    values = _json(raw)
    if not isinstance(values, list) or not values:
        raise ValueError("task manifest must be a nonempty array")
    tasks = []
    for value in values:
        if not isinstance(value, dict):
            raise ValueError("each task must be an object")
        value = dict(value)
        for key, converter in (("expected_answers", tuple), ("relevant_source_ids", frozenset)):
            if value.get(key) is not None:
                if not isinstance(value[key], list) or any(
                    type(item) is not str for item in value[key]
                ):
                    raise ValueError(f"{key} must be an array of strings or null")
                value[key] = converter(value[key])
        tasks.append(EvaluationTask(**value))
    return tasks


def _write(directory: Path, outcome):
    """独占创建最终摘要和详情，不覆盖同名结果。"""
    for name, value in (
        ("report.json", outcome.report.model_dump(mode="json")),
        ("details.json", outcome.details),
    ):
        with (directory / name).open("xb") as output:
            output.write(canonical_json(value) + b"\n")


def main(argv: list[str] | None = None) -> int:
    """实际执行必须显式指定配置、冻结任务集、工具映射和全新输出目录。"""
    parser = argparse.ArgumentParser(description="Proposal 三组消融评估（显式执行真实配置）")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--step-builder",
        required=True,
        help="可信评估模块 module:function；(candidate, tool_specs, question, index) -> AgentToolStep",
    )
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--task-set-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=1)
    args = parser.parse_args(argv)
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    try:
        config = load_config(args.config)
        tasks = _tasks(args.manifest)
        module_name, separator, name = args.step_builder.partition(":")
        if not separator or not module_name or not name or ":" in name:
            raise ValueError("step builder must be module:function")
        builder = getattr(importlib.import_module(module_name), name)
        if not callable(builder):
            raise ValueError("step builder is not callable")
        profile = configured_profile(config)
        directory = args.output_dir.expanduser().absolute()
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        outcome = asyncio.run(
            run_ablation(
                tasks,
                ports_factory=profile.ports_factory,
                config=profile.run_config,
                step_builder=builder,
                dataset_id=args.dataset_id,
                task_set_id=args.task_set_id,
                repeats=args.repeats,
                evaluator="configured ablation runner",
                trace_factory=lambda: JsonlTraceSink(directory / "audit"),
            )
        )
        _write(directory, outcome)
        print(directory / "report.json")
        return 0
    except Exception as exc:
        # 适配器异常可能携带私有选项；终端不回显异常文本。
        parser.error(f"Evaluation configuration, manifest or output failed ({type(exc).__name__}).")
    except KeyboardInterrupt:
        return 130
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
