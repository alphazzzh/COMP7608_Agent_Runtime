"""显式命令行运行 Proposal 三组评估；不属于生产 wheel 或 Web 任务执行模式。

在源码目录使用 python -m tests.ablation_runtime；配置中的真实 A/C 模块和模型由
调用者准备。导入、--help 与离线 pytest 不执行网络或创建文件。运行输出目录必须
不存在，报告和完整 JSONL 审计统一写入该新目录，不覆盖历史 runs。
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import importlib
import inspect
import os
import platform
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

from lake_agent.config import load_config
from lake_agent.trace import JsonlTraceSink
from lake_agent.validation import canonical_json
from lake_agent.web.configuration import configured_profile

from .support.ablation import (
    MODES,
    ablation_identity,
    run_ablation,
    sample_key,
)
from .support.ablation_metrics import EvaluationSample, EvaluationTask, _json

_MAX_FILE_BYTES = 2_000_000
_MAX_DIRECTORY_BYTES = 20_000_000
_CHECKPOINT_VERSION = 2
_FINAL_FILES = ("details.json", "failures.json", "report.json")


def _same_alias(row: dict, *keys: str):
    """读取字段别名；同一对象同时给出冲突值时拒绝静默选取。"""
    values = [(key, row[key]) for key in keys if key in row and row[key] is not None]
    if len(values) > 1 and any(value != values[0][1] for _, value in values[1:]):
        raise ValueError(f"conflicting task fields: {', '.join(key for key, _ in values)}")
    return values[0][1] if values else None


def _hop_count(row: dict) -> int | None:
    """兼容题级 hop 与 hop_id；LakeQA 推理链则以步骤数作为深度。"""
    top_value = _same_alias(row, "hop", "hop_id")
    if top_value is not None and type(top_value) is not str:
        if type(top_value) is not int or top_value < 1:
            raise ValueError("hop must be a positive integer")
    hops = row.get("reasoning_hops")
    if hops is not None:
        if not isinstance(hops, list):
            raise ValueError("reasoning_hops must be an array")
        for hop in hops:
            if not isinstance(hop, dict):
                raise ValueError("each reasoning_hops item must be an object")
            value = _same_alias(hop, "hop", "hop_id")
            if value is not None and type(value) not in (int, str):
                raise ValueError("hop identifiers must be integers or strings")
        return len(hops) or None
    if top_value is None or type(top_value) is str:
        return None
    return top_value


def _task(value: dict) -> EvaluationTask:
    """将 LakeQA 和当前评估清单字段归一化为 gold 隔离的任务对象。"""
    if not isinstance(value, dict):
        raise ValueError("each task must be an object")
    fields = {
        "task_id": _same_alias(value, "task_id", "question_id", "id"),
        "question": _same_alias(value, "question", "query"),
        "hop_count": _hop_count(value),
    }
    answers = _same_alias(value, "expected_answers", "answer")
    if answers is not None:
        if value.get("expected_answers") is not None:
            if not isinstance(answers, list) or any(type(item) is not str for item in answers):
                raise ValueError("expected_answers must be an array of strings")
            fields["expected_answers"] = tuple(answers)
        elif type(answers) is str:
            fields["expected_answers"] = (answers,)
        elif isinstance(answers, list) and all(type(item) is str for item in answers):
            fields["expected_answers"] = tuple(answers)
        else:
            raise ValueError("answer must be a string or an array of strings")
    sources = _same_alias(value, "relevant_source_ids", "relevant_sources", "datasets_used")
    if sources is not None:
        if not isinstance(sources, list) or any(type(item) is not str for item in sources):
            raise ValueError("relevant source annotations must be an array of strings")
        fields["relevant_source_ids"] = frozenset(sources)
    allowed = {
        "numeric_answer",
        "numeric_tolerance",
        "structured_answer_json",
        "not_applicable_metrics",
    }
    unknown = set(value) - {
        "task_id",
        "question_id",
        "id",
        "question",
        "query",
        "expected_answers",
        "answer",
        "relevant_source_ids",
        "relevant_sources",
        "datasets_used",
        "hop",
        "hop_id",
        "hop_count",
        "reasoning_hops",
        *allowed,
    }
    if unknown:
        raise ValueError(f"unknown task fields: {', '.join(sorted(unknown))}")
    if "hop_count" in value:
        if fields["hop_count"] is not None and fields["hop_count"] != value["hop_count"]:
            raise ValueError("conflicting task fields: hop_count")
        fields["hop_count"] = value["hop_count"]
    fields.update({key: value[key] for key in allowed if key in value})
    if fields.get("not_applicable_metrics") is not None:
        values = fields["not_applicable_metrics"]
        if not isinstance(values, list) or any(type(item) is not str for item in values):
            raise ValueError("not_applicable_metrics must be an array of strings or null")
        fields["not_applicable_metrics"] = frozenset(values)
    return EvaluationTask(**fields)


def _tasks(path: Path) -> list[EvaluationTask]:
    """读取单一数组清单、单条任务文件或 LakeQA mini 题单目录。"""
    path = path.expanduser()
    files = (
        sorted(path.rglob("task_*.json")) or sorted(path.rglob("*.json"))
        if path.is_dir()
        else [path]
    )
    if not files:
        raise ValueError("task manifest contains no JSON files")
    maximum = _MAX_DIRECTORY_BYTES if path.is_dir() else _MAX_FILE_BYTES
    remaining = maximum
    tasks = []
    for source_path in files:
        with source_path.open("rb") as source:
            raw = source.read(remaining + 1)
        remaining -= len(raw)
        if remaining < 0:
            limit = "20 MB" if path.is_dir() else "2 MB"
            raise ValueError(f"task manifest exceeds {limit}")
        values = _json(raw, exact_decimals=True)
        rows = values.get("tasks") if isinstance(values, dict) and "tasks" in values else values
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list) or not rows:
            raise ValueError("each task file must contain a nonempty array or task object")
        tasks.extend(_task(row) for row in rows)
    if not tasks:
        raise ValueError("task manifest must be nonempty")
    return tasks


def _callable_digest(func) -> str:
    """摘要可信评估函数所在源码或字节码，防止代码变更后混用旧样本。"""
    target = inspect.unwrap(func)
    try:
        source_file = inspect.getsourcefile(target) or inspect.getfile(target)
    except (TypeError, OSError):
        source_file = None
    if source_file and Path(source_file).is_file():
        return hashlib.sha256(Path(source_file).read_bytes()).hexdigest()
    code = getattr(target, "__code__", None)
    if code is not None:
        return hashlib.sha256(code.co_code).hexdigest()
    return hashlib.sha256(repr(type(target)).encode("utf-8")).hexdigest()


def _identity(args, tasks, profile, *, builder, data_usage_provider=None) -> dict:
    """绑定题单、运行政策、适配器与评估函数实现摘要，恢复时严格比对。"""
    task_digest, config_digest, _ = ablation_identity(tasks, profile.run_config)
    config_path = args.config.expanduser()
    config_file_digest = (
        hashlib.sha256(config_path.read_bytes()).hexdigest() if config_path.is_file() else None
    )
    return {
        "version": _CHECKPOINT_VERSION,
        "task_digest": task_digest,
        "config_digest": config_digest,
        "config_file_digest": config_file_digest,
        "profile_binding_id": getattr(profile, "binding_id", None),
        "dataset_id": args.dataset_id,
        "task_set_id": args.task_set_id,
        "repeats": args.repeats,
        "step_builder": args.step_builder,
        "step_builder_digest": _callable_digest(builder),
        "data_usage_provider": args.data_usage_provider,
        "data_usage_provider_digest": (
            _callable_digest(data_usage_provider) if data_usage_provider is not None else None
        ),
        "python": list(sys.version_info[:3]),
        "platform": platform.platform(),
    }


@contextmanager
def _directory_lock(directory: Path):
    """以非阻塞独占文件锁防止两个进程并发写入或续跑同一输出目录。"""
    lock_path = directory / ".lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError(
                "output directory is already locked by another evaluation process"
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _atomic_write(path: Path, data: bytes) -> None:
    """同目录原子替换小型 JSON 文件，崩溃不会留下半份检查点。"""
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _write_checkpoint(path: Path, identity: dict, completed: set[tuple[str, int, str]]) -> None:
    rows = [
        {"mode": mode, "repeat": repeat, "task_id": task_id}
        for mode in MODES
        for repeat in sorted({key[1] for key in completed if key[0] == mode})
        for task_id in sorted(key[2] for key in completed if key[0] == mode and key[1] == repeat)
    ]
    _atomic_write(path, canonical_json({"identity": identity, "completed": rows}) + b"\n")


def _read_resume(directory: Path, identity: dict, tasks: list[EvaluationTask], repeats: int):
    """读取完整 JSONL 样本；仅忽略崩溃留下的最后一条不完整行。"""
    checkpoint_path = directory / "checkpoint.json"
    sample_path = directory / "samples.jsonl"
    if not checkpoint_path.is_file() or not sample_path.is_file():
        raise ValueError("resume requires checkpoint.json and samples.jsonl")
    checkpoint = _json(checkpoint_path.read_bytes())
    if (
        not isinstance(checkpoint, dict)
        or set(checkpoint) != {"identity", "completed"}
        or checkpoint.get("identity") != identity
        or not isinstance(checkpoint.get("completed"), list)
    ):
        raise ValueError("checkpoint identity or structure does not match this experiment")
    raw = sample_path.read_bytes()
    if raw and not raw.endswith(b"\n"):
        complete = raw.rpartition(b"\n")[0]
        raw = complete + (b"\n" if complete else b"")
        _atomic_write(sample_path, raw)
    tasks_by_id = {task.task_id for task in tasks}
    samples, keys = [], set()
    for line in raw.splitlines():
        if not line.strip():
            continue
        sample = EvaluationSample.from_export(_json(line))
        key = sample_key(sample)
        if (
            key in keys
            or key[2] not in tasks_by_id
            or key[0] not in MODES
            or not 0 <= key[1] < repeats
        ):
            raise ValueError("sample log contains a duplicate or out-of-range task")
        keys.add(key)
        samples.append(sample)
    recorded_rows = checkpoint["completed"]
    if any(
        not isinstance(row, dict)
        or set(row) != {"mode", "repeat", "task_id"}
        or type(row.get("mode")) is not str
        or type(row.get("repeat")) is not int
        or type(row.get("task_id")) is not str
        for row in recorded_rows
    ):
        raise ValueError("checkpoint completion index is invalid")
    recorded = {(row["mode"], row["repeat"], row["task_id"]) for row in recorded_rows}
    if len(recorded) != len(recorded_rows) or recorded - keys:
        raise ValueError("checkpoint references duplicate or missing sample records")
    # JSONL 刷盘先于 checkpoint 替换；崩溃窗口中的完整末行可安全纳入恢复。
    _write_checkpoint(checkpoint_path, identity, keys)
    return samples, keys


def _append_sample(path: Path, sample: EvaluationSample) -> None:
    """把单条已完成样本追加并刷盘，提供低开销的逐任务恢复点。"""
    with path.open("ab") as output:
        output.write(canonical_json(sample.export()) + b"\n")
        output.flush()
        os.fsync(output.fileno())


def _write(directory: Path, outcome):
    """按详情、失败摘要、最终报告顺序原子写入，中断后可用 --resume 补齐。"""
    values = (
        ("details.json", outcome.details),
        ("failures.json", outcome.details["failure_diagnostics"]),
        ("report.json", outcome.report.model_dump(mode="json")),
    )
    for name, value in values:
        _atomic_write(directory / name, canonical_json(value) + b"\n")


def _load_callable(spec: str):
    """仅从命令行显式指定的可信模块名读取评估函数。"""
    module_name, separator, name = spec.partition(":")
    if not separator or not module_name or not name or ":" in name:
        raise ValueError("evaluation function must be module:function")
    function = getattr(importlib.import_module(module_name), name)
    if not callable(function):
        raise ValueError("evaluation function is not callable")
    return function


def main(argv: list[str] | None = None) -> int:
    """运行冻结的三组任务清单；恢复只使用同一身份下的本地完整样本。"""
    parser = argparse.ArgumentParser(description="Proposal LakeQA 三组消融评估")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--step-builder",
        required=True,
        help="可信评估模块 module:function；(candidate, tool_specs, question, index) -> AgentToolStep",
    )
    parser.add_argument(
        "--data-usage-provider",
        help="可选可信评估函数 module:function；(data_port, run_id) -> 按 call_id 索引的实测用量",
    )
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--task-set-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--resume", action="store_true", help="从该输出目录的本地 checkpoint 继续")
    args = parser.parse_args(argv)
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    try:
        config = load_config(args.config)
        tasks = _tasks(args.manifest)
        builder = _load_callable(args.step_builder)
        data_usage_provider = (
            _load_callable(args.data_usage_provider) if args.data_usage_provider else None
        )
        profile = configured_profile(config)
        directory = args.output_dir.expanduser().absolute()
        identity = _identity(
            args,
            tasks,
            profile,
            builder=builder,
            data_usage_provider=data_usage_provider,
        )
        checkpoint_path = directory / "checkpoint.json"
        sample_path = directory / "samples.jsonl"
        if not args.resume:
            directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        elif not directory.is_dir() or not checkpoint_path.is_file() or not sample_path.is_file():
            raise ValueError("resume requires checkpoint.json and samples.jsonl")
        with _directory_lock(directory):
            if args.resume:
                if all((directory / name).is_file() for name in _FINAL_FILES):
                    raise ValueError("resume requires an unfinished output directory")
                resume_samples, completed = _read_resume(directory, identity, tasks, args.repeats)
            else:
                sample_path.touch(mode=0o600, exist_ok=False)
                resume_samples, completed = [], set()
                _write_checkpoint(checkpoint_path, identity, completed)

            def persist(sample: EvaluationSample) -> None:
                key = sample_key(sample)
                if key in completed:
                    raise ValueError("duplicate completed evaluation sample")
                _append_sample(sample_path, sample)
                completed.add(key)
                _write_checkpoint(checkpoint_path, identity, completed)

            outcome = asyncio.run(
                run_ablation(
                    tasks,
                    ports_factory=profile.ports_factory,
                    config=profile.run_config,
                    step_builder=builder,
                    dataset_id=args.dataset_id,
                    task_set_id=args.task_set_id,
                    repeats=args.repeats,
                    resume_samples=resume_samples,
                    on_sample=persist,
                    data_usage_provider=data_usage_provider,
                    evaluator="configured LakeQA ablation runner",
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
