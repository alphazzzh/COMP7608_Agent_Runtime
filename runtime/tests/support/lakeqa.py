"""将本地 LakeQA mini 题单转换为离线评分清单，并提供固定检索组的工具映射。"""

from __future__ import annotations

import argparse
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import TemporaryDirectory

from lake_agent import models as m
from lake_agent.validation import canonical_json

_INPUT_LIMIT = 20_000_000
_MANIFEST_LIMIT = 2_000_000
_WORD = re.compile(r"[^\W_]+")
_COMMON_WORDS = frozenset(
    "and are can could did does for from had has have how into its the their then there "
    "these this those was were what when where which who whose will with would".split()
)


def _json_object(pairs: list[tuple]) -> dict:
    """拒绝重复标注字段，避免选择其中一个值。"""
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("Duplicate JSON key in LakeQA task")
    return result


def _invalid_constant(value: str) -> None:
    """拒绝 JSON 扩展的非有限数。"""
    raise ValueError(f"Invalid LakeQA JSON constant: {value}")


def _task_json(row: dict) -> bytes:
    """输出评估器所需字段，保留原始数值答案的十进制精度。"""
    if not isinstance(row, dict):
        raise ValueError("Each LakeQA task must be a JSON object")
    task_id, question = row.get("question_id"), row.get("question")
    if any(not isinstance(value, str) or not value.strip() for value in (task_id, question)):
        raise ValueError("LakeQA question_id and question must be nonempty strings")
    task = {"task_id": task_id, "question": question}

    answer = row.get("answer")
    numeric = type(answer) in (int, Decimal)
    if numeric:
        task["expected_answers"] = [str(answer)]
    elif isinstance(answer, str) and answer.strip():
        task["expected_answers"] = [answer]
    elif answer is not None:
        raise ValueError("LakeQA answer must be a nonempty string, a number or null")

    sources = row.get("relevant_source_ids")
    if "relevant_source_ids" not in row and row.get("nodes") is not None:
        nodes = row["nodes"]
        if not isinstance(nodes, dict) or any(
            not isinstance(node, dict) for node in nodes.values()
        ):
            raise ValueError("LakeQA nodes must be keyed objects")
        sources = [node.get("source") for node in nodes.values()]
    if sources is not None:
        if not isinstance(sources, list) or any(
            not isinstance(source, str) or not source.strip() for source in sources
        ):
            raise ValueError("LakeQA relevant sources must be nonempty strings")
        task["relevant_source_ids"] = sorted(set(sources))

    encoded = canonical_json(task)
    if numeric:
        # Decimal 已由 JSON 解析器验证，原样写回数值，供现行和早期评估入口读取。
        return encoded[:-1] + b',"numeric_answer":' + str(answer).encode("ascii") + b"}"
    return encoded


def _manifest(directory: Path) -> bytes:
    """有界读取题单目录，输出不含推理链、事实或候选列表的冻结清单。"""
    if not directory.is_dir():
        raise ValueError("LakeQA input must be a task directory")
    remaining = _INPUT_LIMIT
    output_bytes = 2  # 数组的左右括号
    tasks = []
    seen_ids = set()
    for path in sorted(directory.rglob("task_*.json")):
        with path.open("rb") as source:
            raw = source.read(remaining + 1)
        remaining -= len(raw)
        if remaining < 0:
            raise ValueError("LakeQA task input exceeds 20 MB")
        row = json.loads(
            raw,
            parse_float=Decimal,
            parse_constant=_invalid_constant,
            object_pairs_hook=_json_object,
        )
        task = _task_json(row)
        task_id = row["question_id"]
        if task_id in seen_ids:
            raise ValueError(f"Duplicate LakeQA question ID: {task_id}")
        seen_ids.add(task_id)
        output_bytes += len(task) + bool(tasks)
        if output_bytes + 1 > _MANIFEST_LIMIT:
            raise ValueError("LakeQA output exceeds the evaluation manifest's 2 MB limit")
        tasks.append(task)
    if not tasks:
        raise ValueError("No LakeQA task_*.json files found")
    return b"[" + b",".join(tasks) + b"]\n"


def build_step(
    candidate: m.Candidate, specs: list[m.ToolSpec], question: str, index: int
) -> m.AgentToolStep:
    """每个候选做一次字面抽取，优先选择问题和来源路径共有的实词。"""
    if not any(spec.name == "extract" for spec in specs):
        raise ValueError("LakeQA fixed retrieval requires the local extract tool")
    question_words = _WORD.findall(question.casefold())
    source_words = set(_WORD.findall(candidate.source_id.casefold()))
    keywords = [word for word in question_words if word not in _COMMON_WORDS]
    keyword = max(
        keywords or question_words or [question],
        key=lambda word: (word in source_words, not word.isdecimal(), len(word)),
    )
    return m.AgentToolStep(
        step_id=f"fixed-{index}",
        tool_name="extract",
        source_ids=[candidate.source_id],
        args={"source_id": candidate.source_id, "query": keyword, "max_chars": 1000},
        purpose="Read fixed top-k source using a question keyword",
    )


def main(argv: list[str] | None = None) -> int:
    """将冻结清单写到新路径，保留已有评估输入。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="包含 task_*.json 的 LakeQA mini 目录")
    parser.add_argument("output", type=Path, help="新建的离线清单 JSON 路径")
    args = parser.parse_args(argv)
    try:
        payload = _manifest(args.directory.expanduser())
        output = args.output.expanduser()
        # 同一文件系统内先写完再发布，两个并发转换也不会覆盖已有清单。
        with TemporaryDirectory(prefix=".lakeqa-", dir=output.parent) as temporary:
            staged = Path(temporary) / "manifest.json"
            staged.write_bytes(payload)
            output.hardlink_to(staged)
    except (OSError, ValueError, InvalidOperation, RecursionError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
