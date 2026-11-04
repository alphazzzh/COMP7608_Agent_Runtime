"""为已知内容输出错误构造有限、可审计的纠错信息。

这里只检查结构化输出中的引用身份并生成预览，不请求模型、不修改答案、
不推断哪些错误可以重试。控制器决定是否继续，并把反馈纳入原运行预算。
诊断上限独立于模型输出上限；省略信息会明确计入 omitted。
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Iterable, Sized
from itertools import islice
from typing import Literal

from .. import models as m

_MAX_BYTES = 8000
_MAX_ITEMS = 24


def feedback_protocol(content) -> Literal["content-feedback-v1"] | None:
    """只识别精确能力声明，不能把任意私有属性带入恢复身份或公开日志。"""
    try:
        value = getattr(content, "content_feedback_protocol", None)
    except Exception:
        return None
    return (
        m.CONTENT_FEEDBACK_PROTOCOL
        if type(value) is str and value == m.CONTENT_FEEDBACK_PROTOCOL
        else None
    )


def _size(value) -> int:
    return len(
        json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
            "utf-8"
        )
    )


def build_diagnostics(
    *,
    issues: list[dict] | None = None,
    allowed_evidence_ids: Iterable[str] = (),
    allowed_source_ids: Iterable[str] = (),
    rejected_output: m.JsonValue | None = None,
    max_bytes: int = _MAX_BYTES,
) -> m.JsonObject:
    """复制有界的差异和原输出；256 字节以上的小预算也会确定结束。

    每个集合最多预览24项，文本、深度和节点数均有限制。实际全文仍在调用证据中，
    不把截断的 allowed IDs 当作完整授权集。调用方不得传入请求头或凭据。
    """
    if type(max_bytes) is not int or not 256 <= max_bytes <= _MAX_BYTES:
        raise ValueError("diagnostic max_bytes must be between 256 and 8000")
    omitted: dict[str, m.JsonValue] = {}
    nodes_left = 160

    def count(name: str, amount: int = 1):
        omitted[name] = int(omitted.get(name, 0)) + amount

    def preview(value, depth=0):
        nonlocal nodes_left
        nodes_left -= 1
        if nodes_left < 0 or depth > 8:
            count("values")
            return "[omitted]"
        if isinstance(value, str):
            # 非法代理字符不能使一个本来可报告的模型错误再触发诊断错误。
            safe = value.encode("utf-8", "replace").decode("utf-8")
            if len(safe) > 1000:
                count("text_characters", len(safe) - 1000)
                return safe[:1000]
            return safe
        if type(value) is int and value.bit_length() > 256:
            count("values")
            return "[large integer omitted]"
        if value is None or type(value) in (bool, int):
            return value
        if type(value) is float:
            return value if math.isfinite(value) else "[non-finite]"
        if isinstance(value, (list, tuple)):
            if len(value) > _MAX_ITEMS:
                count("values", len(value) - _MAX_ITEMS)
            return [preview(item, depth + 1) for item in value[:_MAX_ITEMS]]
        if isinstance(value, dict):
            if len(value) > _MAX_ITEMS:
                count("values", len(value) - _MAX_ITEMS)
            result = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= _MAX_ITEMS:
                    break
                if type(key) is not str:
                    count("values")
                    continue
                safe_key = key.encode("utf-8", "replace").decode("utf-8")
                if len(safe_key) > 200:
                    count("text_characters", len(safe_key) - 200)
                    safe_key = safe_key[:200]
                result[safe_key] = preview(item, depth + 1)
            return result
        count("values")
        return "[non-JSON value omitted]"

    differences = issues or []
    if len(differences) > _MAX_ITEMS:
        count("issues", len(differences) - _MAX_ITEMS)
    entries = [preview(issue) for issue in differences[:_MAX_ITEMS]]

    def ids(values, name):
        # 控制器传入的ID集合已受目录预算约束；固定上限防止诊断放大输出。
        result = []
        sampled = list(islice(values, _MAX_ITEMS + 1))
        if len(sampled) > _MAX_ITEMS:
            count(name, len(values) - _MAX_ITEMS if isinstance(values, Sized) else 1)
        for value in sampled[:_MAX_ITEMS]:
            # 允许列表只保留完整身份，不把截断文本或省略标记伪装成有效ID。
            if type(value) is not str or len(value.encode("utf-8", "replace")) > 1000:
                count(name)
            else:
                result.append(value.encode("utf-8", "replace").decode("utf-8"))
        return result

    result: m.JsonObject = {
        "issues": entries,
        "allowed_evidence_ids": ids(allowed_evidence_ids, "allowed_evidence_ids"),
        "allowed_source_ids": ids(allowed_source_ids, "allowed_source_ids"),
        "rejected_output": preview(rejected_output),
        "omitted": omitted,
    }
    # 每个分支都删除一个有限项或整个预览；无需依赖输入长度的反复截断循环。
    while _size(result) > max_bytes:
        if result["rejected_output"] is not None:
            result["rejected_output"] = None
            omitted["rejected_output"] = True
        elif result["allowed_source_ids"]:
            result["allowed_source_ids"].pop()
            count("allowed_source_ids")
        elif result["allowed_evidence_ids"]:
            result["allowed_evidence_ids"].pop()
            count("allowed_evidence_ids")
        elif result["issues"]:
            result["issues"].pop()
            count("issues")
        else:
            # 固定元数据在最小预算内；此防御分支避免未来新增字段引入死循环。
            return {"issues": [], "omitted": {"diagnostics": True}}
    return result


def reference_error(
    phase: Literal["compose", "verify"],
    output: m.AnswerRecord | m.Verification,
    *,
    allowed_evidence_ids: Iterable[str],
    allowed_source_ids: Iterable[str] = (),
    sanitize: Callable[[m.JsonValue], m.JsonValue] | None = None,
) -> m.ErrorInfo | None:
    """定位每个非法证据/来源编号；空引用合法，不用来源编号冒充证据编号。"""
    evidence_ids = sorted(set(allowed_evidence_ids))
    source_ids = sorted(set(allowed_source_ids))
    evidence_set, source_set = set(evidence_ids), set(source_ids)
    issues = []
    evidence_invalid = False

    def check(values, allowed, prefix, kind):
        nonlocal evidence_invalid
        for index, identity in enumerate(values):
            if identity in allowed:
                continue
            evidence_invalid |= kind == "evidence"
            issues.append(
                {
                    "path": f"{prefix}/{index}",
                    "code": f"unknown_{kind}_id",
                    "actual": identity,
                    "expected": f"{'an' if kind == 'evidence' else 'a'} {kind}_id from allowed_{kind}_ids",
                }
            )

    if phase == "compose" and isinstance(output, m.AnswerRecord):
        check(output.citations, evidence_set, "/citations", "evidence")
    elif phase == "verify" and isinstance(output, m.Verification):
        for index, issue in enumerate(output.issues):
            check(issue.evidence_ids, evidence_set, f"/issues/{index}/evidence_ids", "evidence")
        if output.recommendation.focus is not None:
            check(
                output.recommendation.focus.source_ids,
                source_set,
                "/recommendation/focus/source_ids",
                "source",
            )
    else:
        raise ValueError("output type does not match content phase")
    if not issues:
        return None
    rejected = output.model_dump(mode="json")
    if sanitize is not None:
        # 必须先脱敏再截断，避免一个私有值跨预览边界后仅留下泄露的前缀。
        issues = sanitize(issues)
        rejected = sanitize(rejected)
    return m.ErrorInfo(
        code="invalid_model_citations" if evidence_invalid else "invalid_model_sources",
        message="The model returned unknown evidence citations."
        if evidence_invalid
        else "The model returned unknown source references.",
        details=build_diagnostics(
            issues=issues,
            allowed_evidence_ids=evidence_ids,
            allowed_source_ids=source_ids,
            rejected_output=rejected,
        ),
    )
