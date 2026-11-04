"""自主决策的有界工作上下文；完整目录、答案和工具结果仍由控制器持有。

所有预览按规范 JSON 的 UTF-8 字节计量，截断和省略显式标记。
此模块不执行适配器、不生成正文证据，不加入时钟或恢复时会变化的计数。
"""

from __future__ import annotations

import json

from ..validation import canonical_json
from .context import _evidence, _observation, _preview
from .errors import RunStopped


def catalog_page(controller, offset: int, limit: int, *, max_bytes: int | None = None) -> dict:
    """按稳定 source_id 顺序分页目录；名称和摘要只描述目录，不充当文档证据。"""
    sources = sorted(controller.sources.values(), key=lambda source: source.source_id)
    if offset > len(sources):
        raise RunStopped("failed", "invalid_catalog_read")
    budget = min(
        controller.cfg.planner_preview_bytes,
        max(256, controller.cfg.max_planner_context_bytes // 3),
    )
    if max_bytes is not None:
        budget = min(budget, max_bytes)
    page = {"offset": offset, "sources": [], "total": len(sources), "next_offset": None}
    # 先为省略标记预留空间，较小预算至少可保留完整source_id/format。
    page["metadata_omitted"] = True
    metadata_omitted = False
    for source in sources[offset : offset + limit]:
        value = source.model_dump(mode="json")
        # 标识符保持完整，较大的描述字段允许明确的预览；不能伪造缩短的 source_id。
        projected = {"source_id": value["source_id"], "format": value["format"]}
        page["sources"].append(projected)
        end = offset + len(page["sources"])
        page["next_offset"] = end if end < len(sources) else None
        if len(canonical_json(page)) > budget:
            page["sources"].pop()
            break
        for key in ("name", "summary", "metadata", "version", "content_hash"):
            projected[key] = _preview(value.get(key), max(64, budget // 8))
            if len(canonical_json(page)) > budget:
                projected.pop(key)
                metadata_omitted = True
    end = offset + len(page["sources"])
    page["next_offset"] = end if end < len(sources) else None
    if not metadata_omitted:
        page.pop("metadata_omitted")
    if offset < len(sources) and not page["sources"]:
        raise RunStopped("budget_exceeded", "catalog_context_limit")
    return page


def _base_context(controller, state: dict) -> dict:
    """构造所有不可丢失字段；分页预算与最终上下文使用同一个确定性框架。"""
    c = controller
    # 反馈的错误码与调用身份必须保留为结构化字段，不能被普通JSON文本预览吞掉。
    # 较大的诊断稍后独立预览；完整反馈仍在下一次内容请求和审计工件中。
    feedbacks = [
        {
            "phase": feedback.phase,
            "call_id": feedback.call_id,
            "error": {
                "code": feedback.error.code,
            },
        }
        for feedback in c.content_feedback.values()
    ]
    result = {
        "catalog": {"total": len(c.sources), "sources": [], "omitted": len(c.sources)},
        "plan": [],
        "completed_subtask_ids": [],
        "candidates": [],
        "evidence_summary": [],
        "tool_observations": [],
        "answer": None,
        "has_answer": c.answer is not None,
        "verification": None,
        "verification_verdict": None,
        "verification_status": "missing",
        "last_action": None,
        "catalog_page": None,
        "observation_read": None,
        "context_status": {
            "evidence": {"total": len(c.evidence), "included": 0, "omitted": len(c.evidence)},
            "observations": {
                "total": len(c.observations),
                "included": 0,
                "omitted": len(c.observations),
            },
            "omitted_fields": [],
        },
    }
    if feedbacks:
        result["content_feedback"] = feedbacks
        # 先为一个固定省略标记预留位置，避免最后补标记反而挤掉合法读取页。
        result["context_status"]["omitted_fields"].append("content_feedback.preview")
    verification = state.get("verification")
    status = (
        c._verification_status()
        if hasattr(c, "_verification_status")
        else ("current" if verification is not None else "missing")
    )
    result["verification_status"] = status
    if verification is not None and status == "current":
        result["verification_verdict"] = verification.verdict
    return result


def context_page_budget(controller, state: dict, *, observation: bool) -> int:
    """在发页前为反馈身份、原有上下文字段和省略标记预留实际JSON空间。

    返回值约束独立页本身，不含观察页外层的历史页包装。不能设置最小256字节
    下限：低预算时应在已知边界内发更短的合法页，而不是发完后截断它。
    """
    base = _base_context(controller, state)
    optional = {
        "last_action": True,
        "answer": controller.answer is not None,
        "verification": state.get("verification") is not None,
        "plan": bool(controller.subtasks),
        "completed_subtask_ids": bool(controller.completed_subtasks),
        "candidates": bool(state.get("candidates")),
    }
    base["context_status"]["omitted_fields"].extend(
        key for key, present in optional.items() if present
    )
    available = controller.cfg.max_planner_context_bytes - len(canonical_json(base)) + len(b"null")
    if observation:
        # 逗号与括号差额使用两个保守预留字节，最大读数覆盖计数位数增长。
        available -= (
            len(
                canonical_json(
                    {"previous_pages": [], "omitted_pages": controller.cfg.max_observation_reads}
                )
            )
            + 1
        )
    return min(max(0, available), controller.cfg.max_planner_context_bytes // 3)


def build_agent_context(controller, state: dict) -> dict:
    """在总字节上限内保留反馈身份和完整当前页，其他内容明确预览或省略。"""
    c = controller
    maximum, preview = c.cfg.max_planner_context_bytes, c.cfg.planner_preview_bytes
    result = _base_context(c, state)
    feedbacks = result.get("content_feedback", [])
    verification = state.get("verification")
    # 为所有可能被省略的非空工作记忆预留标记；成功装入时再移除。
    # 页是否仍能保留也必须按同一个框架计算，避免最后补省略标记才超额。
    values = {
        "last_action": state.get("last_action"),
        "answer": c.answer.model_dump(mode="json") if c.answer else None,
        "verification": verification.model_dump(mode="json") if verification else None,
        "plan": [task.model_dump(mode="json") for task in c.subtasks],
        "completed_subtask_ids": [task.subtask_id for task in c.completed_subtasks],
        "candidates": [item.model_dump(mode="json") for item in state.get("candidates", [])],
    }
    reserved = {key for key, value in values.items() if value not in (None, [])}
    result["context_status"]["omitted_fields"].extend(sorted(reserved))
    # 页的 next_offset 根据实际 text 计算，不能再对整个页面做二次文本预览。
    result["observation_read"] = json.loads(canonical_json(state.get("read_result")))
    result["catalog_page"] = json.loads(canonical_json(state.get("catalog_page")))
    page = result["observation_read"]
    while len(canonical_json(result)) > maximum and page and page.get("previous_pages"):
        page["previous_pages"].pop(0)
        page["omitted_pages"] += 1
    last_kind = (state.get("last_action") or {}).get("kind")
    if (
        len(canonical_json(result)) > maximum
        and last_kind is not None
        and last_kind not in {"read_observation", "read_catalog"}
    ):
        # 最近读取之后模型已经执行其他动作，旧页不再是当前新观察。完整页保留在审计
        # 和运行内索引，可重新读取；这里明确省略，给新产生的反馈身份让出空间。
        for field in ("observation_read", "catalog_page"):
            if result[field] is not None:
                result[field] = None
                result["context_status"]["omitted_fields"].append(field)
    if len(canonical_json(result)) > maximum:
        raise RunStopped("budget_exceeded", "planner_context_limit")
    # 正文/详情是可选预览，按实际UTF-8 JSON字节试装；错误码与调用身份始终独立。
    # 页已完整装入，中文、反斜线或控制字符均不能通过字符截断绕过上下文预算。
    feedback_omitted = False
    for item, feedback in zip(feedbacks, c.content_feedback.values(), strict=True):
        for key, value in (
            ("message", feedback.error.message),
            ("details", feedback.error.details),
        ):
            if value is None:
                continue
            limit = min(preview, maximum // 4)
            while limit >= 64:
                projected = _preview(value, limit)
                item["error"][key] = projected
                if len(canonical_json(result)) <= maximum:
                    if projected != value:
                        feedback_omitted = True
                    break
                item["error"].pop(key)
                limit //= 2
            else:
                feedback_omitted = True
    if feedbacks and not feedback_omitted:
        result["context_status"]["omitted_fields"].remove("content_feedback.preview")
    for key, value in values.items():
        limit = min(preview, maximum // 3)
        while True:
            candidate = _preview(value, limit)
            original = result[key]
            result[key] = candidate
            omitted = result["context_status"]["omitted_fields"]
            if key in reserved:
                omitted.remove(key)
            if len(canonical_json(result)) <= maximum:
                break
            result[key] = original
            if key in reserved:
                omitted.append(key)
            if limit <= 64:
                break
            limit = max(64, limit // 2)
    # omitted_fields 自身也占字节。完整页优先，必要时回收已经装入的
    # 可选预览，而不是因几个省略标签把合法页面变成一次预算失败。
    for key in (
        "candidates",
        "completed_subtask_ids",
        "plan",
        "answer",
        "verification",
        "last_action",
    ):
        if len(canonical_json(result)) <= maximum:
            break
        empty = [] if key in {"candidates", "completed_subtask_ids", "plan"} else None
        if result[key] != empty:
            result[key] = empty
            omitted = result["context_status"]["omitted_fields"]
            if key not in omitted:
                omitted.append(key)
    page = result["observation_read"]
    # 过去的页面可省略，当前请求页的 text 和 next_offset 必须原样保留。
    while len(canonical_json(result)) > maximum and page and page.get("previous_pages"):
        page["previous_pages"].pop(0)
        page["omitted_pages"] += 1
    if len(canonical_json(result)) > maximum:
        raise RunStopped("budget_exceeded", "planner_context_limit")
    entries = []
    for index in range(max(len(c.observations), len(c.evidence))):
        if index < len(c.observations):
            entries.append(
                ("tool_observations", "observations", c.observations[-1 - index], _observation)
            )
        if index < len(c.evidence):
            entries.append(("evidence_summary", "evidence", c.evidence[-1 - index], _evidence))
    for key, group, value, projector in entries:
        limit = preview
        while True:
            result[key].append(projector(value, limit))
            counts = result["context_status"][group]
            counts["included"] += 1
            counts["omitted"] -= 1
            if len(canonical_json(result)) <= maximum:
                break
            result[key].pop()
            counts["included"] -= 1
            counts["omitted"] += 1
            if limit <= 64:
                if key == "tool_observations":
                    # 最小上下文中，核验状态可能挤掉完整观察预览。至少尝试保留
                    # 可读取的真实索引，明确省略正文，不能让已知结果变成不可发现。
                    index = {
                        "observation_id": value["observation_id"],
                        "tool_name": value["tool_name"],
                        "source_ids": value["source_ids"],
                        "status": value["status"],
                        "preview_omitted": True,
                    }
                    result[key].append(index)
                    counts["included"] += 1
                    counts["omitted"] -= 1
                    if len(canonical_json(result)) > maximum:
                        result[key].pop()
                        counts["included"] -= 1
                        counts["omitted"] += 1
                break
            limit = max(64, limit // 2)
    for source in sorted(c.sources.values(), key=lambda item: item.source_id):
        value = {
            "source_id": source.source_id,
            "name": source.name,
            "format": source.format,
            "summary": _preview(source.summary, min(preview, maximum // 4)),
        }
        result["catalog"]["sources"].append(value)
        result["catalog"]["omitted"] -= 1
        if len(canonical_json(result)) > maximum:
            result["catalog"]["sources"].pop()
            result["catalog"]["omitted"] += 1
    if len(canonical_json(result)) > maximum:
        raise RunStopped("budget_exceeded", "planner_context_limit")
    return result
