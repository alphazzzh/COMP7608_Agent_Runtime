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
    metadata_omitted = False
    for index, source in enumerate(sources[offset : offset + limit], start=offset):
        previous_offset = page["next_offset"]
        page["next_offset"] = index + 1 if index + 1 < len(sources) else None
        # 优先保证完整 source_id 能装入极小页面，不被预留的 metadata_omitted 挤走。
        projected = {"source_id": source.source_id}
        page["sources"].append(projected)
        if len(canonical_json(page)) > budget:
            page["sources"].pop()
            if page["sources"] or page.get("omitted_sources"):
                # 先前的行或省略提示已占用空间，让后续 ID 在新页独立试装。
                page["next_offset"] = previous_offset
                break
            # 页首标识本身也放不下时只跳过该行，保留可继续读取的精确偏移。
            page["omitted_sources"] = 1
            if len(canonical_json(page)) > budget:
                raise RunStopped("budget_exceeded", "catalog_context_limit")
            break
        value = source.model_dump(mode="json")
        added_keys: list[str] = []
        row_omitted = False
        for key in ("format", "name", "summary", "metadata", "version", "content_hash"):
            projected[key] = (
                value[key] if key == "format" else _preview(value[key], max(64, budget // 8))
            )
            if key == "format" and len(canonical_json(page)) > budget:
                projected[key] = _preview(value[key], max(64, budget // 8))
            if len(canonical_json(page)) > budget:
                projected.pop(key)
                row_omitted = True
            else:
                added_keys.append(key)
        if row_omitted:
            was_omitted = metadata_omitted
            metadata_omitted = True
            page["metadata_omitted"] = True
            while len(canonical_json(page)) > budget and added_keys:
                projected.pop(added_keys.pop())
            if len(canonical_json(page)) > budget:
                # 无法诚实携带 metadata_omitted 标记时，不静默返回缺字段的条目。
                page["sources"].pop()
                metadata_omitted = was_omitted
                if not metadata_omitted:
                    page.pop("metadata_omitted", None)
                if page["sources"] or page.get("omitted_sources"):
                    page["next_offset"] = previous_offset
                    break
                page["omitted_sources"] = 1
                if len(canonical_json(page)) > budget:
                    raise RunStopped("budget_exceeded", "catalog_context_limit")
                break
    if not metadata_omitted:
        page.pop("metadata_omitted", None)
    if len(canonical_json(page)) > budget:
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
    if state.get("search_history"):
        result["search_history"] = []
    if c.req.continuation is not None:
        # 原始 RunRequest 已在 run 准入时深复制并在持久运行中保存。
        # 交接属于不可丢失输入，在任何试装、分页和最终预算检查前纳入框架；
        # 只导出独立 JSON 数据，不导入历史 EvidenceRecord 或 Observation。
        result["continuation"] = c.req.continuation.model_dump(mode="json")
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
        "search_history": bool(state.get("search_history")),
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
    if state.get("search_history"):
        values["search_history"] = [
            {"query": item["query"], "count": item["count"]}
            for item in state["search_history"][-8:]
        ]
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
    current_bytes = len(canonical_json(result))
    catalog_max_sources = getattr(c.cfg, "planner_catalog_max_sources", None)
    catalog_preview_bytes = getattr(c.cfg, "planner_catalog_preview_bytes", None)
    catalog_limit = (
        min(maximum, current_bytes + catalog_preview_bytes)
        if catalog_preview_bytes is not None
        else maximum
    )
    # 空目录条目 {"format":"","name":"","source_id":"","summary":""} 至少占 49 字节；
    # 剩余空间不足时提前终止，避免大目录下对完整上下文执行 O(N²) 重复序列化。
    for source in sorted(c.sources.values(), key=lambda item: item.source_id):
        if (
            catalog_max_sources is not None
            and len(result["catalog"]["sources"]) >= catalog_max_sources
        ):
            break
        if current_bytes + 48 > catalog_limit:
            break
        value = {
            "source_id": source.source_id,
            "name": source.name,
            "format": source.format,
            "summary": _preview(source.summary, min(preview, maximum // 4)),
        }
        entry_bytes = len(canonical_json(value)) + (1 if result["catalog"]["sources"] else 0)
        # omitted 数值减少最多使数字少占 4 字节，先用增量估算过滤明显超限的条目。
        if current_bytes + entry_bytes > catalog_limit + 4:
            continue
        result["catalog"]["sources"].append(value)
        result["catalog"]["omitted"] -= 1
        updated_bytes = len(canonical_json(result))
        if updated_bytes > catalog_limit:
            result["catalog"]["sources"].pop()
            result["catalog"]["omitted"] += 1
        else:
            current_bytes = updated_bytes
    if current_bytes > maximum:
        raise RunStopped("budget_exceeded", "planner_context_limit")
    return result
