"""将核心已保存的结果压成有界交接；不调用模型，也不复制证据身份。"""

from __future__ import annotations

import json

from .. import models as m
from .models import MissionHandoff


def _fit(fields: dict, *, maximum: int = 4096) -> dict:
    """按完整 UTF-8 JSON 缩减正文，保留身份并显式标记截断。

    输入来自已验证结果或严格 MissionHandoff，集合最多八项；对数次缩短
    不会扫描整个历史，更不会在字符中间切断 UTF-8 字节。
    """
    while (
        len(
            json.dumps(fields, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        > maximum
    ):
        fields["truncated"] = True
        if fields["summary"]:
            fields["summary"] = fields["summary"][: len(fields["summary"]) // 2]
        elif fields["pending_items"]:
            fields["pending_items"].pop()
        elif fields["artifact_refs"]:
            fields["artifact_refs"].pop()
        else:
            raise ValueError("continuation identity exceeds the byte limit")
    return fields


def from_snapshot(snapshot: dict) -> MissionHandoff:
    """只从真实核心终态和已确认计划提取线索，未知结果不能生成成功交接。"""
    result = m.RunResult.model_validate(snapshot["result"])
    text = f"上轮状态：{result.status}；停止原因：{result.stop_reason}。"
    if result.answer is not None:
        text += "\n上轮答案摘录（本轮仍需重新核实）：\n" + result.answer.text
    pending = []
    shortened_objective = False
    progress = snapshot.get("progress", {})
    done = set(progress.get("completed_subtasks", []))
    for raw in progress.get("subtasks", []):
        task = m.Subtask.model_validate(raw)
        if task.subtask_id not in done:
            pending.append(task.objective[:256])
            shortened_objective |= len(task.objective) > 256
    if not pending and result.status != "completed":
        pending.append("原目标尚未完成；根据上轮停止原因重新检查缺失的数据和证据。")
    fields = {
        "summary": text[:2000],
        "pending_items": pending[:8],
        "previous_run_id": result.run_id,
        # run 引用仅作定位线索，不向新一轮注册历史 evidence_id 或文件路径。
        "artifact_refs": [f"run:{result.run_id}"[:200]],
        "truncated": (
            len(text) > 2000
            or len(pending) > 8
            or shortened_objective
            or len(f"run:{result.run_id}") > 200
        ),
    }
    return MissionHandoff.model_validate(_fit(fields))


def continuation(mission_id: str, cycle: int, handoff: dict) -> m.RunContinuation:
    """将领取时冻结的交接纳入新的原始请求，身份开销也计入4096字节。"""
    frozen = MissionHandoff.model_validate(handoff).model_dump(mode="json")
    fields = {"mission_id": mission_id, "cycle": cycle, **frozen}
    return m.RunContinuation.model_validate(_fit(fields))
