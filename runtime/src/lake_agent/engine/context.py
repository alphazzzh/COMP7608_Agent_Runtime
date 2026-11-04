"""有界的模型工作上下文，与完整审计记录相互独立。

设计参考了所提供 Claude Code 资料中的结果持久化和请求前预算控制。
完整结果仍由控制器持有；本模块的纯函数返回独立的 JSON 预览和本地 JSON Pointer 页面。
限制按序列化后的 UTF-8 字节计算，不估算 token 数或服务商用量。
"""

from __future__ import annotations

import json
import re

from ..models import EvidenceRecord, JsonValue
from ..validation import canonical_json

_OBSERVATION_KEYS = frozenset(
    {
        "observation_id",
        "call_id",
        "operation_id",
        "attempt",
        "step_id",
        "subtask_id",
        "tool_name",
        "source_ids",
        "args",
        "artifact_ref",
        "result",
        "status",
    }
)


def _preview(value: JsonValue, limit: int) -> JsonValue:
    """保留较小的结构化值，并明确标记被截取的 JSON 文本。"""
    encoded = canonical_json(value)
    if len(encoded) <= limit:
        return json.loads(encoded)
    text = encoded.decode("utf-8")
    result = {"truncated": True, "text": "", "total_bytes": len(encoded)}
    lo, hi = 0, min(len(text), limit)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        result["text"] = text[:mid]
        if len(canonical_json(result)) <= limit:
            lo = mid
        else:
            hi = mid - 1
    result["text"] = text[:lo]
    return result


def _observation(item: dict, limit: int) -> dict:
    """保留稳定的来源信息，避免大体积数据挤占反馈空间。"""
    result = item["result"]
    projected = {
        key: value
        for key, value in item.items()
        if key in _OBSERVATION_KEYS and key not in {"args", "result"}
    }
    projected["status"] = result.get("status", item.get("status"))
    projected["args"] = _preview(item.get("args", {}), limit)
    projected["result"] = {
        "status": result.get("status"),
        "data": _preview(result.get("data"), limit),
        "error": _preview(result.get("error"), limit),
        "warnings": _preview(result.get("warnings", []), limit),
        "truncated": result.get("truncated", False),
    }
    return json.loads(canonical_json(projected))


def _evidence(item: EvidenceRecord, limit: int) -> dict:
    """预览保留证据 ID，但始终不能替代原始证据。"""
    return {
        "evidence_id": item.evidence_id,
        "source_id": item.source_id,
        "content_type": item.content_type,
        "payload": _preview(item.payload, limit),
        "locator": _preview(item.locator, limit),
        "warnings": _preview(item.warnings, limit),
    }


def read_json_page(
    value: JsonValue,
    *,
    pointer: str,
    offset: int,
    max_chars: int,
    max_bytes: int,
) -> dict:
    """按 RFC 6901 定位并分页读取规范 JSON，在 JSON 转义后检查大小上限。

    本函数不访问路径、URL、文件或对象属性。空指针选择整个结果。
    偏移量按 Unicode 字符而非字节计算；客户端可拼接各页文本，并在末页到达后解析 JSON。
    """
    if (
        not isinstance(pointer, str)
        or (pointer and not pointer.startswith("/"))
        or re.search(r"~(?![01])", pointer)
        or type(offset) is not int
        or offset < 0
        or type(max_chars) is not int
        or not 1 <= max_chars <= 4096
        or type(max_bytes) is not int
        or max_bytes <= 0
    ):
        raise ValueError("invalid JSON page parameters")
    selected = value
    for token in pointer.split("/")[1:] if pointer else ():
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(selected, dict) and token in selected:
            selected = selected[token]
        elif isinstance(selected, list) and re.fullmatch(r"0|[1-9][0-9]*", token):
            # 转换不可信输入前，先限制其十进制表示的长度。
            if len(token) > len(str(len(selected))) or int(token) >= len(selected):
                raise ValueError("JSON pointer array index is out of range")
            selected = selected[int(token)]
        else:
            raise ValueError("JSON pointer does not resolve")
    text = canonical_json(selected).decode("utf-8")
    if offset > len(text):
        raise ValueError("JSON page offset exceeds selected value")

    def page(length: int) -> dict:
        end = offset + length
        return {
            "pointer": pointer,
            "offset": offset,
            "text": text[offset:end],
            "next_offset": end if end < len(text) else None,
            "total_chars": len(text),
            "truncated": end < len(text),
        }

    lo, hi = 0, min(max_chars, len(text) - offset)
    if len(canonical_json(page(hi))) <= max_bytes:
        return page(hi)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if len(canonical_json(page(mid))) <= max_bytes:
            lo = mid
        else:
            hi = mid - 1
    result = page(lo)
    if len(canonical_json(result)) > max_bytes or (lo == 0 and offset < len(text)):
        raise ValueError("JSON page metadata or one character exceeds byte budget")
    return result
