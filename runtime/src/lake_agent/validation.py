"""本地 JSON Schema、数据源授权和依赖关系校验。

所有检查均在工具动作派发前执行。工具参数仍由数据适配器定义，
本模块校验参数声明的模式，并提取每个显式注册的数据源 ID。
远程模式引用会被明确拒绝：校验不可信元数据时绝不能获取 URL 内容。
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from typing import Any, TypeVar

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from pydantic import BaseModel

from .models import Candidate, EvidenceRecord, SourceRecord, Subtask, ToolPlan, ToolSpec, ToolStep

T = TypeVar("T")


def _json_native(value: Any) -> Any:
    """规范化 Pydantic 记录，并拒绝会被 JSON 静默转换类型的值。"""
    if isinstance(value, BaseModel):
        return _json_native(value.model_dump(mode="json"))
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, list):
        return [_json_native(item) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: _json_native(item) for key, item in value.items()}
    raise ValueError("payload contains a non-JSON value")


def canonical_json(value: Any) -> bytes:
    """返回键排序、格式紧凑的 UTF-8 JSON，用于字节限制与可复现哈希。"""
    normalized = _json_native(value)
    try:
        return json.dumps(
            normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (UnicodeError, TypeError, ValueError) as exc:
        raise ValueError("payload cannot be serialized as UTF-8 JSON") from exc


def catalog_digest(sources: Iterable[SourceRecord]) -> str:
    """先按稳定的数据源 ID 排序，再对完整目录计算哈希。"""
    records = sorted(sources, key=lambda source: source.source_id)
    if len({source.source_id for source in records}) != len(records):
        raise ValueError("catalog source IDs must be unique")
    return "sha256:" + hashlib.sha256(canonical_json(records)).hexdigest()


def _tokens(pointer: str) -> list[str]:
    """解码 JSON Pointer 标记；'*' 是用于数组元素的扩展语法。"""
    if not pointer.startswith("/") or re.search(r"~(?![01])", pointer):
        raise ValueError("source path must be a non-root JSON Pointer")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def _check_local_references(node: Any) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in {"$ref", "$dynamicRef", "$recursiveRef"}:
                if not isinstance(value, str) or not value.startswith("#"):
                    raise ValueError("tool schema references must stay within the schema")
            _check_local_references(value)
    elif isinstance(node, list):
        for value in node:
            _check_local_references(value)


def _dereference(node: Any, root: dict[str, Any]) -> dict[str, Any]:
    """解析普通的本地指针引用，避免网络访问与递归循环。"""
    visited: set[str] = set()
    while isinstance(node, dict) and "$ref" in node:
        ref = node["$ref"]
        if not isinstance(ref, str) or not ref.startswith("#/") or ref in visited:
            raise ValueError("source metadata requires nonrecursive local pointer schemas")
        visited.add(ref)
        target: Any = root
        for token in _tokens(ref[1:]):
            if not isinstance(target, dict) or token not in target:
                raise ValueError("tool schema has an unresolved local reference")
            target = target[token]
        if not isinstance(target, dict):
            raise ValueError("source metadata reference must resolve to an object schema")
        # Draft 2020-12 中，引用的同级字段会增加约束。数据源元数据应保持
        # 无歧义，因此直接拒绝结构性同级字段，不猜测约束合并方式。
        if any(key not in {"$ref", "description", "title", "$comment"} for key in node):
            raise ValueError("source path schemas cannot combine $ref and structural siblings")
        node = target
    if not isinstance(node, dict):
        raise ValueError("source path must resolve through typed schema objects")
    return node


def _check_schema_path(schema: dict[str, Any], pointer: str) -> None:
    node: Any = schema
    for token in _tokens(pointer):
        node = _dereference(node, schema)
        if token == "*":
            if node.get("type") != "array" or not isinstance(node.get("items"), dict):
                raise ValueError("wildcard source paths require homogeneous arrays")
            if node.get("minItems", 0) < 1:
                raise ValueError("wildcard source arrays must be nonempty")
            node = node["items"]
        else:
            properties = node.get("properties", {})
            if (
                node.get("type") != "object"
                or not isinstance(properties, dict)
                or token not in properties
                or token not in node.get("required", [])
            ):
                raise ValueError("source paths must follow required object properties")
            node = properties[token]
    leaf = _dereference(node, schema)
    if leaf.get("type") != "string":
        raise ValueError("source path leaf must be a string")


def validate_tool_specs(specs: Iterable[ToolSpec]) -> dict[str, ToolSpec]:
    """在接纳静态能力注册表之前，对整个注册表进行校验。

    数据源路径结构的校验有意保持保守：支持对象属性、同类型元素数组，以及本地 $defs。
    对于条件分支或联合类型中的数据源位置，应拆分为独立工具，
    直到未来的契约版本明确规定条件元数据的语义。
    """
    registered: dict[str, ToolSpec] = {}
    for incoming in specs:
        # model_copy(update=...) 会绕过 Pydantic 校验。信任并发与资源声明前，
        # 须重新校验适配器记录，并创建与原记录独立的副本。
        payload = (
            incoming.model_dump(mode="json", warnings=False)
            if isinstance(incoming, BaseModel)
            else incoming
        )
        spec = ToolSpec.model_validate(payload)
        if spec.name in registered:
            raise ValueError("duplicate tool name")
        schema = spec.input_schema
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as exc:
            raise ValueError("tool input_schema is not valid Draft 2020-12") from exc
        _check_local_references(schema)
        if bool(spec.source_id_paths) != spec.uses_sources:
            raise ValueError("uses_sources must agree with source_id_paths")
        if len(set(spec.source_id_paths)) != len(spec.source_id_paths):
            raise ValueError("source_id_paths must be unique")
        if len(set(spec.exclusive_resource_keys)) != len(spec.exclusive_resource_keys):
            raise ValueError("exclusive resource keys must be unique")
        for pointer in spec.source_id_paths:
            _check_schema_path(schema, pointer)
        registered[spec.name] = spec
    return registered


def source_ids_for(spec: ToolSpec, args: dict[str, Any]) -> set[str]:
    """先校验工具参数，再精确提取注册的数据源 ID。

    通配符只展开数组。值缺失、类型错误或数据源字符串为空时，均拒绝继续。
    调用方将所得集合与 ToolStep.source_ids 及当前候选源和证据的授权集合比较。
    """
    try:
        canonical_json(args)
        Draft202012Validator.check_schema(spec.input_schema)
        _check_local_references(spec.input_schema)
        for pointer in spec.source_id_paths:
            _check_schema_path(spec.input_schema, pointer)
        Draft202012Validator(spec.input_schema).validate(args)
    except (ValidationError, SchemaError) as exc:
        raise ValueError("tool arguments do not match their registered schema") from exc
    except Exception as exc:
        # 注册表通常会先拒绝未解析的引用。适配器直接调用本辅助函数时，
        # 也应获得一致且可预期的本地错误。
        raise ValueError("tool arguments or schema cannot be validated") from exc
    found: set[str] = set()
    for pointer in spec.source_id_paths:
        values: list[Any] = [args]
        for token in _tokens(pointer):
            expanded: list[Any] = []
            for value in values:
                if token == "*" and isinstance(value, list):
                    expanded.extend(value)
                elif token != "*" and isinstance(value, dict) and token in value:
                    expanded.append(value[token])
                else:
                    raise ValueError("source path did not resolve in arguments")
            if token == "*" and not expanded:
                raise ValueError("wildcard source paths must resolve to a nonempty array")
            values = expanded
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError("source IDs must be nonempty strings")
        found.update(values)
    return found


def validate_dag(items: Iterable[T], id_field: str) -> list[T]:
    """返回确定性的深度优先拓扑序，并拒绝无效的有向无环图。

    在依赖关系允许时，保留输入项的原有顺序。
    使用迭代遍历，避免适配器输入触发 Python 递归深度限制。
    """
    records = list(items)
    by_id: dict[str, T] = {}
    for item in records:
        key = getattr(item, id_field)
        if key in by_id:
            raise ValueError("dependency IDs must be unique")
        by_id[key] = item
    dependencies = {getattr(item, id_field): list(item.depends_on) for item in records}
    for key, deps in dependencies.items():
        if len(set(deps)) != len(deps) or key in deps or any(dep not in by_id for dep in deps):
            raise ValueError("invalid dependency reference")
    state: dict[str, int] = {}
    ordered: list[T] = []
    for item in records:
        root = getattr(item, id_field)
        if state.get(root) == 2:
            continue
        stack: list[tuple[str, bool]] = [(root, False)]
        while stack:
            key, finishing = stack.pop()
            if finishing:
                if state.get(key) != 2:
                    state[key] = 2
                    ordered.append(by_id[key])
                continue
            if state.get(key) == 1:
                raise ValueError("dependency graph contains a cycle")
            if state.get(key) == 2:
                continue
            state[key] = 1
            stack.append((key, True))
            stack.extend((dep, False) for dep in reversed(dependencies[key]))
    return ordered


def validate_tool_plan(
    plan: ToolPlan,
    subtask: Subtask,
    candidates: Iterable[Candidate],
    evidence: Iterable[EvidenceRecord],
    tool_specs: Mapping[str, ToolSpec],
    used_step_ids: set[str],
    max_steps: int,
) -> list[ToolStep]:
    """整体校验完整计划，不修改 ID，也不派发工具。

    控制器在调用本辅助函数前，将超预算计划映射为 step_budget；
    独立调用本函数时，超预算会抛出 ValueError。
    """
    if len(plan.steps) > max_steps:
        raise ValueError("tool plan exceeds remaining step budget")
    steps = validate_dag(plan.steps, "step_id")
    authorized = {item.source_id for item in candidates} | {item.source_id for item in evidence}
    for step in steps:
        if step.step_id in used_step_ids or step.subtask_id != subtask.subtask_id:
            raise ValueError("reused step ID or wrong subtask")
        if step.tool_name not in tool_specs:
            raise ValueError("unregistered tool")
        actual = source_ids_for(tool_specs[step.tool_name], step.args)
        if (
            len(set(step.source_ids)) != len(step.source_ids)
            or actual != set(step.source_ids)
            or not actual <= authorized
        ):
            raise ValueError("tool source authorization failed")
    return steps
