"""共享运行模型、工具声明与安全并行参数的严格类型回归。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import urllib.request

import pytest
from pydantic import ValidationError

from lake_agent.models import (
    ErrorInfo,
    ModuleResult,
    RunConfig,
    SourceRecord,
    Subtask,
    ToolSpec,
    Usage,
    Verification,
)
from lake_agent.validation import (
    canonical_json,
    catalog_digest,
    source_ids_for,
    validate_dag,
    validate_tool_specs,
)
from tests.support.ports import DemoData


def spec(**updates):
    values = dict(
        name="extract",
        description="Read source",
        schema_version="1.0",
        idempotent=True,
        uses_sources=True,
        source_id_paths=["/source_id"],
        input_schema={
            "type": "object",
            "properties": {"source_id": {"type": "string"}},
            "required": ["source_id"],
            "additionalProperties": False,
        },
    )
    return ToolSpec(**(values | updates))


def test_config_rejects_coercion_unknown_and_nonfinite_values():
    for values in (
        {"max_data_calls": "2"},
        {"max_data_calls": 0},
        {"max_retries_per_data_operation": -1},
        {"max_a_calls": 3},
        {"max_feedback_rounds": 0},
        {"fixed_extract_max_chars": 12000},
        {"model_params": {"temperature": float("nan")}},
    ):
        with pytest.raises(ValidationError):
            RunConfig(**values)
    assert RunConfig(max_observation_reads=0).max_observation_reads == 0


def test_envelope_success_and_error_cannot_disagree():
    with pytest.raises(ValidationError):
        ModuleResult[str](
            call_id="c", status="error", data="wrong", error=ErrorInfo(code="bad", message="bad")
        )
    with pytest.raises(ValidationError):
        ModuleResult[str](call_id="c", status="ok", data=None)
    with pytest.raises(ValidationError):
        Usage(provider_requests=2)
    assert Usage.local().input_tokens == 0
    assert Usage(provider_requests=1).input_tokens is None


def test_verification_action_constraints_are_structural():
    for payload in (
        {
            "verdict": "supported",
            "recommendation": {"action": "search_more", "focus": {"query": "q"}},
        },
        {"verdict": "invalid_citation", "recommendation": {"action": "none"}},
        {
            "verdict": "insufficient",
            "recommendation": {"action": "query_again", "focus": {"query": "q"}},
        },
    ):
        with pytest.raises(ValidationError):
            Verification.model_validate(payload)


def test_search_feedback_requires_an_explicit_subtask_target():
    with pytest.raises(ValidationError, match="subtask_id"):
        Verification.model_validate(
            {
                "verdict": "insufficient",
                "recommendation": {"action": "search_more", "focus": {"query": "q"}},
            }
        )
    valid = Verification.model_validate(
        {
            "verdict": "insufficient",
            "recommendation": {
                "action": "search_more",
                "focus": {"subtask_id": "subtask-2", "query": "q"},
            },
        }
    )
    assert valid.recommendation.focus.subtask_id == "subtask-2"


def test_canonical_json_and_digest_ignore_source_order():
    a = SourceRecord(source_id="a", name="a", format="text", summary="A", metadata={})
    b = a.model_copy(update={"source_id": "b"})
    assert catalog_digest([a, b]) == catalog_digest([b, a])
    assert canonical_json({"z": 1, "a": "中文"}) == '{"a":"中文","z":1}'.encode()
    with pytest.raises((ValueError, TypeError)):
        canonical_json({"bad": float("inf")})


def test_wildcard_source_paths_and_json_pointer_escaping():
    tool = spec(
        source_id_paths=["/sources~1list/*/id~0key"],
        input_schema={
            "type": "object",
            "required": ["sources/list"],
            "properties": {
                "sources/list": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["id~key"],
                        "properties": {"id~key": {"type": "string"}},
                    },
                },
            },
        },
    )
    # 注册表将可信能力元数据与适配器拥有的实例分离，
    # 避免实例后续修改改变授权与安全属性。
    registered = validate_tool_specs([tool])["extract"]
    assert registered == tool
    assert registered is not tool
    assert source_ids_for(tool, {"sources/list": [{"id~key": "a"}, {"id~key": "b"}]}) == {"a", "b"}


def test_source_ids_for_rejects_remote_refs_without_fetching(monkeypatch):
    calls = []

    def fail_if_called(request, *args, **kwargs):
        calls.append(request)
        raise AssertionError("remote schema resolution was attempted")

    monkeypatch.setattr(urllib.request, "urlopen", fail_if_called)
    tool = spec(
        input_schema={
            "$ref": "https://example.invalid/remote-schema.json",
        }
    )
    with pytest.raises(ValueError, match="schema"):
        source_ids_for(tool, {"source_id": "s"})
    assert calls == []


def test_wildcard_source_paths_require_a_nonempty_array():
    tool = spec(
        source_id_paths=["/source_ids/*"],
        input_schema={
            "type": "object",
            "properties": {"source_ids": {"type": "array", "items": {"type": "string"}}},
            "required": ["source_ids"],
            "additionalProperties": False,
        },
    )
    with pytest.raises(ValueError, match="nonempty"):
        validate_tool_specs([tool])


@pytest.mark.parametrize(
    "updates",
    [
        {"source_id_paths": []},
        {"uses_sources": False},
        {"source_id_paths": ["/missing"]},
        {"source_id_paths": ["/source_id", "/source_id"]},
        {"input_schema": {"$ref": "https://example.invalid/schema.json"}},
        {"input_schema": {"type": "wrong"}},
    ],
)
def test_tool_registration_rejects_bad_metadata_and_remote_refs(updates):
    with pytest.raises(ValueError):
        validate_tool_specs([spec(**updates)])


def test_source_paths_validate_actual_arguments():
    tool = spec()
    assert source_ids_for(tool, {"source_id": "s"}) == {"s"}
    for args in ({}, {"source_id": 5}, {"source_id": ""}):
        with pytest.raises(ValueError):
            source_ids_for(tool, args)


def test_dag_is_stable_and_rejects_missing_cycles_duplicates():
    def task(key, deps=()):
        return Subtask(subtask_id=key, objective="Read", search_query="q", depends_on=list(deps))

    a, b, c = task("a"), task("b", ["a"]), task("c")
    assert validate_dag([b, a, c], "subtask_id") == [a, b, c]
    for items in ([a, a], [task("b", ["absent"])], [task("a", ["b"]), task("b", ["a"])]):
        with pytest.raises(ValueError):
            validate_dag(items, "subtask_id")


def test_removed_fixed_retrieval_mode_is_rejected():
    """已删除的固定流程不能借由核心请求模型重新进入执行链路。"""
    from lake_agent.models import RunRequest

    with pytest.raises(ValidationError):
        RunRequest(task_id="retired", question="Inspect", mode="fixed_retrieval")


def test_parallel_defaults_preserve_legacy_tool_safety():
    spec = DemoData().tool_specs()[0]
    assert spec.parallel_safe is False
    assert spec.exclusive_resource_keys == []
    assert RunConfig().max_parallel_tool_calls == 4


@pytest.mark.parametrize("value", [0, -1, 33, True, 1.5, "2"])
def test_parallel_limit_is_a_bounded_strict_integer(value):
    with pytest.raises(ValidationError):
        RunConfig(max_parallel_tool_calls=value)


def test_parallel_capability_and_resource_keys_are_strict():
    payload = DemoData().tool_specs()[0].model_dump(mode="json")
    with pytest.raises(ValidationError):
        ToolSpec.model_validate(payload | {"parallel_safe": "true"})
    with pytest.raises(ValidationError):
        ToolSpec.model_validate(payload | {"exclusive_resource_keys": [""]})


def test_duplicate_exclusive_keys_are_rejected_before_any_dispatch():
    payload = DemoData().tool_specs()[0].model_dump(mode="json")
    spec = ToolSpec.model_validate(payload | {"exclusive_resource_keys": ["engine", "engine"]})
    with pytest.raises(ValueError, match="resource"):
        validate_tool_specs([spec])


def test_registry_revalidates_adapter_model_copies_before_trusting_parallel_flag():
    spec = DemoData().tool_specs()[0].model_copy(update={"parallel_safe": "true"})
    with pytest.raises(ValueError):
        validate_tool_specs([spec])


def test_registered_capabilities_are_detached_from_later_adapter_mutation():
    spec = DemoData().tool_specs()[0]
    registered = validate_tool_specs([spec])[spec.name]
    spec.parallel_safe = True
    spec.exclusive_resource_keys.append("late-resource")
    spec.input_schema["properties"]["source_id"]["type"] = "number"
    assert registered.parallel_safe is False
    assert registered.exclusive_resource_keys == []
    assert registered.input_schema["properties"]["source_id"]["type"] == "string"
