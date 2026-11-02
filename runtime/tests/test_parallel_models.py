"""严格显式启用的元数据与有界并发配置的回归测试。"""

import pytest
from pydantic import ValidationError

from lake_agent.demo import DemoData
from lake_agent.models import RunConfig, ToolSpec
from lake_agent.validation import validate_tool_specs


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
