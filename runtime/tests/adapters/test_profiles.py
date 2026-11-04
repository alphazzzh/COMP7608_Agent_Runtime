"""正式和隔离 profile 的注册、资源所有权、工厂与恢复身份。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import httpx
import pytest

from lake_agent import models as m
from lake_agent.adapters.planner import JsonPlanner
from lake_agent.config import ConfigError, load_config
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink
from lake_agent.web.profiles import ExecutionProfile, ProfileMetadata, ProfileRegistry
from lake_agent.web.storage import WebError
from tests.support.configuration import config_text, write_config


class EmptyData:
    """提供真实控制器需要的最小空目录，不伪造证据。"""

    async def catalog(self, request):
        return m.CatalogResult(call_id=request.call_id, catalog_snapshot_id="empty-v1")

    def tool_specs(self):
        return []

    async def invoke(self, request):
        raise AssertionError("The empty catalog has no callable tools")


class EmptyContent:
    """只注册与检索空目录；无证据流程不会触发生成或验证。"""

    async def prepare_catalog(self, request):
        return m.ModuleResult(
            call_id=request.call_id,
            status="ok",
            data=m.CatalogHandle(
                catalog_snapshot_id=request.catalog_snapshot_id,
                catalog_digest=request.catalog_digest,
                handle="empty-handle",
            ),
            usage=m.Usage.local(),
            duration_ms=0,
        )

    async def search(self, request):
        return m.ModuleResult(
            call_id=request.call_id,
            status="ok",
            data=m.SearchResponse(),
            usage=m.Usage.local(),
            duration_ms=0,
        )

    async def compose(self, request):
        raise AssertionError("An empty catalog cannot support an answer")

    async def verify(self, request):
        raise AssertionError("An empty catalog cannot support an answer")


@pytest.fixture
def factories(monkeypatch):
    """把测试工厂显式注入可信模块表，不写入产品包或访问网络。"""
    module = ModuleType("configured_test_adapters")
    events = []

    @asynccontextmanager
    async def data(*, options, llm):
        instance = EmptyData()
        events.append(("data-enter", instance, options, llm))
        try:
            yield instance
        finally:
            events.append(("data-exit", instance))

    @asynccontextmanager
    async def content(*, options, llm):
        instance = EmptyContent()
        events.append(("content-enter", instance, options, llm))
        try:
            yield instance
        finally:
            events.append(("content-exit", instance))

    module.data = data
    module.content = content
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return module, events


def api():
    return importlib.import_module("lake_agent.web.configuration")


def load(tmp_path, text=None):
    module = importlib.import_module("lake_agent.config")
    return module.load_config(write_config(tmp_path, text))


async def test_configured_profile_is_private_connected_and_uses_fresh_clients(tmp_path, factories):
    _, events = factories
    profile = api().configured_profile(load(tmp_path))
    assert events == []
    assert profile.sources == ()
    public = profile.public()
    assert public["kind"] == "project"
    assert public["ready"] is True
    assert public["source_count"] == 0
    assert {item["status"] for item in public["modules"].values()} == {"connected"}
    assert "binding_id" not in public
    assert "private-" not in json.dumps(public)
    clients = []
    for _ in range(2):
        async with profile.ports_factory() as ports:
            clients.append(ports)
            assert ports.planner._closed is False
        assert ports.planner._closed is True
        assert ports.planner._client.is_closed
    assert clients[0].data is not clients[1].data
    assert clients[0].content is not clients[1].content
    assert clients[0].planner is not clients[1].planner
    assert [event[0] for event in events] == [
        "data-enter",
        "content-enter",
        "content-exit",
        "data-exit",
        "data-enter",
        "content-enter",
        "content-exit",
        "data-exit",
    ]


async def test_factories_receive_independent_deep_options_and_llm(tmp_path, factories):
    _, events = factories
    config = load(tmp_path)
    profile = api().configured_profile(config)
    # 注册以后外部修改 AppConfig，不得改变已绑定 profile 的实际连接。
    config.adapters.data.options["nested"]["password"] = "changed-after-registration"
    config.llm.model_params["seed"] = 100
    for _ in range(2):
        async with profile.ports_factory():
            entries = [entry for entry in events if entry[0].endswith("-enter")]
            data, content = entries[-2:]
            assert data[2]["nested"]["password"] == "private-adapter-option"
            assert data[3].model_params == {"temperature": 0.25, "seed": 7}
            assert data[3] is not content[3]
            data[2]["nested"]["password"] = "mutated-in-one-run"
            data[3].model_params["seed"] = -1
            assert content[3].model_params["seed"] == 7


@pytest.mark.parametrize("failure", ["raise", "context", "shape"])
async def test_second_adapter_failure_closes_already_entered_data(tmp_path, factories, failure):
    module, events = factories
    if failure == "raise":

        def broken(*, options, llm):
            raise RuntimeError("private-factory-detail")

        module.content = broken
    elif failure == "context":

        @contextmanager
        def synchronous(*, options, llm):
            yield EmptyContent()

        module.content = synchronous
    else:

        @asynccontextmanager
        async def invalid(*, options, llm):
            events.append(("invalid-enter",))
            try:
                yield object()
            finally:
                events.append(("invalid-exit",))

        module.content = invalid
    profile = api().configured_profile(load(tmp_path))
    with pytest.raises(importlib.import_module("lake_agent.config").ConfigError) as error:
        async with profile.ports_factory():
            pytest.fail("invalid adapters must fail before execution")
    assert events[0][0] == "data-enter"
    assert events[-1][0] == "data-exit"
    if failure == "shape":
        assert "invalid-exit" in [entry[0] for entry in events]
    assert "private-factory-detail" not in str(error.value)


@pytest.mark.parametrize("method", ["catalog", "invoke", "tool_specs"])
async def test_wrong_async_method_shape_is_rejected_and_cleaned(tmp_path, factories, method):
    module, events = factories

    @asynccontextmanager
    async def invalid(*, options, llm):
        instance = EmptyData()
        if method == "tool_specs":

            async def wrong():
                return []
        else:

            def wrong(request):
                return None

        setattr(instance, method, wrong)
        try:
            yield instance
        finally:
            events.append(("invalid-data-exit",))

    module.data = invalid
    profile = api().configured_profile(load(tmp_path))
    with pytest.raises(importlib.import_module("lake_agent.config").ConfigError):
        async with profile.ports_factory():
            pytest.fail("invalid method shape must fail")
    assert events == [("invalid-data-exit",)]


@pytest.mark.parametrize("failure", ["missing-module", "missing-function", "not-callable"])
def test_import_and_factory_resolution_errors_are_safe(tmp_path, factories, failure):
    module, _ = factories
    text = config_text()
    if failure == "missing-module":
        text = text.replace("configured_test_adapters:data", "private_missing_module:data")
    elif failure == "missing-function":
        text = text.replace("configured_test_adapters:data", "configured_test_adapters:absent")
    else:
        module.data = "private-non-callable"
    with pytest.raises(importlib.import_module("lake_agent.config").ConfigError) as error:
        api().configured_profile(load(tmp_path, text))
    assert "private-" not in str(error.value)


async def test_async_function_factory_is_rejected_without_leaking_coroutine(tmp_path, factories):
    module, _ = factories

    async def invalid(*, options, llm):
        return EmptyData()

    module.data = invalid
    profile = api().configured_profile(load(tmp_path))
    with pytest.raises(importlib.import_module("lake_agent.config").ConfigError):
        async with profile.ports_factory():
            pytest.fail("a coroutine is not an async context manager")


async def test_execution_exception_propagates_and_resources_close(tmp_path, factories):
    _, events = factories
    profile = api().configured_profile(load(tmp_path))
    with pytest.raises(RuntimeError, match="consumer-failure"):
        async with profile.ports_factory() as ports:
            raise RuntimeError("consumer-failure")
    assert ports.planner._client.is_closed
    assert [entry[0] for entry in events][-2:] == ["content-exit", "data-exit"]


async def test_teardown_failure_is_redacted_and_remaining_stack_still_closes(tmp_path, factories):
    module, events = factories

    @asynccontextmanager
    async def failing_exit(*, options, llm):
        try:
            yield EmptyContent()
        finally:
            events.append(("failing-content-exit",))
            raise RuntimeError("private-adapter-teardown-secret")

    module.content = failing_exit
    profile = api().configured_profile(load(tmp_path))
    with pytest.raises(importlib.import_module("lake_agent.config").ConfigError) as error:
        async with profile.ports_factory() as ports:
            pass
    assert "private-adapter-teardown-secret" not in str(error.value)
    assert ports.planner._client.is_closed
    assert [entry[0] for entry in events][-2:] == ["failing-content-exit", "data-exit"]


@pytest.mark.parametrize("behavior", ["rethrow", "suppress", "cancel"])
async def test_adapter_exit_cannot_suppress_caller_errors_or_cancellation(
    tmp_path, factories, behavior
):
    module, events = factories

    class Context:
        async def __aenter__(self):
            return EmptyContent()

        async def __aexit__(self, exception_type, exception, traceback):
            if behavior == "cancel":
                raise asyncio.CancelledError
            if behavior == "rethrow":
                raise exception
            return True

    module.content = lambda **_: Context()
    profile = api().configured_profile(load(tmp_path))
    expected = asyncio.CancelledError if behavior == "cancel" else ValueError
    with pytest.raises(expected):
        async with profile.ports_factory() as ports:
            if behavior != "cancel":
                raise ValueError("caller-body-error")
    assert ports.planner._client.is_closed
    assert events[-1][0] == "data-exit"


@pytest.mark.parametrize("error_type", [ValueError, asyncio.CancelledError])
async def test_incoming_controller_error_survives_vendor_teardown_failure(
    tmp_path, factories, error_type
):
    """清理产生的新普通异常不能覆盖控制器失败或用于恢复的取消信号。"""
    module, events = factories

    @asynccontextmanager
    async def failing_exit(*, options, llm):
        try:
            yield EmptyContent()
        finally:
            events.append(("failing-content-exit",))
            raise RuntimeError("private-vendor-cleanup-detail")

    module.content = failing_exit
    profile = api().configured_profile(load(tmp_path))
    original = error_type("controller-error")
    with pytest.raises(error_type) as error:
        async with profile.ports_factory() as ports:
            raise original
    assert error.value is original
    assert ports.planner._client.is_closed
    assert [entry[0] for entry in events][-2:] == ["failing-content-exit", "data-exit"]


async def test_planner_creation_failure_closes_both_adapters(tmp_path, factories, monkeypatch):
    _, events = factories

    def failed_planner(*args, **kwargs):
        raise RuntimeError("private-planner-client-detail")

    monkeypatch.setattr(api(), "JsonPlanner", failed_planner)
    profile = api().configured_profile(load(tmp_path))
    with pytest.raises(importlib.import_module("lake_agent.config").ConfigError) as error:
        async with profile.ports_factory():
            pytest.fail("planner creation must fail before execution")
    assert "private-planner-client-detail" not in str(error.value)
    assert [entry[0] for entry in events][-2:] == ["content-exit", "data-exit"]


def test_binding_tracks_connections_and_behavior_but_allows_key_rotation(tmp_path, factories):
    original = api().configured_profile(load(tmp_path))
    rotated = api().configured_profile(
        load(tmp_path, config_text().replace("private-test-key", "rotated-private-key"))
    )
    assert original.binding_id == rotated.binding_id
    assert original.signature() == rotated.signature()
    assert len(original.binding_id) == 64
    for old, new in [
        ("planner.example", "changed-planner.example"),
        ("private-adapter-option", "changed-private-option"),
        ("configured-model", "changed-model"),
        ("seed = 7", "seed = 8"),
    ]:
        changed = api().configured_profile(load(tmp_path, config_text().replace(old, new)))
        assert changed.binding_id != original.binding_id
        assert changed.signature() != original.signature()
    assert "private-" not in json.dumps(original.public())


async def test_actual_run_sends_configured_endpoint_auth_model_and_accepts_plan(
    tmp_path, factories, monkeypatch
):
    """真实控制器和 JsonPlanner 共同执行，只将 HTTP 传输替换为内存响应。"""
    requests = []

    def respond(request):
        body = json.loads(request.content)
        requests.append((request, body))
        assert str(request.url) == "https://planner.example/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer private-test-key"
        assert body["model"] == "configured-model"
        assert body["temperature"] == 0.25
        assert body["seed"] == 7
        assert body["response_format"] == {"type": "json_object"}
        assert body["stream"] is False
        incoming = json.loads(body["messages"][1]["content"])
        assert incoming["phase"] == "decide"
        context = incoming["input"]["context"]
        decision = (
            {
                "kind": "plan",
                "subtasks": [
                    {
                        "subtask_id": "configured-subtask",
                        "objective": "Inspect pool",
                        "search_query": "records",
                    }
                ],
            }
            if not context["plan"]
            else {"kind": "stop", "reason": "The registered catalog contains no evidence."}
        )
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": json.dumps(decision)}, "finish_reason": "stop"}]
            },
        )

    def transport(*, retries):
        assert retries == 0
        return httpx.MockTransport(respond)

    monkeypatch.setattr("lake_agent.adapters.planner.httpx.AsyncHTTPTransport", transport)
    profile = api().configured_profile(load(tmp_path))
    async with profile.ports_factory() as ports:
        result = await run(
            m.RunRequest(
                task_id="configured-task", question="Find records", config=profile.run_config
            ),
            data=ports.data,
            content=ports.content,
            planner=ports.planner,
            trace=JsonlTraceSink(tmp_path / "traces"),
        )
    assert result.status == "insufficient_evidence"
    assert len(requests) == 2
    assert result.usage.provider_requests == 2
    trace = next((tmp_path / "traces").glob("*/events.jsonl")).read_text()
    assert "task_plan.updated" in trace
    assert "planner.decide.call.completed" in trace
    assert "private-test-key" not in trace
    assert ports.planner._client.is_closed


def isolation():
    return importlib.import_module("tests.support.isolated")


def settings(tmp_path):
    text = config_text(extra="[run]\nmax_data_calls = 7\nmax_model_calls = 9\n")
    text = text.replace("configured_test_adapters:data", "unimplemented_peer_a:create")
    text = text.replace("configured_test_adapters:content", "unimplemented_peer_c:create")
    return load_config(write_config(tmp_path, text))


async def test_isolated_profile_replaces_only_dependencies_and_declares_boundaries(
    tmp_path, monkeypatch
):
    from lake_agent.web import configuration

    config = settings(tmp_path)
    before = config.model_copy(deep=True)
    imports = []
    original_import = configuration.import_module

    def checked_import(module):
        imports.append(module)
        assert not module.startswith("unimplemented_peer_")
        return original_import(module)

    monkeypatch.setattr(configuration, "import_module", checked_import)
    planners = []
    original_planner = configuration.JsonPlanner

    def planner(*args, **kwargs):
        instance = original_planner(*args, **kwargs)
        planners.append(instance)
        return instance

    monkeypatch.setattr(configuration, "JsonPlanner", planner)
    profile = isolation().build_profile(config)
    assert planners == []
    assert imports == ["tests.support.isolated", "tests.support.isolated"]
    assert config == before
    assert profile.metadata.profile_id != config.profile.profile_id
    assert profile.metadata.dataset_id != config.profile.dataset_id
    assert profile.metadata.revision == "isolation-fixture-v3"
    assert "隔离" in profile.metadata.title
    assert "合成" in profile.metadata.modules.data.label
    assert "本地" in profile.metadata.modules.content.label
    assert "真实" in profile.metadata.modules.content.label
    assert "固定验证" not in profile.metadata.modules.content.label
    assert "真实" in profile.metadata.modules.planner.label
    assert profile.run_config == config.run
    assert len(profile.sources) == 2
    assert "private-" not in json.dumps(profile.public())
    runs = []
    for _ in range(2):
        async with profile.ports_factory() as ports:
            assert isinstance(ports.planner, JsonPlanner)
            assert ports.planner._api_key == config.llm.api_key.get_secret_value()
            runs.append(ports)
        assert ports.planner._client.is_closed
        assert ports.content._client.is_closed
    assert len(planners) == 2
    assert runs[0].data is not runs[1].data
    assert runs[0].content is not runs[1].content
    assert runs[0].planner is not runs[1].planner


def test_incompatible_snapshot_fails_safely_before_planner_construction(tmp_path, monkeypatch):
    config = settings(tmp_path)
    config.run.dataset_snapshot = "private-project-snapshot"

    def forbidden_planner(*args, **kwargs):
        raise AssertionError("No planner may be constructed for an incompatible snapshot")

    monkeypatch.setattr("lake_agent.web.configuration.JsonPlanner", forbidden_planner)
    with pytest.raises(ConfigError) as error:
        isolation().build_profile(config)
    assert "run.dataset_snapshot" in str(error.value)
    assert "private-project-snapshot" not in str(error.value)


def test_explicit_fixture_snapshot_keeps_the_declared_run_policy(tmp_path):
    config = settings(tmp_path)
    config.run.dataset_snapshot = "demo-v1"
    assert isolation().build_profile(config).run_config == config.run


@pytest.mark.parametrize(
    "question,answer",
    [
        ("目前的合成资料有哪些", "当前合成资料包含年度营收报告和季度背景资料。"),
        ("2024 年年度营收是多少", "2024 年年度营收为 1.2 亿港元。"),
    ],
)
@pytest.mark.parametrize(
    "outcome", ["supported", "invalid-citation", "verification-failure", "api-error"]
)
async def test_full_agent_uses_model_planning_composition_and_verification_without_fixed_success(
    tmp_path, monkeypatch, question, answer, outcome
):
    received = []

    def respond(request):
        body = json.loads(request.content)
        incoming = json.loads(body["messages"][1]["content"])
        received.append(incoming)
        phase = incoming["phase"]
        assert incoming["input"]["question"] == question
        assert request.headers["Authorization"] == "Bearer private-test-key"
        assert str(request.url) == "https://planner.example/v1/chat/completions"
        assert body["model"] == "configured-model"
        assert body["temperature"] == 0.25
        if phase == "decide":
            context = incoming["input"]["context"]
            if context["has_answer"]:
                value = {"kind": "verify" if context["verification_verdict"] is None else "finish"}
            elif context["tool_observations"]:
                assert context["evidence_summary"]
                value = {"kind": "compose"}
            else:
                value = {
                    "kind": "tools",
                    "steps": [
                        {
                            "step_id": "extract-annual",
                            "tool_name": "extract",
                            "source_ids": ["demo-annual"],
                            "purpose": "Collect actual fixture evidence",
                            "args": {
                                "source_id": "demo-annual",
                                "query": question,
                                "max_chars": 4000,
                            },
                        }
                    ],
                }
        elif phase == "compose":
            if outcome == "api-error":
                return httpx.Response(503, text="private-service-error")
            assert {
                source["name"] for source in incoming["input"]["catalog_context"]["sources"]
            } == {"synthetic-report.txt", "synthetic-quarter.txt"}
            citation = (
                "unknown-evidence"
                if outcome == "invalid-citation"
                else incoming["input"]["evidence"][0]["evidence_id"]
            )
            value = {"text": answer, "citations": [citation]}
        else:
            assert phase == "verify"
            assert incoming["input"]["answer"]["text"] == answer
            value = (
                {"verdict": "insufficient", "issues": [], "recommendation": {"action": "stop"}}
                if outcome == "verification-failure"
                else {"verdict": "supported", "issues": [], "recommendation": {"action": "none"}}
            )
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": json.dumps(value)}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 50, "completion_tokens": 20},
            },
        )

    monkeypatch.setattr(
        "lake_agent.adapters.planner.httpx.AsyncHTTPTransport",
        lambda *, retries: httpx.MockTransport(respond),
    )
    profile = isolation().build_profile(settings(tmp_path))
    async with profile.ports_factory() as ports:
        result = await run(
            m.RunRequest(
                task_id="isolation-model-content",
                question=question,
                mode="full_agent",
                config=profile.run_config,
            ),
            data=ports.data,
            content=ports.content,
            planner=ports.planner,
            trace=JsonlTraceSink(tmp_path / "isolated-traces"),
        )
    # 持续返回非法引用的模型不会被固定成功回退；反馈循环继续消耗原始9次额度。
    expected_calls = 3 if outcome == "api-error" else 9 if outcome == "invalid-citation" else 6
    assert result.call_counts.model_calls == len(received) == expected_calls
    assert result.usage.provider_requests == expected_calls
    assert result.call_counts.tools == 1
    if outcome == "api-error":
        assert [message["phase"] for message in received] == ["decide", "decide", "compose"]
        assert result.status == "failed"
        assert result.answer is None
    elif outcome == "invalid-citation":
        assert (result.status, result.stop_reason) == ("budget_exceeded", "model_call_budget")
        assert result.answer is None
        assert result.failure.error.code == "invalid_model_citations"
        compositions = [message["input"] for message in received if message["phase"] == "compose"]
        assert len(compositions) == 4
        assert "feedback" not in compositions[0]
        assert all(
            value["feedback"]["error"]["details"]["issues"][0]["actual"] == "unknown-evidence"
            for value in compositions[1:]
        )
        assert all(message["phase"] != "verify" for message in received)
    else:
        assert [message["phase"] for message in received] == [
            "decide",
            "decide",
            "compose",
            "decide",
            "verify",
            "decide",
        ]
        assert result.answer.text == answer
        assert result.answer.citations
        assert result.answer.structured_result["test_scope"]["model_generated_answer"] is True
        if outcome == "verification-failure":
            assert (result.status, result.stop_reason) == (
                "insufficient_evidence",
                "agent_verification_failed",
            )
        else:
            assert (result.status, result.stop_reason) == ("completed", "agent_finished")
    trace = await asyncio.to_thread(Path(result.trace_ref).read_text)
    assert "private-test-key" not in trace
    assert "private-service-error" not in trace
    assert "content.compose.call.started" in trace
    assert ports.planner._client.is_closed
    assert ports.content._client.is_closed


@asynccontextmanager
async def unopened_ports():
    """注册校验不得提前构造或运行执行端口。"""
    raise AssertionError("Profile admission must not open execution ports")
    yield  # pragma: no cover


def profile(identity="configured-data", **kwargs):
    """提供测试拥有的配置元数据，避免依赖产品默认配置。"""
    return ExecutionProfile(
        metadata=ProfileMetadata(
            profile_id=identity,
            title="Configured data",
            description="Explicit registration for admission checks.",
            kind="project",
            dataset_id="configured-dataset",
            dataset_name="Configured dataset",
            revision="v1",
            modules={
                name: {"status": "connected", "label": name}
                for name in ("data", "planner", "content")
            },
        ),
        ports_factory=unopened_ports,
        **kwargs,
    )


def test_unconfigured_registry_cannot_offer_implicit_execution():
    registry = ProfileRegistry()
    assert registry.listing() == []
    with pytest.raises(WebError) as caught:
        registry.runnable(None)
    assert caught.value.code == "profiles_unconfigured"


def test_single_configured_profile_can_be_selected_without_guessing_identity():
    configured = profile()
    registry = ProfileRegistry((configured,))
    assert registry.runnable(None) is configured


def test_multiple_profiles_require_explicit_selection():
    registry = ProfileRegistry((profile("first"), profile("second")))
    with pytest.raises(WebError) as caught:
        registry.runnable(None)
    assert caught.value.code == "profile_required"
    assert caught.value.status == 422
    assert registry.runnable("second").metadata.profile_id == "second"


def test_missing_saved_profile_identity_cannot_be_inferred_from_scenario():
    registry = ProfileRegistry((profile("demo-standard"),))
    with pytest.raises(WebError) as caught:
        registry.for_record({"scenario": "standard"})
    assert caught.value.code == "profile_unavailable"


def test_connection_change_blocks_recovery_without_publishing_binding():
    original = profile(binding_id="private-binding-a")
    changed = replace(original, binding_id="private-binding-b")
    record = {
        "profile_id": original.metadata.profile_id,
        "profile_signature": original.signature(),
    }
    assert ProfileRegistry((original,)).for_record(record) is original
    with pytest.raises(WebError) as caught:
        ProfileRegistry((changed,)).for_record(record)
    assert caught.value.code == "profile_changed"
    assert "binding_id" not in original.public()
    assert "private-binding-a" not in str(original.public())
