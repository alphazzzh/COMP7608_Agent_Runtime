"""TOML 解析、脱敏、路径和正式 CLI 配置接线的离线回归。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import importlib
import sys

import pytest
from pydantic import SecretStr

from tests.support.configuration import config_text, write_config, write_settings


def api():
    """通过公开配置模块检查加载结果与严格错误边界。"""
    return importlib.import_module("lake_agent.config")


def test_load_config_resolves_defaults_and_model_policy_without_opening_adapters(tmp_path):
    config = api().load_config(write_config(tmp_path))
    assert config.server.port == 8765
    assert config.server.max_active_runs == 2
    assert config.server.data_dir == tmp_path / "runs" / "workspace"
    assert not config.server.data_dir.exists()
    assert config.profile.profile_id == "project-main"
    assert config.profile.description == ""
    assert config.profile.suggested_questions == []
    assert isinstance(config.llm.api_key, SecretStr)
    assert config.llm.api_key.get_secret_value() == "private-test-key"
    assert config.run.model_id == "configured-model"
    assert config.run.model_params == {"temperature": 0.25, "seed": 7}
    assert "private-test-key" not in repr(config)


def test_environment_key_is_resolved_once_and_remains_secret(tmp_path, monkeypatch):
    monkeypatch.setenv("CONFIG_TEST_API_KEY", "environment-private-key")
    config = api().load_config(
        write_config(tmp_path, config_text(credential='api_key_env = "CONFIG_TEST_API_KEY"'))
    )
    monkeypatch.setenv("CONFIG_TEST_API_KEY", "changed-key")
    assert config.llm.api_key.get_secret_value() == "environment-private-key"
    assert "environment-private-key" not in config.llm.model_dump_json()


@pytest.mark.parametrize(
    "credential",
    [
        "",
        'api_key = ""',
        'api_key = "   "',
        'api_key = "private-test-key"\napi_key_env = "CONFIG_TEST_API_KEY"',
        'api_key_env = "CONFIG_MISSING_API_KEY"',
        'api_key_env = ""',
        'api_key_env = "not an env name"',
        "api_key = 42",
        'api_key = "private-test-key\\nleaked"',
    ],
)
def test_credentials_require_one_valid_explicit_source(tmp_path, monkeypatch, credential):
    monkeypatch.delenv("CONFIG_MISSING_API_KEY", raising=False)
    monkeypatch.setenv("CONFIG_TEST_API_KEY", "private-environment-key")
    with pytest.raises(api().ConfigError) as error:
        api().load_config(write_config(tmp_path, config_text(credential=credential)))
    assert "private-test-key" not in str(error.value)
    assert "private-environment-key" not in str(error.value)


@pytest.mark.parametrize(
    "old,new",
    [
        ("https://planner.example/v1", "https://private-user:private-pass@example/v1"),
        ("https://planner.example/v1", "https://planner.example/v1?key=private-url-key"),
        ("https://planner.example/v1", "https://planner.example/v1#private-fragment"),
        ("https://planner.example/v1", "file:///private-url-key"),
        ("https://planner.example/v1", "https://example:invalid-private-port/v1"),
        ('model_id = "configured-model"', 'model_id = ""'),
        ('model_id = "configured-model"', "model_id = 42"),
        ("temperature = 0.25", "temperature = 3"),
        ("temperature = 0.25", "temperature = true"),
        ("temperature = 0.25", "messages = []"),
        ("rows = [1, 2]", "rows = 1979-05-27T07:32:00Z"),
        ('factory = "configured_test_adapters:data"', 'factory = "private-import-string"'),
        ('revision = "v1"', 'revision = "v1"\nprivate-unknown = "private-raw-value"'),
    ],
)
def test_invalid_settings_are_rejected_without_echoing_values(tmp_path, old, new):
    with pytest.raises(api().ConfigError) as error:
        api().load_config(write_config(tmp_path, config_text().replace(old, new)))
    message = str(error.value)
    for value in ("private-", "configured_test_adapters", "1979", "messages"):
        assert value not in message


@pytest.mark.parametrize(
    "extra",
    [
        '[run]\nmodel_id = "duplicate-private-model"',
        "[run.model_params]\ntemperature = 0",
        '[run]\nmax_data_calls = "12"',
        "[run]\nmax_data_calls = 0",
        "[run]\nmax_parallel_tool_calls = 33",
        "[server]\nport = true",
        "[server]\nport = 65536",
        "[server]\nmax_active_runs = 0",
        '[server]\ndata_dir = ""',
        '[unknown]\nvalue = "private-unknown"',
    ],
)
def test_budget_types_unknown_fields_and_duplicate_models_fail(tmp_path, extra):
    with pytest.raises(api().ConfigError):
        api().load_config(write_config(tmp_path, config_text(extra=extra)))


def test_relative_path_and_budgets_use_file_parent(tmp_path, monkeypatch):
    directory = tmp_path / "settings"
    directory.mkdir()
    monkeypatch.chdir(tmp_path)
    config = api().load_config(
        write_config(
            directory,
            config_text(
                extra='[server]\nport = 9234\ndata_dir = "../output/./runs"\n'
                "max_active_runs = 3\n[run]\nmax_data_calls = 4\n"
                "max_wall_time_ms = 30000\n"
            ),
        )
    )
    assert config.server.data_dir == tmp_path / "output" / "runs"
    assert config.server.port == 9234
    assert config.server.max_active_runs == 3
    assert config.run.max_data_calls == 4
    assert config.run.max_wall_time_ms == 30000


@pytest.mark.parametrize("text", ['[llm]\napi_key = "private-broken', "\xff\x00"])
def test_malformed_toml_has_safe_errors(tmp_path, text):
    with pytest.raises(api().ConfigError) as error:
        api().load_config(write_config(tmp_path, text))
    assert "private-broken" not in str(error.value)


def test_missing_configuration_has_safe_error(tmp_path):
    with pytest.raises(api().ConfigError) as error:
        api().load_config(tmp_path / "private-missing-file.toml")
    assert "private-missing-file" not in str(error.value)


def test_validation_errors_identify_known_fields_without_unknown_names(tmp_path):
    with pytest.raises(api().ConfigError) as error:
        api().load_config(
            write_config(tmp_path, config_text(extra='[server]\nport = "private-invalid-port"'))
        )
    assert "server.port" in str(error.value)
    assert "private-invalid-port" not in str(error.value)
    with pytest.raises(api().ConfigError) as error:
        api().load_config(
            write_config(tmp_path, config_text(extra='[private-unknown-name]\nvalue = "secret"'))
        )
    assert "private-unknown-name" not in str(error.value)


@pytest.mark.parametrize(
    "retired_fields",
    [
        'max_feedback_rounds = "private-old-value"',
        'fixed_extract_max_chars = "private-old-value"',
        "max_feedback_rounds = 2\nfixed_extract_max_chars = 12000",
    ],
)
def test_removed_run_settings_report_exact_migration_without_echoing_values(
    tmp_path, retired_fields
):
    """旧 TOML 不能只提示 run；迁移信息只包含代码中固定的退役字段名。"""
    with pytest.raises(api().ConfigError) as error:
        api().load_config(write_config(tmp_path, config_text(extra=f"[run]\n{retired_fields}")))
    message = str(error.value)
    for name in ("max_feedback_rounds", "fixed_extract_max_chars"):
        assert (f"run.{name}" in message) == (name in retired_fields)
    assert "Remove" in message and "[run]" in message
    assert "private-old-value" not in message and "private-test-key" not in message


def test_removed_run_settings_do_not_expose_unknown_fields_or_nested_options(tmp_path):
    with pytest.raises(api().ConfigError) as error:
        api().load_config(
            write_config(
                tmp_path,
                config_text(
                    extra="""[run]
max_feedback_rounds = 2
private-unknown-key = "private-sensitive-value"
[run.fixed_extract_max_chars]
private-nested-key = "private-nested-value"
"""
                ),
            )
        )
    message = str(error.value)
    assert "run.max_feedback_rounds" in message and "run.fixed_extract_max_chars" in message
    assert "private-" not in message


def test_removing_retired_settings_keeps_current_budgets_and_model_config(tmp_path):
    """迁移只删失效配置，现有自主轮数、模型及其他预算继续生效。"""
    config = api().load_config(
        write_config(
            tmp_path,
            config_text(
                extra="""[run]
max_agent_turns = 40
max_model_calls = 16
require_verification = false
"""
            ),
        )
    )
    assert config.run.max_agent_turns == 40 and config.run.max_model_calls == 16
    assert config.run.require_verification is False
    assert config.run.model_id == "configured-model"


@pytest.mark.parametrize("overrides", [False, True])
def test_cli_loads_config_before_constructing_app_without_binding_a_socket(
    tmp_path, monkeypatch, overrides
):
    module = importlib.import_module("lake_agent.web.app")
    import uvicorn

    config_path = write_settings(tmp_path)
    calls = {}
    profile = object()
    application = object()

    def configured(config):
        calls["model"] = config.run.model_id
        return profile

    def create(**kwargs):
        calls["app"] = kwargs
        return application

    def run(app, **kwargs):
        calls["server"] = (app, kwargs)

    monkeypatch.setattr(module, "configured_profile", configured, raising=False)
    monkeypatch.setattr(module, "create_app", create)
    monkeypatch.setattr(uvicorn, "run", run)
    argv = ["lake-agent-web", "--config", str(config_path)]
    if overrides:
        argv += ["--port", "8769", "--data-dir", str(tmp_path / "override")]
    monkeypatch.setattr(sys, "argv", argv)
    module.main()
    assert calls["model"] == "cli-model"
    assert calls["app"]["profiles"] == [profile]
    assert calls["app"]["max_active_runs"] == 3
    assert calls["app"]["data_dir"] == tmp_path / ("override" if overrides else "records")
    assert calls["server"] == (
        application,
        {"host": "127.0.0.1", "port": 8769 if overrides else 8768, "workers": 1},
    )
    assert not (tmp_path / "records").exists()


def test_invalid_config_exits_without_starting_or_printing_secrets(tmp_path, monkeypatch, capsys):
    module = importlib.import_module("lake_agent.web.app")
    import uvicorn

    path = write_settings(tmp_path)
    path.write_text(path.read_text().replace('model_id = "cli-model"', "model_id = 123"))

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid configuration reached the server")

    monkeypatch.setattr(uvicorn, "run", forbidden)
    monkeypatch.setattr(sys, "argv", ["lake-agent-web", "--config", str(path)])
    with pytest.raises(SystemExit) as stopped:
        module.main()
    assert stopped.value.code == 2
    output = capsys.readouterr().err
    assert "cli-private-key" not in output
    assert "configuration" in output.lower()


def test_isolated_cli_reports_retired_run_fields_before_opening_profile_or_server(
    tmp_path, monkeypatch, capsys
):
    """复现用户原命令的启动边界，不进入适配器、创建网络客户端或监听端口。"""
    module = importlib.import_module("tests.isolated_runtime")
    path = write_settings(tmp_path)
    path.write_text(
        path.read_text() + "\n[run]\nmax_feedback_rounds = 2\nfixed_extract_max_chars = 12000\n"
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Retired configuration reached profile creation")

    monkeypatch.setattr(module, "build_profile", forbidden)
    with pytest.raises(SystemExit) as stopped:
        module.main(["--config", str(path), "--web"])
    assert stopped.value.code == 2
    output = capsys.readouterr().err
    assert "run.max_feedback_rounds" in output and "run.fixed_extract_max_chars" in output
    assert "Remove retired settings from [run]" in output
    assert "cli-private-key" not in output and not (tmp_path / "records").exists()
