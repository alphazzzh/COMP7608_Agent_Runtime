"""手动隔离 CLI 的参数、配置错误和资源边界；服务器启动被替换。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import importlib
import json
from typing import get_args

import httpx
import pytest

from lake_agent import models as m
from tests.support.configuration import write_settings


def cli():
    """延迟导入，使新增入口缺失表现为具体测试失败。"""
    return importlib.import_module("tests.isolated_runtime")


def test_isolation_requires_explicit_web_or_question(tmp_path):
    with pytest.raises(SystemExit) as error:
        cli().main(["--config", str(write_settings(tmp_path))])
    assert error.value.code == 2
    assert not (tmp_path / "records").exists()


def test_missing_credential_fails_before_server_or_record_creation(tmp_path, monkeypatch, capsys):
    config = write_settings(tmp_path)
    config.write_text(
        config.read_text().replace(
            'api_key = "cli-private-key"', 'api_key_env = "ISOLATED_TEST_ABSENT_KEY"'
        )
    )
    monkeypatch.delenv("ISOLATED_TEST_ABSENT_KEY", raising=False)
    with pytest.raises(SystemExit) as error:
        cli().main(["--config", str(config), "--web"])
    assert error.value.code == 2
    assert "credential environment variable is empty" in capsys.readouterr().err
    assert not (tmp_path / "records").exists()


@pytest.mark.parametrize("extra", [["--port", "0"], ["--question", "   "]])
def test_invalid_arguments_rejected_before_execution(tmp_path, extra):
    args = ["--config", str(write_settings(tmp_path))]
    if extra[0] == "--port":
        args.append("--web")
    with pytest.raises(SystemExit) as error:
        cli().main(args + extra)
    assert error.value.code == 2
    assert not (tmp_path / "records").exists()


@pytest.mark.parametrize("overrides", [False, True])
def test_web_registers_only_isolated_profile_and_separate_storage(
    tmp_path, monkeypatch, capsys, overrides
):
    import uvicorn

    from lake_agent.web import app as application

    observed = {}
    original_create = application.create_app

    def inspect_application(**kwargs):
        observed["application_options"] = kwargs
        return original_create(**kwargs)

    def inspect_server(app, **kwargs):
        # 仅拦截真正绑定端口的操作，其余配置和应用构造全部执行。
        observed["service"] = app.state.run_service
        observed["server"] = kwargs

    monkeypatch.setattr(uvicorn, "run", inspect_server)
    monkeypatch.setattr(application, "create_app", inspect_application)
    args = ["--config", str(write_settings(tmp_path)), "--web"]
    if overrides:
        args += ["--port", "8779", "--data-dir", str(tmp_path / "custom")]
    assert cli().main(args) == 0
    service = observed["service"]
    entries = service.profiles.listing()
    assert len(entries) == 1
    assert "隔离" in entries[0]["title"]
    assert "测试" in entries[0]["modules"]["data"]["label"]
    assert "测试" in entries[0]["modules"]["content"]["label"]
    assert entries[0]["model_id"] == "cli-model"
    assert service.max_active_runs == 3
    assert service.repository.root == (
        tmp_path / "custom" if overrides else tmp_path / "records" / "isolated-runtime" / "web"
    )
    assert observed["server"] == {
        "host": "127.0.0.1",
        "port": 8779 if overrides else 8768,
        "workers": 1,
    }
    assert observed["application_options"]["allowed_modes"] == get_args(m.Mode)
    assert observed["application_options"]["enable_evaluations"] is True
    captured = capsys.readouterr()
    assert "隔离测试" in captured.err
    assert "cli-private-key" not in captured.out + captured.err
    assert not service.repository.root.exists()


def test_headless_executes_real_planner_and_writes_trace(tmp_path, monkeypatch, capsys):
    def respond(request):
        body = json.loads(request.content)
        assert str(request.url) == "https://configured.example/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer cli-private-key"
        assert body["model"] == "cli-model"
        incoming = json.loads(body["messages"][1]["content"])
        assert incoming["phase"] == "decide"
        context = incoming["input"]["context"]
        decision = (
            {
                "kind": "plan",
                "subtasks": [{"subtask_id": "s1", "objective": "Inspect", "search_query": "2024"}],
            }
            if not context["plan"]
            else {"kind": "stop", "reason": "No evidence was collected for this question."}
        )
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": json.dumps(decision)}, "finish_reason": "stop"}]
            },
        )

    monkeypatch.setattr(
        "lake_agent.adapters.planner.httpx.AsyncHTTPTransport",
        lambda **kwargs: httpx.MockTransport(respond),
    )
    code = cli().main(
        [
            "--config",
            str(write_settings(tmp_path)),
            "--question",
            "  What happened in 2024?  ",
            "--mode",
            "no_verifier",
        ]
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert code == 1  # 非 completed 结果供命令行调用方识别，仍完整输出审计结果。
    assert result["status"] == "insufficient_evidence"
    assert result["mode"] == "no_verifier"
    assert result["usage"]["provider_requests"] == 2
    trace = next((tmp_path / "records" / "isolated-runtime" / "headless").glob("*/events.jsonl"))
    assert result["trace_ref"] == str(trace)
    events = [json.loads(line) for line in trace.read_text().splitlines()]
    assert events[-1]["event_type"] == "run.stopped"
    assert sum(event["event_type"] == "planner.decide.call.completed" for event in events) == 2
    assert "cli-private-key" not in captured.out + captured.err + trace.read_text()
    assert "隔离测试" in captured.err
