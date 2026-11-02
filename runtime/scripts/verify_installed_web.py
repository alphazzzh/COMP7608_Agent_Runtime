"""通过隔离安装与真实本地 HTTP，验证可选网页组件的 wheel 包。

先运行 ``uv build``，再从任意工作目录执行本脚本。
默认安装仅使用缓存依赖；缓存不足时，``--allow-downloads`` 允许下载依赖。
脚本不使用模型端点或凭据。复制后的探测程序在源码检出目录外的隔离 Python 中执行，
启动已安装的命令行入口，检查全部三个合成场景，并重启服务器。
无论验证成功与否，都会先移除临时环境、服务器和合成数据，
再写入 JSON 验证证据报告。
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import html
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
import zipfile
from contextlib import contextmanager
from datetime import UTC, datetime
from email import message_from_bytes
from pathlib import Path


def checked_command(args, *, cwd, env=None):
    """运行有时限的子进程，不转发继承来的 Python 导入路径。"""
    result = subprocess.run(
        args, cwd=cwd, env=env, text=True, capture_output=True, timeout=90, check=False
    )
    if result.returncode:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {args!r}\n{result.stdout}{result.stderr}"
        )
    return result.stdout.strip()


def clean_environment():
    """保留常规操作系统设置，移除可能混入源码的导入配置。"""
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env["PYTHONNOUSERSITE"] = "1"
    return env


def wheel_identity(root, wheel, expected_version):
    """检查安装元数据，并逐字节比较打包的源码与静态资源。"""
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = message_from_bytes(archive.read(metadata_name))
        assert metadata["Name"] == "lake-agent-runtime"
        assert metadata["Version"] == expected_version
        requirements = metadata.get_all("Requires-Dist", [])
        assert any(item.startswith("langgraph<") for item in requirements), requirements
        for dependency in ("fastapi", "uvicorn"):
            selected = [item for item in requirements if item.lower().startswith(dependency)]
            assert selected, f"Missing optional web dependency: {dependency}"
            assert all(
                "extra == web" in item.replace('"', "").replace("'", "") for item in selected
            ), f"Web dependency leaked into the core installation: {selected}"
        entrypoints = configparser.ConfigParser()
        entrypoint_name = metadata_name.rsplit("/", 1)[0] + "/entry_points.txt"
        entrypoints.read_string(archive.read(entrypoint_name).decode())
        assert entrypoints["console_scripts"]["lake-agent-web"] == "lake_agent.web.app:main"
        assert entrypoints["console_scripts"]["lake-agent-demo"] == "lake_agent.demo:main"
        assets = {
            "lake_agent/web/static/index.html",
            "lake_agent/web/static/app.js",
            "lake_agent/web/static/styles.css",
        }
        assert assets <= set(names), f"Missing packaged assets: {assets - set(names)}"
        hashes = {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in sorted(names)
            if name.startswith("lake_agent/") and (name.endswith(".py") or name in assets)
        }
    for name, digest in hashes.items():
        assert hashlib.sha256((root / "src" / name).read_bytes()).hexdigest() == digest, name
    return {
        "filename": wheel.name,
        "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "version": expected_version,
        "requires_python": metadata["Requires-Python"],
        "requires_dist": requirements,
        "console_scripts": dict(entrypoints["console_scripts"]),
        "static_files": sorted(assets),
        "packaged_source_and_static_sha256": hashes,
        "packaged_files_match_checkout": True,
    }


def probe_installed(expected_version):
    """在独立且隔离的解释器中检验已安装的软件包。"""
    import asyncio
    from importlib import metadata

    import httpx

    import lake_agent
    from lake_agent.demo import DemoContent, DemoData, DemoPlanner
    from lake_agent.models import RunRequest
    from lake_agent.runtime import _Controller
    from lake_agent.trace import JsonlTraceSink
    from lake_agent.workflow import build_workflow

    work = Path.cwd().resolve()
    package = Path(lake_agent.__file__).resolve()
    assert package.is_relative_to(Path(sys.prefix).resolve()), package
    assert lake_agent.__version__ == expected_version
    cli = Path(sys.executable).with_name("lake-agent-web")
    assert cli.is_file(), cli
    # 只短暂占用套接字以选出空闲本地端口。监听套接字由服务器自身持有，
    # 离开上下文时始终会终止服务器。
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data_dir = work / "data"
    log_path = work / "server.log"
    exits = []

    @contextmanager
    def server():
        with log_path.open("a") as log:
            child = subprocess.Popen(
                [str(cli), "--port", str(port), "--data-dir", str(data_dir)],
                cwd=work,
                env=clean_environment(),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                with httpx.Client(
                    base_url=f"http://127.0.0.1:{port}", timeout=3, trust_env=False
                ) as client:
                    deadline = time.monotonic() + 15
                    while True:
                        if child.poll() is not None:
                            raise RuntimeError(log_path.read_text())
                        try:
                            response = client.get("/api/info")
                            response.raise_for_status()
                            break
                        except httpx.TransportError:
                            if time.monotonic() >= deadline:
                                raise TimeoutError("Installed CLI did not start") from None
                            time.sleep(0.02)
                    yield client
            finally:
                if child.poll() is None:
                    child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
                    raise RuntimeError("Installed CLI did not drain on shutdown") from None
                exits.append(child.returncode)

    def stopped(client, run_id):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            response = client.get(f"/api/runs/{run_id}")
            response.raise_for_status()
            view = response.json()
            if not view["active"]:
                return view
            time.sleep(0.01)
        raise TimeoutError(f"Installed run did not settle: {run_id}")

    def create(client, scenario, question):
        response = client.post(
            "/api/runs",
            json={"question": question, "profile_id": f"demo-{scenario}", "mode": "full_agent"},
        )
        assert response.status_code == 202, response.text
        return stopped(client, response.json()["run_id"])

    def summarize(view):
        result = view["result"]
        assert result["usage"]["provider_requests"] == 0, result
        assert view["metrics"]["model_calls"] == 0, view
        return {
            "run_id": view["run_id"],
            "status": view["status"],
            "stop_reason": result["stop_reason"],
            "metrics": view["metrics"],
            "provider_requests": result["usage"]["provider_requests"],
            "event_count": len(view["events"]),
            "evidence_ids": [item["evidence_id"] for item in view["evidence"]],
        }

    # 除了下面的 HTTP 验证，还检查已安装的执行引擎。
    # 构建图不会触发适配器 I/O，也不会打开审计目录。
    async def inspect_graph():
        graph = build_workflow(
            _Controller(
                RunRequest(task_id="installed-graph", question="Inspect offline workflow"),
                DemoData(),
                DemoContent(),
                DemoPlanner(),
                JsonlTraceSink(work / "graph-inspection"),
            )
        )
        nodes = sorted(graph.get_graph().nodes)
        assert {
            "catalog",
            "decompose",
            "search",
            "select_tools",
            "read_observation",
            "execute_plan",
            "finish_subtask",
            "compose",
            "verify",
            "feedback",
            "fixed_search",
            "fixed_execute",
        } <= set(nodes), nodes
        assert graph.checkpointer is None and graph.cache is None
        return {
            "engine": "langgraph",
            "langgraph_version": metadata.version("langgraph"),
            "langchain_core_version": metadata.version("langchain-core"),
            "nodes": nodes,
            "graph_checkpointer": False,
            "graph_cache": False,
        }

    checks = {"workflow": asyncio.run(inspect_graph())}
    with server() as client:
        info = client.get("/api/info").json()
        assert info["version"] == expected_version and info["demo_only"] is True
        assert info["application"] == "project-workspace"
        profiles = client.get("/api/profiles").json()["profiles"]
        assert len(profiles) == 4
        assert profiles[0]["profile_id"] == "project-default" and not profiles[0]["ready"]
        unavailable = client.post(
            "/api/runs", json={"question": "Unavailable project", "profile_id": "project-default"}
        )
        assert unavailable.status_code == 409
        directory = client.get("/api/profiles/demo-standard/sources").json()
        assert len(directory["sources"]) == 2
        checks["profiles"] = {"registered": len(profiles), "project_unavailable": True}
        template = client.get("/api/evaluations/template").json()
        assert all(
            value is None for row in template["results"] for value in row["metrics"].values()
        )
        imported = client.post("/api/evaluations", json=template)
        assert imported.status_code == 201, imported.text
        imported = imported.json()
        assert imported["origin"] == "imported"
        downloaded = client.get(f"/api/evaluations/{imported['evaluation_id']}")
        assert downloaded.json() == imported
        assert downloaded.headers.get("content-disposition") == (
            f'attachment; filename="{imported["evaluation_id"]}.json"'
        )
        checks["evaluations"] = {
            "import_preserves_unknown_metrics": True,
            "download_attachment": True,
        }
        assert {item["id"] for item in info["scenarios"]} == {"standard", "parallel", "recovery"}
        static = {}
        for route, filename in (
            ("/", "index.html"),
            ("/static/app.js", "app.js"),
            ("/static/styles.css", "styles.css"),
        ):
            response = client.get(route)
            assert response.status_code == 200, response.text
            installed_asset = package.parent / "web" / "static" / filename
            assert response.content == installed_asset.read_bytes()
            static[route] = {
                "bytes": len(response.content),
                "content_type": response.headers["content-type"],
            }
        checks["info"] = {"version": info["version"], "demo_only": info["demo_only"]}
        checks["static"] = static
        question = "<script>alert(1)</script> & What was 2024 annual revenue?"
        standard = create(client, "standard", question)
        assert standard["status"] == "completed" and standard["metrics"]["tools"] == 1
        assert "120 million HKD" in standard["result"]["answer"]["text"]
        run_path = f"/api/runs/{standard['run_id']}"
        assert client.get(f"{run_path}/export").json() == standard
        report = client.get(f"{run_path}/report")
        assert report.status_code == 200 and html.escape(question) in report.text
        assert "<script" not in report.text.lower()
        event = next(
            item
            for item in standard["events"]
            if item["event_type"] == "tool.call.completed" and item["data_ref"]
        )
        artifact = client.get(f"{run_path}/artifacts/{event['attempt_id']}/{event['data_ref']}")
        assert artifact.status_code == 200 and artifact.json()["call_id"] == event["call_id"]
        checks["standard"] = summarize(standard)
        checks["downloads"] = {
            "json_matches_view": True,
            "html_escaped": True,
            "artifact_scoped": True,
        }
        parallel = create(client, "parallel", "What was 2024 annual revenue?")
        assert parallel["status"] == "completed" and parallel["metrics"]["tools"] == 2
        assert {item["source_id"] for item in parallel["evidence"]} == {
            "demo-annual",
            "demo-quarter",
        }
        checks["parallel"] = summarize(parallel)
        paused = create(client, "recovery", "What was 2024 annual revenue?")
        assert paused["status"] == "paused" and paused["can_resume"] is True
        assert len(paused["evidence"]) == 1
        response = client.post(f"/api/runs/{paused['run_id']}/resume")
        assert response.status_code == 202, response.text
        resumed = stopped(client, paused["run_id"])
        assert resumed["status"] == "completed" and resumed["evidence"] == paused["evidence"]
        assert resumed["metrics"]["tools"] == resumed["metrics"]["steps"] == 1
        assert resumed["metrics"]["attempts"] == 2
        tool_events = [
            item for item in resumed["events"] if item["event_type"] == "tool.call.completed"
        ]
        assert sum(not item["detail"].get("replayed") for item in tool_events) == 1
        assert any(item["detail"].get("replayed") for item in tool_events)
        assert client.post(f"/api/runs/{paused['run_id']}/resume").status_code == 409
        checks["recovery"] = {
            **summarize(resumed),
            "evidence_identity_preserved": True,
            "tool_redispatches": 0,
        }
        saved = {view["run_id"]: view["result"] for view in (standard, parallel, resumed)}

    with server() as client:
        history = client.get("/api/runs").json()["runs"]
        assert {item["run_id"] for item in history} == set(saved)
        for run_id, result in saved.items():
            view = client.get(f"/api/runs/{run_id}").json()
            assert view["result"] == result and view["active"] is False
        checks["restart"] = {"persisted_runs": len(history), "results_unchanged": True}
        assert client.get(f"/api/evaluations/{imported['evaluation_id']}").json() == imported
        checks["evaluations"]["persists_after_restart"] = True
    # 在 POSIX 上，Uvicorn 会在生命周期关闭后重新触发已处理的信号。
    # 只有两次关闭都实际完成，信号退出才算有效。
    assert len(exits) == 2 and all(code in {0, -signal.SIGTERM} for code in exits), exits
    assert log_path.read_text().count("Application shutdown complete.") == 2
    return {
        "package_path": str(package),
        "python": sys.executable,
        "isolated_python": bool(sys.flags.isolated),
        "cli_exit_codes": exits,
        "checks": checks,
    }


def main():
    """关闭全部临时资源后，生成可移植的验证证据报告。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--allow-downloads", action="store_true")
    parser.add_argument("--probe", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--expected-version", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.probe:
        print(json.dumps(probe_installed(args.expected_version)))
        return
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    version = project["version"]
    wheel = (
        args.wheel or root / "dist" / f"lake_agent_runtime-{version}-py3-none-any.whl"
    ).resolve()
    report_path = (args.report or root / "docs" / "installed-wheel-web-2026-11-02.json").resolve()
    report = {
        "verified_at": datetime.now(UTC).isoformat(),
        "verification": "installed optional web wheel; loopback HTTP; synthetic ports; zero model requests",
        "runtime_version": version,
        "status": "running",
    }
    try:
        report["wheel"] = wheel_identity(root, wheel, version)
        with tempfile.TemporaryDirectory(prefix="comp7608-installed-web-") as directory:
            work = Path(directory).resolve()
            assert not work.is_relative_to(root), "Installation must be outside the checkout"
            checked_command(["uv", "venv", str(work / "env"), "--python", sys.executable], cwd=work)
            py = work / "env" / "bin" / "python"
            install = ["uv", "pip", "install", "--python", str(py)]
            if not args.allow_downloads:
                install.append("--offline")
            install.append(f"{wheel}[web]")
            checked_command(install, cwd=work, env=clean_environment())
            probe = work / "probe.py"
            shutil.copy2(Path(__file__), probe)
            result = checked_command(
                [str(py), "-I", str(probe), "--probe", "--expected-version", version],
                cwd=work,
                env=clean_environment(),
            )
            report["isolated_install"] = json.loads(result)
            report["outside_checkout"] = True
        report["temporary_environment_removed"] = not work.exists()
        # 检测构建后并发发生的源码修改，避免把过期 wheel
        # 认证为当前源码检出目录对应的发布工件。
        assert wheel_identity(root, wheel, version) == report["wheel"]
        report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({"report": str(report_path), "status": report["status"]}))


if __name__ == "__main__":
    main()
