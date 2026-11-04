"""隔离安装当前 wheel，验证包内容、配置解析与命令行入口。

先运行 ``uv build``，再从任意工作目录执行本脚本。默认仅使用 uv 缓存安装依赖；
``--allow-downloads`` 显式允许下载公开依赖。探测程序复制到源码目录之外，
通过安装环境中的 ``python -I`` 检查真实发布包，不借用本地源码或测试替身。

验证只解析临时配置、读取静态资源并检查命令行帮助和错误，不启动应用服务器、
进入应用生命周期、打开套接字或请求模型。临时配置仅包含测试专用凭据。
临时安装环境清理后写入 JSON 报告；本脚本不验证真实模块或回答质量。
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from datetime import UTC, datetime
from email import message_from_bytes
from pathlib import Path


def checked_command(args, *, cwd, env=None):
    """运行有时限的子进程；失败时保留安装和导入诊断。"""
    result = subprocess.run(
        args, cwd=cwd, env=env, text=True, capture_output=True, timeout=90, check=False
    )
    if result.returncode:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {args!r}\n{result.stdout}{result.stderr}"
        )
    return result.stdout.strip()


def clean_environment():
    """移除可能混入源码的 Python 导入配置，保留 uv 缓存和操作系统设置。"""
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env.pop("PYTHONOPTIMIZE", None)
    env["PYTHONNOUSERSITE"] = "1"
    return env


def wheel_identity(root, wheel, expected_version):
    """检查发行元数据、完整源码及静态资源，并拒绝演示或测试包。"""
    assets = {
        "lake_agent/web/static/index.html",
        "lake_agent/web/static/app.js",
        "lake_agent/web/static/styles.css",
    }
    sources = {
        path.relative_to(root / "src").as_posix()
        for path in (root / "src" / "lake_agent").rglob("*.py")
    }
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert len(names) == len(set(names)), "Wheel contains duplicate file paths"
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        assert len(metadata_names) == 1, "Wheel must contain exactly one distribution"
        metadata_name = metadata_names[0]
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
        assert dict(entrypoints["console_scripts"]) == {
            "lake-agent-web": "lake_agent.web.app:main"
        }, "Only the configured web entry point may be shipped"
        forbidden = [
            name
            for name in names
            if Path(name).name in {"demo.py", "fixtures.py"}
            or any(
                part in {"test", "tests", "testing", "demo", "fixtures", "__pycache__"}
                for part in Path(name).parts
            )
            or Path(name).name.startswith("test_")
        ]
        assert not forbidden, f"Development-only files were packaged: {forbidden}"
        packaged_sources = {
            name for name in names if name.startswith("lake_agent/") and name.endswith(".py")
        }
        assert packaged_sources == sources, "Packaged Python modules differ from the checkout"
        assert assets <= set(names), f"Missing packaged assets: {assets - set(names)}"
        hashes = {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in sorted(sources | assets)
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
        "development_only_packages_absent": True,
    }


def probe_installed(expected_version):
    """在隔离解释器中读取已安装包；不构造实际数据端口或模型客户端。"""
    import io
    from contextlib import redirect_stderr
    from importlib import metadata, util
    from unittest.mock import patch

    import uvicorn

    import lake_agent
    from lake_agent.adapters.planner import JsonPlanner
    from lake_agent.config import ConfigError, load_config
    from lake_agent.engine.graph_runner import build_workflow
    from lake_agent.engine.recovery import _implementation_digest
    from lake_agent.evaluation.models import EvaluationRow
    from lake_agent.runtime import resume, run
    from lake_agent.web.app import main as web_main

    work = Path.cwd().resolve()
    package = Path(lake_agent.__file__).resolve()
    assert sys.flags.isolated, "The installed probe requires python -I"
    assert package.is_relative_to(Path(sys.prefix).resolve()), package
    assert lake_agent.__version__ == metadata.version("lake-agent-runtime") == expected_version
    # 子包必须进入 wheel，公共执行入口继续可导入；这里只检查对象，不执行任务。
    assert JsonPlanner.agent_protocol == "agent-loop-v1"
    assert all(callable(item) for item in (run, resume, build_workflow, _implementation_digest))
    assert EvaluationRow(mode="fixed_retrieval", task_count=1).mode == "fixed_retrieval"
    for name in ("lake_agent.demo", "lake_agent.web.fixtures", "lake_agent.tests"):
        assert util.find_spec(name) is None, f"Development-only module is installed: {name}"
    entrypoints = {
        item.name: item.value
        for item in metadata.distribution("lake-agent-runtime").entry_points
        if item.group == "console_scripts"
    }
    assert entrypoints == {"lake-agent-web": "lake_agent.web.app:main"}, entrypoints
    cli = Path(sys.executable).with_name("lake-agent-web")
    assert cli.is_file(), cli
    help_text = checked_command([str(cli), "--help"], cwd=work, env=clean_environment())
    assert "--config" in help_text and "--port" in help_text and "--data-dir" in help_text

    # 未安装的工厂名称证明这里只做配置解析，不会为了加载配置导入团队模块。
    # .invalid 保留域名和专用假凭据不会成为真实模型连接目标。
    configuration = work / "configuration"
    configuration.mkdir()
    config_path = configuration / "settings.toml"
    config_path.write_text(
        """[llm]
base_url = "https://example.invalid/v1"
api_key = "installed-check-only-credential"
model_id = "installed-check-model"

[server]
data_dir = "records"

[profile]
profile_id = "installed-check"
title = "Installed configuration check"
dataset_id = "installed-check-data"
dataset_name = "Installed configuration check"
revision = "installed-check-v1"

[adapters.data]
factory = "uninstalled_team_data:create_port"

[adapters.content]
factory = "uninstalled_team_content:create_port"
""",
        encoding="utf-8",
    )
    config = load_config(config_path)
    assert config.server.data_dir == configuration / "records"
    assert not config.server.data_dir.exists(), "Configuration loading created runtime storage"
    assert config.run.model_id == config.llm.model_id == "installed-check-model"
    assert config.run.model_params == config.llm.model_params
    assert config.llm.api_key.get_secret_value() == "installed-check-only-credential"
    assert "installed-check-only-credential" not in config.model_dump_json()

    malformed = configuration / "malformed.toml"
    malformed.write_text("[llm\n", encoding="utf-8")
    missing = configuration / "missing.toml"
    rejected = []
    for name, path in (("missing", missing), ("malformed", malformed)):
        try:
            load_config(path)
        except ConfigError:
            pass
        else:
            raise AssertionError(f"The {name} configuration was accepted")
        # 直接调用已安装入口函数，并封住服务器启动入口。即使错误路径退化，
        # 本探测也只会失败，不会意外启动服务器或进入应用生命周期。
        error_output = io.StringIO()
        with (
            patch.object(sys, "argv", [str(cli), "--config", str(path)]),
            patch.object(uvicorn, "run", side_effect=AssertionError("Server startup is forbidden")),
            redirect_stderr(error_output),
        ):
            try:
                web_main()
            except SystemExit as error:
                assert error.code == 2, error.code
            else:
                raise AssertionError(f"The CLI accepted the {name} configuration")
        assert "error:" in error_output.getvalue()
        rejected.append(name)

    assets = {}
    for filename in ("index.html", "app.js", "styles.css"):
        payload = (package.parent / "web" / "static" / filename).read_bytes()
        assert payload, filename
        assets[filename] = {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
    return {
        "package_path": str(package),
        "python": sys.executable,
        "isolated_python": bool(sys.flags.isolated),
        "checks": {
            "installed_version": expected_version,
            "console_scripts": entrypoints,
            "cli_help": True,
            "configuration_load": True,
            "configuration_relative_data_dir": True,
            "configuration_creates_no_storage": True,
            "credential_serialization_redacted": True,
            "rejected_configurations": rejected,
            "static_files": assets,
            "development_only_modules_absent": True,
            "server_started": False,
            "model_requests": 0,
        },
    }


def main():
    """从当前项目版本定位 wheel，清理临时环境后写入验证报告。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--allow-downloads", action="store_true")
    parser.add_argument("--probe", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--expected-version", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.probe:
        if not args.expected_version:
            parser.error("The isolated probe requires an expected version")
        print(json.dumps(probe_installed(args.expected_version)))
        return
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    version = project["version"]
    wheel = (
        args.wheel or root / "dist" / f"lake_agent_runtime-{version}-py3-none-any.whl"
    ).resolve()
    report_path = (args.report or root / "runs" / "installed-web-check.json").resolve()
    report = {
        "verified_at": datetime.now(UTC).isoformat(),
        "verification": "isolated installed web wheel; configuration and CLI checks; no server or model requests",
        "runtime_version": version,
        "dependency_downloads_allowed": args.allow_downloads,
        "status": "running",
    }
    temporary_root = None
    try:
        report["wheel"] = wheel_identity(root, wheel, version)
        with tempfile.TemporaryDirectory(prefix="comp7608-installed-web-") as directory:
            work = Path(directory).resolve()
            temporary_root = work
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
        # 再次校验源码与 wheel，防止并发编辑使刚验证的包与交付源码不一致。
        assert wheel_identity(root, wheel, version) == report["wheel"]
        report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        # TemporaryDirectory 会在异常路径上同样清理；报告也保留该结果。
        if temporary_root is not None:
            report["temporary_environment_removed"] = not temporary_root.exists()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps({"report": str(report_path), "status": report["status"]}))


if __name__ == "__main__":
    main()
