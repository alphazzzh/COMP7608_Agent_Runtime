"""不依赖 Web 的三组评估报告模型及生产运行模式隔离。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import importlib
import subprocess
import sys

import pytest
from pydantic import ValidationError

from lake_agent.models import RunRequest


def test_evaluation_schema_imports_without_web_or_fastapi():
    """评估脚本读取三组报告无需安装或加载网页层。"""
    program = """
import importlib.abc
import sys

class RejectWeb(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "lake_agent.web" or fullname.startswith("lake_agent.web."):
            raise AssertionError("evaluation schema imported the Web layer")
        if fullname == "fastapi" or fullname.startswith("fastapi."):
            raise AssertionError("evaluation schema imported FastAPI")
        return None

sys.meta_path.insert(0, RejectWeb())
from lake_agent.evaluation.models import EvaluationRow

for mode in ("fixed_retrieval", "no_verifier", "full_agent"):
    assert EvaluationRow(mode=mode, task_count=1).mode == mode
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program],
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_report_baseline_does_not_become_a_production_run_mode():
    """固定检索标签只描述外部对照结果，不能通过正式 RunRequest 执行。"""
    schema = importlib.import_module("lake_agent.evaluation.models")
    assert schema.EvaluationRow(mode="fixed_retrieval", task_count=1).mode == "fixed_retrieval"
    with pytest.raises(ValidationError):
        RunRequest(task_id="task", question="Question", mode="fixed_retrieval")
