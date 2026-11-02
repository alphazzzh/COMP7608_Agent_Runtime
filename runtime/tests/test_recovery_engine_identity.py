"""图引擎升级后不得复用不兼容的持久化执行。

仅凭源代码哈希无法检测安装的调度器版本变化。这些测试只修改包元数据，
不编辑源代码或持久化运行记录，并要求恢复流程的实现身份随引擎变化。
"""

from importlib import metadata

import pytest

from lake_agent.recovery import _implementation_digest


@pytest.mark.parametrize(
    "package", ["langgraph", "langchain-core", "langgraph-checkpoint", "langsmith"]
)
def test_graph_dependency_version_changes_recovery_identity(monkeypatch, package):
    """仅变更引擎版本也必须被源代码兼容性关卡拦截。"""
    original = metadata.version
    before = _implementation_digest()

    def changed_version(name):
        if name == package:
            return "999.0.0-test"
        return original(name)

    monkeypatch.setattr(metadata, "version", changed_version)
    assert _implementation_digest() != before
