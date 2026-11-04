"""测试自己的故障注入；不启动监听服务，不请求外部模型。"""

import pytest


@pytest.fixture(autouse=True)
def isolate_web_fault_injection(monkeypatch):
    """每项测试拥有独立故障记录；生产轨迹仅记录运行事实。"""
    from lake_agent.web import service
    from tests.support.profiles import interrupting_trace

    monkeypatch.setattr(service, "PresentationTrace", interrupting_trace(service.PresentationTrace))
