"""消融测试的显式工具映射，仅供测试宿主导入。

只使用显式测试端口、临时目录和内存传输；测试场景不代表真实模型质量。
"""

from __future__ import annotations

from lake_agent import models as m


def build_step(candidate, specs, question, index):
    """评估宿主显式映射 A 工具；不进入生产包或读取 gold。"""
    assert any(spec.name == "extract" for spec in specs)
    return m.AgentToolStep(
        step_id=f"fixed-{index}",
        tool_name="extract",
        source_ids=[candidate.source_id],
        args={"source_id": candidate.source_id, "query": question, "max_chars": 1000},
        purpose="Read fixed top-k source",
    )
