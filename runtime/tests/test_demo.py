"""离线演示适配器的契约测试，无需网络或轨迹产物。

这些检查保护公开的 ToolResult 截断标志，以及下游集成收到的合成证据载荷。
"""

import pytest

from lake_agent.demo import DemoData
from lake_agent.models import ToolRequest


@pytest.mark.parametrize(
    "source_id,full_text",
    [
        ("demo-annual", "Synthetic fixture: 2024 annual revenue was 120 million HKD."),
        ("demo-quarter", "Synthetic fixture: reporting year was 2024."),
    ],
)
@pytest.mark.parametrize("truncate", [True, False], ids=["one-character", "exact-length"])
async def test_extract_reports_whether_payload_was_truncated(source_id, full_text, truncate):
    # 恰好等于文本长度的额度必须保留完整文本，且不标记为截断。
    max_chars = 1 if truncate else len(full_text)
    request = ToolRequest(
        run_id="demo-contract",
        task_id="demo-contract",
        operation_id="extract-operation",
        call_id="extract-call",
        timeout_ms=1000,
        max_output_bytes=65536,
        step_id="extract-step",
        catalog_snapshot_id="demo-v1",
        tool_name="extract",
        args={"source_id": source_id, "query": "revenue", "max_chars": max_chars},
    )

    result = await DemoData().invoke(request)

    assert result.status == "ok"
    assert result.data["evidence"][0]["payload"] == ("S" if truncate else full_text)
    assert result.truncated is truncate
