"""内容端口请求、证据样本及模型响应构造器。

只使用显式测试端口、临时目录和内存传输；测试场景不代表真实模型质量。
"""

from __future__ import annotations

import json

from lake_agent import models as m
from tests.support.ports import DemoData


def evidence():
    return [
        m.EvidenceRecord(
            evidence_id="evidence-annual",
            call_id="extract-annual",
            source_id="demo-annual",
            content_type="text",
            payload="Synthetic fixture: 2024 annual revenue was 120 million HKD.",
            locator={"line": 1},
        ),
        m.EvidenceRecord(
            evidence_id="evidence-quarter",
            call_id="extract-quarter",
            source_id="demo-quarter",
            content_type="text",
            payload="Synthetic fixture: reporting year was 2024.",
            locator={"line": 1},
        ),
    ]


def compose_request(question="目前的合成资料有哪些", **changes):
    fields = dict(
        run_id="isolated-run",
        task_id="private-task",
        operation_id="compose",
        call_id="compose-call",
        timeout_ms=1000,
        max_output_bytes=16000,
        model_id="configured-model",
        model_params={"temperature": 0.25, "seed": 7},
        question=question,
        evidence=evidence(),
    )
    fields.update(changes)
    return m.ComposeRequest(**fields)


def verify_request(**changes):
    fields = compose_request().model_dump(exclude={"completed_subtasks"})
    fields.update(
        answer=m.AnswerRecord(text="模型提供的答案", citations=["evidence-annual"]),
        call_id="verify-call",
        operation_id="verify",
    )
    fields.update(changes)
    return m.VerifyRequest(**fields)


async def register(content, run_id="isolated-run"):
    return await content.prepare_catalog(
        m.CatalogRegistrationRequest(
            run_id=run_id,
            task_id="private-task",
            operation_id="register",
            call_id="register-call",
            timeout_ms=1000,
            max_output_bytes=16000,
            catalog_snapshot_id="demo-v1",
            catalog_digest="fixture-digest",
            sources=DemoData().sources,
        )
    )


def envelope(value, usage=None):
    payload = {"choices": [{"message": {"content": json.dumps(value)}, "finish_reason": "stop"}]}
    if usage is not None:
        payload["usage"] = usage
    return payload
