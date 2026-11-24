"""用合成文件和模拟提供方复现三组离线集成实验。"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from lake_agent import models as m
from lake_agent.adapters.content_adapter import JsonVerifierAdapter
from lake_agent.adapters.data_adapter import create_port as create_data_port
from lake_agent.config import LLMSettings
from lake_agent.trace import JsonlTraceSink
from lake_agent.validation import canonical_json
from lake_agent.web.profiles import ExecutionPorts
from tests.ablation_runtime import _tasks, _write
from tests.support.ablation import run_ablation
from tests.support.content_requests import envelope
from tests.support.lakeqa import build_step

QUESTIONS = {
    "policy": "What was the warehouse shipping policy?",
    "revenue": "What were the total sales revenue and currency?",
    "inventory": "What was the current inventory quantity?",
    "temperature": "What was yesterday's greenhouse temperature?",
    "capacity": "What was the current storage capacity?",
}
FILES = {
    "shipping-policy.txt": "Warehouse shipping policy: Earth-friendly packaging.\n",
    "sales-revenue.csv": "description,amount\nsales,50\nsales,70\n",
    "currency.json": '{"report":"Sales currency is HKD."}\n',
    "inventory-quantity.txt": "Current inventory quantity: 34.\n",
    "storage-capacity.txt": "Current storage capacity: 18.\n",
}
TASKS = [
    {
        "task_id": "policy",
        "question": QUESTIONS["policy"],
        "expected_answers": ["Earth-friendly packaging"],
        "relevant_source_ids": ["shipping-policy.txt"],
        "not_applicable_metrics": ["numerical_accuracy", "structured_accuracy"],
    },
    {
        "task_id": "revenue",
        "question": QUESTIONS["revenue"],
        "expected_answers": ["120 HKD"],
        "numeric_answer": 120,
        "structured_answer_json": "120",
        "relevant_source_ids": ["sales-revenue.csv", "currency.json"],
    },
    {
        "task_id": "inventory",
        "question": QUESTIONS["inventory"],
        "expected_answers": ["34"],
        "numeric_answer": 34,
        "structured_answer_json": "34",
        "relevant_source_ids": ["inventory-quantity.txt"],
    },
    {
        "task_id": "temperature",
        "question": QUESTIONS["temperature"],
        "expected_answers": ["20"],
        "numeric_answer": 20,
        "structured_answer_json": "20",
        "relevant_source_ids": [],
    },
    {
        "task_id": "capacity",
        "question": QUESTIONS["capacity"],
        "expected_answers": ["18"],
        "numeric_answer": 18,
        "structured_answer_json": "18",
        "relevant_source_ids": ["storage-capacity.txt"],
    },
]
SETTINGS = LLMSettings(
    base_url="https://fixture.invalid/v1", api_key="fixture-only", model_id="mock-evidence-v1"
)
UNRESOLVED = "Insufficient evidence."


def extract(source: str, query: str) -> m.AgentToolStep:
    """使用当前目录中的已知测试文件构造正常抽取请求。"""
    return m.AgentToolStep(
        step_id=f"extract-{source}",
        tool_name="extract",
        source_ids=[source],
        args={"source_id": source, "query": query, "max_chars": 1000},
        purpose="Read synthetic fixture evidence",
    )


def tools(case: str) -> list[m.AgentToolStep]:
    """确定性规划器使用预设工具动作覆盖异构数据路径。"""
    if case == "revenue":
        source = "sales-revenue.csv"
        return [
            m.AgentToolStep(
                step_id="sum-revenue",
                tool_name="query",
                source_ids=[source],
                args={"source_id": source, "sql": "SELECT SUM(amount) AS total FROM data"},
                purpose="Aggregate synthetic sales rows",
            ),
            extract("currency.json", "currency"),
        ]
    source, term = {
        "policy": ("shipping-policy.txt", "shipping policy"),
        "inventory": ("inventory-quantity.txt", "inventory quantity"),
        "capacity": ("storage-capacity.txt", "storage capacity"),
    }[case]
    return [extract(source, term)]


class ControlledPlanner:
    """固定各场景的决策，依据真实控制器的反馈做一次修正。"""

    agent_protocol = m.AGENT_PROTOCOL

    async def decide(self, request):
        case = next(key for key, question in QUESTIONS.items() if question == request.question)
        last_action = request.context.get("last_action") or {}
        stage = last_action.get("kind")
        if stage is None:
            action = m.AgentSearchDecision(query=request.question)
        elif stage == "search":
            action = (
                m.AgentComposeDecision()
                if case == "temperature"
                else m.AgentToolsDecision(steps=tools(case))
            )
        elif stage == "tools" or (stage == "compose" and last_action["status"] == "rejected"):
            action = m.AgentComposeDecision()
        elif stage == "compose":
            action = (
                m.AgentVerifyDecision() if request.require_verification else m.AgentFinishDecision()
            )
        elif stage == "verify" and request.context["verification_verdict"] == "conflicting":
            action = m.AgentComposeDecision(revision_instructions="Recheck the inventory quantity.")
        elif stage == "verify" and request.context["verification_verdict"] == "supported":
            action = m.AgentFinishDecision()
        else:
            action = m.AgentStopDecision(reason="No supporting evidence in the fixture data")
        return m.ModuleResult(call_id=request.call_id, data=action, usage=m.Usage.local())


def evidence_answer(case: str, evidence: list[dict]) -> dict:
    """通过请求中实际检索出的证据构造模拟答案，不查询清单标注。"""
    records = {item["source_id"]: item for item in evidence}
    if case == "revenue":
        table = records.get("sales-revenue.csv")
        currency = records.get("currency.json")
        if table and currency and isinstance(table["payload"], dict):
            number = table["payload"]["rows"][0][0]
            unit = re.search(r"currency is (\w+)", currency["payload"]).group(1)
            return {
                "text": f"{number} {unit}",
                "structured_result": number,
                "citations": [table["evidence_id"], currency["evidence_id"]],
            }
    elif case in {"policy", "inventory", "capacity"}:
        source = {
            "policy": "shipping-policy.txt",
            "inventory": "inventory-quantity.txt",
            "capacity": "storage-capacity.txt",
        }[case]
        if record := records.get(source):
            text = record["payload"].split(": ", 1)[1].rstrip(".")
            return {
                "text": text,
                "structured_result": int(text) if text.isdecimal() else None,
                "citations": [record["evidence_id"]],
            }
    return {"text": UNRESOLVED, "citations": []}


def provider(request: httpx.Request) -> httpx.Response:
    """用断言和证据函数模拟提供方，并在指定场景制造已知负例。"""
    assert request.url.path == "/v1/chat/completions"
    body = json.loads(request.content)
    payload = json.loads(body["messages"][1]["content"])
    case = next(key for key, question in QUESTIONS.items() if question == payload["question"])
    evidence = payload["evidence"]
    expected = evidence_answer(case, evidence)
    if "answer" in payload:
        answer = payload["answer"]
        supported = answer["text"] == expected["text"] and set(answer["citations"]) == set(
            expected["citations"]
        )
        reply = {
            "verdict": "supported" if supported else "conflicting",
            "recommendation": {"action": "none"},
        }
        if not supported:
            reply["issues"] = [{"type": "value_mismatch", "detail": "Recheck the cited quantity."}]
    else:
        reply = expected
        if case == "inventory" and reply["citations"] and not payload.get("revision"):
            wrong_number = reply["structured_result"] + 1
            reply = {**reply, "text": str(wrong_number), "structured_result": wrong_number}
        if case == "capacity" and reply["citations"] and not payload.get("feedback"):
            reply = {**reply, "citations": ["unregistered-evidence"]}
    return httpx.Response(200, json=envelope(reply, {"prompt_tokens": 40, "completion_tokens": 10}))


def write_fixture(directory: Path) -> tuple[Path, Path]:
    """冻结合成文件和评分输入。"""
    directory.mkdir(parents=True, exist_ok=False)
    data = directory / "fixture"
    data.mkdir()
    for name, text in FILES.items():
        (data / name).write_text(text, encoding="utf-8")
    manifest = directory / "manifest.json"
    manifest.write_bytes(canonical_json(TASKS))
    return data, manifest


async def run(directory: Path) -> None:
    """沿用正式评估入口，运行三组并保存审计和指标。"""
    data, manifest = await asyncio.to_thread(write_fixture, directory)

    @asynccontextmanager
    async def ports():
        async with (
            create_data_port(options={"base_dir": str(data)}) as data_port,
            httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http,
            JsonVerifierAdapter(options={}, llm=SETTINGS, client=http) as content_port,
        ):
            yield ExecutionPorts(data=data_port, content=content_port, planner=ControlledPlanner())

    outcome = await run_ablation(
        _tasks(manifest),
        ports_factory=ports,
        config=m.RunConfig(model_id=SETTINGS.model_id, candidate_top_k=3),
        step_builder=build_step,
        dataset_id="synthetic-files",
        task_set_id="controlled-integration-v1",
        title="Controlled offline integration",
        repeats=3,
        trace_factory=lambda: JsonlTraceSink(directory / "audit"),
    )
    _write(directory, outcome)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    options = parser.parse_args()
    asyncio.run(run(options.output.resolve()))
