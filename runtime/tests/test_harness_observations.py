"""Offline integration regressions for the controller's tool-observation loop.

These tests exercise the public ``run`` entry point and its persisted artifacts.
Only the external data/content/planner ports are deterministic local fixtures;
no network, model credentials or production-private controller APIs are needed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lake_agent import models as m
from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink


def reply(request, payload):
    """Return a normal local module result with explicit zero provider use."""
    return m.ModuleResult(call_id=request.call_id, data=payload, usage=m.Usage.local())


def step(request, tool_name, *, suffix="", table="annual", depends_on=None):
    """Build authorized actions; each new plan deliberately gets fresh step IDs."""
    args = {"source_id": "demo-annual"}
    if tool_name == "query":
        args["table"] = table
    return m.ToolStep(
        step_id=f"{request.call_id}-{tool_name}{suffix}",
        subtask_id=request.subtask.subtask_id,
        tool_name=tool_name,
        source_ids=["demo-annual"],
        args=args,
        depends_on=depends_on or [],
        purpose="Inspect the source schema or query its declared table",
    )


class InspectQueryData(DemoData):
    """A source whose schema is useful for planning but is never evidence."""

    def __init__(self, *, schema=None, payload="Annual revenue: 120 million HKD."):
        super().__init__()
        self.schema = schema if schema is not None else {"tables": ["annual"]}
        self.payload = payload
        self.invocations = []

    def tool_specs(self):
        specs = super().tool_specs()
        for name in ("inspect", "query"):
            properties = {"source_id": {"type": "string"}}
            if name == "query":
                properties["table"] = {"type": "string"}
            specs.append(
                m.ToolSpec(
                    name=name,
                    description=f"Read-only local {name} fixture",
                    input_schema={
                        "type": "object",
                        "properties": properties,
                        "required": list(properties),
                        "additionalProperties": False,
                    },
                    idempotent=True,
                    uses_sources=True,
                    source_id_paths=["/source_id"],
                )
            )
        return specs

    async def invoke(self, request):
        self.invocations.append(request)
        if request.tool_name == "extract":
            return await super().invoke(request)
        if request.tool_name == "inspect":
            return m.ToolResult(call_id=request.call_id, data={"schema": self.schema})
        return m.ToolResult(
            call_id=request.call_id,
            data={
                "evidence": [
                    {
                        "source_id": "demo-annual",
                        "content_type": "text",
                        "payload": self.payload,
                        "locator": {"table": request.args["table"], "row": 1},
                    }
                ]
            },
        )


class RecordingContent(DemoContent):
    """Retain real compose/verify inputs to check full evidence preservation."""

    def __init__(self):
        super().__init__()
        self.compositions = []
        self.verifications = []

    async def compose(self, request):
        self.compositions.append(request)
        return await super().compose(request)

    async def verify(self, request):
        self.verifications.append(request)
        return await super().verify(request)


async def execute(tmp_path, *, planner, data=None, content=None, mode="no_verifier", **config):
    """Run the real controller with filesystem traces and independent local ports."""
    return await run(
        m.RunRequest(
            task_id="observation-integration",
            question="What was annual revenue?",
            mode=mode,
            config=m.RunConfig(**config),
        ),
        data=data if data is not None else InspectQueryData(),
        content=content if content is not None else RecordingContent(),
        planner=planner,
        trace=JsonlTraceSink(tmp_path),
    )


def events(result):
    return [json.loads(line) for line in Path(result.trace_ref).read_text().splitlines()]


def canonical(value):
    """Measure the specified JSON wire format, including non-ASCII UTF-8 bytes."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class QueryThenFinish(DemoPlanner):
    """Collect requests while obtaining exactly one evidence-bearing result."""

    def __init__(self):
        self.requests = []

    async def select_tools(self, request):
        self.requests.append(request)
        decision = (
            m.FinishDecision(reason="evidence_complete")
            if request.evidence_summary
            else m.ToolPlan(steps=[step(request, "query")])
        )
        return reply(request, decision)


async def test_inspect_schema_is_visible_to_next_planner_and_query_supplies_evidence(tmp_path):
    class InspectThenQuery(DemoPlanner):
        def __init__(self):
            self.requests = []

        async def select_tools(self, request):
            self.requests.append(request)
            observations = getattr(request, "tool_observations", [])
            if request.evidence_summary:
                decision = m.FinishDecision(reason="evidence_complete")
            elif observations:
                table = observations[-1]["result"]["data"]["schema"]["tables"][0]
                decision = m.ToolPlan(steps=[step(request, "query", table=table)])
            elif len(self.requests) == 1:
                decision = m.ToolPlan(steps=[step(request, "inspect")])
            else:
                decision = m.FinishDecision(reason="insufficient_evidence")
            return reply(request, decision)

    planner, data, content = InspectThenQuery(), InspectQueryData(), RecordingContent()
    result = await execute(tmp_path, planner=planner, data=data, content=content)

    assert result.status == "completed"
    assert [item.tool_name for item in data.invocations] == ["inspect", "query"]
    observation = planner.requests[1].tool_observations[0]
    assert observation["observation_id"]
    assert observation["tool_name"] == "inspect"
    assert observation["subtask_id"] == "annual-revenue"
    assert observation["status"] == "ok"
    assert len(content.compositions[0].evidence) == 1
    assert content.compositions[0].evidence[0].payload == "Annual revenue: 120 million HKD."


async def test_planner_context_is_byte_bounded_while_compose_and_verify_keep_full_evidence(
    tmp_path,
):
    # Multi-byte characters catch implementations that bound Python characters
    # rather than the promised UTF-8 canonical JSON transport size.
    payload = "香港年度收入資料。" * 1200
    planner, content = QueryThenFinish(), RecordingContent()
    result = await execute(
        tmp_path,
        planner=planner,
        data=InspectQueryData(payload=payload),
        content=content,
        mode="full_agent",
        max_planner_context_bytes=4096,
        planner_preview_bytes=512,
    )

    assert result.status == "completed"
    assert len(planner.requests) == 2
    for request in planner.requests:
        context = request.model_dump(
            mode="json",
            include={"evidence_summary", "tool_observations", "context_status", "observation_read"},
        )
        assert len(canonical(context).encode("utf-8")) <= 4096
    assert payload not in canonical(planner.requests[-1].model_dump(mode="json"))
    assert planner.requests[-1].context_status
    assert content.compositions[0].evidence[0].payload == payload
    assert content.verifications[0].evidence[0].payload == payload

    artifacts = [
        json.loads(path.read_text())
        for path in (tmp_path / result.run_id / "artifacts").glob("*.json")
    ]
    assert any(
        isinstance(value, dict)
        and value.get("data", {}).get("evidence", [{}])[0].get("payload") == payload
        for value in artifacts
        if isinstance(value, dict)
        and isinstance(value.get("data"), dict)
        and value["data"].get("evidence")
    )


async def test_observation_read_pages_hidden_schema_without_another_data_call(tmp_path):
    fields = [f"column_{index:02d}_香港" for index in range(45)] + ["annual_final"]
    data = InspectQueryData(
        schema={
            "padding": "schema documentation " * 600,
            "zz/fields": {"~target": fields},
        }
    )

    class ReadSchema(DemoPlanner):
        def __init__(self):
            self.requests = []
            self.pages = []
            self.observation_id = None
            self.preview = None

        async def select_tools(self, request):
            self.requests.append(request)
            if request.evidence_summary:
                return reply(request, m.FinishDecision(reason="evidence_complete"))
            if not request.tool_observations:
                return reply(request, m.ToolPlan(steps=[step(request, "inspect")]))
            if self.observation_id is None:
                observation = request.tool_observations[-1]
                self.observation_id = observation["observation_id"]
                self.preview = canonical(observation)
            offset = 0
            if request.observation_read is not None:
                page = request.observation_read
                self.pages.append(page)
                if not page["truncated"]:
                    table = json.loads("".join(item["text"] for item in self.pages))[-1]
                    return reply(request, m.ToolPlan(steps=[step(request, "query", table=table)]))
                offset = page["next_offset"]
            return reply(
                request,
                m.ReadObservationDecision(
                    observation_id=self.observation_id,
                    pointer="/data/schema/zz~1fields/~0target",
                    offset=offset,
                    max_chars=256,
                ),
            )

    planner = ReadSchema()
    result = await execute(
        tmp_path,
        planner=planner,
        data=data,
        max_planner_context_bytes=4096,
        planner_preview_bytes=512,
    )

    assert result.status == "completed"
    assert "annual_final" not in planner.preview
    assert len(planner.pages) > 1
    assert json.loads("".join(page["text"] for page in planner.pages)) == fields
    assert all(page["observation_id"] == planner.observation_id for page in planner.pages)
    assert all(page["pointer"] == "/data/schema/zz~1fields/~0target" for page in planner.pages)
    assert planner.pages[0]["offset"] == 0
    assert planner.pages[-1]["total_chars"] == len(canonical(fields))
    for previous, following in zip(planner.pages, planner.pages[1:], strict=False):
        assert previous["next_offset"] == following["offset"]
    assert [item.tool_name for item in data.invocations] == ["inspect", "query"]
    assert data.invocations[-1].args["table"] == "annual_final"
    assert result.call_counts.observation_reads == len(planner.pages)
    assert result.call_counts.tools == result.call_counts.steps == 2


@pytest.mark.parametrize("foreign", [False, True], ids=["unknown", "cross-run"])
async def test_observation_read_rejects_unknown_or_other_run_ids(tmp_path, foreign):
    class CaptureObservation(DemoPlanner):
        def __init__(self):
            self.observation_id = None

        async def select_tools(self, request):
            if not request.tool_observations:
                decision = m.ToolPlan(steps=[step(request, "inspect")])
            else:
                self.observation_id = request.tool_observations[0]["observation_id"]
                decision = m.FinishDecision(reason="insufficient_evidence")
            return reply(request, decision)

    observation_id = "observation-that-does-not-exist"
    if foreign:
        origin = CaptureObservation()
        await execute(tmp_path, planner=origin)
        observation_id = origin.observation_id
        assert observation_id

    class ForeignReader(DemoPlanner):
        async def select_tools(self, request):
            return reply(request, m.ReadObservationDecision(observation_id=observation_id))

    data = InspectQueryData()
    result = await execute(tmp_path, planner=ForeignReader(), data=data)

    assert (result.status, result.stop_reason) == ("failed", "invalid_observation_read")
    assert result.call_counts.catalog == 1
    assert result.call_counts.tools == result.call_counts.steps == 0
    assert data.invocations == []
    assert result.answer is None


@pytest.mark.parametrize(
    "pointer",
    ["data/schema", "/data/schema/absent", "/data/schema/tables/01", "/data/schema/~9bad"],
)
async def test_observation_read_rejects_invalid_rfc6901_pointers(tmp_path, pointer):
    class InvalidPointer(DemoPlanner):
        async def select_tools(self, request):
            if not request.tool_observations:
                decision = m.ToolPlan(steps=[step(request, "inspect")])
            else:
                decision = m.ReadObservationDecision(
                    observation_id=request.tool_observations[-1]["observation_id"],
                    pointer=pointer,
                )
            return reply(request, decision)

    data = InspectQueryData()
    result = await execute(tmp_path, planner=InvalidPointer(), data=data)

    assert (result.status, result.stop_reason) == ("failed", "invalid_observation_read")
    assert [item.tool_name for item in data.invocations] == ["inspect"]
    assert result.call_counts.tools == result.call_counts.steps == 1


async def test_observation_read_budget_stops_local_loop_without_another_tool_dispatch(tmp_path):
    class RepeatedReader(DemoPlanner):
        async def select_tools(self, request):
            if not request.tool_observations:
                decision = m.ToolPlan(steps=[step(request, "inspect")])
            else:
                decision = m.ReadObservationDecision(
                    observation_id=request.tool_observations[-1]["observation_id"],
                    pointer="/data/schema",
                )
            return reply(request, decision)

    data = InspectQueryData()
    result = await execute(
        tmp_path,
        planner=RepeatedReader(),
        data=data,
        max_observation_reads=1,
    )

    assert (result.status, result.stop_reason) == ("budget_exceeded", "observation_read_budget")
    assert result.call_counts.observation_reads == 1
    assert result.call_counts.tools == result.call_counts.steps == 1
    assert [item.tool_name for item in data.invocations] == ["inspect"]


@pytest.mark.parametrize("mode", ["no_verifier", "full_agent"])
async def test_recoverable_tool_error_replans_changed_args_without_becoming_evidence(
    tmp_path, mode
):
    class WrongTableData(InspectQueryData):
        async def invoke(self, request):
            if request.args.get("table") == "unknown":
                self.invocations.append(request)
                return m.ToolResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(
                        code="unknown_table",
                        message="Use the annual table",
                        recoverable=True,
                    ),
                )
            return await super().invoke(request)

    class CorrectTable(DemoPlanner):
        def __init__(self):
            self.requests = []

        async def select_tools(self, request):
            self.requests.append(request)
            if request.evidence_summary:
                decision = m.FinishDecision(reason="evidence_complete")
            else:
                table = "annual" if request.tool_observations else "unknown"
                decision = m.ToolPlan(steps=[step(request, "query", table=table)])
            return reply(request, decision)

    planner, data, content = CorrectTable(), WrongTableData(), RecordingContent()
    result = await execute(tmp_path, planner=planner, data=data, content=content, mode=mode)

    assert result.status == "completed"
    assert [item.args["table"] for item in data.invocations] == ["unknown", "annual"]
    failed_observation = planner.requests[1].tool_observations[-1]
    assert failed_observation["status"] == "error"
    assert failed_observation["result"]["error"]["code"] == "unknown_table"
    assert failed_observation["result"]["error"]["recoverable"] is True
    assert planner.requests[1].evidence_summary == []
    assert len(content.compositions[0].evidence) == 1
    assert "unknown_table" not in canonical(content.compositions[0].model_dump(mode="json"))
    assert result.call_counts.tools == result.call_counts.steps == 2


async def test_failed_plan_marks_dependents_blocked_and_independent_steps_skipped(tmp_path):
    class RecoverableData(InspectQueryData):
        async def invoke(self, request):
            self.invocations.append(request)
            return m.ToolResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(
                    code="bad_query", message="Choose another table", recoverable=True
                ),
            )

    class DependentPlan(DemoPlanner):
        def __init__(self):
            self.planned = []

        async def select_tools(self, request):
            if request.tool_observations:
                return reply(request, m.FinishDecision(reason="insufficient_evidence"))
            first = step(request, "query", suffix="-failed")
            child = step(request, "query", suffix="-child", depends_on=[first.step_id])
            grandchild = step(request, "query", suffix="-grandchild", depends_on=[child.step_id])
            independent = step(request, "inspect", suffix="-independent")
            self.planned = [first, child, grandchild, independent]
            return reply(request, m.ToolPlan(steps=self.planned))

    planner, data = DependentPlan(), RecoverableData()
    result = await execute(tmp_path, planner=planner, data=data)

    assert result.status == "insufficient_evidence"
    assert len(data.invocations) == 1
    terminal_states = {
        event["step_id"]: event["status"]
        for event in events(result)
        if event["event_type"] == "step.state" and event["status"] != "running"
    }
    first, child, grandchild, independent = planner.planned
    assert terminal_states[first.step_id] == "failed"
    assert terminal_states[child.step_id] == terminal_states[grandchild.step_id] == "blocked"
    assert terminal_states[independent.step_id] == "skipped"


async def test_identical_actions_stop_before_third_dispatch_despite_fresh_step_ids(tmp_path):
    class LoopingPlanner(DemoPlanner):
        async def select_tools(self, request):
            return reply(request, m.ToolPlan(steps=[step(request, "inspect")]))

    data = InspectQueryData()
    result = await execute(tmp_path, planner=LoopingPlanner(), data=data)

    assert (result.status, result.stop_reason) == ("failed", "repeated_tool_call")
    assert len(data.invocations) == 2
    assert len({item.step_id for item in data.invocations}) == 2
    assert result.call_counts.tools == result.call_counts.steps == 2
    running = [
        event
        for event in events(result)
        if event["event_type"] == "step.state" and event["status"] == "running"
    ]
    assert len(running) == 2


async def test_transport_retry_is_not_counted_as_an_identical_logical_action(tmp_path):
    class RetryOnce(InspectQueryData):
        async def invoke(self, request):
            if not self.invocations:
                self.invocations.append(request)
                return m.ToolResult(
                    call_id=request.call_id,
                    status="error",
                    error=m.ErrorInfo(code="busy", message="Temporary", retryable=True),
                )
            return await super().invoke(request)

    data = RetryOnce()
    result = await execute(
        tmp_path,
        planner=QueryThenFinish(),
        data=data,
        max_identical_tool_calls=1,
    )

    assert result.status == "completed"
    assert result.call_counts.tools == 2
    assert result.call_counts.retries == 1
    assert result.call_counts.steps == 1
    assert data.invocations[0].operation_id == data.invocations[1].operation_id
    assert data.invocations[0].call_id != data.invocations[1].call_id


@pytest.mark.parametrize("recoverable", [False, True])
async def test_fixed_retrieval_keeps_fatal_error_semantics(tmp_path, recoverable):
    class FailedExtract(InspectQueryData):
        async def invoke(self, request):
            self.invocations.append(request)
            return m.ToolResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(
                    code="extract_failed",
                    message="No parseable text",
                    recoverable=recoverable,
                ),
            )

    data, content = FailedExtract(), RecordingContent()
    result = await execute(
        tmp_path,
        planner=DemoPlanner(),
        data=data,
        content=content,
        mode="fixed_retrieval",
    )

    assert (result.status, result.stop_reason) == ("failed", "tool_error")
    assert len(data.invocations) == 1
    assert result.call_counts.planner == result.call_counts.observation_reads == 0
    assert content.compositions == []


async def test_inspected_source_remains_authorized_when_search_excludes_visited(tmp_path):
    class UnvisitedOnly(RecordingContent):
        async def search(self, request):
            result = await super().search(request)
            result.data.candidates = [
                c for c in result.data.candidates if c.source_id not in request.visited_source_ids
            ]
            return result

    class InspectThenQuery(DemoPlanner):
        async def select_tools(self, request):
            if request.evidence_summary:
                choice = m.FinishDecision(reason="evidence_complete")
            else:
                choice = m.ToolPlan(
                    steps=[step(request, "query" if request.tool_observations else "inspect")]
                )
            return reply(request, choice)

    data = InspectQueryData()
    result = await execute(tmp_path, planner=InspectThenQuery(), data=data, content=UnvisitedOnly())
    assert result.status == "completed"
    assert [r.tool_name for r in data.invocations] == ["inspect", "query"]


async def test_read_pages_retain_bounded_history_for_stateless_planner(tmp_path):
    class ReadHistory(DemoPlanner):
        def __init__(self):
            self.requests = []

        async def select_tools(self, request):
            self.requests.append(request)
            if not request.tool_observations:
                decision = m.ToolPlan(steps=[step(request, "inspect")])
            elif request.observation_read is None:
                decision = m.ReadObservationDecision(
                    observation_id=request.tool_observations[0]["observation_id"],
                    pointer="/data/schema",
                    max_chars=20,
                )
            elif request.observation_read["offset"] == 0:
                page = request.observation_read
                decision = m.ReadObservationDecision(
                    observation_id=page["observation_id"],
                    pointer=page["pointer"],
                    offset=page["next_offset"],
                    max_chars=20,
                )
            else:
                decision = m.FinishDecision(reason="insufficient_evidence")
            return reply(request, decision)

    planner = ReadHistory()
    result = await execute(
        tmp_path,
        planner=planner,
        data=InspectQueryData(schema={"columns": ["first", "second", "third", "fourth"]}),
    )
    assert result.status == "insufficient_evidence"
    last_page = planner.requests[-1].observation_read
    assert last_page["previous_pages"][0]["offset"] == 0
    assert last_page["previous_pages"][0]["next_offset"] == last_page["offset"]
    assert last_page["omitted_pages"] == 0


@pytest.mark.parametrize("code", ["unauthorized", "timeout", "invalid_schema"])
async def test_recovery_flag_cannot_override_reserved_fatal_errors(tmp_path, code):
    class Fatal(InspectQueryData):
        async def invoke(self, request):
            self.invocations.append(request)
            return m.ToolResult(
                call_id=request.call_id,
                status="error",
                error=m.ErrorInfo(code=code, message="Cannot continue", recoverable=True),
            )

    data = Fatal()
    result = await execute(tmp_path, planner=QueryThenFinish(), data=data)
    assert result.status in {"failed", "timeout"}
    assert len(data.invocations) == 1


async def test_loop_guard_closes_every_accepted_pending_step(tmp_path):
    class LoopWithDependent(DemoPlanner):
        async def select_tools(self, request):
            root = step(request, "inspect")
            child = step(request, "query", depends_on=[root.step_id])
            return reply(request, m.ToolPlan(steps=[root, child]))

    result = await execute(
        tmp_path,
        planner=LoopWithDependent(),
        data=InspectQueryData(),
        max_identical_tool_calls=1,
    )
    assert result.stop_reason == "repeated_tool_call"
    trace = events(result)
    accepted_ids = set()
    for event in trace:
        if event["event_type"] == "plan.accepted":
            artifact = Path(result.trace_ref).parent / "artifacts" / f"{event['data_ref']}.json"
            accepted_ids.update(
                item["step_id"] for item in json.loads(artifact.read_text())["steps"]
            )
    terminal_ids = {
        event["step_id"]
        for event in trace
        if event["event_type"] == "step.state" and event["status"] != "running"
    }
    assert accepted_ids <= terminal_ids


async def test_page_history_envelope_fits_smallest_working_context(tmp_path):
    class OneRead(DemoPlanner):
        async def select_tools(self, request):
            if request.observation_read is not None:
                choice = m.FinishDecision(reason="insufficient_evidence")
            elif request.tool_observations:
                choice = m.ReadObservationDecision(
                    observation_id=request.tool_observations[0]["observation_id"],
                    pointer="/data/schema",
                    max_chars=4096,
                )
            else:
                choice = m.ToolPlan(steps=[step(request, "inspect")])
            return reply(request, choice)

    result = await execute(
        tmp_path,
        planner=OneRead(),
        data=InspectQueryData(schema={"columns": ["x" * 40] * 100}),
        max_planner_context_bytes=1024,
        planner_preview_bytes=512,
    )
    assert (result.status, result.stop_reason) == (
        "insufficient_evidence",
        "planner_finished_no_evidence",
    )
    assert result.call_counts.observation_reads == 1
