"""工作台 HTTP、任务身份、模式开放和完成政策的真实渲染回归。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import asyncio
import html
import json
import shutil
import subprocess
import threading
from contextlib import asynccontextmanager

import httpx
import pytest

from lake_agent import models as m
from lake_agent.web import service
from lake_agent.web.app import create_app
from tests.support import RUNTIME_ROOT
from tests.support.web import InvokeGate, create_run, running_client, wait_stopped

pytest.importorskip("fastapi", reason="web acceptance tests require the optional web extra")


async def test_standard_run_executes_harness_and_exposes_evidence(tmp_path):
    """表面成功的响应不能替代真实轨迹与来源记录。"""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        view = await wait_stopped(client, run_id)

    assert view["status"] == "completed"
    assert view["result"]["status"] == "completed"
    assert "120 million HKD" in view["result"]["answer"]["text"]
    assert view["metrics"]["tools"] == 1
    assert len(view["evidence"]) == 1
    assert view["evidence"][0]["source_id"] == "demo-annual"
    assert view["result"]["answer"]["citations"] == [view["evidence"][0]["evidence_id"]]
    assert {event["event_type"] for event in view["events"]} >= {
        "run.started",
        "tool.call.completed",
        "evidence.added",
        "run.completed",
    }
    assert view["active"] is view["can_resume"] is False
    expected_metrics = {
        "tools": 1,
        "steps": 1,
        "model_calls": 0,
        "evidence": 1,
        "attempts": 1,
        "duration_ms": view["result"]["duration_ms"],
    }
    assert {key: view["metrics"][key] for key in expected_metrics} == expected_metrics
    assert len(view["attempts"]) == 1
    m.RunResult.model_validate(view["result"])
    for evidence in view["evidence"]:
        m.EvidenceRecord.model_validate(evidence)
    assert {m.SourceRecord.model_validate(source).source_id for source in view["sources"]} >= {
        "demo-annual"
    }
    for event in view["events"]:
        assert event["attempt_id"]
        assert event["run_id"] == run_id
        m.TraceEvent.model_validate(
            {key: value for key, value in event.items() if key != "attempt_id"}
        )


async def test_info_describes_registered_profiles_and_actual_modes_work(tmp_path):
    """离线承诺与模式选择器必须符合实际可执行行为。"""
    async with running_client(tmp_path) as client:
        info = await client.get("/api/info")
        assert info.status_code == 200
        payload = info.json()
        assert len(payload["profiles"]) == 3
        assert payload["version"]
        assert {item["profile_id"] for item in payload["profiles"]} == {
            "test-standard",
            "test-parallel",
            "test-recovery",
        }
        assert payload["capabilities"]
        for capability in payload["capabilities"]:
            assert all(capability[field] for field in ("name", "status", "description"))
        for mode in ("full_agent", "no_verifier"):
            run_id = await create_run(client, mode=mode)
            view = await wait_stopped(client, run_id)
            assert view["status"] == "completed", view
            assert view["result"]["mode"] == mode
            assert view["metrics"]["model_calls"] == 0


async def test_parallel_scenario_overlaps_real_disjoint_tool_calls(tmp_path, monkeypatch):
    """两个工具必须都已进入，之后才允许向执行框架释放任一响应。"""
    gate = InvokeGate(monkeypatch, expected=2)
    async with running_client(tmp_path) as client:
        run_id = await create_run(client, scenario="parallel")
        await gate.wait_entered()
        assert gate.active == gate.peak == 2
        assert {request.args["source_id"] for request in gate.requests} == {
            "demo-annual",
            "demo-quarter",
        }
        gate.release.set()
        view = await wait_stopped(client, run_id)
    assert view["status"] == "completed"
    assert view["metrics"]["tools"] == view["metrics"]["steps"] == 2
    assert {item["source_id"] for item in view["evidence"]} == {"demo-annual", "demo-quarter"}
    assert gate.active == 0


async def test_safe_recovery_keeps_provenance_budget_and_history_after_restart(tmp_path):
    """以稳定 ID 重放持久化的成功结果，绝不再次发送给工具执行。"""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client, scenario="recovery")
        paused = await wait_stopped(client, run_id)
        assert paused["status"] == "interrupted", paused
        assert paused["can_resume"] is True
        assert len(paused["evidence"]) == 1
        original_evidence = paused["evidence"][0]
        original_call = original_evidence["call_id"]
        initial_logs = {path: path.read_bytes() for path in tmp_path.rglob("events.jsonl")}
        assert initial_logs

    async with running_client(tmp_path) as client:
        history = await client.get("/api/runs")
        assert history.status_code == 200
        assert history.json()["runs"][0]["run_id"] == run_id
        restored = (await client.get(f"/api/runs/{run_id}")).json()
        assert restored["status"] == "interrupted"
        assert restored["evidence"] == paused["evidence"]
        response = await client.post(f"/api/runs/{run_id}/resume")
        assert response.status_code == 202, response.text
        completed = await wait_stopped(client, run_id)
        assert completed["status"] == "completed", completed
        assert completed["can_resume"] is False
        assert completed["evidence"] == [original_evidence]
        assert completed["metrics"]["tools"] == completed["metrics"]["steps"] == 1
        assert completed["result"]["call_counts"]["tools"] == 1
        assert completed["metrics"]["attempts"] == len(completed["attempts"]) == 2
        tool_events = [
            event for event in completed["events"] if event["event_type"] == "tool.call.completed"
        ]
        assert {event["call_id"] for event in tool_events} == {original_call}
        assert len([event for event in tool_events if not event["detail"].get("replayed")]) == 1
        assert any(event["detail"].get("replayed") for event in tool_events)
        attempt_ids = list(dict.fromkeys(event["attempt_id"] for event in completed["events"]))
        assert len(attempt_ids) == 2
        for attempt_id in attempt_ids:
            events = [event for event in completed["events"] if event["attempt_id"] == attempt_id]
            assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
        terminal = await client.post(f"/api/runs/{run_id}/resume")
        assert terminal.status_code == 409
        assert_error_detail(terminal)
        assert (await client.get(f"/api/runs/{run_id}")).json()["result"] == completed["result"]

    for path, original_bytes in initial_logs.items():
        assert path.read_bytes() == original_bytes


def assert_error_detail(response):
    """公开错误保持结构化，且不包含原始 Python 异常堆栈。"""
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert isinstance(detail["code"], str) and detail["code"]
    assert isinstance(detail["message"], str) and detail["message"]
    assert "Traceback" not in detail["message"]


@pytest.mark.parametrize(
    "change",
    [
        {"question": ""},
        {"question": "x" * 2001},
        {"question": None},
        {"question": 12},
        {"scenario": "external-service"},
        {"mode": "paid-model"},
    ],
)
async def test_invalid_run_requests_are_rejected_without_creating_history(tmp_path, change):
    """格式错误的公开输入不能创建可执行或持久化的运行。"""
    payload = {
        "question": "Revenue?",
        "profile_id": "test-standard",
        "mode": "full_agent",
        **change,
    }
    async with running_client(tmp_path) as client:
        response = await client.post("/api/runs", json=payload)
        assert response.status_code == 422
        assert_error_detail(response)
        assert (await client.get("/api/runs")).json() == {"runs": []}


async def test_malformed_json_and_unknown_ids_return_safe_http_errors(tmp_path):
    """无效 JSON 与不透明路径组件不得以服务器错误的形式泄漏。"""
    async with running_client(tmp_path) as client:
        malformed = await client.post(
            "/api/runs", content="{", headers={"Content-Type": "application/json"}
        )
        assert malformed.status_code == 422
        assert_error_detail(malformed)
        for suffix in ("", "/export", "/report", "/artifacts/initial/missing"):
            response = await client.get(f"/api/runs/no-such-run{suffix}")
            assert response.status_code == 404
            assert_error_detail(response)
        for action in ("resume", "cancel"):
            response = await client.post(f"/api/runs/no-such-run/{action}")
            assert response.status_code == 404
            assert_error_detail(response)
        traversal = await client.get("/api/runs/..%2F..%2Fprivate")
        assert traversal.status_code == 404


@pytest.mark.parametrize(
    "origin", ["https://evil.example", "http://testserver:8001", "null", "not-an-origin"]
)
async def test_foreign_or_invalid_origins_cannot_create_runs(tmp_path, origin):
    """无关网站不能在本地演示服务中启动工作。"""
    async with running_client(tmp_path) as client:
        response = await client.post(
            "/api/runs",
            json={"question": "Revenue?", "profile_id": "test-standard"},
            headers={"Origin": origin},
        )
        assert response.status_code == 403
        assert_error_detail(response)
        assert "access-control-allow-origin" not in response.headers
        assert (await client.get("/api/runs")).json() == {"runs": []}
        same_origin = await client.post(
            "/api/runs",
            json={"question": "Revenue?", "profile_id": "test-standard"},
            headers={"Origin": "http://testserver"},
        )
        assert same_origin.status_code == 202
        run_id = same_origin.json()["run_id"]
        view = await wait_stopped(client, run_id)
        assert view["result"]["mode"] == "full_agent"
        for action in ("cancel", "resume"):
            forbidden = await client.post(
                f"/api/runs/{run_id}/{action}", headers={"Origin": origin}
            )
            assert forbidden.status_code == 403
            assert_error_detail(forbidden)


async def test_report_escapes_untrusted_content_and_export_preserves_json(tmp_path):
    """共享报告将输入按文本渲染，同时保持 JSON 导出无损。"""
    question = "<script>alert(1)</script><img src=x onerror=alert(2)> & revenue?"
    async with running_client(tmp_path) as client:
        run_id = await create_run(client, question=question)
        view = await wait_stopped(client, run_id)
        exported = await client.get(f"/api/runs/{run_id}/export")
        assert exported.status_code == 200
        assert "attachment" in exported.headers["content-disposition"]
        assert exported.json() == view
        assert exported.json()["question"] == question
        report = await client.get(f"/api/runs/{run_id}/report")
        assert report.status_code == 200
        assert "text/html" in report.headers["content-type"]
        assert "attachment" in report.headers["content-disposition"]
        assert html.escape(question) in report.text
        assert question not in report.text
        assert "<script" not in report.text.lower()
        assert 'src="http' not in report.text.lower()
        assert 'href="http' not in report.text.lower()


async def test_artifacts_require_same_run_attempt_and_recorded_reference(tmp_path):
    """已记录的引用只授权一次限定范围的读取，不授权目录访问。"""
    async with running_client(tmp_path) as client:
        first_id = await create_run(client)
        first = await wait_stopped(client, first_id)
        event = next(
            event
            for event in first["events"]
            if event["event_type"] == "tool.call.completed" and event["data_ref"]
        )
        attempt_id, ref = event["attempt_id"], event["data_ref"]
        route = f"/api/runs/{first_id}/artifacts/{attempt_id}"
        artifact = await client.get(f"{route}/{ref}")
        assert artifact.status_code == 200
        assert artifact.json()["call_id"] == event["call_id"]
        # 在相邻位置放置一个未被引用的 JSON 文件：仅文件名有效
        # 不足以授权通过接口暴露存储内容。
        files = list(tmp_path.rglob(f"{ref}.json"))
        assert len(files) == 1
        (files[0].parent / "unreferenced-test.json").write_text('{"private":true}')
        second_id = await create_run(client)
        await wait_stopped(client, second_id)
        history = (await client.get("/api/runs")).json()["runs"]
        assert [summary["run_id"] for summary in history] == [second_id, first_id]
        for path in (
            f"{route}/unreferenced-test",
            f"{route}/..%2F..%2Fprivate",
            f"/api/runs/{first_id}/artifacts/missing-attempt/{ref}",
            f"/api/runs/{second_id}/artifacts/{attempt_id}/{ref}",
        ):
            response = await client.get(path)
            assert response.status_code == 404, (path, response.text)


async def test_capacity_rejects_extra_run_and_frees_slot_after_cancel(tmp_path, monkeypatch):
    """准入统计活跃的所有者任务，取消时归还对应名额。"""
    gate = InvokeGate(monkeypatch, expected=2)
    async with running_client(tmp_path, max_active_runs=2) as client:
        first_id = await create_run(client)
        second_id = await create_run(client)
        await gate.wait_entered()
        active_resume = await client.post(f"/api/runs/{first_id}/resume")
        assert active_resume.status_code == 409
        assert_error_detail(active_resume)
        rejected = await client.post(
            "/api/runs", json={"question": "Overflow?", "profile_id": "test-standard"}
        )
        assert rejected.status_code == 429
        assert_error_detail(rejected)
        assert rejected.json()["detail"]["code"] == "capacity_reached"
        assert len((await client.get("/api/runs")).json()["runs"]) == 2
        cancelled = await client.post(f"/api/runs/{first_id}/cancel")
        assert cancelled.status_code == 202
        stopped = await wait_stopped(client, first_id)
        assert stopped["status"] == "interrupted"
        assert gate.active == 1
        third_id = await create_run(client)
        gate.release.set()
        assert (await wait_stopped(client, second_id))["status"] == "completed"
        assert (await wait_stopped(client, third_id))["status"] == "completed"
    assert gate.active == 0


async def test_manual_cancel_does_not_bypass_unknown_outcome_recovery(tmp_path, monkeypatch):
    """派发后取消会因调用结果未知而拒绝恢复，且不重放该调用。"""
    gate = InvokeGate(monkeypatch)
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        await gate.wait_entered()
        cancelled = await client.post(f"/api/runs/{run_id}/cancel")
        assert cancelled.status_code == 202
        stopped = await wait_stopped(client, run_id)
        assert stopped["status"] == "interrupted"
        assert gate.active == 0
        resumed = await client.post(f"/api/runs/{run_id}/resume")
        assert resumed.status_code == 202
        refused = await wait_stopped(client, run_id)
        assert refused["status"] == "blocked"
        assert refused["error"]["code"] == "recovery_outcome_unknown"
        assert refused["error"]["message"]
        assert refused["can_resume"] is False
        assert len(gate.requests) == 1


async def test_shutdown_drains_owned_tasks_and_retains_interrupted_history(tmp_path, monkeypatch):
    """关闭服务时，必须先观察取消完成，再释放其存储。"""
    gate = InvokeGate(monkeypatch)
    baseline = asyncio.all_tasks()
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        await gate.wait_entered()
        assert gate.active == 1
    await asyncio.sleep(0)
    assert gate.active == 0
    leaked = [task for task in asyncio.all_tasks() - baseline if not task.done()]
    assert leaked == [], f"Lifespan leaked tasks: {leaked}"
    async with running_client(tmp_path) as client:
        view = (await client.get(f"/api/runs/{run_id}")).json()
        assert view["status"] == "interrupted"
        assert view["active"] is False
        assert len(gate.requests) == 1


pytest.importorskip("fastapi")


async def test_cancelled_http_admission_does_not_leave_queued_run(tmp_path, monkeypatch):
    """POST 连接中断后，不能使运行一直被错误标记为排队中。"""
    from lake_agent.web.storage import Repository

    entered, release = threading.Event(), threading.Event()
    original = Repository._save
    held = False

    def gated_save(repository, run_id, encoded):
        nonlocal held
        if not held and json.loads(encoded)["status"] == "queued":
            held = True
            entered.set()
            assert release.wait(3)
        return original(repository, run_id, encoded)

    monkeypatch.setattr(Repository, "_save", gated_save)
    async with running_client(tmp_path) as client:
        request = asyncio.create_task(
            client.post("/api/runs", json={"question": "revenue?", "profile_id": "test-standard"})
        )
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            request.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await request
            rows = (await client.get("/api/runs")).json()["runs"]
            assert len(rows) == 1
            view = (await client.get(f"/api/runs/{rows[0]['run_id']}")).json()
            assert view["status"] == "interrupted"
            assert view["active"] is view["can_resume"] is False
        finally:
            release.set()
            if not request.done():
                request.cancel()
            await asyncio.gather(request, return_exceptions=True)


async def test_referenced_artifact_symlink_cannot_escape_attempt(tmp_path):
    """即使被引用的文件名有效，也必须仍处于其所属尝试的目录内。"""
    async with running_client(tmp_path) as client:
        run_id = await create_run(client)
        view = await wait_stopped(client, run_id)
        event = next(value for value in view["events"] if value["data_ref"])
        attempt = event["attempt_id"]
        ref = event["data_ref"]
        artifacts = tmp_path / "traces" / run_id / "attempts" / attempt / "artifacts"
        artifacts.rename(artifacts.with_name("artifacts-backup"))
        outside = tmp_path / "outside-attempt"
        outside.mkdir()
        (outside / f"{ref}.json").write_text('{"foreign":"must not be returned"}')
        artifacts.symlink_to(outside, target_is_directory=True)
        response = await client.get(f"/api/runs/{run_id}/artifacts/{attempt}/{ref}")
        assert response.status_code == 404
        assert "foreign" not in response.text


def test_answer_rendering_and_finish_rejection_explain_completion_policy():
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("node is required for the offline JavaScript rendering regression")
    script = RUNTIME_ROOT / "src/lake_agent/web/static/app.js"
    program = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.ownText = ''; }
  set textContent(value) { this.ownText = String(value); this.children = []; }
  get textContent() { return this.ownText + this.children.map(child => child.textContent).join(' '); }
  set innerHTML(value) { throw new Error('Policy and answer text must use textContent'); }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.ownText = ''; this.children = children; }
  setAttribute() {}
}
const answer = new Element('div');
global.document = {
  createElement: tag => new Element(tag),
  createDocumentFragment: () => new Element('fragment'),
  createTextNode: value => { const element = new Element('text'); element.textContent = value; return element; },
  getElementById: () => answer,
};
let source = fs.readFileSync(process.argv[2], 'utf8');
const bootstrap = 'initialize().catch((error) => { showNotice(errorMessage(error), true); setConnection(false); });';
assert(source.includes(bootstrap));
source = source.replace(bootstrap, 'globalThis.policyView = { state, renderAnswer, eventTitle };');
require('node:vm').runInThisContext(source);
policyView.state.run = { status: 'running', mode: 'full_agent', result: null };
policyView.renderAnswer();
assert(answer.textContent.includes('是否核验取决于运行政策'));
assert(!answer.textContent.includes('组合与核验后'));

policyView.state.run = {
  status: 'completed', mode: 'full_agent',
  result: { status: 'completed', answer: {
    text: '<untrusted-answer>', citations: [], unresolved_questions: [], structured_result: null,
  } },
};
policyView.renderAnswer();
assert(answer.textContent.includes('<untrusted-answer>'));
assert(answer.textContent.includes('控制器已按当前运行政策接受完成提议'));
assert(answer.textContent.includes('内部核验通过'));
assert(answer.textContent.includes('外部评测正确'));
assert(answer.textContent.includes('不同概念'));
assert(policyView.eventTitle({ event_type: 'agent.action.rejected', detail: {
  kind: 'finish', code: 'verification_stale',
} }).includes('核验已过期'));
assert(policyView.eventTitle({ event_type: 'agent.action.rejected', detail: {
  kind: 'finish', code: 'verification_required',
} }).includes('需要当前有效的支持核验'));
"""
    result = subprocess.run(
        [executable, "-", str(script)],
        input=program,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


pytest.importorskip("fastapi", reason="web identity checks require the optional web extra")


async def saved_request(client, view, event_type="run.started"):
    """通过公开工件接口读取该次执行实际保存的 RunRequest。"""
    event = next(item for item in view["events"] if item["event_type"] == event_type)
    response = await client.get(
        f"/api/runs/{view['run_id']}/artifacts/{event['attempt_id']}/{event['data_ref']}"
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_questions_on_same_profile_have_distinct_request_result_and_event_task_ids(tmp_path):
    questions = ("What was 2024 annual revenue?", "Find the reporting year and its annual revenue.")
    views = []
    async with running_client(tmp_path) as client:
        for question in questions:
            run_id = await create_run(client, question=question)
            view = await wait_stopped(client, run_id)
            assert view["status"] == "completed", view
            request = await saved_request(client, view)
            assert request["question"] == question
            assert request["task_id"] == f"web:{run_id}"
            assert view["result"]["task_id"] == request["task_id"]
            assert {event["task_id"] for event in view["events"]} == {request["task_id"]}
            assert {event["run_id"] for event in view["events"]} == {run_id}
            views.append(view)
    assert len({view["result"]["task_id"] for view in views}) == 2
    assert len({view["profile_id"] for view in views}) == 1


async def test_resume_preserves_legacy_saved_task_identity(tmp_path, monkeypatch):
    """模拟旧入口保存请求；正常恢复不得按新规则重建或覆盖其 task_id。"""
    original_run = service.run
    legacy_task_id = "web:test-revenue"

    async def legacy_run(request, **ports):
        # 只替换旧入口的请求构造；核心、SQLite、轨迹与恢复始终使用真实实现。
        return await original_run(request.model_copy(update={"task_id": legacy_task_id}), **ports)

    with monkeypatch.context() as legacy_entrypoint:
        legacy_entrypoint.setattr(service, "run", legacy_run)
        async with running_client(tmp_path) as client:
            run_id = await create_run(client, scenario="recovery")
            interrupted = await wait_stopped(client, run_id)
            assert interrupted["status"] == "interrupted", interrupted
            assert interrupted["can_resume"] is True
            assert (await saved_request(client, interrupted))["task_id"] == legacy_task_id

    async with running_client(tmp_path) as client:
        response = await client.post(f"/api/runs/{run_id}/resume")
        assert response.status_code == 202, response.text
        completed = await wait_stopped(client, run_id)
        assert completed["status"] == "completed", completed
        assert completed["result"]["task_id"] == legacy_task_id
        assert {event["task_id"] for event in completed["events"]} == {legacy_task_id}
        assert (await saved_request(client, completed, "run.resumed"))["task_id"] == legacy_task_id


@asynccontextmanager
async def policy_client(directory, **kwargs):
    """这些路由只读应用策略或提前拒绝请求，不启动服务生命周期。"""
    app = create_app(data_dir=directory, **kwargs)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        yield client, app


async def test_product_metadata_declares_only_full_agent_without_experiments(tmp_path):
    async with policy_client(tmp_path / "unopened") as (client, _):
        response = await client.get("/api/info")
    assert response.status_code == 200
    assert response.json()["enabled_modes"] == ["full_agent"]
    assert response.json()["features"] == {"evaluations": False}
    assert not (tmp_path / "unopened").exists()


@pytest.mark.parametrize("enabled", [False, True])
async def test_report_storage_is_only_initialized_for_an_explicit_evaluation_host(
    tmp_path, enabled
):
    """正式工作区不初始化实验报告仓库，显式开启的测试宿主仍能保存报告。"""
    directory = tmp_path / "workspace"
    app = create_app(data_dir=directory, enable_evaluations=enabled)
    async with app.router.lifespan_context(app):
        assert (directory / "evaluations").exists() is enabled
        assert (app.state.run_service.evaluations is not None) is enabled


@pytest.mark.parametrize("mode", ["no_verifier"])
async def test_product_rejects_experiment_mode_before_creating_a_run(tmp_path, mode):
    async with policy_client(tmp_path / "unopened") as (client, _):
        response = await client.post("/api/runs", json={"question": "Inspect", "mode": mode})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "mode_disabled"
    assert not (tmp_path / "unopened").exists()


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/api/evaluations"),
        ("POST", "/api/evaluations"),
        ("GET", "/api/evaluations/template"),
        ("GET", "/api/evaluations/known-report"),
    ],
)
async def test_product_evaluation_routes_are_closed_by_default(tmp_path, method, path):
    async with policy_client(tmp_path / "unopened") as (client, _):
        response = await client.request(method, path)
    assert response.status_code == 404
    assert not (tmp_path / "unopened").exists()


async def test_trusted_host_opt_in_drives_metadata_template_and_admission(tmp_path):
    async with policy_client(
        tmp_path / "unopened", allowed_modes=("full_agent", "no_verifier"), enable_evaluations=True
    ) as (client, _):
        info = (await client.get("/api/info")).json()
        template = (await client.get("/api/evaluations/template")).json()
        denied = await client.post(
            "/api/runs", json={"question": "Inspect", "mode": "fixed_retrieval"}
        )
        admitted = await client.post(
            "/api/runs", json={"question": "Inspect", "mode": "no_verifier"}
        )
    assert info["enabled_modes"] == ["full_agent", "no_verifier"]
    assert info["features"] == {"evaluations": True}
    assert [result["mode"] for result in template["results"]] == [
        "fixed_retrieval",
        "no_verifier",
        "full_agent",
    ]
    assert denied.status_code == 422
    assert denied.json()["detail"]["code"] == "invalid_request"
    # 模式获准后才进入执行配置准入；空配置仍明确拒绝，不创建默认端口。
    assert admitted.json()["detail"]["code"] == "profiles_unconfigured"


async def test_product_refuses_disabled_mode_resume_without_changing_saved_mode(
    tmp_path, monkeypatch
):
    async with policy_client(tmp_path / "unopened") as (client, app):

        async def existing_record(run_id):
            return {"mode": "no_verifier"}

        async def forbidden_restart(run_id):
            pytest.fail("Disabled experiment mode reached execution")

        monkeypatch.setattr(app.state.run_service, "view", existing_record)
        monkeypatch.setattr(app.state.run_service, "restart", forbidden_restart)
        response = await client.post("/api/runs/stopped-experiment/resume")
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "mode_disabled"


async def test_removed_fixed_mode_is_rejected_even_by_explicit_test_host(tmp_path):
    """固定检索已从核心模型删除，测试宿主也不能重新开放它。"""
    with pytest.raises(ValueError, match="supported runtime modes"):
        create_app(data_dir=tmp_path, allowed_modes=("full_agent", "fixed_retrieval"))
    assert not tmp_path.joinpath("workspace.sqlite3").exists()
