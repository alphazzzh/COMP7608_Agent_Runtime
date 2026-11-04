"""具体内容错误、历史回退、敏感信息和 JavaScript 安全渲染。

所有自动化请求使用本地端口或内存传输，不访问真实模型或启动监听服务。
"""

from __future__ import annotations

import json
import shutil
import subprocess
from contextlib import asynccontextmanager

import httpx
import pytest

from lake_agent import models as m
from lake_agent.web.app import create_app
from tests.support import RUNTIME_ROOT

_RUN_ID = "run-" + "a" * 32


_ATTEMPT_ID = "attempt-" + "b" * 32


_STAMP = "2026-11-03T10:00:00+00:00"


_ERROR = {
    "code": "invalid_model_citations",
    "message": "The model returned unknown evidence citations.",
    "retryable": False,
    "recoverable": False,
}


def event(kind, *, phase="content.compose", call_id="call-rejected", error=None):
    """只创建测试拥有的可序列化事件，实际序号由清单构造器填写。"""
    return {
        "event_type": kind,
        "call_id": call_id,
        "error": error,
        "detail": {"phase": phase} if kind.startswith("content.") else {},
    }


@asynccontextmanager
async def saved_client(directory, events, *, status="failed", result=None):
    """持有临时仓库锁，但不进入运行生命周期或创建任何后台任务。"""
    app = create_app(data_dir=directory)
    repository = app.state.run_service.repository
    await repository.start()
    try:
        await repository.save(
            {
                "run_id": _RUN_ID,
                "task_id": "saved-failure",
                "question": "目前的合成资料有哪些？",
                "mode": "full_agent",
                "status": status,
                "created_at": _STAMP,
                "updated_at": _STAMP,
                "attempts": [{"attempt_id": _ATTEMPT_ID}],
                "has_state": False,
                "result": result,
            }
        )
        attempt = repository.traces / _RUN_ID / "attempts" / _ATTEMPT_ID
        attempt.mkdir(parents=True)
        records = [
            m.TraceEvent(
                run_id=_RUN_ID,
                task_id="saved-failure",
                timestamp_utc=_STAMP,
                seq=index,
                **record,
            ).model_dump(mode="json")
            for index, record in enumerate(events, start=1)
        ]
        (attempt / "events.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records)
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            yield client
    finally:
        await repository.close()


async def view(client):
    response = await client.get(f"/api/runs/{_RUN_ID}")
    assert response.status_code == 200, response.text
    return response.json()


async def test_legacy_module_error_is_linked_to_its_specific_failed_call(tmp_path):
    """旧清单没有 RunResult.failure 时，仍从既有事件定位具体原因。"""
    result = {
        "status": "failed",
        "stop_reason": "module_error",
        "call_counts": {},
        "duration_ms": 3,
    }
    async with saved_client(
        tmp_path,
        [event("content.compose.call.failed", error=_ERROR)],
        result=result,
    ) as client:
        payload = await view(client)
    assert payload["result"]["stop_reason"] == "module_error"
    assert payload["failure"]["phase"] == "content.compose"
    assert payload["failure"]["error"] == _ERROR
    assert payload["failure"]["call_id"] == "call-rejected"
    assert payload["failure"]["attempt_id"] == _ATTEMPT_ID
    assert payload["failure"]["event_seq"] == 1


async def test_rejected_content_remains_visible_during_the_next_model_decision(tmp_path):
    """控制器拒绝成功响应后，后续 Planner 调用不能清掉未解决的内容错误。"""
    events = [
        event("content.compose.call.completed"),
        event("content.output.rejected", error=_ERROR),
        event("planner.decide.call.completed", phase="planner.decide", call_id="call-decision"),
    ]
    async with saved_client(tmp_path, events, status="running") as client:
        payload = await view(client)
    assert payload["failure"]["error"]["code"] == "invalid_model_citations"
    assert payload["failure"]["event_seq"] == 2


@pytest.mark.parametrize("resolved_phase", ["content.compose", "content.verify"])
async def test_only_the_corresponding_resolution_clears_pending_content_feedback(
    tmp_path, resolved_phase
):
    """核验成功不能误清未修正的生成错误，成功生成则清除同阶段提示。"""
    events = [
        event("content.output.rejected", error=_ERROR),
        event("content.feedback.resolved", phase=resolved_phase, call_id="call-corrected"),
    ]
    async with saved_client(tmp_path, events, status="running") as client:
        payload = await view(client)
    assert (payload["failure"] is None) is (resolved_phase == "content.compose")


async def test_completed_run_keeps_past_failures_in_timeline_without_a_current_failure(tmp_path):
    """已修正并完成的任务不应在顶栏继续显示历史错误。"""
    result = {
        "status": "completed",
        "stop_reason": "agent_finished",
        "call_counts": {},
        "duration_ms": 3,
    }
    async with saved_client(
        tmp_path,
        [event("content.compose.call.failed", error=_ERROR)],
        status="completed",
        result=result,
    ) as client:
        payload = await view(client)
    assert payload["failure"] is None
    assert payload["events"][0]["error"]["code"] == "invalid_model_citations"


async def test_current_result_failure_wins_over_an_older_unrelated_call_failure(tmp_path):
    """终态保存的明确原因优先，链接仍指向对应调用所在的审计事件。"""
    result = {
        "status": "failed",
        "stop_reason": "module_error",
        "call_counts": {},
        "duration_ms": 3,
        "failure": {"phase": "content.verify", "call_id": "call-final", "error": _ERROR},
    }
    async with saved_client(
        tmp_path,
        [
            event("content.compose.call.failed", error=_ERROR),
            event(
                "content.verify.call.failed",
                phase="content.verify",
                call_id="call-final",
                error=_ERROR,
            ),
        ],
        result=result,
    ) as client:
        payload = await view(client)
    assert payload["failure"]["phase"] == "content.verify"
    assert payload["failure"]["call_id"] == "call-final"
    assert payload["failure"]["event_seq"] == 2


async def test_large_saved_diagnostics_are_previewed_without_rewriting_the_original_record(
    tmp_path,
):
    """清单中的大诊断不能被重复扩展到顶栏；原始保存内容保持可审计。"""
    error = {**_ERROR, "details": {"rejected_output": "x" * 30_000, "issues": ["bad"] * 200}}
    result = {
        "status": "failed",
        "stop_reason": "module_error",
        "call_counts": {},
        "duration_ms": 3,
        "failure": {"phase": "content.compose", "call_id": "call-final", "error": error},
    }
    async with saved_client(tmp_path, [], result=result) as client:
        payload = await view(client)
    assert len(json.dumps(payload["failure"])) < 15_000
    assert "展示" in json.dumps(payload["failure"], ensure_ascii=False) or "…" in json.dumps(
        payload["failure"], ensure_ascii=False
    )
    saved = json.loads((tmp_path / "metadata" / f"{_RUN_ID}.json").read_text())
    assert saved["result"]["failure"]["error"]["details"] == error["details"]


def test_browser_failure_note_renders_differences_as_bounded_plain_text():
    """执行真实页面渲染函数；Node 的最小 DOM 拒绝 innerHTML，不启动浏览器。"""
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("node is required for the offline JavaScript rendering regression")
    script = RUNTIME_ROOT / "src/lake_agent/web/static/app.js"
    program = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.ownText = ''; this.classList = { toggle() {} }; }
  set textContent(value) { this.ownText = String(value); this.children = []; }
  get textContent() { return this.ownText + this.children.map(child => child.textContent).join(' '); }
  set innerHTML(value) { throw new Error('Untrusted diagnostics must use textContent'); }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.ownText = ''; this.children = children; }
  addEventListener() {}
  querySelector() { return null; }
}
const note = new Element('div');
global.document = { createElement: tag => new Element(tag), getElementById: () => note };
let source = fs.readFileSync(process.argv[2], 'utf8');
const bootstrap = 'initialize().catch((error) => { showNotice(errorMessage(error), true); setConnection(false); });';
assert(source.includes(bootstrap));
source = source.replace(bootstrap, 'globalThis.failureView = { state, renderRunNote };');
require('node:vm').runInThisContext(source);
failureView.state.run = {
  status: 'running', active: true,
  failure: { phase: 'content.compose', call_id: 'call-rejected', error: {
    code: 'invalid_model_citations', message: 'Unknown citation',
    details: {
      issues: [{ path: 'citations[0]', actual: '<img src=x onerror=alert(1)>', expected: 'an evidence_id' }, null, 7, 'legacy issue'],
      allowed_evidence_ids: ['ev-allowed'], rejected_output: 'bad answer ' + 'x'.repeat(30000)
    }
  } }
};
failureView.renderRunNote();
assert(note.textContent.includes('组合答案'));
assert(note.textContent.includes('invalid_model_citations'));
assert(note.textContent.includes('citations[0]'));
assert(note.textContent.includes('<img src=x onerror=alert(1)>'));
assert(note.textContent.includes('ev-allowed'));
assert(note.textContent.includes('call-rejected'));
assert(note.textContent.includes('仍在运行'));
assert(note.textContent.includes('legacy issue'));
assert(note.textContent.length < 10000);
failureView.state.run = { status: 'completed', result: { status: 'completed' }, failure: null };
failureView.renderRunNote();
assert.equal(note.hidden, true);
assert(!note.textContent.includes('invalid_model_citations'));
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
