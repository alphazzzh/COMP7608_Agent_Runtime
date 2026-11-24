"""持续任务页面的离线 DOM 行为回归；不监听端口、不请求真实模型。

Node 直接执行生产脚本，轻量 DOM fixture 保存文本、事件及控件状态，
HTTP fixture 控制返回顺序，验证请求与可见状态而不是搜索实现字符串。
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from tests.support import RUNTIME_ROOT

DOM_FIXTURE = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const elements = new Map();
const navigation = [];
class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.children = []; this.ownText = ''; this.dataset = {};
    this.listeners = new Map(); this.attributes = {}; this.value = ''; this.hidden = false;
    this.disabled = false; this.checked = false; this.files = []; this.open = false;
    this.classes = new Set(); this.classList = {
      add: (...values) => values.forEach(value => this.classes.add(value)),
      remove: (...values) => values.forEach(value => this.classes.delete(value)),
      toggle: (value, active) => { if (active) this.classes.add(value); else this.classes.delete(value); },
      contains: value => this.classes.has(value),
    };
  }
  set textContent(value) { this.ownText = String(value); this.children = []; }
  get textContent() { return this.ownText + this.children.map(child => child.textContent).join(' '); }
  set innerHTML(value) { throw new Error('所有动态内容必须使用 textContent'); }
  get childNodes() { return this.children; }
  append(...children) { for (const child of children) this.children.push(...(child.tagName === 'fragment' ? child.children : [child])); }
  replaceChildren(...children) { this.ownText = ''; this.children = []; this.append(...children); }
  replaceWith(value) { if (this.id) elements.set(this.id, value); }
  setAttribute(key, value) { this.attributes[key] = String(value); }
  removeAttribute(key) { delete this.attributes[key]; }
  addEventListener(type, callback) { this.listeners.set(type, [...(this.listeners.get(type) || []), callback]); }
  async dispatch(type) { for (const callback of this.listeners.get(type) || []) await callback({ preventDefault() {}, target: this }); }
  focus() {} scrollIntoView() {} click() { return this.dispatch('click'); }
  querySelector() { return null; }
  querySelectorAll() { return []; }
  contains(value) { return this === value || this.children.some(child => child.contains(value)); }
}
const html = fs.readFileSync(process.argv[3], 'utf8');
for (const match of html.matchAll(/<([a-z]+)\b([^>]+)>/g)) {
  const [, tag, attrs] = match;
  const id = attrs.match(/\bid="([^"]+)"/)?.[1];
  const view = attrs.match(/\bdata-view="([^"]+)"/)?.[1];
  if (!id && !view) continue;
  const element = new Element(tag);
  if (id) { element.id = id; elements.set(id, element); }
  if (view) { element.dataset.view = view; navigation.push(element); }
  element.value = attrs.match(/\bvalue="([^"]*)"/)?.[1] || '';
  element.hidden = /\bhidden\b/.test(attrs); element.disabled = /\bdisabled\b/.test(attrs);
  element.checked = /\bchecked\b/.test(attrs);
}
const $ = id => elements.get(id);
const windowListeners = new Map();
const documentListeners = new Map();
const timers = new Map(); let timerId = 0;
global.window = {
  location: { hash: '#workbench' }, history: { replaceState() {} },
  setTimeout: (callback, delay) => { const id = ++timerId; timers.set(id, { callback, delay }); return id; },
  clearTimeout: id => timers.delete(id),
  addEventListener: (type, callback) => windowListeners.set(type, callback),
  localStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
  crypto: { randomUUID: () => 'event-unique-1' }, confirm: () => true,
};
global.document = {
  hidden: false, createElement: tag => new Element(tag),
  createDocumentFragment: () => new Element('fragment'),
  createTextNode: text => { const element = new Element('text'); element.textContent = text; return element; },
  getElementById: $, querySelector: selector => selector.includes('data-view')
    ? navigation.find(element => selector.includes('"' + element.dataset.view + '"')) : new Element(),
  querySelectorAll: selector => selector === '[data-view]' ? navigation : [],
  addEventListener: (type, callback) => documentListeners.set(type, callback),
};
const requests = []; let responder;
global.fetch = async (path, options) => {
  requests.push({ path, options, body: options.body ? JSON.parse(options.body) : undefined });
  const result = await responder(path, options);
  if (result instanceof Error) throw result;
  return { ok: true, status: 200, json: async () => result };
};
const ready = { profile_id: 'ready', title: '已就绪配置', ready: true, modules: {}, suggested_questions: [] };
const mission = (id = 'mission-a', patch = {}) => ({
  mission_id: id, title: '<任务标题>', question: '<目标内容>', profile_id: 'ready',
  status: 'active', reason: null, interval_seconds: 30, stop_on_completed: true,
  limits: { max_runs: 10, max_model_calls: 120, max_data_calls: 120, max_duration_seconds: 86400 },
  started_runs: 2, completed_runs: 1, used_model_calls: 7, reserved_model_calls: 3,
  used_data_calls: 8, reserved_data_calls: 4, next_wake_at: 1794888000, deadline_at: 1794974400,
  created_at: 1794880000, updated_at: 1794888000, active_trigger_id: null,
  handoff: { summary: '<交接摘要>', pending_items: ['<后续工作>'], previous_run_id: 'run-a', artifact_refs: ['artifact-a'], truncated: true },
  triggers: [{ trigger_id: 'trigger-a', event_id: 'event-a', run_id: 'run-a', status: 'completed', cycle: 1, run_status: 'completed', stop_reason: '<停止原因>', created_at: 1794888000 }],
  ...patch,
});
function defaults(path) {
  if (path === '/api/info') return { features: { missions: true }, enabled_modes: ['full_agent'], capabilities: [] };
  if (path === '/api/profiles') return { profiles: [ready] };
  if (path === '/api/runs') return { runs: [] };
  if (path === '/api/missions') return { missions: [mission()] };
  if (path.startsWith('/api/missions/')) return mission(path.split('/')[3]);
  throw new Error('Unexpected fixture request: ' + path);
}
responder = async path => defaults(path);
let source = fs.readFileSync(process.argv[2], 'utf8');
const bootstrap = 'initialize().catch((error) => { showNotice(errorMessage(error), true); setConnection(false); });';
source = source.replace(bootstrap, 'globalThis.ui = { state, initialize, setView, renderInfo, renderProfiles, renderRun, renderControls };');
require('node:vm').runInThisContext(source);
const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
const allChildren = element => [element, ...element.children.flatMap(allChildren)];
const findButton = (container, label) => allChildren(container).find(element => element.tagName === 'button' && element.textContent.includes(label));
"""


def run_frontend(program: str) -> None:
    """隔离执行真实 app.js；失败时保留完整 Node 断言诊断。"""
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("离线前端行为回归需要 Node.js")
    static = RUNTIME_ROOT / "src/lake_agent/web/static"
    result = subprocess.run(
        [executable, "-", str(static / "app.js"), str(static / "index.html")],
        input=(
            DOM_FIXTURE
            + "\n(async () => {\n"
            + program
            + "\nconsole.log('MISSION_DOM_COMPLETE');"
            + "\n})().catch(error => { console.error(error); process.exitCode = 1; });"
        ),
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MISSION_DOM_COMPLETE" in result.stdout, "DOM 场景存在未释放的异步等待"


def test_mission_creation_uses_ready_profiles_and_bounded_payload():
    """创建仅使用就绪 profile 和公开的有界累计限制。"""
    run_frontend(r"""
await ui.initialize();
const nav = navigation.find(element => element.dataset.view === 'missions');
assert(nav && !nav.hidden, '持续任务导航应由 features.missions 开放');
await nav.click(); await flush();
assert.equal($('view-missions').hidden, false);
assert.equal($('mission-max-runs').value, '10');
assert.equal($('mission-max-model-calls').value, '120');
assert.equal($('mission-max-data-calls').value, '120');
assert.equal($('mission-max-duration').value, '86400');
$('mission-title').value = '  持续跟进  '; $('mission-question').value = '  跟进收入  ';
$('mission-schedule').value = 'manual';
let created;
responder = async (path, options) => {
  if (path === '/api/missions' && options.method === 'POST') { created = JSON.parse(options.body); return mission('created'); }
  return defaults(path);
};
await $('mission-form').dispatch('submit');
assert.deepEqual(created, { title: '持续跟进', question: '跟进收入', profile_id: 'ready', interval_seconds: null,
  start_immediately: true, stop_on_completed: true,
  limits: { max_runs: 10, max_model_calls: 120, max_data_calls: 120, max_duration_seconds: 86400 } });
assert.equal(ui.state.missionId, 'created');
created = null; $('mission-max-runs').value = '101';
await $('mission-form').dispatch('submit'); assert.equal(created, null);
$('mission-max-runs').value = '10'; $('mission-schedule').value = 'interval'; $('mission-interval').value = '0';
await $('mission-form').dispatch('submit'); assert.equal(created, null);
$('mission-interval').value = '30'; $('mission-stop-on-completed').checked = false;
await $('mission-form').dispatch('submit');
assert.equal(created.interval_seconds, 30); assert.equal(created.stop_on_completed, false);
ui.state.profiles = [{ ...ready, ready: false }]; ui.renderProfiles();
assert.equal($('create-mission').disabled, true);
assert.equal($('mission-profile').children[0].disabled, true);
""")


def test_mission_details_render_untrusted_text_and_safe_actions():
    """交接、额度和周期均按文本显示；动作状态遵循任务状态。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
assert($('mission-detail').textContent.includes('<任务标题>'));
assert($('mission-detail').textContent.includes('<交接摘要>'));
assert($('mission-detail').textContent.includes('<后续工作>'));
assert($('mission-detail').textContent.includes('已用 7'));
assert($('mission-detail').textContent.includes('预留 3'));
assert($('mission-detail').textContent.includes('已领取周期会安全收尾'));
assert($('mission-detail').textContent.includes('已截断'));
assert(findButton($('mission-detail'), '暂停'));
assert(findButton($('mission-detail'), '手动唤醒'));
assert.equal(findButton($('mission-detail'), '恢复'), undefined);
assert(findButton($('mission-detail'), '查看运行'));
let actions = [];
responder = async (path, options) => {
  if (options.method === 'POST') { actions.push(path); return mission('mission-a', { status: 'paused' }); }
  if (path === '/api/missions/mission-a') return mission('mission-a', { status: 'paused' });
  return defaults(path);
};
await findButton($('mission-detail'), '暂停').click();
assert.deepEqual(actions, ['/api/missions/mission-a/pause']);
assert(findButton($('mission-detail'), '恢复'));
assert.equal(findButton($('mission-detail'), '手动唤醒'), undefined);
""")


def test_mission_wake_retry_reuses_event_and_blocks_double_submit():
    """网络失败后的重试保留 event_id，并阻止同一动作并发提交。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
let release; const bodies = []; let first = true;
responder = async (path, options) => {
  if (path.endsWith('/wake')) {
    bodies.push(JSON.parse(options.body));
    if (first) { first = false; await new Promise(resolve => { release = resolve; }); throw new Error('网络连接断开'); }
    return { trigger_id: 'new-trigger' };
  }
  return defaults(path);
};
const button = findButton($('mission-detail'), '手动唤醒');
const pending = button.click(); await flush();
assert.equal(button.disabled, true); await button.click(); assert.equal(bodies.length, 1);
release(); await pending;
await findButton($('mission-detail'), '重试唤醒').click();
assert.equal(bodies.length, 2); assert.equal(bodies[0].event_id, bodies[1].event_id);
""")


def test_mission_polling_stops_and_late_details_cannot_replace_selection():
    """仅可见任务视图轮询；离页与 BFCache 返回均保持代次边界。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
let release; let delayDetails = false;
responder = async path => {
  if (path === '/api/missions') return { missions: [mission(), mission('mission-b', { title: 'B任务' })] };
  if (path === '/api/missions/mission-a' && delayDetails) { await new Promise(resolve => { release = resolve; }); return mission(); }
  return path === '/api/missions/mission-b' ? mission('mission-b', { title: 'B任务' }) : defaults(path);
};
await $('refresh-missions').click(); await flush();
delayDetails = true;
const first = allChildren($('missions-list')).find(element => element.dataset.missionId === 'mission-a');
const pending = first.click(); await flush();
await allChildren($('missions-list')).find(element => element.dataset.missionId === 'mission-b').click();
release(); await pending; assert.equal(ui.state.missionId, 'mission-b');
assert($('mission-detail').textContent.includes('B任务'));
const missionTimer = [...timers.entries()].find(([, timer]) => timer.delay === 3000);
assert(missionTimer, '可见持续任务视图应安排有界轮询');
ui.setView('history'); assert(!timers.has(missionTimer[0]));
const before = requests.filter(request => request.path.startsWith('/api/missions')).length;
await missionTimer[1].callback(); assert.equal(requests.filter(request => request.path.startsWith('/api/missions')).length, before);
ui.setView('missions'); await flush(); document.hidden = true; documentListeners.get('visibilitychange')();
assert(![...timers.values()].some(timer => timer.delay === 3000));
document.hidden = false; documentListeners.get('visibilitychange')();
assert([...timers.values()].some(timer => timer.delay === 3000));
windowListeners.get('pagehide')(); assert(![...timers.values()].some(timer => timer.delay === 3000));
windowListeners.get('pageshow')(); assert([...timers.values()].some(timer => timer.delay === 3000));
""")


def test_mission_detail_request_is_not_reused_after_leaving_and_returning():
    """旧页面详情不能在重入后覆盖更新过的任务状态。"""
    run_frontend(r"""
let current = mission(); let release; let delayed = false; let detailReads = 0;
responder = async path => {
  if (path === '/api/missions') return { missions: [current] };
  if (path === '/api/missions/mission-a') {
    detailReads += 1; const snapshot = current;
    if (delayed) { delayed = false; await new Promise(resolve => { release = resolve; }); }
    return snapshot;
  }
  return defaults(path);
};
await ui.initialize(); ui.setView('missions'); await flush();
delayed = true;
const card = allChildren($('missions-list')).find(element => element.dataset.missionId === 'mission-a');
const outstanding = card.click(); await flush(); assert(release);
ui.setView('history'); current = mission('mission-a', { status: 'paused', reason: 'fixture' });
ui.setView('missions'); await flush();
assert.equal(detailReads, 3); assert.equal(ui.state.mission.status, 'paused');
release(); await outstanding; await flush();
assert.equal(ui.state.mission.status, 'paused');
assert(findButton($('mission-detail'), '恢复'));
assert.equal(findButton($('mission-detail'), '手动唤醒'), undefined);
""")


def test_scheduler_health_error_is_visible_and_mission_runs_cannot_be_deleted():
    """调度故障要出现在任务列表，Mission run 的删除入口必须禁用。"""
    run_frontend(r"""
const fixtureRun = { run_id: 'run-a', mode: 'full_agent', status: 'completed', question: 'Done', mission_id: 'mission-a' };
responder = async path => {
  if (path === '/api/missions') return { missions: [mission()], scheduler: { running: false, error: { code: 'mission_scheduler_failed', message: '调度已停止。' } } };
  if (path === '/api/runs') return { runs: [fixtureRun] };
  return defaults(path);
};
await ui.initialize(); ui.setView('missions'); await $('refresh-missions').click(); await flush();
assert($('missions-list').textContent.includes('调度已停止。'));
const recentDelete = allChildren($('recent-runs')).find(item => item.dataset.action === 'delete');
assert(recentDelete); assert.equal(recentDelete.disabled, true);
ui.state.run = fixtureRun; ui.state.selectedRunId = fixtureRun.run_id; ui.renderControls();
assert.equal($('delete-run').disabled, true);
""")


def test_creation_during_list_refresh_releases_loading_and_preserves_new_selection():
    """创建与旧列表并发时，新任务不消失，列表锁最终释放。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
let release;
responder = async (path, options) => {
  if (path === '/api/missions' && options.method !== 'POST') {
    await new Promise(resolve => { release = resolve; }); return { missions: [mission()] };
  }
  if (path === '/api/missions' && options.method === 'POST') return mission('created');
  return defaults(path);
};
const pending = $('refresh-missions').click(); await flush();
$('mission-title').value = '新任务'; $('mission-question').value = '新目标'; $('mission-schedule').value = 'manual';
await $('mission-form').dispatch('submit');
release(); await pending;
assert.equal(ui.state.missionId, 'created');
assert(ui.state.missions.some(item => item.mission_id === 'created'));
assert.equal($('refresh-missions').disabled, false, '旧列表应释放请求锁');
""")


def test_mission_feature_gate_and_all_budget_boundaries():
    """关闭能力时不读取任务，任一额度越界都不能发出创建请求。"""
    run_frontend(r"""
responder = async path => path === '/api/info' ? { features: { missions: false }, capabilities: [] } : defaults(path);
await ui.initialize(); ui.setView('missions'); await flush();
assert.equal(ui.state.view, 'workbench');
assert.equal(navigation.find(item => item.dataset.view === 'missions').hidden, true);
assert.equal(requests.filter(item => item.path.startsWith('/api/missions')).length, 0);
ui.state.info.features.missions = true; ui.renderInfo(); ui.setView('missions'); await flush();
$('mission-title').value = '额度检查'; $('mission-question').value = '跟进目标'; $('mission-schedule').value = 'manual';
const limits = [ ['mission-max-runs', '10', '101'], ['mission-max-model-calls', '120', '10001'],
  ['mission-max-data-calls', '120', '10001'], ['mission-max-duration', '86400', '604801'] ];
for (const [id, normal, tooLarge] of limits) {
  for (const invalid of ['0', '-1', '1.5', 'NaN', tooLarge]) {
    $(id).value = invalid; const before = requests.length;
    await $('mission-form').dispatch('submit');
    assert.equal(requests.length, before, id + ' should reject ' + invalid);
  }
  $(id).value = normal;
}
""")


def test_mission_poll_request_does_not_overlap_or_apply_hidden_response():
    """轮询读取未返回时不重入；页面隐藏后的响应不覆盖已显示数据。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
let release; let listReads = 0;
responder = async path => {
  if (path === '/api/missions') { listReads += 1; await new Promise(resolve => { release = resolve; }); return { missions: [mission('late')] }; }
  return defaults(path);
};
const timer = [...timers.values()].find(item => item.delay === 3000);
const pending = timer.callback(); await flush();
await timer.callback(); assert.equal(listReads, 1);
document.hidden = true; documentListeners.get('visibilitychange')();
release(); await pending;
assert.equal(ui.state.missionId, 'mission-a');
assert.equal(ui.state.missions[0].mission_id, 'mission-a');
assert.equal(ui.state.missionsLoading, false);
assert(![...timers.values()].some(item => item.delay === 3000));
""")


def test_mission_resume_complete_and_blocked_controls():
    """只允许暂停任务恢复，阻塞任务仅保留结束入口。"""
    run_frontend(r"""
let status = 'paused';
responder = async (path, options) => {
  if (path === '/api/missions/mission-a/resume') { status = 'active'; return mission('mission-a', { status }); }
  if (path === '/api/missions/mission-a/complete') { status = 'completed'; return mission('mission-a', { status }); }
  if (path === '/api/missions/mission-a') return mission('mission-a', { status, reason: '<阻塞原因>' });
  return defaults(path);
};
await ui.initialize(); ui.setView('missions'); await flush();
await findButton($('mission-detail'), '恢复').click();
assert(findButton($('mission-detail'), '暂停'));
await findButton($('mission-detail'), '结束任务').click();
assert.equal(findButton($('mission-detail'), '结束任务'), undefined);
assert.equal(findButton($('mission-detail'), '恢复'), undefined);
status = 'blocked'; await $('refresh-missions').click();
assert($('mission-detail').textContent.includes('<阻塞原因>'));
assert(findButton($('mission-detail'), '结束任务'));
assert.equal(findButton($('mission-detail'), '恢复'), undefined);
assert.equal(findButton($('mission-detail'), '手动唤醒'), undefined);
""")


def test_unchanged_poll_preserves_task_controls_and_list_nodes():
    """未改变的轮询不重建按钮，避免键盘焦点被周期刷新打断。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
const pause = findButton($('mission-detail'), '暂停');
const first = $('missions-list').children[0];
await $('refresh-missions').click();
assert.equal(findButton($('mission-detail'), '暂停'), pause);
assert.equal($('missions-list').children[0], first);
""")


def test_failed_mission_action_restores_retryable_button():
    """暂停失败后恢复原按钮文案及可点击状态，不把页面锁在加载中。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
responder = async (path, options) => {
  if (path.endsWith('/pause')) throw new Error('网络连接失败');
  return defaults(path);
};
await findButton($('mission-detail'), '暂停').click();
const retry = findButton($('mission-detail'), '暂停');
assert(retry); assert.equal(retry.disabled, false);
assert($('notice-text').textContent.includes('网络连接失败'));
""")


def test_action_does_not_reuse_detail_request_started_before_mutation():
    """已提交的暂停不能被动作前捕获的 GET 响应回滚成 active。"""
    run_frontend(r"""
await ui.initialize(); ui.setView('missions'); await flush();
let release; let status = 'active'; let delayed = false;
responder = async (path, options) => {
  if (path.endsWith('/pause')) { status = 'paused'; return mission('mission-a', { status }); }
  if (path === '/api/missions/mission-a') {
    const snapshot = status;
    if (!delayed) { delayed = true; await new Promise(resolve => { release = resolve; }); }
    return mission('mission-a', { status: snapshot });
  }
  return defaults(path);
};
const reading = $('refresh-missions').click(); await flush();
const pausing = findButton($('mission-detail'), '暂停').click(); await flush();
release(); await Promise.all([reading, pausing]);
assert.equal(ui.state.mission.status, 'paused');
assert(findButton($('mission-detail'), '恢复'));
assert.equal(findButton($('mission-detail'), '手动唤醒'), undefined);
""")


def test_action_confirmation_does_not_reuse_get_started_while_switching_selection():
    """动作挂起期间切走再切回，确认后的刷新也不能复用旧 GET。"""
    run_frontend(r"""
responder = async path => path === '/api/missions'
  ? { missions: [mission(), mission('mission-b', { title: 'B任务' })] } : defaults(path);
await ui.initialize(); ui.setView('missions'); await flush();
let releaseAction; let releaseDetail; let status = 'active'; let delayed = false;
responder = async (path, options) => {
  if (path.endsWith('/pause')) {
    await new Promise(resolve => { releaseAction = resolve; }); status = 'paused';
    return mission('mission-a', { status });
  }
  if (path === '/api/missions/mission-a') {
    const snapshot = status;
    if (!delayed) { delayed = true; await new Promise(resolve => { releaseDetail = resolve; }); }
    return mission('mission-a', { status: snapshot });
  }
  return defaults(path);
};
const pausing = findButton($('mission-detail'), '暂停').click(); await flush();
await allChildren($('missions-list')).find(element => element.dataset.missionId === 'mission-b').click();
const selecting = allChildren($('missions-list')).find(element => element.dataset.missionId === 'mission-a').click(); await flush();
releaseAction(); await flush(); releaseDetail(); await Promise.all([pausing, selecting]);
assert.equal(ui.state.missionId, 'mission-a');
assert.equal(ui.state.mission.status, 'paused');
assert(findButton($('mission-detail'), '恢复'));
""")


def test_empty_mission_list_finishes_with_create_hint():
    """首次没有任务时，读取完成后应显示创建提示而不是永久加载。"""
    run_frontend(r"""
responder = async path => path === '/api/missions' ? { missions: [] } : defaults(path);
await ui.initialize(); ui.setView('missions'); await flush();
assert.equal(ui.state.missionsLoading, false);
assert($('missions-list').textContent.includes('还没有持续任务'));
assert(!$('missions-list').textContent.includes('正在读取持续任务'));
""")
