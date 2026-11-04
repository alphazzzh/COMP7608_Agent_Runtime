/**
 * 项目数据池工作台：执行配置、任务、证据和导入评估均由服务端管理。
 * 客户端通过带版本检查的读取和有界轮询展示同源 API 状态；
 * 所有不可信文本均按纯文本渲染。
 */
"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const STATUS = {
    queued: "排队中", running: "运行中", completed: "已完成",
    interrupted: "已中断", failed: "执行错误", blocked: "恢复受阻",
    insufficient_evidence: "证据不足", budget_exceeded: "预算停止", timeout: "执行超时",
    error: "执行错误", cancelled: "已停止",
  };
  const VIEW_TITLES = { workbench: "智能问答", sources: "数据来源", history: "任务历史", evaluations: "实验评估" };
  const MODE_TITLES = { full_agent: "完整 Agent", no_verifier: "关闭核验" };
  // 固定检索仅是报告中的对照组，不加入执行模式选择器。
  const EVALUATION_MODE_TITLES = { ...MODE_TITLES, fixed_retrieval: "固定 Top-k 基线" };
  const MODULE_TITLES = { data: "数据模块", planner: "规划模块", content: "内容模块" };
  const MODULE_STATES = { connected: "已连接", unavailable: "未就绪" };
  const METRICS = [
    ["accuracy", "准确率", "percent"], ["numerical_accuracy", "数值答案准确率", "percent"],
    ["structured_accuracy", "结构化答案准确率", "percent"], ["retrieval_precision", "检索精确率", "percent"],
    ["retrieval_recall", "检索召回率", "percent"], ["retrieval_f1", "检索 F1", "percent"],
    ["grounding", "证据支撑率", "percent"], ["completion_rate", "完成率", "percent"],
    ["valid_tool_call_rate", "有效工具调用率", "percent"], ["unnecessary_tool_call_rate", "不必要工具调用率", "percent"],
    ["retry_recovery_rate", "重试恢复率", "percent"], ["tool_calls_mean", "平均工具调用", "number"],
    ["data_calls_mean", "平均数据调用", "number"], ["latency_ms_mean", "平均延迟", "milliseconds"],
    ["external_data_bytes_total", "外部数据访问字节", "number"], ["external_data_cost_usd", "外部数据费用（USD）", "currency"],
    ["provider_requests_total", "模型请求总量", "number"], ["input_tokens_total", "输入 Token 总量", "number"],
    ["output_tokens_total", "输出 Token 总量", "number"],
    ["token_total", "Token 总量", "number"], ["cost_usd", "费用（USD）", "currency"],
  ];
  const MAX_EVALUATION_BYTES = 128 * 1024;
  const EVENT_TITLES = {
    "run.started": "开始执行", "run.resumed": "恢复执行", "run.completed": "运行完成",
    "run.finished": "运行结束", "run.failed": "运行失败", "run.cancelled": "执行已停止",
    "run.stopped": "执行停止", "catalog.page": "读取数据目录", "catalog.loaded": "数据目录就绪",
    "task_plan.accepted": "任务规划已确认", "plan.accepted": "工具计划已确认",
    "task_plan.updated": "更新任务计划", "agent.decision.accepted": "模型已选择下一步",
    "agent.action.rejected": "动作未获准", "agent.stopped": "模型选择停止", "catalog.read": "查看来源目录",
    "content.output.rejected": "内容输出未获接纳", "content.feedback.resolved": "内容错误已修正",
    "evidence.added": "证据已保存", "observation.added": "工具结果已记录",
    "verification": "核验答案", "step.state": "更新执行步骤", "tool.replan_requested": "调整工具计划",
  };
  const PHASE_TITLES = {
    catalog: "读取数据目录", "content.prepare_catalog": "准备检索目录",
    "content.search": "检索相关来源", "content.compose": "组合答案",
    "content.verify": "核验答案", "planner.decompose": "分解问题",
    "planner.decide": "决定下一步",
    "planner.select_tools": "选择执行工具", tool: "调用工具",
  };
  const state = {
    info: null, profiles: [], profileId: null, runs: [], selectedRunId: null, run: null,
    view: "workbench", tab: "answer", selectedEventKey: null, mutation: false,
    runVersion: 0, selectionVersion: 0, inspectorVersion: 0, historyVersion: 0,
    artifactCache: new Map(), pollTimer: null, pollGeneration: 0, pollInFlight: false,
    stopped: false, lastPollError: null, renderSignature: null, historySignature: null,
    sourcesProfileId: null, sourcesLoadedProfileId: null, sourceProfile: null,
    sources: [], selectedSourceId: null, sourcesLoading: false, sourcesError: null,
    sourceVersion: 0, evaluations: [], evaluationId: null, evaluation: null,
    evaluationsLoading: false, evaluationLoading: false, evaluationImporting: false,
    evaluationListError: null, evaluationError: null, evaluationsVersion: 0,
    evaluationVersion: 0, evaluationUserVersion: 0,
  };

  /** 使用 textContent 构造 DOM 节点，将服务端和用户提供的字符串保持为纯文本。 */
  function node(tag, className = "", text = null) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== null && text !== undefined) element.textContent = String(text);
    return element;
  }

  function emptyState(title, description, symbol = "◇") {
    const wrapper = node("div", "empty-state");
    const icon = node("div", "empty-illustration", symbol);
    icon.setAttribute("aria-hidden", "true");
    wrapper.append(icon, node("h3", "", title), node("p", "", description));
    return wrapper;
  }

  function jsonBlock(value) {
    const pre = node("pre", "json-block");
    pre.textContent = JSON.stringify(value, null, 2) ?? "null";
    pre.tabIndex = 0;
    pre.setAttribute("aria-label", "原始 JSON 数据");
    return pre;
  }

  function announce(message) { $("live-status").textContent = message; }
  function showNotice(message, error = false) {
    $("notice-text").textContent = message;
    $("notice").classList.toggle("is-error", error);
    $("notice").hidden = false;
  }
  function hideNotice() { $("notice").hidden = true; }
  function isActive(run) {
    return Boolean(run && (run.active === true || ["queued", "running"].includes(run.status)));
  }
  function selectedProfile() { return state.profiles.find((profile) => profile.profile_id === state.profileId); }
  function runTitle(run) {
    return run?.profile?.title || state.profiles.find((profile) => profile.profile_id === run?.profile_id)?.title
      || run?.profile_id || "历史任务";
  }
  function runStatus(run) {
    if (!run) return null;
    if (isActive(run) || ["interrupted", "blocked"].includes(run.status)) return run.status;
    return run.result?.status || run.status;
  }
  function safeArray(value) { return Array.isArray(value) ? value : []; }
  function number(value) { return Number.isFinite(Number(value)) ? Number(value) : 0; }
  function formatDuration(value) {
    const ms = number(value);
    if (ms < 1000) return `${Math.round(ms)} ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
    return `${Math.floor(ms / 60000)}m ${Math.floor(ms / 1000) % 60}s`;
  }
  function dateLabel(value, withDate = true) {
    if (!value) return "时间未记录";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const options = withDate
      ? { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }
      : { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false };
    return new Intl.DateTimeFormat("zh-CN", options).format(date);
  }
  function statusBadge(status) {
    const known = Object.hasOwn(STATUS, status);
    return node("span", `status-badge status-${known ? status : "idle"}`, known ? STATUS[status] : (status || "等待提问"));
  }
  function errorMessage(error) {
    if (error?.name === "AbortError") return "请求超时，请检查本地服务后重试。";
    return error?.message || "本地服务暂时不可用，请稍后重试。";
  }

  /** 限制网络等待时间，仅访问由客户端构造的同源 URL。 */
  async function api(path, options = {}) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(path, {
        credentials: "same-origin", cache: "no-store", ...options, signal: controller.signal,
        headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}), ...options.headers },
      });
      // 删除接口成功时返回 204；此时没有 JSON 正文。
      if (response.status === 204 && response.ok) return null;
      let body;
      try { body = await response.json(); }
      catch { throw new Error(`服务返回了无法读取的数据（HTTP ${response.status}）。`); }
      if (!response.ok) {
        const detail = body?.detail;
        const message = typeof detail === "string" ? detail : detail?.message;
        const error = new Error(message || (response.status === 422 ? "输入未通过校验，请检查内容、字段和执行配置。" : `请求未完成（HTTP ${response.status}）。`));
        error.code = detail?.code;
        error.status = response.status;
        throw error;
      }
      return body;
    } finally { window.clearTimeout(timeout); }
  }

  function setConnection(online) {
    const label = $("connection-label");
    const dot = node("span", `connection-dot ${online ? "online" : "offline"}`);
    label.replaceChildren(dot, document.createTextNode(online ? "服务已连接" : "连接已断开"));
  }

  function setView(view, focus = false) {
    if (!Object.hasOwn(VIEW_TITLES, view)) return;
    if (view === "evaluations" && state.info?.features?.evaluations !== true) view = "workbench";
    state.view = view;
    for (const button of document.querySelectorAll("[data-view]")) {
      const selected = button.dataset.view === view;
      button.classList.toggle("is-active", selected);
      if (selected) button.setAttribute("aria-current", "page");
      else button.removeAttribute("aria-current");
    }
    for (const [key] of Object.entries(VIEW_TITLES)) $("view-" + key).hidden = key !== view;
    $("breadcrumb-title").textContent = VIEW_TITLES[view];
    if (focus) $("main-content").focus({ preventScroll: true });
    if (view === "history") renderHistory();
    if (view === "sources") { renderSources(); if (state.sourcesLoadedProfileId !== state.sourcesProfileId && !state.sourcesLoading) refreshSources(); }
    if (view === "evaluations") refreshEvaluations();
    window.history.replaceState(null, "", `#${view}`);
  }

  function setTab(tab, focus = false) {
    if (!["answer", "evidence"].includes(tab)) return;
    state.tab = tab;
    for (const button of document.querySelectorAll("[data-tab]")) {
      const selected = button.dataset.tab === tab;
      button.classList.toggle("is-active", selected);
      button.setAttribute("aria-selected", String(selected));
      button.tabIndex = selected ? 0 : -1;
      $("panel-" + button.dataset.tab).hidden = !selected;
      if (selected && focus) button.focus();
    }
  }

  /** 根据元数据说明配置是否就绪；未就绪的配置仍可选择，以便查看详情。 */
  function selectProfile(id, replaceQuestion = true) {
    const profile = state.profiles.find((item) => item.profile_id === id);
    if (!profile) return;
    state.profileId = id;
    $("profile").value = id;
    if (replaceQuestion) $("question").value = String(safeArray(profile.suggested_questions)[0] || "");
    renderProfile();
    updateQuestionCount();
  }

  function renderProfile() {
    const profile = selectedProfile();
    const summary = $("profile-summary");
    if (!profile) {
      state.profileId = null;
      summary.replaceChildren(node("p", "loading-copy", "尚未配置执行模块。"));
      $("profile-description").textContent = "请在服务端配置数据、规划与内容模块后重新启动。";
      $("profile-readiness").textContent = "配置完成后，可在此选择数据池并提交任务。";
      $("profile-readiness").classList.add("is-unavailable");
      $("workspace-kind").textContent = "暂无配置";
      $("workspace-kind").classList.remove("is-project");
      $("environment-title").textContent = "尚未配置执行模块";
      $("environment-description").textContent = "等待服务端加载执行配置";
      $("question-hint").textContent = "请完成服务端配置后重新启动。";
      $("suggested-questions").replaceChildren();
      $("question").disabled = true;
      renderControls();
      return;
    }
    const dataset = node("div", "profile-dataset");
    const datasetHeading = node("div", "profile-dataset-heading");
    datasetHeading.append(node("span", "dataset-symbol", "▤"), node("strong", "", profile.dataset_name || profile.dataset_id));
    dataset.append(datasetHeading, node("span", "profile-dataset-meta", `${profile.source_count ?? "未提供"} 个已注册来源 · 版本 ${profile.revision || "未提供"}`), node("span", "profile-dataset-meta", `模型 ${profile.model_id || "未提供"} · 时限 ${profile.max_wall_time_ms == null ? "未提供" : formatDuration(profile.max_wall_time_ms)}`));
    const modules = node("div", "module-states");
    for (const [key, title] of Object.entries(MODULE_TITLES)) {
      const module = profile.modules?.[key];
      const status = Object.hasOwn(MODULE_STATES, module?.status) ? module.status : "unavailable";
      const item = node("div", `module-state module-${status}`);
      const label = node("span", "module-name", title);
      label.title = module?.label || title;
      item.append(node("span", "module-dot"), label, node("strong", "", MODULE_STATES[status]));
      if (module?.label) item.append(node("small", "", module.label));
      modules.append(item);
    }
    summary.replaceChildren(dataset, modules);
    $("profile-description").textContent = profile.description || "";
    $("profile-readiness").textContent = profile.ready
      ? `项目配置已就绪 · 模型 ${profile.model_id || "未提供"} · 单任务时限 ${formatDuration(profile.max_wall_time_ms)}`
      : `配置尚未就绪：${profile.unavailable_reason || "执行模块尚未连接。请选择已就绪配置，或等待项目模块接入。"}`;
    $("profile-readiness").classList.toggle("is-unavailable", !profile.ready);
    $("workspace-kind").textContent = "项目数据配置";
    $("workspace-kind").classList.add("is-project");
    $("environment-title").textContent = profile.dataset_name || "项目数据池";
    $("environment-description").textContent = profile.ready ? "所选配置已就绪" : "等待模块接入";
    $("question-hint").textContent = "问题将由所选项目配置的数据、规划与内容模块处理。";
    $("question").disabled = false;
    const suggestions = document.createDocumentFragment();
    for (const question of safeArray(profile.suggested_questions).slice(0, 3)) {
      const button = node("button", "suggestion-button", question);
      button.type = "button";
      button.addEventListener("click", () => { $("question").value = String(question); updateQuestionCount(); $("question").focus(); });
      suggestions.append(button);
    }
    $("suggested-questions").replaceChildren(suggestions);
    renderControls();
  }

  function renderProfiles() {
    for (const id of ["profile", "sources-profile"]) {
      const select = $(id);
      const fragment = document.createDocumentFragment();
      for (const profile of state.profiles) {
        const option = node("option", "", `${profile.title} · ${profile.ready ? "已就绪" : "未就绪"}`);
        option.value = profile.profile_id;
        fragment.append(option);
      }
      if (!state.profiles.length) fragment.append(node("option", "", "尚未配置执行模块"));
      select.replaceChildren(fragment);
      select.disabled = !state.profiles.length;
    }
    const initial = state.profiles.find((profile) => profile.ready) || state.profiles[0];
    selectProfile(state.profiles.some((profile) => profile.profile_id === state.profileId) ? state.profileId : initial?.profile_id, !state.profileId);
    if (!selectedProfile()) renderProfile();
    if (!state.profiles.some((profile) => profile.profile_id === state.sourcesProfileId)) state.sourcesProfileId = initial?.profile_id || null;
    $("sources-profile").value = state.sourcesProfileId || "";
    renderRecentRuns();
    renderHistory();
    if (state.view === "sources") refreshSources();
  }

  function renderInfo() {
    $("app-version").textContent = state.info?.version ? `v${state.info.version}` : "Agent Harness";
    // 实验能力由可信服务端显式开放；正式入口默认只有完整 Agent。
    const modes = safeArray(state.info?.enabled_modes).filter((mode) => Object.hasOwn(MODE_TITLES, mode));
    const enabled = modes.length ? modes : ["full_agent"];
    const previous = $("mode").value;
    $("mode").replaceChildren(...enabled.map((mode) => {
      const option = node("option", "", MODE_TITLES[mode]);
      option.value = mode;
      return option;
    }));
    $("mode").value = enabled.includes(previous) ? previous : enabled[0];
    $("mode").disabled = enabled.length === 1;
    document.querySelector('[data-view="evaluations"]').hidden = state.info?.features?.evaluations !== true;
    if (state.view === "evaluations" && state.info?.features?.evaluations !== true) setView("workbench");
    renderCapabilities();
  }

  function renderCapabilities() {
    const fragment = document.createDocumentFragment();
    for (const capability of safeArray(state.info?.capabilities)) {
      const card = node("article", "capability-card");
      const header = node("div", "capability-card-header");
      const status = String(capability.status || "");
      const pending = /pending|planned|incomplete|todo|待|未完成|未接入/i.test(status);
      const labels = { ready: "已具备", complete: "已完成", completed: "已完成", available: "可使用", implemented: "已实现", verified: "已验证", pending: "待联调", planned: "计划中" };
      header.append(node("h3", "", capability.name), node("span", `capability-status${pending ? " pending" : ""}`, labels[status] || status || "状态未提供"));
      card.append(header, node("p", "", capability.description));
      fragment.append(card);
    }
    if (!fragment.childNodes.length) fragment.append(emptyState("尚无能力状态", "服务未返回项目能力列表。"));
    $("capabilities").replaceChildren(fragment);
  }

  function updateQuestionCount() {
    $("question-count").textContent = `${$("question").value.length} / 2000`;
    renderControls();
  }

  function renderControls() {
    const run = state.run;
    const profile = selectedProfile();
    $("start-run").disabled = state.mutation || !profile?.ready || !$("question").value.trim();
    $("reset-question").disabled = !safeArray(profile?.suggested_questions).length;
    $("run-actions").hidden = !run;
    $("cancel-run").hidden = !isActive(run);
    $("cancel-run").disabled = state.mutation;
    $("resume-run").hidden = !run?.can_resume || isActive(run);
    $("resume-run").disabled = state.mutation;
    $("delete-run").disabled = state.mutation || !run || isActive(run);
    $("delete-run").title = isActive(run) ? "请先停止任务，等待清理完成后再删除。" : "删除任务及其日志、证据和恢复记录";
    for (const button of document.querySelectorAll('[data-action="delete"]')) {
      button.disabled = state.mutation || button.dataset.active === "true";
    }
    if (run) {
      const prefix = `/api/runs/${encodeURIComponent(run.run_id)}`;
      $("download-json").href = prefix + "/export";
      $("download-report").href = prefix + "/report";
    }
  }

  async function refreshHistory(silent = false) {
    const version = ++state.historyVersion;
    try {
      const result = await api("/api/runs");
      if (version !== state.historyVersion) return;
      state.runs = safeArray(result.runs);
      renderTimelineTasks();
      setConnection(true);
      const signature = JSON.stringify(state.runs);
      if (signature !== state.historySignature) {
        state.historySignature = signature;
        renderRecentRuns();
        renderHistory();
      }
    } catch (error) {
      if (version !== state.historyVersion) return;
      setConnection(false);
      if (!silent) showNotice(errorMessage(error), true);
      if (!state.runs.length) {
        $("recent-runs").replaceChildren(node("p", "sidebar-empty", "暂时无法读取历史"));
        $("history-list").replaceChildren(emptyState("暂时无法读取历史", "检查本地服务后，点击刷新历史重试。"));
      }
    }
  }

  function renderRecentRuns() {
    $("nav-history-count").textContent = state.runs.length;
    const focusedId = $("recent-runs").contains(document.activeElement) ? document.activeElement?.dataset?.runId : null;
    const focusedAction = document.activeElement?.dataset?.action;
    const fragment = document.createDocumentFragment();
    for (const run of state.runs.slice(0, 5)) {
      const row = node("div", "recent-run-row");
      const button = node("button", `recent-run${run.run_id === state.selectedRunId ? " is-selected" : ""}`);
      button.type = "button";
      button.dataset.runId = run.run_id;
      button.dataset.action = "open";
      button.setAttribute("aria-label", `${STATUS[runStatus(run)] || runStatus(run)}：${run.question}`);
      const dot = node("span", `run-dot ${Object.hasOwn(STATUS, runStatus(run)) ? runStatus(run) : ""}`);
      dot.setAttribute("aria-hidden", "true");
      button.append(dot, node("strong", "", run.question), node("small", "", `${runTitle(run)} · ${dateLabel(run.created_at)}`));
      button.addEventListener("click", () => openRun(run.run_id));
      row.append(button, deleteButton(run));
      fragment.append(row);
    }
    if (!state.runs.length) fragment.append(node("p", "sidebar-empty", "还没有运行记录。\n从右侧开始一次探索。"));
    $("recent-runs").replaceChildren(fragment);
    if (focusedId) Array.from($("recent-runs").querySelectorAll("button")).find((button) => button.dataset.runId === focusedId && button.dataset.action === focusedAction)?.focus({ preventScroll: true });
  }

  function renderHistory() {
    const focusedId = $("history-list").contains(document.activeElement) ? document.activeElement?.dataset?.runId : null;
    const focusedAction = document.activeElement?.dataset?.action;
    const query = $("history-search").value.trim().toLocaleLowerCase();
    const filter = $("history-filter").value;
    const runs = state.runs.filter((run) => {
      const matchesText = `${run.question} ${run.run_id} ${runTitle(run)}`.toLocaleLowerCase().includes(query);
      const matchesStatus = filter === "all" || (filter === "active" && isActive(run)) || (filter === "failed" && ["failed", "blocked", "error"].includes(runStatus(run))) || runStatus(run) === filter;
      return matchesText && matchesStatus;
    });
    $("history-count").textContent = `${runs.length} / ${state.runs.length} 次任务`;
    const fragment = document.createDocumentFragment();
    for (const run of runs) {
      const row = node("div", "history-card-row");
      const card = node("button", "history-card");
      card.type = "button";
      card.dataset.runId = run.run_id;
      card.dataset.action = "open";
      const top = node("div", "history-card-top");
      top.append(node("strong", "", runTitle(run)), statusBadge(runStatus(run)), node("span", "history-card-time", dateLabel(run.created_at)));
      const meta = node("div", "history-card-meta");
      meta.append(node("span", "", MODE_TITLES[run.mode] || run.mode), node("code", "", run.run_id));
      const arrow = node("span", "history-card-arrow", "↗");
      arrow.setAttribute("aria-hidden", "true");
      card.append(top, node("h3", "", run.question), meta, arrow);
      card.addEventListener("click", () => openRun(run.run_id));
      row.append(card, deleteButton(run));
      fragment.append(row);
    }
    if (!runs.length) {
      const empty = emptyState(state.runs.length ? "没有匹配的运行" : "记录从一次提问开始", state.runs.length ? "调整关键词或状态筛选，查找其他运行。" : "在智能问答中选择已就绪的执行配置，提问后即可在这里回看。", "◷");
      empty.classList.add("history-empty");
      fragment.append(empty);
    }
    $("history-list").replaceChildren(fragment);
    if (focusedId) Array.from($("history-list").querySelectorAll("button")).find((button) => button.dataset.runId === focusedId && button.dataset.action === focusedAction)?.focus({ preventScroll: true });
  }

  function rememberRun(runId) {
    try { window.localStorage.setItem("agent-lab:last-run", runId); } catch { /* 本地存储可能被禁用，但仍可通过 API 查看历史记录。 */ }
  }

  /** 删除按钮与打开按钮并列，避免嵌套交互控件；服务端仍负责最终运行状态校验。 */
  function deleteButton(run) {
    const button = node("button", "button button-small button-delete", "删除");
    button.type = "button";
    button.dataset.runId = run.run_id;
    button.dataset.action = "delete";
    button.dataset.active = String(isActive(run));
    button.disabled = state.mutation || isActive(run);
    button.title = isActive(run) ? "请先停止任务，等待清理完成后再删除。" : "删除任务及其日志、证据和恢复记录";
    button.setAttribute("aria-label", `删除任务：${run.question}`);
    button.addEventListener("click", () => deleteRun(run.run_id));
    return button;
  }

  /** 仅在后端确认删除后清理视图，并使删除前发出的读取失效，防止旧响应恢复已删内容。 */
  function forgetRun(runId) {
    state.historyVersion += 1;
    state.historySignature = null;
    state.runs = state.runs.filter((run) => run.run_id !== runId);
    state.artifactCache.clear();
    try {
      if (window.localStorage.getItem("agent-lab:last-run") === runId) window.localStorage.removeItem("agent-lab:last-run");
    } catch { /* 浏览器可禁用本地偏好存储；服务端删除不依赖它。 */ }
    if (state.selectedRunId === runId) {
      state.runVersion += 1;
      state.selectionVersion += 1;
      state.inspectorVersion += 1;
      state.selectedRunId = null;
      state.run = null;
      state.selectedEventKey = null;
      state.renderSignature = null;
      state.lastPollError = null;
      $("selected-run-question").textContent = "";
      $("selected-run-label").textContent = "";
      $("selected-run-id").textContent = "";
      $("timeline-phase").value = "all";
      $("timeline-search").value = "";
      renderRun();
    }
    renderRecentRuns();
    renderHistory();
    renderTimelineTasks();
  }

  async function deleteRun(runId) {
    if (!runId || state.mutation) return;
    const run = state.selectedRunId === runId && state.run ? state.run : state.runs.find((item) => item.run_id === runId);
    if (!run) return;
    if (isActive(run)) { showNotice("请先停止任务，等待清理完成后再删除。"); return; }
    if (!window.confirm(`删除任务“${run.question}”？\n答案、执行日志、证据工件和 SQLite 恢复记录将一起删除，无法恢复。`)) return;
    state.mutation = true;
    renderControls();
    hideNotice();
    try {
      try { await api(`/api/runs/${encodeURIComponent(runId)}`, { method: "DELETE" }); }
      catch (error) { if (error.status !== 404) throw error; }
      forgetRun(runId);
      announce("任务及其运行记录已删除。");
      showNotice("任务及其日志、证据和恢复记录已删除。");
      await refreshHistory(true);
    } catch (error) {
      showNotice(errorMessage(error), true);
    } finally {
      state.mutation = false;
      renderControls();
      schedulePoll();
    }
  }

  async function openRun(runId, switchView = true) {
    if (!runId) return;
    state.selectionVersion += 1;
    const changed = state.selectedRunId !== runId;
    if (changed) {
      $("timeline-phase").value = "all";
      $("timeline-search").value = "";
      $("timeline").scrollTop = 0;
      state.selectedRunId = runId;
      state.run = null;
      state.selectedEventKey = null;
      state.renderSignature = null;
      state.artifactCache.clear();
      state.inspectorVersion += 1;
      renderRun();
      rememberRun(runId);
    }
    if (switchView) setView("workbench");
    renderRecentRuns();
    await refreshRun(false);
    schedulePoll();
  }

  /** 通过版本检查，防止旧响应覆盖新选中的运行记录。 */
  async function refreshRun(silent = true) {
    const runId = state.selectedRunId;
    if (!runId) return;
    const version = ++state.runVersion;
    try {
      const run = await api(`/api/runs/${encodeURIComponent(runId)}`);
      if (runId !== state.selectedRunId || version !== state.runVersion) return;
      const previousStatus = state.run?.status;
      state.run = run;
      state.lastPollError = null;
      setConnection(true);
      // 轮询结果未变时不重建事件按钮，避免打断键盘焦点。
      const signature = JSON.stringify(run);
      if (signature !== state.renderSignature) {
        state.renderSignature = signature;
        renderRun();
      }
      if (previousStatus && previousStatus !== run.status) {
        announce(`运行状态：${STATUS[runStatus(run)] || runStatus(run)}`);
        await refreshHistory(true);
      }
    } catch (error) {
      if (runId !== state.selectedRunId || version !== state.runVersion) return;
      setConnection(false);
      const message = errorMessage(error);
      if (!silent || state.lastPollError !== message) showNotice(message, true);
      state.lastPollError = message;
      if (!state.run) {
        $("timeline").replaceChildren(emptyState("暂时无法读取这次任务", "检查本地服务后，可从任务历史重新打开。", "↻"));
        $("panel-answer").replaceChildren(emptyState("暂时无法读取这次任务", "检查本地服务后，可从任务历史重新打开。", "↻"));
      }
    }
  }

  function renderRun() {
    const run = state.run;
    const status = statusBadge(runStatus(run));
    status.id = "run-status";
    $("run-status").replaceWith(status);
    $("selected-run-info").hidden = !run;
    if (run) {
      $("selected-run-question").textContent = run.question;
      $("selected-run-label").textContent = `${runTitle(run)} · ${MODE_TITLES[run.mode] || run.mode} · ${dateLabel(run.created_at)}`;
      $("selected-run-id").textContent = run.run_id;
    }
    renderControls();
    renderRunNote();
    renderMetrics();
    renderTimeline();
    renderAnswer();
    renderEvidence();
    if ($("audit-details").open) renderInspector();
    else if (!run) $("panel-details").replaceChildren(emptyState("展开一次真实调用", "开始运行后，点击时间线事件查看原始请求和响应。", "{ }"));
  }

  function renderRunNote() {
    const run = state.run;
    const note = $("run-state-note");
    const status = runStatus(run);
    const messages = {
      interrupted: "任务因执行中断而停止。恢复前会检查保存状态；存在未知调用结果时会拒绝恢复。",
      blocked: "任务未通过安全恢复检查。可展开执行过程查看已记录的调用。",
      insufficient_evidence: "本次任务证据不足。请检查已保存证据，或调整问题后创建新任务。",
      budget_exceeded: "任务已达到执行预算并停止。已保存的证据和审计记录仍可查看。",
      timeout: "任务已超过调用或总执行期限。请查看执行过程，了解停止前的已完成工作。",
    };
    const parts = [];
    const failure = status === "completed" ? null : run?.failure || run?.result?.failure;
    const diagnosticOpen = Boolean(note.querySelector(".failure-diagnostic[open]"));
    if (messages[status]) parts.push(messages[status]);
    if (run?.resume_blocked_reason) parts.push(`无法恢复：${run.resume_blocked_reason}`);
    if (run?.error) parts.push(typeof run.error === "string" ? run.error : `${run.error.message || "执行未完成"}${run.error.code ? `（${run.error.code}）` : ""}`);
    if (run?.result?.stop_reason && status !== "completed") parts.push(`停止原因：${run.result.stop_reason}`);
    if (failure?.error) {
      const phase = PHASE_TITLES[failure.phase] || failure.phase || "执行调用";
      parts.push(`${phase}：${failureExplanation(failure.error)}（${diagnosticText(failure.error.code, 100)}）`);
      if (isActive(run)) parts.push("任务仍在运行，模型可根据反馈决定下一步。");
    }
    note.hidden = !parts.length;
    note.replaceChildren();
    if (parts.length) note.append(node("p", "failure-summary", parts.join(" ")));
    if (failure?.error) {
      const diagnostic = renderFailureDiagnostic(failure);
      diagnostic.open = diagnosticOpen;
      note.append(diagnostic);
    }
    note.classList.toggle("is-error", Boolean(run?.error || run?.resume_blocked_reason) || ["failed", "blocked", "error", "timeout"].includes(status));
  }

  /** 诊断是未接纳的模型数据，始终按纯文本、有界预览处理。 */
  function diagnosticText(value, maximum = 600) {
    let text;
    try { text = typeof value === "string" ? value : JSON.stringify(value); }
    catch { text = "无法显示该值"; }
    text = text ?? "null";
    return text.length > maximum ? `${text.slice(0, maximum)}…（预览截断）` : text;
  }

  function failureExplanation(error) {
    const explanations = {
      invalid_model_citations: "模型引用了本次证据之外的编号，输出未被接纳。",
      invalid_model_sources: "模型返回了当前目录之外的来源编号，输出未被接纳。",
      invalid_model_output: "模型输出未通过 JSON 或结构校验。",
      invalid_answer_citations: "待核验答案包含当前证据之外的引用。",
      provider_timeout: "模型请求超过调用期限。",
      provider_http_error: "模型服务返回了 HTTP 错误。",
      provider_transport_error: "模型请求在网络传输时失败。",
      invalid_module_response: "模块响应未通过运行控制器校验。",
    };
    return explanations[error.code] || diagnosticText(error.message || "调用未完成", 300);
  }

  function renderFailureDiagnostic(failure) {
    const diagnostic = node("details", "failure-diagnostic");
    diagnostic.append(node("summary", "", "查看错误位置、引用差异与原始记录"));
    const body = node("div", "failure-diagnostic-body");
    body.append(node("p", "failure-call", `${PHASE_TITLES[failure.phase] || failure.phase || "执行调用"} · ${diagnosticText(failure.call_id || "未记录调用 ID", 200)}`));
    body.append(node("p", "failure-message", diagnosticText(failure.error?.message, 1000)));
    const details = failure.error?.details;
    if (details && typeof details === "object") {
      const issues = safeArray(details.issues);
      if (issues.length) {
        const list = node("ul", "failure-issues");
        for (const issue of issues.slice(0, 6)) {
          const item = node("li");
          if (!issue || typeof issue !== "object") {
            item.textContent = diagnosticText(issue);
            list.append(item);
            continue;
          }
          item.append(node("strong", "", diagnosticText(issue.path || issue.code || "输出字段", 160)));
          if (Object.hasOwn(issue, "actual")) item.append(node("p", "", `实际值：${diagnosticText(issue.actual)}`));
          if (Object.hasOwn(issue, "expected")) item.append(node("p", "", `要求：${diagnosticText(issue.expected)}`));
          if (issue.message) item.append(node("p", "", diagnosticText(issue.message, 300)));
          list.append(item);
        }
        if (issues.length > 6) list.append(node("li", "", `另有 ${issues.length - 6} 项，见原始记录。`));
        body.append(list);
      }
      for (const [key, title] of [["allowed_evidence_ids", "允许引用的证据 ID"], ["allowed_source_ids", "允许使用的来源 ID"]]) {
        if (Array.isArray(details[key])) body.append(node("p", "failure-allowed", `${title}：${diagnosticText(details[key].slice(0, 8), 1200)}${details[key].length > 8 ? "；其他项见原始记录。" : ""}`));
      }
      if (Object.hasOwn(details, "rejected_output")) {
        const rejected = node("details", "failure-rejected");
        rejected.append(node("summary", "", "被拒绝的模型输出预览"), node("pre", "", diagnosticText(details.rejected_output, 2000)));
        body.append(rejected);
      }
      if (details.omitted) body.append(node("p", "failure-omitted", `诊断包含省略项：${diagnosticText(details.omitted, 300)}`));
    } else body.append(node("p", "failure-omitted", "这份记录没有保存具体错误值，无法从摘要还原模型填错的引用。原始请求可用于核对当时允许的证据。"));
    if (failure.attempt_id && failure.event_seq) {
      const button = node("button", "text-button", "查看这次调用的原始记录 ↗");
      button.type = "button";
      button.addEventListener("click", () => selectEvent(`${failure.attempt_id}:${failure.event_seq}`));
      body.append(button);
    }
    diagnostic.append(body);
    return diagnostic;
  }

  function renderMetrics() {
    const metrics = state.run?.metrics;
    for (const key of ["tools", "steps", "evidence", "model_calls"]) {
      $("metric-" + key).textContent = metrics ? number(metrics[key]).toLocaleString("zh-CN") : "—";
    }
    $("metric-duration_ms").textContent = metrics ? formatDuration(metrics.duration_ms) : "—";
    $("metric-attempts").textContent = metrics ? `${number(metrics.attempts)} 次执行尝试` : "尚未开始";
    const evidenceCount = safeArray(state.run?.evidence).length;
    $("evidence-tab-count").textContent = evidenceCount;
  }

  function eventKey(event) { return `${event.attempt_id || ""}:${event.seq}`; }
  function replayed(event) { return event.replayed === true || event.detail?.replayed === true; }
  function eventTitle(event) {
    if (event.event_type === "agent.action.rejected" && event.detail?.kind === "finish") {
      // 过期是适用状态已变化，并不表示新的核验已判定答案错误。
      if (event.detail.code === "verification_stale") return "完成提议未获准 · 核验已过期";
      if (event.detail.code === "verification_required") return "完成提议未获准 · 需要当前有效的支持核验";
    }
    if (EVENT_TITLES[event.event_type]) {
      if (event.event_type === "step.state") {
        const stepStates = { pending: "步骤等待执行", running: "步骤开始执行", completed: "步骤执行完成", failed: "步骤执行失败", blocked: "步骤受阻", skipped: "步骤已跳过" };
        return stepStates[event.status] || EVENT_TITLES[event.event_type];
      }
      return EVENT_TITLES[event.event_type];
    }
    const match = String(event.event_type || "").match(/^(.*)\.call\.(started|completed|failed|cancelled|retrying)$/);
    if (match) {
      const phase = PHASE_TITLES[match[1]] || match[1];
      return `${phase} · ${{ started: "开始", completed: "完成", failed: "失败", cancelled: "取消", retrying: "重试" }[match[2]]}`;
    }
    return event.event_type || "执行事件";
  }

  /** 使用独立 run_id 切换每次提问；恢复段继续归属原任务。 */
  function renderTimelineTasks() {
    const select = $("timeline-task");
    const tasks = [...state.runs];
    if (state.run && !tasks.some((run) => run.run_id === state.run.run_id)) tasks.unshift(state.run);
    const signature = JSON.stringify([state.selectedRunId, tasks.map((run) => [run.run_id, run.question, run.created_at])]);
    if (select.dataset.signature !== signature) {
      const options = document.createDocumentFragment();
      if (!state.selectedRunId) {
        const placeholder = node("option", "", "选择一次提问");
        placeholder.value = "";
        options.append(placeholder);
      }
      for (const run of tasks) {
        const option = node("option", "", `${run.question} · ${dateLabel(run.created_at)} · ${run.run_id.slice(-8)}`);
        option.value = run.run_id;
        option.selected = run.run_id === state.selectedRunId;
        options.append(option);
      }
      select.replaceChildren(options);
      select.dataset.signature = signature;
    }
    select.disabled = !tasks.length;
    const selected = state.run || tasks.find((run) => run.run_id === state.selectedRunId);
    $("timeline-context").textContent = selected ? `当前任务：${selected.question}。每次提问单独记录。` : "每次提问单独记录；选择任务后查看对应的执行过程。";
  }

  function timelinePhase(event) {
    const type = String(event.event_type || "");
    if (type.startsWith("catalog.") || type.startsWith("content.prepare_catalog.")) return "catalog";
    if (type.startsWith("planner.") || type.startsWith("agent.") || type.startsWith("task_plan.")) return "planning";
    if (type.startsWith("content.compose.") || type.startsWith("content.verify.") || type === "verification" || ["content.output.rejected", "content.feedback.resolved"].includes(type)) return "answer";
    if (/^(tool\.|content\.search\.|plan\.|step\.|subtask\.|evidence\.|observation\.)/.test(type)) return "execution";
    return "state";
  }

  function renderTimeline() {
    renderTimelineTasks();
    const allEvents = safeArray(state.run?.events);
    const phase = $("timeline-phase").value;
    const query = $("timeline-search").value.trim().toLocaleLowerCase();
    const filtering = phase !== "all" || Boolean(query);
    const events = allEvents.filter((event) => (phase === "all" || timelinePhase(event) === phase)
      && (!query || `${eventTitle(event)} ${event.seq} ${event.event_type} ${event.step_id || ""} ${event.error?.code || ""} ${event.error?.message || ""} ${JSON.stringify(event.detail || {})}`.toLocaleLowerCase().includes(query)));
    $("event-count").textContent = `本任务 ${allEvents.length} 个事件`;
    $("timeline-count").textContent = `显示 ${events.length} / ${allEvents.length} 个事件`;
    const container = $("timeline");
    const sameRun = container.dataset.runId === state.selectedRunId;
    const expanded = new Map(sameRun ? Array.from(container.querySelectorAll(".timeline-stage")).map((group) => [group.dataset.stageKey, group.open]) : []);
    container.dataset.runId = state.selectedRunId || "";
    const oldScroll = container.scrollTop;
    const nearBottom = container.scrollHeight - container.clientHeight - oldScroll < 45;
    const focusedKey = container.contains(document.activeElement) ? document.activeElement?.dataset?.eventKey : null;
    const focusedStageKey = container.contains(document.activeElement) && document.activeElement?.tagName === "SUMMARY"
      ? document.activeElement.closest(".timeline-stage")?.dataset.stageKey : null;
    if (!events.length && allEvents.length) {
      container.replaceChildren(emptyState("没有匹配的事件", "调整执行阶段或搜索词，查看本任务的其他记录。", "⌕"));
      return;
    }
    if (!events.length) {
      container.replaceChildren(emptyState(state.run ? "等待第一个执行事件" : "每一步，都有迹可循", state.run ? "执行开始后，已保存的事件会自动出现在这里。" : "开始运行后，这里会显示真实的规划、工具调用与证据记录。", "⌁"));
      return;
    }
    const fragment = document.createDocumentFragment();
    const groups = new Map();
    for (const event of events) {
      const id = event.attempt_id || "initial";
      if (!groups.has(id)) groups.set(id, []);
      groups.get(id).push(event);
    }
    const attemptIds = [...new Set(allEvents.map((event) => event.attempt_id || "initial"))];
    let index = 0;
    for (const [id, records] of groups) {
      index += 1;
      const heading = node("div", "attempt-heading");
      const attemptNumber = attemptIds.indexOf(id) + 1;
      heading.append(node("strong", "", `执行 ${String(attemptNumber).padStart(2, "0")}`), node("span", "", attemptNumber === 1 ? "首次执行" : "恢复执行"));
      heading.title = id;
      fragment.append(heading);
      // 仅折叠连续的同阶段事件，保持真实审计顺序，不猜测旧记录的子任务归属。
      const stages = [];
      for (const event of records) {
        const kind = timelinePhase(event);
        if (stages.at(-1)?.kind !== kind) stages.push({ kind, records: [] });
        stages.at(-1).records.push(event);
      }
      for (const stage of stages) {
        const group = node("details", "timeline-stage");
        group.dataset.stageKey = `${id}:${stage.records[0].seq}`;
        const failed = stage.records.some((event) => event.error || /failed|cancelled/.test(event.event_type));
        group.open = filtering || (expanded.get(group.dataset.stageKey) ?? (failed || (isActive(state.run) && stage === stages.at(-1) && index === groups.size)));
        const titles = { catalog: "读取目录", planning: "模型规划", execution: "检索与工具", answer: "回答与核验", state: "状态与其他" };
        const summary = node("summary", "", titles[stage.kind]);
        summary.append(node("small", "", `${stage.records.length} 个事件 · #${stage.records[0].seq}–${stage.records.at(-1).seq}`));
        group.append(summary);
        for (const event of stage.records) {
          const key = eventKey(event);
          const failed = Boolean(event.error) || /failed|cancelled/.test(event.event_type || "");
          const row = node("button", `event-row${replayed(event) ? " is-replayed" : ""}${failed ? " is-error" : ""}${key === state.selectedEventKey ? " is-selected" : ""}`);
          row.type = "button";
          row.dataset.eventKey = key;
          row.setAttribute("aria-label", `${eventTitle(event)}，${dateLabel(event.timestamp_utc, false)}${replayed(event) ? "，历史重放" : ""}，查看记录`);
          const dot = node("span", "event-dot");
          dot.setAttribute("aria-hidden", "true");
          const body = node("span", "event-body");
          const title = node("span", "event-title", eventTitle(event));
          if (replayed(event)) title.append(node("span", "replayed-badge", "历史重放"));
          if (event.data_ref) {
            const ref = node("span", "event-ref-icon", "↗");
            ref.setAttribute("aria-hidden", "true");
            title.append(ref);
          }
          const meta = node("span", "event-meta");
          meta.append(node("span", "", dateLabel(event.timestamp_utc, false)), node("span", "event-type", `#${event.seq} · ${event.event_type}`));
          if (event.duration_ms !== null && event.duration_ms !== undefined) meta.append(node("span", "", formatDuration(event.duration_ms)));
          body.append(title, meta);
          const detail = event.error ? `${diagnosticText(event.error.code, 100)} · ${failureExplanation(event.error)}` : event.detail?.tool_name || event.step_id;
          if (detail) body.append(node("span", "event-detail-line", detail));
          row.append(dot, body);
          row.addEventListener("click", () => selectEvent(key));
          group.append(row);
        }
        fragment.append(group);
      }
    }
    container.replaceChildren(fragment);
    if (focusedKey) {
      const focusedRow = Array.from(container.querySelectorAll("button")).find((row) => row.dataset.eventKey === focusedKey);
      focusedRow?.focus({ preventScroll: true });
    }
    if (focusedStageKey) {
      const focusedGroup = Array.from(container.querySelectorAll(".timeline-stage")).find((group) => group.dataset.stageKey === focusedStageKey);
      focusedGroup?.querySelector("summary")?.focus({ preventScroll: true });
    }
    container.scrollTop = nearBottom && isActive(state.run) ? container.scrollHeight : oldScroll;
  }

  function selectEvent(key) {
    state.selectedEventKey = key;
    if (!Array.from($("timeline").querySelectorAll("button")).some((button) => button.dataset.eventKey === key)) {
      $("timeline-phase").value = "all";
      $("timeline-search").value = "";
      renderTimeline();
    }
    for (const button of $("timeline").querySelectorAll("button")) button.classList.toggle("is-selected", button.dataset.eventKey === key);
    const row = Array.from($("timeline").querySelectorAll("button")).find((button) => button.dataset.eventKey === key);
    const group = row?.closest(".timeline-stage");
    if (group) group.open = true;
    $("audit-details").open = true;
    renderInspector();
  }

  function renderAnswer() {
    const container = $("panel-answer");
    const answer = state.run?.result?.answer;
    if (!answer) {
      const run = state.run;
      let title = "让结论有据可依";
      let description = "运行完成后，查看答案、引用的证据以及仍待解决的问题。";
      if (isActive(run)) { title = "正在寻找有依据的答案"; description = "答案生成后显示；是否核验取决于运行政策，执行过程与证据正在持续记录。"; }
      else if (run && ["failed", "blocked", "interrupted", "error", "insufficient_evidence", "budget_exceeded", "timeout"].includes(runStatus(run))) { title = "本次运行尚未形成答案"; description = "查看时间线、证据和调用详情，了解停止时已完成的工作。"; }
      container.replaceChildren(emptyState(title, description, "✧"));
      return;
    }
    const fragment = document.createDocumentFragment();
    const heading = node("div", "answer-title");
    const complete = runStatus(state.run) === "completed";
    const answerTitle = complete ? "答案已生成" : "当前已保存的答案";
    heading.append(node("span", "", complete ? "✓" : "ⓘ"), document.createTextNode(`${answerTitle} · ${MODE_TITLES[state.run.mode] || state.run.mode}`));
    const content = node("div", "answer-content");
    for (const [index, paragraph] of String(answer.text).split(/\n\s*\n/).entries()) content.append(node("p", index === 0 ? "answer-lead" : "", paragraph));
    fragment.append(heading, content);
    if (complete) fragment.append(node("p", "detail-footnote", "控制器已按当前运行政策接受完成提议。运行完成、内部核验通过和外部评测正确是不同概念；completed 不保证事实正确，核验记录请查看执行过程。"));
    const citations = safeArray(answer.citations);
    if (citations.length) {
      fragment.append(node("h3", "answer-subheading", `依据的证据 · ${citations.length}`));
      const list = node("div", "citation-list");
      const evidence = safeArray(state.run?.evidence);
      const references = new Map(evidence.map((record, index) => [record.evidence_id, { record, index }]));
      const sources = new Map(safeArray(state.run?.sources).map((source) => [source.source_id, source]));
      for (const id of citations) {
        const { record, index } = references.get(id) || {};
        const label = record ? `证据 ${index + 1} · ${sources.get(record.source_id)?.name || "未命名来源"}` : "未找到的引用";
        const button = node("button", "citation-button", label);
        button.type = "button";
        button.title = record ? locatorLabel(record.locator) : `当前记录中不存在引用 ${id}`;
        button.setAttribute("aria-label", `${label}，查看证据`);
        button.addEventListener("click", () => revealEvidence(id));
        list.append(button);
      }
      fragment.append(list);
    }
    if (safeArray(answer.unresolved_questions).length) {
      fragment.append(node("h3", "answer-subheading", "仍待解决的问题"));
      const list = node("ul", "unresolved-list");
      for (const question of answer.unresolved_questions) list.append(node("li", "", question));
      fragment.append(list);
    }
    if (answer.structured_result !== null && answer.structured_result !== undefined) {
      const details = node("details", "structured-output");
      details.append(node("summary", "", "查看结构化结果"), jsonBlock(answer.structured_result));
      fragment.append(details);
    }
    container.replaceChildren(fragment);
  }

  function revealEvidence(id) {
    setTab("evidence");
    const cards = $("panel-evidence").querySelectorAll("[data-evidence-id]");
    let target = null;
    for (const card of cards) {
      const matches = card.dataset.evidenceId === id;
      card.classList.toggle("is-highlighted", matches);
      if (matches) target = card;
    }
    if (target) { target.scrollIntoView({ behavior: "auto", block: "nearest" }); target.focus({ preventScroll: true }); }
    else showNotice("当前运行中未找到该引用对应的证据记录。");
  }

  function payloadText(payload) {
    if (typeof payload === "string") return payload;
    if (payload === null || payload === undefined) return "证据内容为空";
    return JSON.stringify(payload, null, 2);
  }

  /** 只解释已知定位字段，保留原值；未知格式不推断坐标或跳转链接。 */
  function locatorLabel(locator) {
    if (!locator || !Object.keys(locator).length) return "未提供定位信息";
    const labels = [
      ["table", "表"], ["sheet", "工作表"], ["page", "页"], ["line", "行"],
      ["row", "行"], ["column", "列"], ["range", "区域"], ["cell", "单元格"],
      ["path", "路径"], ["json_path", "JSON 路径"], ["pointer", "JSON 指针"], ["query", "查询条件"],
      ["line_start", "起始行"], ["line_end", "结束行"], ["page_start", "起始页"], ["page_end", "结束页"],
    ];
    const parts = [];
    const understood = new Set(["type"]);
    for (const [key, label] of labels) {
      const value = locator[key];
      if (value === null || value === undefined || typeof value === "object" || value === "") continue;
      understood.add(key);
      parts.push(["page", "line", "row", "column"].includes(key) && Number.isSafeInteger(value)
        ? `第 ${value} ${label}` : `${label}：${String(value)}`);
    }
    if (!parts.length) return "定位信息见原始记录";
    if (Object.keys(locator).some((key) => !understood.has(key))) parts.push("其他定位信息见原始记录");
    return parts.join(" · ");
  }

  /** 字段和表格保留原始值；预览有界，完整数据始终可在原始记录中查看。 */
  function evidencePayload(payload) {
    const wrapper = node("div", "evidence-payload");
    if (!payload || typeof payload !== "object") {
      wrapper.textContent = payloadText(payload);
      return wrapper;
    }
    wrapper.classList.add("is-structured");
    if (Array.isArray(payload) && payload.length && payload.every((row) => row && typeof row === "object" && !Array.isArray(row))) {
      const keys = [...new Set(payload.slice(0, 10).flatMap((row) => Object.keys(row)))].slice(0, 8);
      if (!keys.length) {
        wrapper.append(node("p", "", `${payload.length} 条记录，未提供字段；完整数据见原始记录。`));
        return wrapper;
      }
      const table = node("table", "payload-table");
      const header = node("tr");
      for (const key of keys) { const cell = node("th", "", key); cell.scope = "col"; header.append(cell); }
      const head = node("thead"); head.append(header); table.append(head);
      const body = node("tbody");
      for (const row of payload.slice(0, 10)) {
        const line = node("tr");
        for (const key of keys) line.append(node("td", "", Object.hasOwn(row, key) ? payloadText(row[key]) : "—"));
        body.append(line);
      }
      table.append(body);
      const scroll = node("div", "payload-table-scroll");
      scroll.tabIndex = 0; scroll.setAttribute("aria-label", "证据数据表"); scroll.append(table);
      wrapper.append(scroll, node("p", "payload-preview-note", "显示前 10 条以内、前 8 个字段；完整数据见原始记录。"));
    } else {
      const fields = node("dl", "payload-fields");
      const entries = Object.entries(payload);
      for (const [key, value] of entries.slice(0, 12)) fields.append(node("dt", "", Array.isArray(payload) ? `第 ${Number(key) + 1} 项` : key), node("dd", "", payloadText(value)));
      wrapper.append(entries.length ? fields : node("p", "", "未提供内容"));
      if (entries.length > 12) wrapper.append(node("p", "payload-preview-note", "显示前 12 项；完整数据见原始记录。"));
    }
    return wrapper;
  }

  function renderEvidence() {
    const container = $("panel-evidence");
    const scroll = container.scrollTop;
    const openRecords = new Set(Array.from(container.querySelectorAll(".evidence-card details[open]")).map((details) => details.closest("[data-evidence-id]")?.dataset.evidenceId));
    const sourcesOpen = Boolean(container.querySelector(".source-details[open]"));
    const evidence = safeArray(state.run?.evidence);
    const sources = safeArray(state.run?.sources);
    const sourcesById = new Map(sources.map((source) => [source.source_id, source]));
    const fragment = document.createDocumentFragment();
    for (const [index, record] of evidence.entries()) {
      const source = sourcesById.get(record.source_id);
      const card = node("article", "evidence-card");
      card.dataset.evidenceId = record.evidence_id;
      card.tabIndex = -1;
      card.setAttribute("aria-label", `证据 ${index + 1} · ${source?.name || "未命名来源"}`);
      const heading = node("div", "evidence-heading");
      const types = { text: "文字摘录", json: "结构化数据", table: "表格数据" };
      heading.append(node("strong", "", `证据 ${index + 1} · ${source?.name || "未命名来源"}`), node("span", "", Object.hasOwn(types, record.content_type) ? types[record.content_type] : record.content_type));
      card.append(heading, node("p", "evidence-fragment-label", "本次实际读取的原文或数据"), evidencePayload(record.payload));
      const metadata = node("dl", "evidence-meta");
      metadata.append(node("dt", "", "位置"), node("dd", "", locatorLabel(record.locator)));
      card.append(metadata);
      for (const warning of safeArray(record.warnings)) card.append(node("div", "evidence-warning", `提示：${warning}`));
      const details = node("details", "evidence-data");
      details.open = openRecords.has(record.evidence_id);
      details.append(node("summary", "", "查看原始记录与编号"), jsonBlock(record));
      card.append(details);
      fragment.append(card);
    }
    if (!evidence.length) fragment.append(emptyState("证据会在执行中逐条保存", "这里显示去重后的真实证据，包括来源、内容与定位信息。", "▤"));
    if (sources.length) {
      const details = node("details", "source-details");
      details.open = sourcesOpen;
      details.append(node("summary", "", `本次任务记录的来源元数据 · ${sources.length}`));
      for (const source of sources) {
        const item = node("div", "source-item");
        item.append(node("strong", "", `${source.name} · ${source.format}`), node("p", "", source.summary), node("code", "", source.source_id));
        details.append(item);
      }
      fragment.append(details);
    }
    container.replaceChildren(fragment);
    container.scrollTop = scroll;
  }

  /** 按执行尝试和调用 ID 配对，避免恢复回放时误用其他尝试的产物。 */
  function callPair(event) {
    if (!event.call_id) return { request: null, response: null };
    const peers = safeArray(state.run?.events).filter((candidate) =>
      candidate.call_id === event.call_id && candidate.attempt_id === event.attempt_id
      && candidate.attempt === event.attempt);
    return {
      request: peers.find((candidate) => /\.call\.started$/.test(candidate.event_type)) || null,
      response: [...peers].reverse().find((candidate) => /\.call\.(completed|failed|cancelled)$/.test(candidate.event_type)) || null,
    };
  }

  async function artifact(event, runId) {
    const key = `${runId}/${event.attempt_id}/${event.data_ref}`;
    if (state.artifactCache.has(key)) return state.artifactCache.get(key);
    const path = `/api/runs/${encodeURIComponent(runId)}/artifacts/${encodeURIComponent(event.attempt_id)}/${encodeURIComponent(event.data_ref)}`;
    // 同时缓存尚未完成的读取；缓存容量有限，且仅属于当前选中的运行。
    const pending = api(path);
    if (state.selectedRunId === runId) {
      state.artifactCache.set(key, pending);
      while (state.artifactCache.size > 64) state.artifactCache.delete(state.artifactCache.keys().next().value);
    }
    try { return await pending; }
    catch (error) {
      if (state.artifactCache.get(key) === pending) state.artifactCache.delete(key);
      throw error;
    }
  }

  async function renderInspector() {
    const container = $("panel-details");
    const selectFocused = document.activeElement?.id === "inspector-event";
    const recordOpen = Boolean(container.querySelector(".inspector-record[open]"));
    const events = safeArray(state.run?.events);
    const version = ++state.inspectorVersion;
    const runId = state.selectedRunId;
    if (!events.length) {
      container.replaceChildren(emptyState("展开一次真实调用", "开始运行后，点击时间线事件查看原始请求和响应。", "{ }"));
      return;
    }
    const event = events.find((item) => eventKey(item) === state.selectedEventKey) || events.find((item) => item.data_ref) || events[0];
    state.selectedEventKey = eventKey(event);
    for (const button of $("timeline").querySelectorAll("button")) button.classList.toggle("is-selected", button.dataset.eventKey === state.selectedEventKey);
    const label = node("label", "inspector-select-label", "选择审计事件");
    label.htmlFor = "inspector-event";
    const select = node("select", "inspector-select");
    select.id = "inspector-event";
    const attemptIds = [...new Set(events.map((item) => item.attempt_id))];
    for (const record of events) {
      const option = node("option", "", `尝试 ${attemptIds.indexOf(record.attempt_id) + 1} · #${record.seq} ${eventTitle(record)}${replayed(record) ? " [重放]" : ""}`);
      option.value = eventKey(record);
      option.selected = option.value === state.selectedEventKey;
      select.append(option);
    }
    select.addEventListener("change", () => selectEvent(select.value));
    const header = node("div", "inspector-header", eventTitle(event));
    const tags = node("div", "inspector-tags");
    tags.append(node("code", "", event.event_type));
    if (replayed(event)) tags.append(node("span", "replayed-badge", "历史重放 · 未重新派发"));
    const fragment = document.createDocumentFragment();
    fragment.append(label, select, header, tags);
    const pair = callPair(event);
    const failed = event.error ? event : pair.response?.error ? pair.response : null;
    if (failed) fragment.append(renderFailureDiagnostic({
      phase: failed.detail?.phase || failed.event_type.replace(/\.call\..*$/, ""),
      call_id: failed.call_id, error: failed.error,
    }));
    const sections = [];
    const addSection = (title, record, empty) => {
      const section = node("section", "inspector-section");
      const heading = node("div", "inspector-section-heading");
      heading.append(node("span", "", title), node("small", "", record?.data_ref || ""));
      const body = node("div");
      body.append(node("p", "artifact-empty", record?.data_ref ? "正在读取已保存的数据…" : empty));
      section.append(heading, body);
      fragment.append(section);
      if (record?.data_ref && record.attempt_id) sections.push({ record, body });
      else if (record?.data_ref) body.replaceChildren(node("p", "artifact-empty", "该记录没有执行尝试标识，无法定位原始数据。"));
    };
    if (event.call_id) {
      addSection("原始请求", pair.request, "本次尝试尚未记录对应请求。");
      addSection("原始响应", pair.response, "尚未记录响应；调用可能仍在进行，或执行已中断。");
    } else addSection("事件关联数据", event, "此事件仅记录状态，没有关联的数据文件。");
    const details = node("details", "inspector-record");
    details.open = recordOpen;
    details.append(node("summary", "", "完整事件记录"), jsonBlock(event));
    fragment.append(details);
    const scroll = container.scrollTop;
    container.replaceChildren(fragment);
    container.scrollTop = scroll;
    if (selectFocused) select.focus({ preventScroll: true });
    await Promise.allSettled(sections.map(async ({ record, body }) => {
      try {
        const value = await artifact(record, runId);
        if (version === state.inspectorVersion && runId === state.selectedRunId) body.replaceChildren(jsonBlock(value));
      } catch (error) {
        if (version !== state.inspectorVersion || runId !== state.selectedRunId) return;
        const message = record.data_ref ? "无法读取该事件的已保存数据。" : errorMessage(error);
        const retry = node("button", "text-button", "重试读取 ↻");
        retry.type = "button";
        retry.addEventListener("click", () => renderInspector());
        body.replaceChildren(node("p", "artifact-empty", message), retry);
      }
    }));
  }

  async function createRun(event) {
    event.preventDefault();
    if (state.mutation) return;
    const question = $("question").value.trim();
    const profile = selectedProfile();
    if (!question || !profile?.ready) return;
    const selectionVersion = state.selectionVersion;
    state.mutation = true;
    renderControls();
    hideNotice();
    try {
      const summary = await api("/api/runs", { method: "POST", body: JSON.stringify({ question, profile_id: profile.profile_id, mode: $("mode").value }) });
      announce("运行已创建，正在读取执行过程。");
      if (selectionVersion === state.selectionVersion) {
        setTab("answer");
        await openRun(summary.run_id);
      }
      await refreshHistory(true);
    } catch (error) { showNotice(errorMessage(error), true); }
    finally { state.mutation = false; renderControls(); schedulePoll(); }
  }

  async function mutateRun(action) {
    const runId = state.selectedRunId;
    if (!runId || state.mutation) return;
    state.mutation = true;
    renderControls();
    hideNotice();
    try {
      await api(`/api/runs/${encodeURIComponent(runId)}/${action}`, { method: "POST" });
      announce(action === "resume" ? "恢复请求已提交。" : "停止请求已提交，正在等待执行清理。");
      if (action === "cancel") showNotice("停止请求已提交。手动停止可能留下未知调用结果，能否恢复将由内核检查。");
    } catch (error) { showNotice(errorMessage(error), true); }
    finally {
      if (state.selectedRunId === runId) await refreshRun(true);
      await refreshHistory(true);
      state.mutation = false;
      renderControls();
      schedulePoll();
    }
  }

  /** 用轮询代次和进行中标记约束重调度，避免轮询请求重叠。 */
  function schedulePoll() {
    window.clearTimeout(state.pollTimer);
    const generation = ++state.pollGeneration;
    if (state.stopped) return;
    const delay = document.hidden ? 8000 : isActive(state.run) ? 800 : 4500;
    state.pollTimer = window.setTimeout(async () => {
      if (state.stopped || generation !== state.pollGeneration) return;
      if (state.pollInFlight) { schedulePoll(); return; }
      state.pollInFlight = true;
      try {
        if (!state.mutation) {
          await refreshRun(true);
          await refreshHistory(true);
        }
      } finally {
        state.pollInFlight = false;
        if (generation === state.pollGeneration) schedulePoll();
      }
    }, delay);
  }

  /** 直接浏览已注册来源的元数据，无需执行检索工具。 */
  async function refreshSources() {
    const profileId = state.sourcesProfileId;
    const version = ++state.sourceVersion;
    state.sources = [];
    state.selectedSourceId = null;
    state.sourcesError = null;
    state.sourceProfile = state.profiles.find((profile) => profile.profile_id === profileId) || null;
    state.sourcesLoading = Boolean(profileId);
    renderSources();
    if (!profileId) return;
    try {
      const result = await api(`/api/profiles/${encodeURIComponent(profileId)}/sources`);
      if (version !== state.sourceVersion || profileId !== state.sourcesProfileId) return;
      state.sources = safeArray(result.sources);
      state.sourceProfile = result.profile;
      state.sourcesLoadedProfileId = profileId;
    } catch (error) {
      if (version !== state.sourceVersion || profileId !== state.sourcesProfileId) return;
      state.sourcesError = errorMessage(error);
      state.sourcesLoadedProfileId = null;
    } finally {
      if (version === state.sourceVersion && profileId === state.sourcesProfileId) {
        state.sourcesLoading = false;
        renderSources();
      }
    }
  }

  function renderSources() {
    const profile = state.sourceProfile || state.profiles.find((item) => item.profile_id === state.sourcesProfileId);
    $("refresh-sources").disabled = state.sourcesLoading || !state.sourcesProfileId;
    $("source-profile-note").textContent = profile
      ? `${profile.dataset_name || profile.dataset_id} · 项目注册目录${profile.ready ? "" : " · 执行配置尚未就绪"}` : "";
    const query = $("sources-search").value.trim().toLocaleLowerCase();
    const sources = state.sources.filter((source) => `${source.name} ${source.source_id} ${source.format} ${source.summary} ${JSON.stringify(source.metadata || {})}`.toLocaleLowerCase().includes(query));
    $("sources-count").textContent = state.sourcesLoading ? "正在读取…" : `${sources.length} / ${state.sources.length} 个来源`;
    let message = null;
    if (state.sourcesLoading) message = ["正在读取目录", "从所选执行配置读取已注册的来源元数据。"];
    else if (state.sourcesError) message = ["暂时无法读取目录", state.sourcesError];
    else if (!profile) message = ["尚未配置执行模块", "请在服务端配置执行模块后重新启动，再浏览已注册的数据来源。"];
    else if (!sources.length) message = state.sources.length
      ? ["没有匹配的数据来源", "调整关键词，尝试搜索其他名称、格式或来源 ID。"]
      : ["此配置尚无公开来源目录", "目录由服务端显式注册。接入数据模块并注册来源后，元数据会显示在这里。"];
    if (message) {
      $("sources-list").replaceChildren(emptyState(...message, "▤"));
      $("source-detail").replaceChildren(emptyState("来源详情", "选择一个来源，查看摘要、版本和目录元数据。", "◇"));
      return;
    }
    if (!sources.some((source) => source.source_id === state.selectedSourceId)) state.selectedSourceId = sources[0].source_id;
    const focusedId = $("sources-list").contains(document.activeElement) ? document.activeElement?.dataset.sourceId : null;
    const fragment = document.createDocumentFragment();
    for (const source of sources) {
      const selected = source.source_id === state.selectedSourceId;
      const button = node("button", `source-card${selected ? " is-selected" : ""}`);
      button.type = "button";
      button.dataset.sourceId = source.source_id;
      button.setAttribute("aria-pressed", String(selected));
      const top = node("span", "source-card-top");
      top.append(node("span", "source-format", source.format), node("span", "source-card-arrow", "↗"));
      button.append(top, node("strong", "", source.name), node("span", "source-card-summary", source.summary || "目录未提供摘要。"), node("code", "", source.source_id));
      button.addEventListener("click", () => { state.selectedSourceId = source.source_id; renderSources(); });
      fragment.append(button);
    }
    $("sources-list").replaceChildren(fragment);
    if (focusedId) Array.from($("sources-list").querySelectorAll("button")).find((button) => button.dataset.sourceId === focusedId)?.focus({ preventScroll: true });
    const source = sources.find((item) => item.source_id === state.selectedSourceId);
    const detail = document.createDocumentFragment();
    const heading = node("div", "source-detail-heading");
    heading.append(node("span", "eyebrow", "SOURCE METADATA"), node("h2", "", source.name), node("span", "source-format", source.format));
    detail.append(heading, node("p", "source-summary", source.summary || "目录未提供摘要。"));
    const metadata = node("dl", "detail-metadata");
    for (const [title, value] of [["来源 ID", source.source_id], ["版本", source.version], ["内容哈希", source.content_hash]]) {
      metadata.append(node("dt", "", title), node("dd", title === "来源 ID" || title === "内容哈希" ? "mono" : "", value ?? "未提供"));
    }
    detail.append(metadata, node("h3", "source-metadata-title", "已注册的元数据"), jsonBlock(source.metadata || {}), node("p", "detail-footnote", "此处只展示目录描述，不读取原文件。完成提问后，在「证据」中查看实际读取片段及 locator 定位。"));
    $("source-detail").replaceChildren(detail);
  }

  async function refreshProfiles() {
    $("refresh-profiles").disabled = true;
    try {
      const response = await api("/api/profiles");
      state.profiles = safeArray(response.profiles);
      renderProfiles();
      setConnection(true);
    } catch (error) {
      showNotice(errorMessage(error), true);
      if (!state.profiles.length) {
        $("profile-summary").replaceChildren(emptyState("暂时无法读取执行配置", "检查本地服务后，点击「刷新配置」重试。"));
        $("profile-readiness").textContent = "执行配置读取失败，暂时无法创建任务。";
      }
    } finally { $("refresh-profiles").disabled = false; }
  }

  /** 评估报告是单独导入的汇总，不根据执行轨迹虚构评分。 */
  async function refreshEvaluations() {
    const version = ++state.evaluationsVersion;
    state.evaluationsLoading = true;
    state.evaluationListError = null;
    renderEvaluationSelector();
    if (!state.evaluation) renderEvaluation();
    try {
      const result = await api("/api/evaluations");
      if (version !== state.evaluationsVersion) return;
      state.evaluations = safeArray(result.evaluations);
      const chosen = state.evaluations.find((report) => report.evaluation_id === state.evaluationId) || state.evaluations[0];
      state.evaluationsLoading = false;
      renderEvaluationSelector();
      if (!chosen) {
        state.evaluationId = null;
        state.evaluation = null;
        state.evaluationVersion += 1;
        state.evaluationLoading = false;
        state.evaluationError = null;
        renderEvaluation();
      } else if (state.evaluation?.evaluation_id !== chosen.evaluation_id) await selectEvaluation(chosen.evaluation_id);
      else renderEvaluation();
    } catch (error) {
      if (version !== state.evaluationsVersion) return;
      state.evaluationsLoading = false;
      state.evaluationListError = errorMessage(error);
      renderEvaluationSelector();
      renderEvaluation();
    }
  }

  function renderEvaluationSelector() {
    const select = $("evaluation-select");
    const fragment = document.createDocumentFragment();
    for (const report of state.evaluations) {
      const option = node("option", "", `${report.title} · ${dateLabel(report.evaluated_at)}`);
      option.value = report.evaluation_id;
      fragment.append(option);
    }
    if (!state.evaluations.length) {
      const option = node("option", "", state.evaluationsLoading ? "正在读取报告…" : state.evaluationListError ? "报告读取失败" : "暂无报告");
      option.value = "";
      fragment.append(option);
    }
    select.replaceChildren(fragment);
    select.disabled = !state.evaluations.length;
    if (state.evaluations.some((report) => report.evaluation_id === state.evaluationId)) select.value = state.evaluationId;
    $("refresh-evaluations").disabled = state.evaluationsLoading;
  }

  async function selectEvaluation(id, userInitiated = false) {
    if (!id) return;
    if (userInitiated) state.evaluationUserVersion += 1;
    const version = ++state.evaluationVersion;
    state.evaluationId = id;
    state.evaluation = null;
    state.evaluationError = null;
    state.evaluationLoading = true;
    $("evaluation-select").value = id;
    renderEvaluation();
    try {
      const report = await api(`/api/evaluations/${encodeURIComponent(id)}`);
      if (version !== state.evaluationVersion || id !== state.evaluationId) return;
      state.evaluation = report;
    } catch (error) {
      if (version !== state.evaluationVersion || id !== state.evaluationId) return;
      state.evaluationError = errorMessage(error);
    } finally {
      if (version === state.evaluationVersion && id === state.evaluationId) {
        state.evaluationLoading = false;
        renderEvaluation();
      }
    }
  }

  function fullDateLabel(value) {
    if (!value) return "未提供";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat("zh-CN", { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZoneName: "short" }).format(date);
  }

  function metricLabel(value, type) {
    if (value === null || value === undefined || typeof value !== "number" || !Number.isFinite(value)) return "未提供";
    if (type === "percent") return `${(value * 100).toLocaleString("zh-CN", { maximumFractionDigits: 2 })}%`;
    if (type === "milliseconds") return `${value.toLocaleString("zh-CN", { maximumFractionDigits: 2 })} ms`;
    if (type === "currency") return `$${value.toLocaleString("en-US", { maximumFractionDigits: 6 })}`;
    return value.toLocaleString("zh-CN", { maximumFractionDigits: 3 });
  }

  function renderEvaluation() {
    const container = $("evaluation-content");
    const report = state.evaluation;
    $("export-evaluation").hidden = !report || state.evaluationLoading;
    if (!report) {
      let title = "还没有评估报告";
      let description = "下载 JSON 模板，填入实际评估结果后导入。这里不会自动生成或预填实验分数。";
      if (state.evaluationLoading || state.evaluationsLoading) { title = "正在读取评估报告"; description = "从本地服务读取已导入的评估汇总。"; }
      else if (state.evaluationError || state.evaluationListError) { title = "暂时无法读取评估报告"; description = state.evaluationError || state.evaluationListError; }
      const empty = emptyState(title, description, "▥");
      empty.classList.add("evaluation-empty", "panel");
      container.replaceChildren(empty);
      return;
    }
    $("export-evaluation").href = `/api/evaluations/${encodeURIComponent(report.evaluation_id)}`;
    $("export-evaluation").download = `${report.evaluation_id}.json`;
    const fragment = document.createDocumentFragment();
    if (state.evaluationListError) fragment.append(node("p", "comparison-warning", `报告列表读取失败：${state.evaluationListError} 当前仍显示上次读取的报告。`));
    const overview = node("section", "evaluation-overview panel");
    const heading = node("div", "evaluation-report-heading");
    const titles = node("div");
    titles.append(node("p", "eyebrow", "IMPORTED EVALUATION"), node("h2", "", report.title));
    heading.append(titles, node("span", "imported-badge", "用户导入"));
    const metadata = node("dl", "evaluation-metadata");
    for (const [label, value] of [["数据集", report.dataset_id], ["任务集", report.task_set_id], ["模型", report.model_id], ["评估者", report.evaluator], ["评估时间", fullDateLabel(report.evaluated_at)], ["导入时间", fullDateLabel(report.imported_at)]]) {
      const item = node("div");
      item.append(node("dt", "", label), node("dd", "", value ?? "未提供"));
      metadata.append(item);
    }
    overview.append(heading, metadata);
    if (report.notes) overview.append(node("p", "evaluation-notes", report.notes));
    fragment.append(overview);
    const results = safeArray(report.results);
    if (new Set(results.map((row) => row.task_count)).size > 1) {
      const warning = node("div", "comparison-warning");
      warning.setAttribute("role", "note");
      warning.append(node("strong", "", "各模式样本数不同，不能直接横向比较。"), document.createTextNode(" 请先核对任务集、样本选择和评估方式，再解释指标差异。"));
      fragment.append(warning);
    }
    const section = node("section", "comparison-panel panel");
    const sectionHeading = node("div", "panel-heading");
    sectionHeading.append(node("h3", "", "模式对比"), node("span", "muted-caption", `${results.length} 个已报告模式`));
    const tableWrap = node("div", "comparison-scroll");
    tableWrap.tabIndex = 0;
    tableWrap.setAttribute("role", "region");
    tableWrap.setAttribute("aria-label", "评估指标对比，可横向滚动");
    const table = node("table", "comparison-table");
    table.append(node("caption", "visually-hidden", "用户导入报告的模式指标对比。未提供表示原报告未包含该指标。"));
    const thead = node("thead");
    const header = node("tr");
    const metricHeader = node("th", "", "指标");
    metricHeader.scope = "col";
    header.append(metricHeader);
    for (const row of results) {
      const th = node("th", "", EVALUATION_MODE_TITLES[row.mode] || row.mode);
      th.scope = "col";
      header.append(th);
    }
    thead.append(header);
    const tbody = node("tbody");
    const sample = node("tr", "sample-count-row");
    const sampleTitle = node("th", "", "任务样本数");
    sampleTitle.scope = "row";
    sample.append(sampleTitle);
    for (const row of results) sample.append(node("td", "", Number(row.task_count).toLocaleString("zh-CN")));
    tbody.append(sample);
    for (const [key, label, type] of METRICS) {
      const tr = node("tr");
      const th = node("th", "", label);
      th.scope = "row";
      tr.append(th);
      for (const row of results) {
        const text = metricLabel(row.metrics?.[key], type);
        tr.append(node("td", text === "未提供" ? "metric-missing" : "", text));
      }
      tbody.append(tr);
    }
    table.append(thead, tbody);
    tableWrap.append(table);
    section.append(sectionHeading, tableWrap, node("p", "comparison-footnote", "指标按导入报告原值展示；未提供的指标保留为空值，不按 0 计算。导入不代表结果已经独立复核。任务执行记录中的调用次数与耗时保留在智能问答中。"));
    fragment.append(section);
    container.replaceChildren(fragment);
  }

  /** 读取大小受限的文件并校验 JSON 语法，保留原始数值表示。 */
  async function importEvaluation() {
    const file = $("evaluation-file").files?.[0];
    if (!file || state.evaluationImporting) return;
    state.evaluationImporting = true;
    const selectionVersion = state.evaluationUserVersion;
    $("choose-evaluation-file").disabled = true;
    const status = $("evaluation-import-status");
    status.hidden = false;
    status.classList.remove("is-error");
    status.textContent = `正在读取 ${file.name}…`;
    try {
      if (file.size > MAX_EVALUATION_BYTES) throw new Error("文件超过 128 KiB 上限，请导入评估汇总，而非逐条运行记录。");
      const text = (await file.text()).replace(/^\uFEFF/, "");
      try { JSON.parse(text); }
      catch (error) { throw new Error(`文件不是有效 JSON：${String(error.message).slice(0, 160)}`); }
      status.textContent = "正在校验并保存评估报告…";
      // 发送原始文本：JSON.stringify 会将 1e999 静默替换为 null。
      const report = await api("/api/evaluations", { method: "POST", body: text });
      state.evaluationsVersion += 1; // 防止较早的列表响应移除刚导入的报告。
      state.evaluationsLoading = false;
      state.evaluationListError = null;
      state.evaluations = [report, ...state.evaluations.filter((item) => item.evaluation_id !== report.evaluation_id)].slice(0, 100);
      if (selectionVersion === state.evaluationUserVersion) {
        state.evaluationVersion += 1;
        state.evaluationId = report.evaluation_id;
        state.evaluation = report;
        state.evaluationLoading = false;
        state.evaluationError = null;
      }
      renderEvaluationSelector();
      renderEvaluation();
      status.textContent = `已导入「${report.title}」。结果来自用户提供的评估汇总。`;
      announce("评估报告已导入。");
    } catch (error) {
      status.classList.add("is-error");
      status.textContent = errorMessage(error);
    } finally {
      state.evaluationImporting = false;
      $("choose-evaluation-file").disabled = false;
      $("evaluation-file").value = "";
    }
  }

  async function initialize() {
    $("run-form").addEventListener("submit", createRun);
    $("question").addEventListener("input", updateQuestionCount);
    $("profile").addEventListener("change", () => selectProfile($("profile").value));
    $("refresh-profiles").addEventListener("click", refreshProfiles);
    $("reset-question").addEventListener("click", () => { selectProfile(state.profileId); $("question").focus(); });
    $("cancel-run").addEventListener("click", () => mutateRun("cancel"));
    $("resume-run").addEventListener("click", () => mutateRun("resume"));
    $("delete-run").addEventListener("click", () => deleteRun(state.selectedRunId));
    $("dismiss-notice").addEventListener("click", hideNotice);
    $("history-search").addEventListener("input", renderHistory);
    $("history-filter").addEventListener("change", renderHistory);
    $("refresh-history").addEventListener("click", () => refreshHistory());
    $("refresh-sidebar").addEventListener("click", () => refreshHistory());
    $("timeline-task").addEventListener("change", () => openRun($("timeline-task").value));
    $("timeline-phase").addEventListener("change", renderTimeline);
    $("timeline-search").addEventListener("input", renderTimeline);
    $("sources-profile").addEventListener("change", () => { state.sourcesProfileId = $("sources-profile").value; refreshSources(); });
    $("sources-search").addEventListener("input", renderSources);
    $("refresh-sources").addEventListener("click", refreshSources);
    $("refresh-evaluations").addEventListener("click", refreshEvaluations);
    $("evaluation-select").addEventListener("change", () => selectEvaluation($("evaluation-select").value, true));
    $("choose-evaluation-file").addEventListener("click", () => $("evaluation-file").click());
    $("evaluation-file").addEventListener("change", importEvaluation);
    $("audit-details").addEventListener("toggle", () => {
      if ($("audit-details").open) { renderTimeline(); renderInspector(); }
      else state.inspectorVersion += 1;
    });
    document.querySelectorAll("[data-view]").forEach((button) => button.addEventListener("click", () => setView(button.dataset.view, true)));
    document.querySelectorAll("[data-tab]").forEach((button) => {
      button.addEventListener("click", () => setTab(button.dataset.tab));
      button.addEventListener("keydown", (event) => {
        const tabs = ["answer", "evidence"];
        let index = tabs.indexOf(button.dataset.tab);
        if (event.key === "ArrowRight") index = (index + 1) % tabs.length;
        else if (event.key === "ArrowLeft") index = (index + tabs.length - 1) % tabs.length;
        else if (event.key === "Home") index = 0;
        else if (event.key === "End") index = tabs.length - 1;
        else return;
        event.preventDefault();
        setTab(tabs[index], true);
      });
    });
    $("panel-evidence").replaceChildren(emptyState("证据会在执行中逐条保存", "查看本次任务实际读取的内容片段、来源与定位信息。", "▤"));
    $("panel-details").replaceChildren(emptyState("展开一次真实调用", "提问后，选择时间线事件查看原始请求和响应。", "{ }"));
    renderSources();
    renderEvaluation();
    const requestedView = window.location.hash.slice(1);
    setView(Object.hasOwn(VIEW_TITLES, requestedView) ? requestedView : "workbench");
    window.addEventListener("hashchange", () => setView(window.location.hash.slice(1)));
    document.querySelector(".brand").addEventListener("click", (event) => { event.preventDefault(); setView("workbench"); });
    const results = await Promise.allSettled([api("/api/info"), refreshProfiles(), refreshHistory()]);
    if (results[0].status === "fulfilled") {
      state.info = results[0].value;
      renderInfo();
    } else {
      $("capabilities").replaceChildren(emptyState("暂时无法读取服务能力", "检查本地服务后刷新页面。"));
      showNotice(errorMessage(results[0].reason), true);
    }
    let previousRun;
    try { previousRun = window.localStorage.getItem("agent-lab:last-run"); } catch { /* 此处仅保存可选偏好设置。 */ }
    if (!state.mutation && !state.selectedRunId && previousRun && state.runs.some((run) => run.run_id === previousRun)) await openRun(previousRun, false);
    schedulePoll();
    document.addEventListener("visibilitychange", () => { if (!document.hidden) schedulePoll(); });
    window.addEventListener("pagehide", () => { state.stopped = true; state.pollGeneration += 1; window.clearTimeout(state.pollTimer); });
    window.addEventListener("pageshow", () => { if (state.stopped) { state.stopped = false; schedulePoll(); } });
  }

  initialize().catch((error) => { showNotice(errorMessage(error), true); setConnection(false); });
})();
