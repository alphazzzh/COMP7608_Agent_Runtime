# 持续任务：有界周期、交接与恢复

持续任务在原有有界 Agent run 外保存长期目标、触发条件、累计额度和交接摘要。它适合“每次检查同一问题，直到完成、达到额度或截止时间”的工作。每一轮仍由原控制器、已注册 profile 和原 DataPort/ContentPort 执行，不增加工具权限，也不把一次 `completed` 当作目标已经外部独立验证。

本功能面向同一 POSIX 主机、同一个可信本地工作区。服务需要保持运行才能调度；它不是系统级定时器、分布式任务队列或外部副作用 exactly-once 保证。配置与启动方式见[配置说明](configuration.md)，页面入口见[工作台说明](project-workspace.md)。

## 四种身份

| 身份 | 负责什么 | 恢复时如何处理 |
|---|---|---|
| Mission / `mission_id` | 保存目标、profile 身份、触发方式、累计限制和当前交接 | 保留同一目标及原截止时间，不重新计算剩余额度 |
| Trigger / `trigger_id` | 表示一次已接受的触发事件；同一 Mission 的 `event_id` 用于去重 | 入队时就永久绑定一个 `run_id`，不能因重启换号重做 |
| run / `run_id` | 某一轮的完整内核运行，具有冻结的 `RunRequest`、调用日志和结果 | 已有核心状态时走严格恢复；已有终态时只对账结算 |
| attempt / `attempt_id` | 同一个 run 的首次执行或恢复尝试，对应各自追踪记录 | 恢复增加 attempt，不增加新的 Trigger，也不重复预留 Mission 额度 |

同一 Mission 最多有一个 `claimed/running` 周期，并可另保留一个 `pending` 事件。服务的 `max_active_runs` 仍限制所有普通运行和 Mission 运行的总并发。202 或 201 只表示请求已接受，不表示工具已经运行或目标已经完成。

## 在页面创建与管理

1. 打开“持续任务”，选择已就绪的执行配置，填写标题和目标。未就绪配置不能提交。
2. 选择“手动唤醒”或“完成后按周期唤醒”。页面创建时始终安排首次运行；手动模式表示后续需要手动唤醒。
3. 设置最大轮数、累计模型/数据调用和持续时长。页面不接收密钥、工厂、任意路径或完整 `RunConfig`。
4. 默认勾选“首个成功完成后结束任务”。需要后续周期时取消勾选；原 profile 的核验政策保持不变。
5. 在详情中查看状态原因、已用与预留额度、下次唤醒、原截止时间、交接和最近周期。点击“查看运行”进入已有答案、证据和审计页面。

页面只在持续任务视图可见时轮询，不要求浏览器一直打开。真正的调度由服务端负责。离开页面或切换任务不会取消服务端已接受的事件；迟到的页面读取也不能覆盖新的选择。

## 累计硬限制与完成条件

所有限制都是正整数；严格输入拒绝布尔值、字符串、浮点数、额外字段和非有限 JSON 数值。创建时省略 `limits` 或其中字段，使用下面的默认值。

| 字段 | 默认值 | 硬上限 | 实际口径 |
|---|---:|---:|---|
| `limits.max_runs` | 10 | 100 | 累计周期上限；接受的 Trigger 身份总数也受此上限约束，取消待办不会允许无限补发新身份 |
| `limits.max_model_calls` | 120 | 10000 | 核心累计模型请求数；规划与内容生成/核验遵守原模型调用契约 |
| `limits.max_data_calls` | 120 | 10000 | 核心 `call_counts.catalog + call_counts.tools`，包含目录分页、工具调用及原重试口径 |
| `limits.max_duration_seconds` | 86400 | 604800 | 创建时生成 `deadline_at = created_at + max_duration_seconds`，暂停、停机、恢复都不延长 |

领取周期时，在同一个事务中检查状态、原截止时间、次数和所有权，再预留 profile 单轮额度与 Mission 剩余额度中的较小值。新 run 的墙钟上限也不超过 profile 单轮上限及 Mission 剩余时间。单轮其他策略不变，不能用 Mission API 提高 profile 权限或预算。

`used_*` 表示已按真实核心终态结算的调用；`reserved_*` 表示已领取但尚未确定结算的额度。不能把预留当作免费剩余，也不能因网页未显示结果而退款。终态核对只结算一次，多余预留才会释放。`started_runs` 是已领取轮数，`completed_runs` 是已结算轮数；后者不等于成功答案数量，应同时查看 Trigger 的 `run_status/stop_reason`。

到达轮数或原截止时间会结束后续调度；额度耗尽或恢复安全检查失败会阻塞后续派发，原因保存在 `reason`。运行以 `failed`、`timeout` 或 `budget_exceeded` 结束时，active Mission 变为 `blocked`，不自动进入下一轮。`insufficient_evidence` 不会被伪装成成功，但在剩余额度和时间允许时，可以根据手动/周期设置继续后续轮次。人工已经暂停或结束的状态不被结算重新激活。

`stop_on_completed=true` 时，内核首个 `completed` 会结束后续调度；`false` 时可继续后续周期，但不会绕过其他限制。完成、内部核验通过与外部评估正确仍是不同结论。

本功能沿用单轮运行的实测 token 累计止损；配置 `max_input_tokens`/`max_output_tokens` 时，provider 必须回报实际用量，缺测会 fail closed。它不提供美元硬预算，也不把调用次数换算成费用；未测量的 token、外部数据流量和成本保持 `null`。适配器仍须关闭 SDK 隐式重试、遵守原调用预算与取消协议；控制器不能强制终止阻塞线程或忽略取消的外部代码。

## 手动事件与周期语义

- `interval_seconds=null` 为手动模式；正整数为固定延迟周期，范围 `1..604800` 秒。
- `start_immediately=true` 为默认值，创建 Mission 与首次 Trigger 在同一事务中保存。设为 false 时，手动任务等待 wake；周期任务等待第一个间隔。
- 周期是 fixed-delay：上一轮结算后，以下一次 `结算时间 + interval_seconds` 安排。不是固定日历时间，也不并行追赶逾期轮次。
- 服务停机错过多个间隔，恢复后最多为到期目标合并一个待办，不追补每个错过的间隔。
- 手动 wake 的 `event_id` 在同一 Mission 内去重。请求超时或连接断开后，重试必须使用原 `event_id`；返回的是同一 `trigger_id/run_id`，即使该事件后来已完成或 Mission 已结束。真正的新事件才生成新 ID。
- 界面仅在 `active` 时提供唤醒按钮。API 也可为 `paused` 任务保存一个待办，但恢复前不会领取。已有 pending 或已达到可接受身份上限时，新事件返回 429，不静默丢弃，也不换身份无限排队。

`event_id` 是调用者保存的业务事件身份，长度为 1–200 字符，不能是纯空白。服务不替调用者把两个不同事件 ID 判断成同一件事。浏览器对一次失败 wake 的“重试唤醒”会复用事件 ID；自行写脚本时也应保存并重用它。

## 暂停、结束、取消与所有权

| 操作 | 行为 | 已领取周期 |
|---|---|---|
| `pause` | 设为 `paused`，停止新领取，保留待办与原额度/截止时间 | 允许安全收尾，不强杀外部调用 |
| `resume` | 恢复人工暂停的 Mission；不允许通过普通恢复解开 `blocked` 或重开 `completed` | 使用原身份与安全检查，不刷新预算；周期任务恢复后可立即变为到期 |
| `complete` | 明确结束后续调度，并取消 pending 事件 | 同样允许安全收尾，不删除审计 |
| 取消某个 Mission run | `/api/runs/{run_id}/cancel` 只接受当前仍活动的周期；接纳取消后暂停仍 active 的父 Mission，再开放调度 | 可能留下结果未知的调用，不能保证可恢复 |

需要停止当前实际执行时，在对应 run 页面使用“停止运行”；只暂停 Mission 不会终止这一轮。若启用了 `stop_on_completed` 且当前周期正常成功，暂停中的 Mission 也会结束并取消待办。取消历史或已在准入期间正常结束的 run 返回 `not_running`，父任务状态不变。已接纳的取消会完成暂停和清理，即使发起它的请求断开。取消后先检查父任务和调用日志，再决定是否恢复；若内核判定调用结果未知，恢复会阻塞并保留预留，不会自动重做该外部调用。

Mission 所属 run 的普通详情、证据、工件及 JSON/HTML 导出仍可查看。普通 `POST /api/runs/{run_id}/resume` 和 `DELETE /api/runs/{run_id}` 对这些记录返回 `mission_owned`；即使父 Mission 已结束，也保留这些运行用于对账和审计。当前没有 Mission 删除 API。不要通过手动删 SQLite 行或运行目录来“解锁”任务。

## 重启对账、存储与故障

工作区中的 `missions.sqlite3` 保存目标、Trigger、冻结请求和预留，与原 run 清单、核心 SQLite 调用日志及工件共同构成恢复依据。保留或备份时应保持同一 `server.data_dir` 的整组记录一致；更换目录相当于打开另一组历史。原工作区独占锁限制同一目录只有一个宿主调度，不提供跨机器租约抢占。

服务启动后会先对 `claimed/running` 身份对账，再领取新事件：

1. 核心已有终态：只读取累计调用、生成有界交接并一次性结算，不再次调用适配器；缺少 Web 清单时可按已核验的持久身份重建展示记录。
2. 核心存在非终态：只按原 `run_id`、冻结请求和原恢复协议继续；已知成功响应重放，预算和期限不重置。
3. 核心尚无状态：仅在持久身份和尝试记录能够证明尚未执行时，允许同一 `run_id` 首次提交；不能因为缺文件就换号。
4. 调用 started 但结果未知、核心状态缺失、请求不匹配、profile/源码/配置变化或原期限已到：保守阻塞，保留无法确认的预留和原身份，不退款、不新建 run 猜测重试。

若 Mission 已结束，调度器仍先对账已知核心终态；没有活动运行且没有可结算终态时，触发器记为 `blocked/mission_run_unresolved`，父任务继续保留原结束原因及未确认的预留。

人工暂停或结束后，不自动恢复未终结周期；仍可对已经存在的核心终态结算。磁盘、schema 或完整性故障不会被当作普通任务失败吞掉后继续派发：调度停止，`GET /api/missions` 的 `scheduler.error` 公开安全错误，操作可返回 `mission_scheduler_failed`。应先保留现场并检查记录，再使用相同目录和兼容配置重启。

正式入口默认允许 `full_agent`。可信宿主若在 `allowed_modes` 中禁用它，`features.missions=false`，不启动后台 Mission 调度，也拒绝创建、resume 和 wake；历史仍可读取、暂停或结束。不能通过持久队列绕过宿主模式政策。

## 交接不是证据

交接由已保存的核心终态和已确认计划确定性生成，不为摘要额外调用模型。字段包括 `summary`、`pending_items`、`previous_run_id`、`artifact_refs`、`truncated`，规范 UTF-8 JSON 总大小最多 4096 字节；必要时裁剪并明确标记截断。工件引用只是定位线索，不是新一轮的可引用证据。

领取时冻结交接，派发前冻结完整 `RunRequest.continuation`。它进入规划器的 `context.continuation`，受原 `max_planner_context_bytes` 约束；恢复同一 run 不读取最新 Mission 进展重新计算。普通 run 未提供 continuation 时保持原序列化形状。

规划提示明确这些是未经本轮验证的历史线索：不改变 question、工具权限、来源授权、核验政策或预算，不导入旧 `EvidenceRecord/evidence_id`，也不能自动成为新答案引用。每轮仍须通过原数据与内容路径产生本轮答案和证据。

## HTTP API 与可复制示例

路径相对于已运行的本机工作台；写入沿用同源限制和安全错误形状 `{"detail":{"code":"...","message":"..."}}`。创建与 wake 的 JSON 请求体在解码前限制为 **32 KiB**，超限返回 413；非法字段、类型或范围返回 422。暂停、恢复和结束无需请求体。API 不接收凭据、factory、文件路径、Python 代码、任意 `RunConfig` 或 mode 覆盖。

| 方法与路径 | 成功响应 |
|---|---|
| `GET /api/missions` | 200，`{missions:[MissionRecord], scheduler:{running,error}}`；按更新时间倒序，最多 100 项 |
| `POST /api/missions` | 201，创建后的 MissionRecord |
| `GET /api/missions/{mission_id}` | 200，MissionRecord 加 `triggers`，最近事件倒序，最多 100 项 |
| `POST /api/missions/{mission_id}/pause` | 200，暂停后的 MissionRecord |
| `POST /api/missions/{mission_id}/resume` | 200，恢复后的 MissionRecord |
| `POST /api/missions/{mission_id}/complete` | 200，结束后的 MissionRecord |
| `POST /api/missions/{mission_id}/wake` | 202，同一事件绑定的 Trigger，含 `trigger_id/run_id` |

Mission 状态为 `active/paused/completed/blocked`，Trigger 状态为 `pending/claimed/running/completed/blocked/cancelled`。Mission 的 `created_at/updated_at/deadline_at/next_wake_at` 和 Trigger 时间使用有限的 **UTC epoch 秒**，不是毫秒或 ISO 字符串；`next_wake_at` 可为 null。Web run 记录沿用原有日期格式，不要混为同一字段规范。公开返回不包含 profile 私有签名和完整冻结请求。

以下命令只供用户在已配置并启动的服务上手动执行，会创建实际任务并可能请求真实模型。先读取 `/api/profiles`，将示例中的 `project-main` 换成已就绪配置；默认配置样例使用此 ID。若修改了端口，同步调整 `MISSION_BASE`。

```bash
MISSION_BASE='http://127.0.0.1:8765'
curl --fail-with-body "$MISSION_BASE/api/profiles"

curl --fail-with-body -X POST "$MISSION_BASE/api/missions" \
  -H 'Content-Type: application/json' \
  --data-binary @- <<'JSON'
{
  "title": "收入变化跟进",
  "question": "检查最新已注册资料中的收入变化，并给出本轮可追溯的依据。",
  "profile_id": "project-main",
  "interval_seconds": 3600,
  "start_immediately": true,
  "stop_on_completed": false,
  "limits": {
    "max_runs": 10,
    "max_model_calls": 120,
    "max_data_calls": 120,
    "max_duration_seconds": 86400
  }
}
JSON
```

保存返回的 `mission_id` 后，可以查询或管理目标。下面两个变量需填入你保存的身份；同一次业务事件重试时，不要改变 `MISSION_EVENT_ID`。

```bash
MISSION_ID='填入创建响应的mission_id'
MISSION_EVENT_ID='review-request-2026-11-17-001'

curl --fail-with-body "$MISSION_BASE/api/missions/$MISSION_ID"
curl --fail-with-body -X POST "$MISSION_BASE/api/missions/$MISSION_ID/pause"
curl --fail-with-body -X POST "$MISSION_BASE/api/missions/$MISSION_ID/resume"
curl --fail-with-body -X POST "$MISSION_BASE/api/missions/$MISSION_ID/wake" \
  -H 'Content-Type: application/json' \
  --data-binary "{\"event_id\":\"$MISSION_EVENT_ID\"}"
```

需要纯手动、不立即运行的 API 任务时，将创建示例改为 `interval_seconds:null`、`start_immediately:false`。创建 JSON 中 `title` 为 1–200 字符、`question` 为 1–2000 字符、`profile_id` 为 1–100 字符；去除首尾空白后不得为空。省略 profile_id 时遵循原单配置自动选择政策，多配置需明确选择。

## 离线验收与人工检查

以下离线命令从 `runtime/` 执行，依赖须已安装或在本地 uv 缓存。它们使用临时真实 SQLite、内存 ASGI、受控端口与 Node DOM/HTTP fixture，不监听端口、不访问真实模型，不需要私有配置。

```bash
uv run --offline --extra dev --extra web --frozen pytest -q tests/missions
uv run --offline --extra dev --extra web --frozen pytest -q tests/engine/test_continuation.py tests/web/test_mission_runs.py
uv run --offline --extra dev --extra web --frozen pytest -q tests/web tests/recovery
node --check src/lake_agent/web/static/app.js
```

自动验收覆盖事件去重、领取与预留、一次性结算、周期合并、暂停/结束、截止时间、重启窗口、未知结果保留、运行所有权、continuation 冻结和页面竞态。Node DOM fixture 检查真实脚本的信息流与控件状态，不能代替浏览器布局、原生表单和实际数据质量验收。更多分组命令见[测试导航](../tests/README.md)。

下面是人工检查清单，不由上述测试自动连接真实环境。准备好实际模块、模型凭据和独立测试工作区后，再由用户手动启动[工作台](project-workspace.md#配置与使用)：

1. 检查 `GET /api/info` 的 `features.missions` 和已就绪 profile。在页面创建手动任务，确认首次运行、详情额度和实际 run 链接一致。
2. 创建一个短周期任务，关闭“首个完成后结束”，使用较小轮数与累计额度。确认前一轮结算后才安排下次唤醒，达到限制后不再派发；不要用等待固定轮数来判断答案质量。
3. 运行中暂停 Mission：确认没有新领取，已有周期可以收尾。恢复后检查原截止时间未延长。结束后确认 pending 被取消，而已有审计仍可查看。
4. 用同一个 `event_id` 连续提交两次 wake，确认 `trigger_id/run_id` 相同；已有待办时使用不同事件，确认容量错误不会悄悄创建额外周期。
5. 在 run 页面取消活动周期，确认已接纳的取消使父 Mission 变为 paused。尝试普通 resume/delete 应被拒绝；不要为了继续而直接删除记录或更换 run ID。
6. 使用同一数据目录正常停止并重启服务，确认已结算周期不重发，待办按原身份继续；实际异常中断可能产生 unknown outcome，应保留原记录并检查 blocked 与预留，不能期待强制恢复。
7. 切换 Mission、离开视图、隐藏页面并返回，确认当前选择与按钮状态正确；检查窄屏表单、文字换行、键盘焦点和报告导出。浏览器关闭不会代替服务端暂停。

通过离线契约测试或人工操作检查，不等于真实提供方长期稳定、所有故障窗口都可自动恢复、外部副作用 exactly-once，或答案具有事实正确性。真实模型/数据接通后仍需独立端到端与质量评估。
