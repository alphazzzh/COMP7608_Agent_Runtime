# 异构数据池 Agent Harness：运行控制与异步执行接口契约 v0.5

**更新日期：** 2026-11-04（A/C 契约 v0.5 增量修订字段；B 统一采用 agent-loop-v1）

**状态：** 当前运行时包 `0.7.0` 沿用本核心契约；v0.5 在既有受控工具并行基础上增加可选持久调用状态与严格同版本恢复，当前另提供显式 opt-in 的 `content-feedback-v1` 请求扩展。工作台与服务端执行配置不改变核心方法签名；其接入与HTTP接口见[项目工作台说明](project-workspace.md)。此状态不构成真实数据或模型联调通过声明。

**范围：** 定义运行控制器（任务规划与运行控制）与调用方、数据模块、内容模块、规划器及 trace 的输入输出。Harness 面向通用异构数据池；Lake 数据集只用于评测，在线控制策略不读取其标签，也不预设某个数据集的工具名称或数据格式。Python 包名保持 `lake_agent`。

A/C 异步方法及 JSON `schema_version="1.0"` 保持；ComposeRequest 增加默认空的 revision，AgentComposeDecision 增加默认空的 revision_instructions。首次生成省略 revision；外部严格消费者需升级共享模型并实现修订数据，不能仅凭 schema_version 相同推断兼容。输出错误修正另要求 C 显式支持请求子类。B 的唯一规划协议为 `AgentPlannerPort.decide`。运行时不再提供两阶段规划、固定检索生产模式或自动回退图。文档版本、包版本和 JSON 结构版本独立。

LangGraph 只组织动作路由；授权、预算和结果提交仍由运行控制器负责。SQLite 已验证调用账本是恢复依据，不启用第二套图 checkpointer。

## 唯一执行流程：agent-loop-v1

公开执行入口仍为 `lake_agent.runtime.run/resume`。`engine/graph_runner.py` 负责共享图执行、追踪隔离与取消收尾，实际图由 `engine/agent_workflow.py` 构造；原 `workflow.py` 中仍在使用的共享执行机制已移动并改名。初始化目录后，运行始终采用模型决策与动作观察循环，不再根据规划器能力选择另一套业务流程。不支持当前协议的端口在任何外部调用前以 `invalid_planner_protocol` 拒绝；持久运行的对应拒绝码为 `recovery_engine_mismatch`。

目录整理不改变 `models.py`、`ports.py`、`runtime.py`、`config.py`、`state.py`、`trace.py` 与 `validation.py` 的公共路径。真实 JSON 规划器现位于 `adapters/planner.py`，内部图与恢复位于 `engine/`；评估报告格式位于 `evaluation/models.py`，固定基线运行器只在 `tests/`。完整映射见[源码目录说明](source-layout.md)。

“控制器是唯一执行决策者”指最终执行授权与调度：模型选择行动策略，控制器校验后执行或拒绝。它不要求每个问题使用同一种业务流程。目录和工具初始化后每轮调用 `planner.decide`；规划器必须声明 `agent_protocol = "agent-loop-v1"` 并实现 `decide(AgentDecisionRequest) -> ModuleResult[AgentAction]`。

请求保留 ModelCallContext，公开输入为 question、catalog_snapshot_id、tool_specs、budget_remaining、context、remaining_turns、require_verification。context 包含目录/候选、可选计划、已知观察/证据预览、已有答案、核验及上次动作结果。总工作上下文受 max_planner_context_bytes 约束，省略和截断显式记录；请求读取的页保持完整，recent previous_pages 有界保留。已接纳的工具结果工件不因工作上下文预览而截断；错误诊断和被拒输出按第4节单独限制预览。question、工具 schema 等静态字段还受规划器整个 HTTP 请求大小限制。

| kind | 行为与约束 |
|---|---|
| search | 自选 query 和可选 source_ids 过滤；来源必须在冻结目录内，空数组表示空范围 |
| tools | 选择已注册工具批次；每步含 step_id/tool_name/source_ids/args/depends_on/purpose，不要求先检索或建立子任务 |
| read_observation | 读取当前运行已保存观察的 JSON Pointer 页面，沿用原读取预算 |
| read_catalog | 按 offset/limit 分页查看目录元数据；不能把元数据当正文证据 |
| plan | 替换可选 subtasks 和 completed_subtask_ids；可为空，仍校验 DAG 和子任务上限；不固定后续行动顺序 |
| compose | 通过 ContentPort.compose 组织或修订答案，可带 revision_instructions；允许 evidence=[]，返回引用仍须属于实际证据 |
| verify | 通过原 ContentPort.verify 检查当前答案，反馈交回模型；无答案时拒绝并允许下一轮修正 |
| finish | 模型提出完成；控制器按当前政策接受或拒绝，当前有效的不支持核验及未解决内容错误阻止 completed |
| stop | 说明无法继续的原因并以 insufficient_evidence/agent_stopped 停止，可保留已有答案 |

`RunConfig.max_agent_turns` 默认32，每次模型决策消耗一轮，即使返回 Usage.local() 也不豁免；达到上限返回 budget_exceeded/agent_turn_budget。`require_verification` 默认false，允许未核验完成；为true时 finish 必须有绑定当前完整答案、证据和政策的 supported 核验，否则以可继续决策的拒绝观察返回模型。相关输入变化使旧核验过期；完整输入相同的再次生成保留原判定。过期不等于当前答案已被证明错误。completed、内部核验通过、外部评测正确相互区别。测试 no_verifier 下核验动作被拒绝，与 require_verification=true 的组合在外部调用前停止。

所有动作经过同一控制器的预算、超时、工具 schema、来源授权、并发、取消、引用与审计边界。新工具动作使用控制器内部单运行 scope，不要求模型或 A/C 实现新增子任务流程；A 仍接收同样 ToolRequest。C 通过已有 prepare_catalog 获得目录，可用标签说明可用来源，但正文事实必须来自实际输入证据，不能制造 evidence_id。

恢复继续从已知响应日志重建，`planner.decide` phase 使用严格 AgentDecisionRequest/ModuleResult[AgentAction]；持久状态记录固定的 planner_engine="agent-loop-v1"，恢复前验证端口协议。不会重新请求已完成的模型决策，也不会重放结果未知的外部调用。新源码的指纹与旧运行不一致时拒绝跨版本恢复。

显式支持的内容输出错误可进入新的自主决策；这不是适配器内重试。未解决反馈阻止 finish；新内容输出仍必须通过引用授权，Full 强制 supported 与 no_verifier 规则不变。详细错误 schema、能力协商和 provider messages 约定见[内容反馈说明](content-feedback.md)。

## 1. 设计原则

1. 模型决定行动策略，运行控制器拥有最终执行授权与调度权：控制器校验动作后决定是否获准执行，并落实预算、重试、取消和停止边界。
2. 跨模块边界只传 JSON 可序列化数据和稳定 ID，不传 Python 对象、文件句柄、DataFrame 或模型私有推理过程。
3. 工具只从数据模块注册的能力清单中选择；名称和参数都按本地 JSON Schema 校验，禁止模型提交任意 Python、Shell 或未经约定的 SQL。
4. 每一步都有显式预算、状态和事件记录。暂时性调用错误的重试与模型依据核验观察选择新动作分开处理。
5. 公开运行入口、数据模块/内容模块/规划器调用和 trace 写入均为异步接口。单 run 内只有经过可信工具声明和来源/资源冲突检查的 `DataPort.invoke` 可以并行等待 I/O；预算、意图、结果、观察、证据和步骤状态由控制器串行结算。模型决策及内容模块调用保持顺序，不因工具并行扩大范围。调用方仍可并发调度不同 run。
6. 完整审计结果、有界规划上下文和可引用证据分别管理。工具观察支持后续决策，但只有显式证据经过结构及来源校验后才能进入答案引用。

## 2. 运行控制器的公开入口

```python
async def run(
    request: RunRequest,
    *,
    data: DataPort,          # 数据模块提供
    content: ContentPort,    # 内容模块提供
    planner: AgentPlannerPort,    # 规划器/模型适配器
    trace: TraceSink,
    state_store: StateStore | None = None,  # 可选；调用方管理生命周期
) -> RunResult:
    ...
```

调用方使用 `result = await run(...)`；库内部不调用 `asyncio.run()`，以免接管调用方的事件循环。v0.5 要求 Python 3.11+，便于使用标准 `asyncio` 超时和取消原语。`cancel_cleanup_timeout_ms` 为取消 trace 清理的独立上限，避免 trace sink 挂起阻塞上层任务；该上限不能强制终止不配合取消的 adapter。

`run` 为一次任务创建唯一 `run_id`，读取并校验完整目录快照，将目录注册到内容模块并建立检索句柄，再执行受限的规划与工具循环，最后返回 `RunResult`。`run_id` 可由调用方传入以便关联外层实验；未提供时由运行控制器生成。相同 `run_id` 不得同时运行两次；不同 run 可以由调用方并发运行，调用方应以 semaphore 等方式限制总体并发。共享的数据模块、内容模块、规划器适配器和 trace 后端必须能安全处理不同 run 的并发请求；每个 run 的预算、证据、事件序号和目录句柄彼此隔离。

运行控制器以事件循环的单调时钟建立一次 `run_deadline`，并在每次派发前计算 `remaining_wall_time_ms`。若余量小于等于 0，运行控制器不创建新的 adapter task，直接以 `timeout/time_budget` 停止；否则计算 `call_deadline = min(run_deadline, now + per_call_timeout_ms)`，并用 `asyncio.timeout_at(call_deadline)` 包住 awaitable。timeout 异常结算时若当前单调时钟已达到 `run_deadline`，优先映射为 `time_budget`，否则映射为 `call_timeout`。adapter/SDK 同时配置原生请求 timeout；本地 CPU 密集的解析或索引不得阻塞事件循环，需使用有界 executor/process pool 或可分段协作的实现。异步取消是合作式的，不能由运行控制器强行终止忽略取消的 coroutine 或线程；若需要硬截止，必须由可终止的子进程执行并由其 owner 明确终止。

### 2.1 可选持久状态与恢复（v0.5）

```python
async def resume(
    run_id: str, *, data: DataPort, content: ContentPort,
    planner: AgentPlannerPort, trace: TraceSink, state_store: StateStore,
) -> RunResult:
    ...
```

省略 `state_store` 保持普通运行。启用后，`run` 创建同名唯一持久运行，`resume` 仅接收 `run_id`，使用已保存的原请求，不接受替换预算或问题。调用方应事先保存 `run_id`，每次尝试创建全新端口及 trace 实例，最终 `await state_store.aclose()`。运行入口释放自身租约，不关闭调用方存储。

`StateStore` 异步接口为 `create/acquire/load/save/begin_call/finish_call/release/aclose`，以 `StateLease` 证明本地独占所有权。内置 `SqliteStateStore` 使用标准库 SQLite WAL/FULL 事务和 POSIX `flock`，限同一主机的本地文件系统，不提供跨机器租约。磁盘线程必须结算后才传播取消；释放失败不能覆盖已传播的调用方取消。

每次外部派发前写 `started`，有效响应返回后立即写 `completed`；并行 worker 各自提交响应，不等待同伴。控制器仍唯一拥有授权、预算、观察与证据状态。JSONL 是审计，不能代替持久调用账本。恢复逐项匹配已知请求/响应重建状态；完整控制器 ID 序列保持稳定，不重新派发历史模型或工具请求。未知 `started` 调用以 `StateStoreError.code="recovery_outcome_unknown"` 拒绝恢复，包含幂等工具。

恢复读取要求运行时版本、实际源码摘要及请求/结果身份等状态校验通过；继续尚未终结的运行还要求可信工具契约一致。原绝对 UTC deadline 包含停机时间，历史预算不重置；正常终态（含预算停止与 provider 403 等普通错误）直接返回原结果，不读取工具清单或重新比较工具契约。取消、审计或状态写入故障仅在全部相关外部结果已知时可能恢复。源码变更、请求分叉或损坏状态均不能退回真实调用来尝试。

持久状态另记录受支持的 `content_feedback_protocol` 或 null。恢复在返回缓存终态及异步端口调用前比较当前 C 能力，变化以 `recovery_contract_mismatch` 拒绝。失败内容响应和扩展修正请求按调用日志严格重建，不依赖 C 的会话内存；修正调用只有 started、没有有效 completed 时也以 `recovery_outcome_unknown` 拒绝，原预算和期限不刷新。

首次新派发前，用原目录 snapshot/digest 在全新 ContentPort 重新执行本地 `prepare_catalog`，验证新的临时 handle；这是真实额外调用，计入 content 次数和已报告用量，必须返回 `provider_requests=0`。辅助身份独立于历史 ID 流。未知用量保持 null；违反本地调用契约也不能抹去已报告的实际 provider 请求。

持久模式要求 trace 实现 `create_attempt(run_id, attempt_id)`，内置 sink 写入 `<root>/<run_id>/attempts/<attempt_id>/events.jsonl`；每段序号独立，`detail.replayed=true` 标识历史调用事件。普通模式保留 `<root>/<run_id>/events.jsonl`。正常终态复用不创建新审计段。此接口不承诺任意协程指令恢复、跨版本迁移或外部工具 exactly-once。

完整接入示例及错误码见 [持久恢复说明](durable-recovery.md)。JSON `schema_version` 仍为 `1.0`，当前包为 `0.7.0`，本文档为 v0.5；三者独立。

### `RunRequest`

| 字段 | 类型 | 调用时必需 | 含义 |
|---|---|---:|---|
| `schema_version` | literal string | 否 | 固定为 `1.0`，省略时默认 `1.0` |
| `task_id` | string | 是 | 数据集或实验中的稳定任务 ID |
| `question` | string | 是 | 原始自然语言问题，不包含 gold answer |
| `mode` | enum | 否 | `no_verifier` 或 `full_agent`；默认 `full_agent` |
| `run_id` | string/null | 否 | 外部关联 ID；null 时由运行控制器生成 |
| `config` | `RunConfig` | 否 | 本次运行的限制和版本信息；省略时构造默认 `RunConfig` |

本文字段清单描述共享模型规范化后的对象字段，JSON 样例可省略具有默认值的字段；“调用时必需”表示没有默认值、必须由调用方提供的字段。省略字段与显式传入 null 不等价：只有类型允许 null 的字段才接受 null。共享模型仍拒绝额外字段和错误类型；运行控制器发给 adapter 的规范化请求包含已填充的默认值。

### `RunConfig`

| 字段 | 类型 | 必需 | 含义 |
|---|---|---:|---|
| `max_wall_time_ms` | integer | 是 | 整个 run 的墙钟时间上限 |
| `per_call_timeout_ms` | integer | 是 | 单个数据模块/内容模块/规划器调用的超时上限 |
| `cancel_cleanup_timeout_ms` | integer | 是 | 调用方取消后，追加取消事件并关闭 trace 的独立清理上限；不计入正常 run deadline |
| `max_subtasks` | integer | 是 | 可选 `plan` 动作包含的子任务上限，与工具步骤预算分开；不要求建立计划 |
| `max_agent_turns` | integer | 否 | 默认 32；每次 `decide` 消耗一轮，独立于 provider 请求数 |
| `require_verification` | boolean | 否 | 默认 false；true 时必须有当前有效的 supported 核验才可接受 finish；no_verifier 不允许与 true 组合 |
| `max_plan_steps` | integer | 是 | 本次 run 最多启动的 DataPort.invoke 逻辑步骤；目录分页和内容模块/规划器调用不计入 |
| `max_data_calls` | integer | 是 | DataPort.catalog 与 DataPort.invoke 的物理调用尝试总数，失败调用和重试也计数 |
| `max_model_calls` | integer | 是 | 实际发起模型推理的请求总数；本地目录注册/搜索不计入 |
| `max_retries_per_data_operation` | integer | 是 | 每个 DataPort.catalog/DataPort.invoke 逻辑操作可增加的重试次数；0 表示不重试 |
| `candidate_top_k` | integer | 是 | 单次 ContentPort.search 返回的候选源上限 |
| `max_output_bytes_per_call` | integer | 是 | 数据模块、内容模块、规划器单次响应的最大序列化大小 |
| `catalog_page_size` | integer | 是 | 数据模块每次返回的目录条目数 |
| `max_catalog_sources` | integer | 是 | 运行控制器本次最多接收的目录条目数；超出时拒绝用部分目录运行 |
| `max_catalog_bytes` | integer | 是 | 完整目录 `SourceRecord[]` 规范化 JSON 的 UTF-8 字节上限，含序列化字段开销 |
| `max_planner_context_bytes` | integer | 否 | 默认 24000，最小 1024；完整 `AgentDecisionRequest.context` 规范化 JSON 总 UTF-8 字节上限 |
| `planner_preview_bytes` | integer | 否 | 默认 2000，最小 256，且不超过 `max_planner_context_bytes` 的一半；单个字段预览与观察读取页的字节目标，实际装箱可缩小预览或省略条目 |
| `max_observation_reads` | integer | 否 | 默认 8，允许 0；每个 run 成功接受的本地观察读取次数上限 |
| `max_identical_tool_calls` | integer | 否 | 默认 2，最小 1；相同子任务、工具名称和规范化参数可接受的逻辑动作次数上限 |
| `max_parallel_tool_calls` | integer | 否 | 默认 4；严格整数 1–32，拒绝布尔值、浮点数和字符串；同一 run 同时运行的工具 worker 上限，1 表示串行限制，不为工具授予安全并行权限 |
| `max_input_tokens` / `max_output_tokens` | integer/null | 否 | 预留的 run 级累计上限；当前运行时尚不支持，任何非 null 配置均在数据/模型 adapter 调用前以 `failed/unsupported_token_budget` 拒绝 |
| `dataset_snapshot` | string/null | 否 | 数据目录版本或快照 ID |
| `model_id` | string | 是 | 所有规划器/内容模块 provider 请求使用的同一模型 ID，便于公平比较 |
| `model_params` | object | 是 | 所有规划器/内容模块 provider 请求共用的采样/解码参数，至少记录 temperature 或确定性设置 |
| `prompt_versions` | object | 是 | `{planner: string|null, content: string|null}`，分别记录规划器与内容模块的提示词版本 |
| `code_version` | string/null | 否 | Git commit、发布版本或代码哈希 |
| `config_version` | string | 是 | 本次运行配置版本 |

每次 `AgentPlannerPort.decide` 请求使用 `prompt_versions.planner`；`ContentPort.compose` 和 `ContentPort.verify` 使用 `prompt_versions.content`。放入 `ModelCallContext` 的 `prompt_version` 只包含当前调用对应的单个字符串，不直接传整个 `prompt_versions` 对象。

`prompt_version` 是宿主配置声明的审计标签，不验证外部模块实际发出的提示正文。本轮内容修正扩展不改默认标签或私有配置；实际协议、调用请求/事件和源码摘要共同提供行为证据。

表中“必需”指有效配置中的字段；共享 `RunConfig` 会为省略的字段填充默认值。规划提示词默认版本为 `planner-v1.1-exploration`，内容提示词默认版本仍为 `content-v0.1`。共享模型采用严格类型并拒绝额外字段，所有上限须满足表中整数约束；重试数及观察读取次数允许为零，token 上限可为 null，非 null 时须为正整数。旧工具的 `parallel_safe` 默认为 false，因此省略新字段时不会因并发上限默认 4 而自动并行。

当前以时间、调用次数、单次响应字节数、规划上下文字节数及完整规划 HTTP 请求字节数控量，不宣称 token 硬预算。未来若启用 token cap，须先定义与指定模型匹配的完整输入 tokenizer、每次输出上限及实际 usage 对账；只裁剪工作上下文不足以提供此保证。一次已开始的失败请求仍消耗模型调用数、时间及实际 token/费用。未发生或未测到的用量分别按已知的 0 或 `null` 记录，不得把未知用量填为 0。每个 AgentPlannerPort.decide 调用以及内容模块的 `compose`/`verify` 最多发起一个模型请求；`prepare_catalog`/`search` 仅执行本地索引或检索，不发模型请求。adapter/SDK 必须关闭隐式重试和后台请求。`max_data_calls` 包含 DataPort.catalog 分页和 DataPort.invoke 的全部物理尝试及中断时保留的意图预留；工具并行不会增加该额度。

## 3. 运行控制器与数据端口

数据模块提供目录读取和已注册工具清单；运行控制器不预设数据模块的解析库或 `query_spec` 内部结构。

### `DataPort`

```python
class DataPort(Protocol):
    async def catalog(self, request: CatalogRequest) -> CatalogResult: ...
    def tool_specs(self) -> list[ToolSpec]: ...
    async def invoke(self, request: ToolRequest) -> ToolResult: ...
```

所有跨模块请求共享 `CallContext`：`schema_version`、`run_id`、不透明的 `task_id`、`operation_id`、本次尝试唯一的 `call_id`、`attempt`、`timeout_ms`、`max_output_bytes`。数据模块响应及内容模块/规划器的 `ModuleResult` 必须回传相同 `call_id`。实际发起模型推理的内容模块/规划器请求另带 `ModelCallContext`，其中包含 `model_id`、`model_params`、本次 phase 的 `prompt_version` 和 `token_budget`；纯本地内容模块调用不带此字段。`task_id` 只用于关联日志，不得用于读取基准 gold 或标签。

| 公共字段 | 类型 | 语义 |
|---|---|---|
| `operation_id` | string | 同一逻辑操作的稳定 ID；重试时保持不变 |
| `call_id` | string | 每次物理尝试唯一；请求与响应必须一致 |
| `attempt` | integer | 从 1 开始，重试时递增 |
| `timeout_ms` | integer | 运行控制器按 `min(per_call_timeout_ms, remaining_wall_time_ms)` 计算；运行控制器与 adapter 均对 awaitable/底层请求应用 timeout |
| `max_output_bytes` | integer | 单次响应序列化字节上限，超限响应由运行控制器拒绝 |

`ModelCallContext.token_budget` 保留 `{remaining_input_tokens, remaining_output_tokens, max_output_tokens_this_call}` 三字段；当前可启动运行中均为 null。共享类型约束这些值为非负整数或 null，并要求单次 output 上限不高于剩余额度，但该类型约束本身不代表当前控制器已实现 token cap。`JsonPlanner` 也会在派发前拒绝带非 null token budget 的直接请求。本地无 provider 请求时对应 usage 记为 0，未测得的 provider token 用量保留 null。

`CatalogRequest` 在上述元数据外含 `snapshot_id`（首页使用请求中的 `dataset_snapshot`；若为 null，由数据模块建立当前冻结快照；后续页必须传回首页的 `catalog_snapshot_id`）、`page_size` 和 `cursor`（第一页为 null）。`CatalogResult` 含 `call_id`、`status`、`catalog_snapshot_id`、`sources`、`next_cursor`、`has_more`、`error` 和 `duration_ms`；`status` 为 `ok` 或 `error`，成功时 `error=null`，失败时 `sources=[]`、`next_cursor=null`、`has_more=false` 且 `error` 含 `code`、`message`、`retryable`。数据模块必须保证同一快照所有页稳定、无重复且 cursor 可继续读取；运行控制器按 cursor 读取完整目录，并在每页后检查累计数量/字节数。若超过 `max_catalog_sources`/`max_catalog_bytes`，run 以 `budget_exceeded/catalog_limit` 结束，不把部分目录当完整目录交给内容模块搜索。若请求了具体 `dataset_snapshot`，每页返回的快照必须完全匹配，否则运行控制器以 `failed/catalog_snapshot_mismatch` 结束。

目录第一页样例（`lake-v3` 共两页）：

```json
{
  "schema_version": "1.0",
  "call_id": "catalog-1-attempt-1",
  "status": "ok",
  "catalog_snapshot_id": "lake-v3",
  "sources": [{"source_id": "src-18", "name": "annual_report.pdf", "format": "pdf", "summary": "Annual financial report", "metadata": {"year": 2024}}],
  "next_cursor": "cursor-2",
  "has_more": true,
  "error": null,
  "duration_ms": 32
}
```

同一快照的终页回包示例：

```json
{
  "schema_version": "1.0",
  "call_id": "catalog-2-attempt-1",
  "status": "ok",
  "catalog_snapshot_id": "lake-v3",
  "sources": [{"source_id": "src-19", "name": "quarterly_report.csv", "format": "csv", "summary": "Quarterly financial data", "metadata": {"year": 2024}}],
  "next_cursor": null,
  "has_more": false,
  "error": null,
  "duration_ms": 27
}
```

每个 `SourceRecord` 调用时必填 `source_id`、`name`、`format`、`summary`；`metadata` 可省略，默认 `{}`，adapter 应填入检索所需的元数据；`content_hash`/`version` 可省略，默认 null。同一快照内 `source_id` 必须稳定、唯一。目录响应不得包含 gold answer、相关文件标签或根据答案生成的提示。

`tool_specs()` 是唯一保留同步形式的 port 方法：它只返回本地静态能力清单，不执行数据访问或模型调用。其他数据模块 port 方法为 async。`input_schema` 以 JSON Schema Draft 2020-12 作本地校验契约，并遵守下文的本地引用及来源路径限制；若 provider strict schema 只支持子集，运行控制器的 adapter 负责转换为受支持子集，本地仍须用 Draft 2020-12 校验完整参数。`ToolSpec` 调用时必填 `name`、`description`、`input_schema`、`idempotent` 和 `uses_sources`；规范化后的完整字段如下：

| 字段 | 类型 | 含义 |
|---|---|---|
| `name` | string | 工具稳定名称，例如 `inspect`、`query`、`extract` |
| `description` | string | 简短用途说明 |
| `input_schema` | JSON Schema object | 参数 schema，按 Draft 2020-12 和下文的接入限制本地校验 |
| `schema_version` | string | 参数结构版本，默认 `1.0` |
| `idempotent` | boolean | 相同请求重试是否安全 |
| `uses_sources` | boolean | 工具是否读取指定数据来源 |
| `source_id_paths` | string[] | 默认空数组；`args` 中引用来源 ID 的 JSON Pointer，路径不得重复；无来源引用时为空数组，数组项用 `*` 标识 |
| `parallel_safe` | boolean | 默认 false，严格布尔值；可信 adapter 承诺同实例、同快照的独立调用可以安全并行，独立于幂等重试权限 |
| `exclusive_resource_keys` | string[] | 默认空数组；非空字符串组成的静态互斥资源 key，列表中不得重复；不同工具访问同一连接、文件或引擎等互斥资源时声明相同 key |

`parallel_safe`、互斥 key、来源路径与参数 schema 均由可信数据 adapter 的本地注册提供，规划器不能在工具步骤中覆盖。`idempotent=true` 只声明重试安全，不推导 `parallel_safe=true`；反之，允许安全并行也不授予自动重试权限。未声明安全并行的旧工具保持串行。运行控制器重新严格校验并复制工具注册记录及嵌套元数据，拒绝重复 key 或绕过模型校验的错误类型，防止 adapter 后续修改原对象改变当前 run 的权限或资源声明。跨 adapter 调用的请求使用深层隔离副本；规划器即使修改收到的工具 schema、来源路径或并行声明，也不能修改控制器的可信注册表。接入代码应按字段值比较注册结果，不依赖 Python 对象身份。互斥 key 只约束当前 run 的调度，跨 run 共享资源仍需 adapter 自行保证可重入与互斥。

生产运行不要求某个工具名称或固定提取参数。数据模块可提供 `inspect`、`query`、`extract`、`download` 等工具，各自参数完全由其 `input_schema` 定义。下文使用 `extract` 作为接入示例；是否调用、何时调用以及使用哪个来源均由模型选择。显式 `data.evidence` 是 `EvidencePayload[]`，允许空数组表示成功但无匹配内容。

每个会引用来源的工具必须设 `uses_sources=true`，并在 `source_id_paths` 完整列出所有必需来源 ID 参数位置；不读取来源的纯计算工具设 `uses_sources=false` 且路径为空。例如 `extract` 为 `["/source_id"]`，多来源数组可用 `["/source_ids/*"]`。包含 `*` 的数组 schema 必须声明 `minItems >= 1`，空数组不得作为有来源工具的授权依据。运行控制器在加载工具清单时检查该字段组合及路径与 `input_schema` 一致；不完整或无效的定义按数据工具契约错误拒绝。执行时运行控制器解析路径，要求提取出的 ID 集合与规划器提交的 `AgentToolStep.source_ids` 一致，并验证它们属于当前冻结的完整来源目录；候选检索仅提供相关性提示，不缩小目录授权范围。这样本地 JSON Schema 负责类型/形状校验，来源路径元数据负责授权范围校验；数据模块返回的 `EvidencePayload.source_id` 也必须与本次实际参数来源一致。

工具 schema 中的 `$ref`、`$dynamicRef` 和 `$recursiveRef` 必须是以 `#` 开头的本地引用，禁止远程引用。`source_id_paths` 使用非根 JSON Pointer，只能沿 schema 中标为 required 的对象字段或上述非空同质数组解析，最终节点必须为 string。来源路径解析支持非递归的本地 `$ref` 指针（`#/...`）；该 `$ref` 节点只允许附带 `description`、`title`、`$comment`，不能带其他结构性兄弟字段。条件或联合 schema 中的来源位置不在当前支持范围内，应拆成独立工具注册。

`extract.input_schema` 的最小建议形状：

```json
{
  "type": "object",
  "properties": {
    "source_id": {"type": "string"},
    "query": {"type": "string"},
    "max_chars": {"type": "integer", "minimum": 1}
  },
  "required": ["source_id", "query", "max_chars"],
  "additionalProperties": false
}
```

### `ToolRequest`（运行控制器 → 数据模块）

```json
{
  "schema_version": "1.0",
  "run_id": "run-123",
  "task_id": "task-7",
  "step_id": "step-2",
  "operation_id": "op-2",
  "call_id": "call-2-attempt-1",
  "attempt": 1,
  "catalog_snapshot_id": "lake-v3",
  "tool_name": "extract",
  "args": {"source_id": "src-18", "query": "annual revenue", "max_chars": 12000},
  "timeout_ms": 15000,
  "max_output_bytes": 65536
}
```

`operation_id` 在同一逻辑步骤的重试中保持不变；每次物理调用使用新的 `call_id` 和递增 `attempt`。数据模块必须回传相同的 `call_id`。数据模块提供工具参数 schema；运行控制器在调用前检查 `tool_name` 已注册且 `args` 合 schema，再把超时和输出上限传给数据模块。

### `ToolResult`（数据模块 → 运行控制器）

```json
{
  "schema_version": "1.0",
  "call_id": "call-2-attempt-1",
  "status": "ok",
  "data": {"evidence": []},
  "error": null,
  "duration_ms": 84,
  "warnings": [],
  "truncated": false
}
```

- `status` 只能是 `ok` 或 `error`。成功但没有匹配内容时返回 `status="ok"` 和空数组；不能把空结果伪装成失败。
- `error` 包含 `code`、`message`、`retryable` 和 `recoverable`；后两项默认 false。成功时 `error=null`，失败时 `data=null`。`retryable` 授权同一逻辑操作按既有规则重试；`recoverable` 声明一次明确失败可安全通过修正输入重新规划，两者独立。
- 错误码至少区分 `invalid_args`、`unsupported_format`、`parse_error`、`timeout`、`rate_limited`、`unavailable` 和 `internal`。超时不能作为空证据返回。运行控制器通过 `asyncio.timeout_at(call_deadline)` 对 awaitable 设置基于绝对单调 deadline 的合作式单次 timeout，adapter/SDK 也须配置原生网络 timeout；两层共同减少超时资源残留，但不能强杀忽略取消的 coroutine 或阻塞线程。CPU 密集的解析若需要可控硬截止，应放在可终止的子进程中。数据模块必须把 timeout 表达为 error，不得把部分结果伪装成成功。
- 文本或表格证据须提供真实来源定位和 `source_id`；无法精确定位时给出文件级定位与 warning。裁剪时设 `truncated=true`。
- 数据模块应遵守 `max_output_bytes`；运行控制器也会拒绝超出上限的响应。已接受的完整工具结果与规划器的有界预览另行管理，不能把预览大小当作响应大小上限。
- `data` 允许包含 schema、查询结果等其他 JSON 字段。工具可以省略 `data.evidence`，或返回空数组而只产生工具观察。若提供了 `data.evidence`，必须是有效的 `EvidencePayload[]`，不能用任意工具输出代替证据。
- 数据模块返回的每条 `EvidencePayload` 调用时必填 `source_id`、`content_type`、`payload`、`locator`；`warnings` 可省略，默认 `[]`。evidence ID 由运行控制器生成，格式对调用方不透明、在单个 run 内唯一，并在传给内容模块前补成 `EvidenceRecord`（再附 `evidence_id` 和 `call_id`）；数据模块不生成 evidence ID。locator 必须指向实际页码、行列、JSON 路径或文本位置，不能由模型补造。
- 运行控制器校验每条 `EvidencePayload.source_id` 属于当前目录快照且与本次工具参数相符，再生成 run-scoped evidence ID。运行控制器/内容模块不解析或推断数据模块的 locator。

非空证据示例：

```json
{
  "source_id": "src-18",
  "content_type": "text",
  "payload": "Annual revenue was ...",
  "locator": {"type": "pdf_page", "page": 4},
  "warnings": []
}
```

超时错误样例：

```json
{
  "schema_version": "1.0",
  "call_id": "call-2-attempt-1",
  "status": "error",
  "data": null,
  "error": {"code": "timeout", "message": "Tool exceeded timeout_ms", "retryable": true, "recoverable": false},
  "duration_ms": 15000,
  "warnings": [],
  "truncated": false
}
```

## 4. 运行控制器与内容端口

内容模块只做目录索引/搜索、证据整合、答案生成和在线验证，不直接调用数据模块工具、不改变运行控制器的状态，也不能访问离线 gold。部署时在线进程及数据模块/内容模块/规划器适配器必须无法读取 evaluator/gold 文件；`task_id` 仅作不透明关联字段。每个响应使用统一外层：

```json
{
  "schema_version": "1.0",
  "call_id": "call-3-attempt-1",
  "status": "ok",
  "data": {},
  "error": null,
  "usage": {"input_tokens": null, "output_tokens": null, "cost": null, "currency": null, "provider_requests": 1},
  "duration_ms": 120
}
```

错误时 `status="error"`、`data=null`，`error` 含 `code/message/retryable/recoverable`，可另含 `details` 诊断。运行控制器按调用预算累计内容模块的用量；内容模块不必自行决定是否重试。`search` 没有候选源时以 `status="ok"` 返回空 `candidates`。实际 provider 请求在失败前已产生的 usage 仍需返回；纯本地检索或索引未发 provider 请求时 token usage 为 0，无法测量时为 null。

单次 `ModuleResult.usage` 固定包含 `input_tokens`、`output_tokens`（非负整数或 null）、`cost`（非负数或 null）、`currency`（string/null）和 `provider_requests`（0 或 1）。未发 provider 请求的本地实现必须显式回报 `provider_requests=0`、`input_tokens=0`、`output_tokens=0`；使用本库模型时应传 `Usage.local()`。等价 JSON 为 `{"input_tokens": 0, "output_tokens": 0, "cost": null, "currency": null, "provider_requests": 0}`。实际发起 provider 请求时回报 `provider_requests=1`，包括已发起但失败的请求；未知 token/费用保持 null。若遗漏 `provider_requests`，当前模型按 1 保守计数（fail-closed），不能依靠两个 token 字段为 0 推断没有请求。`prepare_catalog`/`search` 必须回报 0；若回报 1，运行控制器保留该调用消耗并以 `failed/invalid_module_response` 拒绝。

所有内容模块和规划器调用都带统一调用封套。统一响应 `ModuleResult[T]` 含 `schema_version`、相同的 `call_id`、`status`、`data`、`error`、`usage` 和 `duration_ms`；错误时 `status="error"`、`data=null`。`max_model_calls` 与 `call_counts.model_calls` 按 provider 请求计数（包括失败请求），不计有效报告为本地执行的 `prepare_catalog`/`search` 调用；异常/timeout 后无法确认是否已派发时，运行控制器保守保留一次额度预留。每个规划器方法以及内容模块的 `compose`/`verify` 最多发起一个模型请求，单 run 内这些调用不并行；SDK/adapter 隐式重试必须关闭。`call_counts.content` 仍统计所有运行控制器→ContentPort 方法调用。单次 `usage` 的五个字段遵循上述口径；未发起模型请求时 token 用量为 0，无法测量的费用为 null。数据模块的 `ToolResult` 不带模型 `usage`，其物理调用次数与耗时由运行控制器单独记账。

```python
class ContentPort(Protocol):
    async def prepare_catalog(self, request: CatalogRegistrationRequest) -> ModuleResult[CatalogHandle]: ...
    async def search(self, request: SearchRequest) -> ModuleResult[SearchResponse]: ...
    async def compose(self, request: ComposeRequest) -> ModuleResult[AnswerRecord]: ...
    async def verify(self, request: VerifyRequest) -> ModuleResult[Verification]: ...
```

`prepare_catalog` 和 `search` 即使只使用本地索引，也遵守 async port 形式以保持一致；实现若包含 CPU 密集工作，必须避免直接占用事件循环。纯同步的小型校验和内存数据变换仍可留在运行控制器/内容模块内部。

`prepare_catalog` 在普通新运行中调用一次；持久恢复在首次新派发前按第2.1节额外重新注册目录，正常终态直接复用结果时不重新注册。运行控制器将经快照及大小校验的完整 `SourceRecord[]` 交给内容模块；内容模块构建可复用的本地检索索引并返回 `{catalog_snapshot_id, catalog_digest, handle}`。`handle` 仅在该 `run_id` 和快照内有效。该调用不得将全量目录发给模型 provider，也不得在运行中执行模型推理；之后 `search` 只传 handle、问题和子任务，不重复携带目录摘要，且不执行模型推理。内容模块可采用词法与本地向量混合排序，具体算法由内容模块决定；若使用向量，运行前需完成向量计算并记录 embedding 模型/版本。若内容模块不能完整索引该快照，返回错误，运行控制器不得悄悄把部分目录当完整索引。

`CatalogRegistrationRequest` = `CallContext` + `{catalog_snapshot_id, catalog_digest, sources[]}`；`sources` 必须是该快照的完整目录，输入 UTF-8 JSON 字节数不得超过 `max_catalog_bytes`。`catalog_digest` 是由运行控制器按稳定 `source_id` 排序并规范化 JSON 后计算的 SHA-256。内容模块返回的 `CatalogHandle` 含 `catalog_snapshot_id`、相同的 `catalog_digest` 和不透明 `handle`。运行控制器校验快照和 digest 一致后才开始搜索。注册失败、超时或 digest 不匹配时不继续 run；调用计入一次 `call_counts.content`，但不消耗 `max_model_calls`，也不属于工具重试范围。

```json
{
  "catalog_snapshot_id": "lake-v3",
  "catalog_digest": "sha256:abc123...",
  "handle": "catalog-run-123"
}
```

### `search`（运行控制器 → 内容模块）

该本地检索调用只带 `CallContext`。A/C 请求字段保持原样：原问题、`subtask`（含当前查询 `search_query`）、`catalog_snapshot_id`、`catalog_handle`、已访问 `source_id`、`source_id_filter`（可为 null）、`limit`。控制器把模型 `search` 动作的 query 放入既有 `subtask` 字段；它是本次检索的兼容封装，不要求模型先拆分任务。source_ids 由模型动作选择并映射到 source_id_filter，运行控制器先校验其属于目录；显式空数组表示空范围，null 表示整个目录。`limit` 不得大于 `candidate_top_k`。内容模块必须确认 handle 对应给定快照；运行控制器校验候选的 `source_id` 属于该完整目录及过滤集合（若提供）。`search` 不调用模型，usage 中 token 记为 0。

输出数据：`candidates` 不超过请求的 `limit`，`source_id` 不重复，`score` 是有限的 0–1 排序分数；无结果返回空数组。

```json
{
  "candidates": [
    {"source_id": "src-18", "score": 0.82, "match_reason": "包含相关年份的财务表"}
  ]
}
```

运行控制器会检查候选 `source_id` 属于本次目录快照，并在后续搜索中传入去重后的已访问 ID。分数是检索排序信号，不是答案置信度。

### `compose`（运行控制器 → 内容模块）

除统一调用封套及 `ModelCallContext` 外，输入数据含原问题、`completed_subtasks` 和带定位的 `EvidenceRecord[]`。completed_subtasks 是可选计划中由模型标记完成的摘要，可以为空；evidence 也可以为空，用于目录说明、澄清或资料不足说明。正文事实不得超出实际可用输入，不能编造引用。当前 `token_budget.max_output_tokens_this_call` 为 null，不构成输出 token 上限；未来启用该上限时，内容模块不得覆盖或扩大其非 null 值。

`ComposeRequest.revision: AnswerRevision | None` 是默认空的增量字段，None 时序列化省略；首次生成继续接收原有业务字段。有旧答案时，控制器发送以下修订信息，使无状态 C 不必依赖内部会话：

| revision 字段 | 含义 |
|---|---|
| `previous_answer` | 当前完整已接纳 AnswerRecord，包含正文、引用、未解决问题和结构化结果 |
| `verification` | 针对该答案保存的核验意见，可为 null；不是输出格式错误的 ContentFeedback |
| `verification_is_current` | 核验是否仍适用于当前完整可信输入；无核验时必须 false |
| `instructions` | planner 的可选修订目标，非空时为1–4000字符 |

这些数据不替代 question 或 evidence，不具有授权、预算或修改 require_verification 的效力；历史核验过期时只能当作历史参考。控制器不把失败核验伪装成 EvidenceRecord。C 必须把 revision 内容放进提供方的 messages，并遵守当前完整证据与运行契约。本文只描述仓库已实现的隔离内容适配器，外部真实 C 需要升级共享模型和实际模型消息构造，不因 Python 多了字段就自动支持。旧版 extra=forbid 消费者会拒绝非空 revision，应先升级再联调；不静默丢弃修订数据。

输出 `AnswerRecord`：

```json
{
  "text": "...",
  "citations": ["ev-4"],
  "unresolved_questions": [],
  "structured_result": null
}
```

运行控制器检查每个 citation ID 都存在于本次传入的证据集合。内容模块不得编造 evidence ID；证据不足时应在 `unresolved_questions` 明示缺口。Verifier 的 `issues[].evidence_ids` 也必须全部属于本次传入的证据集合。

### `verify`（运行控制器 → 内容模块，仅 `full_agent`）

除统一调用封套及 `ModelCallContext` 外，输入数据含原问题、当前 `AnswerRecord`、本次回答可见的 `EvidenceRecord[]`。

控制器将合法核验结果绑定到实际传入的完整问题、完整 AnswerRecord、完整有序 EvidenceRecord 列表和运行政策（mode、require_verification）的确定性指纹。比较依据是可信运行状态，不是 planner context 中的摘要或截断预览。指纹不纳入新时间戳、attempt ID 或审计事件顺序；同一运行重放保存的请求/响应可重建相同适用状态。

输出数据：

```json
{
  "verdict": "insufficient",
  "issues": [{"type": "missing_evidence", "evidence_ids": [], "detail": "缺少 2024 年的值"}],
  "recommendation": {
    "action": "query_again",
    "focus": {"subtask_id": "subtask-2", "query": "2024 annual revenue", "source_ids": ["src-18"], "missing_fields": ["2024 revenue"]}
  }
}
```

`verdict` 为 `supported`、`insufficient`、`conflicting` 或 `invalid_citation`；`recommendation.action` 为 `none`、`compose_again`、`search_more`、`query_again` 或 `stop`。`focus` 给出建议标识 `subtask_id`、查询、来源或缺失字段。为保持 C 的既有请求/响应结构，`search_more`/`query_again` 仍须提供 focus 和非空 subtask_id；该标识不要求来自模型的可选计划，也不触发自动子任务重访。控制器校验引用及目录来源后，把完整建议作为下一轮模型观察。内容模块不决定动作路由，验证超时或错误绝不能解释为 supported。

### 内容输出诊断与显式修正扩展

`ErrorInfo.details: JsonObject | None` 是可选的有界 JSON 诊断；None 时序列化省略该字段。内容输出校验可记录 `issues[].path/code/actual/expected`、允许的证据/来源 ID 预览、`rejected_output` 和 `omitted`。path 使用 JSON Pointer；详情最大8000字节，还受单次响应总大小限制，省略项不能作为完整授权集合。不得在诊断中写 HTTP 鉴权、私有连接封装或任意原始异常文本。

C 只有声明 `content_feedback_protocol = CONTENT_FEEDBACK_PROTOCOL`（精确值 `"content-feedback-v1"`）后才会收到扩展请求：`ComposeFeedbackRequest(ComposeRequest)`、`VerifyFeedbackRequest(VerifyRequest)` 各增加必填 `feedback: ContentFeedback`。反馈含此前同阶段的 `phase/call_id/error`。这与 ComposeRequest 中默认空的 revision 是不同扩展：revision 传递旧答案及合法核验意见，feedback 传递非法输出的诊断。未声明能力的模块不接收 feedback 字段；旧的 extra=forbid 消费者仍需升级共享修订模型，若还需输出错误纠正，再实现反馈处理并声明能力。未知声明不构成支持。

显式支持后，`invalid_model_citations`、`invalid_model_sources`、`invalid_model_output` 这些已知模型输出错误可交回 decide。控制器也校验 status=ok 响应中的引用；拒绝输出不覆盖有效答案或核验，未解决反馈使 finish 返回 `unresolved_content_failure` 拒绝观察。同阶段输出成功清除反馈；接纳不同的新答案时，另外清除针对旧答案的 verify 输出反馈与核验收据，相同答案不能借此清除原状态。模型继续自由选择动作。

规划模型通过既有 context 接收反馈，内容模型则通过扩展请求接收；二者不共享隐式聊天历史。C 应把动态 feedback 放进提供方的 `messages[].content`，不新增 HTTP 顶层参数、不改原 question、不伪造 evidence。每个内容调用仍最多一个模型请求；修正是新的受预算调用，不由 adapter 内部循环重试。HTTP/鉴权/传输/超时/提供方封装与端口契约错误维持终止，旧模块也不因 retryable/recoverable 标志就获得内容纠错能力。完整样例见[内容反馈说明](content-feedback.md)。

## 5. 统一规划器请求与动作

规划器属于 B 内部适配器，A/C 无需实现。唯一端口为：

```python
class AgentPlannerPort(Protocol):
    agent_protocol: str  # 必须为 "agent-loop-v1"

    async def decide(self, request: AgentDecisionRequest) -> ModuleResult[AgentAction]: ...
```

`AgentDecisionRequest` 包含 `ModelCallContext` 以及以下业务输入：

| 字段 | 含义 |
|---|---|
| `question` | 原始问题 |
| `catalog_snapshot_id` | 当前完整目录的冻结身份 |
| `tool_specs` | 数据模块的可信工具声明；模型不能覆盖权限 |
| `budget_remaining` | 非负整数 `wall_time_ms/model_calls/data_calls/plan_steps/subtasks` |
| `context` | 有界目录、候选、观察、证据、可选计划、当前答案、核验与动作反馈 |
| `remaining_turns` | 此次决策前剩余轮数，受 max_agent_turns 限制 |
| `require_verification` | 宿主是否要求当前答案通过核验后完成 |

控制器在调用前计算余量。`data_calls` 等于上限减去已预留的目录/工具物理调用；不得由适配器增加。token 预留字段继续放在 ModelCallContext.token_budget，当前仅接受 null。JsonPlanner 仅向 provider 发送上述七项业务输入，运行 ID 等不透明关联身份保留在本地封套。

动作是以 kind 区分的严格联合类型，详见本文开头的动作表；未知 kind、额外字段或类型错误均不被静默修正。

- `tools.steps` 每项是 AgentToolStep，包含唯一 step_id、tool_name、显式 source_ids、args、purpose 及默认空 depends_on；不要求 subtask_id。依赖只可引用同一动作中的步骤，必须无环。所有步骤整体验证后才提交 ID 或派发第一批；后面的非法步骤不能让前面的合法步骤先执行。
- 工具参数须符合本地 Draft 2020-12 schema，参数路径提取的来源集合须精确等于 source_ids，并全部属于完整冻结目录。模型可以直接选择目录中来源，不要求先搜索。
- `plan` 只替换可选 subtasks 和 completed_subtask_ids，可为空。Subtask 的 subtask_id/objective/search_query 必填，required_evidence/depends_on 默认空数组；计划 ID 唯一、依赖存在且无环。完成 ID 必须属于本次计划。它记录工作进展，不建立固定的工具执行顺序。
- 超出剩余工具步骤的动作整体以 budget_exceeded/step_budget 停止，不截断后执行。执行动作、观察读取及内容调用都仍消耗各自时间和调用预算。
- `compose` 通过 C 生成或修订答案，可带默认空的 revision_instructions（1–4000字符），控制器把它放入 revision.instructions。没有旧答案却请求修订时，返回 revision_answer_required 拒绝观察且不调用 C；普通首次 compose 不受影响。
- `finish` 是提交已有答案的完成提议，不替模型自动生成答案；最终是否接受由控制器按当前政策判断。无答案 finish/verify 作为拒绝反馈返回模型，允许下一轮修正。

一个合法工具动作响应：

```json
{
  "schema_version": "1.0",
  "call_id": "decide-2-attempt-1",
  "status": "ok",
  "data": {
    "kind": "tools",
    "steps": [{
      "step_id": "read-report",
      "tool_name": "extract",
      "source_ids": ["src-18"],
      "args": {"source_id": "src-18", "query": "annual revenue", "max_chars": 12000},
      "depends_on": [],
      "purpose": "Read the requested annual value."
    }]
  },
  "error": null,
  "usage": {"input_tokens": null, "output_tokens": null, "cost": null, "currency": null, "provider_requests": 1},
  "duration_ms": 930
}
```

可选计划示例为 `{"kind":"plan","subtasks":[],"completed_subtask_ids":[]}`；最终提交为 `{"kind":"finish"}`；无法继续为 `{"kind":"stop","reason":"说明尚缺少的资料"}`。模型决定这些动作何时发生，控制器决定执行是否获准。日志记录动作及公开状态，不记录私有推理全文。

JsonPlanner 使用 provider JSON object 模式与本地严格类型校验；动态工具参数未必能由每家 provider 的 strict schema 子集表示，因此不宣称 provider 端严格 schema 保证。它不自动修复格式或重试，单次 decide 至多发一个请求。

### 5.1 完整工具观察与引用证据

每个通过封套、大小及 `call_id` 校验的 `ToolResult`，包括有效错误响应，先完整提交审计 artifact，再建立 run 内观察索引并记录 `tool.observed`。观察保留：`observation_id`、`call_id`、`operation_id`、`attempt`、`step_id`、`subtask_id`、`tool_name`、已授权 `source_ids`、原始 `args`、`status`、`artifact_ref` 和完整 `result`。内部重试的有效错误响应也保留为观察；未返回有效响应的异常、timeout 和响应校验失败不被伪造为有效工具观察。adapter 显式返回的有效 timeout 错误仍可被审计，但不能据此进入错误恢复。

工具观察不拥有 evidence ID。只有成功结果的显式 `data.evidence` 通过 `EvidencePayload` 和当前快照/参数来源校验后，才生成完整 `EvidenceRecord` 并独立持久化；无效证据仍会终止 run，不因工具响应封套有效而放行。`compose` 和 `verify` 始终接收完整证据集合；上下文预览不替换证据，也不允许直接引用观察 ID、错误文本或摘要。

### 5.2 有界工作上下文与完整请求限制

完整 `AgentDecisionRequest.context` 按规范化 JSON 的 UTF-8 字节数计量，不得超过 max_planner_context_bytes。上下文包括目录/候选、计划、答案/核验、最近动作、观察与证据预览和分页结果。小值保留结构，大值用带 truncated/text/total_bytes 的对象显式预览；ID 无法完整容纳时省略整项，不编造缩短后的 ID。

context_status.observations/evidence 记录 total/included/omitted，omitted_fields 记录整字段省略。按确定顺序优先尝试最近的工具观察和证据；分页生成前预留未解决反馈的阶段/调用ID/错误码，紧随读取的决策保留完整 text/next_offset，其他预览和更老 previous_pages 可让出空间。后续动作产生新反馈时，旧页面可通过 omitted_fields 明确省略。裁剪或省略不证明原始结果不存在该字段。完整结果、证据与审计文件继续保留。

当完整工具观察预览装不下时，构造器会在剩余预算内尝试保留带 preview_omitted=true 的最小索引：完整 observation_id、tool_name、source_ids 和 status。该索引不含被省略的结果正文，模型可用 read_observation 读取；若连索引也无法容纳，仍由 context_status.observations.omitted 明示省略。上述预览处理不改变控制器保存的完整状态或核验指纹。

此上限不包含 question、tool_specs、系统提示词、输出 schema 或 HTTP 封装。JsonPlanner(max_request_bytes=256000) 在派发前另限完整序列化 HTTP JSON 请求体；超限返回 planner_input_too_large 且 usage.provider_requests=0。两层均是字节边界，不是模型 token/context-window 保证，也不限制 compose/verify 所需的完整证据输入。

### 5.3 本地观察分页读取

```json
{"kind":"read_observation","observation_id":"obs-...","pointer":"/data/schema","offset":0,"max_chars":2000}
```

读取只接受当前 run 观察索引中的 ID，不将 ID、pointer 或工具输出解释为文件路径、URL、对象属性或其他 run 的资源。pointer 使用 RFC 6901：空字符串选择完整 `ToolResult`，`/data/schema` 选择对应字段，`~0`/`~1` 分别转义 `~` 和 `/`。解析后的目标值被规范化为 JSON 文本，再按 Unicode 字符 offset 分页；`max_chars` 为 1–4096，字节预算还可进一步缩短实际页。

成功读取页含 `observation_id`、`pointer`、`offset`、`text`、`next_offset`、`total_chars` 和 `truncated`。`text` 可能只是部分 JSON，规划器应按 `next_offset` 继续，不能把单页当成完整 JSON 或原始字符串。每页先持久化，再记录 `observation.read`，并增加 `call_counts.observation_reads`。无效 ID、pointer、越界 offset 或无法在字节预算内返回的页以 `failed/invalid_observation_read` 停止；已用完 `max_observation_reads` 时以 `budget_exceeded/observation_read_budget` 停止。

规划器 HTTP 请求不保存会话，下一轮 context.observation_read 除最新页外，在有界空间保留连续读取阶段的最近 previous_pages，并以 omitted_pages 记录已移出上下文的页数。新工具动作执行后清空该读取历史；完整观察与审计页不因此删除。当前已请求页不再二次截断，next_offset 保持与实际 text 一致。读取本身不调用 `DataPort` 或 provider，不消耗 `max_data_calls`、`max_model_calls` 或 `max_plan_steps`；为产生读取决策和消费返回页而进行的规划器请求仍正常占用模型/时间预算。

## 6. 运行策略、状态和重试语义

生产 RunRequest.mode（运行模式）与 EvaluationMode（报告中的实验组）分别管理，不能把实验组直接作为生产模式派发。

| 生产运行模式 | 行为 |
|---|---|
| `full_agent` | 同一个自由动作循环，允许模型选择核验；日常默认 require_verification=false |
| `no_verifier` | 同一个自由动作循环，verify 被拒绝；不允许 require_verification=true |

离线 `tests/support/ablation.py` 的 EvaluationMode 有 fixed_retrieval/no_verifier/full_agent 三组。固定 top-k 仅由评估侧本地规划器提出 search/tools/compose/finish 动作，不存在生产固定检索分支；其执行封套使用 no_verifier。评估 full_agent 明确设 require_verification=true，no_verifier 与固定组设 false。实验必须使用同一任务、冻结快照、基础模型/采样及公开预算；评估标签仅在执行结束后用于评分，不能传入执行端口。

max_subtasks 只约束可选计划大小；max_plan_steps 与 call_counts.steps 只计算实际派发的工具逻辑操作。目录/工具物理尝试共享 max_data_calls，provider 请求共享 max_model_calls，本地 prepare_catalog/search 不耗 provider 额度。每次 decide 还消耗一轮 max_agent_turns，因此零提供方用量的本地决策不能无限循环。不同 run 可以并发，但宿主须限制总并发并确保共享适配器可重入。

### 6.1 有界工具批次与稳定提交

运行控制器对整份已授权计划建立稳定拓扑顺序，以已成功提交的步骤集合判断依赖就绪，并从待执行步骤中选择一批操作（wave）。每批数量同时受 `max_parallel_tool_calls` 和剩余物理数据额度限制；只有 `parallel_safe=true`、具有明确来源且来源集合和 `exclusive_resource_keys` 均不冲突的就绪步骤才能进入同一批。同源或 key 冲突结束本批选择，后续步骤不越过该冲突步骤；未声明安全并行的工具和无来源工具形成全局串行屏障，屏障即使尚未就绪也不能被后续步骤越过。无依赖、只读或幂等均不能代替显式安全声明。

每批按以下三个阶段执行：

1. 控制器按选择顺序串行检查预算、建立请求 artifact、预留物理调用额度并提交所有 `tool.call.started` 意图。整批意图全部提交后才允许创建 worker；若准备中断或失败，不派发任何已准备操作。意图写入和后续等待都消耗各自的绝对 `call_deadline`，不能因排队重新获得完整 timeout。
2. 有界 worker 仅执行 `DataPort.invoke` 的异步 I/O、单次合作式 deadline 和响应封套/大小/`call_id` 校验，结果保留在各自尝试记录中。worker 不修改控制器预算、观察、证据或 trace；语义提交不按物理返回时间竞争。
3. 整批 worker 全部结束后，控制器按本批选择顺序串行提交结果 artifact 与完成/失败事件、工具观察、经来源校验的证据及步骤状态。先完成的同伴不提前释放新批次。子步骤只有在全部父步骤成功、父步骤审计及证据提交完成、且当前整批结算完成后才可派发；父步骤成功但没有证据的合法工具也须完成其观察和步骤提交。

批次内某个尝试仍可按既有幂等重试规则处理时，先结算本批其他已启动结果，再组织重试批次。某个逻辑步骤最终进入可恢复失败或致命停止时，冻结所有新派发，包括同伴尚未启动的重试；先收齐并结算已启动同伴、保留成功证据，再将失败步骤标记 `failed`、未启动的依赖后代标记 `blocked`、其余未执行步骤标记 `skipped`，最后重新规划或停止。不会把一个同伴的失败解释为其他已启动操作从未发生。全局 deadline、取消或 trace 失败可能中断正常事件提交，适用第 7 节的逐调用审计边界。

### 6.2 重试、错误恢复与取消

重试规则：

- `max_retries_per_data_operation` 只适用于 DataPort.catalog/DataPort.invoke，表示首次调用之外最多额外尝试次数；采用有上限的指数退避加 full jitter，实际等待时间写入 trace 并服从全局 deadline。DataPort.catalog 是只读幂等操作；DataPort.invoke 仅当对应 `ToolSpec.idempotent=true` 时可自动重试。两者还必须满足 `error.retryable=true`。目录页重试保持相同 `operation_id`、快照和 cursor，使用新的 `call_id` 并递增 `attempt`；快照不匹配属于不可重试错误。每次物理尝试都消耗一次 `max_data_calls`。
- 规划器/内容模块不自动重试；支持 `content-feedback-v1` 的内容输出修正先回 decide，再由模型选择新的内容调用，各自计数。数据模块的 per-call timeout 可按既有规则仅对幂等操作且 `error.retryable=true` 的操作重试；达到 run deadline 时立即以 `timeout/time_budget` 结束。单次 timeout 已耗尽且不能重试时以 `timeout/call_timeout` 结束。已派发后被取消或超时的工具调用结果视为未知；不得记录为成功，只有幂等且标记 retryable 的操作才可重试。
- 工具重试保持原 `operation_id`，使用新 `call_id` 和递增 `attempt`；每次物理意图重新占用数据额度。待重试操作按剩余物理预算拆成可完整准备的有界小批；预算不足时不先准备全部重试，更不超额派发。内部重试不重复增加逻辑 `steps` 或重复动作签名计数；`retries` 仅在额外尝试实际进入 adapter 后增加。若同批存在最终失败决定，停止其余尚未派发的重试并先结算已启动同伴。
- 验证器的 `insufficient`/`conflicting` 属于业务反馈，交回模型决定下一动作，不是对同一调用的重试。每次尝试都消耗调用预算并记录独立 `call_id`；adapter/SDK 不得再做隐式重试。

工具错误恢复与重试分开结算。只有可信数据 adapter 返回有效的 `ToolResult(status="error", error.recoverable=true)`，声明该操作已明确失败、可安全修正输入，且既有重试规则已不再派发额外尝试时，`no_verifier`/`full_agent` 才接受错误观察并进入下一轮规划。可重试且幂等的操作先按重试额度执行；耗尽后若最后一次有效错误仍可恢复，才转入重新规划。恢复不要求 `idempotent=true`，因为它是新的决策，不是重放未知副作用；adapter 不得把未知结果标记为 recoverable。新的 decide、工具动作、轮数和时间照常计入各自预算，不存在独立固定反馈循环额度。

工具的 `recoverable` 不覆盖停止规则：授权/权限错误、timeout/provider timeout、schema/响应/工具契约违规、token 预算错误即使错误标记为 true 也不能恢复；运行控制器的工具校验失败、adapter 异常、网络异常与未知执行结果也不能通过此字段进入重新规划。`recoverable=false` 时沿用既有 `tool_error`/`retry_exhausted` 等终止语义。第4节的内容输出修正采用独立的能力声明与错误类别判断，不放宽这些工具规则；错误本身不进入证据集合。

每次工具派发前，以 `[subtask_id, tool_name, args]` 的规范化 JSON 计算重复动作签名，按 run 累计；step ID、purpose 和参数键顺序不会改变签名。默认允许 2 次相同逻辑动作，第 3 次在派发前记录 `tool.loop_detected`，将本步骤标记 `skipped`、依赖步骤标记 `blocked`、其余步骤标记 `skipped`，并以 `failed/repeated_tool_call` 停止，不新增该动作的 DataPort 调用或逻辑步骤计数。同一次逻辑动作的内部重试只计算一次重复动作次数，但每次物理尝试继续消耗 `max_data_calls`。

调用方取消当前 run 时，运行控制器不把 `asyncio.CancelledError` 转成 `RunResult`、模块错误或可重试错误。控制器先取消并等待全部所属工具 worker，观察每个子任务结果并结算实际派发计数，再在独立 `cancel_cleanup_timeout_ms` 上限内，以 shielded cleanup 尽力追加 `run.cancelled` 事件并关闭 trace 后重新抛出；trace 清理超时或失败时放弃清理并保留原始取消。合作式 worker 的观察完成先于 trace 关闭，但不能用该 trace 清理上限强杀不配合取消的 adapter。取消前已提交或提交结果未知的 `*.call.started` 物理尝试仍消耗调用预算，即使 adapter 尚未真正发出底层请求也不退还；未派发的首尝试和重试分别不增加逻辑 `steps` 与 `retries`。`CancelledError` 与 `asyncio.timeout_at()` 产生的 timeout 分开处理。trace sink 在取消或进程被强制终止时可能无法写完最后事件，因此 `run.cancelled` 是尽力记录而非持久性保证。

`RunResult.status`：`completed`、`insufficient_evidence`、`budget_exceeded`、`timeout`、`failed`。`completed` 表示控制器按当前运行政策接受了模型的完成提议；运行完成、内部核验通过、外部评测正确是不同概念，均不能互相替代。未核验的完成和 supported 均不是事实正确性的保证。`answer` 在 `completed` 时必填；其余状态可保留最后一个 citation 已校验的答案，若尚无则为 null。状态与 `stop_reason` 映射固定如下：

调用方任务取消会抛出 `asyncio.CancelledError`，不返回上述 `RunResult`；`RunResult` 仅表示 运行控制器自身正常完成、预算停止、超时或失败后的结算结果。

| 条件/stop_reason | status | answer |
|---|---|---|
| `agent_finished`：已有答案，满足当前政策，且没有当前有效的不支持核验或未解决内容错误 | `completed` | 必填 |
| `agent_stopped`、`agent_verification_failed` | `insufficient_evidence` | 保留最后的有效答案或 null |
| `agent_turn_budget`、`step_budget`、`data_call_budget`、`model_call_budget`、`token_budget`、`catalog_limit`、`observation_read_budget`、`planner_context_limit`、`catalog_context_limit` | `budget_exceeded` | 保留最后的有效答案或 null |
| `time_budget`、`call_timeout` | `timeout` | 保留最后的有效答案或 null |
| `retry_exhausted`、`tool_error`、`module_error`、`invalid_plan`、`invalid_module_response`、`invalid_observation_read`、`invalid_catalog_read`、`repeated_tool_call`、`unsupported_token_budget`、`invalid_planner_protocol`、`invalid_run_policy`、目录/审计/适配器错误 | `failed` | 保留最后的有效答案或 null |

| verdict | 合法 recommendation.action | 控制器行为 |
|---|---|---|
| `supported` | `none` | 保存核验，返回下一轮模型决策；不自动结束 |
| `insufficient` / `conflicting` | `search_more` / `query_again` / `stop` / `none` | 保存失败核验及建议，交回模型；同一答案不能直接 finish 成功 |
| `invalid_citation` | `compose_again` / `stop` | 保存失败核验及建议，交回模型；实际动作继续经过控制器 |

C 的现有 focus schema 保持不变：search_more 必须含非空 subtask_id/query，query_again 另需非空 source_ids；compose_again/none/stop 的 focus 必须为 null。控制器校验 issue.evidence_ids 和 focus.source_ids，不强制根据建议重访某个固定子任务。建议中的 none/stop 都不是 supported 的替代证明。

require_verification=true 时，finish 必须已有当前有效的 supported 核验。旧结果存在但指纹不匹配时返回 agent.action.rejected/verification_stale；缺失核验或当前核验不支持时返回 verification_required，并提供继续 verify 或修订的原因。新增或改变证据、完整答案变化会使旧核验过期；过期不等同于当前答案已被证明不支持。完全相同的再次生成保留相同输入上的失败判定，不能靠重复 compose 清洗。工具、目录读取、搜索或计划操作若没有改变核验相关状态，也不会仅因发生一次调用就使结果过期。

context.verification_status 为 missing/current/stale，独立于有界核验预览；只有 current 时 verification_verdict 才表示当前判定。成功生成不同答案后，控制器清除当前收据，新答案为 missing；答案未变而证据改变时，保留的历史收据为 stale。截断预览无法使旧结果变成 current。require_verification=false 允许缺失或过期核验时完成，但当前有效的不支持核验仍沿用 agent_verification_failed 的停止行为。full_agent 模式本身不启用强制策略，默认值保持 false。SQLite 恢复按原请求/响应重建指纹与修订输入，已保存调用不重复派发，未知结果继续拒绝恢复；本轮源码变化仍受既有实现指纹检查约束，不绕过旧任务兼容性检查。

## 7. `RunResult` 与轨迹存储（`TraceSink`）

`RunResult` 至少包含：`schema_version`、`run_id`、`task_id`、`mode`、`status`、`stop_reason`、`answer`（无答案时为 null）、累计 `usage`、`call_counts`、`started_at`、`ended_at`、`duration_ms`、`versions` 和 `trace_ref`。trace 是事件的唯一权威来源；不在返回对象里复制第二份事件数组。

`RunResult.failure` 可空，非空时是 `{phase, call_id, error}`，用于保留最近尚未解决的具体失败；None时序列化省略。预算/时间停止可以同时保留此前内容错误，成功终态不携带残留失败。Web优先使用此字段，旧历史从已有失败事件回退展示；显示推导不改变恢复状态，也不补造旧记录未保存的模型输出。

`call_counts` 的计数口径固定为：`planner` 是运行控制器→AgentPlannerPort.decide 调用次数，`content` 是运行控制器→ContentPort 方法调用次数（包含本地 `prepare_catalog`/`search`），`model_calls` 是按上述保守规则结算的 provider 请求数，`catalog` 与 `tools` 分别统计 DataPort.catalog/DataPort.invoke 的物理尝试及中断时保留的意图预留，`retries` 是实际派发的数据模块额外尝试次数，`steps` 是实际派发的 DataPort.invoke 逻辑步骤数，`observation_reads` 是成功接受的本地观察读取页数。`max_subtasks` 限制计划子任务数，`max_plan_steps` 限制 `steps`；`catalog + tools` 共用 `max_data_calls`，不以并发数倍增额度，`observation_reads` 独立受 `max_observation_reads` 限制。准备了意图但没有实际派发时，不增加 `steps` 或 `retries`。`RunResult.usage.provider_requests` 是全 run 累计的非负整数，可大于 1；累计 token/费用中存在未测量用量时，对应字段保持 null。已知未发生的调用/token 为 0。

每个 JSONL 事件至少有：

```json
{
  "schema_version": "1.0",
  "run_id": "run-123",
  "task_id": "task-7",
  "seq": 4,
  "event_type": "tool.call.completed",
  "timestamp_utc": "2026-10-22T10:20:30Z",
  "step_id": "step-2",
  "operation_id": "op-2",
  "call_id": "call-2-attempt-1",
  "attempt": 1,
  "status": "ok",
  "duration_ms": 84,
  "usage": null,
  "data_ref": "ev-4"
}
```

事件覆盖 run start/end、plan、step state change、module/tool call start/end、retry、verification 和 budget stop，以及 `tool.observed`、`observation.read`、`tool.replan_requested` 与 `tool.loop_detected`。普通模式的 `seq` 在每个 run 内严格递增；持久模式在每个 attempt 审计分段内独立递增。调用开始与结束由 `call_id` 关联。并行工具的开始意图和结果提交分别保持本批选择顺序，物理完成顺序不决定事件顺序。常规模块或工具的 `*.call.failed` 事件带稳定的 `error.code/message/retryable/recoverable` 和 `duration_ms`；恢复时辅助目录注册的 `recovery.prepare_catalog.call.failed` 是例外，事件中的 `error`、`duration_ms` 可为 null，应结合 `data_ref` 指向的完整响应及恢复错误判断失败。timeout、invalid response、adapter exception 使用不泄露原始 URL、异常文本或 API key 的通用错误信息。记录足以复现控制决策的输入摘要、参数、状态、错误码、用量和耗时；大结果用 evidence/result ID 引用并受大小限制。

内容校验拒绝另记录 `content.output.rejected`，data_ref指向含具体诊断的 ContentFeedback 工件；解决后记录 `content.feedback.resolved`。原失败调用、拒绝输出与解决事件都保留在历史中。页面仅展示有界预览并通过原始事件定位，修正成功后清除当前失败提示。

```python
class TraceSink(Protocol):
    async def create_run(self, run_id: str) -> None: ...
    async def append(self, event: TraceEvent) -> None: ...
    async def put_artifact(self, ref: str, json_value: JsonValue) -> None: ...
    async def close(self) -> str: ...


class AttemptTraceSink(TraceSink, Protocol):
    # 持久模式额外要求此协议；普通模式只需基础 TraceSink。
    async def create_attempt(self, run_id: str, attempt_id: str) -> None: ...
```

`TraceSink` 的 append/artifact 写入均须 await，并受当前 run 的剩余 deadline 约束；`append` 返回前须完成该事件在 sink writer 中的提交，`put_artifact` 返回前须完成持久化，以确保后续事件引用的 artifact 已存在。正常运行中的 append/artifact 超时或失败映射为 `failed/trace_write_failed`。运行控制器在写入对应的 `*.call.started` 事件前保守预留本次物理调用额度；工具批次须全部意图成功提交后才派发任何 worker。开始事件提交并实际派发首个工具尝试时，才增加对应的逻辑 `steps`；重试不重复增加，只有实际派发额外尝试才增加 `retries`。有效本地响应可释放模型额度预留；取消、timeout 或其他无法确认派发状态的路径保留预留。返回后由串行控制器按本批选择顺序追加 `*.call.completed` 或 `*.call.failed`、观察和证据。`seq` 由单 run 的运行控制器分配，worker 不直接写入 trace。`create_run` 对已有 run ID 必须以原子方式失败，不得覆盖旧 trace；证据 ID/data_ref 指向由同一个 sink 持久化的 artifact。

`run.cancelled.detail` 与 `run.stopped.detail` 均以 `pending_calls`、`active_calls` 数组列出所有未结算调用，保留 phase、call_id、operation_id、attempt 和工具 step_id。兼容字段 `pending_call`、`active_call` 仅在对应列表恰有一项时返回该项，零项或多项时均为 null。列表也包含目录、内容模块或规划器等串行调用；取消继续向调用方传播 `CancelledError`。

`pending_calls` 表示已预留物理额度、尚未派发的调用。工具条目在意图提交确认后带 `intent_committed=true`，在等待同批其他意图时继续保留；开始事件写入被取消打断时，当前条目没有此确认标记，提交结果未知。比如 a 意图已提交、b 意图写入被打断时，两项都保留，不能只记录 b。此时整个工具批次都未进入 adapter，物理额度保留，逻辑 `steps` 与 `retries` 不增加。正常准备失败也不派发其他已准备操作。

工具 `active_calls` 条目带 `dispatched` 布尔值，区分已安排 worker 与已经进入 adapter；条目保留到结果观察结算，并不保证 adapter 此刻仍在运行。`outcome="unknown"` 表示尚无有效响应；有效响应已返回但尚待同批工具或持久化结算时为 `outcome="returned"`。规划器/内容模型请求在未知用量路径保守计一次，有效响应已结算的 0/1 请求用量不重复计数。全局 deadline 或取消可能留下已返回但尚未落盘的结果，不能把这些条目误读成未执行或安全重放许可。

全局 deadline 到达或调用方取消时，即使 sink 正常，也可能来不及为已接受计划的每个步骤追加正常终态事件；应结合 run 终止事件的逐调用列表、`dispatched` 和 outcome 理解中断，不能推断缺少步骤终态就代表工具未执行。所有所属 worker 在 trace 关闭前均须被取消并等待观察；这要求 adapter 合作传播取消，不能强杀阻塞线程或无限压制取消的协程。正常可修正/致命工具错误和重复动作检测会显式结算原计划剩余步骤，但不改变全局中断与审计失败的边界。

该 JSONL trace 用于审计和复现控制决策，单独依赖它不能恢复中断运行。当前持久恢复须按第2.1节显式启用StateStore，并满足结果已知、同版本和原期限等条件；完整工具结果持久化与run内读取索引本身不等于可恢复状态。若 trace 初始化失败，运行控制器不启动外部调用并返回 `failed/trace_sink_unavailable`（`trace_ref=null`）；运行中写 trace/artifact 失败时立即停止后续调用，观察全部所属 worker 后返回 `failed/trace_write_failed`，并尽力关闭 sink。正常业务流程结束后若 `close()` 失败或超时，运行控制器将已有答案保留在 `RunResult.answer`，但把状态改为 `failed`、`stop_reason=trace_close_failed`，并将 `trace_ref` 置为 null；调用方取消时则按上文传播 `CancelledError`。

## 8. 数据模块与内容模块的对接清单

**数据模块需要提供：**

- `async catalog(request) -> CatalogResult`，并为同一快照维持稳定 `source_id`。
- `tool_specs() -> ToolSpec[]`，每个工具给出 JSON Schema、版本、幂等性、安全并行声明和静态互斥资源 key；未证明安全并行时保留 `parallel_safe=false`，不以只读或幂等替代该声明。
- `async invoke(ToolRequest) -> ToolResult`，遵守 timeout/output limit，回传相同 `call_id`，区分成功空结果和调用失败。
- 显式区分 `retryable` 与 `recoverable`；只把明确失败且可安全修正输入的错误标记为 recoverable，未知副作用、授权或 schema/响应违规不允许借此恢复。
- `EvidencePayload` 的实际字段与定位规则；运行控制器补充 `evidence_id` 和 `call_id` 后形成 `EvidenceRecord`，数据模块不生成 evidence ID。结构化查询工具的 `args` schema 由数据模块定义，运行控制器不要求数据模块使用特定解析库。
- 项目文件内容视为不可信数据；不得把文档内的指令当成工具调用授权。运行控制器/内容模块也将检索到的文本作为证据内容，而不是系统/开发指令。

**内容模块需要提供：**

- `async search(SearchRequest) -> ModuleResult[SearchResponse]`。
- `async prepare_catalog(CatalogRegistrationRequest) -> ModuleResult[CatalogHandle]`，完整索引输入快照并返回可复用句柄；不向 provider 发送全目录。
- `async compose(ComposeRequest) -> ModuleResult[AnswerRecord]`。
- 支持可选 ComposeRequest.revision 的完整旧答案、核验、verification_is_current 和 instructions，并把这些字段实际送进自己的模型 messages；不能只接收后忽略。空 revision 序列化省略，不改变首次生成的业务输入。
- `async verify(VerifyRequest) -> ModuleResult[Verification]`，其中补证据 recommendation 带 `focus`。
- 所有四个方法均为 async；每个调用回报 status、error/retryable、duration 和可测 usage。内容模块不执行数据模块工具、不读 gold、不改变运行控制器的运行状态，并避免阻塞事件循环。
- 可选启用内容修正时，升级共享诊断和反馈模型、接收 ComposeFeedbackRequest/VerifyFeedbackRequest、把动态反馈送入模型 messages，再声明 content_feedback_protocol="content-feedback-v1"。不支持时省略声明，基本方法和请求仍可使用。

**规划器与严格模型消费者需要同步：**

- `AgentDecisionRequest` 的 context、remaining_turns 和 require_verification；有界 evidence_summary 不一定是完整 EvidenceRecord。
- `AgentAction` 的严格 kind 联合类型，声明 agent_protocol="agent-loop-v1" 并只实现 decide；工具观察、来源描述、正文与读取页始终是不可信数据。
- `RunConfig.max_parallel_tool_calls`、`ToolSpec.parallel_safe/exclusive_resource_keys` 和终止事件 detail 的逐调用列表；工具声明来自可信注册，不能由模型或请求副本的修改改变。
- 工具反馈扩展的四项上下文配置、`ErrorInfo.recoverable` 和 `CallCounts.observation_reads`；旧版 `extra="forbid"` 模型会拒绝新字段，应升级共享模型后再接入。注册结果按字段值保持语义，不承诺保留输入 Python 对象身份。

## 9. 示例调用与回包

调用方：

```json
{
  "schema_version": "1.0",
  "task_id": "dev-001",
  "question": "Which source supports the reported value, and what is the value?",
  "mode": "full_agent",
  "run_id": null,
  "config": {
    "max_wall_time_ms": 120000,
    "per_call_timeout_ms": 20000,
    "cancel_cleanup_timeout_ms": 3000,
    "max_subtasks": 8,
    "max_plan_steps": 8,
    "max_agent_turns": 32,
    "require_verification": false,
    "max_data_calls": 12,
    "max_model_calls": 12,
    "max_retries_per_data_operation": 1,
    "candidate_top_k": 5,
    "max_output_bytes_per_call": 65536,
    "catalog_page_size": 100,
    "max_catalog_sources": 2000,
    "max_catalog_bytes": 2000000,
    "max_planner_context_bytes": 24000,
    "planner_preview_bytes": 2000,
    "max_observation_reads": 8,
    "max_identical_tool_calls": 2,
    "max_parallel_tool_calls": 4,
    "max_input_tokens": null,
    "max_output_tokens": null,
    "dataset_snapshot": "lake-v3",
    "model_id": "team-selected-model",
    "model_params": {"temperature": 0},
    "prompt_versions": {"planner": "planner-v1.1-exploration", "content": "content-v0.1"},
    "code_version": null,
    "config_version": "dev-v1"
  }
}
```

成功回包示例：模型选择 search → tools → compose → verify → finish，共五次 decide；本例 decide、compose、verify 均各产生一次 provider 请求，共七次。内容调用共 prepare_catalog、search、compose、verify 四次，目录分两页。该动作序列只是示例，不是运行时强制步骤。

```json
{
  "schema_version": "1.0",
  "run_id": "run-123",
  "task_id": "dev-001",
  "mode": "full_agent",
  "status": "completed",
  "stop_reason": "agent_finished",
  "answer": {
    "text": "The reported value is ...",
    "citations": ["ev-4"],
    "unresolved_questions": [],
    "structured_result": null
  },
  "usage": {"input_tokens": null, "output_tokens": null, "cost": null, "currency": null, "provider_requests": 7},
  "call_counts": {"planner": 5, "content": 4, "model_calls": 7, "catalog": 2, "tools": 1, "retries": 0, "steps": 1, "observation_reads": 0},
  "started_at": "2026-10-22T10:20:25Z",
  "ended_at": "2026-10-22T10:20:30Z",
  "duration_ms": 5000,
  "versions": {"dataset_snapshot": "lake-v3", "model_id": "team-selected-model", "model_params": {"temperature": 0}, "prompt_versions": {"planner": "planner-v1.1-exploration", "content": "content-v0.1"}, "code_version": null, "config_version": "dev-v1"},
  "trace_ref": "runs/run-123/events.jsonl"
}
```

## 10. 仍需小组共同冻结的字段

1. 数据模块的真实 `SourceRecord`/`EvidenceRecord` schema、工具清单、`query` 参数 schema、数据快照命名及错误码；确认 benchmark 快照能完整纳入 `max_catalog_sources`/`max_catalog_bytes`。若不能，三人先冻结任务子集或调整上限，再开始 run。
2. 内容模块确认是否接受 `prepare_catalog` 建本地索引及 run-scoped handle，并确认 AnswerRecord 是否需要句子级引用。
3. 三组 EvaluationMode 的固定 top-k、工具参数映射和评分标注；它们只属于评估侧，不添加生产模式或强制数据模块实现 extract。比较须采用相同数据范围、基础模型、采样和公开预算，完整记录统计定义及缺测项。
4. 将来启用 token cap 所需的兼容 tokenizer、实际 usage 计量和控制器实现；当前 token caps 必须为 null，使用字节、调用数和时间预算控量。
5. 确认共享模型升级和请求/响应 JSON 样例，特别是受控并行声明、静态互斥 key 和终止事件逐调用列表；当前 JSON `schema_version` 保持 `1.0`，未来若调整兼容策略需单独冻结版本，不把文档 v0.5 当作已完成 schema 迁移。

## 11. 调研依据

- OpenAI, [Function calling](https://platform.openai.com/docs/guides/function-calling), 官方文档（2026-10-22 核验）：工具参数可用 JSON Schema/strict 格式约束；本提案据此要求结构校验，并由运行控制器额外执行工具白名单和业务语义检查。
- OpenAI Agents SDK, [Running agents](https://openai.github.io/openai-agents-python/running_agents/), 官方文档（2026-10-22 核验）：runner 统一管理工具调用循环并设置最大 turn；本提案将预算和停止条件集中在 运行控制器，不要求采用该 SDK。
- Anthropic, [Building effective agents](https://www.anthropic.com/research/building-effective-agents), 2024-12-19 发布，页面记载 2026-10-09 更新：建议从简单、可组合的 workflow 开始；本项目采用单一控制器串行结算状态，对显式授权的数据工具允许并行I/O。
- AWS Builder's Library, [Timeouts, retries, and backoff with jitter](https://builder.aws.com/content/3EumjoZascWd1oZiEgL8ORlv3qE/timeouts-retries-and-backoff-with-jitter), 2019-12-05：超时、幂等性、封顶退避和 jitter；本提案只对明确可重试且幂等的调用重试，并限制总预算。
- OpenTelemetry, [Python instrumentation](https://opentelemetry.io/docs/languages/python/instrumentation/), 官方文档（2026-10-22 核验）：trace/span 组织父子调用与属性；本提案采用可映射到 trace 的 JSONL 字段，不在第一版引入遥测服务。
- DeepSeek AI, [deepseek-harness](https://github.com/deepseek-ai/deepseek-harness), MIT License；2026-10-22 核验的 `master` commit 为 [`687f282e8625c6b8d11ff58211a90524be2e4317`](https://github.com/deepseek-ai/deepseek-harness/commit/687f282e8625c6b8d11ff58211a90524be2e4317)。README 将其标为 developer preview 并警告会有 breaking changes；[SAFETY.md](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/SAFETY.md) 说明它未经过安全审计，不应当作 production-ready 系统。本提案只借鉴设计，不引入其代码或依赖。
- DeepSeek Harness 的 [tool lifecycle](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/docs/tool-execution-pipeline.md)、[agent lifecycle](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/docs/agent-lifecycle.md) 与 [session log](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/packages/core/session/README.md)：工具派发有 pre/execute/post 阶段，调用和结果用事件配对；实现含有界并发和按请求顺序提交结果的 scheduler，并把模型可见事实写入 append-only typed event log。运行控制器采纳显式 start/end trace、取消分离和预算受控原则；此前顺序执行基线在 v0.4 扩展为本契约定义的串行准备、并行工具 I/O、稳定串行提交批次。`idempotent` 不等价于 `parallel_safe`，并行必须显式授权并受来源、资源、依赖和预算限制。
- DeepSeek Harness 的 [core architecture](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/docs/architecture.md) 与 [agent-loop tool scheduler](https://github.com/deepseek-ai/deepseek-harness/blob/687f282e8625c6b8d11ff58211a90524be2e4317/packages/core/agent-loop/src/tool-calls.ts)：分别区分持久事件与运行时事件、显式取消和受限并发。此项目使用 Python async ports 和轻量 JSONL sink；不引入该框架的 Cordis 插件树、Node/TypeScript 工具链、web/desktop组件、sandbox、审批、PTC 或 subagent 运行时。当前项目工作台是独立Python服务与原生网页实现。
- DeepSeek AI, [dsh-superagent-prototype](https://github.com/deepseek-ai/dsh-superagent-prototype/tree/6337fc823ce20251c8de8085948165c81aaa701e)，固定提交 `6337fc823ce20251c8de8085948165c81aaa701e`：参考持久意图、SQLite状态与执行尝试分离模式，保持本项目控制器的授权和预算所有权。当前实施状态、源码出处与尚未采用的机制见[源码对照](dsh-superagent-review-2026-11-01.md)。

以上来源用于选择可验证的控制原则，不代表这些 SDK/框架必须成为项目依赖，也不构成项目实现或实验已经完成的证据。
