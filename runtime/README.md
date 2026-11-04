# 异构数据池 Agent Harness

这是 Python 3.11+ 的异步运行控制框架，面向通用异构数据池。运行控制器校验计划、工具、来源和预算，实际数据处理与答案生成由适配器负责。端口契约见 [runtime-contract.md](docs/runtime-contract.md)。

## 按任务阅读

| 目标 | 入口 |
|---|---|
| 首次配置或了解待接入模块 | [配置与工厂](docs/configuration.md) → [接口契约](docs/runtime-contract.md) |
| 理解实际执行路径与完成条件 | [工作流说明](docs/langgraph-workflow.md) → [源码目录与阅读顺序](docs/source-layout.md) |
| 排查错误引用、输出格式或答案修订 | [内容反馈与修订](docs/content-feedback.md) |
| 理解中断后能否继续、哪些调用不会重发 | [持久恢复](docs/durable-recovery.md) |
| 执行三组受控对照或查看报告边界 | [消融评估](docs/ablation-evaluation.md) |
| 使用任务历史、来源页和导出报告 | [工作台说明](docs/project-workspace.md) |

## 配置与启动

产品入口读取服务端 TOML 配置，不携带合成数据、固定回答、固定核验或默认演示配置。

1. 在本目录安装依赖：`uv sync --extra dev --extra web --frozen`。
2. 将 [config.example.toml](config.example.toml) 复制为私有 `config.toml`，填写真实 API 地址、模型名和密钥来源。
3. 安装实际数据与内容模块，将 `[adapters.data/content].factory` 改为它们可导入的 `module:function` 工厂，并填写私有 options。
4. 准备好模块与配置后，手动执行：

```bash
uv run --extra web --frozen lake-agent-web --config config.toml
```

打开配置的本机地址，默认 http://127.0.0.1:8765 。`--port`、`--data-dir` 可覆盖配置。配置中的存储路径相对于配置文件解释；显式命令行路径相对于启动目录解释。保持终端运行，`Ctrl+C` 关闭。

配置缺失、凭据缺失、字段非法或工厂无法导入时，命令在启动前退出，不回退到测试替身。完整配置、密钥读取、工厂签名与客户端所有权见 [configuration.md](docs/configuration.md)。当前仓库未提供待接入模块的生产 DataPort/ContentPort 实现，具体实现与凭据必须由实际集成方提供。

待接入模块未就绪时，可以在源码目录独立测试运行时。配置好 `[llm]` 和密钥后，手动运行：

```bash
uv run --extra web --frozen python -m tests.isolated_runtime --config config.toml --web
```

此入口使用真实控制器；规划、回答生成和核验实际调用配置中的 LLM。数据和本地检索由测试依赖提供，无需填写待接入模块的实际工厂；回答接收当前问题和证据，不再使用固定答案或固定成功核验。合成资料和测试适配器身份明确标注，记录保存于独立目录。命令行单问题方式与检查步骤见 [隔离测试说明](docs/configuration.md#待接入模块未就绪时的隔离测试)。

## 模块职责

生产代码按职责归入 `engine/`、`adapters/` 和 `web/`；独立的 `evaluation/` 只定义评估报告格式。实验执行器与合成测试端口保留在 `tests/`，不会打包进生产 wheel。完整目录、文件职责和迁移关系见 [源码目录说明](docs/source-layout.md)。

| 组件 | 职责 |
|---|---|
| DataPort | 目录快照、可信工具注册、数据处理与工具调用 |
| ContentPort | 本地目录准备和检索、答案组合、核验 |
| AgentPlannerPort | 通过 decide 提出下一动作；配置入口使用真实 JsonPlanner |
| TraceSink | 请求/响应审计及完整结果工件 |
| StateStore | 可选持久记录、已知响应与恢复协调 |
| 运行控制器 | 来源授权、预算、超时、重试、调度和证据提交 |

三类适配器通过 [ports.py](src/lake_agent/ports.py) 的进程内异步协议连接，不要求分别部署 HTTP 服务。数据和内容工厂以 `factory(*, options: dict, llm: LLMSettings)` 返回异步上下文管理器，每次 run/resume 获得独立端口并负责清理。规划器按配置构造，向 `{base_url}/chat/completions` 发送一次异步请求，不自动重试。

`JsonPlanner` 的 `decide()` 选择下一步行动，不执行工具或替内容模块生成答案，只提供这一种模型决策接口。最终回答和核验需要真实 ContentPort；不得将真实规划器配合测试内容的运行描述为完整 Agent 已联调。模型名、采样参数只在 `[llm]` 声明，`[run]` 复用 [RunConfig](src/lake_agent/models.py) 的预算字段。

## 执行、恢复与工作台

公开执行入口为 [runtime.py](src/lake_agent/runtime.py) 的 `run/resume`。[engine/graph_runner.py](src/lake_agent/engine/graph_runner.py) 负责共享的图编译、追踪隔离、取消传播和资源收尾，承接原 `workflow.py` 中仍在使用的执行机制；唯一运行图由 [engine/agent_workflow.py](src/lake_agent/engine/agent_workflow.py) 实现 `decide → action → decide` 循环。模型可选择搜索、工具、观察读取、目录分页、可选计划、生成、核验或结束，不强制分解或检索。控制器负责授权、预算和结果接纳。旧子任务图、两阶段规划 API 和自动协议回退已经删除。图层不额外启用重试、缓存或第二套 checkpointer，恢复以 SQLite 已验证的调用日志为准。

`[run].max_agent_turns` 默认 32，约束自主决策轮数；`require_verification` 默认 false，允许未核验完成，由模型选择是否核验。设为 true 时，必须存在适用于当前完整答案、证据列表和运行政策的 supported 核验。`finish` 是模型的完成提议，由控制器按当前政策接受；`full_agent` 名称本身不启用强制核验。空证据允许生成目录说明、澄清或资料不足说明，不允许伪造引用。`completed`、内部核验通过、外部评测正确是不同概念，均不能互相替代。已实现有界规划上下文、已知观察分页、可信工具安全并行、有界重试和重新规划。工具结果与可引用证据分离；只有通过结构和来源检查的记录可作为答案引用。详细限制见 [接口契约](docs/runtime-contract.md) 与 [工作流说明](docs/langgraph-workflow.md)。

复杂任务的提示鼓励模型将可选 DAG 计划作为工作记忆，随进展更新完成项；结构未知时先检查目录、schema 或观察分页，再构造过滤与聚合。核验失败后依据 issues/recommendation 定向补证或修改答案，动作顺序仍由模型选择。

核验绑定完整可信输入的确定性指纹：接纳不同答案时清除旧核验收据，状态为 missing；答案未变而证据变化时，旧收据为 stale。只调用工具但未改变验收相关状态不会自动要求再核验。过期只表示旧意见不再适用于当前状态，不表示当前答案已被证明错误。`compose` 的可选 `revision` 向无状态内容模块提供旧答案、相关核验及其新鲜度和修改要求，规划器可通过 `revision_instructions` 指定目标；这些信息进入模型消息，不能改变权限、预算或验收政策。待接入的生产内容模块需要升级共享模型并实现该字段，仓库只修改了自身隔离适配器。

生成或核验出现非法引用、目录来源越界或模型正文 JSON/schema 错误时，显式声明 `content_feedback_protocol="content-feedback-v1"` 的内容端口可进入自主修正：控制器拒绝当前输出、保存有界诊断，规划器决定下一步；再次调用内容端口时，扩展请求把具体反馈送到模型 `messages`。原问题、证据和 HTTP 顶层参数不混入错误信息，每次调用仍最多请求一次模型。未声明能力的内容端口不接收 feedback 扩展并保留原失败语义，但参与答案修订仍需支持基础请求中的可选 revision；鉴权/传输/契约错误保持停止。无需增加 TOML 配置，协议和恢复边界见[内容错误诊断与修正](docs/content-feedback.md)。

日常交互的 `require_verification=false` 允许模型选择核验；Proposal 消融中的 `full_agent` 必须使用 `require_verification=true`，`no_verifier` 使用 false 并禁止核验动作。固定 top-k 一次生成基线仅由 `tests/support/ablation.py` 执行；评估报告仍可比较三组结果，任务标注、指标和执行命令见 [消融评估说明](docs/ablation-evaluation.md)。

| 阅读入口 | 内容 |
|---|---|
| [ports.py](src/lake_agent/ports.py) / [models.py](src/lake_agent/models.py) | 模块接入接口、动作和运行配置 |
| [engine/agent_workflow.py](src/lake_agent/engine/agent_workflow.py) | 模型动作与图路由 |
| [engine/graph_runner.py](src/lake_agent/engine/graph_runner.py) | 图执行、取消及追踪隔离 |
| [runtime.py](src/lake_agent/runtime.py) / [engine/tool_runtime.py](src/lake_agent/engine/tool_runtime.py) | 授权、预算与工具执行 |
| [engine/recovery.py](src/lake_agent/engine/recovery.py) | 持久恢复与已知响应重放 |
| [engine/content_feedback.py](src/lake_agent/engine/content_feedback.py) | 有界输出诊断、引用差异与内容反馈能力识别 |
| [adapters/planner.py](src/lake_agent/adapters/planner.py) | 真实模型请求、策略提示与动作解析 |
| [evaluation/models.py](src/lake_agent/evaluation/models.py) | 三组评估报告格式；不执行基线 |

正式工作台支持完整 Agent 提问、来源目录、历史、真正删除任务及 JSON/HTML 报告。关闭核验与评估导入仅由测试宿主显式开启；正式入口默认不构造评估仓库、不创建评估目录。固定检索只在测试评估脚本中作为基线，不是应用运行模式。浏览器不接收凭据、模块导入路径或预算覆盖；配置与工厂留在服务端。来源页只展示注册目录，不等于读取了数据池全文；测试评估页只显示导入的实际结果，未知指标保持 null。页面和 HTTP 接口见 [工作台说明](docs/project-workspace.md)。

失败结果可携带 `failure.phase/call_id/error`，页面展开具体字段差异、允许的证据 ID 和被拒输出预览；旧历史从已有失败事件回退展示，不补造未保存内容。修正成功后清除当前错误，历史事件保留；未解决内容反馈不能直接 finish，强制核验要求当前有效的 supported 核验。

`run(..., state_store=store)` 与 `resume(run_id, ...)` 保留原请求、期限和累计预算。历史已知响应不重新派发；started 但没有有效 completed 响应的调用结果未知，会拒绝自动恢复。连接配置变化也会阻止旧任务恢复。详见 [恢复说明](docs/durable-recovery.md)。

## 离线验证

```bash
uv run --extra dev --extra web --frozen pytest -q
uv run --extra dev --extra web --frozen ruff check src tests scripts
uv run --extra dev --extra web --frozen ruff format --check src tests scripts
node --check src/lake_agent/web/static/app.js
uv build --offline
uv run --frozen python scripts/verify_installed_web.py --report runs/installed-package-check.json
```

测试按契约、适配器、执行引擎、恢复、工作台和评估分组；目录职责、共享 fixture 与分组命令见 [测试导航](tests/README.md)。测试替身与共享请求构造器位于 `tests/support/`，不打包到生产 wheel。测试通过内存 HTTP transport 检查真实规划器的 endpoint、Authorization、模型和输出解析，同时覆盖真实控制器、SQLite、审计及配置失败行为；不访问外部模型，不启动监听服务。安装验证也仅检查隔离安装、配置、资源和 CLI，不启动应用。历史的合成场景在线专项和固定旧版本脚本已移除。

API 凭据是否可用、提供方响应兼容性及真实数据答案质量仍需实际模块联调。历史版本证据保留在 [2026-11-02 验收记录](docs/project-workspace-acceptance-2026-11-02.md)，不作为本次改动的实时验证。

## 运行边界

支持 macOS/Linux/WSL，工作台与 SQLite 恢复依赖同一 POSIX 主机的可信本地文件系统。应用只监听回环地址，不提供登录、多用户权限或公网部署。共享适配器必须可重入；每次尝试使用独立 trace。

当前不支持累计 token 硬预算、跨版本恢复、动态 worker 委派或外部副作用 exactly-once。未测量 token/cost 保持 null。超时与取消依赖适配器合作，不能强制终止阻塞线程或忽略取消的协程。完整语义仍以接口契约为准。
