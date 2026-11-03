# 异构数据池 Agent Runtime

让 Agent 根据自然语言问题发现相关数据，选择工具处理不同格式的资料，汇总多来源证据，并返回带引用的答案。Harness 面向通用异构数据池；LakeQA 只是评测数据集。

本仓库只维护 `runtime/` 中的运行时代码、配置、测试和必要的接入文档。课程提案、截图、运行记录与生成报告不纳入同步。

## 当前交付状态

Python 包 **0.7.0**，核心接口契约 **v0.5**，JSON 封套 **1.0**。

| 部分 | 当前状态 |
|---|---|
| 运行控制 Harness | LangGraph负责实际流程节点与分支；已实现异步控制、工具授权、预算/超时/重试、有界上下文、受控工具并行、证据校验和可选 SQLite 恢复 |
| 项目工作台 | 已实现智能问答、数据来源、任务历史、评估导入四页，支持执行记录、恢复及 JSON/HTML 导出 |
| 模型规划 | `JsonPlanner` 支持 API 根地址、API key 和模型名，通过异步 Chat Completions 生成计划 |
| 数据处理与内容模块 | 已定义接口并提供离线示例；队友的真实 DataPort、ContentPort 尚待接入 |
| 实际实验 | 尚待真实模块联调、固定任务集测量和结果导入 |

当前可运行配置使用明确标注的合成数据和离线规则。`JsonPlanner` 只负责规划，真实检索、最终答案生成与核验由 ContentPort 提供。测试、安装验证与浏览器检查的范围见[验收记录](runtime/docs/project-workspace-acceptance-2026-11-02.md)。

## 五分钟启动

环境：**Python 3.11+、uv、macOS/Linux/WSL**。SQLite 恢复与工作台依赖 POSIX 文件锁，原生 Windows 暂不支持；Windows 同学请在 WSL 中运行。先按 [uv 安装说明](https://docs.astral.sh/uv/getting-started/installation/)安装 uv。

以下命令从**仓库根目录（包含本 README 和 runtime 文件夹）**执行：

```bash
cd runtime
uv sync --extra dev --extra web --frozen
uv run --extra web --frozen lake-agent-web --data-dir runs/project-workspace
```

打开 http://127.0.0.1:8765 ，选择标准、并行或安全恢复对应的示例配置。默认“项目数据池”尚未接入，暂时不能提交真实任务。示例不需要 API key、不调用外部模型。保持终端运行，`Ctrl+C` 关闭；重新启动时使用相同的 `--data-dir` 可查看原历史。

组员可以自行启动副本，或直接打开下载的“任务报告”HTML 查看结果。`127.0.0.1` 只对启动者本机可见。

## 队友如何并行接入

三部分通过同一进程内的 **Python 异步端口**连接，不要求分别部署 HTTP 服务。直接导入[共享模型 models.py](runtime/src/lake_agent/models.py)和[端口 ports.py](runtime/src/lake_agent/ports.py)，不要手抄另一套字段。

| 模块 | 需要实现的入口 | 输入与输出的职责 |
|---|---|---|
| 数据处理 | `DataPort.catalog / tool_specs / invoke` | 目录及稳定来源 ID、可信工具 schema、授权范围内的工具执行结果和可定位证据片段。`tool_specs()` 是同步本地注册，其余方法异步 |
| 检索与回答 | `ContentPort.prepare_catalog / search / compose / verify` | 注册目录并回传快照/digest、搜索候选来源、根据已接受证据生成带引用答案、核验并提出结构化反馈；四个方法均异步 |
| Harness 与工作台 | `run / resume`、`PlannerPort`、Web 注册 | 规划、授权、预算、调度、证据身份、恢复、审计和前端；现有代码已提供 |

完整输入、输出、错误与调用约束以[接口契约](runtime/docs/runtime-contract.md)为准。接口已与实现逐项核对；契约描述规范化对象，调用方可省略模型中有默认值的字段。

建议每个模块先做一个可运行 adapter 和一个小测试：

1. **数据模块**：用少量混合格式样本验证 `catalog → invoke`，确认来源 ID 稳定、参数满足注册 schema、片段有 locator。数据模块的物理调用与耗时由 Harness 记录。先与现有 `DemoContent`、`DemoPlanner` 测控制流程，需要使用它们支持的工具契约；真实自定义工具可配一个最小确定性 planner。
2. **内容模块**：先用固定 `SourceRecord` 和证据测 `prepare_catalog/search/compose/verify`，确认快照/digest 一致，引用仅使用输入中的 evidence ID；本地阶段返回 `Usage.local()`，模型阶段如实回报请求和用量。再用 `DemoData` 和可匹配的确定性 planner 联调。
3. **一起接入**：在服务端 `ExecutionProfile` 的异步 factory 中构造 DataPort、ContentPort 和 PlannerPort，按每次 run/resume 管理并关闭客户端。可直接复制[工作台说明中的完整注册示例](runtime/docs/project-workspace.md#同学如何接入模块)，再替换示例端口。

每个人只需交付自己的适配器、测试、依赖和一条运行命令。接入失败时提供 `run_id`、错误码和已脱敏的执行记录即可定位；不需要先改动 Harness 内部。真实 adapter 不应通过隐藏重试或后台请求绕开统一预算。适配器行为、连接目标或数据版本变化时更新 `metadata.revision`。

## 最小联调验收

- 在同一数据快照和一组小问题上跑通真实 `full_agent`：目录、计划、检索、工具、证据、回答与核验均来自实际模块；检查引用能回到原文件位置。
- 验证空检索、非法工具参数、网络超时和主动取消能得到规范停止；已知结果恢复不重复调用工具，未知结果拒绝自动恢复。
- 固定任务集、模型参数、提示词版本和预算后运行三种模式。真实测量后再导入评估；未测的质量、token、费用保持 `null`。

示例运行成功只证明控制流程可用，不能作为真实数据池质量或性能成绩。旧版本运行暂不支持跨版本恢复。

## 代码与文档入口

| 入口 | 内容 |
|---|---|
| [运行时 README](runtime/README.md) | Python 调用、模型 API 配置、预算、测试与运行限制 |
| [核心接口契约](runtime/docs/runtime-contract.md) | 所有模块共享字段、样例与错误语义 |
| [LangGraph流程](runtime/docs/langgraph-workflow.md)、[运行控制器](runtime/src/lake_agent/runtime.py)、[工具调度](runtime/src/lake_agent/tool_runtime.py) | 图编排、安全控制与受控并行执行 |
| [规划器](runtime/src/lake_agent/planner.py) | OpenAI-compatible API 规划与严格结果解析 |
| [恢复说明](runtime/docs/durable-recovery.md) | SQLite 状态、调用意图、恢复和未知结果边界 |
| [工作台说明](runtime/docs/project-workspace.md) | 页面操作、模块注册、HTTP 路由、评估格式 |
| [验收记录](runtime/docs/project-workspace-acceptance-2026-11-02.md) | 当前验证与真实集成边界 |
| [公开源码对照](runtime/docs/dsh-superagent-review-2026-11-01.md) | 恢复设计参考与采用范围 |

## 本地复核与上传范围

在 `runtime` 目录执行：

```bash
uv run --extra dev --extra web --frozen pytest -q
uv run --extra dev --extra web --frozen ruff check src tests scripts
uv run --extra dev --extra web --frozen ruff format --check src tests scripts
uv build
uv run --frozen python scripts/verify_installed_web.py --report runs/installed-web-check.json
```

离线测试不发外部模型请求；安装验证默认从本机缓存装依赖，冷缓存时需加 `--allow-downloads`，仍不请求模型。只有 `scripts/live_*` 是额外的付费联网专项，运行前需检查其参数及服务限制。

根目录仅跟踪本 README、`.gitignore` 与 `runtime/`。其中提交源代码、`pyproject.toml`、`uv.lock`、测试、脚本和必要的接入文档；提案、截图、生成的验收 JSON、虚拟环境、运行历史、构建包、系统文件和本地密钥配置均不提交。API key 只从服务端环境或本地私密配置读取；`.gitignore` 不替代提交前检查。当前网页没有登录、多用户权限或公网部署能力。
