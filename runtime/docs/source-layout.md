# 当前源码目录与职责

`src/lake_agent/` 只包含可发布的正式能力。唯一生产执行引擎在 `engine/`，真实模型接入在 `adapters/`，本地工作台在 `web/`；`evaluation/` 只提供评估报告契约。实验策略、合成资料、隔离运行器和评分脚本位于仓库的 `tests/`，不会打包进生产 wheel。

## 推荐阅读顺序

首次阅读主执行链时，按 `ports.py / models.py → runtime.py 的 run/resume → engine/agent_workflow.py → runtime.py 的控制器方法` 前进：端口定义边界，图选择路由，控制器完成校验与状态提交。需要理解模型实际收到什么，再读 `engine/agent_context.py` 和 `adapters/planner.py`。

核验与修订沿 `engine/agent_workflow.py` 的 `compose/verify/finish → runtime.py` 的 `_compose/_verify/_verification_status → engine/verification_state.py` 阅读；非法输出诊断另外进入 `engine/content_feedback.py`。合法核验意见与被拒输出反馈使用不同请求字段，不要把两条通路当成同一种重试机制。

只有排查相应边界时才继续读 `engine/tool_runtime.py / execution.py` 的工具调度、`engine/recovery.py / state.py` 的持久恢复，或 `web/configuration.py → service.py → storage.py` 的工作台生命周期。下列表格用于按职责定位，不要求从头逐文件阅读。

## 目录总览

```text
runtime/
  config.toml                      # 本地私有配置，不入库
  config.example.toml              # 可共享配置模板
  src/lake_agent/
    models.py / ports.py            # 共享模型与异步端口契约
    runtime.py                     # 公共 run/resume 与唯一运行控制器
    config.py                      # 配置解析、密钥来源与模型参数校验
    state.py / trace.py             # SQLite 状态与 JSONL 审计
    validation.py                  # 共享的结构、来源与 DAG 校验
    engine/                        # 正式模型自主循环与执行机制
    adapters/                      # 正式模型适配器
    evaluation/                    # 独立评估报告格式，不运行基线
    web/                           # 工作台、任务管理和报告存储
  tests/
    README.md                      # 分组导航、共享支持代码和离线命令
    isolated_runtime.py            # 运行时隔离测试入口
    ablation_runtime.py            # Proposal 三组对照入口
    contracts/                     # 共享模型与接口契约
    adapters/                      # 模型适配、配置读取与工厂接线
    engine/                        # 动作循环、执行和上下文
    recovery/                      # SQLite、审计尝试与恢复
    web/                           # 工作台策略、生命周期和展示
    evaluation/                    # 消融运行、指标与报告边界
    support/                       # 合成端口、固定策略和评估统计
  scripts/
    verify_installed_web.py        # wheel 隔离安装与入口检查
```

## 保留的公共模块

这些路径保持原样，待接入模块实现 DataPort/ContentPort 或宿主调用运行时不需要依赖 `engine/` 内部状态。

| 文件 | 职责 |
|---|---|
| [__init__.py](../src/lake_agent/__init__.py) | 包说明和发布版本 |
| [models.py](../src/lake_agent/models.py) | 各模块共享请求、响应、动作、运行配置和结果；生产 `RunMode` 只有 `full_agent`、`no_verifier` |
| [ports.py](../src/lake_agent/ports.py) | DataPort、ContentPort、AgentPlannerPort 与 TraceSink 异步协议 |
| [runtime.py](../src/lake_agent/runtime.py) | 公共 `run/resume`；每次运行的控制器、授权、预算、调用结算和证据提交 |
| [config.py](../src/lake_agent/config.py) | 严格读取 TOML、解析密钥来源、校验模型和运行配置；不创建客户端或执行模块 |
| [state.py](../src/lake_agent/state.py) | StateStore 契约及 SQLite 实现、独占租约、已知调用日志与删除 |
| [trace.py](../src/lake_agent/trace.py) | JsonlTraceSink、审计工件与独立执行尝试；内存测试接收端保留在 tests 中 |
| [validation.py](../src/lake_agent/validation.py) | 规范 JSON、目录摘要、可信工具声明、参数、来源和依赖校验 |

典型集成继续使用：

```python
from lake_agent.models import RunRequest
from lake_agent.ports import ContentPort, DataPort
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink
```

## 正式引擎与模型适配器

| 文件 | 职责 |
|---|---|
| [engine/__init__.py](../src/lake_agent/engine/__init__.py) | 内部生产引擎的边界说明 |
| [engine/agent_workflow.py](../src/lake_agent/engine/agent_workflow.py) | 唯一 `decide → action → decide` 图、动作接纳和结束门禁 |
| [engine/graph_runner.py](../src/lake_agent/engine/graph_runner.py) | 图编译、步数限制、外层追踪隔离、取消传播和退出收尾 |
| [engine/agent_protocol.py](../src/lake_agent/engine/agent_protocol.py) | 检查唯一支持的 `agent-loop-v1` 声明与 decide 能力，不选择旧流程 |
| [engine/agent_context.py](../src/lake_agent/engine/agent_context.py) | 为模型决策构造有界工作上下文、目录与观察页 |
| [engine/context.py](../src/lake_agent/engine/context.py) | 观察和证据预览、JSON Pointer 分页等纯函数 |
| [engine/content_feedback.py](../src/lake_agent/engine/content_feedback.py) | 内容修正能力识别、非法引用位置、允许 ID 和被拒输出的有界诊断；不请求模型 |
| [engine/verification_state.py](../src/lake_agent/engine/verification_state.py) | 完整核验输入与运行政策的确定性指纹、控制器私有核验收据；不读取上下文预览或尝试元数据 |
| [engine/tool_runtime.py](../src/lake_agent/engine/tool_runtime.py) | 控制器工具批次、预算预留、重试、观察和证据结算 |
| [engine/execution.py](../src/lake_agent/engine/execution.py) | 单次工具等待与结果封装、可信并行批次选择；不自行决定预算或重试 |
| [engine/recovery.py](../src/lake_agent/engine/recovery.py) | 持久运行准入、实现指纹、调用响应重放和继续派发 |
| [engine/errors.py](../src/lake_agent/engine/errors.py) | 内部受控停止异常与致命工具错误码 |
| [adapters/__init__.py](../src/lake_agent/adapters/__init__.py) | 正式模型适配器的边界说明 |
| [adapters/planner.py](../src/lake_agent/adapters/planner.py) | JsonPlanner：真实异步 HTTP、探索规划提示、严格动作解析与客户端生命周期 |

原顶层 `workflow.py` 中的共享执行机制移动并改名为 `engine/graph_runner.py`，避免与唯一动作图混淆。旧固定子任务状态机已删除；目录整理没有把它保留成另一种生产流程。模型自由选择行动，确定性图和控制器负责边界约束。

使用实际模型适配器时，其当前导入路径为：

```python
from lake_agent.adapters.planner import JsonPlanner
```

恢复实现指纹覆盖顶层公共 Python 模块和 `engine/`、`adapters/` 的递归 Python 文件，使用包内相对路径区分文件。目录移动也会改变指纹，旧版本未终结任务不能直接继续；既有 `runs/` 记录保持原样，不执行跨版本迁移。

## 评估报告与工作台

| 文件 | 职责 |
|---|---|
| [evaluation/__init__.py](../src/lake_agent/evaluation/__init__.py) | 报告契约包说明，不导入实验运行器 |
| [evaluation/models.py](../src/lake_agent/evaluation/models.py) | EvaluationMode、指标、行与导入报告模型；允许记录三组消融结果 |
| [web/__init__.py](../src/lake_agent/web/__init__.py) | 可选工作台的入口说明 |
| [web/app.py](../src/lake_agent/web/app.py) | FastAPI/CLI 入口、请求校验、路由、模式与评估功能开放策略 |
| [web/configuration.py](../src/lake_agent/web/configuration.py) | 从服务端配置绑定实际模块工厂与执行配置，按每次尝试创建规划器并管理客户端 |
| [web/profiles.py](../src/lake_agent/web/profiles.py) | 端口工厂、数据池元数据、执行配置注册与恢复连接身份 |
| [web/service.py](../src/lake_agent/web/service.py) | 任务准入、并发容量、执行生命周期、停止、恢复与删除 |
| [web/storage.py](../src/lake_agent/web/storage.py) | 界面清单、工作区独占锁、审计视图与删除标记；SQLite 仍是恢复依据 |
| [web/reports.py](../src/lake_agent/web/reports.py) | 已有运行结果的 HTML 导出 |
| [web/evaluation_storage.py](../src/lake_agent/web/evaluation_storage.py) | 导入报告的私有文件存储、路径约束、原子写入和取消收尾 |
| [web/static/index.html](../src/lake_agent/web/static/index.html) | 工作台页面结构 |
| [web/static/app.js](../src/lake_agent/web/static/app.js) | 页面交互、任务视图、结果与评估报告展示 |
| [web/static/styles.css](../src/lake_agent/web/static/styles.css) | 页面样式 |

`evaluation/` 出现在正式包中，是为了让独立评估脚本和工作台共享严格报告格式，不代表生产引擎包含实验。该包不依赖 Web/FastAPI，也不实现固定检索、不发请求、不读取任务标签。原 `web/evaluations.py` 已拆为独立 `evaluation/models.py` 与 `web/evaluation_storage.py`。

报告的 `EvaluationMode` 可以记录 `fixed_retrieval`、`no_verifier`、`full_agent`；生产的 `RunMode` 继续只有后两项。导入一份固定检索报告不会增加生产执行能力。指标缺测保持 null，不从展示层补造实验结果。

正式 Web 默认 `enable_evaluations=false`，既不开放报告接口，也不构造评估仓库或创建评估目录；`RunService.evaluations` 为 None。只有可信测试/评估宿主显式开启功能后才初始化报告存储，普通任务运行不依赖实验报告能力。

## 实验与测试的位置

| 目录或入口 | 用途与边界 |
|---|---|
| [tests/README.md](../tests/README.md) | 测试分组、共享支持代码与离线执行导航 |
| `tests/contracts/` | 共享请求/响应、RunConfig 模型约束和接口约束 |
| `tests/adapters/` | 配置加载、工厂接线，真实规划与隔离内容适配器的请求、输出、兼容和客户端边界 |
| `tests/engine/` | 自主动作、工具调度、上下文、核验适用状态和答案修订 |
| `tests/recovery/` | SQLite 状态、租约、调用重放、预算、取消与恢复兼容性 |
| `tests/web/` | HTTP/页面策略、生命周期、失败信息、报告和任务管理 |
| `tests/evaluation/` | 三组消融执行、CLI、统计与报告契约 |
| [tests/isolated_runtime.py](../tests/isolated_runtime.py) | 源码侧运行时隔离 CLI；显式运行时可接真实模型并启动测试工作台 |
| [tests/ablation_runtime.py](../tests/ablation_runtime.py) | 源码侧三组消融 CLI，使用宿主提供的冻结任务与真实工具映射 |
| `tests/support/` | 合成端口、隔离执行配置、模型请求构造器、恢复/工作台辅助及消融统计；测试间共享这些模块，不互相导入 `test_*.py` |
| [scripts/verify_installed_web.py](../scripts/verify_installed_web.py) | 检查安装后的公共导入、CLI、配置和静态资源，不启动服务 |

测试支持包和 CLI 都不进入 wheel。两个手动 CLI 的模块名继续为 `tests.isolated_runtime` 和 `tests.ablation_runtime`，分组目录不改变其启动命令。离线 pytest 使用 MockTransport 和本地端口，不会自动运行手动真实模型入口。

内容修正的共享 ErrorInfo.details、ContentFeedback、请求子类和 RunResult.failure 在 models.py；控制器与图保存反馈并继续模型自主决策，engine/recovery.py 从 SQLite 已知响应重建，web/storage.py 与 app.js 展示当前具体失败和旧历史事件回退。相关回归分别归入契约、适配器、引擎、恢复和工作台分组；协议和兼容规则见[内容反馈说明](content-feedback.md)。

待接入模块的接口约定见[运行契约](runtime-contract.md)，三组指标口径见[消融评估说明](ablation-evaluation.md)。
