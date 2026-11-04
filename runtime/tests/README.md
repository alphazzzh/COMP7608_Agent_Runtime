# 测试导航与维护约定

测试按当前职责组织，避免按某轮修复新增 `*_review.py`、`*_final.py` 等文件。默认使用本地端口、临时 SQLite/JSONL、`httpx.MockTransport` 与内存 ASGI；不需要私有 `config.toml`，不会启动监听服务或访问真实模型。

## 目录与定位

| 目录 | 负责验证什么 | 主要文件 |
|---|---|---|
| [contracts/](contracts/) | 共享严格模型、并行参数、修订和内容反馈字段、JSON 边界 | `test_models.py`、`test_content.py` |
| [adapters/](adapters/) | 模型请求和解析、配置读取、工厂资源所有权、隔离 CLI | `test_planner.py`、`test_model_content.py`、`test_configuration.py`、`test_profiles.py`、`test_isolated_cli.py` |
| [engine/](engine/) | 自主动作用法、完成政策、核验新鲜度、反馈修订、观察分页、工具授权、预算及并行 | `test_agent_loop.py`、`test_verification.py`、`test_content_feedback.py`、`test_observations.py`、`test_execution.py`、`test_runtime.py`、`test_runtime_guards.py`、`test_parallel.py`、`test_graph.py` |
| [recovery/](recovery/) | SQLite/审计、已知响应重放、准入和版本身份、状态恢复、取消清理 | `test_replay.py`、`test_agent_replay.py`、`test_admission.py`、`test_frontier.py`、`test_content_feedback.py`、`test_answer_state.py`、`test_cancellation.py`、`test_state.py`、`test_trace.py` |
| [web/](web/) | HTTP/展示政策、任务身份、真正删除、工作台生命周期、失败详情、评估导入 | `test_http.py`、`test_lifecycle.py`、`test_workspace.py`、`test_failures.py`、`test_evaluations.py` |
| [evaluation/](evaluation/) | 固定 Top-k、无核验、强制核验三组对照，离线指标与报告契约 | `test_ablation.py`、`test_models.py` |
| [support/](support/) | 多个测试复用的显式端口、请求构造器、资源关卡和评估工具 | 见下表；本目录不是生产包 |

图测试的 `test_graph.py` 验证当前唯一的 `agent-loop-v1`，没有保留旧固定工作流。核验和答案修订的集成回归分别位于 `engine/test_verification.py`、`engine/test_agent_loop.py`，恢复后的同一状态由 `recovery/test_answer_state.py` 检查。

## 常用离线命令

以下命令从 `runtime/` 执行，要求依赖已经安装或在本地 uv 缓存中可用。`--offline` 不允许 uv 下载依赖；模型边界由测试传输替身控制。

```bash
# 完整回归与测试收集
uv run --offline --extra dev --extra web --frozen pytest -q
uv run --offline --extra dev --extra web --frozen pytest --collect-only -q

# 按职责运行
uv run --offline --extra dev --extra web --frozen pytest -q tests/contracts tests/adapters
uv run --offline --extra dev --extra web --frozen pytest -q tests/engine tests/recovery
uv run --offline --extra dev --extra web --frozen pytest -q tests/web tests/evaluation

# 按实际行为定位；可以组合 -k、-x、-vv
uv run --offline --extra dev --extra web --frozen pytest -q tests/engine/test_verification.py
uv run --offline --extra dev --extra web --frozen pytest -q tests/engine tests/recovery -k 'revision or verification'

# 静态检查与打包
uv run --offline --extra dev --extra web --frozen ruff check src tests scripts
uv run --offline --extra dev --extra web --frozen ruff format --check src tests scripts
node --check src/lake_agent/web/static/app.js
uv build --offline
```

某个参数化用例可以直接使用 `--collect-only` 输出的完整 node ID。迁移后的 node ID 包含职责目录，旧的 `tests/test_*.py` 路径不再适用；项目中的历史计划和验收记录只记录当时路径，不作为当前执行命令。

## 共用支持代码

| 模块 | 职责 |
|---|---|
| `support/__init__.py` | 统一的源码资源根路径 `RUNTIME_ROOT` |
| `support/ports.py`、`support/web_ports.py`、`support/profiles.py` | 合成模块、工作台端口和显式故障注入 |
| `support/agent.py` | 动作规划替身、记录请求的内容端口、无状态修订样本、内存审计 |
| `support/recovery.py` | 跨全新端口实例的记录器、取消关卡、恢复调用和轨迹读取 |
| `support/web.py` | 内存 ASGI 生命周期、任务轮询与工具关卡 |
| `support/configuration.py` | 仅用于临时目录的脱敏配置文本和写入器 |
| `support/planner_requests.py`、`support/content_requests.py` | 模型请求和 provider 响应的构造器 |
| `support/isolated.py`、`support/model_content.py` | 手动隔离测试的数据池及模型内容适配器；pytest 通过 MockTransport 检查它们 |
| `support/ablation.py`、`support/ablation_metrics.py`、`support/ablation_cases.py` | 三组对照执行、标注评分及测试工具映射 |

共用代码只导入生产模块或其他支持模块，不导入 `test_*.py`。仅单个测试文件使用的替身留在该文件；确实跨模块复用时才放入 `support/`。不要为了让输出更好看而把失败替换成固定成功。

## 手动测试入口

根部保留两个明确的执行模块，其模块名不变，也不会因 pytest 收集而自动启动：

- `tests.isolated_runtime`：待接入模块未就绪时的隔离环境；明确执行提问或 `--web` 才会使用真实模型，接入步骤见[配置说明](../docs/configuration.md#待接入模块未就绪时的隔离测试)。
- `tests.ablation_runtime`：显式给定任务集和配置的评估宿主；指标边界见[消融评估说明](../docs/ablation-evaluation.md)。

这两个入口的 `--help` 只显示用法。`tests/` 不打包进生产 wheel；正式入口不会自动导入测试端口或固定检索基线。

```bash
uv run --offline --extra web --frozen python -m tests.isolated_runtime --help
uv run --offline --extra web --frozen python -m tests.ablation_runtime --help
```

## 新增与整理测试

先按责任选取现有文件，在相邻测试中补用例；只有新的独立职责或文件已经难以阅读时才拆分。测试要检查真实控制器行为和端口请求的信息流，复用原有取消/恢复关卡，避免依赖调用次数得到固定的第二次成功。

移动或合并测试时，核对参数化用例清单，防止同名函数覆盖、夹具作用域变化或导入路径残留；完整回归通过后再完成整理。离线测试只能证明已覆盖的契约和行为，不能证明真实提供方兼容性、答案事实正确或外部评估得分。
