# 异构数据池 Agent Runtime

让 Agent 根据自然语言问题发现数据、选择工具、收集证据，并返回带引用的答案。本仓库维护 `runtime/` 中的运行控制框架、服务端配置、工作台、测试和接入文档。

## 当前接入方式

产品入口从 TOML 加载真实模型和待接入模块，不包含默认演示配置、固定资料、固定答案或固定核验逻辑。数据处理模块与内容模块的生产实现尚待接入；Harness 通过同一进程内的异步端口连接它们。

| 部分 | 实现与职责 |
|---|---|
| Harness | 规划授权、参数/来源校验、预算/超时/重试、安全并行、证据、恢复与审计 |
| JsonPlanner | 按配置调用真实 OpenAI-compatible Chat Completions API，通过 `decide` 提出下一动作与可选计划 |
| 工作台 | 智能问答、来源目录、任务历史、真正删除及报告导出；评估导入由可信测试宿主显式开启 |
| DataPort / ContentPort | 分别由待接入的数据处理模块与内容模块实现，通过配置声明工厂及私有参数 |

## 准备配置

环境要求 Python 3.11+、uv 和 macOS/Linux/WSL。

```bash
cd runtime
uv sync --extra dev --extra web --frozen
cp config.example.toml config.toml
```

填写 `config.toml` 的 API 地址、模型和密钥来源，并安装待接入模块的适配器，将两个 factory 改成可导入的 `module:function`。配置与工厂协议详见 [服务端配置说明](runtime/docs/configuration.md)。准备好真实模块后，手动运行：

```bash
uv run --extra web --frozen lake-agent-web --config config.toml
```

配置非法或模块无法导入会在启动前退出；不会静默切换到测试数据。应用仅监听本机，默认 http://127.0.0.1:8765 。相同配置和存储目录保留同一组历史；连接变化或内核版本不兼容的旧任务不能继续恢复。

## 待接入模块

- 数据模块实现 `DataPort.catalog/tool_specs/invoke`。`tool_specs()` 为同步可信元数据，其他方法异步，返回稳定 source ID、受工具 schema 约束的执行结果与 locator。
- 内容模块实现 `ContentPort.prepare_catalog/search/compose/verify`。本地目录与检索返回 `Usage.local()`；生成和核验如实报告模型用量，引用只能来自输入证据。
- 两个工厂签名均为 `factory(*, options: dict, llm: LLMSettings)`，返回异步上下文管理器；每次运行/恢复独立创建并关闭客户端。工厂和连接配置不由浏览器提交。

直接使用 [共享模型](runtime/src/lake_agent/models.py) 与 [端口协议](runtime/src/lake_agent/ports.py)，不要复制另一套字段。`JsonPlanner` 只负责规划，不替内容模块生成或核验最终答案。

唯一生产流程是 `initialize → decide → action → decide`。模型的 `finish` 是完成提议，控制器根据当前运行政策决定是否接纳。`require_verification=false` 允许未核验完成；开启后必须有适用于当前完整答案和证据的 `supported` 核验。`full_agent` 的名称本身不启用强制核验；`completed` 与内部核验通过都不代表外部评测正确。

## 验收与阅读入口

离线测试使用 `tests/support/` 中显式注入的替身和内存 HTTP transport；不发外部模型请求、不启动监听服务。只有真实模块、真实模型与实际数据共同运行后，才能验收答案质量、资料不足和引用定位。

- [运行时 README](runtime/README.md)：安装、配置、验证命令及实际边界。
- [接口契约](runtime/docs/runtime-contract.md)：共享字段、错误、预算和恢复语义。
- [工作流](runtime/docs/langgraph-workflow.md)、[运行控制器](runtime/src/lake_agent/runtime.py)：流程节点、授权和结果接纳。
- [工作台说明](runtime/docs/project-workspace.md)：页面、HTTP 接口和评估格式。
- [恢复说明](runtime/docs/durable-recovery.md)：已知响应重用与未知结果拒绝。
- [源码导航](runtime/docs/source-layout.md)：正式模块、隔离运行器和评估代码的职责边界。
- [测试导航](runtime/tests/README.md)：按契约、适配器、引擎、恢复、工作台和评估分组的测试及离线命令。

仓库提交源码、测试和脱敏配置样例，不提交本地 `config.toml`、凭据、环境、运行历史或构建输出。历史验收记录仅表示其对应版本的结果。
