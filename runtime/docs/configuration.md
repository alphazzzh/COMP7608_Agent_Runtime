# 服务端配置与真实模块接入

产品入口从 TOML 读取 API、模型、预算、存储目录和模块工厂。配置缺失、凭据缺失或模块无法导入时直接报错；不注册演示配置，不提供固定答案或固定核验结果。配置加载阶段不发模型请求、不进入模块上下文。

## 配置文件

从 `config.example.toml` 复制一份私有 `config.toml`。填写 `[llm]` 的实际 `base_url`、`model_id`，并在 `api_key_env` 指定的服务端环境变量中提供密钥。也可在私有文件中使用 `api_key`，此时必须删除 `api_key_env`。两种凭据来源只能选一个。

`config.toml` 是实际运行读取的配置；`config.example.toml` 是可提交且不含凭据的模板，两个名称承担不同用途。`RunConfig` 类继续在 `models.py` 定义字段类型、默认值和校验，具体环境的配置值写在 TOML 中。TOML 不能代替 Python 的类型定义。

例如 `api_key_env = "MODEL_API_KEY"` 填的是环境变量名称。在准备启动的同一个终端执行 `export MODEL_API_KEY="你的实际密钥"`，启动的 Python 进程才会继承它。当前不会自动读取 `.env`。如果报 `The configured model credential environment variable is empty.`，表示该进程中的变量不存在或内容为空，与待接入模块无关。也可以在私有 TOML 的 `[llm]` 中改用 `api_key = "你的实际密钥"` 并删除 `api_key_env`。

`base_url` 是 API 根地址，例如提供方的 `/v1` 根路径；规划器追加 `/chat/completions`。地址必须是绝对 HTTP(S) URL，不能带用户名、密码、query 或 fragment。模型参数仅支持规划器已有的允许列表，不能通过参数覆盖 `model`、`messages` 或开启流式响应。

| 配置表 | 作用 |
|---|---|
| `[llm]` | API 根地址、模型名、凭据来源与采样参数 |
| `[server]` | 本机端口、运行记录目录、同时执行任务上限 |
| `[profile]` | 浏览器可见的数据池身份、说明和版本 |
| `[adapters.data]` | 实际 DataPort 的 `module:function` 工厂与私有 `options` |
| `[adapters.content]` | 实际 ContentPort 的工厂与私有 `options` |
| `[run]` | `RunConfig` 预算字段；禁止重复声明模型名和模型参数 |

配置严格拒绝未知字段、错误类型和非有限数值。`server.data_dir` 相对于配置文件所在目录解析，读取时不创建目录。适配器的 `options` 按原样传给实际模块，不自动修改其中的路径；各模块应明确自己的路径语义。

`profile` 的身份字段必须填写；`description` 与 `suggested_questions` 可省略。`revision` 应随数据版本或适配器实现行为改变而更新。

## Harness 自主决策配置

真实 JsonPlanner 声明唯一支持的 `agent-loop-v1`，通过 `AgentPlannerPort.decide` 逐轮选择动作。数据处理模块与内容模块的异步方法不变；ComposeRequest 新增默认空的 revision，内容输出错误的 feedback 扩展仍仅在内容模块明确声明能力后使用，见下文。没有 agent-loop-v1 声明或 decide 方法的规划器以 invalid_planner_protocol 拒绝执行；旧两阶段规划 API、旧图与自动回退已删除。

`[run]` 可设置 `max_agent_turns = 32` 和 `require_verification = false`。轮数是整个自主循环的上限，模型和内容实际派发仍共用 `max_model_calls`；搜索和目录读取也不能形成无限循环。默认 false 允许未核验完成，仍由模型决定是否核验；设为 true 时，控制器只接受有当前有效 supported 核验的 finish 提议，不强制核验发生在哪一步。核验绑定实际送入 verify 的完整问题、答案、证据列表及运行模式/强制核验政策；答案或证据变化后须重新核验，单纯调用工具而没有改变相关状态不会自动过期。该要求与测试模式 no_verifier 冲突时，在外部调用前以 invalid_run_policy 停止。

日常交互默认 `require_verification=false`；Proposal 对照实验中的 `full_agent` 使用 true，`no_verifier` 使用 false 且禁止核验动作，两组均通过同一自主图运行。固定 top-k 检索后一次生成的基线只在 `tests/support/ablation.py` 实现，不由应用的 mode 参数启动；评估导入仍允许三组报告。已删除的 `max_feedback_rounds`、`fixed_extract_max_chars` 不应再写入 `[run]`，未知字段会严格报错。

如果旧 `config.toml` 启动时出现 `Application configuration failed strict validation. Check: run.`，检查 `[run]` 是否还保留这两个退役字段；删除对应配置行，再使用原启动命令。`max_feedback_rounds` 属于旧核验反馈状态机，`fixed_extract_max_chars` 属于旧固定提取流程；当前自主循环不使用它们，不能通过重新添加模型字段来兼容。现在的错误信息会明确列出 `run.max_feedback_rounds` / `run.fixed_extract_max_chars` 并要求从 `[run]` 删除。其余有效预算和 `[llm]` 设置无需改变；这类错误与密钥及待接入模块没有关系。

旧配置如果显式覆盖了 `[run.prompt_versions]`，也应将 `planner` 更新为 `"planner-v1.1-exploration"`，避免审计记录仍显示旧提示版本。省略该字段时使用当前模型默认值。

这些版本字符串是宿主声明的审计标签，不是对提供方实际提示内容的证明。内容反馈扩展不新增配置项，也不自动改写本地提示标签；实际能力与纠错行为同时由协议、调用请求/事件和源码恢复指纹记录。

提示鼓励复杂任务使用可修改的 DAG 计划作为工作记忆并更新完成项；对未知结构先查看目录、可信 schema 工具和观察分页，再构造过滤或聚合。核验失败时依据 issues/recommendation 补证或重新 compose，避免无新信息地重复失败参数。这些是模型可按任务选择的启发，不是固定工作流。

模型可以不拆分任务、不检索而请求生成，或直接调用已注册目录来源的工具；每次工具仍检查 schema、来源权限、调用数与并发限制。内容模块已通过 prepare_catalog 接收完整目录，可以解释来源名称和元数据；空 evidence 时不应凭空生成资料正文的事实。ComposeRequest.revision 默认空且空值不在线上 JSON 中增加字段；已有答案时，内容模块收到旧答案、相关核验、verification_is_current 和可选修改要求。规划器的 compose.revision_instructions 最多4000字符，没有旧答案时不能请求修订。生产内容模块必须升级共享模型并把 revision 送入自己模型的 messages；这些文字不能被当成权限或预算变更。显式支持输出错误修正的内容模块还需实现含 feedback 的请求子类。核验失败返回模型，由模型选择继续调查、修改答案或停止；完整输入未变时，同一答案不能通过重复 compose 清除失败核验。

full_agent 只表示可用的自主动作模式，不替代 require_verification=true。completed 表示控制器按运行政策接受完成提议，不保证事实正确；内部 supported 也不等于外部数据集评测正确。过期核验只表示旧判定不适用于当前状态。强制政策下会以 verification_stale 拒绝过期核验的完成提议，以 verification_required 拒绝缺失或当前不支持的核验；模型可以据此再次 verify 或修订。

运行日志会记录 planner.decide 和 agent.decision.accepted，可检查模型选择了什么、为何被拒绝、实际调用和引用是否一致。新源码或引擎协议与旧持久运行不兼容时拒绝恢复，旧记录仍可查看。

## 待接入模块的适配器工厂

本仓库负责 Harness 和规划器，未附带待接入的数据处理模块或内容模块的生产实现。请安装实际模块，并把两个 `factory` 改为它们可导入的 Python 工厂。现有端口仍在同一进程内以异步方法连接，不新增 HTTP 模块协议。

两种工厂都使用下面的签名：

```python
from contextlib import asynccontextmanager
from lake_agent.config import LLMSettings

@asynccontextmanager
async def create_port(*, options: dict, llm: LLMSettings):
    # 实际模块在这里构造自己的适配器并管理资源。
    # 返回值分别实现 DataPort 或 ContentPort。
    adapter = YourAdapter(options=options, llm=llm)
    try:
        yield adapter
    finally:
        await adapter.aclose()
```

`YourAdapter` 指待接入模块的实际适配器实现；这个片段只说明包装协议，不是本仓库提供的类。工厂必须返回异步上下文管理器，不能返回裸端口或未经包装的协程。数据模块的 `tool_specs()` 是同步方法；其他数据与内容端口方法均异步，详见 `src/lake_agent/ports.py`。

每次 `run/resume` 都以独立配置副本进入两个工厂，然后创建真实 `JsonPlanner`；任何阶段失败都会关闭已进入的上下文，退出时按逆序清理。`llm.api_key` 是 `SecretStr`；实际模型客户端通过 `llm.api_key.get_secret_value()` 取值。最终每次规划或内容请求里的 `model_id/model_params` 才是本次运行的模型设置，内容模块应遵守请求字段。

适配器上下文不能抑制控制器的异常；普通清理异常也不能覆盖已有失败或取消。无原始异常时，清理失败以脱敏错误报告，同时继续清理其他已进入的资源。

`prepare_catalog/search` 仍应为本地检索操作；`compose/verify` 每次最多发起一个模型请求，准确报告用量并禁用 SDK 隐式重试。`JsonPlanner` 只负责规划，不替内容模块生成或核验答案。

## 内容模块的修正能力

本轮不增加 TOML 配置，也不修改私有 `config.toml`。合法核验意见的答案修订使用 ComposeRequest 的可选 revision 字段，不要求 content-feedback-v1；生产内容模块的实现方需要升级该共享模型并实现 messages 中的修订信息流。非法模型输出的纠错另采用端口能力声明：内容模块升级共享模型并支持 `ComposeFeedbackRequest`、`VerifyFeedbackRequest` 后，声明 `content_feedback_protocol = "content-feedback-v1"`。未声明的模块不接收 feedback 扩展；不能只在配置里打开开关就让旧严格消费者接收新字段。

声明后，已知的引用、来源或模型正文 JSON/schema 错误可作为观察交回规划器。模型再次选择同阶段调用时，内容端口接收此前失败的 `call_id`、错误详情和被拒输出预览，并将它们放入下一次提供方 `messages[].content`。HTTP 顶层不新增专用参数，原始 question 和 evidence 不被改写。鉴权、传输、超时与契约错误仍停止，单次内容调用不在内部多请求模型。

所有修正消耗原 max_agent_turns、max_model_calls 和运行期限，未解决反馈不允许 finish；强制核验和 no_verifier 规则不变。SQLite 记录能力与反馈，能力变化拒绝恢复；核验适用状态由已保存请求和响应确定性重建，源码升级仍须通过既有恢复指纹检查，不提供旧任务绕过通道。诊断字段、模块接入和页面行为详见[内容反馈说明](content-feedback.md)。源码隔离端口已经实现 revision 与显式反馈协议，待接入的生产内容模块仍需升级与验证。

## 待接入模块未就绪时的隔离测试

待接入模块未就绪时也可测试运行时。源码中的 `tests.isolated_runtime` 入口保留真实控制器和 `JsonPlanner`，数据与本地检索使用测试依赖，`tests/support/model_content.py` 将答案生成和核验也接到真实 LLM。它读取现有 `config.toml` 的 `[llm]`、`[run]`、`[server]`，在内存副本中替换两项工厂，因此原 `[adapters]` 可以保留占位值，待接入模块不会被导入。原文件不会被修改；仍需保留合法的配置表结构。

先配置真实 API 地址、模型和密钥，然后在 `runtime` 目录执行下面任一方式。命令需由用户手动运行；提交问题后，决策、回答和模型主动选择的核验会请求配置的真实 API，共用运行预算。每次 compose/verify 最多请求一次，不设隐式重试或固定成功回退。

通过网页操作：

```bash
export MODEL_API_KEY="你的实际密钥"
uv run --extra web --frozen python -m tests.isolated_runtime --config config.toml --web
```

打开配置的端口，默认 http://127.0.0.1:8765 。选择“B 模块隔离测试 · 真实 LLM”，使用 `full_agent`，分别提问“目前的合成资料有哪些？”和“合成资料中 2024 年年度营收是多少？”，检查模型是否根据不同问题组织答案。页面保留事件、证据、调用、取消、恢复与删除操作；该测试宿主显式开启对照模式和实验评估。`Ctrl+C` 关闭服务。

直接执行单个问题：

```bash
uv run --frozen python -m tests.isolated_runtime --config config.toml \
  --question "合成资料中 2024 年年度营收是多少？请调用 extract 工具收集证据。"
```

标准输出是 `RunResult` JSON，范围说明写到标准错误。默认 `full_agent`，可用 `--mode no_verifier` 禁止核验。退出码 0 表示 `completed`，1 表示运行已产生其他终止结果（查看 `status/stop_reason`），2 表示启动参数或配置错误。`no_verifier` 禁止核验动作，不能同时设置 require_verification=true。此单次入口保存 JSONL 与工件；恢复交互使用网页入口。

默认记录目录为配置的 `server.data_dir/isolated-runtime/web` 或 `headless`，与正式运行记录分开。两种入口都支持 `--data-dir`；网页入口可用 `--port`。预算原样保留；`run.dataset_snapshot` 须省略或为测试目录的 `demo-v1`，不能填写真实数据池快照。

检查一次执行是否合理时，看模型是否根据问题选择必要动作（允许不拆任务、不搜索或直接说明资料不足）、是否在需要读取正文时选择相关来源并生成符合工具 schema 的参数、控制器是否先接纳计划再执行、工具输出是否成为可追溯的 evidence、回答 citation 是否指向实际证据。营收问题的年度原文包含 `2024 annual revenue was 120 million HKD`；资料概览问题则应回答有哪些来源及内容范围。模型也可能生成非法计划、重复提取或提前停止；应根据实际事件和 `stop_reason` 分析，入口不会强制成功。

模型生成接收当前问题、完成的子任务、实际证据及本运行的目录标签；来源名称可以帮助解释目录，不能充当文档内容的引用证据。核验同时检查是否回答了问题、事实是否有依据、引用是否有效。程序保留模型返回的答案和核验判定，错误引用、API 错误或不支持判定不会变成固定成功。`structured_result.test_scope` 标记合成数据及真实模型生成/核验能力，实际是否执行核验仍以运行模式和审计事件为准。自动化测试以同一证据、不同问题验证接线，不请求外部模型；真实提供方兼容性和答案质量仍需手动验证，待接入模块的生产能力不在此隔离测试范围内。

页面引用显示“证据 1 · 来源文件名”，点击后定位到相应证据。证据默认显示原文或字段/表格预览，以及“第几页/行”等已知位置；自定义定位、完整证据 ID 与 JSON 可展开原始记录查看。列表预览限制为 10 条/8 个字段，其他字段预览最多 12 项，完整数据始终保留。历史运行不会重写答案；调整后的隔离回答需新建任务查看。

“执行过程与原始记录”中的任务选择器按每次提问的 `run_id` 切换，当前问题始终显示在上方。时间线仅包含所选任务，恢复尝试仍归入原任务；连续阶段按原始顺序折叠，可按阶段筛选或搜索事件、工具与编号。完成任务的阶段默认折叠，切换问题会清除旧筛选。新任务使用独立 `task_id=web:{run_id}`；旧恢复任务保留其已保存身份。

此入口仅存在于源码 `tests/`，不会进入生产 wheel；生产 `lake-agent-web` 仍需接入真实数据处理模块与内容模块。

源码归档后，上面的启动命令和 `lake_agent.config.load_config`、`lake_agent.runtime.run/resume` 路径不变。真实规划器位于 `lake_agent.adapters.planner`，图和恢复实现在 `lake_agent.engine`，外部实验报告模型位于 `lake_agent.evaluation.models`；详情见[源码目录与职责](source-layout.md)。这些目录都不包含合成数据或固定检索执行策略。

隔离数据和本地搜索仍为测试替身，包括固定的两份样本及候选顺序；它适合检查运行时的规划、执行、预算和持久状态接线，不能验证真实检索是否相关。正式入口不会导入这些替身。

## 完整模块联调时手动启动

准备好真实模块及配置后，在 `runtime` 目录执行：

```bash
uv sync --extra dev --extra web --frozen
export MODEL_API_KEY="你的实际服务端密钥"
uv run --extra web --frozen lake-agent-web --config config.toml
```

可用 `--port`、`--data-dir` 覆盖配置。显式 `--data-dir` 相对于当前启动目录解释；配置文件中的路径仍相对于配置文件解释。

正式网页只开放 full_agent；关闭核验和实验评估仅在显式测试宿主中启用，服务端也会拒绝被禁用的模式与评估请求。网页只接收问题、允许的模式和已注册 profile 的 ID，不接收 API key、工厂路径或私有选项。未显式指定 profile 时，仅当注册表中恰有一个配置才自动选择；多个配置要求用户选择。

连接目标、模型设置或适配器工厂/选项改变时，私有连接身份摘要改变，旧任务会拒绝恢复。密钥轮换不改变该身份。旧运行仍受到内核源码/版本、原始期限、预算和已知调用日志校验；不会刷新期限或重放结果未知的调用。

## 离线验证边界

测试替身与共享请求构造器位于 `tests/support/`，不随生产 wheel 分发。共享模型契约位于 `tests/contracts/`，配置加载、工厂接线和模型请求适配位于 `tests/adapters/`，工作台接入位于 `tests/web/`；目录及命令见[测试导航](../tests/README.md)。pytest 使用内存 HTTP transport 验证真实规划器请求格式、鉴权头、模型和参数，并验证配置加载、模块生命周期及异常路径，不访问外部 API。安装验证脚本也不启动服务器。

这类验证不证明用户凭据可用、实际提供方响应兼容，或生产数据处理模块与内容模块的答案质量。运行时可以先按上述隔离入口连接真实规划模型；完整系统的真实问题、资料不足、网络失败与引用定位验收仍需待接入模块的生产实现。
