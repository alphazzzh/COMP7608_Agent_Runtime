# 项目数据池工作台

正式工作台提供智能问答、数据来源和任务历史，执行仍委托原异步 Harness。生产入口从服务端 TOML 注册真实模块，不包含演示配置或固定回答。

## 配置与使用

按 [配置说明](configuration.md) 填写 API、模型和两个实际适配器工厂。准备好后在 runtime 目录手动执行 `uv run --extra web --frozen lake-agent-web --config config.toml`。缺配置或模块导入失败会在启动前退出。

1. **智能问答**：选择已注册配置、输入实际问题，以 full_agent 提交后查看答案、证据与原始调用。正式入口只开放 full_agent；固定检索不是应用执行模式。
2. **数据来源**：展示服务端公开注册的 SourceRecord；配置式工厂默认不预读目录，因此此列表可以为空。实际运行仍通过 DataPort.catalog 发现目录，证据中的 locator 指向本次读取片段。
3. **任务历史**：查看实际运行记录。取消或停机后的未终结任务可手动尝试恢复，是否安全继续由内核决定。恢复不刷新期限、预算，也不重复派发已知成功结果。
4. **实验评估（仅显式测试宿主）**：下载模板，填入实际评估结果后导入。未知质量、token 或成本指标保持 null，页面不从一次运行推算实验分数。

每次提交问题产生独立运行。“执行过程与原始记录”可按提问切换任务，时间线只显示当前任务，恢复尝试留在同一任务下。连续执行阶段保持审计顺序并可折叠；支持阶段筛选和事件、工具、编号搜索。切换问题会清除旧筛选。新 Web 请求的 task_id 使用 web:{run_id}，旧任务恢复继续使用已保存身份。

配置中的 server.data_dir 相对于配置文件解析。相同目录保留同一组历史，更换目录会展示另一组记录。重启时未结束的 queued/running 记录会标为 interrupted，不自动重新派发。Ctrl+C 关闭服务时会等待其任务与磁盘操作结束。

报告是下载时刻的独立 HTML 快照，不会随运行更新。实际模块、模型和数据必须接通后才能做问题理解、证据不足和答案质量验收。

删除入口位于当前任务、最近任务和任务历史。删除前需要确认；正在运行的任务须先停止并等待资源清理，后端会再次检查。成功后清除答案、证据、时间线、缓存和上次选择的任务偏好，迟到的读取不会把已删任务重新显示。删除仅影响指定run，其他任务及共享SQLite数据库继续保留；跨进程锁文件保留以维护互斥。已下载的报告副本不会被远程删除。

删除通过持久标记协调SQLite与文件清理；发生中断时，重试删除或下次服务启动会继续清理，未清理完成不会返回204。

## 编程注册

CLI 使用 load_config() 与 configured_profile() 构造一个 ExecutionProfile。宿主也可通过 create_app(profiles=[...]) 显式注入多份配置；没有注入时注册表为空，不能提交任务。可信测试宿主可用 allowed_modes 显式开放对照模式，用 enable_evaluations=True 开放实验评估；正常CLI不开放这些能力。

ExecutionProfile 的 ports_factory 每次 run/resume 返回独立 ExecutionPorts，并在退出时清理资源。metadata、sources 和 run_config 是公开身份及策略；factory、私有 options 和密钥留在宿主。绑定摘要参与恢复签名而不公开连接参数。实际适配器行为或数据版本变化时更新 metadata.revision。

sources 只承担公开目录展示，可以为空，不代替 DataPort.catalog。它最多 2000 条，source_id 不重复。不要在公开名称、模型 ID 或来源元数据中放私密连接信息。

## 评估导入格式

页面模板是导入字段结构，不会自动加入评估列表。报告包含`title,dataset_id,task_set_id,model_id,evaluator,evaluated_at,notes,results`；时间为带时区的ISO日期字符串，最多64字符，每个mode只出现一次。`results`含1–3项，每个result含1–1000000的整数`task_count`和`metrics`。

`title`为1–160字符，`dataset_id/task_set_id/model_id/evaluator`各为1–200字符，`notes`最多2000字符、默认空串。服务端拒绝额外字段、类型不符及NaN/Infinity；`metrics`可省略，缺失指标保持null。

| 字段 | 含义与范围 |
|---|---|
| accuracy | 标注所定义的答案准确率，0..1或null；内置评估使用规范化精确匹配 |
| numerical_accuracy / structured_accuracy | 数值容差／结构化结果匹配比例，0..1或null |
| retrieval_precision / retrieval_recall / retrieval_f1 | 与标注来源比较的检索指标，0..1或null |
| grounding | 由评估程序定义并在notes说明的证据支持比例，0..1或null |
| completion_rate | 任务完成率，0..1或null |
| valid_tool_call_rate / unnecessary_tool_call_rate | 合法成功响应／独立标注的不必要尝试占工具调用意图的比例，0..1或null |
| retry_recovery_rate | 有重试的操作中最终获得成功响应的比例，0..1或null |
| tool_calls_mean / data_calls_mean / latency_ms_mean | 平均工具尝试数／含目录的数据调用数／平均毫秒耗时，非负或null |
| input_tokens_total / output_tokens_total / token_total | 该组任务实测输入／输出／总token，缺测保持null |
| provider_requests_total / cost_usd | 实际提供方请求总数／美元成本，非负或null |

评估层接受 `fixed_retrieval`、`no_verifier`、`full_agent` 三组名称，用于导入 Proposal 的比较结果；这不赋予运行 API 执行固定检索的能力。`tests/support/ablation.py` 单独运行固定 top-k 一次生成基线；完整组使用 `require_verification=true`，无核验组使用 false 并禁止核验。日常 full_agent 默认 false，不应把交互运行直接当作完整组的受控实验。完整命令和指标口径见[消融评估说明](ablation-evaluation.md)。

导入体最大128KiB，服务端校验指标、模式、日期和额外字段，保存为私有原子JSON。页面明确标为“导入评估”；它不验证评估程序本身是否正确。比较前固定数据集、任务集、模型/参数和计分方式；同一报告各模式任务数不同时页面提示不能直接比较。不得从一次运行的工具数或耗时推算准确率、检索质量或未知token/成本。

运行JSON与评估JSON属于不同结构；运行JSON保留审计和证据，评估JSON保存实际测量汇总，不能直接互相导入。完整评估下载还包含服务端生成的`evaluation_id/imported_at/origin`；如需再次导入，先移除这三项，恢复模板规定的导入结构。

## HTTP接口

下表路径相对于本机服务地址。评估接口仅在测试宿主显式启用时可用，正式入口返回404。除HTML报告、静态页面和DELETE的204空响应外，成功响应为JSON。API不接受Python代码、factory路径、任意本地文件路径、密钥或客户端自行设置的运行预算；模块及预算由服务端注册。

| 方法与路径 | 输入及成功响应 |
|---|---|
| `GET /api/info` | 返回`version,application,profiles,capabilities,enabled_modes,features`；`application`为`project-workspace` |
| `GET /api/profiles` | 返回`{profiles:[公开配置]}`；公开字段含metadata及`ready,unavailable_reason,source_count,model_id,max_wall_time_ms` |
| `GET /api/profiles/{profile_id}/sources` | 返回`{profile:公开配置,sources:[SourceRecord]}`；只读注册目录，不执行数据工具 |
| `POST /api/runs` | JSON请求见下文；202返回运行摘要，表示已接受后台执行 |
| `GET /api/runs` | 返回`{runs:[运行摘要]}`，按创建时间从新到旧，最多100条 |
| `DELETE /api/runs/{run_id}` | 无需请求体；204空响应，清除该任务的清单、所有执行段的JSONL/工件以及SQLite的run_states/call_records；活动任务返回409，未知任务返回404 |
| `GET /api/runs/{run_id}` | 返回运行详情：摘要字段加`active,can_resume,resume_blocked_reason,attempts,events,evidence,sources,metrics` |
| `POST /api/runs/{run_id}/resume` | 无需请求体；202返回运行摘要。只接受可恢复且有持久状态的 interrupted 任务；是否能继续仍由内核判断 |
| `POST /api/runs/{run_id}/cancel` | 无需请求体；取消并等待所属任务结算后，202返回摘要。已停止任务返回409 |
| `GET /api/runs/{run_id}/artifacts/{attempt_id}/{ref}` | 返回该run指定执行段中已被审计事件引用的artifact JSON；未知身份或未引用内容返回404 |
| `GET /api/runs/{run_id}/export` | 下载完整运行详情JSON，附件名为`<run_id>.json` |
| `GET /api/runs/{run_id}/report` | 下载独立HTML快照，附件名为`<run_id>.html`；内容转义，报告无远程资源或脚本 |
| `GET /api/evaluations` | 返回`{evaluations:[完整评估报告]}`，按导入时间从新到旧，最多100条 |
| `POST /api/evaluations` | 请求体为上节评估导入结构；201返回完整报告，增加服务端生成的`evaluation_id,imported_at,origin`，其中origin固定为`imported` |
| `GET /api/evaluations/template` | 下载合法导入模板`evaluation-template.json`，指标为null；不会保存为评估结果 |
| `GET /api/evaluations/{evaluation_id}` | 返回完整评估报告JSON，同时设置`Content-Disposition: attachment`，附件名为`<evaluation_id>.json`；浏览器可下载，`fetch`仍可按JSON读取 |

`GET /`提供页面，`GET /static/{name}`只提供固定的网页资源。此应用未开放自动生成的`/docs`、`/redoc`和OpenAPI接口页。

新客户端创建运行的最小请求：

```json
{"question":"比较两类来源对同一指标的定义，并给出依据。","profile_id":"project-main","mode":"full_agent"}
```

`question`必填，原始字符串长度为1–2000字符，去除首尾空白后必须非空。正式入口的`mode`只允许`full_agent`，省略时为`full_agent`；测试宿主可显式开启`no_verifier`，禁用模式返回422 mode_disabled，恢复旧的禁用模式同样拒绝。`profile_id`长度为1–100字符，必须对应服务端注册且已就绪的配置。省略 profile_id 时，仅有一个配置才能自动选择；多个配置返回 profile_required，空注册表返回 profiles_unconfigured。历史 scenario 字段不再接受。

运行摘要包含`run_id,profile_id,profile,question,mode,status,created_at,updated_at,error,result`；`profile`为创建任务时保存的公开快照。详情中的事件保留原TraceEvent字段并增加`attempt_id`，按执行段和段内seq展示；证据按evidence_id去重。`metrics`包含`tools,steps,model_calls,evidence,attempts,duration_ms`，已有终态时以RunResult累计计数为准，不能把多个执行段的重放事件重复相加。

若本次返回审计故障而SQLite仍保留未终结状态，网页显示`interrupted`并保留恢复入口；`can_resume`只表示允许尝试，是否安全继续仍由内核决定。

Web状态包括`queued/running/interrupted/blocked`，以及内核返回的`completed/insufficient_evidence/budget_exceeded/timeout/failed`。202不保证任务成功，也不保证恢复一定可行；后台恢复若被内核拒绝，详情会变为`blocked`并提供安全错误码。活动任务、已保存终态或不满足恢复条件的请求返回409；执行尝试最多20次。原本可恢复的记录若遇到配置缺失或改变，详情的`can_resume`为false，并给出`resume_blocked_reason`。

业务错误和请求校验错误使用`{"detail":{"code":"...","message":"..."}}`，不回显原始请求或内部异常。常见响应如下：

| HTTP状态 | 错误码与含义 |
|---|---|
| 422 | `invalid_request`：非法JSON、字段类型/长度/额外字段、空白问题、非法模式；`profile_required`：多配置时未选择 profile；`invalid_evaluation`：评估结构、指标、任务数或带时区日期不合法 |
| 403 | `origin_forbidden`：写入请求来自其他Origin，或被浏览器标为cross-site |
| 404 | `profile_not_found`或`not_found`：配置、运行、执行段、引用artifact或评估不存在 |
| 409 | `profiles_unconfigured/profile_unavailable/profile_changed`、`already_running/not_resumable/not_running`：配置或任务状态不允许本次操作 |
| 413 | `request_too_large`：评估请求体超过128KiB；`evaluation_too_large`：序列化后的保存记录超过上限 |
| 429 | `capacity_reached`：已达服务并发上限（默认2）；`attempt_limit`：已达20次执行尝试 |
| 503 | `service_stopping/history_unavailable/evaluations_unavailable`：服务关闭中，或本地记录无法安全读取/保存 |

写入请求若带`Origin`，必须匹配当前协议、主机和端口；没有Origin的本地脚本请求仍可使用，`Sec-Fetch-Site: cross-site`会被拒绝。服务不开放CORS，页面使用同源内容安全策略和文本渲染；这些措施不提供登录或多人权限。路径参数只定位服务生成的记录及已引用artifact，不能用于读取任意文件。

## 当前可用范围

页面、服务端注册入口、运行/恢复/审计、来源目录和评估导入可独立验收。默认安装没有真实数据池与内容适配器，也没有预置API密钥；接入后需要单独做真实端到端和质量评估。本轮开发和测试不调用外部模型。

`127.0.0.1`地址只能在本机使用。向组员展示可共享屏幕或发送下载的HTML报告；他人要运行应用，可在自己的项目副本启动。此版本没有公网部署、登录或多人权限系统。

评估报告的 external_data_bytes_total / external_data_cost_usd 可以记录独立测得的外部数据流量和费用；当前 DataPort 不提供可信计量字段，运行器保持 null。data_calls_mean 只表示调用次数，不能替代字节量或费用。
