# 项目数据池工作台

适用于0.6.1。工作台现已按整个项目组织：智能问答、数据来源、任务历史、实验评估。执行仍由原异步Harness控制，页面和服务不重新实现工具调度、预算或恢复。

## 启动与使用

在仓库的`runtime`目录执行（macOS/Linux/WSL，Python 3.11+与uv）：

```bash
uv sync --extra dev --extra web
uv run --extra web --frozen lake-agent-web
```

打开 http://127.0.0.1:8765 ，保持启动终端运行。默认记录保存在`runs/web-demo`；可用`--data-dir runs/project-workspace --port 8766`选择另一组记录。服务仅监听本机，同一目录只允许一个服务。终端`Ctrl+C`关闭；后台会取消并等待自己的任务与磁盘操作。

历史归属于服务实际使用的数据目录；相对`--data-dir`按启动时的工作目录解释。更换启动目录或`--data-dir`后看到不同历史，不表示原记录已经删除。同一目录重启会保留运行和评估记录，将遗留的`queued/running`任务标为`interrupted`（已中断），不会自动恢复或重新派发；需要打开记录后手动尝试恢复。

1. **智能问答**：选择执行配置、输入问题、选择完整Agent或两种baseline，再提交任务。默认“项目数据池”显示尚未接入；先选择明确标注的示例即可测试页面全部主流程。示例模式只用合成资料和离线规则，不发模型请求。
2. **数据来源**：选择执行配置，搜索公开目录并查看格式、摘要和元数据。这是模块注册的目录信息，不表示已经扫描整池或读取文件全文；实际读取片段在任务证据中显示定位信息。
3. **任务历史**：查看过去的问题、答案、证据和执行情况。详细审计包含请求、响应和恢复重放标记；重放标记表示复用已保存结果，不表示再次调用工具。可下载JSON或独立HTML报告发给组员，对方无需安装项目。
4. **实验评估**：下载JSON模板，填写实际评估输出后导入，比较full_agent/no_verifier/fixed_retrieval。未测量指标保留null；页面显示“未提供”。没有导入报告时不会生成分数。

标准、并行、安全恢复三个示例都调用真实`run`/`resume`。安全恢复示例在证据保存后暂停；点击恢复后建立新执行段。手动停止可能留下结果未知的调用，内核会拒绝自动重派发。每次新任务示例期限10分钟，暂停与停机时间也计入；恢复不会刷新期限或累计预算。

「任务报告」下载的是下载时刻的独立HTML快照，包含当时状态、答案、证据、累计指标和审计时间线，之后不会自动更新。需要完整结果时，等待任务完成；安全恢复示例应先完成恢复再下载。进行中或暂停时也可导出，但报告会如实保留该时刻状态。

## 同学如何接入模块

公开接口是`lake_agent.web.profiles.ExecutionProfile`与`ExecutionPorts`，服务端通过`create_app(profiles=[...])`注册。一个执行配置对应一个数据池和一组DataPort、ContentPort、PlannerPort；它们分别负责数据工具、检索/组合/核验、任务规划。

下面是可直接运行的注册示例，仍保持显式合成标签，用于先验证同学对注册方法的理解。将其保存到任意本地Python文件，在安装了本包的环境执行即可：

```python
from contextlib import asynccontextmanager

import uvicorn

from lake_agent.web.app import create_app
from lake_agent.web.fixtures import demo_ports
from lake_agent.web.profiles import (
    ExecutionPorts, ExecutionProfile, ProfileMetadata, default_profiles,
)

sample = default_profiles()[1]

@asynccontextmanager
async def ports():
    # 每次run/resume都会进入一次。真实实现需要在此管理客户端生命周期。
    data, content, planner = demo_ports("standard")
    try:
        yield ExecutionPorts(data=data, content=content, planner=planner)
    finally:
        # 这个示例没有需要关闭的客户端。
        # 实际适配器的await aclose()或AsyncExitStack应写在这里。
        pass

metadata = ProfileMetadata.model_validate({
    **sample.metadata.model_dump(),
    "profile_id": "team-registration-check",
    "title": "小组接口接入检查（合成示例）",
})
profile = ExecutionProfile(
    metadata=metadata,
    sources=sample.sources,
    ports_factory=ports,
    run_config=sample.run_config,
)
app = create_app(data_dir="runs/team-check", profiles=[profile])
uvicorn.run(app, host="127.0.0.1", port=8765)
```

接真实模块时，替换factory中的三个实现，按实际情况设置`metadata.kind="project"`及`modules.data/planner/content`的状态和名称。状态允许connected（已接入）、fixture（示例）、unavailable（不可用）；任一unavailable或缺少factory都会禁用运行。混用真实与示例模块时，保留各自准确状态。

`sources`是可选的公开SourceRecord目录，最多2000条且source_id不能重复；可以为空，实际运行时仍通过`DataPort.catalog`发现目录。注册目录不承担工具执行。不要在公开名称、摘要、目录元数据或模型ID中放密钥或私有连接字符串。

`run_config`由服务端控制预算、模型ID、参数、数据快照等。真实API规划可使用已有`JsonPlanner(base_url, api_key)`并在factory退出时关闭；密钥从服务端环境或配置读取，浏览器不传递或存储密钥。**JsonPlanner只实现规划，真实检索、答案生成和核验仍需ContentPort实现。**

每次run/resume需要fresh端口；不要返回已关闭客户端或依赖上一轮短期句柄。每条任务保留当时公开配置快照及配置签名；签名覆盖完整`metadata`、注册的`sources`、`run_config`和`pause_after_evidence`，不包含factory函数本身或其客户端。因此改变适配器行为、连接目标或数据版本时，接入方必须更新`metadata.revision`，不能依赖自动识别函数实现变化。

原配置被移除、不可用或签名改变后，服务在打开factory前拒绝恢复；内核另行检查代码版本、工具契约、历史调用与原期限。旧版记录没有配置签名时仍走兼容路径，不能据此推断其factory身份已被校验。跨版本恢复未提供迁移支持。

## 评估导入格式

页面模板是导入字段结构，不会自动加入评估列表。报告包含`title,dataset_id,task_set_id,model_id,evaluator,evaluated_at,notes,results`；时间为带时区的ISO日期字符串，最多64字符，每个mode只出现一次。`results`含1–3项，每个result含1–1000000的整数`task_count`和`metrics`。

`title`为1–160字符，`dataset_id/task_set_id/model_id/evaluator`各为1–200字符，`notes`最多2000字符、默认空串。服务端拒绝额外字段、类型不符及NaN/Infinity；`metrics`可省略，缺失指标保持null。

| 字段 | 含义与范围 |
|---|---|
| accuracy | 实际任务答案准确率，0..1或null |
| retrieval_precision / retrieval_recall / retrieval_f1 | 与标注来源比较的检索指标，0..1或null |
| grounding | 由评估程序定义并在notes说明的证据支持比例，0..1或null |
| completion_rate | 任务完成率，0..1或null |
| tool_calls_mean / latency_ms_mean | 平均工具调用数／平均毫秒耗时，非负或null |
| token_total / cost_usd | 该行任务的总token／美元成本，非负或null |

导入体最大128KiB，服务端校验指标、模式、日期和额外字段，保存为私有原子JSON。页面明确标为“导入评估”；它不验证评估程序本身是否正确。比较前固定数据集、任务集、模型/参数和计分方式；同一报告各模式任务数不同时页面提示不能直接比较。不得从一次运行的工具数或耗时推算准确率、检索质量或未知token/成本。

运行JSON与评估JSON属于不同结构；运行JSON保留审计和证据，评估JSON保存实际测量汇总，不能直接互相导入。完整评估下载还包含服务端生成的`evaluation_id/imported_at/origin`；如需再次导入，先移除这三项，恢复模板规定的导入结构。

## HTTP接口

下表适用于0.6.1，路径相对于本机服务地址。除HTML报告和静态页面外，成功响应为JSON。API不接受Python代码、factory路径、任意本地文件路径、密钥或客户端自行设置的运行预算；模块及预算由服务端注册。

| 方法与路径 | 输入及成功响应 |
|---|---|
| `GET /api/info` | 返回`version,application,demo_only,profiles,scenarios,capabilities`；`application`为`project-workspace`，只有存在已就绪的project配置时`demo_only`才为false |
| `GET /api/profiles` | 返回`{profiles:[公开配置]}`；公开字段含metadata及`ready,unavailable_reason,source_count,model_id,max_wall_time_ms` |
| `GET /api/profiles/{profile_id}/sources` | 返回`{profile:公开配置,sources:[SourceRecord]}`；只读注册目录，不执行数据工具 |
| `POST /api/runs` | JSON请求见下文；202返回运行摘要，表示已接受后台执行 |
| `GET /api/runs` | 返回`{runs:[运行摘要]}`，按创建时间从新到旧，最多100条 |
| `GET /api/runs/{run_id}` | 返回运行详情：摘要字段加`active,can_resume,resume_blocked_reason,attempts,events,evidence,sources,metrics` |
| `POST /api/runs/{run_id}/resume` | 无需请求体；202返回运行摘要。只接受有持久状态的paused/interrupted任务；是否能继续仍由内核判断 |
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
{"question":"比较两类来源对同一指标的定义，并给出依据。","profile_id":"demo-standard","mode":"full_agent"}
```

`question`必填，原始字符串长度为1–2000字符，去除首尾空白后必须非空。`mode`只允许`full_agent/no_verifier/fixed_retrieval`，省略时为`full_agent`。`profile_id`长度为1–100字符，必须对应服务端注册且已就绪的配置。为兼容旧客户端，可改传`scenario:"standard"/"parallel"/"recovery"`，映射为相应`demo-*`配置；`profile_id`和`scenario`不能同时指定，两者均省略时使用`demo-standard`。这项HTTP兼容默认值与页面默认选中的未就绪项目配置不同。

运行摘要包含`run_id,scenario,profile_id,profile,question,mode,status,created_at,updated_at,error,result`；`profile`为创建任务时保存的公开快照。详情中的事件保留原TraceEvent字段并增加`attempt_id`，按执行段和段内seq展示；证据按evidence_id去重。`metrics`包含`tools,steps,model_calls,evidence,attempts,duration_ms`，已有终态时以RunResult累计计数为准，不能把多个执行段的重放事件重复相加。

若本次返回审计故障而SQLite仍保留未终结状态，网页显示`interrupted`并保留恢复入口；`can_resume`只表示允许尝试，是否安全继续仍由内核决定。

Web状态包括`queued/running/paused/interrupted/blocked`，以及内核返回的`completed/insufficient_evidence/budget_exceeded/timeout/failed`。202不保证任务成功，也不保证恢复一定可行；后台恢复若被内核拒绝，详情会变为`blocked`并提供安全错误码。活动任务、已保存终态或不满足恢复条件的请求返回409；执行尝试最多20次。原本可恢复的记录若遇到配置缺失或改变，详情的`can_resume`为false，并给出`resume_blocked_reason`。

业务错误和请求校验错误使用`{"detail":{"code":"...","message":"..."}}`，不回显原始请求或内部异常。常见响应如下：

| HTTP状态 | 错误码与含义 |
|---|---|
| 422 | `invalid_request`：非法JSON、字段类型/长度/额外字段、空白问题、模式或profile/scenario冲突；`invalid_evaluation`：评估结构、指标、任务数或带时区日期不合法 |
| 403 | `origin_forbidden`：写入请求来自其他Origin，或被浏览器标为cross-site |
| 404 | `profile_not_found`或`not_found`：配置、运行、执行段、引用artifact或评估不存在 |
| 409 | `profile_unavailable/profile_changed`、`already_running/not_resumable/not_running`：配置或任务状态不允许本次操作 |
| 413 | `request_too_large`：评估请求体超过128KiB；`evaluation_too_large`：序列化后的保存记录超过上限 |
| 429 | `capacity_reached`：已达服务并发上限（默认2）；`attempt_limit`：已达20次执行尝试 |
| 503 | `service_stopping/history_unavailable/evaluations_unavailable`：服务关闭中，或本地记录无法安全读取/保存 |

写入请求若带`Origin`，必须匹配当前协议、主机和端口；没有Origin的本地脚本请求仍可使用，`Sec-Fetch-Site: cross-site`会被拒绝。服务不开放CORS，页面使用同源内容安全策略和文本渲染；这些措施不提供登录或多人权限。路径参数只定位服务生成的记录及已引用artifact，不能用于读取任意文件。

## 当前可用范围

页面、服务端注册入口、运行/恢复/审计、来源目录和评估导入可独立验收。默认安装没有真实数据池与内容适配器，也没有预置API密钥；接入后需要单独做真实端到端和质量评估。既有真实模型并行验收HTTP403未在本轮解决。本轮开发和测试不调用外部模型。

`127.0.0.1`地址只能在本机使用。向组员展示可共享屏幕或发送下载的HTML报告；他人要运行应用，可在自己的项目副本启动。此版本没有公网部署、登录或多人权限系统。
