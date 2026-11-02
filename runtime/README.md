# 异构数据池 Agent Harness

当前包为 **0.7.0**，核心[接口契约](../运行控制接口与异步执行契约.md)为 **v0.5**，JSON封套版本为 **1.0**。这是面向通用异构数据池的Python 3.11+异步运行控制框架；LakeQA只是评测载体。整个项目的目标、文档与提案入口见[项目README](../README.md)。

## 安装与启动

环境要求Python 3.11+与uv。网页与SQLite持久恢复依赖POSIX文件锁，支持macOS/Linux/WSL；原生Windows暂不支持。

如果其他项目曾设置旧版LangChain追踪旗标，启动前执行`unset LANGCHAIN_TRACING LANGCHAIN_HANDLER`；当前Core不支持这两项废弃全局配置。现代V2上下文由图局部隔离，本地审计无需LangSmith服务。

从仓库根目录（包含runtime文件夹）执行：

```bash
cd runtime
uv sync --extra dev --extra web --frozen
uv run --extra web --frozen lake-agent-web --data-dir runs/project-workspace
```

打开 http://127.0.0.1:8765 ，可使用智能问答、数据来源、任务历史和实验评估。默认项目配置尚未接入，选择明确标注的示例即可运行；示例使用合成资料和离线规则，不需要密钥、不发模型请求。服务只监听本机，保持终端运行；`Ctrl+C`关闭。

每次使用相同的`--data-dir`才能读取同一组历史。下载「任务报告」可获得独立HTML快照，下载「JSON」可获得运行记录。来源页只展示注册目录，评估页只导入已有测量结果，未知指标保持null。页面操作、服务端配置注册、HTTP接口和评估格式集中维护在[项目工作台说明](docs/project-workspace.md)。

不使用网页时，可直接运行三个离线模式：

```bash
uv run --frozen lake-agent-demo --mode full_agent --output ./runs
uv run --frozen lake-agent-demo --mode no_verifier --output ./runs
uv run --frozen lake-agent-demo --mode fixed_retrieval --output ./runs
```

命令打印`RunResult`并写入`runs/<run_id>/events.jsonl`与`artifacts/`。这些结果验证控制流程，不代表真实数据池答案或benchmark质量。

## 模块责任与Python入口

| 组件 | 负责内容 |
|---|---|
| DataPort | 目录快照、可信工具注册、数据处理与工具调用 |
| ContentPort | 本地目录准备和检索、答案组合、核验 |
| PlannerPort | 子任务分解与工具计划 |
| TraceSink | 请求/响应审计、完整结果artifact |
| StateStore（可选） | 原请求、调用意图和有效响应的持久记录 |
| 运行控制器 | 来源授权、校验、预算、超时、重试、调度和证据提交 |

在应用自己的异步函数中调用：

```python
from lake_agent.models import RunRequest
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink

request = RunRequest(task_id="task-001", question="比较不同来源的年度营收并给出依据。")
result = await run(
    request,
    data=data_adapter,
    content=content_adapter,
    planner=planner_adapter,
    trace=JsonlTraceSink("runs"),
)
```

端口协议见[ports.py](src/lake_agent/ports.py)，严格共享模型见[models.py](src/lake_agent/models.py)。应用负责端口客户端与事件循环生命周期；并发run需要独立trace、唯一run_id，以及可重入或独立的适配器。

## LangGraph流程编排

`workflow.py`使用LangGraph StateGraph组织目录、任务分解、子任务检索/工具循环、答案与核验反馈。`run/resume`和队友端口不变，原控制器继续负责授权、预算、调用与证据。没有额外图重试、缓存或图checkpointer；SQLite调用日志仍是恢复依据，取消时先等待图节点退出。节点和迁移边界见[LangGraph流程说明](docs/langgraph-workflow.md)。

## 已实现的控制能力

- 工具结果与可引用证据分开。完整结果保留于artifact，有界观察用于下轮规划；ReadObservationDecision只读取本run已知观察的JSON Pointer分页，不访问任意文件或URL。
- 规划上下文四字段默认上限24000 UTF-8字节，单字段预览目标2000字节，观察读取最多8次，相同工具动作默认最多接受2次。完整JsonPlanner请求另有256000字节上限；这些都不是模型token保证。
- 可信工具显式声明parallel_safe，且来源、互斥资源和依赖均无冲突时才允许并行。默认工具并发上限4，范围1–32；旧工具未声明时保持串行。控制器串行准备和结算，子任务规划与内容调用保持顺序。
- 可修正工具错误支持有界重新规划；重试仍受幂等声明、真实用量和原期限约束。适配器不得通过SDK隐式重试或后台请求绕过控制器。

完整字段、错误码、观察/步骤状态、并行规则及审计格式以[核心契约](../运行控制接口与异步执行契约.md)为准。

## 接入真实规划模型

内置 `JsonPlanner` 通过一次异步 OpenAI-compatible Chat Completions HTTPS 请求产生子任务或工具计划；它不执行工具、不生成最终答案、不自动重试。它不是本地部署适配器，也不使用 Responses API：调用方提供 API 根地址和 API key，适配器向 `{base_url}/chat/completions` 发送 `Authorization: Bearer ...` 请求。原生 `httpx` 已包含在运行时依赖中，不需要安装 provider SDK。

先在当前 shell 中设置连接信息；API key 不要写入仓库、LaTeX 文档或运行配置文件：

```bash
export MODEL_API_BASE_URL="https://api.openai.com/v1"
export MODEL_API_KEY="your-api-key"
export MODEL_ID="gpt-4o-mini"
```

OpenAI-compatible 的其他服务只需替换地址和模型名，例如 DeepSeek：

```bash
export MODEL_API_BASE_URL="https://api.deepseek.com/v1"
export MODEL_ID="deepseek-chat"
```

如果使用中转站，将 `MODEL_API_BASE_URL` 设置为中转站公开的 API 根地址（例如 `https://proxy.example/v1`），并使用该服务签发的 key。地址必须是绝对 HTTP(S) URL，不能内嵌用户名、密码、query 或 fragment；`JsonPlanner` 会自动补上 `/chat/completions`。

```python
import os
from lake_agent.planner import JsonPlanner

async with JsonPlanner(
    os.environ["MODEL_API_BASE_URL"],
    os.environ["MODEL_API_KEY"],
) as planner:
    request = request.model_copy(
        update={
            "config": request.config.model_copy(
                update={"model_id": os.environ["MODEL_ID"]}
            )
        }
    )
    result = await run(
        request, data=data_adapter, content=content_adapter,
        planner=planner, trace=JsonlTraceSink("runs"),
    )
```

还可将已配置的 `httpx.AsyncClient` 作为 `client=` 注入；注入者负责关闭客户端并禁用 transport 层重试。默认请求启用 JSON object 模式，随后用本地 Pydantic 类型校验完整输出。动态工具参数无法保证适用于各 provider 的 strict JSON Schema 子集，所以当前不宣称 provider 端 strict schema 校验。模型 ID 和允许的采样参数放在 `RunConfig.model_id/model_params`；实际项目中建议在构造 `RunRequest` 时设置，而不是修改共享请求对象。

`JsonPlanner` 只覆盖 `PlannerPort.decompose/select_tools`。当前仓库没有通用的真实 `ContentPort` 实现；`compose`、`verify` 仍需由接入方提供，因此不能把仅替换规划器后的运行描述成真实 full-agent 已联调。离线 `DemoPlanner`/`DemoContent` 完全不调用网络模型。

真实 adapter 需满足 `ports.py` 中的异步 Protocol，并遵循共享模型。所有本地 `ModuleResult` 都应显式回报 `Usage.local()`；若遗漏 usage，运行时会按 fail-closed 规则保留一次 provider 请求，防止低报实际调用。首版不支持累计输入/输出 token 硬预算；配置 `max_input_tokens` 或 `max_output_tokens` 时，运行时会在任何数据/模型 adapter 调用前拒绝启动，因为泛化 adapter 尚未声明精确 tokenizer 和 usage 对账能力。未测 token/cost 保持 `null`。Adapter 的 timeout 是合作式边界；忽略取消的 coroutine 或线程不能被 asyncio 强制终止。

## 持久化与恢复

`run(..., state_store=store)`可启用SQLite持久状态；后续`resume(run_id, ...)`用原请求与已验证响应重建控制器。每次尝试使用fresh端口和独立审计段；应用最终关闭StateStore。普通无状态run继续可用，JSONL本身不提供恢复能力。

恢复要求相同运行时源码/版本和图引擎依赖版本；继续未终结运行还校验可信工具契约，原绝对期限包含停机时间，预算不会重置。历史已知调用不重新派发；存在started但没有有效completed响应的未知调用时，拒绝自动恢复。详细示例、错误码、辅助目录注册与取消语义见[持久恢复接入说明](docs/durable-recovery.md)。

## 验证与实际边界

0.7.0完整离线回归为443项通过（14.46s），构建与源码外全新环境的真实本机HTTP安装验证通过。0.6.0实际浏览器验收保留，当前前端静态文件未改动。完整范围见[验收记录](docs/project-workspace-acceptance-2026-11-02.md)，包身份与原始检查结果见[安装证据](docs/installed-wheel-web-2026-11-02.json)。历史SIGKILL验证与真实模型403结果集中见该验收记录的「历史专项证据」。

在本目录复核当前版本：

```bash
uv run --extra dev --extra web --frozen pytest -q
uv run --extra dev --extra web --frozen ruff check src tests scripts
uv run --extra dev --extra web --frozen ruff format --check src tests scripts
uv run --frozen python -m compileall -q src tests scripts
node --check src/lake_agent/web/static/app.js
uv build
uv run --frozen python scripts/verify_installed_web.py --report runs/installed-web-check.json
```

安装验证默认只从uv缓存安装依赖，缓存不足时可加`--allow-downloads`下载公开依赖；日常复核报告写入runs，避免覆盖归档证据。默认pytest使用本地fixtures和MockTransport，不发外部模型请求。`verify_installed_recovery.py`是硬编码0.4.0 wheel的历史专项，不用于认证当前0.7.0源码；当前恢复回归包含在上述完整测试及Web安装验收中。

真实规划器专项脚本仍位于`scripts/live_controller_acceptance.py`、`live_harness_acceptance.py`和`live_parallel_acceptance.py`，它们会调用外部API。`live_controller_acceptance.py`支持覆盖地址和模型；另两个脚本仍带历史专项服务/模型默认值，应先查看各脚本`--help`与源码；优先读取进程环境MODEL_API_KEY，历史服务也支持本机钥匙串凭据。普通使用和本轮文档整理无需运行这些专项。

真实DataPort/ContentPort与benchmark尚待联调；只替换JsonPlanner不代表真实完整Agent已集成。当前没有累计token硬预算、动态worker委派、跨版本恢复或外部副作用exactly-once保证。SQLite后端限同一POSIX主机的可信本地文件系统；取消依赖适配器合作，不能强杀阻塞线程或忽略取消的协程。
