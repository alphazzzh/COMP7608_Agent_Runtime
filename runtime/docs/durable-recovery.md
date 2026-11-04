# 持久恢复接入说明

持久模式为异构数据池的单控制器保存原请求、调用意图和已验证响应。进程中断后，控制器通过这些已知响应重新建立证据、观察、步骤与预算状态，再决定是否继续新工作。它复用现有授权和调度规则；Lake 数据集仍仅是评测载体。

公开入口保留在 [runtime.py](../src/lake_agent/runtime.py)，SQLite 存储保留在 [state.py](../src/lake_agent/state.py)。恢复协调器归入 [engine/recovery.py](../src/lake_agent/engine/recovery.py)，图执行收尾归入 [engine/graph_runner.py](../src/lake_agent/engine/graph_runner.py)；调用方无需依赖这些内部模块。目录完整映射见[源码目录说明](source-layout.md)。

## 接口与资源生命周期

普通 `run(request, *, data, content, planner, trace)` 保持原行为。传入 `state_store=SqliteStateStore(path)` 后启用持久模式；后续调用 `resume(run_id, *, data, content, planner, trace, state_store)` 使用数据库中的原请求，无需也不接受一份替代请求。

调用方在首次运行前设置并保存唯一 `RunRequest.run_id`；自动生成的 ID 若尚未返回就遇到进程退出，应用可能无法定位要恢复的运行。再次调用 `run` 不能覆盖数据库中的同名运行；已有运行使用 `resume`。

每次执行尝试都重新创建 `DataPort`、`ContentPort`、`AgentPlannerPort` 和 TraceSink 实例。新进程不能依赖旧实例的缓存、打开连接或临时 handle。应用负责端口客户端的创建与关闭，且需要保证新实例仍履行原数据快照和工具语义契约。凭据继续由适配器配置持有，不应混入请求、工具参数或持久状态。

StateStore 由调用方持有；`run` 和 `resume` 在退出时释放各自租约，但不会关闭调用方的存储对象。应用应使用 `try/finally` 调用 `await store.aclose()`。同一存储可保留到应用生命周期结束，也可在后续进程中用同一路径创建新实例。

持久模式要求审计端口额外实现 `create_attempt(run_id, attempt_id)`。`JsonlTraceSink` 已提供此方法；不支持它的自定义端口会收到 `recovery_trace_unsupported`，不会静默退回普通审计模式。

以下最小示例在应用的异步函数中执行，应用事先创建并管理三类适配器及其客户端。两段分别代表首次运行和后续进程中的恢复尝试；正常终态无需再恢复，不应把它们当作每次启动都顺序执行的脚本。

```python
from lake_agent.models import RunRequest
from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink

request = RunRequest(
    run_id="pool-run-001",
    task_id="compare-source-definitions",
    question="比较两类来源对同一指标的定义，并给出可引用依据。",
)
store = SqliteStateStore("runs/state.sqlite3")
try:
    result = await run(
        request,
        data=data_adapter,
        content=content_adapter,
        planner=planner_adapter,
        trace=JsonlTraceSink("runs"),
        state_store=store,
    )
finally:
    await store.aclose()
```

在后续进程中导入相同类型，重新创建三类适配器，并打开同一个状态数据库。这里的`fresh_*_adapter`表示本次新建的实例，不能复用已关闭的客户端：

```python
store = SqliteStateStore("runs/state.sqlite3")
try:
    result = await resume(
        "pool-run-001",
        data=fresh_data_adapter,
        content=fresh_content_adapter,
        planner=fresh_planner_adapter,
        trace=JsonlTraceSink("runs"),
        state_store=store,
    )
finally:
    await store.aclose()
```

示例中的相对路径按进程工作目录解释；恢复进程必须打开原数据库，沿用原审计根目录可将各次attempt保存在同一run目录下。原请求中`max_wall_time_ms`的默认值不会因恢复而刷新；需要较长中断窗口时，应在首次运行前按任务需要设置该配置。

## 持久记录与独立审计分段

StateStore 使用两种调用记录：派发前持久化 `started` 意图；响应返回并通过类型、身份与大小校验后，替换为带完整响应的 `completed` 记录。同一调用只能开始一次、完成一次。工具 worker 在有效响应返回后立即保存自己的结果，不等待同批其他工具或后续审计事件提交。

数据库同时保存原请求、运行时与源码摘要、可信工具摘要、原始开始时间与绝对 deadline、身份序列种子、执行尝试列表、最近进度以及可用的终态结果。恢复的依据是已验证调用记录；最近进度仅提供检查信息，不代表可以跳过请求匹配或未知结果检查。

启用持久模式后，每次实际执行写入独立目录：

```text
runs/
  state.sqlite3
  state.sqlite3.locks/
  <run_id>/
    attempts/
      <attempt_id>/
        events.jsonl
        artifacts/
```

原 run ID 保持不变，每个审计分段的事件序列从一开始，既有分段与 artifact 不被覆写。历史调用在新分段中再次产生审计事件时带 `detail.replayed=true`，不表示再次派发了该调用。不能直接汇总所有分段的调用开始事件来推断真实 provider 请求数。

省略 StateStore 的普通运行继续使用 `runs/<run_id>/events.jsonl`。这两种方式的 JSONL 都是审计记录，不能单独用于恢复。

图流程从START开始按已知调用日志重建，没有第二套图checkpointer；迁移机制见[LangGraph流程说明](langgraph-workflow.md)。

## 恢复前提与执行规则

恢复严格要求相同的运行时版本、实际 Python 源文件摘要，以及LangGraph、LangChain-Core、checkpoint和LangSmith依赖版本；仅保留相同包版本号不足以通过源码校验。继续未终结运行时还要求相同的可信工具契约；复用已保存终态时不重新读取工具清单。工具摘要来自重新校验的 `DataPort.tool_specs()`，包括工具 schema 和可信能力声明。新端口仍须遵守原不可变数据快照的语义；控制器不会将恢复当作跨数据版本迁移。

源码摘要覆盖根目录公共 Python 模块，并递归包含 `engine/` 与 `adapters/`，以包内相对路径和文件内容共同计算。`evaluation/` 报告格式及 `web/` 展示层不参与执行身份计算。此次目录迁移会改变摘要，因此旧版本未终结运行不能直接恢复；不修改原数据库，也不进行跨版本迁移。

控制器按稳定调用 ID 查找历史响应，不按并行 worker 的返回顺序读取。重建时逐项比较 phase 与请求，只规范化随时间变化的 timeout、剩余 wall time，以及经原 snapshot/digest 重新注册确认的内容 handle。请求分叉、未知调用或不兼容记录不会退回一次真实 adapter 调用来“试试看”。

历史响应经过现有控制流程重新结算一次，恢复原证据、观察、身份和已消费预算；历史重试不会再次等待退避时间。全部较早记录匹配后，或匹配记录属于当前已完整准备的工具批次时，才允许越过历史进入新派发。新工作继续受原授权、剩余额度、并行上限、来源冲突与互斥资源规则约束。

原绝对 deadline 包含停机时间。恢复不会重新获得一份完整 wall-time 预算；原期限已到时，未终结的运行以 `recovery_deadline_expired` 停止恢复。应用应在首次创建请求时，根据实际任务和允许的中断窗口选择 `max_wall_time_ms`。

## 重新注册内容目录的真实额外调用

当恢复需要继续新工作，且历史已完成目录注册时，控制器先在新的 ContentPort 上重新调用本地 `prepare_catalog`。它使用独立调用身份和剩余期限，核对原 `catalog_snapshot_id` 与 SHA-256 digest，获取当前实例可用的新 handle。控制器不会让新 ContentPort 直接使用旧进程的临时 handle，也不会因此重新派发已保存的 DataPort 目录检索。

这次辅助注册是真实的额外 content 调用，写入独立的 started/completed 记录和配对审计事件，累计进入 `call_counts.content`；它不伪装为免费历史回放。接入方必须将其实现为本地操作，返回 `Usage.local()`，即 `provider_requests=0`。snapshot/digest 或用量不符以 `recovery_catalog_mismatch` 拒绝继续。若辅助注册中断而结果未确认，后续恢复同样遵循未知结果停止规则。

## 结果已知与结果未知的边界

对于尚未终结的运行，只要任何调用保留 `started` 而没有有效、持久化的 `completed` 响应，就以 `StateStoreError.code == "recovery_outcome_unknown"` 拒绝自动继续。持久化意图不能证明 adapter 已经执行，也不能证明它尚未执行；即使工具声明幂等，也不会自动重放该未知调用。

因此，取消发生在所有相关响应保存之后时，可能仍可重建并继续；取消发生在 adapter 进行中，或者响应尚未持久化时，则不能承诺可恢复。存储或审计失败也不自动意味着可以重试，仍取决于记录是否完整、结果是否已知。

已保存的正常终态直接返回原 `RunResult`，保留其中的结果、计数与 `trace_ref`，不再调用 adapter。这里包括成功、预算停止以及正常模块错误；例如已记录的 HTTP 403 不会因为 `resume` 重新获得模型请求机会。审计或状态存储提交失败的停止保留恢复入口，但仍须通过完整性与未知结果检查。

| 条件 | 调用方可观察的结果 |
|---|---|
| 同一运行已有有效租约 | `lease_conflict`；不允许第二个所有者继续 |
| 数据库版本或结构不受支持 | `schema_mismatch`；不修改该数据库 |
| 已保存 JSON 或完整性摘要损坏 | `corrupt_state` |
| 运行时版本或实际源码不同 | `recovery_version_mismatch` |
| 可信工具契约改变 | `recovery_contract_mismatch` |
| 未终结运行存在未知调用结果 | `recovery_outcome_unknown` |
| 未终结运行已超过原期限 | `recovery_deadline_expired` |
| 重建请求或内容目录不匹配 | `recovery_request_mismatch` / `recovery_catalog_mismatch` |
| 已保存可返回的终态 | 原 `RunResult`；不重新请求模型或执行工具 |

恢复错误通过 `StateStoreError.code` 判断；错误消息不输出完整存储内容、SQL、路径或适配器配置。

## 本地存储边界与验证入口

`SqliteStateStore` 使用标准库 SQLite、WAL、`synchronous=FULL`、逐记录 SHA-256 完整性检查和私有权限。相邻目录中的 POSIX `flock` 文件提供本机非阻塞单运行独占所有权；进程退出后由操作系统释放锁，新所有者使用新令牌。锁文件保留原位，调用方不应在运行期间删除或替换数据库和锁目录。

磁盘工作在事件循环之外执行；调用方取消时，存储等待已派发线程结算后再传播取消，避免后台写入继续修改后续所有者的状态。已创建但尚未交给调用方的租约会先被释放。磁盘等待仍受 SQLite 的有界锁等待约束，不应将存储取消理解为立即终止系统调用。

此后端限定同一 POSIX 主机的可信本地文件系统，不支持 Windows、跨主机分布式租约或网络文件系统协调。当前实现不恢复任意协程、线程或外部进程的执行指令，不提供跨版本迁移、外部副作用 exactly-once 保证或动态 worker 委派。

专项离线验证入口：

```bash
uv run --extra dev --extra web --frozen pytest -q tests/recovery
```

恢复回归集中于 `tests/recovery/`，更细的文件定位见[测试导航](../tests/README.md)。覆盖范围包括真实 SQLite 与跨进程锁、取消等待与资源释放、独立审计尝试、全新端口重建、已知调用不重复派发、预算与原期限保留、辅助目录注册以及未知结果拒绝。离线 fixture 验证控制与持久化行为，不证明真实数据池检索、答案质量或真实模型的恢复联调。

历史固定版本和合成数据安装专项已移除。当前离线恢复回归包含在 pytest；安装配置验证使用 `scripts/verify_installed_web.py`，不启动服务。历史结果仅适用于其归档版本。

此前受控并行的真实规划器验收在后续请求收到 HTTP 403，完整真实模型流程保持未通过；本次恢复功能不能将该历史失败改写为通过。原始结论与报告入口见[历史专项证据](project-workspace-acceptance-2026-11-02.md#历史专项证据)。

## 自主决策引擎的恢复边界

当前唯一支持 agent-loop-v1，不再保留旧子任务图或协议回退。planner.decide 请求和严格动作响应进入同一调用账本；已完成决策在恢复时复用，不重新询问模型。持久状态保存 planner_engine，重建前检查当前规划器的协议声明。协议变化使用 recovery_engine_mismatch 拒绝，源码或依赖变化仍使用 recovery_version_mismatch。上下文不含瞬时时钟/审计序号，决策轮数与有界页面从已知响应确定性重建；不能重置 max_agent_turns、模型调用数或原运行期限。A/C 端口的恢复协议不变。日常 require_verification=false 和实验 full_agent=true/no_verifier=false 的策略均写入原 RunConfig，恢复不能改变策略；固定检索评估基线不作为应用执行模式暴露，而由测试侧本地策略复用相同控制器；评估脚本不启用 SQLite 恢复。
