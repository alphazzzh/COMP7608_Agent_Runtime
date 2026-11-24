# Proposal 三组消融评估

本页对应 `proposal/proposal.tex` 第 3–4 节的受控比较。生产运行始终使用 `engine/agent_workflow.py` 和 `agent-loop-v1`，没有旧子任务状态机、自动回退或生产 `fixed_retrieval` 分支。数据与内容模块仍实现原异步端口；SQLite 恢复接口没有改变。

## 三组策略

| 报告 EvaluationMode | 实际运行策略 | 核验策略 |
|---|---|---|
| `fixed_retrieval` | 测试侧本地策略：搜索一次，按分数取最多 Top-k 来源，一个固定工具批次，一次 compose，finish | 不请求模型规划器或 verify |
| `no_verifier` | 模型自主选择动作和工具 | `require_verification=false`，verify 动作由图拒绝 |
| `full_agent` | 同一个模型自主动作循环 | `require_verification=true`，当前答案必须先获 supported 才能 finish |

运行器在 [tests/support/ablation.py](../tests/support/ablation.py)，统计在 [ablation_metrics.py](../tests/support/ablation_metrics.py)。固定策略实现本地 `decide`，通过公开 `run` 复用控制器的来源/schema/预算/重试/安全并行/审计约束；内部 RunRequest 使用 `no_verifier`，结果详情另外记录真实对照标签。它没有增加生产 RunMode，不能作为网页创建任务的模式。固定策略允许控制器对幂等临时错误执行同样的有限重试，但不会根据错误或答案重规划；不可恢复错误仍结束，失败也保留在评测分母中。

报告契约单独位于 [evaluation/models.py](../src/lake_agent/evaluation/models.py)，允许三组 `EvaluationMode`，不依赖 Web 或 FastAPI，不执行实验，也不读取评分标签。工作台报告存储位于 [web/evaluation_storage.py](../src/lake_agent/web/evaluation_storage.py)。因此 `evaluation/` 可以随 wheel 提供报告导入格式，而固定策略、合成端口和指标运行器始终留在 `tests/`，不会进入生产包。全部目录映射见[源码目录说明](source-layout.md)。

正式工作台默认关闭评估导入，不初始化报告仓库或创建评估目录；只有可信测试宿主显式传入 `enable_evaluations=true` 后才提供该能力。报告导入与三组实际执行是不同入口，工作台只展示已有测量结果。

Top-k 等于共享 `RunConfig.candidate_top_k`。具体工具与参数由宿主 `step_builder(candidate, tool_specs, question, index) -> AgentToolStep` 显式映射；生产核心不要求工具叫 extract。固定组候选在有界上下文中不完整时会明确停止，避免悄悄减少 Top-k。没有候选时允许单次空证据回答；该回答被拒后也不重新检索或生成，样本照常计入分母。

## 公平性与核验门禁

同一冻结任务清单、模型标识、采样参数、提示版本、Top-k、工具步数和调用/时间预算用于全部组。唯一配置差异是 Full 强制核验；固定组替换为本地规划策略。每个任务、重复和组均创建新端口与 TraceSink；跨运行的目录快照 ID 不一致时拒绝导出比较。宿主须保证相同快照 ID 代表不可变数据及同一模型配置，记录文件标识、取得时间和实际硬件条件；运行器无法证明外部服务履行这些承诺。

复杂任务的 DAG 只是模型可选工作记忆，启发式提示鼓励更新计划、先探测未知结构、按核验 issues/recommendation 补证；确定性图不锁定动作顺序。日常交互可使用 `require_verification=false`，由模型选择核验。Proposal Full 评估必须使用 true。`no_verifier + true` 在初始化、任何数据处理模块或内容模块调用之前返回 `failed/invalid_run_policy`。强制核验不能通过重复生成完全相同答案来清除失败 verdict；实际改变答案后须重新核验。

## 任务标注与输出

冻结 JSON 清单示意（只作格式示例，不是已取得的 LakeQA 标注）：

```json
[
  {
    "task_id": "task-001",
    "question": "What is the requested total?",
    "expected_answers": ["120"],
    "relevant_source_ids": ["source-annual"],
    "numeric_answer": 120.0,
    "numeric_tolerance": 0.01,
    "structured_answer_json": "120"
  }
]
```

缺少某种标注时省略字段或设 null；没有相关源标注与明确的空集合不同。`numeric_tolerance` 省略时为 0。数值标注可用 JSON 数字或精确数字字符串；当十进制小数无法无损导出为原有浮点格式时，详情留存其精确字符串。标签、数值容差和相关源在执行结束后才评分，不进入模型、内容模块或工具输入。只有问题与任务标识进入运行请求。

在运行真实 LakeQA 对照前，先在 runtime 源码目录把原始题单转换为一份冻结 JSON。转换只读取题单，用 `nodes` 中的来源作文件级对照标签，并保留数值答案。检查这些 ID 与内置数据端口在选定 `base_dir` 下报告的相对路径一致。

```bash
uv run --frozen python -m tests.support.lakeqa \
  /absolute/path/to/lakeqa-mini /absolute/path/to/frozen-manifest.json
```

下面的命令读取冻结清单并执行配置中的模型与数据端口。

```bash
uv run --extra web --frozen python -m tests.ablation_runtime \
  --config config.toml \
  --manifest /absolute/path/to/frozen-manifest.json \
  --step-builder tests.support.lakeqa:build_step \
  --dataset-id lakeqa-mini-frozen \
  --task-set-id mini-manifest-v1 \
  --repeats 3 \
  --output-dir /absolute/path/to/new-evaluation-directory
```

`tests.support.lakeqa:build_step` 为内置本地数据端口选择问题中的关键词，再对每个固定候选调用一次 `extract`。使用其他数据端口时，由评估宿主提供相应映射。输出目录必须不存在，不覆盖历史数据。命令保存 `report.json`（可导入评估工作台）、`details.json`（任务/配置摘要、每组有效配置、逐任务结果、集合和统计定义）以及 `audit/<run_id>/events.jsonl` 和工件。配置摘要不含 API 密钥或连接选项。CLI 错误只报告异常类型，端口进入/退出的错误也不会回显私有文本。

Python 调用可传入 `ports_factory`、`step_builder`、`trace_factory` 和可选的 `unnecessary(request) -> bool | None` 独立判定器。判定器只用于事后评分；CLI 默认不提供该判定，因此不必要调用率为 null。运行器不读取或更改实际任务的 SQLite，不承诺评估任务中断续跑；生产持久恢复不受影响。

## 指标定义与缺测

| 指标 | 当前计算口径 |
|---|---|
| accuracy | 对保留答案文本做 NFKC、casefold 和空白归一化后，与任一标准答案完全匹配；无答案计 0 |
| numerical_accuracy | 优先比较数值型 structured_result，否则答案全文必须是一个数；按精确十进制比较绝对误差，避免相邻大整数被浮点舍入为同一个数 |
| structured_accuracy | 优先使用 structured_result，否则解析答案全文 JSON；规范 JSON 相等、键顺序无关，布尔值与数字不同，数值类型须一致 |
| retrieval_precision/recall/F1 | 去重的控制器接纳工具来源集合，对比相关文件标注；搜索候选集合与所有已派发访问另行导出，不混作成功检索结果 |
| grounding | 去重的引用文件中属于相关源标注的比例；仅是文件相关性代理，不能证明每项事实被证据支持；无引用或无标注为 null |
| completion_rate | `status=completed` 的样本比例，与答案准确率分别计算；非 completed 但保留正确答案仍可有 accuracy=1 |
| valid_tool_call_rate | 控制器接纳的 ok ToolResult 尝试数 / 已审计工具调用意图数；来源或证据校验拒绝的结果不计入，也不声称其他调用对任务有用 |
| unnecessary_tool_call_rate | 独立判定为不必要的工具尝试 / 所有工具意图；缺少任一必要判定为 null，不把 inspect 或失败自动当作无用 |
| tool_calls_mean/data_calls_mean | 每个样本的工具预算账本计数；后者加上目录计数，含重试和已保留但未派发的调用额度 |
| retry_recovery_rate | 发生额外尝试且后续获得 ok 结果的逻辑操作 / 发生额外尝试的逻辑操作；没有重试时为 null |
| latency_ms_mean | 控制器运行耗时均值；包含其审计写入，不包含工厂创建/关闭时间 |
| provider_requests_total | 各样本保守记账的模型请求数；本地固定策略不计模型请求，回答/核验正常计费 |
| input/output_tokens_total、token_total | 实际上报用量合计；任一样本对应值未知则总量为 null |
| cost_usd | 仅合计已测得且统一为 USD 的模型费用；缺失、混币种或非 USD 为 null |
| external_data_bytes_total/external_data_cost_usd | 按实际目录与工具尝试合计独立测得的外部流量和美元费用；失败及重试按各自 `call_id` 计。任一必要测量缺失时该项为 null |

如需记账，Python 宿主传入 `data_usage_provider(data_port, run_id)`，或在 CLI 添加 `--data-usage-provider evaluation_adapter:read_usage`。函数在每个样本运行结束后、端口关闭前读取已收集的本地计量；原 DataPort 的请求、响应不增加字段。返回 `{"call-id": {"bytes_transferred": 123, "cost_usd": 0.01}}`，分别提供非负整数字节与实际 USD 费用。宿主负责对三组采用同一传输量测量边界，并从实际计费记录取得费用。按 `call_id` 记录所有本轮目录与工具尝试，包括失败和重试。未测到的字段用 `null` 或留空；只有确认未产生流量或费用时才写 `0`。未提供用量读取器的运行保持原来的 null，不能拿调用次数、正文长度或价格猜测替代实测。详情的 `data_attempt_usage` 可与运行审计核对；已明确未派发的意图不纳入计量。

质量指标仅对明确适用的任务×重复样本取宏平均（排除在清单中通过 `not_applicable_metrics` 显式标记为不适用的任务，并在 `metric_coverage` 中分别导出 `applicable_count`、`not_applicable_count` 与 `unknown_count`）；若任一样本该指标的适用性未知（既无标注也未标明不适用）或适用样本数为 0，则整组指标置为 `null`，避免只选择可评分任务。调用比率采用总计数作分子/分母。空调用集合的调用比率、没有重试的恢复率为 null。相关源明确为空时 recall 在未访问来源时为 1，否则为 0；precision 在访问集合为空时为 0，F1 按二者计算。

`details.variability` 保存耗时、工具调用、accuracy、F1、请求数和输入/输出 Token 在全部任务×重复样本上的均值、总体标准差、样本数和已知数；这不是逐任务重复实验的标准误或置信区间。原始逐任务结果保留，可按研究设计进一步分组计算。

## 离线验证

```bash
uv run --extra dev --extra web --frozen pytest -q tests/evaluation tests/engine tests/web
```

该目录级命令包含消融评估、动作门禁和工作台报告边界，具体测试文件见[测试导航](../tests/README.md)。验证使用 MockTransport、本地端口和临时 JSONL/SQLite，不请求真实 API、不启动监听服务。它覆盖三组执行路径、门禁、统计与报告边界，不是 LakeQA 或真实模型效果报告。完成真实标注与模块接入后，再按冻结清单测量 Proposal 的实际表现。


## LakeQA 批量运行、断点续跑与失败摘要

正式 runner 仍放在评测宿主 `tests/ablation_runtime.py`，不会进入生产 wheel。`--manifest` 可以是当前评测 JSON 数组、单条 JSON 任务，或包含 `task_*.json` 的 LakeQA mini 目录；任务字段兼容 `task_id`/`question_id`/`id`、`question`/`query`、`expected_answers`/`answer`、`relevant_source_ids`/`relevant_sources`/`datasets_used`，并记录 `hop`/`hop_id` 或 `reasoning_hops` 的深度。冲突别名会在任何端口创建前拒绝。

每完成一个 `mode × repeat × task`，runner 会刷盘 `samples.jsonl` 并原子更新 `checkpoint.json`。中断后使用相同的 manifest、配置、数据集标识、任务集标识、重复次数和可信 step builder 执行 `--resume`；身份不一致会拒绝恢复，已有样本不会再次调用数据、内容或规划器。完成后输出 `report.json`、`details.json`、`failures.json` 和完整 `samples.jsonl`。失败摘要按 `missed_sources`、`tool_errors`、`evidence_conflicts`、`unsupported_answers` 与 `other` 聚合；它是轨迹归类，不是答案质量的替代指标。

示例：

```bash
uv run --extra web --frozen python -m tests.ablation_runtime \
  --config config.toml \
  --manifest /absolute/path/to/frozen-manifest.json \
  --step-builder tests.support.lakeqa:build_step \
  --dataset-id lakeqa \
  --task-set-id lakeqa-mini-frozen-2026-11 \
  --output-dir /absolute/path/to/output/lakeqa-run \
  --repeats 3

# 进程中断后继续
uv run --extra web --frozen python -m tests.ablation_runtime \
  --config config.toml \
  --manifest /absolute/path/to/frozen-manifest.json \
  --step-builder tests.support.lakeqa:build_step \
  --dataset-id lakeqa \
  --task-set-id lakeqa-mini-frozen-2026-11 \
  --output-dir /absolute/path/to/output/lakeqa-run \
  --repeats 3 --resume
```
