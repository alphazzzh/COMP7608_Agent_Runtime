# LakeQA 与 Runtime 完整实验结果

实验日期为 2026 年 11 月 21 日，运行环境为 Apple M4、16 GiB 内存、macOS 15.6.1，Python 3.12.12、LangGraph 1.2.19、DuckDB 2.0.1、SQLGlot 30.33.0。本文档汇总全部离线测试、Runtime 基础设施微基准、受控集成消融实验、真实 LakeQA 冻结子集消融实验以及前期全量压测的实验设置与实测结果。

## 1. 离线功能测试与静态检查

最新全量离线测试执行命令为 `PYTHONPATH=src .venv/bin/python -m pytest -q -p no:cacheprovider`，结果为 **1,085 passed, 17 skipped**，耗时 **35.62 秒**。代码静态检查 `.venv/bin/ruff check src tests` 全部通过（0 错误、0 告警）。

测试覆盖范围包括适配器（DataPort / ContentPort / JsonPlanner）、接口契约、执行引擎（来源授权、预算、超时、重试、安全并行、目录上下文上限、搜索去重与熔断、核验新鲜度与内容自纠错）、评估指标与断点续跑、持续任务（Persistent Missions）、SQLite 崩溃恢复与 Web 工作台。

## 2. Runtime 基础设施微基准

为测量与问答准确率解耦的底层运行时行为，在本地直接调用生产控制器、`SqliteStateStore`、`MissionStore`、`RunContinuation`、`normalize_query` 与 `LocalDataPort` 执行了 4 组确定性微基准（不发起外部模型请求）。

| 基准项 | 测试条件 | 实测结果 |
| --- | --- | --- |
| **安全并行调度与资源互斥** | 4 个独立 80 ms I/O 工具步骤，对比串行（`parallel_safe=False`）、并行（`parallel_safe=True`，4 个独立来源）与共享互斥键（`exclusive_resource_keys=["duckdb-shared"]`） | 串行 `612.4 ms`（冷态）/ `356.3 ms`（热态互斥，并发度 1）→ 并行 **`108.4 ms`**（实测并发度 **4**，相对热态提速 **3.29×**、冷态 **5.65×**）；三种模式下证据顺序均保持确定；共享互斥键下并发度自动退回 **1** |
| **SQLite 崩溃恢复与 Fail-Closed** | 场景 A：证据落盘后中断并调用 `resume()`；场景 B：工具调用执行中途中断并调用 `resume()` | 场景 A：`resumed_status=completed`，恢复耗时 **`4.4 ms`**，**新增目录调用 = 0、新增工具调用 = 0**；场景 B：触发 **`recovery_outcome_unknown`** 拒绝自动重派，**新增工具调用 = 0** |
| **持续任务多周期与硬预算截停** | 创建 `max_runs=3`、`max_model_calls=10`、`max_data_calls=15` 的持续任务，连续运行 3 周期并携带 `RunContinuation` 交接，再尝试触发第 4 周期与超长交接 | 3 周期完成后自动置 `completed`（累计 `used_runs=3`、`used_model_calls=6`、`used_data_calls=3`）；第 4 周期被 **`mission_closed`** 拒绝（预算超支 **0**）；实测交接最大 `254 B ≤ 4096 B`，超过 4096 B 的交接被校验拦截 |
| **只读 SQL 沙箱与 16 MiB 防护** | 8 类越权/恶意 SQL（`read_csv_auto` 外部文件读取、未授权表、schema 前缀、递归 CTE、`DROP`、`DELETE`、`getenv`、多语句注入）、3 类合法分析 SQL、1 个 >16 MiB CSV | 越权 SQL 拦截率 **`100.0%`（8/8）**，平均解析拦截耗时 **`1.201 ms`**；合法查询放行 **`100.0%`（3/3）**；超过 16 MiB 的文件返回 **`source_too_large`** |

此外，历史独立 wheel 验收记录（`installed-wheel-recovery-2026-11-01.json`）在真实 `SIGKILL (-9)` 进程强杀恢复测试中同样验证了 `new_data_catalog_calls=0`、`new_data_tool_calls=0` 与 `evidence_identity_preserved=true`。

## 3. 受控集成消融实验（45 样本）

### 3.1 合成任务构造与实验设置

本实验旨在隔离外部 LLM 输出的随机性，单独验证多工具协同、内容反馈纠错与核验门禁机制。实验在本地目录中构造了 5 份确定性合成文件（`shipping-policy.txt`、`sales-revenue.csv`、`currency.json`、`inventory-quantity.txt`、`storage-capacity.txt`），接入正式 `LocalDataPort`、DuckDB 与 `JsonVerifierAdapter`，通过 `httpx.MockTransport` 模拟模型端点，并在特定场景注入受控故障。5 道任务在三组模式（`fixed_retrieval`、`no_verifier`、`full_agent`）下各重复 3 次，共 **45 个样本**（`candidate_top_k = 3`）：

| 任务 ID | 数据来源与格式 | 标准答案 | 构造意图与故障注入方式 |
| --- | --- | --- | --- |
| `policy` | `shipping-policy.txt`（TXT） | `Earth-friendly packaging` | 单文件文本抽取基线（数值/结构化指标标记为不适用） |
| `revenue` | `sales-revenue.csv`（CSV，2 行金额 50/70）+ `currency.json`（JSON，币种 HKD） | `120 HKD`（数值 `120`） | 跨异构文件取证：自主组调用 DuckDB `SELECT SUM(amount)` 结合 JSON 抽取；固定检索仅做关键词抽取无法聚合 |
| `inventory` | `inventory-quantity.txt`（TXT，真实库存 34） | `34` | 数值冲突负例：首次 `compose` 人为将数值偏移 `+1` 输出 `35`，核验器返回 `conflicting`，检验 `full_agent` 依据 `revision_instructions` 定向修订并重验 |
| `capacity` | `storage-capacity.txt`（TXT，真实容量 18） | `18` | 非法引用负例：首次 `compose` 人为输出未注册的证据 ID `unregistered-evidence`，检验 `content_feedback-v1` 拦截并驱动重生成 |
| `temperature` | 无（`relevant_source_ids = []`） | `20`（目录中故意缺失该数据） | 证据缺失负例：检验无核验组直接提交无依据回答与完整组以 `insufficient` 终止的差异 |

数值与结构化准确率按每组 12 个适用样本计算（排除 `policy`），其余指标按每组 15 个样本计算。

### 3.2 指标对比

| 指标 | `fixed_retrieval` | `no_verifier` | `full_agent` |
| --- | ---: | ---: | ---: |
| 答案准确率（`accuracy`） | 20%（3/15） | 60%（9/15） | 80%（12/15） |
| 任务完成率（`completion_rate`） | 80%（12/15） | 100%（15/15） | 80%（12/15） |
| 数值 / 结构化准确率（12 适用样本） | 0% | 50% | 75% |
| 检索 Precision / Recall / F1 | 80% / 100% / 80% | 80% / 100% / 80% | 80% / 100% / 80% |
| 平均工具调用数 / 有效工具调用率 | 1.00 / 100% | 1.00 / 100% | 1.00 / 100% |
| 模拟模型请求总数 / 模拟 Token 总数 | 15 / 750 | 18 / 900 | 36 / 1,800 |
| 本地执行延迟均值 ± 总体标准差（ms） | 32.1 ± 64.4 | 187.9 ± 347.6 | 177.3 ± 313.2 |

### 3.3 机制对照结果

- **跨文件取证（`revenue`）：** 两个自主组输出 `120 HKD` 并保留双来源引用；固定组成功调用了工具但未取得聚合结果。
- **数值冲突与修订（`inventory`）：** 无核验组直接提交错误值 `35`；完整组收到 `conflicting` 核验意见后修订为 `34` 并通过核验。
- **非法引用纠错（`capacity`）：** 固定组在生成被拒后停止；两个自主组收到 `content_feedback` 后重新生成带有效引用的 `18`。
- **证据缺失（`temperature`）：** 无核验组提交无证据回答（完成率 100% 但错误）；完整组以 `insufficient` 终止，因此完整组完成率（80%）低于无核验组（100%）。

## 4. 真实 LakeQA 冻结子集消融实验（主实验，`subset20-r1`）

### 4.1 语料范围、20 题抽样规则与运行参数依据

- **语料范围与异构构成：** LakeQA public mini 冻结全集含 **135 道题**、**478 个不同标注来源**（取自 `nodes`）。本地语料已下载适配器支持的全部 **898 个文件**（共 14,294,547,406 字节，覆盖 100% 标注来源），物理格式包含 `.txt`（799 份）、`.json`（89 份）、`.csv`（5 份）与 `.pdf`（5 份），分属 `datagov` 结构化/半结构化数据（686 份）与 `wikipedia` 非结构化百科正文（212 份）；另有 8 个 XLSX/HTML/DOCX 文件超出本地适配器读取范围。
- **20 题冻结子集抽取规则（确定性抽样，无人工挑选）：**
  1. **文件大小过滤：** 从 135 题中剔除任一金标文件超过本地 DuckDB 16 MiB 单表查询上限的 62 道题，得到 **73 道全金标文件均 ≤ 16 MiB 的候选池**（其中纯数字答案 48 道、文本答案 25 道；原清单中 73 题标注的 `hop_count` 均为 1）。
  2. **排序与分层均衡抽取：** 将 73 道候选按 `(最大金标文件字节数升序, task_id 升序)` 确定性排序，拆分为数值题与文本题两个队列，采用 **1:1 交替轮询（interleaved round-robin）** 抽取前 **20 道题**（**10 道数值题 + 10 道文本题**，全部金标文件 ≤ 0.91 MiB，单题涉及金标文件数覆盖 1 至 12 份；其中 **19/20 题**同时跨越 `datagov` 结构化表格与 `wikipedia` 非结构化正文，129 份金标文件含 `datagov` 75 份、`wikipedia` 54 份）。
  3. **标注补齐：** 对标准答案匹配纯数字正则 `^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$` 的 10 道题补齐 `numeric_answer`（整数容差 `0`，小数容差 `0.01`），非数值题标记 `not_applicable_metrics=["numerical_accuracy", "structured_accuracy"]`，数值题标记 `not_applicable_metrics=["structured_accuracy"]`，以消除整组指标因未知适用性而留空的问题。
- **三组对照策略：**
  - `fixed_retrieval`：固定关键词搜索 Top-5 来源，对每个候选执行一次 `extract`，再执行一次 `compose` 并结束。
  - `no_verifier`：模型自主选择搜索、工具与作答，禁用核验（`require_verification=false`）。
  - `full_agent`：同一模型自主决策循环，强制当前完整答案获得 `supported` 核验后方可 `finish`（`require_verification=true`）。
- **共享配置及前期测试依据：** 模型 `deepseek-flash`，`temperature = 0`，`candidate_top_k = 5`，`per_call_timeout_ms = 60000`，`max_wall_time_ms = 240000`，`max_model_calls = 12`，`max_data_calls = 20`，`max_plan_steps = 12`，`planner_catalog_preview_bytes = 1500`，`planner_catalog_max_sources = 10`，`deduplicate_searches = true`，`max_consecutive_searches = 3`，`repeats = 1`，`shards = 2`（共 **60 个样本**，总消耗 **1,991,285 tokens**）。各项关键参数均经前期压测与两轮 3 题 Pilot 标定：
  - **目录上限（`1500 B` / `10` 条）：** 前期 `live-r3` 与初始单题 Pilot 显示默认 24 KB 上下文每轮塞满 ~158 条无关目录（单次请求 ~10k input tokens）；独立限制内联目录后单样本总消耗降低 31%–59%，其余目录按需通过 `search` 和 `read_catalog` 检索。
  - **搜索去重与 3 次连续搜索熔断：** 初始单题 Pilot 观测到无状态规划器在 `temperature = 0` 下连续 11 轮重复同一搜索；开启 `deduplicate_searches = true` 与 `max_consecutive_searches = 3` 后消除了空搜死循环。
  - **调用上限（`max_model_calls = 12`、`max_plan_steps = 12`）：** 第一轮 3 题 Pilot 曾设 `max_model_calls = 8`、`max_plan_steps = 8`，实测跨 4 文件多跳题（`EQA000229`、`EQA000230`）在找到答案前即触发 `model_call_budget` 或 `step_budget`；调回 `12 / 12` 后，第二轮 3 题 Pilot 中 `full_agent` 在 `EQA000003` 上顺利完成并得分。
  - **并发与轮次（`shards = 2`、`repeats = 1`）：** 前期 `live-r3` 使用 24 并发触发 734 次 HTTP 429/402 拒绝；降至 2 并发后 60 个样本实现 **0 次 429/402**；采用 `repeats = 1` 并在 20 题冻结子集上汇报跨任务均值与总体标准差。
- **评分口径：** 答案准确率（`accuracy`）对标量 `structured_result` 或 `text` 做 NFKC、大小写与空白归一化后的精确匹配；数值准确率（`numerical_accuracy`）按精确十进制比较数值答案；检索 Precision/Recall/F1 按控制器接纳的成功工具访问文件集合计算；文件级 Grounding 按引用文件与标注来源的重合比例计算。

### 4.2 三组定量指标对比

| 类别 | 指标 | `fixed_retrieval` | `no_verifier` | `full_agent` |
| --- | --- | ---: | ---: | ---: |
| **数据集结果指标** | 样本数 | 20 | 20 | 20 |
| | 答案准确率（`accuracy`） | 0.0%（0/20） | 10.0%（2/20） | 10.0%（2/20） |
| | 数值准确率（`numerical_accuracy`, 10 题） | 0.0%（0/10） | 20.0%（2/10） | 20.0%（2/10） |
| | 检索精确率（`retrieval_precision`） | 11.0% | 49.2% | 58.0% |
| | 检索召回率（`retrieval_recall`） | 10.0% | 25.8% | 30.7% |
| | 检索 F1（`retrieval_f1`） | 10.3% | 31.2% | 37.5% |
| | 文件级 Grounding（有引用样本条件均值） | 18.4%（n=19） | 100.0%（n=3） | 100.0%（n=3） |
| **执行层指标** | 任务完成率（`completion_rate`） | 95.0%（19/20） | 10.0%（2/20） | 5.0%（1/20） |
| | 有效工具调用率（`valid_tool_call_rate`） | 100.0% | 92.5% | 84.9% |
| | 平均工具调用数（`tool_calls_mean`） | 5.00 | 2.65 | 3.25 |
| | 异构工具调用拆分（`query` SQL / `extract` 文本） | 0 / 100（0 题混用） | 40 / 13（3 题双工具混用） | 44 / 21（6 题双工具混用） |
| | 平均目录+工具调用数（`data_calls_mean`） | 9.00 | 6.65 | 7.25 |
| | 延迟均值 ± 总体标准差（秒） | 8.57 ± 12.23 | 45.24 ± 25.08 | 57.53 ± 50.89 |
| | 模型请求总数（含失败派发） | 20 | 190 | 176 |
| | 已知样本 Token 总量 | 34,058（19/20） | 1,050,699（20/20） | 906,528（18/20） |
| | 已知样本 Token 均值 ± 标准差 | 1,793 ± 492 | 52,535 ± 20,449 | 50,363 ± 26,044 |
| | 服务端拒绝（HTTP 429 / 402） | 0 | 0 | 0 |

### 4.3 来源流转漏斗（至少命中 1 份标注来源的样本数，分母 = 20）

| 来源阶段 | `fixed_retrieval` | `no_verifier` | `full_agent` |
| --- | ---: | ---: | ---: |
| 搜索候选（`searched_sources`） | 8（40%） | 18（90%） | 17（85%） |
| 已发起访问（`visited_sources`） | 8（40%） | 11（55%） | 14（70%） |
| 已接纳证据（`evidence_sources`） | 7（35%） | 11（55%） | 14（70%） |
| 最终引用（`cited_sources`） | 6（30%） | 3（15%） | 3（15%） |

### 4.4 终止原因分布

| 终止原因（`stop_reason`） | `fixed_retrieval` | `no_verifier` | `full_agent` |
| --- | ---: | ---: | ---: |
| `agent_finished`（正常完成） | 19 | 2 | 1 |
| `model_call_budget`（达到 12 轮调用上限） | 0 | 8 | 9 |
| `invalid_model_output`（连续 JSON/Schema 校验失败） | 0 | 6 | 4 |
| `tool_error`（SQL 执行错 / 非法查询 / 超 16 MiB 文件） | 0 | 4 | 3 |
| `call_timeout` / `time_budget` / `invalid_observation_read` | 1 | 0 | 3 |

### 4.5 10 道数值题逐题结果

| 题号 | 金标文件数 | 标准答案 | `no_verifier` 结果 | `full_agent` 结果 | 观察说明 |
| --- | :---: | --- | --- | --- | --- |
| `EQA000003` | 1 | `2007` | 正确（`2007`，completed） | 正确（`2007`，completed） | 单表 SQL 聚合，两组均正确完成 |
| `EQA000229` | 4 | `115` | `model_call_budget`（命中 3/4 金标） | `model_call_budget`（命中 3/4 金标） | 12 轮耗尽，未及 `compose` |
| `EQA000493` | 6 | `830` | `invalid_model_output`（3 轮） | 正确（`830`，`model_call_budget`） | 完整组已算出 `830`，但撞 12 轮未及 `verify` |
| `EQA000771` | 6 | `99357947` | 正确（`99357947`，completed） | `invalid_model_output`（3 轮） | 无核验组跨 6 文件求解完成 |
| `EQA000481` / `482` / `759` | 6 | `1650` / `1832` / `1585` | `model_call_budget` / `invalid_model_output` | `model_call_budget` | 跨 6 文件多条件聚合超出 12 轮预算 |
| `EQA000764` | 6 | `816` | `tool_error`（`source_too_large`） | `tool_error`（`source_too_large`） | 探索到非金标超大文件触发 16 MiB 上限 |
| `EQA000812` / `971` | 11–12 | `4` / `35503` | `model_call_budget` / `invalid_model_output` | `model_call_budget` / `invalid_model_output` | 单表 SQL 每次仅查 1 文件，12 轮不足以遍历 11–12 张表 |

### 4.6 典型样本轨迹

- **`full_agent` 完整 SQL 求解与核验（`EQA000003`）：** 自主检索命中 `lottery-ny-lotto-winning-numbers-beginning-2001/files/rows.txt` → 执行 `SELECT * FROM data LIMIT 5` 探测列名 → 使用 `substr("Draw Date", 7, 4)` 与聚合函数算出各年 BVAL → 输出 `structured_result: "2007"` → 通过 `verify`（`supported`）完成。
- **`no_verifier` 跨 6 文件计算（`EQA000771`）：** 经 9 次模型调用完成跨文件取证与数值计算，输出 `99357947` 并完成。
- **核验门禁与预算上限冲突（`full_agent` / `EQA000493`）：** 第 12 轮已生成正确答案 `830`（`accuracy=1.0`、`numerical_accuracy=1.0`），但因 `require_verification=true` 需额外调用执行 `verify`，在达到 `max_model_calls=12` 时中止，未计入 `completed`。
- **形式完成率与答案准确率差距（`fixed_retrieval`）：** 固定组 19/20 正常完成，但全部样本精确匹配为 0%。关键词抽取在多跳表格题上多仅命中 CSV 表头或元数据，生成模块输出证据不足说明并正常提交。

## 5. 前期 135 题压测记录与调优前后对比

在加入目录上下文上限、搜索去重熔断、字段级 Schema 诊断、`structured_result` 短答案对齐及低并发控制之前，曾对全集 135 题 × 3 组 × 3 轮（1,215 样本）以 24 并发分片运行过一次批量测试（`live-r3`）。该次运行累计消耗约 **7,861,011 tokens**，且自主组因大面积 HTTP 429/402 提前失败导致三组不可比。

| 观测维度 | 调优前（`live-r3` / 初始 `pilot`） | 调优后（`subset20-r1`） |
| --- | --- | --- |
| 并发分片数 | 24 | 2 |
| 服务端 HTTP 429 / 402 拒绝数 | 734（`no_verifier` 331，`full_agent` 403） | 0 |
| `no_verifier` 单样本 Token 均值 | 76,512（仅 67 条完整记录） | 52,535（20/20） |
| `full_agent` 单样本 Token 均值 | 123,550（初始单题 pilot，11 轮重复搜索） | 50,363（18/20，无重复搜索死循环） |
| `full_agent` 检索 F1 | 0.0%（0 次工具调用） | 37.5% |
| 自主组答案 / 数值准确率 | 0.0% / 缺测 | 10.0% / 20.0% |

## 6. 局限与缺测说明

- **单文件大小与单表查询边界：** 本地 DuckDB 查询限制单文件 ≤ 16 MiB 且每次仅查询单张表（不支持跨文件 SQL JOIN），导致 135 题中有 62 题超出本地 SQL 文件上限，且 6–12 文件的多跳题在 12 轮调用预算内较难全部遍历。
- **模型结构化输出稳定性：** 使用轻量模型 `deepseek-flash` 时，复杂动作 JSON Schema 下仍有 4–6/20 条样本因连续 `invalid_model_output` 提前中止。
- **词汇检索上限：** 本地目录检索基于路径、文件名、摘要和元数据的词汇重合排序，跨术语或正文深层内容仍存在召回瓶颈。
- **缺测项口径：** 本次实验未采集外部网络传输字节数、美元计费明细及人工“不必要工具调用”标签，对应字段按评估契约保持 `null`；整组 `grounding` 在存在无引用样本时置 `null`，第 4.2 节中另列出了有引用样本的条件均值。
