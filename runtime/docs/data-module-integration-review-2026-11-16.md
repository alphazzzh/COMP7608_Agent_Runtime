# 数据处理模块接入审查与联调说明

## 1. 当前结论

数据适配器提供了 `create_port(*, options, llm)` 异步上下文工厂、同步 `tool_specs()`、异步 `catalog()` 和异步 `invoke()`；`extract` 的工具声明、`source_id_paths`、成功证据结构和错误结果封套匹配主仓共享模型。这只表示接口形状兼容，不代表真实工厂能在干净环境导入或运行。

目前不能直接接入主仓运行完整任务。最先会遇到的是运行依赖和目录分页问题：适配器未声明 `pandas`/`pypdf`，主仓当前运行环境也没有这两个包，而适配器在模块导入阶段就导入它们；其 `catalog()` 又忽略分页请求并把整个目录放入单页。离线临时目录放入 101 个文件时，主仓在第一次目录调用后以 `failed/invalid_module_response` 停止，内容模块和规划器都没有被调用。`boto3`/`botocore` 仅供单独下载脚本使用，不是工厂导入依赖。

即使修复接入阻塞，当前实现仍属于“小型本地关键词抽取原型”。它可以在小规模、已下载的文本/CSV/JSON/PDF 上尝试返回一条证据，但不能据此声称已经支持 LakeQA 全部复杂查询、跨文件计算或任意异构数据池。题单静态声明了 135 个任务、906 个不同的 `datasets_used` 文件，其中 799 个 `.txt`、89 个 `.json`、5 个 `.pdf`、5 个 `.csv`、5 个 `.xlsx`、2 个 `.html` 和 1 个 `.docx`；节点来源全部是 `.txt`。这些数字描述题单覆盖范围，不代表文件已下载或任务已成功运行。

## 2. 必须在接入前处理的问题

### P1：运行依赖没有进入可安装包

证据：

- 副本 `runtime/src/lake_agent/adapters/data_adapter.py:3-4` 在模块导入时执行 `import pandas` 和 `from pypdf import PdfReader`。
- 副本 `runtime/pyproject.toml:10-17` 及锁文件未声明这两个库；当前主仓运行环境探测到 `pandas=false`、`pypdf=false`。
- 主仓配置工厂会先导入 `module:function`（`runtime/src/lake_agent/web/configuration.py:30-41`），所以即使只想使用 JSON，缺少 pandas 或 pypdf 也会在工厂导入时失败。

处理方式二选一并在接入包中固定：把数据适配器需要的依赖及版本加入该模块自己的部署清单，或将格式解析器改为按能力延迟导入并在 `catalog/tool_specs` 中明确不可用格式。`bulk_download.py` 另需 `boto3`/`botocore`，它不应成为运行时工厂的隐式导入依赖。

### P1：目录必须实现真正的分页和稳定快照

证据：`data_adapter.py:32-59` 没有读取 `request.page_size`、`request.cursor` 或 `request.snapshot_id`，始终扫描整个目录，返回固定的 `local-snapshot-v1`、`next_cursor=null` 和 `has_more=false`。主仓契约要求每页不超过请求页大小，同一快照的 cursor 可继续读取，且不能重复 `source_id`（见 `runtime/docs/runtime-contract.md:156-205`）。

主仓的 `runtime/src/lake_agent/runtime.py:581-617` 会检查这些约束。超过默认 100 条时会拒绝结果；显式请求其他 snapshot 时会产生 `catalog_snapshot_mismatch`。另外，固定 snapshot 没有反映文件新增、删除或内容变化，不能满足恢复时的新实例快照核对。

建议实现：

1. 首次请求建立内存或持久化的冻结目录，生成内容敏感的 snapshot ID，并保存排序后的 `SourceRecord` 列表。
2. 按 `page_size` 返回连续页；cursor 应绑定快照、可校验且对调用方不透明。同一 cursor 重试时返回同一页，不能因重试跳页。
3. 后续请求校验 snapshot；目录或文件内容变化时生成新 snapshot，并填充 `content_hash`/`version`。
4. 用真实单调时钟计算 `duration_ms`，不要返回固定的 10/50。

仅改变 snapshot ID 或记录 hash 不足以支持旧 run 恢复：当前 `invoke()` 会重新打开磁盘路径。run 期间及恢复读取都必须访问 snapshot 对应的不可变数据（例如内容寻址副本/只读快照），或者在目标内容变化时明确拒绝继续；不能拿新内容冒充旧 snapshot。

### P1：异步方法内部执行同步且无界的文件操作

证据：`data_adapter.py:62-77` 的 `invoke()` 直接调用同步 `_extract()`；其中 `data_adapter.py:118` 使用 `pandas.read_csv`，`143-146` 全量 `json.load`，`181-183` 创建 `PdfReader` 并逐页提取文本。目录扫描 `os.walk` 也在 async `catalog()` 中同步执行。

主仓只能对可协作的 await 应用 deadline/取消（`runtime/src/lake_agent/engine/execution.py:46-100`）。大文件解析会阻塞事件循环，使 `per_call_timeout_ms`、并行工具和取消清理无法及时生效；全量加载还没有文件大小、页数、行数或递归深度边界。

有界线程池或异步分块读取可以避免部分 I/O 阻塞事件循环，但线程池不能强制终止已开始的同步函数。需要硬截止的 CPU/解析任务应使用可终止子进程；线程超时后不得采纳迟到结果，也不能不断提交新的后台任务。解析前检查文件大小、JSON 深度和 PDF 页数，解析中限制累计输出字节并在结果边界再次核对。

### P1：路径边界和符号链接未受控

证据：`data_adapter.py:102` 使用 `os.path.join(self.base_dir, source_id)`，没有路径规范化、根目录 containment 或符号链接检查。离线临时复现中，`source_id="../outside.json"` 和绝对路径都能指向根目录外文件；目录中的指向根外文件的符号链接也会被 `os.walk` 列出并可读取。`bulk_download.py:59-61` 同样直接拼接 manifest key 到本地目录。

主仓正常运行会先检查来源 ID 是否来自完整目录，因此模型不能凭空提交未注册的 `../`；但目录自身如果注册了符号链接，或适配器被直接调用，边界仍然失守。下载脚本面对不可信 manifest 时还可能把文件写到目标目录外。

建议在初始化时解析并固定 `base_dir`，对每个候选路径校验规范化后的路径仍在根目录内，并按策略拒绝符号链接；如果运行目录可能被并发修改，应进一步用目录文件描述符和 no-follow 打开方式避免检查与打开之间的竞态。下载 key 只允许相对 POSIX 路径，拒绝绝对路径、`..`、NUL，并使用临时文件加原子替换。

另一个独立正确性问题是 `data_adapter.py:39-40` 用字符串 `replace()` 生成 `source_id`，不是相对路径计算。离线复现中，`/lake/xa.json` 和 `/lake/x/lake/a.json` 会得到相同 ID；当配置目录以斜线结尾时，来源 ID 还可能错误地包含绝对本地路径。用规范化根目录的 `relpath()` 生成 ID，并验证唯一性。

### P1：响应裁剪、错误和证据语义不满足边界

证据：

- `data_adapter.py:134,156,192` 直接按 Python 字符切片 payload，`data_adapter.py:86` 却始终返回 `truncated=false`；多字节 UTF-8 和 envelope 开销可能使整体响应超过 `max_output_bytes`，被主仓以无效响应拒绝。
- JSON/PDF/表格都只返回第一条匹配（`125-137`、`148-161`、`183-197`），命中词在长值末尾时，前缀裁剪可以完全不包含命中词；离线复现 `max_chars=6` 返回 `prefix`，且仍标记未截断。
- `data_adapter.py:113-114` 把 `str(e)` 原样放入错误消息，可能泄露绝对路径和底层解析细节。主仓会把结构化 `ErrorInfo` 写入审计。

建议在封套级别按 UTF-8 字节预算裁剪，裁剪发生时标记 `truncated=true`，优先保留命中位置附近的上下文并在 `warnings` 中说明；错误消息使用稳定、安全文本，有限诊断放到受限 `details`。`max_chars` 的 JSON Schema 需要声明正数和上限，避免 0/负数切片产生空串或反向切片。

## 3. 能力边界与正确性风险

### 3.1 当前工具不能完成题单中的复杂计算

适配器只注册一个 `extract` 工具（`data_adapter.py:11-30`）。它没有列结构探测、结果分页、过滤、排序、分组、聚合、跨表连接或安全表达式执行；每种格式也只返回第一条匹配。

代表性静态需求包括：

- `runtime/lakeqa_mini/k-1-d-1/task_1.json:2-8`（EQA000002）要求按年跨州求和、增长率和排名。
- `runtime/lakeqa_mini/k-3-d-3/task_1.json:2-8`（EQA000301）要求跨学年取 top 7 并求交集。
- `runtime/lakeqa_mini/k-4-d-2/task_1.json:2-3,27-28`（EQA000302）要求把主表和 lookup 表按学校名称/DBN 对齐。

这些条目证明能力需求存在，不等于当前数据已下载或已经运行失败。要支持它们，需要增加受限的 `inspect/query/aggregate/join` 等工具，或者由数据模块返回已计算且带输入来源和 locator 的结构化结果；工具参数和返回字段必须先与主仓 `ToolSpec`/`EvidencePayload` 契约对齐。

### 3.2 `.txt` 的格式语义不统一

`data_adapter.py:105-118` 把所有 `.txt` 交给 `pandas.read_csv`。题单中 330 个 Wikipedia 节点明确使用自然语言 `content.txt`，覆盖 118 个任务；这些文件需要按行/段落/字节范围检索，而不是默认把第一行当表头的 CSV 语义。建议按真实媒体类型或文件探测结果分流，并为正文提供可逆 locator。

### 3.3 查询和 locator 语义不一致

表格使用 `str.contains(query, case=False)`，默认把 query 当正则；JSON/PDF 使用字面量子串。因此同一个 query 在不同格式上的行为不一致，非法正则还会进入 `parse_error`。JSON 展平路径用点号和方括号拼接（`164-177`），当原始键本身含 `.` 或 `[]` 时会碰撞并静默覆盖；离线复现 `{"a.b": "wanted", "a": {"b": "other"}}` 后，真实匹配值消失。建议统一 literal/regex 策略、明确大小写和 Unicode 规则，并使用 RFC 6901 JSON Pointer 或其他可逆 locator。

### 3.4 未支持格式是声明覆盖缺口

`datasets_used` 中有 5 个 `.xlsx`、2 个 `.html`、1 个 `.docx` 的唯一文件；它们没有出现在节点 source 中，因此当前只能确认格式覆盖不足，不能直接断言对应任务必然失败。接入验收应先标注这些文件是主数据、lookup 辅助数据还是仅供下载的替代格式，再决定是否加入解析器和工具 schema。

### 3.5 目录扫描与批量下载结果需要可诊断

`data_adapter.py:35` 的 `os.walk()` 未提供 `onerror`，而不存在的 `base_dir` 也可能被当成成功的空目录返回；这会把“数据不可用”误报成“没有匹配资料”。目录权限错误应返回安全的 `unavailable` 错误，空目录则由调用者显式确认是否允许。

`bulk_download.py` 目前在模块顶层创建 S3 客户端、扫描 manifest 并执行下载；导入它会产生外部副作用。读取或下载失败只打印错误，脚本末尾仍输出完成信息且没有失败退出码；已有目标文件则直接跳过，不核对大小或 hash。当前 135 份 manifest 没有绝对路径或 `..` key，所以不能说现有清单触发了目录逃逸；但下载流程仍需加 `main` 入口、逐项结构化结果、完整性核验和失败退出状态，才能作为可复现的数据准备工具。

### 3.6 后续评测需要先规范题单

135 份题单中，119 份使用 `hop`，12 份使用 `hop_id`，4 份的 reasoning hop 没有编号键；14 份的目录 `k` 与实际 `len(reasoning_hops)` 不一致。当前运行时没有消费这些字段，因此它们不是已发生的运行时失败，但后续评测不能直接用目录名作推理步数，也不能只读取一种编号键。题单中的参考 fact/answer 只用于离线评分，不得进入数据工具返回、来源摘要或模型请求。

### 主仓当前的联调限制

- 主仓刻意不包含生产 DataPort/ContentPort；当前配置只有在安装并注册真实工厂后才会执行真实端到端流程。当前 Git 提交也不包含被忽略的队友副本。
- `resume()` 要求新的端口实例满足同一快照、工具契约和实现/依赖指纹；它不是跨版本迁移机制。
- 内置规划器调用 OpenAI 兼容的 `/chat/completions`，对没有匹配 tokenizer 和可信 usage 的端点拒绝 token budget；模型列举成功不等于推理兼容。
- 目前没有把队友适配器纳入主仓测试矩阵；896 项通过不能替代真实适配器的合同测试。
