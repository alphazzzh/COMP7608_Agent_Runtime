# 内容输出诊断与模型自主修正

模型生成了格式错误或引用越界的答案时，控制器拒绝接纳该输出，保存具体诊断，并允许声明支持修正的内容端口进入下一轮自主决策。模型可以重新生成、补充证据、读取观察或停止；Harness 不替模型改引用，也不强制执行一条固定修复流程。

这条通路适用于生成和核验输出，不改变唯一的 `agent-loop-v1` 引擎。代码分别位于 [运行控制器](../src/lake_agent/runtime.py)、[动作图](../src/lake_agent/engine/agent_workflow.py)、[诊断构造器](../src/lake_agent/engine/content_feedback.py) 和[共享模型](../src/lake_agent/models.py)。

## 哪些错误可以继续

| 情况 | 当前处理 |
|---|---|
| `invalid_model_citations`：答案引用或核验问题引用了本次证据之外的 ID | 显式支持反馈的内容端口收到下一轮错误上下文，模型决定如何处理 |
| `invalid_model_sources`：核验建议引用了当前目录之外的来源 ID | 同上；来源授权仍以完整注册目录为准 |
| `invalid_model_output`：已经提取出的模型正文不是合法 JSON，或不满足答案/核验 schema | 同上；诊断保留错误路径与有界正文预览 |
| HTTP、鉴权、传输、超时、提供方封装、输入配置、模块响应契约或输出大小错误 | 停止；不会通过纠错通路隐式重发 |
| 没有声明反馈能力的旧内容端口 | 保留原失败语义，不发送扩展请求；具体错误仍可用于审计和页面说明 |

内容端口返回 `status="ok"` 也不能绕过校验：控制器再次检查答案引用、核验问题的 `evidence_ids` 和核验建议的 `source_ids`。非法的成功响应同样被拒绝。引用 ID 按字符串身份匹配，来源 ID 不能替代证据 ID；空 `citations=[]` 本身合法，但不代表任意正文事实获得支持。

这不是 `retryable` 或 `recoverable` 的新含义。前者继续控制符合条件的幂等数据操作重试，后者继续表达可信工具的已知失败能否通过新规划修正。内容输出修正采用显式能力与错误类别判断，不因某个错误把这两个标志设为 true 就自动重发。

## 诊断数据

`ErrorInfo` 保留 `code`、`message`、`retryable`、`recoverable`，新增可空的 `details`。没有诊断时，序列化省略 `details`，普通旧错误仍保留原字段集合。

引用错误的详情示例：

```json
{
  "code": "invalid_model_citations",
  "message": "The model returned unknown evidence citations.",
  "retryable": false,
  "recoverable": false,
  "details": {
    "issues": [
      {
        "path": "/citations/0",
        "code": "unknown_evidence_id",
        "actual": "demo-annual",
        "expected": "an evidence_id from allowed_evidence_ids"
      }
    ],
    "allowed_evidence_ids": ["ev-current-annual"],
    "allowed_source_ids": ["demo-annual"],
    "rejected_output": {
      "text": "本轮模型生成的答案。",
      "citations": ["demo-annual"]
    },
    "omitted": {}
  }
}
```

`issues[].path` 是 JSON Pointer，例如 `/citations/0`、`/issues/0/evidence_ids/0`；JSON 语法错误可以提供行、列信息。`actual` 与 `expected` 描述失败值和要求，`rejected_output` 是被拒绝的模型内容预览，不是 HTTP 请求或提供方完整响应。

诊断的紧凑 UTF-8 JSON 最大为 8000 字节。构造器对集合、文本、嵌套深度和节点数另设有限预览；`omitted` 明确记录省略情况。`allowed_*_ids` 可能只是预览，完整证据和目录才是授权依据。隔离适配器清理已知的本地 API 凭据与端点，不把鉴权头、提供方封装或未受控异常文本写入诊断。

完整模块响应仍受本次 `max_output_bytes` 限制。隔离适配器在装入诊断时预留错误封装和用量字段所需的字节；预算较小时缩小预览，必要时省略 `details`，保留错误码、固定说明与已发生请求的用量。不会为了增加诊断而越过响应预算，也不会额外调用模型。

## 显式启用，保持基本端口兼容

DataPort 和 ContentPort 的异步方法签名保持原样。ComposeRequest 新增默认空的 revision（见下一节），VerifyRequest 业务字段保持；普通调用不发送 `feedback: null`，首次生成也不发送 `revision: null`。

支持修正的内容适配器必须声明：

```python
from lake_agent.models import CONTENT_FEEDBACK_PROTOCOL

class MyContent:
    content_feedback_protocol = CONTENT_FEEDBACK_PROTOCOL  # "content-feedback-v1"
```

该实现还必须能接收两种扩展请求，而不能只加一个属性：

| 请求 | 基础字段之外的内容 |
|---|---|
| `ComposeFeedbackRequest(ComposeRequest)` | 必填 `feedback: ContentFeedback` |
| `VerifyFeedbackRequest(VerifyRequest)` | 必填 `feedback: ContentFeedback` |

`ContentFeedback` 包含 `phase`（`compose` 或 `verify`）、此前失败调用的 `call_id` 与完整的 `error: ErrorInfo`。只有同阶段仍有未解决反馈，控制器才构造该子类。未知能力声明不会被当成支持，也不会以原始私有值进入恢复元数据。

旧版 `extra="forbid"` 消费者不能天然接收扩展子类序列化后的 JSON。内容模块的实现方需要升级共享模型并实现反馈处理后，才声明能力；未声明模块不接收 feedback 扩展，但仍须支持基础 ComposeRequest 新增的可选 revision 才能参与答案修订。该能力不是 TOML 配置项，不需要修改私有 `config.toml`。

`prompt_versions` 是宿主在配置中声明的审计标签，不证明提供方实际使用了哪份提示文本。本轮不改默认标签或私有配置；能力协议、具体请求/事件与源码恢复指纹共同记录实际行为，新增修正提示不要求用户再增加配置项。

## 反馈怎样真正到达模型

规划模型和答案生成/核验模型是独立调用。规划器通过已有 `AgentDecisionRequest.context` 获得被拒动作、未解决反馈和错误摘要；只让规划器看到错误，不能代替内容端口把反馈送给自己的模型。

规划上下文优先保留每个未解决反馈的阶段、调用 ID 和错误码，较大的说明与详情按 UTF-8 JSON 字节预览。观察与目录分页在生成前预留这些字段和省略标记的空间，紧随读取的下一轮决策收到完整页面，`text` 与 `next_offset` 不再事后截断。目录页始终保留完整的来源 ID 与格式，空间不足时通过 `metadata_omitted` 标记省略可选元数据。后续动作产生新反馈时，较旧的页面可以通过 `omitted_fields` 明确省略；审计和完整观察仍保留，模型可以再次读取。

支持该协议的内容适配器应把 `request.feedback` 序列化到下一次提供方请求的 `messages[].content` 中。当前隔离实现 [ModelContent](../tests/support/model_content.py) 将它放入 user 消息的 `input.feedback`；HTTP 请求体顶层仍只有模型设置、`messages`、输出格式等既有参数，没有新增提供方专用的 `feedback` 参数。

长期规则属于 system prompt，动态的错误、被拒输出与证据属于待解释的数据。内容端口不得将模型上次生成的文字升级成可信系统指令，也不应把反馈拼入原始 `question`、伪装成证据或虚构子任务。

每个 `compose/verify` 调用仍最多发送一个提供方请求。适配器不在内部再次请求模型；下一次修正必须由新的 `decide` 和受控内容调用产生，模型也可以选择补证、读取信息或停止。

## 合法核验意见驱动的答案修订

| 通路 | 输入来源与目的 | 请求字段 | 能力要求 |
|---|---|---|---|
| 答案修订 | 已接纳旧答案、可选合法核验与模型修改目标，供内容模块改写答案 | `ComposeRequest.revision` | 升级基础共享模型，并实际传入内容模型消息 |
| 非法输出修正 | 被拒输出的格式或引用诊断，供内容模块纠正输出错误 | `ComposeFeedbackRequest.feedback` / `VerifyFeedbackRequest.feedback` | 另声明并实现 `content-feedback-v1` |

两者可以同时出现在一次 compose 请求中：revision 描述最后一份已接纳答案，feedback 描述最近一次被拒绝的生成输出。被拒输出不会替代 previous_answer，也不会成为证据。

合法 Verification（例如 conflicting 的具体 issues）与上文非法模型输出的 ContentFeedback 是不同信息。规划器看到核验意见后可选择 compose，并通过可空的 revision_instructions 给出1–4000字符的修订目标。有旧答案时，控制器在 ComposeRequest.revision 中发送完整 previous_answer、相关 verification（可空）、verification_is_current 和 instructions；无核验时 verification_is_current 必须 false。没有旧答案却指定修改目标时，以 revision_answer_required 拒绝动作，不调用内容模块。普通首次生成不需要修订载荷。

隔离 ModelContent 将 revision 实际写入下一次提供方消息的 input.revision；外部真实 ContentPort 也必须升级共享模型并实现该消息信息流。不能只在 Python 模型里接收字段后忽略，也不能把旧答案或核验问题拼入用户问题、伪装成证据。修订意见与 planner 的要求始终是待解释的数据，不得扩大权限、预算或强制核验政策。

历史核验的 verification_is_current=false 只表示它不再适用于当前完整输入，其意见仍可用作有明确时间范围的修订参考；不能据此宣称新证据已被该核验检查。最终事实和引用必须以本次完整证据为准。改写答案不等于修正已被验证，强制政策仍要求对当前输入取得新的 supported。

## 接纳、完成门禁与预算

拒绝的生成输出不覆盖最后一份已接纳答案，拒绝的核验输出也不成为有效核验。非法输出反馈按 compose/verify 阶段保留；对应阶段成功后清除。成功生成不同答案时，还会清除针对旧答案的 verify 输出反馈与核验收据；重复生成完全相同答案不会清除这些状态。合法核验绑定实际送入 verify 的完整问题、完整 AnswerRecord、完整证据列表与运行模式/require_verification 政策的确定性指纹；完整可信状态用于比较，不使用 planner 的截断预览。生成不同答案后，新答案核验状态为 missing；证据变化但答案未变时，历史收据为 stale。完全相同输入下的再次生成仍保留失败判定。工具若没有新增或改变验收相关状态，不会仅因发生一次调用而自动失效。

有任意未解决内容反馈时，`finish` 返回 `unresolved_content_failure` 拒绝观察，任务回到决策循环。`full_agent + require_verification=true` 要求当前有效的 `supported` 核验；过期时返回 verification_stale，缺失或当前不支持时返回 verification_required，模型可以继续核验或修订。context.verification_status 为 missing/current/stale，只有 current 时 verification_verdict 才表示当前结论；过期不等同于当前答案已被判定错误。默认 require_verification=false 保持不变，允许没有核验或旧核验过期时完成；当前有效的不支持核验仍不能直接 completed。no_verifier 仍拒绝 verify，也不能与强制核验同时启用。

finish 只是模型提议，控制器按当前运行政策决定是否接受。full_agent 模式名称不能代替强制核验配置；completed、内部核验通过、外部评测正确相互区别，完成不是事实正确性的保证。

每次新决策消耗 `max_agent_turns`，每次实际模型请求计入 `max_model_calls`，失败请求的已报告 token/费用继续累计。修正使用原始运行期限和调用超时；没有额外刷新预算，也没有恢复退役的 `max_feedback_rounds` 配置。反复生成相同错误会耗尽已有轮数、调用或时间上限，最终结果保留最近未解决的具体失败信息。

## 审计与 SQLite 恢复

`*.call.failed` 保存模块返回的错误响应和用量；控制器另写 `content.output.rejected`，其 `data_ref` 指向 `ContentFeedback` 工件。成功解决某阶段反馈后写 `content.feedback.resolved`。错误输出不进入证据集合，也不被当作已接纳答案。

新 `RunResult.failure` 可空，存在时含 `phase`、`call_id`、`error`；空值省略。最终预算或时间停止也可保留此前尚未解决的具体失败，不必只显示顶层停止分类；成功结果不携带残留失败。

SQLite 调用日志保存已知失败响应与扩展修正请求。恢复通过缓存响应重新生成反馈、核验适用指纹和 revision，并按原请求匹配，不依赖内容模块的内存聊天历史；已知失败与已知修正结果不重复派发。指纹不依赖新时间戳、attempt ID 或审计事件顺序。持久状态记录支持的内容反馈协议，能力改变时在任何异步端口调用或返回缓存终态前拒绝，错误码为 `recovery_contract_mismatch`。本轮源码和接口变化仍经现有实现指纹检查处理，旧任务不能绕过兼容性校验继续执行。

修正调用也属于外部调用。`started` 没有对应有效 `completed` 时仍以 `recovery_outcome_unknown` 拒绝恢复，不能因为“这只是纠错”而重发。原 deadline 包含停机时间，预算、已付费失败请求和确定性身份保持；源码改变仍不支持跨版本恢复。

## 页面与旧历史

工作台显示阶段、错误码、中文解释与调用 ID，可展开路径差异、允许 ID 和被拒输出预览，并定位相应原始事件。正在纠错时展示未解决反馈，解决后清除当前提示，历史事件保留。

新结果优先使用明确的 `failure`。旧历史只有 `module_error` 时，展示层从已有失败事件寻找具体错误，同阶段后续成功会清除旧失败提示；成功终态不把历史失败重新显示成当前错误。这只是显示推导，不改变 SQLite 恢复状态。旧记录若没有保存错误值，页面会明确说明无法还原，不能补造模型当时的引用列表。

## 离线验证

```bash
uv run --offline --extra dev --extra web --frozen pytest -q \
  tests/contracts tests/adapters tests/engine tests/recovery tests/web
```

上述目录级命令覆盖内容反馈所在的各层回归；更细的文件定位见[测试导航](../tests/README.md)。测试通过临时 SQLite、JSONL、本地端口和 `MockTransport` 覆盖反馈传入 messages、一次调用一次请求、具体引用差异、错误后自主修正、预算和完成门禁、能力兼容与恢复。测试不访问真实模型，也不证明真实提供方一定能修正或回答正确。
