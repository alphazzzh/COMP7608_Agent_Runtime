# dsh-superagent 与当前 Agent Harness 的源码对照

源码核查日期：2026-11-01；本项目采用情况更新于2026-11-02，对应运行时0.7.0。此文保留固定提交的公开源码出处，并区分已经实现的持久恢复与尚未采用的自主worker机制；恢复模块源码注释也引用本文。

## 来源与判断范围

- 官方仓库为 [deepseek-ai/dsh-superagent-prototype](https://github.com/deepseek-ai/dsh-superagent-prototype)，2026-10-31 公开，MIT 许可。本次固定检查提交 `6337fc823ce20251c8de8085948165c81aaa701e`，下载并核对 57 个选定文件的 Git blob 哈希，没有执行仓库代码或安装脚本。
- [README](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/README.md) 明确定位为研究原型、并非官方 DeepSeek 产品；公开版本与内部实现及文章描述不完全一致，文章中的一些方向尚未实现。
- 已定位用户提到的[知乎文章](https://zhuanlan.zhihu.com/p/2099987320443237060)，但页面连接失败，未完整读取。下面的实现判断来自官方仓库的 README、设计文档与源码，不将搜索摘要作为文章内容依据。
- 本次只做架构评估和文档记录，没有修改运行代码、增加依赖、执行参考项目或调用付费模型；不能据此宣称性能或检索准确率提高。

## 参考仓库中的机制

| 机制 | 固定提交中的源码证据 | 对本项目的设计启发 |
|---|---|---|
| 长期工作项与一次执行尝试分开 | `src/core/model.ts`、`assignments.ts` 区分 Ticket、Assignment、Input；一次重试/重启不等于重新创建整项工作 | 引入持久任务与执行尝试状态，保存目标、依赖、输入、进度、产物引用和运行次数；不重置已消耗预算 |
| 状态变更与待执行操作一起提交 | `src/app/store.ts:130` 的事务与 `effects.ts:60` 的 SQLite 队列；提交后执行，失败保留，按 kind/key 合并，generation 防止旧尝试删除新内容 | 为状态保存、派发与恢复建立统一持久边界；将可恢复操作队列与只用于审计的 JSONL 分开 |
| 明确的 worker 容量预留 | `src/executor/dsh.ts:171` 的 WorkerSlots 在异步启动前预留槽位 | 后续自主子任务采用统一调度器，限制 worker 数、父级期限和共享调用预算；不能直接并发共享控制器的子任务流程 |
| 重启先对账再继续 | `src/executor/dsh.ts:524` 的 resume 与 `src/core/assignments.ts:83` 的 interrupted；恢复提示要求先检查外部状态，避免重复完成的操作 | 定义版本化 StateStore、恢复入口和未知调用的处理规则。已派发但未知结果的操作不得自动重放，包含幂等工具 |
| 稳定消息标识与交付去重 | `src/app/delivery.ts:15`、`:40`、`:65` 使用固定消息 ID、历史/收件箱检查及 persistence flush | 若新增 worker 输入或中途补充指令，分别记录 queued、delivered、consumed，防止恢复时重复投递；不能推导任意工具 exactly-once |
| 有界信息卡片与按需读取 | `src/executor/dsh.ts:112` 限制内联任务/触发文本，超出给读取指针；`docs/007-architecture.md:581` 说明报告与输入消息形式 | 沿用当前预览/完整 artifact 分离，扩展任务账本，明确已知事实、失败尝试和未解决问题；摘要保留来源引用并与原始证据分开 |

以上源码均指固定提交，入口可从 [执行设计](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/docs/003-execution.md)、[持久操作队列](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/src/app/effects.ts)、[执行器](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/src/executor/dsh.ts) 查看。

## 本项目已经采用的部分

当前运行时0.7.0已提供来源授权、目录快照、统一预算、工具观察与证据隔离、按需分页、完整规划请求字节上限、显式安全的工具并行，以及可选持久状态与恢复。恢复能力自0.4.0发布，核心契约仍为v0.5；不能再将它描述为尚无checkpoint/resume。

[StateStore](../src/lake_agent/state.py)保存原请求、started意图和有效completed响应；[恢复协调器](../src/lake_agent/recovery.py)通过原控制器匹配已知调用重建状态。历史调用不重新派发，任何未知调用结果都拒绝自动恢复，包括声明幂等的工具。原预算和绝对期限保持；每次执行使用独立attempt审计段。详细规则与接入示例见[持久恢复说明](durable-recovery.md)。

这些是基于公开模式的Python实现，不是运行参考仓库或引入其Harness依赖，也没有复用其完整持久操作队列。JSONL仍是审计，不能代替独立调用账本。模型提议行动，运行控制器继续唯一拥有授权、预算与证据提交。

## 尚未采用的部分

- 动态worker委派、独立子代理上下文和跨worker消息交付尚未实现。[runtime.py](../src/lake_agent/runtime.py)仍先宏观分解，再顺序推进子任务；当前并行指可信数据工具I/O，不是自主子代理并行，不能直接将共享可变控制器的子任务用gather并发。
- 带来源引用的长期任务账本和主动上下文选择尚未提供。现有[context.py](../src/lake_agent/context.py)提供有界观察与按需读取，不等于长期记忆。
- 现有重试受调用次数、原期限、幂等和错误类型约束；没有移植参考项目全部provider恢复策略。HTTP403不能通过盲目重试解决，累计token硬预算仍未实现。

持久恢复的离线、进程退出和当前工作台验收集中见[验收记录](project-workspace-acceptance-2026-11-02.md)。这些验证不证明与参考系统能力等价，亦不证明检索质量或性能提升。

## 必须区分的边界

- `src/core/memory.ts` 是测试与临时实例的内存仓库实现，不是 LLM 长期记忆。
- [cordis.patch.yml](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/6337fc823ce20251c8de8085948165c81aaa701e/cordis.patch.yml) 第79–115行配置底层 DeepSeek Harness 的 compaction、结果裁剪、subagent 和 todo 插件；该仓库没有独立实现其中全部能力。
- `src/app/retry-health.ts` 只观察模型预算重试状态，不修改模型重试策略，也不是本项目所缺的累计 token 硬预算实现。
- 它的重启恢复、消息去重与队列 generation 机制，不提供所有外部操作 exactly-once 保证。持久队列在外部动作完成但确认落盘前仍存在结果不确定窗口；迁移到通用 DataPort 时必须设计幂等或状态核对协议。
- 官方项目主要面向单 GitHub 仓库的主动编码工作。GitHub/飞书接入、自动消息、工作目录、Shell 权限和面板不属于当前异构数据池 harness 的优先缺口。具体移植代码时保留 MIT 要求的版权与许可声明。

本次结论支持选择性借鉴和 Python 重实现；不会改变当前真实 DataPort/ContentPort 尚待联调、完整真实模型并行验收遇到 HTTP 403 的既有状态。
