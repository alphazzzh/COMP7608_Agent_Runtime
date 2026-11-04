"""用明确的合成场景展示真实执行框架的控制流程。

所有端口均在本地运行。并行场景将两个互不冲突的只读操作显式交给核心调度器，
不增加网页层工作线程调度或模型调用。
答案引用实际收集到的合成载荷，不暗示这些内容来自在线 LLM 输出。
"""

import asyncio

from lake_agent import models as m

from .ports import DemoContent, DemoData, DemoPlanner, _reply

SCENARIOS = [
    {
        "id": "standard",
        "title": "完整检索流程",
        "description": "从问题到规划、工具、证据与带引用的演示答案。",
        "question": "2024 年的年度营收是多少？请给出来源依据。",
    },
    {
        "id": "parallel",
        "title": "独立工具并行",
        "description": "读取两个独立来源，观察安全工具并行与证据汇合。",
        "question": "请同时查看年度营收和报告年份，并列出各自的证据。",
    },
    {
        "id": "recovery",
        "title": "中断后安全恢复",
        "description": "证据保存后自动中断；点击恢复，继续处理并保留原证据。",
        "question": "查找 2024 年营收，并演示保存证据后的中断与恢复。",
    },
]

CAPABILITIES = [
    {
        "name": "任务规划与工具调度",
        "status": "implemented",
        "description": "控制器校验工具参数、来源与依赖；当前用离线规则规划器演示。",
    },
    {
        "name": "受控工具并行",
        "status": "implemented",
        "description": "仅可信声明安全且来源、资源不冲突的工具可进入同批执行。",
    },
    {
        "name": "证据、引用与运行记录",
        "status": "implemented",
        "description": "完整审计与可引用证据分别保存，支持追溯工具请求和响应。",
    },
    {
        "name": "预算、超时与安全恢复",
        "status": "implemented",
        "description": "持久化已验证响应；恢复保留原期限与预算，未知结果拒绝重放。",
    },
    {
        "name": "真实数据与内容模块联调",
        "status": "pending",
        "description": "当前网页只使用明确标注的合成数据；真实数据、检索、生成模块仍待接入。",
    },
    {
        "name": "真实模型端到端验收",
        "status": "pending",
        "description": "规划器已有API适配器；此前并行专项验收遇到HTTP403，本演示不发模型请求。",
    },
]


class ShowcaseData(DemoData):
    """只读本地端口，显式声明并发能力，并使用可协作取消的模拟 I/O 等待。"""

    def __init__(self, parallel=False):
        super().__init__()
        self.parallel = parallel

    def tool_specs(self):
        return [
            spec.model_copy(update={"parallel_safe": self.parallel})
            for spec in super().tool_specs()
        ]

    async def invoke(self, request):
        # 一段短暂的模拟 I/O 等待，使真实的执行重叠能从时间戳中观察到。
        # 这是测试替身的特性，不代表执行框架的性能结论。
        await asyncio.sleep(0.08 if request.args["source_id"] == "demo-annual" else 0.03)
        return await super().invoke(request)


class ShowcasePlanner(DemoPlanner):
    """仅在显式并行场景中，同时规划两个独立数据源。"""

    def __init__(self, parallel=False):
        self.parallel = parallel

    async def decide(self, request):
        context = request.context
        if not self.parallel or context["evidence_summary"] or not context["candidates"]:
            return await super().decide(request)
        return _reply(
            request,
            m.AgentToolsDecision(
                steps=[
                    m.AgentToolStep(
                        step_id=f"extract-{candidate['source_id']}-{request.call_id}",
                        tool_name="extract",
                        source_ids=[candidate["source_id"]],
                        args={
                            "source_id": candidate["source_id"],
                            "query": request.question,
                            "max_chars": 4000,
                        },
                        purpose="Read an independent synthetic source",
                    )
                    for candidate in context["candidates"]
                ]
            ),
        )


class ShowcaseContent(DemoContent):
    """展示实际收集的证据，并明确标注离线输出的性质。"""

    async def compose(self, request):
        excerpts = [str(item.payload) for item in request.evidence]
        return _reply(
            request,
            m.AnswerRecord(
                text="离线演示输出：本轮从合成资料中收集到以下内容。\n\n" + "\n\n".join(excerpts),
                citations=[item.evidence_id for item in request.evidence],
                unresolved_questions=[
                    "以上为固定合成资料与离线规则流程，不代表真实模型或真实数据池的回答。"
                ],
            ),
        )


def demo_ports(scenario):
    """返回新的实例；恢复不得依赖先前的临时句柄。"""
    parallel = scenario == "parallel"
    return ShowcaseData(parallel), ShowcaseContent(), ShowcasePlanner(parallel)
