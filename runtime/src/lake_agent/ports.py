"""基于结构类型的异步集成协议；资源的所有权保留在各实现中。

控制器等待这些方法完成，不会在内部启动事件循环。适配器必须允许不同运行重入，
传播取消信号，并禁用服务商的隐式重试。tool_specs 是唯一的同步方法，
因为它只返回本地静态元数据，不得访问数据。

同一运行内，只有可信的 parallel_safe 工具，且其数据源声明与独占资源声明均不冲突，
invoke 调用才允许重叠。幂等声明只赋予重试资格，不赋予并发资格。控制器仍负责预算
和全部语义状态提交；适配器不得通过修改请求对象改变控制策略，也不得发送未上报的
服务商请求。
"""

from typing import Protocol

from .models import (
    AnswerRecord,
    CatalogHandle,
    CatalogRegistrationRequest,
    CatalogRequest,
    CatalogResult,
    ComposeRequest,
    FinishDecision,
    JsonValue,
    ModuleResult,
    PlannerRequest,
    ReadObservationDecision,
    SearchRequest,
    SearchResponse,
    TaskPlan,
    ToolPlan,
    ToolPlanningRequest,
    ToolRequest,
    ToolResult,
    ToolSpec,
    TraceEvent,
    Verification,
    VerifyRequest,
)


class DataPort(Protocol):
    """提供数据源目录与已注册工具，数据访问资源由实现自行管理。"""

    async def catalog(self, request: CatalogRequest) -> CatalogResult:
        """按请求的快照、游标和页大小返回一页数据源目录。"""
        ...

    def tool_specs(self) -> list[ToolSpec]:
        """返回可信的静态工具声明，不执行数据访问或网络请求。"""
        ...

    async def invoke(self, request: ToolRequest) -> ToolResult:
        """执行一次指定工具调用，返回结构化结果或明确的错误。"""
        ...


class ContentPort(Protocol):
    """负责目录注册、候选源检索、答案组织与证据校验。"""

    async def prepare_catalog(
        self, request: CatalogRegistrationRequest
    ) -> ModuleResult[CatalogHandle]:
        """注册冻结的数据源目录，并返回后续检索所用的目录句柄。"""
        ...

    async def search(self, request: SearchRequest) -> ModuleResult[SearchResponse]:
        """在已注册目录中检索候选数据源，遵守请求中的筛选与数量限制。"""
        ...

    async def compose(self, request: ComposeRequest) -> ModuleResult[AnswerRecord]:
        """根据问题与已收集证据组织答案，并返回使用的证据引用。"""
        ...

    async def verify(self, request: VerifyRequest) -> ModuleResult[Verification]:
        """检查答案与证据的支持关系，返回判定及后续动作建议。"""
        ...


class PlannerPort(Protocol):
    """只提出任务与工具计划，工具执行及预算控制仍由控制器负责。"""

    async def decompose(self, request: PlannerRequest) -> ModuleResult[TaskPlan]:
        """根据问题、工具声明与剩余预算生成子任务计划。"""
        ...

    async def select_tools(
        self, request: ToolPlanningRequest
    ) -> ModuleResult[ToolPlan | FinishDecision | ReadObservationDecision]:
        """选择工具计划、结束决定或读取已有观测结果的分页请求。"""
        ...


class TraceSink(Protocol):
    """接收运行审计事件与独立工件，不参与控制器的执行决策。"""

    async def create_run(self, run_id: str) -> None:
        """为指定运行建立审计记录。"""
        ...

    async def append(self, event: TraceEvent) -> None:
        """追加一条具有运行身份与顺序信息的审计事件。"""
        ...

    async def put_artifact(self, ref: str, json_value: JsonValue) -> None:
        """按给定引用保存 JSON 工件，供审计事件引用。"""
        ...

    async def close(self) -> str:
        """完成审计输出并返回其位置引用。"""
        ...


class AttemptTraceSink(TraceSink, Protocol):
    """可选审计接收端，在同一运行 ID 下隔离不同恢复尝试。

    每次执行都使用新的接收端和尝试 ID。事件保留原运行 ID，
    事件序号与工件则仅属于当前尝试。普通运行仍可直接使用基础 TraceSink 协议。
    """

    async def create_attempt(self, run_id: str, attempt_id: str) -> None:
        """以排他方式创建尝试分段，不得覆盖已有分段。"""
        ...
