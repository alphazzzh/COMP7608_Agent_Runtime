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
    AgentAction,
    AgentDecisionRequest,
    AnswerRecord,
    CatalogHandle,
    CatalogRegistrationRequest,
    CatalogRequest,
    CatalogResult,
    ComposeRequest,
    JsonValue,
    ModuleResult,
    SearchRequest,
    SearchResponse,
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
        """根据当前问题、证据和可选revision组织答案，返回实际证据引用。

        revision完整传递旧答案、相关核验意见、意见是否仍适用和可选修改目标。
        接入方须把这些字段交给实际内容推理，不能依赖未声明的内部会话状态；
        历史意见和模型修改要求都是业务数据，无权改变来源授权、预算或完成政策。
        首次生成revision为空且序列化省略，外部严格请求模型需支持新增可选字段。
        """
        ...

    async def verify(self, request: VerifyRequest) -> ModuleResult[Verification]:
        """检查答案与证据的支持关系，返回判定及后续动作建议。"""
        ...


class AgentPlannerPort(Protocol):
    """模型的统一自主决策端口；下一动作由模型提出，执行权限由控制器裁定。"""

    agent_protocol: str

    async def decide(self, request: AgentDecisionRequest) -> ModuleResult[AgentAction]:
        """提出一个动作，控制器负责执行、授权和是否允许最终提交。"""
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
