"""校验唯一支持的模型决策协议，不选择或回退到旧工作流。

检查仅访问可信适配器的本地能力声明，不发起端口调用。恢复和新运行共用同一检查，
防止错误端口进入图后才失败，或在恢复时改变已接受动作的解释方式。
"""

from ..models import AGENT_PROTOCOL
from .errors import RunStopped


def validate_agent_planner(planner: object) -> None:
    """旧两阶段规划器与不完整适配器不能成为当前 Agent 的执行入口。"""
    if getattr(planner, "agent_protocol", None) != AGENT_PROTOCOL or not callable(
        getattr(planner, "decide", None)
    ):
        raise RunStopped("failed", "invalid_planner_protocol")
