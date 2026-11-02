"""顺序编排与工具执行共用的内部停止原因。

这些异常表示控制器终止，不表示调用方取消，也不能证明被中断的外部操作未曾执行。
"""


class RunStopped(Exception):
    """携带公开的运行状态与原因，同时不吞掉 CancelledError。"""

    def __init__(self, status: str, reason: str):
        self.status, self.reason = status, reason
        super().__init__(reason)


# 可信适配器只能对已知可安全处理的失败请求修正输入。
# 以下保留错误码即使标记为 recoverable，也不能获得纠错恢复权限。
FATAL_TOOL_CODES = frozenset(
    {
        "timeout",
        "provider_timeout",
        "unauthorized",
        "forbidden",
        "permission_denied",
        "authorization_failed",
        "source_authorization_failed",
        "invalid_response",
        "invalid_module_response",
        "invalid_schema",
        "schema_validation_failed",
        "invalid_tool_contract",
        "token_budget_exceeded",
    }
)
