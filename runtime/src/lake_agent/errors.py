"""Internal stop reasons shared by sequential orchestration and tool execution.

These exceptions describe controller termination, never caller cancellation or
proof that an interrupted external operation did not execute.
"""


class RunStopped(Exception):
    """Carry a public run status/reason without swallowing CancelledError."""

    def __init__(self, status: str, reason: str):
        self.status, self.reason = status, reason
        super().__init__(reason)


# Trusted adapters may only request input correction for known safe failures.
# These reserved codes cannot grant recovery even if marked recoverable.
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
