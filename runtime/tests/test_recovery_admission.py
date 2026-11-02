"""公开恢复流程必须能承受短暂的审计准入故障。

首次尝试无法打开轨迹分段，且不派发任何端口调用。
换用正常的新接收器后，同一个持久化运行必须能够开始实际工作。
"""

from test_recovery_runtime import (
    RecordingContent,
    RecordingData,
    RecordingPlanner,
    assert_no_port_calls,
    request,
)

from lake_agent.runtime import resume, run
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink


class UnavailableAttemptTrace(JsonlTraceSink):
    """在创建审计轨迹前失败；其他状态与轨迹存储仍使用真实实现。"""

    async def create_attempt(self, run_id, attempt_id):
        raise OSError("temporary trace admission failure")


async def test_healthy_resume_after_initial_trace_admission_failure(tmp_path):
    """不可用的审计接收器不能将尚未开始工作的运行永久缓存为终结状态。"""
    store = SqliteStateStore(tmp_path / "state.db")
    trace_root = tmp_path / "traces"
    try:
        data, content, planner = RecordingData(), RecordingContent(), RecordingPlanner()
        stopped = await run(
            request(),
            data=data,
            content=content,
            planner=planner,
            trace=UnavailableAttemptTrace(trace_root),
            state_store=store,
        )
        assert (stopped.status, stopped.stop_reason) == ("failed", "trace_sink_unavailable")
        assert_no_port_calls(data, content, planner)

        fresh_data, fresh_content, fresh_planner = (
            RecordingData(),
            RecordingContent(),
            RecordingPlanner(),
        )
        result = await resume(
            stopped.run_id,
            data=fresh_data,
            content=fresh_content,
            planner=fresh_planner,
            trace=JsonlTraceSink(trace_root),
            state_store=store,
        )
        assert (result.status, result.stop_reason) == ("completed", "verified_supported")
        assert result.run_id == stopped.run_id
        assert len(fresh_data.catalog_requests) == len(fresh_data.tool_requests) == 1
        assert len(fresh_planner.decompositions) == 1
        assert len(fresh_content.preparations) == 1
    finally:
        await store.aclose()
