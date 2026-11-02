"""项目适配器在持久化完成之后清理资源的独立回归测试。

测试仍经过真实的离线核心、SQLite 调用日志、轨迹和 HTTP 视图。
仅让上下文所有者的最终清理失败或等待：后续资源生命周期步骤被中断时，
核心已经终结的结果必须仍然可用。
"""

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from lake_agent.demo import DemoContent, DemoData, DemoPlanner
from lake_agent.web.app import create_app
from lake_agent.web.evaluations import EvaluationImport, EvaluationRepository
from lake_agent.web.profiles import ExecutionPorts, ExecutionProfile, ProfileMetadata
from lake_agent.web.storage import WebError


@pytest.mark.parametrize("cleanup", ["error", "cancel"])
async def test_adapter_cleanup_preserves_already_committed_terminal_result(tmp_path, cleanup):
    """后置清理不能抹掉已完成的答案，也不能声称该任务仍可恢复。"""
    cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def owned_ports():
        # 每次尝试都获得真实且全新的本地适配器。测试关卡只作用于清理阶段，
        # 此时 run() 已将结果持久化并返回。
        try:
            yield ExecutionPorts(data=DemoData(), content=DemoContent(), planner=DemoPlanner())
        finally:
            cleanup_entered.set()
            if cleanup == "error":
                raise RuntimeError("test adapter cleanup failed")
            await cleanup_release.wait()

    profile = ExecutionProfile(
        metadata=ProfileMetadata(
            profile_id="review-project",
            title="Review project",
            description="Offline adapter teardown regression.",
            kind="project",
            dataset_id="review-data",
            dataset_name="Review data",
            revision="review-v1",
            modules={
                name: {"status": "connected", "label": "Local review adapter"}
                for name in ("data", "planner", "content")
            },
        ),
        ports_factory=owned_ports,
    )
    app = create_app(data_dir=tmp_path, profiles=(profile,))
    service = app.state.demo_service
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            response = await client.post(
                "/api/runs",
                json={
                    "question": "What was 2024 annual revenue?",
                    "profile_id": "review-project",
                },
            )
            assert response.status_code == 202, response.text
            run_id = response.json()["run_id"]
            try:
                async with asyncio.timeout(5):
                    await cleanup_entered.wait()
                # 触发取消前先验证持久化结果，
                # 从而区分后置清理与尚未完成的任务。
                lease = await service.state_store.acquire(run_id)
                try:
                    saved = await service.state_store.load(lease)
                    assert saved.state["result"]["status"] == "completed"
                finally:
                    await service.state_store.release(lease)
                if cleanup == "cancel":
                    cancelled = await client.post(f"/api/runs/{run_id}/cancel")
                    assert cancelled.status_code == 202, cancelled.text
                async with asyncio.timeout(5):
                    while True:
                        view = (await client.get(f"/api/runs/{run_id}")).json()
                        if not view["active"]:
                            break
                        await asyncio.sleep(0.01)
                assert view["result"] is not None, view
                assert view["result"]["status"] == view["status"] == "completed"
                assert "120 million HKD" in view["result"]["answer"]["text"]
                assert view["can_resume"] is False
            finally:
                cleanup_release.set()


async def test_repository_does_not_publish_a_report_larger_than_its_read_limit(tmp_path):
    """直接导入不能创建表面有效、却会破坏所有后续读取的记录。"""
    repository = EvaluationRepository(tmp_path)
    await repository.start()
    # Python 接受任意长的小数秒。HTTP 大小限制独立于
    # 公开仓库关于创建可读文件的契约。
    value = {
        "title": "Oversize timestamp",
        "dataset_id": "review-data",
        "task_set_id": "review-tasks",
        "model_id": "offline-demo",
        "evaluator": "Review author",
        "evaluated_at": "2026-11-02T08:30:00." + "1" * 300_000 + "+08:00",
        "results": [{"mode": "full_agent", "task_count": 1}],
    }
    # 严格导入模型或仓库均可拒绝该大小；
    # 两者都不能发布无法读取的文件后仍报告创建成功。
    with pytest.raises((ValueError, WebError)):
        await repository.create(EvaluationImport.model_validate(value))
    assert await repository.listing() == []
    assert list((tmp_path / "evaluations").iterdir()) == []
