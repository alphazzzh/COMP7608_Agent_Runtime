"""在服务端可信地绑定数据池与新建的执行端口。

浏览器只接收声明式元数据。工厂函数与凭据留在宿主进程中；
每次运行或恢复都会进入新的异步上下文，由其所有者关闭客户端。
签名将运行绑定到已注册的数据与配置，但不替代核心更严格的恢复校验。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Annotated, Literal

from pydantic import Field

from .. import models as m
from ..ports import ContentPort, DataPort, PlannerPort
from ..validation import canonical_json
from .fixtures import SCENARIOS, demo_ports
from .storage import WebError

_Id = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")]
_Label = Annotated[str, Field(min_length=1, max_length=200)]


class ModuleState(m.Model):
    """说明执行中的某一部分由哪个实现提供。"""

    status: Literal["connected", "fixture", "unavailable"]
    label: _Label


class ModuleConnections(m.Model):
    data: ModuleState
    planner: ModuleState
    content: ModuleState


class ProfileMetadata(m.Model):
    """公开且受大小限制的配置身份信息，不得用于存放凭据。"""

    profile_id: _Id
    title: _Label
    description: Annotated[str, Field(min_length=1, max_length=2000)]
    kind: Literal["project", "demo"]
    dataset_id: _Id
    dataset_name: _Label
    revision: _Label
    modules: ModuleConnections
    suggested_questions: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=2000)]], Field(max_length=8)
    ] = Field(default_factory=list)


@dataclass(frozen=True)
class ExecutionPorts:
    """一次尝试所用的适配器；资源所有权保留在其工厂中。"""

    data: DataPort
    content: ContentPort
    planner: PlannerPort


@dataclass(frozen=True)
class ExecutionProfile:
    """注册数据池，不通过 HTTP 暴露代码加载能力或路径。

    适配器行为或数据身份改变时，须递增 metadata.revision。
    sources 是可选的已发布数据源目录，不会隐式调用工具，
    也不保证已枚举整个数据池。RunConfig 仍是可信的服务端策略；
    浏览器调用方不能提高预算。
    """

    metadata: ProfileMetadata
    sources: tuple[m.SourceRecord, ...] = ()
    ports_factory: Callable[[], AbstractAsyncContextManager[ExecutionPorts]] | None = None
    run_config: m.RunConfig = field(default_factory=lambda: m.RunConfig(max_wall_time_ms=600_000))
    pause_after_evidence: bool = False

    def __post_init__(self):
        # 将注册信息与可变输入模型分离。下面为每次运行生成的副本，
        # 也可防止适配器修改其他运行的配置。
        object.__setattr__(self, "metadata", self.metadata.model_copy(deep=True))
        object.__setattr__(self, "run_config", self.run_config.model_copy(deep=True))
        values = tuple(source.model_copy(deep=True) for source in self.sources)
        if len(values) > 2000 or len({source.source_id for source in values}) != len(values):
            raise ValueError("published sources must be unique and bounded to 2000")
        object.__setattr__(self, "sources", values)
        if self.ports_factory is not None and not callable(self.ports_factory):
            raise ValueError("ports_factory must be callable")

    def public(self):
        modules = self.metadata.modules.model_dump()
        ready = self.ports_factory is not None and all(
            module["status"] != "unavailable" for module in modules.values()
        )
        return {
            **self.metadata.model_dump(mode="json"),
            "ready": ready,
            "unavailable_reason": None
            if ready
            else "该配置尚未接入完整执行模块。可选择示例数据池，或由服务端注册数据、规划与检索生成模块。",
            "source_count": len(self.sources),
            "model_id": self.run_config.model_id,
            "max_wall_time_ms": self.run_config.max_wall_time_ms,
        }

    def signature(self):
        """固定已声明的适配器、数据与配置身份，不纳入可调用对象。"""
        return sha256(
            canonical_json(
                {
                    "metadata": self.metadata.model_dump(mode="json"),
                    "sources": [source.model_dump(mode="json") for source in self.sources],
                    "config": self.run_config.model_dump(mode="json"),
                    "pause_after_evidence": self.pause_after_evidence,
                }
            )
        ).hexdigest()


class ProfileRegistry:
    """固定的可信注册表；HTTP 层不导入或构造适配器。"""

    def __init__(self, profiles: Sequence[ExecutionProfile] | None = None):
        entries = default_profiles() if profiles is None else list(profiles)
        self.entries = {}
        for profile in entries:
            identity = profile.metadata.profile_id
            if identity in self.entries:
                raise ValueError("duplicate execution profile ID")
            self.entries[identity] = profile

    def listing(self):
        return [profile.public() for profile in self.entries.values()]

    def get(self, identity):
        try:
            return self.entries[identity]
        except (KeyError, TypeError):
            raise WebError("profile_not_found", "未找到该执行配置。", 404) from None

    def runnable(self, identity):
        profile = self.get(identity)
        if not profile.public()["ready"]:
            raise WebError("profile_unavailable", profile.public()["unavailable_reason"])
        return profile

    def for_record(self, record):
        identity = record.get("profile_id") or f"demo-{record['scenario']}"
        try:
            profile = self.runnable(identity)
        except WebError as error:
            if error.code == "profile_not_found":
                raise WebError(
                    "profile_unavailable", "原运行使用的执行配置当前未注册，不能继续恢复。"
                ) from None
            raise
        if record.get("profile_signature") not in {None, profile.signature()}:
            raise WebError("profile_changed", "执行配置或已注册数据目录已改变，不能继续原运行。")
        return profile


def default_profiles():
    """显式保留项目配置入口，并让示例可在无网络环境下运行。"""
    pending = ModuleState(status="unavailable", label="尚未接入")
    profiles = [
        ExecutionProfile(
            metadata=ProfileMetadata(
                profile_id="project-default",
                title="项目数据池",
                description="连接项目数据处理、规划与检索生成模块后，在此提交实际任务。",
                kind="project",
                dataset_id="project-data",
                dataset_name="项目数据池",
                revision="unconfigured-v1",
                modules=ModuleConnections(data=pending, planner=pending, content=pending),
            ),
            run_config=m.RunConfig(max_wall_time_ms=600_000, model_id="not-configured"),
        )
    ]
    for scenario in SCENARIOS:
        identity = scenario["id"]

        @asynccontextmanager
        async def factory(selected=identity):
            data, content, planner = demo_ports(selected)
            yield ExecutionPorts(data=data, content=content, planner=planner)

        data, _, _ = demo_ports(identity)
        fixture = ModuleState(status="fixture", label="本地合成示例")
        profiles.append(
            ExecutionProfile(
                metadata=ProfileMetadata(
                    profile_id=f"demo-{identity}",
                    title=f"示例 · {scenario['title']}",
                    description=scenario["description"],
                    kind="demo",
                    dataset_id="synthetic-revenue",
                    dataset_name="合成营收资料",
                    revision="synthetic-v1",
                    modules=ModuleConnections(data=fixture, planner=fixture, content=fixture),
                    suggested_questions=[scenario["question"]],
                ),
                sources=tuple(data.sources),
                ports_factory=factory,
                pause_after_evidence=identity == "recovery",
            )
        )
    return profiles
