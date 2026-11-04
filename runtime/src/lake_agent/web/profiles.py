"""在服务端可信地绑定数据池与新建的执行端口。

浏览器只接收声明式元数据。工厂函数与凭据留在宿主进程中；
每次运行或恢复都会进入新的异步上下文，由其所有者关闭客户端。
签名将运行绑定到已注册的数据与配置，但不替代核心更严格的恢复校验。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Annotated, Literal

from pydantic import Field

from .. import models as m
from ..ports import AgentPlannerPort, ContentPort, DataPort
from ..validation import canonical_json
from .storage import WebError

_Id = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")]
_Label = Annotated[str, Field(min_length=1, max_length=200)]


class ModuleState(m.Model):
    """说明执行中的某一部分由哪个实现提供。"""

    status: Literal["connected", "unavailable"]
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
    kind: Literal["project"]
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
    planner: AgentPlannerPort


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
    # 连接身份只参与恢复校验，不公开连接地址、选项或凭据。
    binding_id: str | None = field(default=None, repr=False)

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
            else "尚未配置完整执行模块。请在服务端配置数据、规划与内容模块后重新启动。",
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
                    "binding_id": self.binding_id,
                }
            )
        ).hexdigest()


class ProfileRegistry:
    """固定的可信注册表；HTTP 层不导入或构造适配器。"""

    def __init__(self, profiles: Sequence[ExecutionProfile] | None = None):
        entries = [] if profiles is None else list(profiles)
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

    def runnable(self, identity=None):
        """仅在配置唯一时省略身份；空配置或歧义必须明确拒绝。"""
        if identity is None:
            if not self.entries:
                raise WebError(
                    "profiles_unconfigured", "尚未配置执行模块。请配置服务端执行模块后重新启动。"
                )
            if len(self.entries) != 1:
                raise WebError("profile_required", "请选择一个执行配置后提交任务。", 422)
            identity = next(iter(self.entries))
        profile = self.get(identity)
        if not profile.public()["ready"]:
            raise WebError("profile_unavailable", profile.public()["unavailable_reason"])
        return profile

    def for_record(self, record):
        # 恢复必须使用运行保存的明确身份，不能按当前默认配置猜测旧连接。
        identity = record.get("profile_id")
        if identity is None:
            raise WebError("profile_unavailable", "原运行未保存执行配置身份，不能继续恢复。")
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
