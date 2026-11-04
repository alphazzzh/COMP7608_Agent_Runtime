"""将可信 TOML 配置绑定为真实执行 profile，不提供合成数据或规则规划器。

工厂只从服务端配置导入。每次 run/resume 进入独立 AsyncExitStack，
依次取得数据端口、内容端口和真实 JsonPlanner，并按逆序释放资源。
连接参数只进入进程内工厂与私有身份摘要，不出现在公开 profile 元数据中。
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from contextlib import AsyncExitStack, asynccontextmanager
from copy import deepcopy
from hashlib import sha256
from importlib import import_module
from typing import Any

from ..adapters.planner import JsonPlanner
from ..config import AppConfig, ConfigError, LLMSettings
from ..validation import canonical_json
from .profiles import (
    ExecutionPorts,
    ExecutionProfile,
    ModuleConnections,
    ModuleState,
    ProfileMetadata,
)


def _import_factory(target: str) -> Callable[..., Any]:
    """解析可信 module:function；任何第三方导入异常都使用固定错误文字。"""
    try:
        module_name, function_name = target.split(":", 1)
        factory = getattr(import_module(module_name), function_name)
        if not callable(factory):
            raise TypeError("factory must be callable")
        # 提前检查约定的两个关键字，避免把签名错误延迟成请求期间的模糊失败。
        inspect.signature(factory).bind(options={}, llm=None)
        return factory
    except Exception:
        raise ConfigError("A configured adapter factory could not be loaded.") from None


def _is_async_callable(value: object) -> bool:
    """同时支持 async def 方法和以 async __call__ 实现的可调用对象。"""
    return callable(value) and (
        inspect.iscoroutinefunction(value) or inspect.iscoroutinefunction(type(value).__call__)
    )


def _validate_port(port: object, role: str) -> None:
    """只检查协议形状，不调用工具、目录或生成方法。"""
    methods = (
        ("catalog", "invoke")
        if role == "data"
        else ("prepare_catalog", "search", "compose", "verify")
    )
    if any(not _is_async_callable(getattr(port, name, None)) for name in methods):
        raise ConfigError("A configured adapter does not implement the async port contract.")
    if role == "data":
        specs = getattr(port, "tool_specs", None)
        if not callable(specs) or _is_async_callable(specs) or inspect.isasyncgenfunction(specs):
            raise ConfigError("The configured data adapter requires synchronous tool metadata.")


class _PrivateAdapterContext:
    """遮盖清理错误；适配器不能抑制或替换控制器的失败与取消信号。"""

    def __init__(self, manager: Any) -> None:
        self.manager = manager

    async def __aenter__(self) -> object:
        return await self.manager.__aenter__()

    async def __aexit__(self, exception_type, exception, traceback):
        try:
            await self.manager.__aexit__(exception_type, exception, traceback)
        except Exception:
            # 清理期间的普通错误不能覆盖控制器错误，尤其不能把原始取消
            # 改为 ConfigError，否则服务层会丢失可恢复的取消状态。
            # 新产生的 CancelledError 属于 BaseException，仍直接向上传播。
            if exception is not None:
                return False
            raise ConfigError("A configured adapter failed while closing its context.") from None
        # 工厂只拥有资源，不能通过返回 True 吞掉控制器异常并伪装执行成功。
        return False


async def _open_adapter(
    stack: AsyncExitStack,
    factory: Callable[..., Any],
    *,
    options: dict,
    llm: LLMSettings,
    role: str,
) -> object:
    """进入工厂上下文后立刻登记清理，再校验返回端口的异步方法。"""
    try:
        manager = factory(options=deepcopy(options), llm=llm.model_copy(deep=True))
        if inspect.iscoroutine(manager):
            # 错误的 async factory 会返回未等待协程；主动关闭避免资源警告。
            manager.close()
            raise TypeError("factory returned a coroutine")
        if inspect.isasyncgen(manager):
            await manager.aclose()
            raise TypeError("factory returned an async generator")
        if not all(
            _is_async_callable(getattr(manager, method, None))
            for method in ("__aenter__", "__aexit__")
        ):
            raise TypeError("factory requires an async context manager")
        port = await stack.enter_async_context(_PrivateAdapterContext(manager))
        _validate_port(port, role)
        return port
    except Exception:
        raise ConfigError("A configured adapter failed to enter its port context.") from None


def configured_profile(config: AppConfig) -> ExecutionProfile:
    """注册真实连接；此阶段只导入工厂，不进入上下文或发起 HTTP 请求。"""
    snapshot = config.model_copy(deep=True)
    data_factory = _import_factory(snapshot.adapters.data.factory)
    content_factory = _import_factory(snapshot.adapters.content.factory)

    # 仅摘要纳入恢复签名；密钥轮换不改变行为身份。options 可能包含连接密码，
    # 因此不将摘要前的对象、工厂路径或选项复制进任何公开元数据。
    binding_id = sha256(
        canonical_json(
            {
                "endpoint": snapshot.llm.base_url,
                "model_id": snapshot.llm.model_id,
                "model_params": snapshot.llm.model_params,
                "adapters": snapshot.adapters.model_dump(mode="json"),
            }
        )
    ).hexdigest()

    @asynccontextmanager
    async def ports_factory():
        """每次进入均拥有独立适配器、模型客户端与配置副本。"""
        async with AsyncExitStack() as stack:
            data = await _open_adapter(
                stack,
                data_factory,
                options=snapshot.adapters.data.options,
                llm=snapshot.llm,
                role="data",
            )
            content = await _open_adapter(
                stack,
                content_factory,
                options=snapshot.adapters.content.options,
                llm=snapshot.llm,
                role="content",
            )
            try:
                planner = await stack.enter_async_context(
                    JsonPlanner(
                        snapshot.llm.base_url,
                        snapshot.llm.api_key.get_secret_value(),
                    )
                )
            except Exception:
                raise ConfigError("The configured planner client could not be created.") from None
            yield ExecutionPorts(data=data, content=content, planner=planner)

    metadata = snapshot.profile
    return ExecutionProfile(
        metadata=ProfileMetadata(
            **metadata.model_dump(exclude={"description"}),
            description=metadata.description
            or "使用服务端配置的数据、规划与检索生成模块处理任务。",
            kind="project",
            modules=ModuleConnections(
                data=ModuleState(status="connected", label="项目数据适配器"),
                planner=ModuleState(status="connected", label="JSON 规划器"),
                content=ModuleState(status="connected", label="项目内容适配器"),
            ),
        ),
        ports_factory=ports_factory,
        run_config=snapshot.run,
        binding_id=binding_id,
    )
