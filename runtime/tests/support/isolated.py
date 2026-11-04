"""显式隔离 B 模块：合成数据、本地检索及真实模型规划、生成和验证。

这个模块仅供独立测试脚本使用，不属于产品入口，也不为配置失败提供回退。
数据与检索采用测试替身，内容端口实际调用配置中的 LLM 生成并验证当前问题的答案。
不会回退固定答案或固定验证成功；模型连接与运行预算来自原始配置。
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace

from lake_agent.config import AdapterSettings, AppConfig, ConfigError, LLMSettings, ProfileSettings
from lake_agent.web.configuration import configured_profile
from lake_agent.web.profiles import ExecutionProfile, ModuleConnections, ModuleState

from .model_content import ModelContent
from .ports import DemoData


@asynccontextmanager
async def create_data_port(*, options: dict, llm: LLMSettings):
    """提供新的合成数据实例；options/llm 仅遵守真实工厂签名，不发起请求。"""
    yield DemoData()


@asynccontextmanager
async def create_content_port(*, options: dict, llm: LLMSettings):
    """每次运行独占真实内容模型客户端，退出时关闭连接并清除本地目录。"""
    async with ModelContent(llm) as content:
        yield content


def build_profile(config: AppConfig) -> ExecutionProfile:
    """显式替换尚未接入的 A/C，保留配置中的真实模型与全部运行限制。"""
    if config.run.dataset_snapshot not in {None, "demo-v1"}:
        # 保留真实预算，但不允许项目快照误绑定到合成目录；错误不回显原快照值。
        raise ConfigError("Isolation requires run.dataset_snapshot to be unset or 'demo-v1'.")
    isolated = config.model_copy(deep=True)
    isolated.adapters.data = AdapterSettings(factory="tests.support.isolated:create_data_port")
    isolated.adapters.content = AdapterSettings(
        factory="tests.support.isolated:create_content_port"
    )
    isolated.profile = ProfileSettings(
        profile_id="isolated-b-runtime",
        title="B 模块隔离测试 · 真实 LLM",
        dataset_id="isolated-synthetic-revenue",
        dataset_name="隔离测试合成资料",
        revision="isolation-fixture-v3",
        description="数据与本地检索使用合成测试依赖；规划、答案生成和证据验证均调用配置的真实 LLM。"
        "模型接收当前问题、实际收集证据及本运行的来源标签；不回退固定答案或固定通过判定。",
        suggested_questions=["目前的合成资料有哪些？", "合成资料中 2024 年年度营收是多少？"],
    )
    # 必须沿用产品的真实工厂与客户端生命周期，不引入另一套规划器接线路径。
    profile = configured_profile(isolated)
    metadata = profile.metadata.model_copy(
        deep=True,
        update={
            "modules": ModuleConnections(
                data=ModuleState(status="connected", label="A：合成数据与工具测试替身"),
                planner=ModuleState(status="connected", label="B：真实 JsonPlanner / 配置 LLM"),
                content=ModuleState(
                    status="connected", label="C：本地测试检索 / 真实 LLM 生成与验证"
                ),
            )
        },
    )
    return replace(profile, metadata=metadata, sources=tuple(DemoData().sources))
