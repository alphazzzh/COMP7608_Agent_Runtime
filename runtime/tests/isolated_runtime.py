"""显式运行 B 模块隔离测试，不依赖队友尚未实现的数据与内容模块。

在 runtime 源码目录用 ``python -m tests.isolated_runtime --config config.toml``，
并明确选择 ``--web`` 或 ``--question``。规划、答案生成和验证调用配置中的真实 LLM；
数据与本地检索来自 tests/support，隔离内容适配器也不随生产 wheel 分发。
导入本模块不启动服务、不创建客户端、不写入运行记录。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import get_args
from uuid import uuid4

from lake_agent import models as m
from lake_agent.config import ConfigError, load_config
from lake_agent.runtime import run
from lake_agent.trace import JsonlTraceSink
from lake_agent.web.profiles import ExecutionProfile

from .support.isolated import build_profile

_SCOPE = (
    "B 模块隔离测试：数据与本地检索使用合成测试依赖。\n"
    "规划、回答生成和核验调用配置中的真实 LLM；未接入队友的生产 A/C 模块。"
)


async def _run_question(profile: ExecutionProfile, question: str, mode: m.Mode, directory: Path):
    """沿用真实控制器和审计接收端，退出时由 profile 关闭全部客户端。"""
    assert profile.ports_factory is not None
    async with profile.ports_factory() as ports:
        return await run(
            m.RunRequest(
                task_id=f"isolated-{uuid4().hex}",
                question=question,
                mode=mode,
                config=profile.run_config,
            ),
            data=ports.data,
            content=ports.content,
            planner=ports.planner,
            trace=JsonlTraceSink(directory),
        )


def main(argv: list[str] | None = None) -> int:
    """验证显式启动意图和配置；Web 仅绑定回环地址，记录使用独立目录。"""
    parser = argparse.ArgumentParser(
        description="隔离测试 B 模块：合成数据，真实模型规划、回答与核验"
    )
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--web", action="store_true", help="手动启动隔离测试网页")
    target.add_argument("--question", help="直接执行一个问题并将 RunResult JSON 打印到标准输出")
    parser.add_argument(
        "--mode", choices=get_args(m.Mode), help="仅用于 --question，默认 full_agent"
    )
    parser.add_argument("--port", type=int, help="仅用于 --web，覆盖配置端口")
    parser.add_argument("--data-dir", type=Path, help="覆盖隔离运行记录目录")
    args = parser.parse_args(argv)
    if args.port is not None and (not args.web or not 1 <= args.port <= 65535):
        parser.error("--port requires --web and a value from 1 to 65535.")
    if args.web and args.mode is not None:
        parser.error("--mode is only available with --question; select the Web mode in the page.")
    question = args.question.strip() if args.question is not None else None
    if question is not None and not 1 <= len(question) <= 2000:
        parser.error("--question must contain 1 to 2000 nonblank characters.")
    try:
        config = load_config(args.config)
        profile = build_profile(config)
        # Web 索引与 headless JSONL 的存储形状不同，默认分别存放以免相互干扰。
        directory = (
            args.data_dir.expanduser().resolve()
            if args.data_dir is not None
            else config.server.data_dir / "isolated-runtime" / ("web" if args.web else "headless")
        )
        print(_SCOPE, file=sys.stderr)
        if args.web:
            try:
                import uvicorn

                from lake_agent.web.app import create_app
            except ImportError:
                parser.error("Web dependencies are missing; install the 'web' extra.")
            app = create_app(
                data_dir=directory,
                max_active_runs=config.server.max_active_runs,
                profiles=[profile],
                # 实验能力只由这个显式测试入口启用，正式 CLI 采用产品默认策略。
                allowed_modes=get_args(m.Mode),
                enable_evaluations=True,
            )
            uvicorn.run(app, host="127.0.0.1", port=args.port or config.server.port, workers=1)
            return 0
        result = asyncio.run(_run_question(profile, question, args.mode or "full_agent", directory))
        print(result.model_dump_json(indent=2))
        return 0 if result.status == "completed" else 1
    except ConfigError as error:
        parser.error(str(error))
    except OSError:
        parser.error("Isolation storage or local server could not be opened.")
    except KeyboardInterrupt:
        return 130
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
