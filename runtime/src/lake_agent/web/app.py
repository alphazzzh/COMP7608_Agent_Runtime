"""可选本地数据池项目工作区的 FastAPI 入口。

HTTP 层校验输入后委托 RunService 处理。命令行从 TOML 加载真实执行配置，
配置缺失或校验失败时在绑定端口之前退出，且仅绑定回环地址。
同源写入限制、严格的浏览器策略、固定静态路由和按引用限定的工件读取，
共同控制此本地展示界面的访问范围。
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, get_args
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from pydantic import Field, ValidationError
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .. import __version__
from .. import models as m
from ..config import ConfigError, load_config
from ..evaluation.models import EvaluationImport, EvaluationMode
from .configuration import configured_profile
from .reports import html_report
from .service import RunService
from .storage import WebError, utc_now

_STATIC = Path(__file__).with_name("static")

# 只描述内核实际提供的能力；模块连接状态由各执行配置单独返回。
_CAPABILITIES = [
    {
        "name": "受控规划与工具执行",
        "status": "implemented",
        "description": "验证模型提出的计划、工具参数、来源授权与依赖，再执行实际适配器。",
    },
    {
        "name": "证据、引用与运行审计",
        "status": "implemented",
        "description": "保留实际请求、响应和可引用证据，支持按执行尝试追溯。",
    },
    {
        "name": "预算、取消与安全恢复",
        "status": "implemented",
        "description": "统一控制预算与超时；恢复复用已验证的响应，结果未知时拒绝重派发。",
    },
]


class StartRun(m.Model):
    """只接受展示选项，不接收凭据、代码或外部路径。"""

    question: Annotated[str, Field(min_length=1, max_length=2000)]
    profile_id: Annotated[str, Field(min_length=1, max_length=100)] | None = None
    mode: m.Mode = "full_agent"


def create_app(
    data_dir=None,
    max_active_runs=2,
    *,
    profiles=None,
    allowed_modes: Sequence[m.Mode] = ("full_agent",),
    enable_evaluations: bool = False,
):
    """正式入口只开放完整 Agent；实验模式与评估由可信测试宿主显式启用。"""
    modes = tuple(allowed_modes)
    if (
        not modes
        or any(mode not in get_args(m.Mode) for mode in modes)
        or len(set(modes)) != len(modes)
    ):
        raise ValueError("allowed_modes must contain unique supported runtime modes")
    if type(enable_evaluations) is not bool:
        raise ValueError("enable_evaluations must be a boolean")
    service = RunService(
        data_dir or Path.cwd() / "runs" / "workspace",
        max_active_runs,
        profiles=profiles,
        enable_evaluations=enable_evaluations,
    )

    def require_mode(mode):
        # 创建与恢复采用相同的服务端策略，浏览器无法通过自造请求切换实验行为。
        if mode not in modes:
            raise WebError("mode_disabled", "当前工作台未开放该执行模式。", 422)

    def require_evaluations():
        if service.evaluations is None:
            raise WebError("not_found", "未找到该资源。", 404)
        return service.evaluations

    @asynccontextmanager
    async def lifespan(app):
        try:
            await service.start()
            yield
        finally:
            await service.close()

    app = FastAPI(
        title="Agent 项目工作台", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )
    app.state.run_service = service
    app.add_middleware(
        TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "[::1]", "testserver"]
    )

    @app.middleware("http")
    async def local_boundary(request, call_next):
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            origin = request.headers.get("origin")
            allowed = True
            if origin is not None:
                try:
                    parsed = urlsplit(origin)
                    allowed = (
                        parsed.scheme == request.url.scheme
                        and parsed.netloc == request.headers.get("host")
                        and not parsed.path
                        and not parsed.query
                        and not parsed.fragment
                        and parsed.hostname in {"localhost", "127.0.0.1", "::1", "testserver"}
                        and parsed.username is None
                        and parsed.password is None
                    )
                except ValueError:
                    allowed = False
            if not allowed or request.headers.get("sec-fetch-site") == "cross-site":
                return JSONResponse(
                    {
                        "detail": {
                            "code": "origin_forbidden",
                            "message": "仅允许从当前工作台页面操作。",
                        }
                    },
                    status_code=403,
                )
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
        )
        response.headers["Cache-Control"] = (
            "no-store" if request.url.path.startswith("/api/") else "no-cache"
        )
        return response

    @app.exception_handler(WebError)
    async def web_error(request, error):
        return JSONResponse(
            {"detail": {"code": error.code, "message": error.message}}, status_code=error.status
        )

    @app.exception_handler(RequestValidationError)
    async def bad_request(request, error):
        # Pydantic 错误输入可能包含调用方文本。固定响应可避免将这些文本
        # 回显到运维日志或 HTML 错误界面中。
        return JSONResponse(
            {
                "detail": {
                    "code": "invalid_request",
                    "message": "请求格式不正确，请检查问题、执行配置和运行模式。",
                }
            },
            status_code=422,
        )

    @app.get("/api/info")
    async def info():
        entries = service.profiles.listing()
        return {
            "version": __version__,
            "application": "project-workspace",
            "profiles": entries,
            "capabilities": _CAPABILITIES,
            "enabled_modes": list(modes),
            "features": {"evaluations": enable_evaluations},
        }

    @app.get("/api/profiles")
    async def profiles_list():
        return {"profiles": service.profiles.listing()}

    @app.get("/api/profiles/{profile_id}/sources")
    async def published_sources(profile_id: str):
        profile = service.profiles.get(profile_id)
        return {
            "profile": profile.public(),
            "sources": [source.model_dump(mode="json") for source in profile.sources],
        }

    @app.get("/api/evaluations")
    async def evaluations_list():
        repository = require_evaluations()
        return {"evaluations": await repository.listing()}

    @app.post("/api/evaluations", status_code=201)
    async def evaluation_import(request: Request):
        repository = require_evaluations()
        # JSON 解码前先统计流式读取的字节。单靠 Content-Length 无法限制
        # 分块请求，也无法应对客户端声明有误的情况。
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > 131_072:
                raise WebError("request_too_large", "评估文件不能超过 128 KiB。", 413)
            body.extend(chunk)
        try:
            report = EvaluationImport.model_validate_json(bytes(body))
        except ValidationError:
            raise WebError(
                "invalid_evaluation",
                "评估格式不正确，请检查指标范围、模式、任务数和带时区日期。",
                422,
            ) from None
        return await repository.create(report)

    @app.get("/api/evaluations/template")
    async def evaluation_template():
        require_evaluations()
        # 这里只提供导入模板，不代表已持久化或实测的分数。
        report = EvaluationImport.model_validate(
            {
                "title": "填写评估报告名称",
                "dataset_id": "填写数据集标识",
                "task_set_id": "填写固定任务集版本",
                "model_id": "填写模型与参数配置标识",
                "evaluator": "填写评估程序或评估者",
                "evaluated_at": utc_now(),
                "notes": "请填入实际评估时间、任务数和测量结果。未知指标保留 null。",
                # 报告对照不受可执行模式限制；fixed 仅记录外部评估结果。
                "results": [{"mode": mode, "task_count": 1} for mode in get_args(EvaluationMode)],
            }
        )
        return JSONResponse(
            report.model_dump(mode="json"),
            headers={"Content-Disposition": 'attachment; filename="evaluation-template.json"'},
        )

    @app.get("/api/evaluations/{evaluation_id}")
    async def evaluation_detail(evaluation_id: str):
        repository = require_evaluations()
        report = await repository.get(evaluation_id)
        # 链接和服务端都声明下载行为。内嵌浏览器可能忽略 download 属性；
        # fetch 仍按 JSON 读取响应。
        # get() 会先校验持久化记录的身份，再将其写入此响应头。
        return JSONResponse(
            report,
            headers={
                "Content-Disposition": f'attachment; filename="{report["evaluation_id"]}.json"'
            },
        )

    @app.get("/api/runs")
    async def listing():
        return {"runs": await service.listing()}

    @app.post("/api/runs", status_code=202)
    async def create(payload: StartRun):
        require_mode(payload.mode)
        question = payload.question.strip()
        if not question:
            raise WebError("invalid_request", "请输入问题。", 422)
        return await service.create(question, payload.mode, profile_id=payload.profile_id)

    @app.get("/api/runs/{run_id}")
    async def detail(run_id: str):
        return await service.view(run_id)

    @app.delete("/api/runs/{run_id}", status_code=204, response_class=Response)
    async def delete(run_id: str):
        """删除已停止任务的文件与持久状态；活动任务须先停止。"""
        await service.delete(run_id)
        return Response(status_code=204)

    @app.post("/api/runs/{run_id}/resume", status_code=202)
    async def recover(run_id: str):
        record = await service.view(run_id)
        require_mode(record["mode"])
        return await service.restart(run_id)

    @app.post("/api/runs/{run_id}/cancel", status_code=202)
    async def cancel(run_id: str):
        return await service.cancel(run_id)

    @app.get("/api/runs/{run_id}/artifacts/{attempt_id}/{ref}")
    async def artifact(run_id: str, attempt_id: str, ref: str):
        return await service.repository.artifact(run_id, attempt_id, ref)

    @app.get("/api/runs/{run_id}/export")
    async def export(run_id: str):
        return JSONResponse(
            await service.view(run_id),
            headers={"Content-Disposition": f'attachment; filename="{run_id}.json"'},
        )

    @app.get("/api/runs/{run_id}/report", response_class=HTMLResponse)
    async def report(run_id: str):
        view = await service.view(run_id)
        return HTMLResponse(
            html_report(view),
            headers={"Content-Disposition": f'attachment; filename="{run_id}.html"'},
        )

    @app.get("/")
    async def index():
        return FileResponse(_STATIC / "index.html")

    @app.get("/static/{name}")
    async def static(name: str):
        if name not in {"app.js", "styles.css"}:
            raise WebError("not_found", "未找到该资源。", 404)
        return FileResponse(_STATIC / name)

    return app


def main():
    """先校验配置并注册真实端口，再启动本地单工作进程服务器。"""
    import uvicorn

    parser = argparse.ArgumentParser(description="从 TOML 配置启动本地 Agent 项目工作台")
    parser.add_argument(
        "--config", type=Path, default=Path("config.toml"), help="服务端 TOML 配置文件"
    )
    parser.add_argument("--port", type=int, help="覆盖配置中的本机监听端口")
    parser.add_argument("--data-dir", type=Path, help="覆盖配置中的运行记录目录")
    args = parser.parse_args()
    if args.port is not None and not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    try:
        config = load_config(args.config)
        profile = configured_profile(config)
    except ConfigError as exc:
        parser.error(str(exc))
    data_dir = args.data_dir.expanduser().resolve() if args.data_dir else config.server.data_dir
    app = create_app(
        data_dir=data_dir,
        max_active_runs=config.server.max_active_runs,
        profiles=[profile],
    )
    uvicorn.run(app, host="127.0.0.1", port=args.port or config.server.port, workers=1)
