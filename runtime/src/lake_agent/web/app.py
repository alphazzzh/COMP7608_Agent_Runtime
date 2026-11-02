"""FastAPI entry point for the optional local data-pool project workspace.

The HTTP layer validates input and delegates to DemoService. The CLI binds only
loopback. Same-origin writes, a restrictive browser policy, fixed static routes
and reference-scoped artifact reads keep this local presentation surface small.
"""

from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import Field, ValidationError
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .. import __version__
from .. import models as m
from .evaluations import EvaluationImport
from .fixtures import CAPABILITIES, SCENARIOS
from .reports import html_report
from .service import DemoService
from .storage import WebError, utc_now

_STATIC = Path(__file__).with_name("static")


class StartRun(m.Model):
    """Accept presentation choices only; no credentials, code or external paths."""

    question: Annotated[str, Field(min_length=1, max_length=2000)]
    scenario: Literal["standard", "parallel", "recovery"] | None = None
    profile_id: Annotated[str, Field(min_length=1, max_length=100)] | None = None
    mode: m.Mode = "full_agent"


def create_app(data_dir=None, max_active_runs=2, *, profiles=None):
    """Create a single-owner application with caller-selected local storage."""
    service = DemoService(
        data_dir or Path.cwd() / "runs" / "web-demo", max_active_runs, profiles=profiles
    )

    @asynccontextmanager
    async def lifespan(app):
        try:
            await service.start()
            yield
        finally:
            await service.close()

    app = FastAPI(
        title="Agent 演示工作台", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None
    )
    app.state.demo_service = service
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
                            "message": "仅允许从当前演示页面操作。",
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
        # Pydantic error inputs may contain caller text. A fixed response avoids
        # echoing it into operational logs or HTML error displays.
        return JSONResponse(
            {
                "detail": {
                    "code": "invalid_request",
                    "message": "请求格式不正确，请检查问题、场景和运行模式。",
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
            "demo_only": not any(item["ready"] and item["kind"] == "project" for item in entries),
            "profiles": entries,
            "scenarios": SCENARIOS,
            "capabilities": CAPABILITIES,
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
        return {"evaluations": await service.evaluations.listing()}

    @app.post("/api/evaluations", status_code=201)
    async def evaluation_import(request: Request):
        # Count streamed bytes before JSON decoding. Content-Length alone does
        # not bound chunked requests or malformed client declarations.
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
        return await service.evaluations.create(report)

    @app.get("/api/evaluations/template")
    async def evaluation_template():
        # This is an import template, never a persisted or measured score.
        report = EvaluationImport.model_validate(
            {
                "title": "填写评估报告名称",
                "dataset_id": "填写数据集标识",
                "task_set_id": "填写固定任务集版本",
                "model_id": "填写模型与参数配置标识",
                "evaluator": "填写评估程序或评估者",
                "evaluated_at": utc_now(),
                "notes": "请填入实际评估时间、任务数和测量结果。未知指标保留 null。",
                "results": [
                    {"mode": mode, "task_count": 1}
                    for mode in ("full_agent", "no_verifier", "fixed_retrieval")
                ],
            }
        )
        return JSONResponse(
            report.model_dump(mode="json"),
            headers={"Content-Disposition": 'attachment; filename="evaluation-template.json"'},
        )

    @app.get("/api/evaluations/{evaluation_id}")
    async def evaluation_detail(evaluation_id: str):
        report = await service.evaluations.get(evaluation_id)
        # Declare downloads server-side as well as on the anchor. Embedded
        # browsers can ignore the download attribute; fetch still reads JSON.
        # get() validates the persisted identity before it enters this header.
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
        if payload.profile_id is not None and payload.scenario is not None:
            raise WebError("invalid_request", "执行配置与旧版场景不能同时指定。", 422)
        question = payload.question.strip()
        if not question:
            raise WebError("invalid_request", "请输入问题。", 422)
        return await service.create(
            question, payload.scenario, payload.mode, profile_id=payload.profile_id
        )

    @app.get("/api/runs/{run_id}")
    async def detail(run_id: str):
        return await service.view(run_id)

    @app.post("/api/runs/{run_id}/resume", status_code=202)
    async def recover(run_id: str):
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
    """Start a local single-worker server; data and port are caller-controlled."""
    import uvicorn

    parser = argparse.ArgumentParser(
        description="启动本地 Agent 演示工作台（合成数据、无模型请求）"
    )
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--data-dir", type=Path, default=Path("runs/web-demo"))
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    uvicorn.run(create_app(args.data_dir), host="127.0.0.1", port=args.port, workers=1)
