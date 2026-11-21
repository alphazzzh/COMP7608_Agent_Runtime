"""组员本地数据端口的接入版，提供分页、带来源抽取和单文件表格查询。"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path

from .. import models as m
from ..validation import canonical_json
from ._data_query import normalize_query, run_query

_EXTENSIONS = {".txt", ".csv", ".json", ".pdf"}
_MAX_QUERY_FILE_BYTES = 16 * 1024 * 1024
_SNAPSHOT = "local-snapshot-v1"


def _error(call_id: str, code: str, message: str) -> dict:
    """生成紧凑错误封套，默认字段由共享模型补足。"""
    return {"call_id": call_id, "status": "error", "error": {"code": code, "message": message}}


def _size_error(call_id: str, limit: int) -> dict:
    """若最小合法错误都放不下，明确交由 Runtime 按无效回复处理。"""
    error = _error(call_id, "response_too_large", "Output exceeds byte budget.")
    if len(canonical_json(error)) > limit:
        error["error"]["message"] = "Limit"
    if len(canonical_json(error)) > limit:
        raise ValueError("response budget cannot fit the minimal error envelope")
    return error


def _fit_response(response: dict, limit: int) -> dict:
    """按 Runtime 的完整 JSON 字节数裁剪正文或表格，不破坏来源和行结构。"""
    evidence = (response.get("data") or {}).get("evidence", [])
    if len(canonical_json(response)) <= limit:
        return response
    if not evidence:
        return _size_error(response["call_id"], limit)

    response["truncated"] = True
    record = evidence[0]
    payload = record["payload"]
    content = payload if isinstance(payload, str) else payload["rows"]
    low, high = 0, len(content)
    while low < high:
        length = (low + high + 1) // 2
        if isinstance(payload, str):
            record["payload"] = content[:length]
        else:
            payload["rows"] = content[:length]
        if len(canonical_json(response)) <= limit:
            low = length
        else:
            high = length - 1
    if not low:
        return _size_error(response["call_id"], limit)
    if isinstance(payload, str):
        record["payload"] = content[:low]
    else:
        payload["rows"] = content[:low]
    return response


def _record(source_id: str, kind: str, payload, locator: dict) -> dict:
    """创建不含绝对路径的证据。"""
    return {"source_id": source_id, "content_type": kind, "payload": payload, "locator": locator}


def _json_leaves(value, pointer: str = "") -> Iterator[tuple[str, object]]:
    """使用 JSON Pointer 表示字段或数组位置，避免字段名中的标点产生冲突。"""
    if isinstance(value, dict):
        for key, child in value.items():
            token = key.replace("~", "~0").replace("/", "~1")
            yield from _json_leaves(child, f"{pointer}/{token}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _json_leaves(child, f"{pointer}/{index}")
    else:
        yield pointer, value


def _extract(path: Path, source_id: str, query: str, max_chars: int) -> tuple[list[dict], bool]:
    """在附件原有格式中提取首条匹配，尽量保留命中词附近内容。"""
    suffix = path.suffix.lower()
    keyword = query.casefold()

    def find(candidates, kind):
        for locator, value in candidates:
            text = str(value).strip()
            hit = text.casefold().find(keyword)
            if hit >= 0:
                start = max(0, min(hit - max_chars // 4, len(text) - max_chars))
                return [_record(source_id, kind, text[start : start + max_chars], locator)], len(
                    text
                ) > max_chars
        return [], False

    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        return find(
            (({"pointer": pointer}, value) for pointer, value in _json_leaves(data)), "json_value"
        )
    if suffix == ".pdf":
        from pypdf import PdfReader

        with path.open("rb") as handle:
            pages = PdfReader(handle).pages
            return find(
                (
                    ({"page": number}, page.extract_text() or "")
                    for number, page in enumerate(pages, 1)
                ),
                "pdf_text",
            )
    if suffix == ".csv":
        import csv

        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = csv.DictReader(handle)
            return find(
                (({"row_index": index}, row) for index, row in enumerate(rows)), "table_row"
            )
    with path.open(encoding="utf-8-sig", errors="replace") as handle:
        return find(
            (({"line_number": number}, line) for number, line in enumerate(handle, 1)), "text_line"
        )


async def _stop_worker(launch: asyncio.Task, io: asyncio.Task | None) -> None:
    """结束仍运行的工作进程并排空管道，避免超时或取消后遗留子进程。"""
    (process,) = await asyncio.gather(launch, return_exceptions=True)
    if not isinstance(process, asyncio.subprocess.Process):
        return
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    if io is None:
        await process.communicate()
    else:
        await asyncio.gather(io, return_exceptions=True)


async def _query(path: Path, sql: str, request: m.ToolRequest) -> dict | None:
    """让同步 DuckDB 工作可由适配器和 Runtime 取消，不依赖数据库超时配置。"""
    launch = asyncio.create_task(
        asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            "-m",
            "lake_agent.adapters.data_adapter",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    )
    process = None
    io = None
    try:
        # 留出少量时间清理进程和将超时状态送回控制器。
        seconds = request.timeout_ms * 0.9 / 1000
        async with asyncio.timeout(seconds):
            process = await asyncio.shield(launch)
            payload = [str(path), sql, request.call_id, request.args["source_id"]]
            io = asyncio.create_task(
                process.communicate(canonical_json([*payload, request.max_output_bytes]))
            )
            stdout, _ = await asyncio.shield(io)
        return json.loads(stdout) if process.returncode == 0 and stdout else None
    finally:
        if process is None or process.returncode is None or io is None or not io.done():
            # 创建和清理期间也可能收到取消，收割进程后再向调用方传播。
            cleanup = asyncio.create_task(_stop_worker(launch, io))
            cancelled = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    cancelled = True
            cleanup.result()
            if cancelled:
                raise asyncio.CancelledError


def _query_worker() -> None:
    """执行父进程校验过的查询，只向主进程传输预算内的响应。"""
    path, sql, call_id, source_id, limit = json.load(sys.stdin)
    table, truncated = run_query(Path(path), sql, limit)
    if truncated and not table["rows"]:
        response = _size_error(call_id, limit)
    else:
        evidence = [_record(source_id, "structured_result", table, {"type": "sql_query"})]
        response = _fit_response(
            {"call_id": call_id, "data": {"evidence": evidence}, "truncated": truncated}, limit
        )
    sys.stdout.buffer.write(canonical_json(response))


class LocalDataPort:
    """为每轮运行注册有序来源目录，使用调用方冻结的本地材料。"""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir).expanduser().resolve()
        if not self.base_dir.is_dir():
            raise ValueError("options.base_dir must name an existing directory")
        self.sources = []
        for path in sorted(self.base_dir.rglob("*")):
            if path.suffix.lower() in _EXTENSIONS and path.is_file() and not path.is_symlink():
                self.sources.append(
                    {
                        "source_id": path.relative_to(self.base_dir).as_posix(),
                        "name": path.name,
                        "format": path.suffix.lower()[1:],
                        "summary": f"文件: {path.name}",
                    }
                )
        self.source_ids = {source["source_id"] for source in self.sources}

    def _source_path(self, source_id: str) -> Path:
        """文件仍须存在且位于配置的数据根目录内。"""
        path = (self.base_dir / source_id).resolve(strict=True)
        if not path.is_relative_to(self.base_dir) or not path.is_file():
            raise PermissionError
        return path

    def tool_specs(self) -> list[m.ToolSpec]:
        """声明源参数以及可用的抽取和单表 SQL 能力。"""
        return [
            m.ToolSpec(
                name=name,
                description=description,
                idempotent=True,
                uses_sources=True,
                source_id_paths=["/source_id"],
                input_schema={
                    "type": "object",
                    "properties": {"source_id": {"type": "string", "minLength": 1}, **args},
                    "required": ["source_id", *args],
                    "additionalProperties": False,
                },
            )
            for name, description, args in (
                (
                    "extract",
                    "抽取本地 TXT、CSV、JSON 或 PDF 的首条匹配证据",
                    {
                        "query": {"type": "string", "minLength": 1},
                        "max_chars": {"type": "integer", "minimum": 1},
                    },
                ),
                (
                    "query",
                    "在此来源的 CSV、表格 TXT 或 JSON 上查询 data 表。支持 SELECT、常用聚合和非递归 CTE，最多返回 100 行",
                    {
                        "sql": {"type": "string", "minLength": 1, "maxLength": 4000},
                    },
                ),
            )
        ]

    async def catalog(self, request: m.CatalogRequest) -> dict:
        """只返回响应预算内的完整来源条目和下一页游标。"""
        if request.snapshot_id not in (None, _SNAPSHOT):
            return _fit_response(
                _error(request.call_id, "catalog_snapshot_mismatch", "Snapshot unavailable."),
                request.max_output_bytes,
            )
        try:
            offset = int(request.cursor or "0")
        except ValueError:
            offset = -1
        if not 0 <= offset <= len(self.sources):
            return _fit_response(
                _error(request.call_id, "invalid_cursor", "Invalid cursor."),
                request.max_output_bytes,
            )
        end = min(offset + request.page_size, len(self.sources))
        response = {"call_id": request.call_id, "catalog_snapshot_id": _SNAPSHOT}
        while end >= offset:
            more = end < len(self.sources)
            if end == offset and more:
                break
            response.update(
                sources=self.sources[offset:end],
                has_more=more,
                next_cursor=str(end) if more else None,
            )
            if len(canonical_json(response)) <= request.max_output_bytes:
                return response
            end -= 1
        return _size_error(request.call_id, request.max_output_bytes)

    async def invoke(self, request: m.ToolRequest) -> dict:
        """按已注册来源派发并统一计量完整响应。"""
        result = await self._invoke(request)
        return _fit_response(result, request.max_output_bytes)

    async def _invoke(self, request: m.ToolRequest) -> dict:
        """校验当前目录身份并返回可审计的工具结果。"""
        call_id, args = request.call_id, request.args
        source_id = args.get("source_id")
        if (
            request.catalog_snapshot_id != _SNAPSHOT
            or not isinstance(source_id, str)
            or source_id not in self.source_ids
        ):
            return _error(call_id, "source_authorization_failed", "Source unavailable.")
        try:
            path = await asyncio.to_thread(self._source_path, source_id)
            if request.tool_name == "extract":
                evidence, truncated = await asyncio.to_thread(
                    _extract, path, source_id, args["query"], args["max_chars"]
                )
            elif request.tool_name == "query":
                if path.suffix.lower() not in {".csv", ".txt", ".json"}:
                    return _error(call_id, "unsupported_format", "Source is not a supported table.")
                if (await asyncio.to_thread(path.stat)).st_size > _MAX_QUERY_FILE_BYTES:
                    return _error(call_id, "source_too_large", "Query source exceeds 16 MiB.")
                sql = normalize_query(args["sql"])
                if sql is None:
                    return _error(call_id, "invalid_query", "Use a supported SELECT on data.")
                response = await _query(path, sql, request)
                if response is None:
                    return _error(
                        call_id, "sql_execution_error", "Check table format and column names."
                    )
                return response
            else:
                return _error(call_id, "invalid_args", "Unknown data tool.")
        except TimeoutError:
            return _error(call_id, "timeout", "Data call timed out.")
        except (FileNotFoundError, PermissionError):
            return _error(call_id, "source_authorization_failed", "Source unavailable.")
        except (OSError, ValueError, RuntimeError, TypeError, KeyError):
            return _error(call_id, "data_read_error", "Unable to read source.")
        return {"call_id": call_id, "data": {"evidence": evidence}, "truncated": truncated}


@asynccontextmanager
async def create_port(*, options: dict, llm=None):
    """从宿主提供的资料目录加载数据端口。"""
    base_dir = options.get("base_dir")
    if not isinstance(base_dir, str) or not base_dir:
        raise ValueError("options.base_dir must name a local data directory")
    yield await asyncio.to_thread(LocalDataPort, base_dir)


if __name__ == "__main__":
    _query_worker()
