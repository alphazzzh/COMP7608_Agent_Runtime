"""本地数据表的只读查询校验和独立执行进程。"""

from __future__ import annotations

import json
import math
from pathlib import Path

from sqlglot import exp, parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import Scope, traverse_scope

from ..validation import canonical_json

MAX_ROWS = 100

# 只接纳课程演示所需的 SQL 表达式。通用函数和表函数不属于此接口。
_QUERY_NODES = frozenset(
    {
        exp.Select,
        exp.From,
        exp.Table,
        exp.TableAlias,
        exp.Column,
        exp.Identifier,
        exp.Alias,
        exp.Star,
        exp.CTE,
        exp.With,
        exp.Literal,
        exp.Null,
        exp.Boolean,
        exp.Where,
        exp.Group,
        exp.Having,
        exp.Order,
        exp.Ordered,
        exp.Limit,
        exp.Offset,
        exp.Distinct,
        exp.Paren,
        exp.And,
        exp.Or,
        exp.Not,
        exp.EQ,
        exp.NEQ,
        exp.GT,
        exp.GTE,
        exp.LT,
        exp.LTE,
        exp.Is,
        exp.In,
        exp.Between,
        exp.Like,
        exp.ILike,
        exp.Add,
        exp.Sub,
        exp.Mul,
        exp.Div,
        exp.Mod,
        exp.Neg,
        exp.Count,
        exp.Sum,
        exp.Avg,
        exp.Min,
        exp.Max,
        exp.Coalesce,
        exp.Round,
        exp.Case,
        exp.If,
    }
)


def normalize_query(sql: str) -> str | None:
    """用 DuckDB 方言重建单条查询，只接纳已注册的 data 表及局部 CTE。"""
    try:
        (query,) = parse(sql, read="duckdb")
        if not isinstance(query, exp.Select):
            return None
        if any(type(node) not in _QUERY_NODES for node in query.walk()):
            return None
        if any(node.args.get("recursive") for node in query.find_all(exp.With)):
            return None
        reads_data = False
        for scope in traverse_scope(query):
            for _, source in scope.selected_sources.values():
                if isinstance(source, Scope):
                    continue
                if (
                    not isinstance(source, exp.Table)
                    or not isinstance(source.this, exp.Identifier)
                    or source.name.casefold() != "data"
                    or source.db
                    or source.catalog
                ):
                    return None
                reads_data = True
        return query.sql(dialect="duckdb", comments=False) if reads_data else None
    except (SqlglotError, ValueError, RecursionError):
        return None


def _json_cell(value):
    """保留普通数值，为日期和精确小数提供可序列化文本。"""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value if value is None or type(value) in (bool, int, float, str) else str(value)


def run_query(path: Path, sql: str, limit: int) -> tuple[dict, bool]:
    """在可独立终止的进程中解析本地材料并取回有限行数。"""
    import duckdb
    import pandas as pd

    if path.suffix.lower() == ".json":
        frame = pd.json_normalize(json.loads(path.read_text(encoding="utf-8")))
    else:
        frame = pd.read_csv(
            path, sep="," if path.suffix.lower() == ".csv" else None, engine="python"
        )
    with duckdb.connect(
        config={
            "enable_external_access": False,
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
            "python_enable_replacements": False,
            "threads": 1,
            "memory_limit": "128MB",
            "max_temp_directory_size": "0B",
        }
    ) as connection:
        connection.register("data", frame)
        result = connection.execute(sql)
        columns = [column[0] for column in result.description]
        table = {"columns": columns, "rows": []}
        used = len(canonical_json(table))
        for index in range(MAX_ROWS + 1):
            row = result.fetchone()
            if row is None:
                return table, False
            if index == MAX_ROWS:
                break
            values = [_json_cell(value) for value in row]
            used += len(canonical_json(values)) + bool(index)
            if used > limit:
                break
            table["rows"].append(values)
        return table, True
