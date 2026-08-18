"""Bounded read-only SQL queries over portable runpacks."""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.model import JsonValue
from runtime_tools.storage import RunpackReader


class QueryError(ValueError):
    """Raised when a runpack query is invalid or attempts mutation."""


MAX_QUERY_ROWS = 100_000
MAX_QUERY_VM_STEPS = 25_000_000
_QUERY_PROGRESS_INTERVAL = 1_000


_ALLOWED_SQLITE_ACTIONS = {
    sqlite3.SQLITE_FUNCTION,
    sqlite3.SQLITE_READ,
    sqlite3.SQLITE_RECURSIVE,
    sqlite3.SQLITE_SELECT,
}


def _authorize_read(
    action: int,
    _argument_one: str | None,
    _argument_two: str | None,
    _database: str | None,
    _source: str | None,
) -> int:
    return sqlite3.SQLITE_OK if action in _ALLOWED_SQLITE_ACTIONS else sqlite3.SQLITE_DENY


@dataclass(frozen=True, slots=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[JsonValue, ...], ...]
    truncated: bool

    def as_json_value(self) -> dict[str, JsonValue]:
        return {
            "columns": list(self.columns),
            "rows": [list(row) for row in self.rows],
            "truncated": self.truncated,
        }


def _value(value: object) -> JsonValue:
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            label = "nan"
        else:
            label = "infinity" if value > 0 else "-infinity"
        return {"encoding": "non-finite-float", "value": label}
    if value is None or isinstance(value, (str, int, float)):
        return value
    if isinstance(value, bytes):
        return {"encoding": "hex", "value": value.hex()}
    return str(value)


def query_runpack(path: Path, sql: str, *, limit: int = 1000) -> QueryResult:
    if not sql.strip():
        raise QueryError("SQL query cannot be empty")
    if limit <= 0:
        raise QueryError("query limit must be positive")
    if limit > MAX_QUERY_ROWS:
        raise QueryError(f"query limit cannot exceed {MAX_QUERY_ROWS}")
    with RunpackReader(path):
        pass
    uri = f"{path.resolve().as_uri()}?mode=ro"
    completed_steps = 0
    work_limit_reached = False

    def enforce_work_limit() -> int:
        nonlocal completed_steps, work_limit_reached
        completed_steps += _QUERY_PROGRESS_INTERVAL
        if completed_steps >= MAX_QUERY_VM_STEPS:
            work_limit_reached = True
            return 1
        return 0

    try:
        with sqlite3.connect(uri, uri=True) as connection:
            connection.set_authorizer(_authorize_read)
            connection.set_progress_handler(enforce_work_limit, _QUERY_PROGRESS_INTERVAL)
            cursor = connection.execute(sql)
            if cursor.description is None:
                raise QueryError("query must return rows")
            columns = tuple(str(item[0]) for item in cursor.description)
            raw_rows = cursor.fetchmany(limit + 1)
    except sqlite3.Error as exc:
        if work_limit_reached:
            raise QueryError(
                f"query exceeded the work limit of {MAX_QUERY_VM_STEPS} SQLite steps"
            ) from exc
        raise QueryError(f"query failed: {exc}") from exc
    truncated = len(raw_rows) > limit
    rows = tuple(tuple(_value(value) for value in row) for row in raw_rows[:limit])
    return QueryResult(columns, rows, truncated)


def render_query(result: QueryResult, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(result.as_json_value(), allow_nan=False, indent=2, sort_keys=True)
    if output_format == "jsonl":
        return "\n".join(
            json.dumps(dict(zip(result.columns, row, strict=True)), allow_nan=False, sort_keys=True)
            for row in result.rows
        )
    widths = [len(column) for column in result.columns]
    rendered_rows = [[_display(value) for value in row] for row in result.rows]
    for row in rendered_rows:
        for index, value in enumerate(row):
            widths[index] = min(60, max(widths[index], len(value)))
    header = "  ".join(column.ljust(widths[index]) for index, column in enumerate(result.columns))
    divider = "  ".join("-" * width for width in widths)
    lines = [header, divider]
    for row in rendered_rows:
        lines.append(
            "  ".join(
                value[: widths[index]].ljust(widths[index]) for index, value in enumerate(row)
            )
        )
    if result.truncated:
        lines.append("… result truncated")
    return "\n".join(lines)


def _display(value: JsonValue) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    if value is None:
        return "NULL"
    return str(value)
