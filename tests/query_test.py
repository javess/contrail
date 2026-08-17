from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from runtime_tools import record_process
from runtime_tools.query import QueryError, query_runpack, render_query


def test_runpack_query_returns_bounded_structured_rows(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    result = query_runpack(
        runpack,
        "SELECT kind, name FROM events ORDER BY name",
        limit=1,
    )

    assert result.columns == ("kind", "name")
    assert result.rows == (("process.run", Path(sys.executable).name),)
    assert result.truncated is False
    payload = json.loads(render_query(result, "json"))
    assert payload["rows"] == [["process.run", Path(sys.executable).name]]
    assert "kind" in render_query(result, "table")


def test_runpack_query_truncates_without_loading_the_full_result(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    result = query_runpack(
        runpack,
        "SELECT name FROM measurements ORDER BY id",
        limit=2,
    )

    assert len(result.rows) == 2
    assert result.truncated is True


def test_runpack_query_cannot_mutate_the_artifact(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    with pytest.raises(QueryError, match="not authorized"):
        query_runpack(runpack, "DELETE FROM events")

    with sqlite3.connect(runpack) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_runpack_query_cannot_attach_or_create_another_database(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    attached = tmp_path / "side-effect.sqlite"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    with pytest.raises(QueryError):
        query_runpack(runpack, f"ATTACH DATABASE '{attached}' AS side_effect")

    assert not attached.exists()
