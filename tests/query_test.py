from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from runtime_tools import query, record_process
from runtime_tools.query import (
    MAX_QUERY_CELL_BYTES,
    MAX_QUERY_VM_STEPS,
    QueryError,
    query_runpack,
    render_query,
)


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


def test_runpack_query_preserves_duplicate_columns_in_jsonl(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    result = query_runpack(runpack, "SELECT name, name, kind AS name FROM events")

    assert result.columns == ("name", "name_2", "name_3")
    assert json.loads(render_query(result, "jsonl")) == {
        "name": Path(sys.executable).name,
        "name_2": Path(sys.executable).name,
        "name_3": "process.run",
    }


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


def test_runpack_query_rejects_excessive_output_limits(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    with pytest.raises(QueryError, match="query limit cannot exceed 100000"):
        query_runpack(runpack, "SELECT name FROM events", limit=100_001)


def test_runpack_query_stops_read_only_work_that_exceeds_its_budget(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    with pytest.raises(
        QueryError,
        match=f"query exceeded the work limit of {MAX_QUERY_VM_STEPS} SQLite steps",
    ):
        query_runpack(
            runpack,
            """
            WITH RECURSIVE counter(value) AS (
                VALUES (1)
                UNION ALL
                SELECT value + 1 FROM counter WHERE value < 1000000000
            )
            SELECT max(value) FROM counter
            """,
        )


def test_runpack_query_encodes_non_finite_sql_results_as_json_safe_values(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    result = query_runpack(runpack, "SELECT 1e999 AS value")

    assert result.rows == (({"encoding": "non-finite-float", "value": "infinity"},),)
    assert json.loads(render_query(result, "json"))["rows"] == [
        [{"encoding": "non-finite-float", "value": "infinity"}]
    ]
    assert json.loads(render_query(result, "jsonl")) == {
        "value": {"encoding": "non-finite-float", "value": "infinity"}
    }


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


def test_runpack_query_bounds_aggregate_result_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")
    monkeypatch.setattr(query, "MAX_QUERY_RESULT_BYTES", 16)

    with pytest.raises(QueryError, match="query result exceeded the byte limit of 16"):
        query_runpack(runpack, "SELECT '0123456789abcdefg' AS value")


def test_runpack_query_bounds_individual_values(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    with pytest.raises(QueryError, match="string or blob too big"):
        query_runpack(runpack, f"SELECT zeroblob({MAX_QUERY_CELL_BYTES + 1})")
