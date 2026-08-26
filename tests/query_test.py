from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import pytest

from runtime_tools import query, record_process
from runtime_tools.model import Attachment, Execution
from runtime_tools.query import (
    MAX_QUERY_CELL_BYTES,
    MAX_QUERY_VM_STEPS,
    QueryError,
    QueryResult,
    query_runpack,
    render_query,
)
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter


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


def test_runpack_query_requires_exactly_one_execution(tmp_path: Path) -> None:
    runpack = tmp_path / "empty.runpack"
    with RunpackWriter(runpack):
        pass

    with pytest.raises(RunpackError, match="expected exactly one execution, found 0"):
        query_runpack(runpack, "SELECT name FROM events")


def test_runpack_query_uses_the_same_resolved_artifact_it_validates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validated = tmp_path / "validated.runpack"
    replacement = tmp_path / "replacement.runpack"
    requested = tmp_path / "requested.runpack"
    record_process((sys.executable, "-c", "pass"), validated, name="validated")
    record_process((sys.executable, "-c", "pass"), replacement, name="replacement")
    real_resolve = Path.resolve
    resolutions = iter((validated, replacement))

    def retarget_between_resolutions(path: Path, strict: bool = False) -> Path:
        if path == requested:
            return next(resolutions)
        return real_resolve(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", retarget_between_resolutions)

    result = query_runpack(requested, "SELECT name FROM executions")

    assert result.rows == (("validated",),)


def test_runpack_query_uses_the_same_open_artifact_it_validates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requested = tmp_path / "requested.runpack"
    replacement = tmp_path / "replacement.sqlite"
    record_process((sys.executable, "-c", "pass"), requested, name="validated")
    with sqlite3.connect(replacement) as connection:
        connection.execute("CREATE TABLE executions(name TEXT)")
        connection.execute("INSERT INTO executions VALUES ('unvalidated')")
    execute = RunpackReader.execution

    def replace_after_validation(reader: RunpackReader) -> Execution:
        execution = execute(reader)
        os.replace(replacement, reader.path)
        return execution

    monkeypatch.setattr(RunpackReader, "execution", replace_after_validation)

    result = query_runpack(requested, "SELECT name FROM executions")

    assert result.rows == (("validated",),)


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


def test_query_jsonl_preserves_result_column_order() -> None:
    result = QueryResult(("zeta", "alpha"), ((1, 2),), False)

    assert render_query(result, "jsonl") == '{"zeta": 1, "alpha": 2}'


def test_runpack_query_escapes_terminal_control_characters(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    result = query_runpack(runpack, 'SELECT char(27) || "[31m" AS "line\nname"')
    table = render_query(result, "table")

    assert "\x1b" not in table
    assert r"line\nname" in table
    assert r"\x1b[31m" in table


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

    with pytest.raises(QueryError, match="query limit must be an integer"):
        query_runpack(runpack, "SELECT name FROM events", limit=True)


def test_runpack_query_validates_sql_text_before_loading_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing.runpack"
    with pytest.raises(QueryError, match="SQL query must be a string"):
        query_runpack(missing, cast(str, True))
    with pytest.raises(QueryError, match="SQL query must be valid UTF-8"):
        query_runpack(missing, "SELECT '\udcff'")

    monkeypatch.setattr(query, "MAX_QUERY_SQL_BYTES", 8)
    with pytest.raises(QueryError, match="SQL query exceeds the 8-byte input limit"):
        query_runpack(missing, "SELECT 12")


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


def test_runpack_query_charges_work_budget_only_to_caller_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runpack = tmp_path / "large.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="large")
    with sqlite3.connect(runpack) as connection:
        connection.executemany(
            "INSERT INTO measurements(name, value, unit, attributes_json) VALUES (?, ?, ?, ?)",
            (("sample", value, "count", "{}") for value in range(5_000)),
        )
    monkeypatch.setattr(query, "MAX_QUERY_VM_STEPS", 1_000)

    assert query_runpack(runpack, "SELECT 1").rows == ((1,),)
    with pytest.raises(
        QueryError,
        match="query exceeded the work limit of 1000 SQLite steps",
    ):
        query_runpack(
            runpack,
            """
            WITH RECURSIVE counter(value) AS (
                VALUES (0)
                UNION ALL
                SELECT value + 1 FROM counter WHERE value < 100000
            )
            SELECT sum(value) FROM counter
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


def test_runpack_query_denies_extension_loading_even_if_sqlite_enables_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")
    connect = sqlite3.connect

    def extension_enabled_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = cast(sqlite3.Connection, connect(*args, **kwargs))
        connection.enable_load_extension(True)
        return connection

    monkeypatch.setattr(sqlite3, "connect", extension_enabled_connect)

    with pytest.raises(QueryError, match="not authorized"):
        query_runpack(
            runpack,
            "SELECT load_extension('/private/tmp/contrail-missing-extension')",
        )


@pytest.mark.parametrize(
    "statement",
    (
        "SELECT '0123456789abcdefg' AS value",
        "SELECT '' AS value",
        "SELECT 1 AS long_column WHERE 0",
    ),
    ids=("value-bytes", "json-structure", "column-metadata"),
)
def test_runpack_query_bounds_aggregate_result_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    statement: str,
) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")
    monkeypatch.setattr(query, "MAX_QUERY_RESULT_BYTES", 16)

    with pytest.raises(QueryError, match="query result exceeded the byte limit of 16"):
        query_runpack(runpack, statement)


def test_runpack_query_bounds_individual_values(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")

    with pytest.raises(QueryError, match="string or blob too big"):
        query_runpack(runpack, f"SELECT zeroblob({MAX_QUERY_CELL_BYTES + 1})")


def test_runpack_query_validates_large_attachments_before_applying_its_cell_limit(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "large-attachment.runpack"
    content = b"x" * (MAX_QUERY_CELL_BYTES + 1024 * 1024)
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_attachments(
            (Attachment("large", "raw", "large", "application/octet-stream", content, {}),)
        )

    assert query_runpack(runpack, "SELECT 1").rows == ((1,),)
    with pytest.raises(QueryError, match="string or blob too big"):
        query_runpack(runpack, "SELECT content FROM attachments")


def test_query_renderers_bound_format_expansion(monkeypatch: pytest.MonkeyPatch) -> None:
    result = QueryResult(("long-column-name",), (("",),) * 20, False)
    monkeypatch.setattr(query, "MAX_QUERY_RESULT_BYTES", 200)

    with pytest.raises(QueryError, match="rendered query output exceeded the byte limit of 200"):
        render_query(result, "jsonl")
    with pytest.raises(QueryError, match="rendered query output exceeded the byte limit of 200"):
        render_query(result, "table")


def test_query_table_marks_truncated_column_labels_and_values(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")
    long_column = "column-" + ("x" * 60)
    omitted_suffix_values = (f"{'x' * 60}A", f"{'x' * 60}B")
    shared_prefix = "x" * 58
    retained_difference_values = (f"{shared_prefix}A-tail", f"{shared_prefix}B-tail")

    result = query_runpack(
        runpack,
        f'''SELECT '{omitted_suffix_values[0]}' AS "{long_column}"
            UNION ALL SELECT '{omitted_suffix_values[1]}'
            UNION ALL SELECT '{retained_difference_values[0]}'
            UNION ALL SELECT '{retained_difference_values[1]}' ''',
    )
    lines = render_query(result, "table").splitlines()

    assert lines[0].endswith("…")
    assert lines[2].endswith("…")
    assert lines[3].endswith("…")
    assert lines[2] == lines[3]
    assert lines[4].endswith("…")
    assert lines[5].endswith("…")
    assert lines[4] != lines[5]
    assert max(map(len, lines)) == 60
