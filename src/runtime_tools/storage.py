"""Versioned SQLite storage for portable ``.runpack`` artifacts."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import TracebackType

from runtime_tools import __version__
from runtime_tools.model import Entity, Event, Execution, JsonValue, Measurement

SCHEMA_VERSION = "1"
APPLICATION_ID = 0x4354524C  # CTRL

_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE manifest (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE executions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    started_at_ns INTEGER NOT NULL,
    finished_at_ns INTEGER,
    command_json TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    exit_code INTEGER,
    revision TEXT,
    metadata_json TEXT NOT NULL
);

CREATE TABLE entities (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    parent_entity_id TEXT REFERENCES entities(id),
    attributes_json TEXT NOT NULL
);

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    entity_id TEXT REFERENCES entities(id),
    started_at_ns INTEGER,
    finished_at_ns INTEGER,
    clock_domain TEXT,
    uncertainty_ns INTEGER,
    sequence INTEGER,
    attributes_json TEXT NOT NULL,
    CHECK (finished_at_ns IS NULL OR started_at_ns IS NULL OR finished_at_ns >= started_at_ns),
    CHECK (uncertainty_ns IS NULL OR uncertainty_ns >= 0)
);

CREATE TABLE causal_edges (
    source_event_id TEXT NOT NULL REFERENCES events(id),
    target_event_id TEXT NOT NULL REFERENCES events(id),
    kind TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 1.0,
    attributes_json TEXT NOT NULL,
    PRIMARY KEY (source_event_id, target_event_id, kind),
    CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

CREATE TABLE measurements (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    timestamp_ns INTEGER,
    entity_id TEXT REFERENCES entities(id),
    attributes_json TEXT NOT NULL
);

CREATE INDEX events_time_idx ON events(started_at_ns, finished_at_ns);
CREATE INDEX events_entity_idx ON events(entity_id);
CREATE INDEX events_semantic_idx ON events(kind, name);
CREATE INDEX edges_target_idx ON causal_edges(target_event_id);
CREATE INDEX measurements_name_time_idx ON measurements(name, timestamp_ns);
CREATE INDEX measurements_entity_idx ON measurements(entity_id);
"""


class RunpackError(ValueError):
    """Raised when a runpack cannot be read safely."""


class UnsupportedSchemaError(RunpackError):
    """Raised when a runpack uses an unsupported schema major version."""


def _json(value: JsonValue | tuple[str, ...]) -> str:
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _object(value: str) -> dict[str, JsonValue]:
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise RunpackError("expected a JSON object in runpack")
    return decoded


class RunpackWriter:
    """Incrementally writes a new runpack."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._connection = sqlite3.connect(path)
        try:
            self._connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            self._connection.execute("PRAGMA journal_mode = DELETE")
            self._connection.executescript(_SCHEMA)
            self._connection.executemany(
                "INSERT INTO manifest(key, value) VALUES (?, ?)",
                (("schema_version", SCHEMA_VERSION), ("producer_version", __version__)),
            )
            self._connection.commit()
        except BaseException:
            self._connection.close()
            raise

    def add_execution(self, execution: Execution) -> None:
        self._connection.execute(
            """
            INSERT INTO executions(
                id, name, started_at_ns, finished_at_ns, command_json,
                working_directory, exit_code, revision, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                execution.id,
                execution.name,
                execution.started_at_ns,
                execution.finished_at_ns,
                _json(execution.command),
                execution.working_directory,
                execution.exit_code,
                execution.revision,
                _json(execution.metadata),
            ),
        )
        self._connection.commit()

    def add_entity(self, entity: Entity) -> None:
        self._connection.execute(
            """
            INSERT INTO entities(id, kind, name, parent_entity_id, attributes_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                entity.id,
                entity.kind,
                entity.name,
                entity.parent_entity_id,
                _json(entity.attributes),
            ),
        )
        self._connection.commit()

    def add_event(self, event: Event) -> None:
        self._connection.execute(
            """
            INSERT INTO events(
                id, kind, name, entity_id, started_at_ns, finished_at_ns,
                clock_domain, uncertainty_ns, sequence, attributes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.id,
                event.kind,
                event.name,
                event.entity_id,
                event.started_at_ns,
                event.finished_at_ns,
                event.clock_domain,
                event.uncertainty_ns,
                event.sequence,
                _json(event.attributes),
            ),
        )
        self._connection.commit()

    def finish_execution(
        self,
        execution_id: str,
        *,
        finished_at_ns: int,
        exit_code: int,
        metadata: dict[str, JsonValue],
        event: Event,
        measurements: tuple[Measurement, ...],
    ) -> None:
        with self._connection:
            self._connection.execute(
                """
                UPDATE executions
                SET finished_at_ns = ?, exit_code = ?, metadata_json = ?
                WHERE id = ?
                """,
                (finished_at_ns, exit_code, _json(metadata), execution_id),
            )
            self._connection.execute(
                """
                INSERT INTO events(
                    id, kind, name, entity_id, started_at_ns, finished_at_ns,
                    clock_domain, uncertainty_ns, sequence, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.id,
                    event.kind,
                    event.name,
                    event.entity_id,
                    event.started_at_ns,
                    event.finished_at_ns,
                    event.clock_domain,
                    event.uncertainty_ns,
                    event.sequence,
                    _json(event.attributes),
                ),
            )
            self._connection.executemany(
                """
                INSERT INTO measurements(
                    name, value, unit, timestamp_ns, entity_id, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        item.name,
                        item.value,
                        item.unit,
                        item.timestamp_ns,
                        item.entity_id,
                        _json(item.attributes),
                    )
                    for item in measurements
                ),
            )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> RunpackWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class RunpackReader:
    """Reads and validates one supported runpack."""

    def __init__(self, path: Path) -> None:
        if not path.is_file():
            raise RunpackError(f"runpack does not exist: {path}")
        try:
            self._connection = sqlite3.connect(path)
            self._connection.row_factory = sqlite3.Row
            self._validate()
        except sqlite3.DatabaseError as exc:
            self._connection.close()
            raise RunpackError(f"invalid runpack: {path}") from exc
        except RunpackError:
            self._connection.close()
            raise

    def _validate(self) -> None:
        row = self._connection.execute(
            "SELECT value FROM manifest WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            raise RunpackError("runpack has no schema version")
        if row["value"] != SCHEMA_VERSION:
            raise UnsupportedSchemaError(
                f"unsupported runpack schema {row['value']!r}; supported: {SCHEMA_VERSION}"
            )

    def execution(self) -> Execution:
        rows = self._connection.execute("SELECT * FROM executions").fetchall()
        if len(rows) != 1:
            raise RunpackError(f"expected exactly one execution, found {len(rows)}")
        row = rows[0]
        command = json.loads(row["command_json"])
        if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
            raise RunpackError("execution command is invalid")
        return Execution(
            id=row["id"],
            name=row["name"],
            started_at_ns=row["started_at_ns"],
            finished_at_ns=row["finished_at_ns"],
            command=tuple(command),
            working_directory=row["working_directory"],
            exit_code=row["exit_code"],
            revision=row["revision"],
            metadata=_object(row["metadata_json"]),
        )

    def measurements(self) -> tuple[Measurement, ...]:
        rows = self._connection.execute(
            "SELECT name, value, unit, timestamp_ns, entity_id, attributes_json "
            "FROM measurements ORDER BY id"
        ).fetchall()
        return tuple(
            Measurement(
                name=row["name"],
                value=row["value"],
                unit=row["unit"],
                timestamp_ns=row["timestamp_ns"],
                entity_id=row["entity_id"],
                attributes=_object(row["attributes_json"]),
            )
            for row in rows
        )

    def counts(self) -> dict[str, int]:
        return {
            table: self._connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("entities", "events", "causal_edges", "measurements")
        }

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> RunpackReader:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
