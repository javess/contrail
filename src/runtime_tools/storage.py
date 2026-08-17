"""Versioned SQLite storage for portable ``.runpack`` artifacts."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import TracebackType

from runtime_tools import __version__
from runtime_tools.model import CausalEdge, Entity, Event, Execution, JsonValue, Measurement

SCHEMA_VERSION = "1"
APPLICATION_ID = 0x4354524C  # CTRL
_REQUIRED_TABLES = {
    "manifest",
    "executions",
    "entities",
    "events",
    "causal_edges",
    "measurements",
}

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
    try:
        decoded = json.loads(value)
    except (json.JSONDecodeError, TypeError) as exc:
        raise RunpackError("invalid JSON object in runpack") from exc
    if not isinstance(decoded, dict):
        raise RunpackError("expected a JSON object in runpack")
    return decoded


def _validate_connection(connection: sqlite3.Connection) -> None:
    application_id = connection.execute("PRAGMA application_id").fetchone()
    if application_id is None or application_id[0] != APPLICATION_ID:
        raise RunpackError("file is not a Contrail runpack")
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table'"
        ).fetchall()
    }
    missing = sorted(_REQUIRED_TABLES - tables)
    if missing:
        raise RunpackError(f"runpack is missing required tables: {', '.join(missing)}")
    row = connection.execute(
        "SELECT value FROM manifest WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        raise RunpackError("runpack has no schema version")
    if row[0] != SCHEMA_VERSION:
        raise UnsupportedSchemaError(
            f"unsupported runpack schema {row[0]!r}; supported: {SCHEMA_VERSION}"
        )


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

    @classmethod
    def open_existing(cls, path: Path) -> RunpackWriter:
        """Open a validated runpack for adapter enrichment."""
        if not path.is_file():
            raise RunpackError(f"runpack does not exist: {path}")
        writer = cls.__new__(cls)
        writer.path = path
        try:
            writer._connection = sqlite3.connect(path)
            writer._connection.execute("PRAGMA foreign_keys = ON")
            _validate_connection(writer._connection)
        except sqlite3.DatabaseError as exc:
            if hasattr(writer, "_connection"):
                writer._connection.close()
            raise RunpackError(f"invalid runpack: {path}") from exc
        except RunpackError:
            if hasattr(writer, "_connection"):
                writer._connection.close()
            raise
        return writer

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

    def add_causal_edge(self, edge: CausalEdge) -> None:
        self._connection.execute(
            """
            INSERT INTO causal_edges(
                source_event_id, target_event_id, kind, confidence, attributes_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                edge.source_event_id,
                edge.target_event_id,
                edge.kind,
                edge.confidence,
                _json(edge.attributes),
            ),
        )
        self._connection.commit()

    def add_measurement(self, measurement: Measurement) -> None:
        self._connection.execute(
            """
            INSERT INTO measurements(
                name, value, unit, timestamp_ns, entity_id, attributes_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                measurement.name,
                measurement.value,
                measurement.unit,
                measurement.timestamp_ns,
                measurement.entity_id,
                _json(measurement.attributes),
            ),
        )
        self._connection.commit()

    def expand_execution_bounds(self, started_at_ns: int, finished_at_ns: int | None) -> None:
        self._connection.execute(
            """
            UPDATE executions
            SET started_at_ns = min(started_at_ns, ?),
                finished_at_ns = CASE
                    WHEN ? IS NULL THEN finished_at_ns
                    WHEN finished_at_ns IS NULL THEN ?
                    ELSE max(finished_at_ns, ?)
                END
            """,
            (started_at_ns, finished_at_ns, finished_at_ns, finished_at_ns),
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
        connection: sqlite3.Connection | None = None
        try:
            uri = f"{path.resolve().as_uri()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            self._connection = connection
            self._connection.row_factory = sqlite3.Row
            _validate_connection(self._connection)
        except sqlite3.DatabaseError as exc:
            if connection is not None:
                connection.close()
            raise RunpackError(f"invalid runpack: {path}") from exc
        except RunpackError:
            if connection is not None:
                connection.close()
            raise

    def _execute(self, sql: str) -> sqlite3.Cursor:
        try:
            return self._connection.execute(sql)
        except sqlite3.DatabaseError as exc:
            raise RunpackError(f"could not read runpack: {exc}") from exc

    def execution(self) -> Execution:
        rows = self._execute("SELECT * FROM executions").fetchall()
        if len(rows) != 1:
            raise RunpackError(f"expected exactly one execution, found {len(rows)}")
        row = rows[0]
        try:
            command = json.loads(row["command_json"])
        except (json.JSONDecodeError, TypeError) as exc:
            raise RunpackError("execution command is invalid JSON") from exc
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
        rows = self._execute(
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

    def entities(self) -> tuple[Entity, ...]:
        rows = self._execute("SELECT * FROM entities ORDER BY id").fetchall()
        return tuple(
            Entity(
                id=row["id"],
                kind=row["kind"],
                name=row["name"],
                parent_entity_id=row["parent_entity_id"],
                attributes=_object(row["attributes_json"]),
            )
            for row in rows
        )

    def events(self) -> tuple[Event, ...]:
        rows = self._execute(
            "SELECT * FROM events ORDER BY started_at_ns, sequence, id"
        ).fetchall()
        return tuple(
            Event(
                id=row["id"],
                kind=row["kind"],
                name=row["name"],
                entity_id=row["entity_id"],
                started_at_ns=row["started_at_ns"],
                finished_at_ns=row["finished_at_ns"],
                clock_domain=row["clock_domain"],
                uncertainty_ns=row["uncertainty_ns"],
                sequence=row["sequence"],
                attributes=_object(row["attributes_json"]),
            )
            for row in rows
        )

    def causal_edges(self) -> tuple[CausalEdge, ...]:
        rows = self._execute(
            """
            SELECT source_event_id, target_event_id, kind, confidence, attributes_json
            FROM causal_edges
            ORDER BY source_event_id, target_event_id, kind
            """
        ).fetchall()
        return tuple(
            CausalEdge(
                source_event_id=row["source_event_id"],
                target_event_id=row["target_event_id"],
                kind=row["kind"],
                confidence=row["confidence"],
                attributes=_object(row["attributes_json"]),
            )
            for row in rows
        )

    def clock_inconsistency_count(self) -> int:
        row = self._execute(
            """
            SELECT count(*)
            FROM causal_edges AS edge
            JOIN events AS parent ON parent.id = edge.source_event_id
            JOIN events AS child ON child.id = edge.target_event_id
            WHERE edge.kind = 'parent'
              AND (
                (parent.started_at_ns IS NOT NULL AND child.started_at_ns IS NOT NULL
                 AND child.started_at_ns < parent.started_at_ns)
                OR
                (parent.finished_at_ns IS NOT NULL AND child.finished_at_ns IS NOT NULL
                 AND child.finished_at_ns > parent.finished_at_ns)
              )
            """
        ).fetchone()
        return int(row[0])

    def operation_counts(self) -> dict[tuple[str, str, str, str], int]:
        rows = self._execute(
            """
            SELECT
                COALESCE(entity.kind, 'unowned') AS entity_kind,
                COALESCE(entity.name, 'unowned') AS entity_name,
                event.kind AS event_kind,
                event.name AS event_name,
                count(*) AS event_count
            FROM events AS event
            LEFT JOIN entities AS entity ON entity.id = event.entity_id
            GROUP BY entity_kind, entity_name, event_kind, event_name
            """
        ).fetchall()
        return {
            (row["entity_kind"], row["entity_name"], row["event_kind"], row["event_name"]): int(
                row["event_count"]
            )
            for row in rows
        }

    def edge_counts(self) -> dict[tuple[str, str, str, str, str], int]:
        rows = self._execute(
            """
            SELECT
                COALESCE(source_entity.kind, 'unowned') AS source_kind,
                COALESCE(source_entity.name, 'unowned') AS source_name,
                COALESCE(target_entity.kind, 'unowned') AS target_kind,
                COALESCE(target_entity.name, 'unowned') AS target_name,
                edge.kind AS edge_kind,
                count(*) AS edge_count
            FROM causal_edges AS edge
            JOIN events AS source_event ON source_event.id = edge.source_event_id
            JOIN events AS target_event ON target_event.id = edge.target_event_id
            LEFT JOIN entities AS source_entity ON source_entity.id = source_event.entity_id
            LEFT JOIN entities AS target_entity ON target_entity.id = target_event.entity_id
            GROUP BY source_kind, source_name, target_kind, target_name, edge_kind
            """
        ).fetchall()
        return {
            (
                row["source_kind"],
                row["source_name"],
                row["target_kind"],
                row["target_name"],
                row["edge_kind"],
            ): int(row["edge_count"])
            for row in rows
        }

    def counts(self) -> dict[str, int]:
        return {
            table: self._execute(f"SELECT count(*) FROM {table}").fetchone()[0]
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
