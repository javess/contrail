"""Versioned SQLite storage for portable ``.runpack`` artifacts."""

from __future__ import annotations

import json
import math
import os
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType

from runtime_tools import __version__
from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    JsonValue,
    Measurement,
)

SCHEMA_VERSION = "1.1"
SCHEMA_MAJOR_VERSION = "1"
APPLICATION_ID = 0x4354524C  # CTRL
_MIN_INTEGER = -(1 << 63)
_MAX_INTEGER = (1 << 63) - 1
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

CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    content BLOB NOT NULL,
    attributes_json TEXT NOT NULL
);

CREATE INDEX events_time_idx ON events(started_at_ns, finished_at_ns);
CREATE INDEX events_entity_idx ON events(entity_id);
CREATE INDEX events_semantic_idx ON events(kind, name);
CREATE INDEX edges_target_idx ON causal_edges(target_event_id);
CREATE INDEX measurements_name_time_idx ON measurements(name, timestamp_ns);
CREATE INDEX measurements_entity_idx ON measurements(entity_id);
CREATE INDEX attachments_kind_name_idx ON attachments(kind, name);
"""


class RunpackError(ValueError):
    """Raised when a runpack cannot be read or written safely."""


class UnsupportedSchemaError(RunpackError):
    """Raised when a runpack uses an unsupported schema major version."""


def _json(value: JsonValue) -> str:
    try:
        normalized = _checked_json(value)
        return json.dumps(normalized, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except RunpackError as exc:
        raise RunpackError("invalid JSON value for runpack") from exc
    except (OverflowError, TypeError, ValueError, RecursionError) as exc:
        raise RunpackError("invalid JSON value for runpack") from exc


def _command_json(value: object) -> str:
    if not isinstance(value, tuple) or not all(isinstance(item, str) for item in value):
        raise RunpackError("execution command must be a tuple of strings")
    return _json(list(value))


def _object(value: str) -> dict[str, JsonValue]:
    try:
        decoded = _checked_json(json.loads(value))
    except (json.JSONDecodeError, TypeError) as exc:
        raise RunpackError("invalid JSON object in runpack") from exc
    if not isinstance(decoded, dict):
        raise RunpackError("expected a JSON object in runpack")
    return decoded


def _checked_json(value: object) -> JsonValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RunpackError("non-finite number in runpack JSON")
        return value
    if isinstance(value, list):
        return [_checked_json(item) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: _checked_json(item) for key, item in value.items()}
    raise RunpackError("invalid value in runpack JSON")


def _blob(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    raise RunpackError("invalid binary attachment content in runpack")


def _measurement_value(value: object) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise RunpackError("measurement value must be finite")
    return float(value)


def _integer_value(value: object, label: str, *, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        suffix = " or null" if optional else ""
        raise RunpackError(f"{label} must be an integer{suffix}")
    if not _MIN_INTEGER <= value <= _MAX_INTEGER:
        raise RunpackError(f"{label} exceeds the runpack integer range")
    return value


def _text_value(value: object, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        suffix = " or null" if optional else ""
        raise RunpackError(f"{label} must be a non-empty string{suffix}")
    return value


def _required_text(value: object, label: str) -> str:
    result = _text_value(value, label)
    assert result is not None
    return result


def _execution_interval(started_at_ns: object, finished_at_ns: object) -> tuple[int, int | None]:
    started = _integer_value(started_at_ns, "execution start timestamp")
    finished = _integer_value(finished_at_ns, "execution finish timestamp", optional=True)
    assert started is not None
    if finished is not None and finished < started:
        raise RunpackError("execution cannot finish before it starts")
    return started, finished


def _event_values(event: Event) -> tuple[object, ...]:
    started = _integer_value(event.started_at_ns, "event start timestamp", optional=True)
    finished = _integer_value(event.finished_at_ns, "event finish timestamp", optional=True)
    if started is not None and finished is not None and finished < started:
        raise RunpackError("event cannot finish before it starts")
    uncertainty = _integer_value(event.uncertainty_ns, "event uncertainty", optional=True)
    if uncertainty is not None and uncertainty < 0:
        raise RunpackError("event uncertainty cannot be negative")
    sequence = _integer_value(event.sequence, "event sequence", optional=True)
    return (
        _text_value(event.id, "event id"),
        _text_value(event.kind, "event kind"),
        _text_value(event.name, "event name"),
        _text_value(event.entity_id, "event entity id", optional=True),
        started,
        finished,
        _text_value(event.clock_domain, "event clock domain", optional=True),
        uncertainty,
        sequence,
        _json(event.attributes),
    )


def _edge_values(edge: CausalEdge) -> tuple[object, ...]:
    return (
        _text_value(edge.source_event_id, "causal edge source event id"),
        _text_value(edge.target_event_id, "causal edge target event id"),
        _text_value(edge.kind, "causal edge kind"),
        _confidence_value(edge.confidence),
        _json(edge.attributes),
    )


def _confidence_value(value: object) -> float:
    confidence = value
    if (
        not isinstance(confidence, (int, float))
        or isinstance(confidence, bool)
        or not math.isfinite(confidence)
        or not 0 <= confidence <= 1
    ):
        raise RunpackError("causal edge confidence must be between 0 and 1")
    return float(confidence)


def _measurement_values(measurement: Measurement) -> tuple[object, ...]:
    return (
        _text_value(measurement.name, "measurement name"),
        _measurement_value(measurement.value),
        _text_value(measurement.unit, "measurement unit"),
        _integer_value(measurement.timestamp_ns, "measurement timestamp", optional=True),
        _text_value(measurement.entity_id, "measurement entity id", optional=True),
        _json(measurement.attributes),
    )


def _attachment_values(attachment: Attachment) -> tuple[object, ...]:
    return (
        _text_value(attachment.id, "attachment id"),
        _text_value(attachment.kind, "attachment kind"),
        _text_value(attachment.name, "attachment name"),
        _text_value(attachment.media_type, "attachment media type"),
        _blob(attachment.content),
        _json(attachment.attributes),
    )


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
    row = connection.execute("SELECT value FROM manifest WHERE key = 'schema_version'").fetchone()
    if row is None:
        raise RunpackError("runpack has no schema version")
    version = str(row[0])
    version_parts = version.split(".")
    if not version_parts or any(not part.isdigit() for part in version_parts):
        raise RunpackError(f"runpack schema version is invalid: {version!r}")
    if version_parts[0] != SCHEMA_MAJOR_VERSION:
        raise UnsupportedSchemaError(
            f"unsupported runpack schema {version!r}; supported major: {SCHEMA_MAJOR_VERSION}"
        )
    relationship_error = connection.execute("PRAGMA foreign_key_check").fetchone()
    if relationship_error is not None:
        table, row_id = relationship_error[:2]
        raise RunpackError(f"runpack contains an invalid relationship in {table} row {row_id}")


class RunpackWriter:
    """Incrementally writes a new runpack."""

    def __init__(self, path: Path) -> None:
        self.path = path
        connection: sqlite3.Connection | None = None
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        except FileExistsError as exc:
            raise RunpackError(f"refusing to overwrite existing runpack: {path}") from exc
        except OSError as exc:
            path.unlink(missing_ok=True)
            raise RunpackError(f"could not create runpack {path}: {exc}") from exc
        try:
            connection = sqlite3.connect(path)
            self._connection = connection
            self._connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            self._connection.execute("PRAGMA journal_mode = DELETE")
            self._connection.executescript(_SCHEMA)
            self._connection.executemany(
                "INSERT INTO manifest(key, value) VALUES (?, ?)",
                (("schema_version", SCHEMA_VERSION), ("producer_version", __version__)),
            )
            self._connection.commit()
        except sqlite3.DatabaseError as exc:
            if connection is not None:
                connection.close()
            path.unlink(missing_ok=True)
            raise RunpackError(f"could not create runpack {path}: {exc}") from exc
        except BaseException:
            if connection is not None:
                connection.close()
            path.unlink(missing_ok=True)
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

    @contextmanager
    def _writing(self) -> Iterator[None]:
        try:
            yield
        except (OverflowError, sqlite3.DatabaseError) as exc:
            raise RunpackError(f"could not write runpack {self.path}: {exc}") from exc

    def add_execution(self, execution: Execution) -> None:
        started_at_ns, finished_at_ns = _execution_interval(
            execution.started_at_ns, execution.finished_at_ns
        )
        exit_code = _integer_value(execution.exit_code, "execution exit code", optional=True)
        execution_id = _text_value(execution.id, "execution id")
        execution_name = _text_value(execution.name, "execution name")
        working_directory = _text_value(execution.working_directory, "execution working directory")
        revision = _text_value(execution.revision, "execution revision", optional=True)
        with self._writing():
            existing = self._connection.execute("SELECT count(*) FROM executions").fetchone()[0]
            if existing:
                raise RunpackError("runpack already contains an execution")
            self._connection.execute(
                """
                INSERT INTO executions(
                    id, name, started_at_ns, finished_at_ns, command_json,
                    working_directory, exit_code, revision, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution_id,
                    execution_name,
                    started_at_ns,
                    finished_at_ns,
                    _command_json(execution.command),
                    working_directory,
                    exit_code,
                    revision,
                    _json(execution.metadata),
                ),
            )
            self._connection.commit()

    def add_entity(self, entity: Entity) -> None:
        with self._writing():
            self._connection.execute(
                """
            INSERT INTO entities(id, kind, name, parent_entity_id, attributes_json)
            VALUES (?, ?, ?, ?, ?)
            """,
                (
                    _text_value(entity.id, "entity id"),
                    _text_value(entity.kind, "entity kind"),
                    _text_value(entity.name, "entity name"),
                    _text_value(entity.parent_entity_id, "parent entity id", optional=True),
                    _json(entity.attributes),
                ),
            )
            self._connection.commit()

    def add_entities(self, entities: Iterable[Entity]) -> None:
        with self._writing(), self._connection:
            self._connection.executemany(
                """
                INSERT INTO entities(id, kind, name, parent_entity_id, attributes_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    (
                        _text_value(entity.id, "entity id"),
                        _text_value(entity.kind, "entity kind"),
                        _text_value(entity.name, "entity name"),
                        _text_value(entity.parent_entity_id, "parent entity id", optional=True),
                        _json(entity.attributes),
                    )
                    for entity in entities
                ),
            )

    def add_event(self, event: Event) -> None:
        with self._writing():
            self._connection.execute(
                """
            INSERT INTO events(
                id, kind, name, entity_id, started_at_ns, finished_at_ns,
                clock_domain, uncertainty_ns, sequence, attributes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                _event_values(event),
            )
            self._connection.commit()

    def add_events(self, events: Iterable[Event]) -> None:
        with self._writing(), self._connection:
            self._connection.executemany(
                """
                INSERT INTO events(
                    id, kind, name, entity_id, started_at_ns, finished_at_ns,
                    clock_domain, uncertainty_ns, sequence, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (_event_values(event) for event in events),
            )

    def add_causal_edge(self, edge: CausalEdge) -> None:
        with self._writing():
            self._connection.execute(
                """
            INSERT INTO causal_edges(
                source_event_id, target_event_id, kind, confidence, attributes_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
                _edge_values(edge),
            )
            self._connection.commit()

    def add_causal_edges(self, edges: Iterable[CausalEdge]) -> None:
        with self._writing(), self._connection:
            self._connection.executemany(
                """
                INSERT INTO causal_edges(
                    source_event_id, target_event_id, kind, confidence, attributes_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (_edge_values(edge) for edge in edges),
            )

    def add_measurement(self, measurement: Measurement) -> None:
        with self._writing():
            self._connection.execute(
                """
            INSERT INTO measurements(
                name, value, unit, timestamp_ns, entity_id, attributes_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                _measurement_values(measurement),
            )
            self._connection.commit()

    def add_measurements(self, measurements: Iterable[Measurement]) -> None:
        with self._writing(), self._connection:
            self._connection.executemany(
                """
                INSERT INTO measurements(
                    name, value, unit, timestamp_ns, entity_id, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (_measurement_values(measurement) for measurement in measurements),
            )

    def add_attachments(self, attachments: Iterable[Attachment]) -> None:
        with self._writing(), self._connection:
            self._connection.executemany(
                """
                INSERT INTO attachments(id, kind, name, media_type, content, attributes_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (_attachment_values(attachment) for attachment in attachments),
            )

    def expand_execution_bounds(self, started_at_ns: int, finished_at_ns: int | None) -> None:
        normalized_start, normalized_finish = _execution_interval(started_at_ns, finished_at_ns)
        with self._writing():
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
                (
                    normalized_start,
                    normalized_finish,
                    normalized_finish,
                    normalized_finish,
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
        normalized_execution_id = _text_value(execution_id, "execution id")
        row = self._connection.execute(
            "SELECT started_at_ns FROM executions WHERE id = ?", (normalized_execution_id,)
        ).fetchone()
        if row is None:
            raise RunpackError(f"execution does not exist: {normalized_execution_id}")
        _, normalized_finish = _execution_interval(row[0], finished_at_ns)
        normalized_exit_code = _integer_value(exit_code, "execution exit code")
        with self._writing(), self._connection:
            self._connection.execute(
                """
                UPDATE executions
                SET finished_at_ns = ?, exit_code = ?, metadata_json = ?
                WHERE id = ?
                """,
                (normalized_finish, normalized_exit_code, _json(metadata), normalized_execution_id),
            )
            self._connection.execute(
                """
                INSERT INTO events(
                    id, kind, name, entity_id, started_at_ns, finished_at_ns,
                    clock_domain, uncertainty_ns, sequence, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _event_values(event),
            )
            self._connection.executemany(
                """
                INSERT INTO measurements(
                    name, value, unit, timestamp_ns, entity_id, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (_measurement_values(item) for item in measurements),
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
        started_at_ns, finished_at_ns = _execution_interval(
            row["started_at_ns"], row["finished_at_ns"]
        )
        exit_code = _integer_value(row["exit_code"], "execution exit code", optional=True)
        return Execution(
            id=_required_text(row["id"], "execution id"),
            name=_required_text(row["name"], "execution name"),
            started_at_ns=started_at_ns,
            finished_at_ns=finished_at_ns,
            command=tuple(command),
            working_directory=_required_text(
                row["working_directory"], "execution working directory"
            ),
            exit_code=exit_code,
            revision=_text_value(row["revision"], "execution revision", optional=True),
            metadata=_object(row["metadata_json"]),
        )

    def manifest(self) -> dict[str, str]:
        rows = self._execute("SELECT key, value FROM manifest ORDER BY key").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def measurements(self) -> tuple[Measurement, ...]:
        rows = self._execute(
            "SELECT name, value, unit, timestamp_ns, entity_id, attributes_json "
            "FROM measurements ORDER BY id"
        ).fetchall()
        return tuple(
            Measurement(
                name=_required_text(row["name"], "measurement name"),
                value=_measurement_value(row["value"]),
                unit=_required_text(row["unit"], "measurement unit"),
                timestamp_ns=_integer_value(
                    row["timestamp_ns"], "measurement timestamp", optional=True
                ),
                entity_id=_text_value(row["entity_id"], "measurement entity id", optional=True),
                attributes=_object(row["attributes_json"]),
            )
            for row in rows
        )

    def entities(self) -> tuple[Entity, ...]:
        rows = self._execute("SELECT * FROM entities ORDER BY id").fetchall()
        return tuple(
            Entity(
                id=_required_text(row["id"], "entity id"),
                kind=_required_text(row["kind"], "entity kind"),
                name=_required_text(row["name"], "entity name"),
                parent_entity_id=_text_value(
                    row["parent_entity_id"], "parent entity id", optional=True
                ),
                attributes=_object(row["attributes_json"]),
            )
            for row in rows
        )

    def events(self) -> tuple[Event, ...]:
        rows = self._execute("SELECT * FROM events ORDER BY started_at_ns, sequence, id").fetchall()
        events = []
        for row in rows:
            started_at_ns = _integer_value(
                row["started_at_ns"], "event start timestamp", optional=True
            )
            finished_at_ns = _integer_value(
                row["finished_at_ns"], "event finish timestamp", optional=True
            )
            if (
                started_at_ns is not None
                and finished_at_ns is not None
                and finished_at_ns < started_at_ns
            ):
                raise RunpackError("event cannot finish before it starts")
            uncertainty_ns = _integer_value(
                row["uncertainty_ns"], "event uncertainty", optional=True
            )
            if uncertainty_ns is not None and uncertainty_ns < 0:
                raise RunpackError("event uncertainty cannot be negative")
            events.append(
                Event(
                    id=_required_text(row["id"], "event id"),
                    kind=_required_text(row["kind"], "event kind"),
                    name=_required_text(row["name"], "event name"),
                    entity_id=_text_value(row["entity_id"], "event entity id", optional=True),
                    started_at_ns=started_at_ns,
                    finished_at_ns=finished_at_ns,
                    clock_domain=_text_value(
                        row["clock_domain"], "event clock domain", optional=True
                    ),
                    uncertainty_ns=uncertainty_ns,
                    sequence=_integer_value(row["sequence"], "event sequence", optional=True),
                    attributes=_object(row["attributes_json"]),
                )
            )
        return tuple(events)

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
                source_event_id=_required_text(
                    row["source_event_id"], "causal edge source event id"
                ),
                target_event_id=_required_text(
                    row["target_event_id"], "causal edge target event id"
                ),
                kind=_required_text(row["kind"], "causal edge kind"),
                confidence=_confidence_value(row["confidence"]),
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
            WHERE event.kind != 'log.record'
            GROUP BY entity_kind, entity_name, event_kind, event_name
            """
        ).fetchall()
        return {
            (row["entity_kind"], row["entity_name"], row["event_kind"], row["event_name"]): int(
                row["event_count"]
            )
            for row in rows
        }

    def operation_duration_totals(self) -> dict[tuple[str, str, str, str], float]:
        rows = self._execute(
            """
            SELECT
                COALESCE(entity.kind, 'unowned') AS entity_kind,
                COALESCE(entity.name, 'unowned') AS entity_name,
                event.kind AS event_kind,
                event.name AS event_name,
                total(CAST(event.finished_at_ns - event.started_at_ns AS REAL))
                    / 1000000000.0 AS duration_seconds
            FROM events AS event
            LEFT JOIN entities AS entity ON entity.id = event.entity_id
            WHERE event.kind != 'log.record'
            GROUP BY entity_kind, entity_name, event_kind, event_name
            HAVING count(*) = count(event.started_at_ns)
               AND count(*) = count(event.finished_at_ns)
            """
        ).fetchall()
        return {
            (row["entity_kind"], row["entity_name"], row["event_kind"], row["event_name"]): float(
                row["duration_seconds"]
            )
            for row in rows
        }

    def operation_max_concurrency(self) -> dict[tuple[str, str, str, str], int]:
        rows = self._execute(
            """
            WITH normalized AS (
                SELECT
                    COALESCE(entity.kind, 'unowned') AS entity_kind,
                    COALESCE(entity.name, 'unowned') AS entity_name,
                    event.kind AS event_kind,
                    event.name AS event_name,
                    CASE
                        WHEN event.clock_domain IS NULL
                            THEN 'entity:' || COALESCE(event.entity_id, 'unowned')
                        ELSE 'clock:' || event.clock_domain
                    END AS concurrency_domain,
                    event.started_at_ns,
                    event.finished_at_ns
                FROM events AS event
                LEFT JOIN entities AS entity ON entity.id = event.entity_id
                WHERE event.kind != 'log.record'
            ),
            eligible AS (
                SELECT entity_kind, entity_name, event_kind, event_name
                FROM normalized
                GROUP BY entity_kind, entity_name, event_kind, event_name
                HAVING count(*) = count(started_at_ns)
                   AND count(*) = count(finished_at_ns)
            ),
            boundaries AS (
                SELECT normalized.entity_kind, normalized.entity_name,
                       normalized.event_kind, normalized.event_name,
                       normalized.concurrency_domain,
                       normalized.started_at_ns AS timestamp_ns, 1 AS delta
                FROM normalized
                JOIN eligible USING (entity_kind, entity_name, event_kind, event_name)
                WHERE normalized.finished_at_ns > normalized.started_at_ns
                UNION ALL
                SELECT normalized.entity_kind, normalized.entity_name,
                       normalized.event_kind, normalized.event_name,
                       normalized.concurrency_domain,
                       normalized.finished_at_ns AS timestamp_ns, -1 AS delta
                FROM normalized
                JOIN eligible USING (entity_kind, entity_name, event_kind, event_name)
                WHERE normalized.finished_at_ns > normalized.started_at_ns
            ),
            active AS (
                SELECT entity_kind, entity_name, event_kind, event_name,
                       concurrency_domain,
                       sum(delta) OVER (
                           PARTITION BY entity_kind, entity_name, event_kind, event_name,
                                        concurrency_domain
                           ORDER BY timestamp_ns, delta
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS active_count
                FROM boundaries
            )
            SELECT entity_kind, entity_name, event_kind, event_name,
                   max(active_count) AS max_concurrency
            FROM active
            GROUP BY entity_kind, entity_name, event_kind, event_name
            """
        ).fetchall()
        return {
            (row["entity_kind"], row["entity_name"], row["event_kind"], row["event_name"]): int(
                row["max_concurrency"]
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
            WHERE source_event.kind != 'log.record'
              AND target_event.kind != 'log.record'
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

    def peer_service_edge_counts(self) -> dict[tuple[str, str, str, str, str], int]:
        rows = self._execute(
            """
            SELECT
                COALESCE(entity.kind, 'unowned') AS source_kind,
                COALESCE(entity.name, 'unowned') AS source_name,
                event.attributes_json
            FROM events AS event
            LEFT JOIN entities AS entity ON entity.id = event.entity_id
            WHERE event.kind = 'client.request'
            """
        ).fetchall()
        counts: dict[tuple[str, str, str, str, str], int] = {}
        for row in rows:
            peer = _object(row["attributes_json"]).get("peer.service")
            if not isinstance(peer, str):
                continue
            key = (row["source_kind"], row["source_name"], "service", peer, "calls")
            counts[key] = counts.get(key, 0) + 1
        return counts

    def attachments(self) -> tuple[Attachment, ...]:
        tables = {
            str(row[0])
            for row in self._execute(
                "SELECT name FROM sqlite_schema WHERE type = 'table'"
            ).fetchall()
        }
        if "attachments" not in tables:
            return ()
        rows = self._execute(
            """
            SELECT id, kind, name, media_type, content, attributes_json
            FROM attachments
            ORDER BY id
            """
        ).fetchall()
        return tuple(
            Attachment(
                id=_required_text(row["id"], "attachment id"),
                kind=_required_text(row["kind"], "attachment kind"),
                name=_required_text(row["name"], "attachment name"),
                media_type=_required_text(row["media_type"], "attachment media type"),
                content=_blob(row["content"]),
                attributes=_object(row["attributes_json"]),
            )
            for row in rows
        )

    def counts(self) -> dict[str, int]:
        counts = {
            table: self._execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("entities", "events", "causal_edges", "measurements")
        }
        attachment_count = self._execute(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'attachments'"
        ).fetchone()[0]
        counts["attachments"] = (
            self._execute("SELECT count(*) FROM attachments").fetchone()[0]
            if attachment_count
            else 0
        )
        return counts

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
