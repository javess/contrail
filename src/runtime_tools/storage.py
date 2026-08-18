"""Versioned SQLite storage for portable ``.runpack`` artifacts."""

from __future__ import annotations

import json
import math
import os
import sqlite3
from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType

from runtime_tools import __version__
from runtime_tools.json_support import reject_duplicate_object
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
MAX_RUNPACK_JSON_BYTES = 4 * 1024 * 1024
MAX_RUNPACK_TEXT_BYTES = 4 * 1024 * 1024
MAX_RUNPACK_ATTACHMENT_BYTES = 64 * 1024 * 1024
MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES = 256 * 1024 * 1024
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
_REQUIRED_COLUMNS = {
    "manifest": {"key", "value"},
    "executions": {
        "id",
        "name",
        "started_at_ns",
        "finished_at_ns",
        "command_json",
        "working_directory",
        "exit_code",
        "revision",
        "metadata_json",
    },
    "entities": {"id", "kind", "name", "parent_entity_id", "attributes_json"},
    "events": {
        "id",
        "kind",
        "name",
        "entity_id",
        "started_at_ns",
        "finished_at_ns",
        "clock_domain",
        "uncertainty_ns",
        "sequence",
        "attributes_json",
    },
    "causal_edges": {
        "source_event_id",
        "target_event_id",
        "kind",
        "confidence",
        "attributes_json",
    },
    "measurements": {
        "id",
        "name",
        "value",
        "unit",
        "timestamp_ns",
        "entity_id",
        "attributes_json",
    },
}
_OPTIONAL_COLUMNS = {
    "attachments": {"id", "kind", "name", "media_type", "content", "attributes_json"}
}
_PRIMARY_KEYS = {
    "manifest": ("key",),
    "executions": ("id",),
    "entities": ("id",),
    "events": ("id",),
    "causal_edges": ("source_event_id", "target_event_id", "kind"),
    "measurements": ("id",),
    "attachments": ("id",),
}
_FOREIGN_KEYS = {
    "entities": {("parent_entity_id", "entities", "id")},
    "events": {("entity_id", "entities", "id")},
    "causal_edges": {
        ("source_event_id", "events", "id"),
        ("target_event_id", "events", "id"),
    },
    "measurements": {("entity_id", "entities", "id")},
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

_ATTACHMENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS attachments (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    content BLOB NOT NULL,
    attributes_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS attachments_kind_name_idx ON attachments(kind, name);
"""


class RunpackError(ValueError):
    """Raised when a runpack cannot be read or written safely."""


class UnsupportedSchemaError(RunpackError):
    """Raised when a runpack uses an unsupported schema major version."""


def _json(value: JsonValue) -> str:
    try:
        normalized = _checked_json(value)
        encoded = json.dumps(normalized, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except RunpackError as exc:
        raise RunpackError("invalid JSON value for runpack") from exc
    except (OverflowError, TypeError, ValueError, RecursionError) as exc:
        raise RunpackError("invalid JSON value for runpack") from exc
    _validate_json_size(encoded)
    return encoded


def _command_json(value: object) -> str:
    if not isinstance(value, tuple) or not all(isinstance(item, str) for item in value):
        raise RunpackError("execution command must be a tuple of strings")
    _validate_command_parts(value)
    return _json(list(value))


def _validate_command_parts(value: tuple[str, ...] | list[str]) -> None:
    if value and not value[0]:
        raise RunpackError("execution command executable must be non-empty")
    if any("\0" in item for item in value):
        raise RunpackError("execution command arguments cannot contain NUL bytes")


def _validate_json_size(value: str) -> None:
    byte_count = len(value.encode("utf-8", errors="surrogatepass"))
    if byte_count > MAX_RUNPACK_JSON_BYTES:
        raise RunpackError(f"runpack JSON exceeds the {MAX_RUNPACK_JSON_BYTES}-byte field limit")


def _object(value: object) -> dict[str, JsonValue]:
    if not isinstance(value, str):
        raise RunpackError("invalid JSON object in runpack")
    _validate_json_size(value)
    try:
        decoded = json.loads(value, object_pairs_hook=reject_duplicate_object)
    except (TypeError, ValueError, RecursionError) as exc:
        raise RunpackError("invalid JSON object in runpack") from exc
    try:
        decoded = _checked_json(decoded)
    except RecursionError as exc:
        raise RunpackError("invalid JSON object in runpack") from exc
    if not isinstance(decoded, dict):
        raise RunpackError("expected a JSON object in runpack")
    return decoded


def _checked_json(value: object) -> JsonValue:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise RunpackError("runpack JSON strings must be valid UTF-8") from exc
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RunpackError("non-finite number in runpack JSON")
        return value
    if isinstance(value, list):
        return [_checked_json(item) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {_checked_json_key(key): _checked_json(item) for key, item in value.items()}
    raise RunpackError("invalid value in runpack JSON")


def _checked_json_key(value: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise RunpackError("runpack JSON strings must be valid UTF-8") from exc
    return value


def _blob(value: object) -> bytes:
    if isinstance(value, bytes):
        content = value
    elif isinstance(value, memoryview):
        content = value.tobytes()
    else:
        raise RunpackError("invalid binary attachment content in runpack")
    if len(content) > MAX_RUNPACK_ATTACHMENT_BYTES:
        raise RunpackError(
            "attachment content exceeds the "
            f"{MAX_RUNPACK_ATTACHMENT_BYTES}-byte runpack field limit"
        )
    return content


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
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise RunpackError(f"{label} must be valid UTF-8") from exc
    if len(encoded) > MAX_RUNPACK_TEXT_BYTES:
        raise RunpackError(
            f"{label} exceeds the {MAX_RUNPACK_TEXT_BYTES}-byte runpack text field limit"
        )
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


def _entity_values(entity: Entity) -> tuple[object, ...]:
    entity_id = _text_value(entity.id, "entity id")
    parent_id = _text_value(entity.parent_entity_id, "parent entity id", optional=True)
    if entity_id == parent_id:
        raise RunpackError("entity cannot be its own parent")
    return (
        entity_id,
        _text_value(entity.kind, "entity kind"),
        _text_value(entity.name, "entity name"),
        parent_id,
        _json(entity.attributes),
    )


def _validate_entity_hierarchy(entities: tuple[Entity, ...]) -> None:
    if any(entity.id == entity.parent_entity_id for entity in entities):
        raise RunpackError("entity cannot be its own parent")
    parent_by_child = {
        entity.id: entity.parent_entity_id
        for entity in entities
        if entity.parent_entity_id is not None
    }
    complete: set[str] = set()
    for entity_id in parent_by_child:
        trail: set[str] = set()
        current = entity_id
        while current in parent_by_child and current not in complete:
            if current in trail:
                raise RunpackError("entity parent relationships contain a cycle")
            trail.add(current)
            parent = parent_by_child[current]
            assert parent is not None
            current = parent
        complete.update(trail)


def _parent_first_entities(entities: tuple[Entity, ...]) -> tuple[Entity, ...]:
    _validate_entity_hierarchy(entities)
    entity_ids = {entity.id for entity in entities}
    children: dict[str, list[int]] = {}
    dependencies = [0] * len(entities)
    for index, entity in enumerate(entities):
        parent_id = entity.parent_entity_id
        if parent_id is not None and parent_id in entity_ids:
            dependencies[index] = 1
            children.setdefault(parent_id, []).append(index)
    ready = deque(index for index, dependency in enumerate(dependencies) if dependency == 0)
    ordered: list[Entity] = []
    while ready:
        index = ready.popleft()
        entity = entities[index]
        ordered.append(entity)
        for child_index in children.get(entity.id, ()):
            dependencies[child_index] -= 1
            if dependencies[child_index] == 0:
                ready.append(child_index)
    if len(ordered) != len(entities):
        raise RunpackError("entity parent relationships contain a cycle")
    return tuple(ordered)


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
    source_event_id = _text_value(edge.source_event_id, "causal edge source event id")
    target_event_id = _text_value(edge.target_event_id, "causal edge target event id")
    if source_event_id == target_event_id:
        raise RunpackError("causal edge cannot reference the same event twice")
    return (
        source_event_id,
        target_event_id,
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
    for table, required_columns in (_REQUIRED_COLUMNS | _OPTIONAL_COLUMNS).items():
        if table not in tables:
            continue
        table_info = connection.execute(f"PRAGMA table_info({table})").fetchall()
        columns = {str(row[1]) for row in table_info}
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            raise RunpackError(
                f"runpack table {table} is missing required columns: {', '.join(missing_columns)}"
            )
        primary_key = tuple(
            str(row[1]) for row in sorted(table_info, key=lambda row: row[5]) if row[5]
        )
        expected_primary_key = _PRIMARY_KEYS[table]
        if primary_key != expected_primary_key:
            raise RunpackError(
                f"runpack table {table} does not enforce required primary key: "
                f"{', '.join(expected_primary_key)}"
            )
        foreign_keys = {
            (str(row[3]), str(row[2]), str(row[4]))
            for row in connection.execute(f"PRAGMA foreign_key_list({table})").fetchall()
        }
        missing_foreign_keys = _FOREIGN_KEYS.get(table, set()) - foreign_keys
        if missing_foreign_keys:
            source, target_table, target = sorted(missing_foreign_keys)[0]
            raise RunpackError(
                f"runpack table {table} does not enforce required relationship: "
                f"{source} -> {target_table}.{target}"
            )
    row = connection.execute("SELECT value FROM manifest WHERE key = 'schema_version'").fetchone()
    if row is None:
        raise RunpackError("runpack has no schema version")
    version = _required_text(row[0], "runpack schema version")
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
                _entity_values(entity),
            )
            self._connection.commit()

    def add_entities(self, entities: Iterable[Entity]) -> None:
        ordered = _parent_first_entities(tuple(entities))
        with self._writing(), self._connection:
            self._connection.executemany(
                """
                INSERT INTO entities(id, kind, name, parent_entity_id, attributes_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (_entity_values(entity) for entity in ordered),
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

    def add_event_graph(self, events: Iterable[Event], edges: Iterable[CausalEdge]) -> None:
        """Add related events and edges in one transaction."""
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
            self._connection.executemany(
                """
                INSERT INTO causal_edges(
                    source_event_id, target_event_id, kind, confidence, attributes_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (_edge_values(edge) for edge in edges),
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
        values = tuple(_attachment_values(attachment) for attachment in attachments)
        added_bytes = 0
        for value in values:
            content = value[4]
            assert isinstance(content, bytes)
            added_bytes += len(content)
        with self._writing(), self._connection:
            self._connection.executescript(_ATTACHMENTS_SCHEMA)
            existing_bytes = self._connection.execute(
                "SELECT COALESCE(sum(length(content)), 0) FROM attachments"
            ).fetchone()[0]
            if existing_bytes + added_bytes > MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES:
                raise RunpackError(
                    "attachment content exceeds the "
                    f"{MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES}-byte aggregate runpack limit"
                )
            self._connection.executemany(
                """
                INSERT INTO attachments(id, kind, name, media_type, content, attributes_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    def expand_execution_bounds(self, started_at_ns: int, finished_at_ns: int | None) -> None:
        normalized_start, normalized_finish = _execution_interval(started_at_ns, finished_at_ns)
        with self._writing():
            cursor = self._connection.execute(
                """
            UPDATE executions
            SET started_at_ns = min(started_at_ns, ?),
                finished_at_ns = CASE
                    WHEN finished_at_ns IS NULL THEN NULL
                    WHEN ? IS NULL THEN finished_at_ns
                    ELSE max(finished_at_ns, ?)
                END
            """,
                (
                    normalized_start,
                    normalized_finish,
                    normalized_finish,
                ),
            )
            if cursor.rowcount != 1:
                raise RunpackError("runpack must contain exactly one execution to expand")
            self._connection.commit()

    def set_execution_metadata(
        self,
        execution_id: str,
        metadata: dict[str, JsonValue],
    ) -> None:
        normalized_execution_id = _text_value(execution_id, "execution id")
        encoded_metadata = _json(metadata)
        with self._writing():
            cursor = self._connection.execute(
                "UPDATE executions SET metadata_json = ? WHERE id = ?",
                (encoded_metadata, normalized_execution_id),
            )
            if cursor.rowcount != 1:
                raise RunpackError(f"execution does not exist: {normalized_execution_id}")
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

    def _execute(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Cursor:
        try:
            return self._connection.execute(sql, parameters)
        except sqlite3.DatabaseError as exc:
            raise RunpackError(f"could not read runpack: {exc}") from exc

    def execution(self) -> Execution:
        rows = self._execute("SELECT * FROM executions").fetchall()
        if len(rows) != 1:
            raise RunpackError(f"expected exactly one execution, found {len(rows)}")
        row = rows[0]
        raw_command = row["command_json"]
        if not isinstance(raw_command, str):
            raise RunpackError("execution command is invalid JSON")
        _validate_json_size(raw_command)
        try:
            command = json.loads(raw_command, object_pairs_hook=reject_duplicate_object)
        except (TypeError, ValueError, RecursionError) as exc:
            raise RunpackError("execution command is invalid JSON") from exc
        if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
            raise RunpackError("execution command is invalid")
        _validate_command_parts(command)
        _checked_json(command)
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
        return {
            _required_text(row["key"], "manifest key"): _required_text(
                row["value"], "manifest value"
            )
            for row in rows
        }

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

    def first_measurement_values(
        self, identities: tuple[tuple[str, str], ...]
    ) -> dict[tuple[str, str], float]:
        values: dict[tuple[str, str], float] = {}
        for name, unit in identities:
            normalized_name = _required_text(name, "measurement name")
            normalized_unit = _required_text(unit, "measurement unit")
            row = self._execute(
                "SELECT value FROM measurements WHERE name = ? AND unit = ? ORDER BY id LIMIT 1",
                (normalized_name, normalized_unit),
            ).fetchone()
            if row is not None:
                values[(name, unit)] = _measurement_value(row[0])
        return values

    def entities(self) -> tuple[Entity, ...]:
        rows = self._execute("SELECT * FROM entities ORDER BY id").fetchall()
        entities = tuple(
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
        _validate_entity_hierarchy(entities)
        return entities

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
        edges = []
        for row in rows:
            source_event_id = _required_text(row["source_event_id"], "causal edge source event id")
            target_event_id = _required_text(row["target_event_id"], "causal edge target event id")
            if source_event_id == target_event_id:
                raise RunpackError("causal edge cannot reference the same event twice")
            edges.append(
                CausalEdge(
                    source_event_id=source_event_id,
                    target_event_id=target_event_id,
                    kind=_required_text(row["kind"], "causal edge kind"),
                    confidence=_confidence_value(row["confidence"]),
                    attributes=_object(row["attributes_json"]),
                )
            )
        return tuple(edges)

    def clock_inconsistency_count(self) -> int:
        row = self._execute(
            """
            SELECT count(*)
            FROM causal_edges AS edge
            JOIN events AS parent ON parent.id = edge.source_event_id
            JOIN events AS child ON child.id = edge.target_event_id
            WHERE (
                edge.kind = 'parent'
                AND (
                    (parent.started_at_ns IS NOT NULL AND child.started_at_ns IS NOT NULL
                     AND parent.started_at_ns - child.started_at_ns
                         > COALESCE(parent.uncertainty_ns, 0)
                           + COALESCE(child.uncertainty_ns, 0))
                    OR
                    (parent.finished_at_ns IS NOT NULL AND child.finished_at_ns IS NOT NULL
                     AND child.finished_at_ns - parent.finished_at_ns
                         > COALESCE(parent.uncertainty_ns, 0)
                           + COALESCE(child.uncertainty_ns, 0))
                )
            ) OR (
                edge.kind != 'parent'
                AND parent.started_at_ns IS NOT NULL
                AND child.started_at_ns IS NOT NULL
                AND parent.started_at_ns - child.started_at_ns
                    > COALESCE(parent.uncertainty_ns, 0)
                      + COALESCE(child.uncertainty_ns, 0)
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

    def operation_error_counts(self) -> dict[tuple[str, str, str, str], int]:
        rows = self._execute(
            """
            SELECT
                COALESCE(entity.kind, 'unowned') AS entity_kind,
                COALESCE(entity.name, 'unowned') AS entity_name,
                event.kind AS event_kind,
                event.name AS event_name,
                event.attributes_json
            FROM events AS event
            LEFT JOIN entities AS entity ON entity.id = event.entity_id
            WHERE event.kind != 'log.record'
            """
        ).fetchall()
        counts: dict[tuple[str, str, str, str], int] = {}
        for row in rows:
            attributes = _object(row["attributes_json"])
            status = attributes.get("otel.status.code")
            error_type = attributes.get("error.type")
            if not (
                attributes.get("error") is True
                or status in (2, "2", "STATUS_CODE_ERROR")
                or isinstance(error_type, str)
                and bool(error_type)
            ):
                continue
            key = (
                row["entity_kind"],
                row["entity_name"],
                row["event_kind"],
                row["event_name"],
            )
            counts[key] = counts.get(key, 0) + 1
        return counts

    def entity_counts(self) -> dict[tuple[str, str], int]:
        rows = self._execute(
            """
            SELECT kind, name, count(*) AS entity_count
            FROM entities
            GROUP BY kind, name
            """
        ).fetchall()
        return {(row["kind"], row["name"]): int(row["entity_count"]) for row in rows}

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
                    event.clock_domain AS concurrency_domain,
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
                   AND count(*) = count(concurrency_domain)
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
              AND source_event.entity_id IS NOT target_event.entity_id
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
            if not isinstance(peer, str) or not peer:
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
        total_bytes = self._execute(
            "SELECT COALESCE(sum(length(content)), 0) FROM attachments"
        ).fetchone()[0]
        if total_bytes > MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES:
            raise RunpackError(
                "attachment content exceeds the "
                f"{MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES}-byte aggregate runpack limit"
            )
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

    def normalized_json_bytes(self) -> int:
        row = self._execute(
            """
            SELECT CAST(total(byte_count) AS INTEGER)
            FROM (
                SELECT length(CAST(command_json AS BLOB))
                     + length(CAST(metadata_json AS BLOB)) AS byte_count
                FROM executions
                UNION ALL
                SELECT length(CAST(attributes_json AS BLOB)) FROM entities
                UNION ALL
                SELECT length(CAST(attributes_json AS BLOB)) FROM events
                UNION ALL
                SELECT length(CAST(attributes_json AS BLOB)) FROM causal_edges
                UNION ALL
                SELECT length(CAST(attributes_json AS BLOB)) FROM measurements
            )
            """
        ).fetchone()
        return int(row[0])

    def normalized_text_bytes(self) -> int:
        row = self._execute(
            """
            SELECT CAST(total(byte_count) AS INTEGER)
            FROM (
                SELECT length(CAST(key AS BLOB)) + length(CAST(value AS BLOB)) AS byte_count
                FROM manifest
                UNION ALL
                SELECT length(CAST(id AS BLOB)) + length(CAST(name AS BLOB))
                     + length(CAST(working_directory AS BLOB))
                     + COALESCE(length(CAST(revision AS BLOB)), 0)
                FROM executions
                UNION ALL
                SELECT length(CAST(id AS BLOB)) + length(CAST(kind AS BLOB))
                     + length(CAST(name AS BLOB))
                     + COALESCE(length(CAST(parent_entity_id AS BLOB)), 0)
                FROM entities
                UNION ALL
                SELECT length(CAST(id AS BLOB)) + length(CAST(kind AS BLOB))
                     + length(CAST(name AS BLOB))
                     + COALESCE(length(CAST(entity_id AS BLOB)), 0)
                     + COALESCE(length(CAST(clock_domain AS BLOB)), 0)
                FROM events
                UNION ALL
                SELECT length(CAST(source_event_id AS BLOB))
                     + length(CAST(target_event_id AS BLOB)) + length(CAST(kind AS BLOB))
                FROM causal_edges
                UNION ALL
                SELECT length(CAST(name AS BLOB)) + length(CAST(unit AS BLOB))
                     + COALESCE(length(CAST(entity_id AS BLOB)), 0)
                FROM measurements
            )
            """
        ).fetchone()
        return int(row[0])

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
