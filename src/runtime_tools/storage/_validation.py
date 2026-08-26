"""Versioned SQLite storage for portable ``.runpack`` artifacts."""

from __future__ import annotations

import json
import math
import sqlite3
from collections import deque
from dataclasses import dataclass

from runtime_tools.json_support import JsonValueModel, reject_duplicate_object
from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
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
MAX_RUNPACK_FILE_BYTES = 2 * 1024**3
# These whole-artifact limits keep normalized reads within a 4 GiB host envelope while
# admitting the supported million-record OTLP and Prometheus inputs.
MAX_RUNPACK_NORMALIZED_JSON_BYTES = 256 * 1024 * 1024
MAX_RUNPACK_NORMALIZED_TEXT_BYTES = 256 * 1024 * 1024
MAX_RUNPACK_MANIFEST_RECORDS = 1_024
MAX_RUNPACK_EXECUTION_RECORDS = 1
MAX_RUNPACK_ENTITY_RECORDS = 1_000_000
MAX_RUNPACK_EVENT_RECORDS = 1_000_000
MAX_RUNPACK_CAUSAL_EDGE_RECORDS = 2_000_000
MAX_RUNPACK_MEASUREMENT_RECORDS = 1_000_000
MAX_RUNPACK_ATTACHMENT_RECORDS = 100_000
# SQLite applies SQLITE_LIMIT_LENGTH to both individual values and encoded rows. Leave
# enough row headroom for a maximum attachment plus its bounded normalized metadata.
MAX_RUNPACK_SQLITE_LENGTH_BYTES = 128 * 1024 * 1024
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
_BINARY_PRIMARY_KEY_TABLES = frozenset(_PRIMARY_KEYS) - {"measurements"}
_FOREIGN_KEYS = {
    "entities": {("parent_entity_id", "entities", "id")},
    "events": {("entity_id", "entities", "id")},
    "causal_edges": {
        ("source_event_id", "events", "id"),
        ("target_event_id", "events", "id"),
    },
    "measurements": {("entity_id", "entities", "id")},
}
_TEXT_COLUMNS = {
    "manifest": ("key", "value"),
    "executions": ("id", "name", "working_directory", "revision"),
    "entities": ("id", "kind", "name", "parent_entity_id"),
    "events": ("id", "kind", "name", "entity_id", "clock_domain"),
    "causal_edges": ("source_event_id", "target_event_id", "kind"),
    "measurements": ("name", "unit", "entity_id"),
    "attachments": ("id", "kind", "name", "media_type"),
}
_JSON_COLUMNS = {
    "manifest": (),
    "executions": ("command_json", "metadata_json"),
    "entities": ("attributes_json",),
    "events": ("attributes_json",),
    "causal_edges": ("attributes_json",),
    "measurements": ("attributes_json",),
    "attachments": ("attributes_json",),
}
_TEXT_COLUMN_LABELS = {
    ("manifest", "key"): "manifest key",
    ("manifest", "value"): "manifest value",
    ("executions", "id"): "execution id",
    ("executions", "name"): "execution name",
    ("executions", "working_directory"): "execution working directory",
    ("executions", "revision"): "execution revision",
    ("entities", "id"): "entity id",
    ("entities", "kind"): "entity kind",
    ("entities", "name"): "entity name",
    ("entities", "parent_entity_id"): "entity parent id",
    ("events", "id"): "event id",
    ("events", "kind"): "event kind",
    ("events", "name"): "event name",
    ("events", "entity_id"): "event entity id",
    ("events", "clock_domain"): "event clock domain",
    ("causal_edges", "source_event_id"): "causal edge source event id",
    ("causal_edges", "target_event_id"): "causal edge target event id",
    ("causal_edges", "kind"): "causal edge kind",
    ("measurements", "name"): "measurement name",
    ("measurements", "unit"): "measurement unit",
    ("measurements", "entity_id"): "measurement entity id",
    ("attachments", "id"): "attachment id",
    ("attachments", "kind"): "attachment kind",
    ("attachments", "name"): "attachment name",
    ("attachments", "media_type"): "attachment media type",
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
    """Raised when a runpack schema cannot be read or safely modified."""


@dataclass(frozen=True, slots=True)
class RunpackArtifactIdentity(JsonValueModel):
    """Byte identity of the exact runpack backing a validated read snapshot."""

    size_bytes: int
    sha256: str


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


def _operation_identity(row: sqlite3.Row) -> tuple[str, str, str, str]:
    return (
        _required_text(row["entity_kind"], "operation entity kind"),
        _required_text(row["entity_name"], "operation entity name"),
        _required_text(row["event_kind"], "operation kind"),
        _required_text(row["event_name"], "operation name"),
    )


def _execution_interval(started_at_ns: object, finished_at_ns: object) -> tuple[int, int | None]:
    started = _integer_value(started_at_ns, "execution start timestamp")
    finished = _integer_value(finished_at_ns, "execution finish timestamp", optional=True)
    assert started is not None
    if finished is not None and finished < started:
        raise RunpackError("execution cannot finish before it starts")
    return started, finished


def _entity_identity(entity: Entity) -> tuple[str, str | None]:
    entity_id = _text_value(entity.id, "entity id")
    parent_id = _text_value(entity.parent_entity_id, "parent entity id", optional=True)
    assert entity_id is not None
    if entity_id == parent_id:
        raise RunpackError("entity cannot be its own parent")
    return entity_id, parent_id


def _entity_values(entity: Entity) -> tuple[object, ...]:
    entity_id, parent_id = _entity_identity(entity)
    return (
        entity_id,
        _text_value(entity.kind, "entity kind"),
        _text_value(entity.name, "entity name"),
        parent_id,
        _json(entity.attributes),
    )


def _validate_entity_hierarchy(entities: tuple[Entity, ...]) -> None:
    identities = tuple(_entity_identity(entity) for entity in entities)
    parent_by_child = {
        entity_id: parent_id for entity_id, parent_id in identities if parent_id is not None
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


def _set_runpack_connection_limits(connection: sqlite3.Connection) -> None:
    connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_RUNPACK_SQLITE_LENGTH_BYTES)


def _require_runpack_file_size(size_bytes: int) -> None:
    if size_bytes > MAX_RUNPACK_FILE_BYTES:
        raise RunpackError(
            f"runpack is {size_bytes} bytes; file limit is {MAX_RUNPACK_FILE_BYTES} bytes"
        )


def _record_limits() -> dict[str, int]:
    return {
        "manifest": MAX_RUNPACK_MANIFEST_RECORDS,
        "executions": MAX_RUNPACK_EXECUTION_RECORDS,
        "entities": MAX_RUNPACK_ENTITY_RECORDS,
        "events": MAX_RUNPACK_EVENT_RECORDS,
        "causal_edges": MAX_RUNPACK_CAUSAL_EDGE_RECORDS,
        "measurements": MAX_RUNPACK_MEASUREMENT_RECORDS,
        "attachments": MAX_RUNPACK_ATTACHMENT_RECORDS,
    }


def _column_bytes(column: str) -> str:
    return f"length(CAST({column} AS BLOB))"


def _row_bytes(columns: tuple[str, ...]) -> str:
    if not columns:
        return "0"
    return " + ".join(f"COALESCE({_column_bytes(column)}, 0)" for column in columns)


def _preflight_normalized_content(connection: sqlite3.Connection, tables: set[str]) -> None:
    total_text_bytes = 0
    total_json_bytes = 0
    for table, record_limit in _record_limits().items():
        if table not in tables:
            continue
        record_overflow = connection.execute(
            f"SELECT 1 FROM {table} LIMIT 1 OFFSET ?", (record_limit,)
        ).fetchone()
        if record_overflow is not None:
            raise RunpackError(f"runpack table {table} exceeds the record limit of {record_limit}")
        text_columns = _TEXT_COLUMNS[table]
        json_columns = _JSON_COLUMNS[table]
        text_maxima = tuple(f"COALESCE(max({_column_bytes(column)}), 0)" for column in text_columns)
        json_maxima = tuple(f"COALESCE(max({_column_bytes(column)}), 0)" for column in json_columns)
        content_maximum = (
            f", COALESCE(max({_column_bytes('content')}), 0), "
            f"COALESCE(sum({_column_bytes('content')}), 0)"
            if table == "attachments"
            else ", 0, 0"
        )
        row = connection.execute(
            "SELECT count(*), " + ", ".join((*text_maxima, *json_maxima)) + ", "
            f"COALESCE(sum({_row_bytes(text_columns)}), 0), "
            f"COALESCE(sum({_row_bytes(json_columns)}), 0)"
            f"{content_maximum} FROM (SELECT * FROM {table} LIMIT {record_limit})"
        ).fetchone()
        if row is None:
            raise RunpackError(f"could not preflight runpack table {table}")
        offset = 1
        text_column_maxima = row[offset : offset + len(text_columns)]
        for column, maximum in zip(text_columns, text_column_maxima, strict=True):
            if int(maximum) > MAX_RUNPACK_TEXT_BYTES:
                label = _TEXT_COLUMN_LABELS[(table, column)]
                raise RunpackError(
                    f"{label} exceeds the {MAX_RUNPACK_TEXT_BYTES}-byte runpack text field limit"
                )
        offset += len(text_columns)
        json_column_maxima = row[offset : offset + len(json_columns)]
        if any(int(maximum) > MAX_RUNPACK_JSON_BYTES for maximum in json_column_maxima):
            raise RunpackError(
                f"runpack JSON exceeds the {MAX_RUNPACK_JSON_BYTES}-byte field limit"
            )
        offset += len(json_columns)
        text_bytes, json_bytes = map(int, row[offset : offset + 2])
        offset += 2
        total_text_bytes += text_bytes
        total_json_bytes += json_bytes
        if table == "attachments":
            content_maximum_bytes, content_bytes = map(int, row[offset : offset + 2])
            if content_maximum_bytes > MAX_RUNPACK_ATTACHMENT_BYTES:
                raise RunpackError(
                    "attachment content exceeds the "
                    f"{MAX_RUNPACK_ATTACHMENT_BYTES}-byte runpack field limit"
                )
            if content_bytes > MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES:
                raise RunpackError(
                    "attachment content exceeds the "
                    f"{MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES}-byte aggregate runpack limit"
                )
    if total_text_bytes > MAX_RUNPACK_NORMALIZED_TEXT_BYTES:
        raise RunpackError(
            f"runpack contains {total_text_bytes} normalized text bytes; aggregate limit is "
            f"{MAX_RUNPACK_NORMALIZED_TEXT_BYTES}"
        )
    if total_json_bytes > MAX_RUNPACK_NORMALIZED_JSON_BYTES:
        raise RunpackError(
            f"runpack contains {total_json_bytes} normalized JSON bytes; aggregate limit is "
            f"{MAX_RUNPACK_NORMALIZED_JSON_BYTES}"
        )


def _validate_connection(connection: sqlite3.Connection) -> str:
    application_id = connection.execute("PRAGMA application_id").fetchone()
    if application_id is None or application_id[0] != APPLICATION_ID:
        raise RunpackError("file is not a Contrail runpack")
    journal_mode = connection.execute("PRAGMA journal_mode").fetchone()
    if journal_mode is None or str(journal_mode[0]).lower() != "delete":
        raise RunpackError("runpack must use DELETE journal mode for single-file portability")
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table'"
        ).fetchall()
    }
    missing = sorted(_REQUIRED_TABLES - tables)
    if missing:
        raise RunpackError(f"runpack is missing required tables: {', '.join(missing)}")
    triggers = tuple(
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'trigger' ORDER BY name"
        ).fetchall()
    )
    if triggers:
        raise RunpackError(f"runpack contains unsupported SQLite triggers: {', '.join(triggers)}")
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
        primary_key_indexes = tuple(
            str(row[1])
            for row in connection.execute(f"PRAGMA index_list({table})").fetchall()
            if row[3] == "pk"
        )
        if table == "measurements":
            measurement_id = next(row for row in table_info if str(row[1]) == "id")
            if str(measurement_id[2]).upper() != "INTEGER" or primary_key_indexes:
                raise RunpackError("runpack table measurements must use id INTEGER PRIMARY KEY")
        elif table in _BINARY_PRIMARY_KEY_TABLES:
            indexed_primary_key: tuple[tuple[str, str], ...] = ()
            if len(primary_key_indexes) == 1:
                indexed_primary_key = tuple(
                    (str(row[0]), str(row[1]).upper())
                    for row in connection.execute(
                        "SELECT name, coll FROM pragma_index_xinfo(?) "
                        'WHERE "key" = 1 ORDER BY seqno',
                        (primary_key_indexes[0],),
                    ).fetchall()
                )
            expected_binary_key = tuple((column, "BINARY") for column in expected_primary_key)
            if indexed_primary_key != expected_binary_key:
                raise RunpackError(f"runpack table {table} primary key must use BINARY collation")
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
    _preflight_normalized_content(connection, tables)
    row = connection.execute("SELECT value FROM manifest WHERE key = 'schema_version'").fetchone()
    if row is None:
        raise RunpackError("runpack has no schema version")
    version = _required_text(row[0], "runpack schema version")
    if version != SCHEMA_VERSION:
        raise UnsupportedSchemaError(
            f"unsupported runpack schema {version!r}; supported schema: {SCHEMA_VERSION}"
        )
    relationship_error = connection.execute("PRAGMA foreign_key_check").fetchone()
    if relationship_error is not None:
        table, row_id = relationship_error[:2]
        raise RunpackError(f"runpack contains an invalid relationship in {table} row {row_id}")
    return version


def _require_writable_schema(schema_version: str) -> None:
    if schema_version == SCHEMA_VERSION:
        return
    raise UnsupportedSchemaError(
        f"cannot modify runpack schema {schema_version!r}; writable schema: {SCHEMA_VERSION}"
    )
