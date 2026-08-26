"""Validated read-only access to normalized runpack records."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from pathlib import Path
from types import TracebackType

from runtime_tools.json_support import reject_duplicate_object
from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    Measurement,
)
from runtime_tools.semantics import is_operation_error
from runtime_tools.storage._validation import (
    MAX_RUNPACK_ATTACHMENT_BYTES,
    MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES,
    RunpackError,
    _blob,
    _checked_json,
    _confidence_value,
    _execution_interval,
    _integer_value,
    _measurement_value,
    _object,
    _operation_identity,
    _require_runpack_file_size,
    _require_writable_schema,
    _required_text,
    _set_runpack_connection_limits,
    _text_value,
    _validate_command_parts,
    _validate_connection,
    _validate_entity_hierarchy,
    _validate_json_size,
)
from runtime_tools.storage._writer import RunpackWriter


def resolve_runpack_path(path: Path) -> Path:
    """Resolve a runpack path once so later analysis cannot retarget a symlink."""
    try:
        resolved_path = path.resolve()
        is_file = resolved_path.is_file()
        size_bytes = resolved_path.stat().st_size if is_file else 0
    except (OSError, RuntimeError) as exc:
        raise RunpackError(f"could not resolve runpack path: {path}") from exc
    if not is_file:
        raise RunpackError(f"runpack does not exist: {path}")
    _require_runpack_file_size(size_bytes)
    return resolved_path


class RunpackReader:
    """Reads and validates one supported runpack."""

    def __init__(
        self,
        path: Path,
        *,
        prepare_connection: Callable[[sqlite3.Connection], None] | None = None,
    ) -> None:
        resolved_path = resolve_runpack_path(path)
        self.path = resolved_path
        connection: sqlite3.Connection | None = None
        try:
            uri = f"{resolved_path.as_uri()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            self._connection = connection
            self._connection.row_factory = sqlite3.Row
            _set_runpack_connection_limits(self._connection)
            if prepare_connection is not None:
                prepare_connection(self._connection)
            self._connection.execute("BEGIN")
            self._schema_version = _validate_connection(self._connection)
        except sqlite3.DatabaseError as exc:
            if connection is not None:
                connection.close()
            raise RunpackError(f"invalid runpack: {path}") from exc
        except RunpackError:
            if connection is not None:
                connection.close()
            raise
        except BaseException:
            if connection is not None:
                connection.close()
            raise

    def _execute(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Cursor:
        try:
            return self._connection.execute(sql, parameters)
        except sqlite3.DatabaseError as exc:
            raise RunpackError(f"could not read runpack: {exc}") from exc

    def copy_snapshot_to(self, writer: RunpackWriter) -> None:
        """Copy this validated snapshot into a writable runpack."""
        _require_writable_schema(self._schema_version)
        self.execution()
        try:
            self._connection.backup(writer._connection)
            writer._connection.execute("PRAGMA foreign_keys = ON")
        except sqlite3.Error as exc:
            raise RunpackError(f"could not copy runpack snapshot: {exc}") from exc

    def execution(self) -> Execution:
        rows = self._execute("SELECT * FROM executions LIMIT 2").fetchall()
        if not rows:
            raise RunpackError("expected exactly one execution, found 0")
        if len(rows) != 1:
            raise RunpackError("expected exactly one execution, found more than one")
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
            WHERE event.kind NOT IN (
                'log.record', 'python.call.aggregate', 'python.stack.sample', 'python.callsite'
            )
            GROUP BY entity_kind, entity_name, event_kind, event_name
            """
        ).fetchall()
        return {_operation_identity(row): int(row["event_count"]) for row in rows}

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
            WHERE event.kind NOT IN (
                'log.record', 'python.call.aggregate', 'python.stack.sample', 'python.callsite'
            )
            """
        ).fetchall()
        counts: dict[tuple[str, str, str, str], int] = {}
        for row in rows:
            key = _operation_identity(row)
            attributes = _object(row["attributes_json"])
            if not is_operation_error(attributes):
                continue
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
        return {
            (
                _required_text(row["kind"], "entity kind"),
                _required_text(row["name"], "entity name"),
            ): int(row["entity_count"])
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
            WHERE event.kind NOT IN (
                'log.record', 'python.call.aggregate', 'python.stack.sample', 'python.callsite'
            )
            GROUP BY entity_kind, entity_name, event_kind, event_name
            HAVING count(*) = count(event.started_at_ns)
               AND count(*) = count(event.finished_at_ns)
            """
        ).fetchall()
        return {_operation_identity(row): float(row["duration_seconds"]) for row in rows}

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
                WHERE event.kind NOT IN (
                    'log.record', 'python.call.aggregate', 'python.stack.sample', 'python.callsite'
                )
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
        return {_operation_identity(row): int(row["max_concurrency"]) for row in rows}

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
            WHERE source_event.kind NOT IN (
                    'log.record', 'python.call.aggregate', 'python.stack.sample', 'python.callsite'
                  )
              AND target_event.kind NOT IN (
                    'log.record', 'python.call.aggregate', 'python.stack.sample', 'python.callsite'
                  )
              AND source_event.entity_id IS NOT target_event.entity_id
            GROUP BY source_kind, source_name, target_kind, target_name, edge_kind
            """
        ).fetchall()
        return {
            (
                _required_text(row["source_kind"], "edge source entity kind"),
                _required_text(row["source_name"], "edge source entity name"),
                _required_text(row["target_kind"], "edge target entity kind"),
                _required_text(row["target_name"], "edge target entity name"),
                _required_text(row["edge_kind"], "edge kind"),
            ): int(row["edge_count"])
            for row in rows
        }

    def peer_service_edge_counts(self) -> dict[tuple[str, str, str, str, str], int]:
        explicit_calls = {
            (
                _required_text(row["source_event_id"], "edge source event id"),
                _required_text(row["target_kind"], "edge target entity kind"),
                _required_text(row["target_name"], "edge target entity name"),
            )
            for row in self._execute(
                """
                SELECT
                    edge.source_event_id,
                    COALESCE(target_entity.kind, 'unowned') AS target_kind,
                    COALESCE(target_entity.name, 'unowned') AS target_name
                FROM causal_edges AS edge
                JOIN events AS source_event ON source_event.id = edge.source_event_id
                JOIN events AS target_event ON target_event.id = edge.target_event_id
                LEFT JOIN entities AS target_entity ON target_entity.id = target_event.entity_id
                WHERE edge.kind = 'calls'
                  AND source_event.kind != 'log.record'
                  AND target_event.kind != 'log.record'
                  AND source_event.entity_id IS NOT target_event.entity_id
                """
            ).fetchall()
        }
        rows = self._execute(
            """
            SELECT
                event.id AS event_id,
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
            source_kind = _required_text(row["source_kind"], "edge source entity kind")
            source_name = _required_text(row["source_name"], "edge source entity name")
            peer = _object(row["attributes_json"]).get("peer.service")
            if not isinstance(peer, str) or not peer:
                continue
            event_id = _required_text(row["event_id"], "peer service event id")
            if (event_id, "service", peer) in explicit_calls:
                continue
            key = (source_kind, source_name, "service", peer, "calls")
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
        maximum_bytes, total_bytes = self._execute(
            "SELECT COALESCE(max(length(CAST(content AS BLOB))), 0), "
            "COALESCE(sum(length(CAST(content AS BLOB))), 0) FROM attachments"
        ).fetchone()
        if maximum_bytes > MAX_RUNPACK_ATTACHMENT_BYTES:
            raise RunpackError(
                "attachment content exceeds the "
                f"{MAX_RUNPACK_ATTACHMENT_BYTES}-byte runpack field limit"
            )
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
            table: int(self._execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in ("entities", "events", "causal_edges", "measurements")
        }
        attachment_count = int(
            self._execute(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'attachments'"
            ).fetchone()[0]
        )
        counts["attachments"] = (
            int(self._execute("SELECT count(*) FROM attachments").fetchone()[0])
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
