"""Transactional writer for versioned runpack artifacts."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType

from runtime_tools._version import __version__
from runtime_tools.artifacts import remove_best_effort
from runtime_tools.model import (
    Attachment,
    CausalEdge,
    Entity,
    Event,
    Execution,
    JsonValue,
    Measurement,
)
from runtime_tools.storage._validation import (
    _SCHEMA,
    APPLICATION_ID,
    MAX_RUNPACK_ATTACHMENT_BYTES,
    MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES,
    SCHEMA_VERSION,
    RunpackError,
    _attachment_values,
    _command_json,
    _edge_values,
    _entity_values,
    _event_values,
    _execution_interval,
    _integer_value,
    _json,
    _measurement_values,
    _parent_first_entities,
    _require_runpack_file_size,
    _require_writable_schema,
    _set_runpack_connection_limits,
    _text_value,
    _validate_connection,
)


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
            raise RunpackError(f"could not create runpack {path}: {exc}") from exc
        try:
            connection = sqlite3.connect(path)
            self._connection = connection
            _set_runpack_connection_limits(self._connection)
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
            remove_best_effort(path)
            raise RunpackError(f"could not create runpack {path}: {exc}") from exc
        except BaseException:
            if connection is not None:
                connection.close()
            remove_best_effort(path)
            raise

    @classmethod
    def open_existing(cls, path: Path) -> RunpackWriter:
        """Open a validated runpack for adapter enrichment."""
        if not path.is_file():
            raise RunpackError(f"runpack does not exist: {path}")
        try:
            _require_runpack_file_size(path.stat().st_size)
        except OSError as exc:
            raise RunpackError(f"could not inspect runpack size: {path}") from exc
        writer = cls.__new__(cls)
        writer.path = path
        try:
            writer._connection = sqlite3.connect(path)
            _set_runpack_connection_limits(writer._connection)
            writer._connection.execute("PRAGMA foreign_keys = ON")
            schema_version = _validate_connection(writer._connection)
            _require_writable_schema(schema_version)
        except sqlite3.DatabaseError as exc:
            if hasattr(writer, "_connection"):
                writer._connection.close()
            raise RunpackError(f"invalid runpack: {path}") from exc
        except RunpackError:
            if hasattr(writer, "_connection"):
                writer._connection.close()
            raise
        except BaseException:
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

    def add_event_graph_and_set_execution_metadata(
        self,
        execution_id: str,
        events: Iterable[Event],
        edges: Iterable[CausalEdge],
        metadata: dict[str, JsonValue],
    ) -> None:
        """Add one derived graph and its execution metadata atomically."""
        normalized_execution_id = _text_value(execution_id, "execution id")
        encoded_metadata = _json(metadata)
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
            cursor = self._connection.execute(
                "UPDATE executions SET metadata_json = ? WHERE id = ?",
                (encoded_metadata, normalized_execution_id),
            )
            if cursor.rowcount != 1:
                raise RunpackError(f"execution does not exist: {normalized_execution_id}")

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
        values: list[tuple[object, ...]] = []
        added_bytes = 0
        for attachment in attachments:
            value = _attachment_values(attachment)
            content = value[4]
            assert isinstance(content, bytes)
            added_bytes += len(content)
            if added_bytes > MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES:
                raise RunpackError(
                    "attachment content exceeds the "
                    f"{MAX_RUNPACK_ATTACHMENT_TOTAL_BYTES}-byte aggregate runpack limit"
                )
            values.append(value)
        with self._writing(), self._connection:
            maximum_bytes, existing_bytes = self._connection.execute(
                "SELECT COALESCE(max(length(CAST(content AS BLOB))), 0), "
                "COALESCE(sum(length(CAST(content AS BLOB))), 0) FROM attachments"
            ).fetchone()
            if maximum_bytes > MAX_RUNPACK_ATTACHMENT_BYTES:
                raise RunpackError(
                    "attachment content exceeds the "
                    f"{MAX_RUNPACK_ATTACHMENT_BYTES}-byte runpack field limit"
                )
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
        with self._writing(), self._connection:
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
            "SELECT started_at_ns, finished_at_ns FROM executions WHERE id = ?",
            (normalized_execution_id,),
        ).fetchone()
        if row is None:
            raise RunpackError(f"execution does not exist: {normalized_execution_id}")
        if row[1] is not None:
            raise RunpackError(f"execution is already finished: {normalized_execution_id}")
        _, normalized_finish = _execution_interval(row[0], finished_at_ns)
        normalized_exit_code = _integer_value(exit_code, "execution exit code")
        with self._writing(), self._connection:
            cursor = self._connection.execute(
                """
                UPDATE executions
                SET finished_at_ns = ?, exit_code = ?, metadata_json = ?
                WHERE id = ? AND finished_at_ns IS NULL
                """,
                (normalized_finish, normalized_exit_code, _json(metadata), normalized_execution_id),
            )
            if cursor.rowcount != 1:
                raise RunpackError(f"execution is already finished: {normalized_execution_id}")
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
