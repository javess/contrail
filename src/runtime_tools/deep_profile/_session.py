"""Bounded normalization for zero-touch Python profiling modes."""

from __future__ import annotations

import os
import socket
import stat
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Never

from runtime_tools.deep_profile._common import (
    _BOOTSTRAP_NAME,
    _MAX_INTEGER,
    _PROFILE_SUPPORT_BOOTSTRAP_NAME,
    _SEMANTIC_BOOTSTRAP_NAME,
    _SOCKET_PATH_LIMIT,
    DEEP_PROFILE_DIRECTORY_ENV,
    MAX_DEEP_PROFILE_DIRECTORY_ENTRIES,
    MAX_DEEP_PROFILE_FILE_BYTES,
    MAX_PROFILE_SNAPSHOT_MESSAGES,
    MAX_PROFILE_SNAPSHOT_SERIALIZATION_NS,
    PROFILE_PUBLICATION_METRICS_VERSION,
    PROFILE_SESSION_CHECKPOINT_VERSION,
    PROFILE_SNAPSHOT_SOCKET_ENV,
    SAMPLE_PROFILE_DIRECTORY_ENV,
    DeepProfileError,
    PythonProfileMode,
    _write_all,
)
from runtime_tools.deep_profile._ranking import _ProfileSnapshotCollector
from runtime_tools.model import JsonValue


@dataclass(frozen=True, slots=True)
class _SnapshotTransportStats:
    dropped_profile_process_count: int
    dropped_profile_process_count_truncated: bool
    metrics_status: Literal["available", "unavailable"]
    message_count: int
    payload_bytes: int
    max_payload_bytes: int
    serialization_ns: int
    max_serialization_ns: int
    checkpoint_message_count: int
    checkpoint_payload_bytes: int
    max_checkpoint_payload_bytes: int
    checkpoint_serialization_ns: int
    max_checkpoint_serialization_ns: int


@dataclass(frozen=True, slots=True)
class _PublicationMetrics:
    status: Literal["available", "unavailable", "invalid"]
    fallback_process_ids: tuple[int, ...]
    socket_attempted_process_count: int
    socket_failure_ns: int
    max_socket_failure_ns: int


@dataclass(slots=True)
class _PublicationMetricsAccumulator:
    saw_available: bool = False
    saw_unavailable: bool = False
    invalid: bool = False
    fallback_process_ids: set[int] = field(default_factory=set)
    socket_attempted_process_count: int = 0
    socket_failure_ns: int = 0
    max_socket_failure_ns: int = 0

    def add(
        self,
        document: dict[str, object],
        *,
        pid: int,
        snapshot_kind: Literal["checkpoint", "final"],
        registration_only: bool,
    ) -> None:
        version = document.get("publication_metrics_version")
        if version is None:
            self.saw_unavailable = True
            return
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version != PROFILE_PUBLICATION_METRICS_VERSION
        ):
            self.invalid = True
            return
        self.saw_available = True
        raw_fallback = document.get("publication_fallback")
        if raw_fallback is None:
            return
        if not isinstance(raw_fallback, dict):
            self.invalid = True
            return
        socket_attempted = raw_fallback.get("socket_attempted")
        socket_failure_ns = raw_fallback.get("socket_failure_ns")
        raw_fallback_kind = raw_fallback.get("snapshot_kind")
        expected_kind = "registration" if registration_only else snapshot_kind
        if (
            not isinstance(socket_attempted, bool)
            or not isinstance(socket_failure_ns, int)
            or isinstance(socket_failure_ns, bool)
            or not 0 <= socket_failure_ns <= _MAX_INTEGER
            or raw_fallback_kind != expected_kind
            or (not socket_attempted and socket_failure_ns != 0)
        ):
            self.invalid = True
            return
        self.fallback_process_ids.add(pid)
        if socket_attempted:
            self.socket_attempted_process_count += 1
            if self.socket_failure_ns > _MAX_INTEGER - socket_failure_ns:
                self.invalid = True
                return
            self.socket_failure_ns += socket_failure_ns
            self.max_socket_failure_ns = max(self.max_socket_failure_ns, socket_failure_ns)

    def result(self) -> _PublicationMetrics:
        if self.invalid or (self.saw_available and self.saw_unavailable):
            return _PublicationMetrics("invalid", (), 0, 0, 0)
        if not self.saw_available:
            return _PublicationMetrics("unavailable", (), 0, 0, 0)
        return _PublicationMetrics(
            "available",
            tuple(sorted(self.fallback_process_ids)),
            self.socket_attempted_process_count,
            self.socket_failure_ns,
            self.max_socket_failure_ns,
        )


@dataclass(slots=True)
class DeepProfileSession:
    directory: Path
    identity: tuple[int, int]
    mode: PythonProfileMode = "deep"
    collector: _ProfileSnapshotCollector | None = None
    recovered_transport_stats: _SnapshotTransportStats | None = None
    recovered_collector_error_count: int = 0

    def configure_environment(self, environment: dict[str, str]) -> None:
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            str(self.directory)
            if not existing_pythonpath
            else f"{self.directory}{os.pathsep}{existing_pythonpath}"
        )
        directory_environment = (
            DEEP_PROFILE_DIRECTORY_ENV if self.mode == "deep" else SAMPLE_PROFILE_DIRECTORY_ENV
        )
        environment[directory_environment] = str(self.directory)
        environment.pop(PROFILE_SNAPSHOT_SOCKET_ENV, None)
        if self.collector is not None:
            environment[PROFILE_SNAPSHOT_SOCKET_ENV] = str(self.collector.socket_path)

    @property
    def transport(self) -> str:
        return (
            "controller-unix-socket"
            if self.snapshot_metrics_status == "available"
            else "workload-file"
        )

    @property
    def collector_error_count(self) -> int:
        if self.collector is not None:
            return self.collector.error_count
        return self.recovered_collector_error_count

    @property
    def snapshot_metrics_status(self) -> Literal["available", "unavailable"]:
        if self.collector is not None:
            return "available"
        if self.recovered_transport_stats is not None:
            return self.recovered_transport_stats.metrics_status
        return "unavailable"

    def finish_collection(self) -> None:
        if self.collector is not None:
            self.collector.stop()

    def close(self) -> None:
        self.finish_collection()
        try:
            status = self.directory.stat(follow_symlinks=False)
        except OSError:
            return
        if not stat.S_ISDIR(status.st_mode) or (status.st_dev, status.st_ino) != self.identity:
            return
        try:
            entries = tuple(self.directory.iterdir())
        except OSError:
            return
        for entry in entries:
            try:
                entry_status = entry.stat(follow_symlinks=False)
                if stat.S_ISREG(entry_status.st_mode) or stat.S_ISLNK(entry_status.st_mode):
                    entry.unlink()
                elif entry.name in {"__pycache__", _SEMANTIC_BOOTSTRAP_NAME} and stat.S_ISDIR(
                    entry_status.st_mode
                ):
                    _remove_private_directory(entry, depth=2)
            except OSError:
                pass
        try:
            self.directory.rmdir()
        except OSError:
            pass


def _remove_private_directory(directory: Path, *, depth: int) -> None:
    if depth < 0:
        return
    try:
        entries = tuple(directory.iterdir())
    except OSError:
        return
    for entry in entries:
        try:
            status = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(status.st_mode):
                _remove_private_directory(entry, depth=depth - 1)
            else:
                entry.unlink()
        except OSError:
            pass
    try:
        directory.rmdir()
    except OSError:
        pass


def _profile_transport(session: DeepProfileSession, metrics: _PublicationMetrics) -> str:
    if session.snapshot_metrics_status == "unavailable":
        return "workload-file"
    if metrics.status == "available" and metrics.fallback_process_ids:
        return "mixed"
    return "controller-unix-socket"


def _snapshot_transport_stats(
    session: DeepProfileSession,
    dropped_file_count: int,
) -> _SnapshotTransportStats:
    collector = session.collector
    if collector is None:
        recovered = session.recovered_transport_stats
        if recovered is not None:
            return _SnapshotTransportStats(
                recovered.dropped_profile_process_count + dropped_file_count,
                recovered.dropped_profile_process_count_truncated,
                recovered.metrics_status,
                recovered.message_count,
                recovered.payload_bytes,
                recovered.max_payload_bytes,
                recovered.serialization_ns,
                recovered.max_serialization_ns,
                recovered.checkpoint_message_count,
                recovered.checkpoint_payload_bytes,
                recovered.max_checkpoint_payload_bytes,
                recovered.checkpoint_serialization_ns,
                recovered.max_checkpoint_serialization_ns,
            )
        return _SnapshotTransportStats(
            dropped_file_count,
            False,
            "unavailable",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        )
    return _SnapshotTransportStats(
        dropped_file_count + len(collector.dropped_process_ids),
        collector.dropped_process_count_truncated,
        "available",
        collector.message_count,
        collector.total_payload_bytes,
        collector.max_payload_bytes,
        collector.total_serialization_ns,
        collector.max_serialization_ns,
        collector.checkpoint_message_count,
        collector.checkpoint_payload_bytes,
        collector.max_checkpoint_payload_bytes,
        collector.checkpoint_serialization_ns,
        collector.max_checkpoint_serialization_ns,
    )


def _prepare_snapshot_collector(directory: Path) -> _ProfileSnapshotCollector | None:
    socket_path = Path(tempfile.gettempdir()) / f"contrail-profile-{uuid.uuid4().hex[:16]}.sock"
    if len(os.fsencode(socket_path)) > _SOCKET_PATH_LIMIT:
        return None
    listener: socket.socket | None = None
    try:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        os.chmod(socket_path, 0o600)
        listener.listen(64)
        collector = _ProfileSnapshotCollector(
            directory,
            socket_path,
            listener,
            threading.Event(),
        )
        collector.start()
        return collector
    except (OSError, RuntimeError):
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        try:
            socket_path.unlink()
        except OSError:
            pass
        return None


def _prepare_profile_session(parent: Path, mode: PythonProfileMode) -> DeepProfileSession:
    package_root = Path(__file__).parent.parent
    source = package_root / (
        "_deep_profile_bootstrap.py" if mode == "deep" else "_sampling_profile_bootstrap.py"
    )
    semantic_source = package_root / _SEMANTIC_BOOTSTRAP_NAME
    support_source = package_root / _PROFILE_SUPPORT_BOOTSTRAP_NAME
    try:
        bootstrap = source.read_bytes()
        semantic_bootstraps = tuple(
            (path.name, path.read_bytes()) for path in sorted(semantic_source.glob("*.py"))
        )
        if not semantic_bootstraps:
            raise OSError("semantic bootstrap package is empty")
        support_bootstrap = support_source.read_bytes()
    except OSError as exc:
        raise DeepProfileError(f"could not read the packaged {mode} capture bootstrap") from exc
    directory = parent / f".contrail-{mode}-profile-{uuid.uuid4().hex}"
    try:
        directory.mkdir(mode=0o700)
        status = directory.stat(follow_symlinks=False)
        for name, content in (
            (_BOOTSTRAP_NAME, bootstrap),
            (_PROFILE_SUPPORT_BOOTSTRAP_NAME, support_bootstrap),
        ):
            descriptor = os.open(
                directory / name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                _write_all(descriptor, content)
            finally:
                os.close(descriptor)
        semantic_directory = directory / _SEMANTIC_BOOTSTRAP_NAME
        semantic_directory.mkdir(mode=0o700)
        for name, content in semantic_bootstraps:
            descriptor = os.open(
                semantic_directory / name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                _write_all(descriptor, content)
            finally:
                os.close(descriptor)
    except OSError as exc:
        for name in (
            _BOOTSTRAP_NAME,
            _PROFILE_SUPPORT_BOOTSTRAP_NAME,
        ):
            try:
                (directory / name).unlink(missing_ok=True)
            except OSError:
                pass
        _remove_private_directory(directory / _SEMANTIC_BOOTSTRAP_NAME, depth=1)
        try:
            directory.rmdir()
        except OSError:
            pass
        raise DeepProfileError(f"could not prepare the private {mode}-profile bootstrap") from exc
    collector = _prepare_snapshot_collector(directory)
    return DeepProfileSession(directory, (status.st_dev, status.st_ino), mode, collector)


def prepare_deep_profile_session(parent: Path) -> DeepProfileSession:
    return _prepare_profile_session(parent, "deep")


def prepare_sample_profile_session(parent: Path) -> DeepProfileSession:
    return _prepare_profile_session(parent, "sample")


def _reject_json_constant(value: str) -> Never:
    raise ValueError(f"non-finite constant {value}")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise DeepProfileError(f"{label} must be an object")
    return {str(key): item for key, item in value.items()}


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise DeepProfileError(f"{label} must be an array")
    return list(value)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DeepProfileError(f"{label} must be a non-empty string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise DeepProfileError(f"{label} must be valid UTF-8") from exc
    if len(encoded) > 4_096:
        raise DeepProfileError(f"{label} exceeds the 4096-byte limit")
    return value


def _integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _MAX_INTEGER:
        raise DeepProfileError(f"{label} must be a bounded non-negative integer")
    return value


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise DeepProfileError(f"{label} must be a boolean")
    return value


def _optional_boolean(value: object, label: str) -> bool | None:
    if value is None:
        return None
    return _boolean(value, label)


def profile_session_checkpoint(session: DeepProfileSession) -> dict[str, JsonValue]:
    """Freeze and describe one profile session for post-exit recovery."""
    session.finish_collection()
    try:
        directory = session.directory.resolve(strict=True)
        status = directory.stat(follow_symlinks=False)
    except (OSError, RuntimeError) as exc:
        raise DeepProfileError("could not checkpoint the private profile session") from exc
    if not stat.S_ISDIR(status.st_mode) or (status.st_dev, status.st_ino) != session.identity:
        raise DeepProfileError("private profile session changed before checkpointing")
    transport = _snapshot_transport_stats(session, 0)
    return {
        "format_version": PROFILE_SESSION_CHECKPOINT_VERSION,
        "directory": str(directory),
        "directory_device": status.st_dev,
        "directory_inode": status.st_ino,
        "mode": session.mode,
        "collector_error_count": session.collector_error_count,
        "transport": {
            "dropped_profile_process_count": transport.dropped_profile_process_count,
            "dropped_profile_process_count_truncated": (
                transport.dropped_profile_process_count_truncated
            ),
            "metrics_status": transport.metrics_status,
            "message_count": transport.message_count,
            "payload_bytes": transport.payload_bytes,
            "max_payload_bytes": transport.max_payload_bytes,
            "serialization_ns": transport.serialization_ns,
            "max_serialization_ns": transport.max_serialization_ns,
            "checkpoint_message_count": transport.checkpoint_message_count,
            "checkpoint_payload_bytes": transport.checkpoint_payload_bytes,
            "max_checkpoint_payload_bytes": transport.max_checkpoint_payload_bytes,
            "checkpoint_serialization_ns": transport.checkpoint_serialization_ns,
            "max_checkpoint_serialization_ns": transport.max_checkpoint_serialization_ns,
        },
    }


def recover_profile_session(value: object) -> DeepProfileSession:
    """Validate and reopen one frozen profile session without a live collector."""
    checkpoint = _object(value, "profile session checkpoint")
    if checkpoint.get("format_version") != PROFILE_SESSION_CHECKPOINT_VERSION:
        raise DeepProfileError("profile session checkpoint version is unsupported")
    raw_mode = checkpoint.get("mode")
    if raw_mode == "sample":
        mode: PythonProfileMode = "sample"
    elif raw_mode == "deep":
        mode = "deep"
    else:
        raise DeepProfileError("profile session checkpoint mode is unsupported")
    raw_directory = _text(checkpoint.get("directory"), "profile session checkpoint directory")
    directory = Path(raw_directory)
    if not directory.is_absolute():
        raise DeepProfileError("profile session checkpoint directory must be absolute")
    device = _integer(
        checkpoint.get("directory_device"),
        "profile session checkpoint directory device",
    )
    inode = _integer(
        checkpoint.get("directory_inode"),
        "profile session checkpoint directory inode",
    )
    try:
        status = directory.stat(follow_symlinks=False)
    except OSError as exc:
        raise DeepProfileError("profile session checkpoint directory is unavailable") from exc
    if not stat.S_ISDIR(status.st_mode) or (status.st_dev, status.st_ino) != (device, inode):
        raise DeepProfileError("profile session checkpoint directory identity changed")

    collector_error_count = _integer(
        checkpoint.get("collector_error_count"),
        "profile session checkpoint collector error count",
    )
    raw_transport = _object(
        checkpoint.get("transport"),
        "profile session checkpoint transport",
    )
    raw_metrics_status = raw_transport.get("metrics_status")
    if raw_metrics_status == "available":
        metrics_status: Literal["available", "unavailable"] = "available"
    elif raw_metrics_status == "unavailable":
        metrics_status = "unavailable"
    else:
        raise DeepProfileError("profile session checkpoint transport status is unsupported")
    dropped_count = _integer(
        raw_transport.get("dropped_profile_process_count"),
        "profile session checkpoint dropped process count",
    )
    dropped_truncated = _boolean(
        raw_transport.get("dropped_profile_process_count_truncated"),
        "profile session checkpoint dropped process truncation",
    )
    integer_fields = (
        "message_count",
        "payload_bytes",
        "max_payload_bytes",
        "serialization_ns",
        "max_serialization_ns",
        "checkpoint_message_count",
        "checkpoint_payload_bytes",
        "max_checkpoint_payload_bytes",
        "checkpoint_serialization_ns",
        "max_checkpoint_serialization_ns",
    )
    counts = {
        field: _integer(
            raw_transport.get(field),
            f"profile session checkpoint {field.replace('_', ' ')}",
        )
        for field in integer_fields
    }
    if (
        dropped_count > MAX_DEEP_PROFILE_DIRECTORY_ENTRIES
        or counts["message_count"] > MAX_PROFILE_SNAPSHOT_MESSAGES
        or counts["max_payload_bytes"] > MAX_DEEP_PROFILE_FILE_BYTES
        or counts["max_payload_bytes"] > counts["payload_bytes"]
        or counts["max_serialization_ns"] > MAX_PROFILE_SNAPSHOT_SERIALIZATION_NS
        or counts["max_serialization_ns"] > counts["serialization_ns"]
        or counts["checkpoint_message_count"] > counts["message_count"]
        or counts["checkpoint_payload_bytes"] > counts["payload_bytes"]
        or counts["max_checkpoint_payload_bytes"] > counts["checkpoint_payload_bytes"]
        or counts["max_checkpoint_payload_bytes"] > counts["max_payload_bytes"]
        or counts["checkpoint_serialization_ns"] > counts["serialization_ns"]
        or counts["max_checkpoint_serialization_ns"] > counts["checkpoint_serialization_ns"]
        or counts["max_checkpoint_serialization_ns"] > counts["max_serialization_ns"]
    ):
        raise DeepProfileError("profile session checkpoint transport metrics are inconsistent")
    if metrics_status == "unavailable" and (
        collector_error_count != 0
        or dropped_count != 0
        or dropped_truncated
        or any(counts.values())
    ):
        raise DeepProfileError("unavailable profile session checkpoint has transport metrics")
    transport = _SnapshotTransportStats(
        dropped_count,
        dropped_truncated,
        metrics_status,
        counts["message_count"],
        counts["payload_bytes"],
        counts["max_payload_bytes"],
        counts["serialization_ns"],
        counts["max_serialization_ns"],
        counts["checkpoint_message_count"],
        counts["checkpoint_payload_bytes"],
        counts["max_checkpoint_payload_bytes"],
        counts["checkpoint_serialization_ns"],
        counts["max_checkpoint_serialization_ns"],
    )
    return DeepProfileSession(
        directory,
        (device, inode),
        mode,
        recovered_transport_stats=transport,
        recovered_collector_error_count=collector_error_count,
    )
