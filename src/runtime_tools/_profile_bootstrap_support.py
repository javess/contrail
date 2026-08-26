"""Standalone transport shared by the copied Python profile bootstraps."""

from __future__ import annotations

import json
import os
import socket
import struct
import sys
import time
from collections.abc import Callable
from types import CodeType, FrameType

SNAPSHOT_PROTOCOL_MAGIC = b"CTRP0002"
SNAPSHOT_HEADER = struct.Struct("!8sQQQQ")
SNAPSHOT_KIND_REGISTRATION = 1
SNAPSHOT_KIND_CHECKPOINT = 2
SNAPSHOT_KIND_FINAL = 3
SNAPSHOT_SOCKET_TIMEOUT_SECONDS = 0.1
FINAL_SNAPSHOT_SOCKET_TIMEOUT_SECONDS = 1.0
PUBLICATION_METRICS_VERSION = 1
MAX_PUBLICATION_FALLBACK_BYTES = 256

type FunctionKey = tuple[str, str, str, int, str]


class FunctionCatalog:
    """Create bounded, cached identities for sampled or profiled Python frames."""

    __slots__ = ("_code_keys", "_max_functions", "_max_text_characters", "_runtime_prefixes")

    def __init__(self, max_functions: int, max_text_characters: int) -> None:
        self._max_functions = max_functions
        self._max_text_characters = max_text_characters
        self._code_keys: dict[CodeType, FunctionKey] = {}
        self._runtime_prefixes = tuple(
            dict.fromkeys(
                os.path.realpath(prefix)
                for prefix in (
                    sys.base_prefix,
                    sys.prefix,
                    *(
                        entry
                        for entry in sys.path
                        if isinstance(entry, str)
                        and entry
                        and any(
                            component in {"site-packages", "dist-packages"}
                            for component in os.path.normpath(entry).split(os.sep)
                        )
                    ),
                )
                if isinstance(prefix, str) and prefix
            )
        )

    def clear(self) -> None:
        self._code_keys.clear()

    def bounded_text(self, value: object, fallback: str) -> str:
        text = value if isinstance(value, str) and value else fallback
        return text.encode("utf-8", "backslashreplace").decode("utf-8")[: self._max_text_characters]

    def key(self, frame: FrameType) -> FunctionKey:
        code = frame.f_code
        existing = self._code_keys.get(code)
        if existing is not None:
            return existing
        filename = self.bounded_text(code.co_filename, "<unknown>")
        key = (
            self.bounded_text(frame.f_globals.get("__name__"), "<unknown>"),
            self.bounded_text(code.co_qualname, code.co_name or "<unknown>"),
            filename,
            max(0, code.co_firstlineno),
            self._scope(filename),
        )
        if len(self._code_keys) < self._max_functions:
            self._code_keys[code] = key
        return key

    def _scope(self, filename: str) -> str:
        if filename.startswith("<frozen "):
            return "runtime"
        if filename.startswith("<"):
            return "application"
        resolved = os.path.realpath(filename)
        for prefix in self._runtime_prefixes:
            try:
                if os.path.commonpath((resolved, prefix)) == prefix:
                    return "library"
            except ValueError:
                continue
        return "application"


class ReportPublisher[SuppressionToken]:
    """Publish one bounded profile snapshot over a socket with file fallback."""

    __slots__ = (
        "_begin_network_suppression",
        "_directory_env",
        "_end_network_suppression",
        "_max_report_bytes",
        "_socket_env",
    )

    def __init__(
        self,
        directory_env: str,
        socket_env: str,
        max_report_bytes: int,
        begin_network_suppression: Callable[[], SuppressionToken],
        end_network_suppression: Callable[[SuppressionToken], None],
    ) -> None:
        self._directory_env = directory_env
        self._socket_env = socket_env
        self._max_report_bytes = max_report_bytes
        self._begin_network_suppression = begin_network_suppression
        self._end_network_suppression = end_network_suppression

    def publish(
        self,
        encoded: bytes,
        serialization_ns: int,
        snapshot_kind: str,
        *,
        registration_only: bool = False,
    ) -> bool:
        """Publish a report and return whether either transport accepted it."""
        transport_kind = (
            SNAPSHOT_KIND_REGISTRATION
            if registration_only
            else SNAPSHOT_KIND_CHECKPOINT
            if snapshot_kind == "checkpoint"
            else SNAPSHOT_KIND_FINAL
        )
        socket_path = os.environ.get(self._socket_env)
        socket_attempted = bool(socket_path and os.path.isabs(socket_path))
        socket_started_at_ns = time.perf_counter_ns()
        sent = self.send_to_collector(encoded, serialization_ns, transport_kind)
        if sent:
            return True
        socket_failure_ns = (
            max(0, time.perf_counter_ns() - socket_started_at_ns) if socket_attempted else 0
        )
        return self._write_file(
            self._fallback_report(
                encoded,
                socket_attempted=socket_attempted,
                socket_failure_ns=socket_failure_ns,
                snapshot_kind=transport_kind,
            )
        )

    def _fallback_report(
        self,
        encoded: bytes,
        *,
        socket_attempted: bool,
        socket_failure_ns: int,
        snapshot_kind: int,
    ) -> bytes:
        kind = {
            SNAPSHOT_KIND_REGISTRATION: "registration",
            SNAPSHOT_KIND_CHECKPOINT: "checkpoint",
            SNAPSHOT_KIND_FINAL: "final",
        }[snapshot_kind]
        fallback = json.dumps(
            {
                "socket_attempted": socket_attempted,
                "socket_failure_ns": socket_failure_ns,
                "snapshot_kind": kind,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if not encoded.endswith(b"}"):
            raise ValueError("profile report must be a JSON object")
        result = encoded[:-1] + b',"publication_fallback":' + fallback + b"}"
        if len(result) > self._max_report_bytes:
            raise ValueError("profile fallback report exceeds its byte limit")
        return result

    def send_to_collector(
        self,
        encoded: bytes,
        serialization_ns: int,
        snapshot_kind: int,
    ) -> bool:
        socket_path = os.environ.get(self._socket_env)
        if not socket_path or not os.path.isabs(socket_path):
            return False
        suppression_token = self._begin_network_suppression()
        try:
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                    connection.settimeout(
                        FINAL_SNAPSHOT_SOCKET_TIMEOUT_SECONDS
                        if snapshot_kind == SNAPSHOT_KIND_FINAL
                        else SNAPSHOT_SOCKET_TIMEOUT_SECONDS
                    )
                    connection.connect(socket_path)
                    connection.sendall(
                        SNAPSHOT_HEADER.pack(
                            SNAPSHOT_PROTOCOL_MAGIC,
                            os.getpid(),
                            len(encoded),
                            serialization_ns,
                            snapshot_kind,
                        )
                    )
                    connection.sendall(encoded)
                return True
            except OSError:
                return False
        finally:
            self._end_network_suppression(suppression_token)

    def _write_file(self, encoded: bytes) -> bool:
        directory = os.environ.get(self._directory_env)
        if not directory or not os.path.isabs(directory):
            return False
        temporary = os.path.join(directory, f".profile-{os.getpid()}.tmp")
        destination = os.path.join(directory, f"profile-{os.getpid()}.json")
        descriptor: int | None = None
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    return False
                view = view[written:]
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, destination)
            return True
        except OSError:
            return False
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            try:
                os.unlink(temporary)
            except OSError:
                pass
