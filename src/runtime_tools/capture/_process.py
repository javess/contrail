"""Output relay, process lifecycle, and environment identity helpers."""

from __future__ import annotations

import hashlib
import os
import resource
import select
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from typing import BinaryIO

from runtime_tools.capture._common import (
    _IDENTIFIED_ENVIRONMENT_VARIABLES,
    _STATUS_POLL_EVENT,
    MAX_CUSTOM_ENVIRONMENT_IDENTITIES,
    MAX_ENVIRONMENT_NAME_BYTES,
    MAX_POST_EXIT_DRAIN_BYTES,
    PROCESS_TERMINATION_TIMEOUT_SECONDS,
    CaptureError,
    OutputDigest,
)
from runtime_tools.model import (
    JsonValue,
)
from runtime_tools.terminal import terminal_text


def _git_revision(cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        )
    except (OSError, UnicodeError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    revision = result.stdout.strip()
    if len(revision) not in (40, 64) or any(
        character not in "0123456789abcdefABCDEF" for character in revision
    ):
        return None
    return revision.lower()


def _pump(
    source: BinaryIO,
    sink: BinaryIO | None,
    capture_limit: int | None,
    process_done: threading.Event,
) -> OutputDigest:
    digest = hashlib.sha256()
    byte_count = 0
    captured = bytearray() if capture_limit is not None else None
    relay_error: str | None = None
    pipe_open_after_exit = False
    post_exit_bytes = 0
    descriptor = source.fileno()
    os.set_blocking(descriptor, False)

    def read(max_bytes: int) -> bytes:
        while True:
            try:
                return os.read(descriptor, max_bytes)
            except InterruptedError:
                continue

    def consume(chunk: bytes) -> None:
        nonlocal byte_count, relay_error, sink
        digest.update(chunk)
        byte_count += len(chunk)
        if captured is not None:
            assert capture_limit is not None
            if len(captured) < capture_limit:
                captured.extend(chunk[: capture_limit - len(captured)])
        if sink is not None:
            try:
                remaining = memoryview(chunk)
                while remaining:
                    written = sink.write(remaining)
                    if written is None or written <= 0 or written > len(remaining):
                        raise OSError("output relay did not accept the complete chunk")
                    remaining = remaining[written:]
                sink.flush()
            except Exception as exc:
                relay_error = terminal_text(f"{type(exc).__name__}: {exc}")
                sink = None

    while True:
        if process_done.is_set():
            try:
                chunk = read(64 * 1024)
            except BlockingIOError:
                pipe_open_after_exit = True
                break
            if not chunk:
                break
            consume(chunk)
            post_exit_bytes += len(chunk)
            if post_exit_bytes >= MAX_POST_EXIT_DRAIN_BYTES:
                try:
                    continuation = read(1)
                except BlockingIOError:
                    pipe_open_after_exit = True
                else:
                    if continuation:
                        consume(continuation)
                        pipe_open_after_exit = True
                break
            continue
        try:
            readable, _, _ = select.select((descriptor,), (), (), 0.05)
        except InterruptedError:
            continue
        if not readable:
            continue
        try:
            chunk = read(64 * 1024)
        except BlockingIOError:
            continue
        if not chunk:
            break
        consume(chunk)

    return OutputDigest(
        byte_count=byte_count,
        sha256=digest.hexdigest(),
        captured=bytes(captured) if captured is not None else None,
        truncated=captured is not None and byte_count > len(captured),
        relay_error=relay_error,
        pipe_open_after_exit=pipe_open_after_exit,
    )


def _output_metadata(digest: OutputDigest) -> dict[str, JsonValue]:
    metadata: dict[str, JsonValue] = {
        "bytes": digest.byte_count,
        "sha256": digest.sha256,
    }
    if digest.relay_error is not None:
        metadata["relay_error"] = digest.relay_error
    if digest.pipe_open_after_exit:
        metadata["pipe_open_after_exit"] = True
    return metadata


def _peak_memory_bytes(value: float) -> float:
    # Linux reports KiB; macOS and the other supported BSDs report bytes.
    return value * 1024 if sys.platform.startswith("linux") else value


def _wait_with_usage(
    process: subprocess.Popen[bytes],
    output_pumps: tuple[Future[OutputDigest], ...] = (),
) -> tuple[int, resource.struct_rusage]:
    while True:
        for future in output_pumps:
            if future.done():
                future.result()
        try:
            child_pid, status, usage = os.wait4(process.pid, os.WNOHANG)
        except InterruptedError:
            continue
        except OSError as exc:
            raise CaptureError(f"could not collect captured process status: {exc}") from exc
        if child_pid == process.pid:
            break
        _STATUS_POLL_EVENT.wait(0.01)
    exit_code = os.waitstatus_to_exitcode(status)
    process.returncode = exit_code
    return exit_code, usage


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _signal_process_group(process_group: int, signal_number: int) -> None:
    try:
        os.killpg(process_group, signal_number)
    except ProcessLookupError:
        pass
    except OSError:
        pass


def _terminate_and_reap(process: subprocess.Popen[bytes]) -> None:
    """Best-effort cleanup for a captured process group after capture failure."""
    process_group = process.pid
    _signal_process_group(process_group, signal.SIGTERM)
    deadline = time.monotonic() + PROCESS_TERMINATION_TIMEOUT_SECONDS
    while _process_group_exists(process_group) and time.monotonic() < deadline:
        try:
            process.poll()
        except OSError:
            pass
        remaining = deadline - time.monotonic()
        if remaining > 0:
            _STATUS_POLL_EVENT.wait(min(0.01, remaining))
    if _process_group_exists(process_group):
        _signal_process_group(process_group, signal.SIGKILL)
    try:
        process.wait(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        _signal_process_group(process_group, signal.SIGKILL)
        try:
            process.wait(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
        except (subprocess.TimeoutExpired, ProcessLookupError, OSError):
            pass
    except ProcessLookupError:
        try:
            process.wait()
        except OSError:
            pass
    except OSError:
        pass


def _identified_environment_names(custom: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(custom, tuple) or not all(isinstance(name, str) for name in custom):
        raise CaptureError("identified environment names must be a tuple of strings")
    if len(custom) > MAX_CUSTOM_ENVIRONMENT_IDENTITIES:
        raise CaptureError(
            "cannot identify more than "
            f"{MAX_CUSTOM_ENVIRONMENT_IDENTITIES} custom environment variables"
        )
    for name in custom:
        if not name or "\0" in name or "=" in name:
            raise CaptureError(
                "identified environment names must be non-empty and cannot contain '=' or NUL"
            )
        try:
            encoded = name.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise CaptureError("identified environment names must be valid UTF-8") from exc
        if len(encoded) > MAX_ENVIRONMENT_NAME_BYTES:
            raise CaptureError(
                "identified environment names cannot exceed "
                f"{MAX_ENVIRONMENT_NAME_BYTES} UTF-8 bytes"
            )
    return tuple(dict.fromkeys((*_IDENTIFIED_ENVIRONMENT_VARIABLES, *custom)))
