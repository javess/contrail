"""Transient CLI client and capture-worker process control."""

from __future__ import annotations

import json
import os
import queue
import runpy
import select
import signal
import stat
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field

from runtime_tools.capture import (
    _CAPTURE_JOB_ID_ENV,
    _CAPTURE_JOB_ROOT_ENV,
    _CAPTURE_WORKER_CLIENT_FD_ENV,
    _CAPTURE_WORKER_DETACHED_ENV,
    _CAPTURE_WORKER_STDERR_FD_ENV,
    _CAPTURE_WORKER_STDOUT_FD_ENV,
    CaptureError,
)
from runtime_tools.capture_jobs import (
    CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES,
    CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
    CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
    MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES,
    CaptureJob,
    CaptureJobError,
    CaptureJobOutputStream,
    capture_job_document,
    checkpoint_current_capture_job_output_tail,
    create_capture_job,
    fail_capture_job_start,
    finish_current_capture_job,
    mark_current_capture_job_disconnected,
    mark_current_capture_job_output,
    mark_current_capture_job_output_incomplete,
    open_current_capture_job_cancel_channel,
    open_current_capture_job_output_sink,
    start_current_capture_job,
)

CAPTURE_WORKER_INTERRUPT_TIMEOUT_SECONDS = 5.0
CAPTURE_WORKER_OUTPUT_DRAIN_TIMEOUT_SECONDS = 1.0
CAPTURE_WORKER_OUTPUT_TAIL_CHECKPOINT_SECONDS = 0.5


class _DetachedOutputUnstableError(CaptureJobError):
    """Raised when output threads cannot stop before lifecycle finalization."""


@dataclass(slots=True)
class _DetachedOutputSpool:
    threads: tuple[threading.Thread, threading.Thread]
    stopped: threading.Event
    errors: queue.SimpleQueue[BaseException]
    finished: bool = False


@dataclass(slots=True)
class _RetainedOutputTail:
    chunks: deque[bytes] = field(default_factory=deque)
    size_bytes: int = 0

    def append(self, content: bytes) -> None:
        if not content:
            return
        if len(content) >= CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES:
            self.chunks.clear()
            retained = content[-CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES:]
            self.chunks.append(retained)
            self.size_bytes = len(retained)
            return
        self.chunks.append(content)
        self.size_bytes += len(content)
        while self.size_bytes > CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES:
            excess = self.size_bytes - CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES
            first = self.chunks[0]
            if len(first) <= excess:
                self.chunks.popleft()
                self.size_bytes -= len(first)
            else:
                self.chunks[0] = first[excess:]
                self.size_bytes -= excess

    def snapshot(self) -> bytes:
        return b"".join(self.chunks)


def capture_worker_client_event() -> threading.Event | None:
    detached = os.environ.get(_CAPTURE_WORKER_DETACHED_ENV)
    if detached not in {None, "1"}:
        raise CaptureError("capture worker detached state is invalid")
    raw_descriptor = os.environ.pop(_CAPTURE_WORKER_CLIENT_FD_ENV, None)
    if detached == "1":
        if raw_descriptor is not None:
            raise CaptureError("detached capture worker cannot have a client channel")
        return threading.Event()
    if raw_descriptor is None:
        return None
    try:
        descriptor = int(raw_descriptor)
    except ValueError as exc:
        raise CaptureError("capture worker client channel is invalid") from exc
    if descriptor < 3:
        raise CaptureError("capture worker client channel is invalid")
    try:
        descriptor_status = os.fstat(descriptor)
    except OSError as exc:
        raise CaptureError("capture worker client channel is unavailable") from exc
    if not stat.S_ISFIFO(descriptor_status.st_mode):
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise CaptureError("capture worker client channel is not a pipe")
    disconnected = threading.Event()
    threading.Thread(
        target=_watch_capture_client,
        args=(descriptor, disconnected),
        name="contrail-capture-client-monitor",
        daemon=True,
    ).start()
    return disconnected


def run_capture_worker(
    arguments: tuple[str, ...],
    *,
    module: str,
    detached: bool = False,
    detached_format: str = "text",
) -> int:
    if module not in {
        "runtime_tools.cli",
        "runtime_tools.contrail_cli",
        "runtime_tools.proofline.cli",
        "runtime_tools.rundiff.cli",
    }:
        raise CaptureError("capture worker module is unsupported")
    if not isinstance(detached, bool):
        raise CaptureError("capture worker detached state must be a boolean")
    if detached_format not in {"text", "json"}:
        raise CaptureError("detached capture worker format must be 'text' or 'json'")
    read_descriptor: int | None = None
    write_descriptor: int | None = None
    stdout_read_descriptor: int | None = None
    stdout_write_descriptor: int | None = None
    stderr_read_descriptor: int | None = None
    stderr_write_descriptor: int | None = None
    job_id: str | None = None
    worker_started = False
    try:
        operation = _capture_operation(module, arguments)
        job, job_root = create_capture_job(operation, detached=detached)
        job_id = job.job_id
        environment = {
            **os.environ,
            _CAPTURE_JOB_ID_ENV: job.job_id,
            _CAPTURE_JOB_ROOT_ENV: str(job_root),
        }
        if detached:
            stdout_read_descriptor, stdout_write_descriptor = os.pipe()
            stderr_read_descriptor, stderr_write_descriptor = os.pipe()
            environment.update(
                {
                    _CAPTURE_WORKER_DETACHED_ENV: "1",
                    _CAPTURE_WORKER_STDOUT_FD_ENV: str(stdout_read_descriptor),
                    _CAPTURE_WORKER_STDERR_FD_ENV: str(stderr_read_descriptor),
                }
            )
            passed_descriptors = (stdout_read_descriptor, stderr_read_descriptor)
        else:
            read_descriptor, write_descriptor = os.pipe()
            environment[_CAPTURE_WORKER_CLIENT_FD_ENV] = str(read_descriptor)
            passed_descriptors = (read_descriptor,)
        process = subprocess.Popen(
            (sys.executable, "-m", "runtime_tools.capture_worker", module, *arguments),
            env=environment,
            pass_fds=passed_descriptors,
            start_new_session=True,
            stdin=subprocess.DEVNULL if detached else None,
            stdout=stdout_write_descriptor if detached else None,
            stderr=stderr_write_descriptor if detached else None,
        )
        worker_started = True
        if detached:
            stdout_read_descriptor = _close_descriptor(stdout_read_descriptor)
            stdout_write_descriptor = _close_descriptor(stdout_write_descriptor)
            stderr_read_descriptor = _close_descriptor(stderr_read_descriptor)
            stderr_write_descriptor = _close_descriptor(stderr_write_descriptor)
            threading.Thread(
                target=_reap_detached_capture_worker,
                args=(process,),
                name="contrail-detached-worker-reaper",
                daemon=True,
            ).start()
            _print_detached_capture_job(job, module=module, output_format=detached_format)
            return 0
        read_descriptor = _close_descriptor(read_descriptor)
        try:
            return _capture_worker_exit_status(process.wait())
        except KeyboardInterrupt:
            _signal_capture_worker(process, signal.SIGINT)
            try:
                process.wait(timeout=CAPTURE_WORKER_INTERRUPT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                _signal_capture_worker(process, signal.SIGKILL)
                process.wait()
            return 128 + signal.SIGINT
    except KeyboardInterrupt:
        if job_id is not None and not worker_started:
            fail_capture_job_start(job_id)
        raise
    except (CaptureJobError, OSError) as exc:
        if job_id is not None and not worker_started:
            fail_capture_job_start(job_id)
        raise CaptureError(f"could not start capture worker: {exc}") from exc
    finally:
        _close_descriptor(read_descriptor)
        _close_descriptor(write_descriptor)
        _close_descriptor(stdout_read_descriptor)
        _close_descriptor(stdout_write_descriptor)
        _close_descriptor(stderr_read_descriptor)
        _close_descriptor(stderr_write_descriptor)


def _close_descriptor(descriptor: int | None) -> None:
    if descriptor is None:
        return None
    try:
        os.close(descriptor)
    except OSError:
        pass
    return None


def _reap_detached_capture_worker(process: subprocess.Popen[bytes]) -> None:
    try:
        process.wait()
    except OSError:
        pass


def _print_detached_capture_job(job: CaptureJob, *, module: str, output_format: str) -> None:
    if output_format == "json":
        print(
            json.dumps(
                capture_job_document(job),
                allow_nan=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    command = {
        "runtime_tools.cli": "runtime",
        "runtime_tools.contrail_cli": "contrail",
        "runtime_tools.proofline.cli": "runtime",
        "runtime_tools.rundiff.cli": "runtime",
    }[module]
    print("CAPTURE JOB DETACHED")
    print()
    print(f"id:     {job.job_id}")
    print(f"status: {command} job status {job.job_id}")
    print(f"wait:   {command} job wait {job.job_id}")
    print(f"output: {command} job output {job.job_id}")
    print(f"follow: {command} job output {job.job_id} --follow")


def _watch_capture_client(descriptor: int, disconnected: threading.Event) -> None:
    try:
        while True:
            try:
                content = os.read(descriptor, 1)
            except InterruptedError:
                continue
            except OSError:
                break
            if not content:
                break
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass
        disconnected.set()
        mark_current_capture_job_disconnected()


def _start_detached_output_spool() -> _DetachedOutputSpool | None:
    stdout_raw = os.environ.pop(_CAPTURE_WORKER_STDOUT_FD_ENV, None)
    stderr_raw = os.environ.pop(_CAPTURE_WORKER_STDERR_FD_ENV, None)
    if os.environ.get(_CAPTURE_WORKER_DETACHED_ENV) != "1":
        if stdout_raw is not None or stderr_raw is not None:
            raise CaptureJobError("attached capture worker cannot have output channels")
        return None
    if stdout_raw is None or stderr_raw is None:
        raise CaptureJobError("detached capture worker output channels are unavailable")
    stdout_descriptor: int | None = None
    stderr_descriptor: int | None = None
    try:
        stdout_descriptor = _capture_output_descriptor(stdout_raw, "stdout")
        stderr_descriptor = _capture_output_descriptor(stderr_raw, "stderr")
        if stdout_descriptor == stderr_descriptor:
            raise CaptureJobError("detached capture worker output channels must be distinct")
        stopped = threading.Event()
        errors: queue.SimpleQueue[BaseException] = queue.SimpleQueue()
        stdout_thread = threading.Thread(
            target=_drain_detached_output,
            args=(stdout_descriptor, "stdout", stopped, errors),
            name="contrail-detached-stdout",
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_drain_detached_output,
            args=(stderr_descriptor, "stderr", stopped, errors),
            name="contrail-detached-stderr",
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        stdout_descriptor = None
        stderr_descriptor = None
        return _DetachedOutputSpool((stdout_thread, stderr_thread), stopped, errors)
    except BaseException:
        _close_descriptor(stdout_descriptor)
        _close_descriptor(stderr_descriptor)
        raise


def _capture_output_descriptor(raw: str, stream: CaptureJobOutputStream) -> int:
    try:
        descriptor = int(raw)
    except ValueError as exc:
        raise CaptureJobError(f"detached capture worker {stream} channel is invalid") from exc
    if descriptor < 3:
        raise CaptureJobError(f"detached capture worker {stream} channel is invalid")
    try:
        metadata = os.fstat(descriptor)
    except OSError as exc:
        raise CaptureJobError(f"detached capture worker {stream} channel is unavailable") from exc
    if not stat.S_ISFIFO(metadata.st_mode):
        raise CaptureJobError(f"detached capture worker {stream} channel is not a pipe")
    return descriptor


def _drain_detached_output(
    source_descriptor: int,
    stream: CaptureJobOutputStream,
    stopped: threading.Event,
    errors: queue.SimpleQueue[BaseException],
) -> None:
    sink_descriptor: int | None = None
    head_size_bytes = 0
    total_size_bytes = 0
    tail = _RetainedOutputTail()
    tail_dirty = False
    incomplete = False
    last_tail_checkpoint = time.monotonic()
    try:
        sink_descriptor = open_current_capture_job_output_sink(stream)
        while True:
            if stopped.is_set():
                incomplete = True
                break
            try:
                readable, _, _ = select.select((source_descriptor,), (), (), 0.1)
            except InterruptedError:
                continue
            if not readable:
                now = time.monotonic()
                if (
                    tail_dirty
                    and now - last_tail_checkpoint >= CAPTURE_WORKER_OUTPUT_TAIL_CHECKPOINT_SECONDS
                ):
                    _checkpoint_detached_output(
                        sink_descriptor,
                        stream,
                        head_size_bytes,
                        tail,
                        total_size_bytes,
                        incomplete=incomplete,
                    )
                    tail_dirty = False
                    last_tail_checkpoint = now
                continue
            try:
                content = os.read(source_descriptor, 64 * 1024)
            except InterruptedError:
                continue
            if not content:
                break
            total_size_bytes += len(content)
            available = CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES - head_size_bytes
            head = content[:available]
            if head:
                _write_all(sink_descriptor, head)
                head_size_bytes += len(head)
            remainder = content[len(head) :]
            if remainder:
                tail.append(remainder)
                tail_dirty = True
            now = time.monotonic()
            if (
                tail_dirty
                and now - last_tail_checkpoint >= CAPTURE_WORKER_OUTPUT_TAIL_CHECKPOINT_SECONDS
            ):
                _checkpoint_detached_output(
                    sink_descriptor,
                    stream,
                    head_size_bytes,
                    tail,
                    total_size_bytes,
                    incomplete=incomplete,
                )
                tail_dirty = False
                last_tail_checkpoint = now
        _checkpoint_detached_output(
            sink_descriptor,
            stream,
            head_size_bytes,
            tail,
            total_size_bytes,
            incomplete=incomplete,
        )
    except (CaptureJobError, OSError) as exc:
        errors.put(exc)
    finally:
        _close_descriptor(sink_descriptor)
        _close_descriptor(source_descriptor)


def _checkpoint_detached_output(
    head_descriptor: int,
    stream: CaptureJobOutputStream,
    head_size_bytes: int,
    tail: _RetainedOutputTail,
    total_size_bytes: int,
    *,
    incomplete: bool,
) -> None:
    os.fsync(head_descriptor)
    tail_content = tail.snapshot()
    checkpoint_current_capture_job_output_tail(stream, tail_content)
    omitted_size = max(0, total_size_bytes - CAPTURE_JOB_OUTPUT_LIMIT_BYTES)
    omitted_bytes = min(omitted_size, MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES)
    omitted_bytes_truncated = omitted_size > MAX_CAPTURE_JOB_OUTPUT_OMITTED_BYTES
    mark_current_capture_job_output(
        stream,
        head_size_bytes,
        len(tail_content),
        omitted_bytes,
        omitted_bytes_truncated=omitted_bytes_truncated or incomplete,
        truncated=bool(omitted_size) or incomplete,
    )


def _write_all(descriptor: int, content: bytes) -> None:
    written = 0
    while written < len(content):
        try:
            count = os.write(descriptor, content[written:])
        except InterruptedError:
            continue
        if count <= 0:
            raise OSError("capture job output write made no progress")
        written += count


def _finish_detached_output_spool(spool: _DetachedOutputSpool | None) -> None:
    if spool is None or spool.finished:
        return
    spool.finished = True
    redirect_error = _redirect_standard_output_to_null()
    deadline = time.monotonic() + CAPTURE_WORKER_OUTPUT_DRAIN_TIMEOUT_SECONDS
    for thread in spool.threads:
        thread.join(timeout=max(0.0, deadline - time.monotonic()))
    if any(thread.is_alive() for thread in spool.threads):
        spool.stopped.set()
        for thread in spool.threads:
            thread.join(timeout=0.2)
    if any(thread.is_alive() for thread in spool.threads):
        raise _DetachedOutputUnstableError("detached capture worker output drain did not stop")
    try:
        drain_error = spool.errors.get_nowait()
    except queue.Empty:
        drain_error = None
    if redirect_error is not None:
        raise CaptureJobError(f"could not finalize detached capture output: {redirect_error}")
    if drain_error is not None:
        raise CaptureJobError(f"could not retain detached capture output: {drain_error}")


def _redirect_standard_output_to_null() -> BaseException | None:
    first_error: BaseException | None = None
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except (BrokenPipeError, OSError, ValueError) as exc:
            if first_error is None:
                first_error = exc
    null_descriptor: int | None = None
    try:
        null_descriptor = os.open(os.devnull, os.O_WRONLY)
        os.dup2(null_descriptor, 1)
        os.dup2(null_descriptor, 2)
    except OSError as exc:
        if first_error is None:
            first_error = exc
    finally:
        _close_descriptor(null_descriptor)
    return first_error


def _watch_capture_job_cancel(descriptor: int, stopped: threading.Event) -> None:
    while not stopped.is_set():
        try:
            readable, _, _ = select.select((descriptor,), (), (), 0.1)
        except (OSError, ValueError):
            return
        if not readable:
            continue
        try:
            content = os.read(descriptor, 1)
        except BlockingIOError:
            continue
        except OSError:
            return
        if content:
            os.kill(os.getpid(), signal.SIGINT)
            return


def _stop_capture_job_cancel_monitor(
    thread: threading.Thread,
    stopped: threading.Event,
) -> bool:
    interrupted = False
    while True:
        try:
            stopped.set()
            thread.join()
            return interrupted
        except KeyboardInterrupt:
            interrupted = True


def _signal_capture_worker(process: subprocess.Popen[bytes], signal_number: int) -> None:
    try:
        os.killpg(process.pid, signal_number)
    except (ProcessLookupError, PermissionError, OSError):
        pass


def _capture_worker_exit_status(return_code: int) -> int:
    return 128 - return_code if return_code < 0 else return_code


def _capture_operation(module: str, arguments: tuple[str, ...]) -> str:
    command = arguments[0] if arguments else "capture"
    labels = {
        "runtime_tools.cli": "runtime",
        "runtime_tools.contrail_cli": "contrail",
        "runtime_tools.proofline.cli": "proofline",
        "runtime_tools.rundiff.cli": "rundiff",
    }
    commands = {
        "runtime_tools.cli": frozenset({"record"}),
        "runtime_tools.contrail_cli": frozenset({"record", "run", "search"}),
        "runtime_tools.proofline.cli": frozenset({"run", "search"}),
        "runtime_tools.rundiff.cli": frozenset({"record"}),
    }
    if module not in labels or command not in commands[module]:
        raise CaptureError("capture worker operation is unsupported")
    return f"{labels[module]} {command}"


def _system_exit_status(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value & 0xFF
    if value:
        print(value, file=sys.stderr)
    return 1


def _run_worker_module(module: str, arguments: tuple[str, ...]) -> int:
    if module not in {
        "runtime_tools.cli",
        "runtime_tools.contrail_cli",
        "runtime_tools.proofline.cli",
        "runtime_tools.rundiff.cli",
    }:
        raise CaptureJobError("capture worker module is unsupported")
    lock_descriptor = start_current_capture_job()
    output_spool: _DetachedOutputSpool | None = None
    cancel_descriptor: int | None = None
    cancel_stopped = threading.Event()
    cancel_thread: threading.Thread | None = None
    original_arguments = sys.argv
    try:
        output_spool = _start_detached_output_spool()
        cancel_descriptor = open_current_capture_job_cancel_channel()
        thread = threading.Thread(
            target=_watch_capture_job_cancel,
            args=(cancel_descriptor, cancel_stopped),
            name="contrail-capture-cancel-monitor",
            daemon=True,
        )
        thread.start()
        cancel_thread = thread
        sys.argv = [module, *arguments]
        try:
            runpy.run_module(module, run_name="__main__")
        except SystemExit as exc:
            exit_status = _system_exit_status(exc.code)
        except KeyboardInterrupt:
            exit_status = 128 + signal.SIGINT
        except BaseException:
            if cancel_thread is not None:
                _stop_capture_job_cancel_monitor(cancel_thread, cancel_stopped)
            _finish_detached_output_spool(output_spool)
            finish_current_capture_job(1, failed=True)
            raise
        else:
            exit_status = 0
        if _stop_capture_job_cancel_monitor(cancel_thread, cancel_stopped):
            exit_status = 128 + signal.SIGINT
        _finish_detached_output_spool(output_spool)
        finish_current_capture_job(exit_status)
        return exit_status
    except KeyboardInterrupt:
        if cancel_thread is not None:
            _stop_capture_job_cancel_monitor(cancel_thread, cancel_stopped)
        _finish_detached_output_spool(output_spool)
        finish_current_capture_job(128 + signal.SIGINT)
        return 128 + signal.SIGINT
    except CaptureJobError as exc:
        if cancel_thread is not None:
            _stop_capture_job_cancel_monitor(cancel_thread, cancel_stopped)
        try:
            _finish_detached_output_spool(output_spool)
        except CaptureJobError:
            pass
        mark_current_capture_job_output_incomplete()
        if not isinstance(exc, _DetachedOutputUnstableError):
            try:
                finish_current_capture_job(2, failed=True)
            except CaptureJobError:
                pass
        raise
    finally:
        sys.argv = original_arguments
        cancel_stopped.set()
        if cancel_thread is not None:
            _stop_capture_job_cancel_monitor(cancel_thread, cancel_stopped)
        if cancel_descriptor is not None:
            os.close(cancel_descriptor)
        try:
            _finish_detached_output_spool(output_spool)
        except CaptureJobError:
            pass
        os.close(lock_descriptor)


def _worker_main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        print("capture worker: target module is required", file=sys.stderr)
        return 2
    module, *module_arguments = arguments
    try:
        return _run_worker_module(module, tuple(module_arguments))
    except CaptureJobError as exc:
        print(f"capture worker: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(_worker_main())
