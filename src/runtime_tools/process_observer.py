"""Bounded controller-side observation of one POSIX process group."""

from __future__ import annotations

import hashlib
import math
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from runtime_tools.model import Entity, JsonValue, Measurement


class ProcessObserverError(ValueError):
    """Raised when the host process table cannot be sampled safely."""


PROCESS_OBSERVER_INTERVAL_SECONDS = 0.1
MAX_PROCESS_OBSERVER_PROCESSES = 2_000
MAX_PROCESS_OBSERVER_SAMPLES = 50_000
MAX_PROCESS_TABLE_BYTES = 16 * 1024 * 1024
PROCESS_TABLE_TIMEOUT_SECONDS = 1.0
_MAX_TEXT_CHARACTERS = 1_024
_MAX_INTEGER = (1 << 63) - 1
_PS = Path("/bin/ps")


@dataclass(frozen=True, slots=True)
class _RawProcessSample:
    pid: int
    ppid: int
    rss_bytes: int
    cpu_seconds: float
    name: str


@dataclass(slots=True)
class _ObservedProcess:
    entity_id: str
    pid: int
    ppid: int
    name: str
    parent_entity_id: str | None
    first_seen_ns: int
    last_seen_ns: int
    sample_count: int = 0


@dataclass(frozen=True, slots=True)
class _ResourceSample:
    entity_id: str
    pid: int
    ppid: int
    process_name: str
    timestamp_ns: int
    rss_bytes: int
    cpu_seconds: float


@dataclass(frozen=True, slots=True)
class ProcessObservationResult:
    entities: tuple[Entity, ...]
    measurements: tuple[Measurement, ...]
    process_count: int
    descendant_process_count: int
    sample_count: int
    poll_count: int
    truncated: bool
    dropped_process_count: int
    error: str | None

    def as_metadata(self) -> dict[str, JsonValue]:
        if self.sample_count == 0:
            status = "unavailable"
        elif self.error is not None:
            status = "partial"
        elif self.truncated:
            status = "truncated"
        else:
            status = "complete"
        value: dict[str, JsonValue] = {
            "requested": True,
            "observer": "posix-process-table",
            "status": status,
            "interval_ns": int(PROCESS_OBSERVER_INTERVAL_SECONDS * 1_000_000_000),
            "process_count": self.process_count,
            "descendant_process_count": self.descendant_process_count,
            "sample_count": self.sample_count,
            "measurement_count": len(self.measurements),
            "poll_count": self.poll_count,
            "truncated": self.truncated,
            "dropped_process_count": self.dropped_process_count,
            "limits": {
                "max_processes": MAX_PROCESS_OBSERVER_PROCESSES,
                "max_samples": MAX_PROCESS_OBSERVER_SAMPLES,
                "max_process_table_bytes": MAX_PROCESS_TABLE_BYTES,
            },
        }
        if self.error is not None:
            value["error"] = self.error
        return value


def _bounded_text(value: str) -> str:
    text = value.encode("utf-8", "backslashreplace").decode("utf-8")
    return text[:_MAX_TEXT_CHARACTERS]


def _cpu_seconds(value: str) -> float:
    days = 0
    clock = value
    if "-" in clock:
        day_value, clock = clock.split("-", 1)
        days = int(day_value)
    components = clock.split(":")
    if len(components) == 2:
        hours = 0
        minutes = int(components[0])
        seconds = float(components[1])
    elif len(components) == 3:
        hours = int(components[0])
        minutes = int(components[1])
        seconds = float(components[2])
    else:
        raise ValueError("unsupported process CPU clock")
    result = days * 86_400 + hours * 3_600 + minutes * 60 + seconds
    if (
        days < 0
        or hours < 0
        or minutes < 0
        or seconds < 0
        or not math.isfinite(result)
        or result > _MAX_INTEGER
    ):
        raise ValueError("invalid process CPU clock")
    return result


def _read_process_table(process_group_id: int) -> tuple[_RawProcessSample, ...]:
    if not _PS.is_file():
        raise ProcessObserverError("/bin/ps is unavailable")
    try:
        completed = subprocess.run(
            (
                str(_PS),
                "-axo",
                "pid=,ppid=,pgid=,rss=,time=,comm=",
            ),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=PROCESS_TABLE_TIMEOUT_SECONDS,
            env={**os.environ, "LC_ALL": "C"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ProcessObserverError("could not read the host process table") from exc
    if completed.returncode != 0:
        raise ProcessObserverError(f"process table command exited {completed.returncode}")
    if len(completed.stdout) > MAX_PROCESS_TABLE_BYTES:
        raise ProcessObserverError("process table output exceeds its byte limit")
    text = completed.stdout.decode("utf-8", "backslashreplace")
    samples: list[_RawProcessSample] = []
    for line in text.splitlines():
        fields = line.strip().split(maxsplit=5)
        if len(fields) != 6:
            continue
        try:
            pid = int(fields[0])
            ppid = int(fields[1])
            pgid = int(fields[2])
            rss_kib = int(fields[3])
            cpu_seconds = _cpu_seconds(fields[4])
        except ValueError:
            continue
        if pgid != process_group_id:
            continue
        if pid <= 0 or ppid < 0 or rss_kib < 0 or rss_kib > _MAX_INTEGER // 1_024:
            continue
        name = _bounded_text(Path(fields[5]).name or "process")
        samples.append(_RawProcessSample(pid, ppid, rss_kib * 1_024, cpu_seconds, name))
    samples.sort(key=lambda sample: sample.pid)
    return tuple(samples)


def _entity_id(execution_id: str, pid: int, generation: int) -> str:
    identity = f"{execution_id}\0{pid}\0{generation}".encode()
    return f"observed-process:{hashlib.sha256(identity).hexdigest()}"


class ProcessTreeObserver:
    """Sample resource use for processes that remain in a workload's process group."""

    def __init__(
        self,
        *,
        process_group_id: int,
        execution_id: str,
        root_entity_id: str,
        started_at_ns: int,
        started_monotonic_ns: int,
    ) -> None:
        self._process_group_id = process_group_id
        self._execution_id = execution_id
        self._root_entity_id = root_entity_id
        self._started_at_ns = started_at_ns
        self._started_monotonic_ns = started_monotonic_ns
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._observations: dict[tuple[int, int], _ObservedProcess] = {}
        self._active: dict[int, tuple[int, int]] = {}
        self._generations: dict[int, int] = {}
        self._samples: list[_ResourceSample] = []
        self._dropped_pids: set[int] = set()
        self._poll_count = 0
        self._truncated = False
        self._error: str | None = None
        self._result: ProcessObservationResult | None = None

    def start(self) -> None:
        if self._thread is not None or self._result is not None:
            return
        try:
            thread = threading.Thread(
                target=self._run,
                name="contrail-process-observer",
                daemon=True,
            )
            thread.start()
            self._thread = thread
        except (OSError, RuntimeError) as exc:
            self._error = _bounded_text(f"could not start process observer: {exc}")

    def stop(self) -> ProcessObservationResult:
        if self._result is not None:
            return self._result
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=PROCESS_TABLE_TIMEOUT_SECONDS + 1.0)
            if self._thread.is_alive():
                with self._lock:
                    self._error = "process observer did not stop within its timeout"
        with self._lock:
            self._result = self._build_result()
        return self._result

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                samples = _read_process_table(self._process_group_id)
            except ProcessObserverError as exc:
                with self._lock:
                    self._error = _bounded_text(str(exc))
                return
            timestamp_ns = self._started_at_ns + max(
                0, time.perf_counter_ns() - self._started_monotonic_ns
            )
            with self._lock:
                if self._stop.is_set():
                    return
                self._record_poll(samples, timestamp_ns)
                should_stop = self._truncated and len(self._samples) >= MAX_PROCESS_OBSERVER_SAMPLES
            if should_stop or self._stop.wait(PROCESS_OBSERVER_INTERVAL_SECONDS):
                return

    def _record_poll(
        self,
        samples: tuple[_RawProcessSample, ...],
        timestamp_ns: int,
    ) -> None:
        self._poll_count += 1
        previous_active = self._active
        current_active: dict[int, tuple[int, int]] = {}
        created: list[_ObservedProcess] = []
        accepted: list[tuple[_RawProcessSample, _ObservedProcess]] = []
        for sample in samples:
            key = previous_active.get(sample.pid)
            observation = self._observations.get(key) if key is not None else None
            if observation is None:
                if len(self._observations) >= MAX_PROCESS_OBSERVER_PROCESSES:
                    self._truncated = True
                    self._dropped_pids.add(sample.pid)
                    continue
                generation = self._generations.get(sample.pid, 0) + 1
                self._generations[sample.pid] = generation
                key = (sample.pid, generation)
                observation = _ObservedProcess(
                    (
                        self._root_entity_id
                        if sample.pid == self._process_group_id
                        else _entity_id(self._execution_id, sample.pid, generation)
                    ),
                    sample.pid,
                    sample.ppid,
                    sample.name,
                    None,
                    timestamp_ns,
                    timestamp_ns,
                )
                self._observations[key] = observation
                created.append(observation)
            assert key is not None
            current_active[sample.pid] = key
            accepted.append((sample, observation))

        for observation in created:
            if observation.pid == self._process_group_id:
                continue
            if observation.ppid == self._process_group_id:
                observation.parent_entity_id = self._root_entity_id
                continue
            parent_key = current_active.get(observation.ppid)
            parent = self._observations.get(parent_key) if parent_key is not None else None
            if parent is not None:
                observation.parent_entity_id = parent.entity_id

        self._active = current_active
        for sample, observation in accepted:
            if len(self._samples) >= MAX_PROCESS_OBSERVER_SAMPLES:
                self._truncated = True
                return
            observation.last_seen_ns = timestamp_ns
            observation.sample_count += 1
            self._samples.append(
                _ResourceSample(
                    observation.entity_id,
                    sample.pid,
                    sample.ppid,
                    observation.name,
                    timestamp_ns,
                    sample.rss_bytes,
                    sample.cpu_seconds,
                )
            )

    def _build_result(self) -> ProcessObservationResult:
        observations = tuple(self._observations.values())
        entities = tuple(
            Entity(
                observation.entity_id,
                "process",
                observation.name,
                observation.parent_entity_id,
                {
                    "source": "process-observer",
                    "pid": observation.pid,
                    "ppid": observation.ppid,
                    "first_seen_ns": observation.first_seen_ns,
                    "last_seen_ns": observation.last_seen_ns,
                    "sample_count": observation.sample_count,
                },
            )
            for observation in observations
            if observation.entity_id != self._root_entity_id
        )
        measurements: list[Measurement] = []
        for index, sample in enumerate(self._samples):
            attributes: dict[str, JsonValue] = {
                "source": "process-observer",
                "pid": sample.pid,
                "ppid": sample.ppid,
                "process_name": sample.process_name,
                "sample_index": index,
            }
            measurements.extend(
                (
                    Measurement(
                        "process.memory.rss",
                        float(sample.rss_bytes),
                        "By",
                        sample.timestamp_ns,
                        sample.entity_id,
                        attributes,
                    ),
                    Measurement(
                        "process.cpu.total",
                        sample.cpu_seconds,
                        "s",
                        sample.timestamp_ns,
                        sample.entity_id,
                        attributes,
                    ),
                )
            )
        process_count = len(observations)
        return ProcessObservationResult(
            entities,
            tuple(measurements),
            process_count,
            sum(item.entity_id != self._root_entity_id for item in observations),
            len(self._samples),
            self._poll_count,
            self._truncated,
            len(self._dropped_pids),
            self._error,
        )
