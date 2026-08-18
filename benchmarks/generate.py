"""Deterministic fixture generators for the release benchmark harness."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO

from runtime_tools.model import CausalEdge, Entity, Event, Execution
from runtime_tools.storage import RunpackWriter


def _write_json_array(stream: TextIO, values: Iterator[dict[str, object]]) -> None:
    write = stream.write
    first = True
    for value in values:
        if not first:
            write(",")
        first = False
        write(json.dumps(value, separators=(",", ":"), sort_keys=True))


def write_otlp_trace(path: Path, count: int) -> None:
    trace_id = "1" * 32
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write(
            '{"resourceSpans":[{"resource":{"attributes":['
            '{"key":"service.name","value":{"stringValue":"benchmark"}}]},'
            '"scopeSpans":[{"scope":{"name":"release-benchmark"},"spans":['
        )
        _write_json_array(
            stream,
            (
                {
                    "endTimeUnixNano": str(index + 2),
                    "name": "work",
                    "spanId": f"{index + 1:016x}",
                    "startTimeUnixNano": str(index + 1),
                    "traceId": trace_id,
                }
                for index in range(count)
            ),
        )
        stream.write("]}]}]}")


def write_prometheus_response(path: Path, count: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write(
            '{"status":"success","data":{"resultType":"matrix","result":['
            '{"metric":{"__name__":"benchmark_total","job":"release"},"values":['
        )
        first = True
        for index in range(count):
            if not first:
                stream.write(",")
            first = False
            stream.write(f'[{index + 1},"{index + 1}"]')
        stream.write("]}]}}")


def write_kubernetes_snapshot(path: Path, count: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        stream.write('{"apiVersion":"v1","kind":"List","items":[')
        _write_json_array(
            stream,
            (
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "creationTimestamp": "2024-01-01T00:00:00Z",
                        "name": f"pod-{index}",
                        "namespace": "benchmark",
                        "uid": f"benchmark-pod-{index}",
                    },
                    "spec": {"containers": []},
                    "status": {"phase": "Pending"},
                }
                for index in range(count)
            ),
        )
        stream.write("]}")


def write_finished_runpack(path: Path, *, finished_at_ns: int) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "benchmark",
                "benchmark",
                0,
                finished_at_ns,
                (),
                str(path.parent),
                0,
                None,
                {},
            )
        )


def write_causal_chain(path: Path, count: int) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "benchmark-chain",
                "benchmark-chain",
                0,
                count,
                (),
                str(path.parent),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            Event(
                f"event-{index}",
                "operation",
                "work",
                "worker",
                index,
                index + 1,
                "benchmark",
                None,
                index,
                {},
            )
            for index in range(count)
        )
        writer.add_causal_edges(
            CausalEdge(
                f"event-{index}",
                f"event-{index + 1}",
                "parent",
                1.0,
                {},
            )
            for index in range(count - 1)
        )
