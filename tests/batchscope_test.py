from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Literal

import pytest

from runtime_tools import record_process
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.analysis import (
    BatchAnalysis,
    Bottleneck,
    CriticalPath,
    LifecyclePhase,
)
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.model import CausalEdge, Entity, Event, Execution, JsonValue
from runtime_tools.providers.builtins.kubernetes import import_kubernetes_snapshot
from runtime_tools.storage import RunpackWriter


def _event(
    event_id: str,
    kind: str,
    name: str,
    start: int,
    end: int,
    attributes: dict[str, JsonValue] | None = None,
) -> Event:
    return Event(event_id, kind, name, "worker", start, end, "test", None, None, attributes or {})


def _write_batch(path: Path) -> None:
    events = (
        _event("process", "process.run", "python", 0, 100_000_000),
        _event("run", "run", "pipeline", 5_000_000, 95_000_000),
        _event("compute", "stage", "compute", 10_000_000, 50_000_000),
        _event("task-a", "operation", "task-a", 10_000_000, 40_000_000),
        _event("task-b", "operation", "task-b", 20_000_000, 45_000_000),
        _event("drain", "stage", "drain", 50_000_000, 90_000_000, {"concurrency": 1}),
        _event("db", "client.request", "database.flush", 55_000_000, 85_000_000),
        _event(
            "progress-1",
            "progress",
            "progress",
            20_000_000,
            20_000_000,
            {"completed": 20, "total": 100},
        ),
        _event(
            "progress-2",
            "progress",
            "progress",
            60_000_000,
            60_000_000,
            {"completed": 60, "total": 100},
        ),
    )
    edges = (
        CausalEdge("process", "run", "parent", 1.0, {}),
        CausalEdge("run", "compute", "parent", 1.0, {}),
        CausalEdge("run", "drain", "parent", 1.0, {}),
        CausalEdge("compute", "task-a", "parent", 1.0, {}),
        CausalEdge("compute", "task-b", "parent", 1.0, {}),
        CausalEdge("drain", "db", "parent", 1.0, {}),
    )
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution("batch", "batch", 0, 100_000_000, (), str(path.parent), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        for event in events:
            writer.add_event(event)
        for edge in edges:
            writer.add_causal_edge(edge)


def _write_python_exception_batch(
    path: Path,
    *,
    exception_count: int,
    call_count: int,
    profile_status: Literal["complete", "truncated"] = "complete",
    include_observer_integrity: bool = True,
    trace_hook_setter_call_count: int = 0,
    hotter_function_count: int = 0,
    reported_exception_count: int | None = None,
    non_control_flow_exception_count: int | None = None,
    include_control_flow_filter: bool = True,
    control_flow_filter_exception_types_captured: bool = False,
) -> None:
    summary_exception_count = (
        exception_count if reported_exception_count is None else reported_exception_count
    )
    diagnostic_exception_count = (
        exception_count
        if non_control_flow_exception_count is None
        else non_control_flow_exception_count
    )
    python_exception_capture: dict[str, JsonValue] = {
        "enabled": True,
        "deep_only": True,
        "event_semantics": "per_propagated_frame",
        "function_count": 1,
        "event_count": summary_exception_count,
        "dropped_event_count": 0,
        "arguments_captured": False,
        "locals_captured": False,
        "exception_types_captured": False,
        "exception_values_captured": False,
        "exception_messages_captured": False,
        "tracebacks_captured": False,
        "line_events_enabled": False,
        "opcode_events_enabled": False,
        "limits": {"max_functions_per_process": 2_000},
    }
    if include_control_flow_filter:
        python_exception_capture["control_flow_filter"] = {
            "format_version": 1,
            "status": profile_status,
            "event_semantics": "exact_type_identity",
            "filtered_exception_types": [
                "GeneratorExit",
                "StopAsyncIteration",
                "StopIteration",
            ],
            "exception_type_identity_inspected": True,
            "exception_types_captured": control_flow_filter_exception_types_captured,
            "non_control_flow_function_count": int(diagnostic_exception_count > 0),
            "non_control_flow_event_count": diagnostic_exception_count,
            "filtered_event_count": summary_exception_count - diagnostic_exception_count,
            "dropped_non_control_flow_event_count": 0,
            "dropped_filtered_event_count": 0,
        }
    instrumentation: dict[str, JsonValue] = {
        "mode": "deep",
        "status": profile_status,
        "process_count": 1,
        "function_count": 1 + hotter_function_count,
        "edge_count": 0,
        "truncated": profile_status == "truncated",
        "dropped_call_count": 0,
        "python_exception_capture": python_exception_capture,
    }
    if include_observer_integrity:
        instrumentation["observer_integrity"] = {
            "format_version": 1,
            "status": "partial" if trace_hook_setter_call_count else "complete",
            "process_count": 1,
            "missing_process_count": 0,
            "profile_hook_setter_call_count": 0,
            "profile_hook_setter_process_count": 0,
            "trace_hook_setter_call_count": trace_hook_setter_call_count,
            "trace_hook_setter_process_count": int(trace_hook_setter_call_count > 0),
            "arguments_captured": False,
            "locals_captured": False,
            "hook_values_captured": False,
        }
    metadata: dict[str, JsonValue] = {"capture": {"instrumentation": instrumentation}}
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "exception-heavy-batch",
                0,
                1_000_000_000,
                ("python",),
                str(path.parent),
                0,
                None,
                metadata,
            )
        )
        writer.add_entity(Entity("process", "process", "python", None, {}))
        writer.add_event(
            Event(
                "profile",
                "python.call.aggregate",
                "application.retry",
                "process",
                None,
                None,
                None,
                None,
                None,
                {
                    "filename": "/work/retry.py",
                    "firstlineno": 10,
                    "scope": "application",
                    "implementation": "python",
                    "call_count": call_count,
                    "exception_count": exception_count,
                    **(
                        {"non_control_flow_exception_count": (diagnostic_exception_count)}
                        if include_control_flow_filter
                        else {}
                    ),
                    "total_seconds": 0.2,
                    "self_seconds": 0.1,
                    "max_seconds": 0.02,
                },
            )
        )
        for index in range(hotter_function_count):
            writer.add_event(
                Event(
                    f"hotter-profile-{index}",
                    "python.call.aggregate",
                    f"application.hotter_{index:03d}",
                    "process",
                    None,
                    None,
                    None,
                    None,
                    None,
                    {
                        "filename": "/work/hotter.py",
                        "firstlineno": index + 1,
                        "scope": "application",
                        "implementation": "python",
                        "call_count": 1,
                        "exception_count": 0,
                        **(
                            {"non_control_flow_exception_count": 0}
                            if include_control_flow_filter
                            else {}
                        ),
                        "total_seconds": 0.2,
                        "self_seconds": 0.2,
                        "max_seconds": 0.2,
                    },
                )
            )


def test_batchscope_derives_overlap_aware_critical_path_and_throughput(tmp_path: Path) -> None:
    runpack = tmp_path / "batch.runpack"
    _write_batch(runpack)

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("compute", 0.04),
        ("drain", 0.04),
    ]
    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.1
    assert analysis.critical_path.parallel_slack_seconds == 0.0
    assert analysis.critical_path.event_ids[:2] == ("process", "run")
    assert analysis.critical_path.event_names[:2] == ("python", "pipeline")
    assert analysis.throughput is not None
    assert analysis.throughput.rate_per_second == 1000.0
    assert analysis.throughput.remaining == 40.0
    assert analysis.throughput.estimated_drain_seconds == 0.04
    assert analysis.throughput.compute_finished_at_ns == 50_000_000
    assert analysis.throughput.remaining_at_compute_completion == 80.0
    assert analysis.throughput.post_compute_seconds == 0.05
    assert analysis.throughput.post_compute_rate_per_second is None
    assert {item.classification for item in analysis.bottlenecks} == {"serialized_stage"}

    report = render_analysis(analysis, "text")
    assert "Bottleneck\n  serialized_stage (90%)" in report
    assert "Observation\n  compute completed with 80 / 100 work items remaining" in report
    assert "50.0ms of post-compute wall time followed" in report


def test_batchscope_marks_inconsistent_profile_process_attribution_invalid(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "invalid-process-attribution.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "deep",
                "status": "complete",
                "process_count": 2,
                "function_count": 1,
                "edge_count": 0,
                "truncated": False,
                "dropped_call_count": 0,
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-attribution",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )
        writer.add_entity(Entity("process", "process", "python", None, {}))
        writer.add_event(
            Event(
                "profile",
                "python.call.aggregate",
                "application.work",
                "process",
                None,
                None,
                None,
                None,
                None,
                {
                    "filename": "/work/workload.py",
                    "firstlineno": 1,
                    "scope": "application",
                    "call_count": 2,
                    "total_seconds": 0.2,
                    "self_seconds": 0.2,
                    "max_seconds": 0.1,
                    "process_count": 1,
                    "processes": [
                        {
                            "pid": 42,
                            "role": "root",
                            "call_count": 1,
                            "total_seconds": 0.1,
                            "self_seconds": 0.1,
                            "max_seconds": 0.1,
                        }
                    ],
                },
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    assert analysis.deep_profile.process_coverage.status == "unavailable"
    assert analysis.deep_profile.snapshot_metrics.status == "unavailable"
    assert analysis.deep_profile.publication_metrics.status == "unavailable"
    assert analysis.deep_profile.normalization_metrics.status == "unavailable"
    assert analysis.python_hotspots[0].process_attribution_status == "invalid"
    assert analysis.python_hotspots[0].processes == ()
    assert "process attribution: invalid" in render_analysis(analysis, "text")


def test_batchscope_marks_malformed_profile_process_ids_invalid(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-profile-process-ids.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "deep",
                "status": "complete",
                "process_count": 2,
                "process_ids": [42, 42],
                "function_count": 0,
                "edge_count": 0,
                "truncated": False,
                "dropped_call_count": 0,
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-profile-process-ids",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )
        writer.add_entity(Entity("process", "process", "python", None, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    assert analysis.deep_profile.status == "complete"
    assert analysis.deep_profile.process_coverage.status == "invalid"
    assert "process coverage invalid" in render_analysis(analysis, "text")


def test_batchscope_rejects_native_capture_privacy_claims(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-native-privacy.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "deep",
                "status": "complete",
                "process_count": 1,
                "function_count": 0,
                "edge_count": 0,
                "truncated": False,
                "dropped_call_count": 0,
                "native_call_capture": {
                    "enabled": True,
                    "deep_only": True,
                    "function_count": 0,
                    "call_count": 0,
                    "exception_count": 0,
                    "arguments_captured": True,
                    "return_values_captured": False,
                    "exception_messages_captured": False,
                    "limits": {
                        "max_functions_per_process": 2_000,
                        "max_edges_per_process": 10_000,
                    },
                },
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-native-privacy",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    assert analysis.deep_profile.native_call_capture is not None
    assert analysis.deep_profile.native_call_capture.status == "invalid"
    assert "native-call capture metadata was invalid" in render_analysis(analysis, "text")
    document = analysis.as_json_value()
    deep_profile = document["deep_profile"]
    assert isinstance(deep_profile, dict)
    native_capture = deep_profile["native_call_capture"]
    assert isinstance(native_capture, dict)
    assert native_capture["status"] == "invalid"
    assert native_capture["arguments_captured"] is False


def test_batchscope_rejects_python_exception_capture_privacy_claims(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-python-exception-privacy.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "deep",
                "status": "complete",
                "process_count": 1,
                "function_count": 1,
                "edge_count": 0,
                "truncated": False,
                "dropped_call_count": 0,
                "python_exception_capture": {
                    "enabled": True,
                    "deep_only": True,
                    "event_semantics": "per_propagated_frame",
                    "function_count": 1,
                    "event_count": 2,
                    "dropped_event_count": 0,
                    "arguments_captured": False,
                    "locals_captured": True,
                    "exception_types_captured": False,
                    "exception_values_captured": False,
                    "exception_messages_captured": False,
                    "tracebacks_captured": False,
                    "line_events_enabled": False,
                    "opcode_events_enabled": False,
                    "limits": {"max_functions_per_process": 2_000},
                },
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-python-exception-privacy",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    capture = analysis.deep_profile.python_exception_capture
    assert capture is not None
    assert capture.status == "invalid"
    assert "Python-exception capture metadata was invalid" in render_analysis(analysis, "text")
    document = analysis.as_json_value()
    deep_profile = document["deep_profile"]
    assert isinstance(deep_profile, dict)
    exception_capture = deep_profile["python_exception_capture"]
    assert isinstance(exception_capture, dict)
    assert exception_capture["locals_captured"] is False


def test_batchscope_classifies_complete_exception_heavy_control_flow(tmp_path: Path) -> None:
    runpack = tmp_path / "exception-heavy.runpack"
    _write_python_exception_batch(runpack, exception_count=20, call_count=10)

    analysis = analyze_runpack(runpack)

    finding = next(
        item for item in analysis.bottlenecks if item.classification == "python_exception_churn"
    )
    assert finding.confidence == 0.6
    assert finding.evidence == (
        "application.retry recorded 20 non-control-flow Python exception propagation events "
        "across 10 calls (2.00 per call); built-in iterator completion is excluded and "
        "events are not unique failures"
    )
    report = render_analysis(analysis, "text")
    assert "python_exception_churn (60%)" in report
    assert "events are not unique failures" in report
    document = json.loads(render_analysis(analysis, "json"))
    assert document["bottlenecks"] == [
        {
            "classification": "python_exception_churn",
            "confidence": 0.6,
            "evidence": finding.evidence,
        }
    ]


def test_batchscope_diagnoses_exception_churn_beyond_public_hotspot_limit(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "exception-heavy-beyond-hotspot-limit.runpack"
    _write_python_exception_batch(
        runpack,
        exception_count=20,
        call_count=10,
        hotter_function_count=100,
    )

    analysis = analyze_runpack(runpack)

    assert len(analysis.python_hotspots) == 100
    assert all(item.name != "application.retry" for item in analysis.python_hotspots)
    assert any(item.classification == "python_exception_churn" for item in analysis.bottlenecks)


def test_batchscope_classifies_exception_churn_at_documented_threshold(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "exception-churn-threshold.runpack"
    _write_python_exception_batch(runpack, exception_count=10, call_count=20)

    analysis = analyze_runpack(runpack)

    assert any(item.classification == "python_exception_churn" for item in analysis.bottlenecks)


@pytest.mark.parametrize(("exception_count", "call_count"), ((9, 1), (10, 21)))
def test_batchscope_does_not_classify_low_exception_churn(
    tmp_path: Path,
    exception_count: int,
    call_count: int,
) -> None:
    runpack = tmp_path / f"low-exception-churn-{exception_count}-{call_count}.runpack"
    _write_python_exception_batch(
        runpack,
        exception_count=exception_count,
        call_count=call_count,
    )

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "python_exception_churn" for item in analysis.bottlenecks)


@pytest.mark.parametrize(
    ("profile_status", "include_observer_integrity", "trace_hook_setter_call_count"),
    (
        ("truncated", True, 0),
        ("complete", False, 0),
        ("complete", True, 1),
    ),
)
def test_batchscope_requires_complete_trustworthy_exception_evidence_for_churn(
    tmp_path: Path,
    profile_status: Literal["complete", "truncated"],
    include_observer_integrity: bool,
    trace_hook_setter_call_count: int,
) -> None:
    runpack = tmp_path / (
        f"incomplete-exception-churn-{profile_status}-{include_observer_integrity}-"
        f"{trace_hook_setter_call_count}.runpack"
    )
    _write_python_exception_batch(
        runpack,
        exception_count=20,
        call_count=10,
        profile_status=profile_status,
        include_observer_integrity=include_observer_integrity,
        trace_hook_setter_call_count=trace_hook_setter_call_count,
    )

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "python_exception_churn" for item in analysis.bottlenecks)


def test_batchscope_requires_reconciled_exception_evidence_for_churn(tmp_path: Path) -> None:
    runpack = tmp_path / "unreconciled-exception-churn.runpack"
    _write_python_exception_batch(
        runpack,
        exception_count=20,
        call_count=10,
        reported_exception_count=21,
    )

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "python_exception_churn" for item in analysis.bottlenecks)


def test_batchscope_does_not_diagnose_legacy_unfiltered_exception_counts(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "legacy-unfiltered-exception-churn.runpack"
    _write_python_exception_batch(
        runpack,
        exception_count=20,
        call_count=10,
        include_control_flow_filter=False,
    )

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    assert analysis.deep_profile.python_exception_capture is not None
    assert analysis.deep_profile.python_exception_capture.control_flow_filter is None
    assert not any(item.classification == "python_exception_churn" for item in analysis.bottlenecks)
    assert "control-flow filtering unavailable; exception-churn diagnosis disabled" in (
        render_analysis(analysis, "text")
    )


def test_batchscope_rejects_control_flow_filter_type_capture_claim(tmp_path: Path) -> None:
    runpack = tmp_path / "unsafe-control-flow-filter.runpack"
    _write_python_exception_batch(
        runpack,
        exception_count=20,
        call_count=10,
        control_flow_filter_exception_types_captured=True,
    )

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    exception_capture = analysis.deep_profile.python_exception_capture
    assert exception_capture is not None
    assert exception_capture.status == "invalid"
    assert exception_capture.control_flow_filter is None
    assert not any(item.classification == "python_exception_churn" for item in analysis.bottlenecks)


def test_batchscope_rejects_observer_integrity_hook_value_claims(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-observer-integrity.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "deep",
                "status": "complete",
                "process_count": 1,
                "function_count": 0,
                "edge_count": 0,
                "truncated": False,
                "dropped_call_count": 0,
                "observer_integrity": {
                    "format_version": 1,
                    "status": "complete",
                    "process_count": 1,
                    "missing_process_count": 0,
                    "profile_hook_setter_call_count": 0,
                    "profile_hook_setter_process_count": 0,
                    "trace_hook_setter_call_count": 0,
                    "trace_hook_setter_process_count": 0,
                    "arguments_captured": False,
                    "locals_captured": False,
                    "hook_values_captured": True,
                },
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-observer-integrity",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    integrity = analysis.deep_profile.observer_integrity
    assert integrity is not None
    assert integrity.status == "invalid"
    assert "observer-integrity metadata was invalid" in render_analysis(analysis, "text")
    document = analysis.as_json_value()
    deep_profile = document["deep_profile"]
    assert isinstance(deep_profile, dict)
    normalized_integrity = deep_profile["observer_integrity"]
    assert isinstance(normalized_integrity, dict)
    assert normalized_integrity["hook_values_captured"] is False


def test_batchscope_marks_inconsistent_snapshot_metrics_invalid(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-snapshot-metrics.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "sample",
                "status": "complete",
                "process_count": 1,
                "process_ids": [42],
                "function_count": 0,
                "edge_count": 0,
                "truncated": False,
                "snapshot_metrics": {
                    "status": "available",
                    "message_count": 1,
                    "payload_bytes": 10,
                    "max_payload_bytes": 20,
                    "serialization_ns": 10,
                    "max_serialization_ns": 5,
                    "checkpoint_message_count": 0,
                    "checkpoint_payload_bytes": 0,
                    "max_checkpoint_payload_bytes": 0,
                    "checkpoint_serialization_ns": 0,
                    "max_checkpoint_serialization_ns": 0,
                },
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-snapshot-metrics",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )
        writer.add_entity(Entity("process", "process", "python", None, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.sample_profile is not None
    assert analysis.sample_profile.snapshot_metrics.status == "invalid"
    assert "snapshot transport metrics were invalid and were ignored" in render_analysis(
        analysis, "text"
    )


def test_batchscope_marks_inconsistent_publication_metrics_invalid(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-publication-metrics.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "sample",
                "status": "complete",
                "process_count": 1,
                "process_ids": [42],
                "function_count": 0,
                "edge_count": 0,
                "truncated": False,
                "publication_metrics": {
                    "status": "available",
                    "fallback_process_count": 1,
                    "fallback_process_ids": [42],
                    "socket_attempted_process_count": 2,
                    "socket_failure_ns": 10,
                    "max_socket_failure_ns": 5,
                },
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-publication-metrics",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )
        writer.add_entity(Entity("process", "process", "python", None, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.sample_profile is not None
    assert analysis.sample_profile.publication_metrics.status == "invalid"
    assert "snapshot publication metrics were invalid and were ignored" in render_analysis(
        analysis, "text"
    )


def test_batchscope_marks_inconsistent_profile_normalization_metrics_invalid(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "invalid-normalization-metrics.runpack"
    metadata: dict[str, JsonValue] = {
        "capture": {
            "instrumentation": {
                "mode": "deep",
                "status": "complete",
                "process_count": 1,
                "process_ids": [42],
                "function_count": 0,
                "edge_count": 0,
                "truncated": False,
                "normalization_metrics": {
                    "status": "available",
                    "duration_ns": 10,
                    "ranking_database_peak_bytes": 20,
                    "ranking_database_limit_bytes": 10,
                },
            }
        }
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "execution",
                "invalid-normalization-metrics",
                0,
                1_000_000_000,
                ("python",),
                str(tmp_path),
                0,
                None,
                metadata,
            )
        )
        writer.add_entity(Entity("process", "process", "python", None, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.deep_profile is not None
    assert analysis.deep_profile.normalization_metrics.status == "invalid"
    assert "profile normalization metrics were invalid and were ignored" in render_analysis(
        analysis, "text"
    )


def test_batchscope_scopes_compute_boundaries_to_the_progress_parent(tmp_path: Path) -> None:
    runpack = tmp_path / "scoped-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("scoped", "scoped", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("run-a", "run", "run-a", 0, 60_000_000),
                _event("compute-a", "stage", "compute", 10_000_000, 50_000_000),
                _event(
                    "progress-a",
                    "progress",
                    "progress",
                    40_000_000,
                    40_000_000,
                    {"completed": 50, "total": 100},
                ),
                _event(
                    "progress-b",
                    "progress",
                    "progress",
                    60_000_000,
                    60_000_000,
                    {"completed": 75, "total": 100},
                ),
                _event("run-b", "run", "run-b", 60_000_000, 100_000_000),
                _event("compute-b", "stage", "compute", 70_000_000, 90_000_000),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("run-a", "compute-a", "parent", 1.0, {}),
                CausalEdge("run-a", "progress-a", "parent", 1.0, {}),
                CausalEdge("run-a", "progress-b", "parent", 1.0, {}),
                CausalEdge("run-b", "compute-b", "parent", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.compute_finished_at_ns == 50_000_000
    assert analysis.throughput.post_compute_seconds == 0.01


def test_batchscope_lifecycle_does_not_double_count_nested_explicit_stages(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "nested-stages.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("nested", "nested", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("outer", "stage", "outer", 0, 100_000_000),
                _event("inner", "stage", "inner", 20_000_000, 80_000_000),
            )
        )
        writer.add_causal_edge(CausalEdge("outer", "inner", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("outer", 0.1)
    ]


def test_kubernetes_lifecycle_uses_the_uniquely_correlated_job(tmp_path: Path) -> None:
    runpack = tmp_path / "multiple-jobs.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job-a", "workload.job", "job-a", 10, 90),
                _event("pod-a", "workload.pod", "pod-a", 20, 80),
                _event("container-a", "workload.container", "container-a", 30, 70),
                _event("job-b", "workload.job", "job-b", 0, 100),
                _event("pod-b", "workload.pod", "pod-b", 1, 99),
                _event("container-b", "workload.container", "container-b", 2, 98),
                _event("trace-root", "operation", "trace-root", 30, 70),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("job-a", "pod-a", "owns", 1.0, {}),
                CausalEdge("pod-a", "container-a", "contains", 1.0, {}),
                CausalEdge("pod-a", "trace-root", "correlates", 1.0, {}),
                CausalEdge("job-b", "pod-b", "owns", 1.0, {}),
                CausalEdge("pod-b", "container-b", "contains", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("provisioning", 10 / 1_000_000_000),
        ("starting", 10 / 1_000_000_000),
        ("executing", 40 / 1_000_000_000),
        ("cleanup", 20 / 1_000_000_000),
    ]


def test_kubernetes_lifecycle_does_not_borrow_unrelated_container_intervals(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unrelated-container.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job-a", "workload.job", "job-a", 10, 90),
                _event("pod-a", "workload.pod", "pod-a", 20, 80),
                _event("job-b", "workload.job", "job-b", 0, 100),
                _event("pod-b", "workload.pod", "pod-b", 1, 99),
                _event("container-b", "workload.container", "container-b", 2, 98),
                _event("trace-root", "operation", "trace-root", 30, 70),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("job-a", "pod-a", "owns", 1.0, {}),
                CausalEdge("pod-a", "trace-root", "correlates", 1.0, {}),
                CausalEdge("job-b", "pod-b", "owns", 1.0, {}),
                CausalEdge("pod-b", "container-b", "contains", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("provisioning", 10 / 1_000_000_000)
    ]


def test_kubernetes_lifecycle_does_not_close_phases_from_partial_container_timing(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "partial-containers.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job", "workload.job", "job", 0, 100),
                Event(
                    "pod",
                    "workload.pod",
                    "pod",
                    "worker",
                    10,
                    None,
                    "test",
                    None,
                    None,
                    {},
                ),
                _event("finished", "workload.container", "finished", 20, 60),
                Event(
                    "open",
                    "workload.container",
                    "open",
                    "worker",
                    None,
                    None,
                    "test",
                    None,
                    None,
                    {},
                ),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("job", "pod", "owns", 1.0, {}),
                CausalEdge("pod", "finished", "contains", 1.0, {}),
                CausalEdge("pod", "open", "contains", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("provisioning", 10 / 1_000_000_000)
    ]


def test_kubernetes_lifecycle_does_not_invent_executing_for_untimed_cohort(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "untimed-containers.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job", "workload.job", "job", 0, 100),
                Event(
                    "pod",
                    "workload.pod",
                    "pod",
                    "worker",
                    None,
                    None,
                    "test",
                    None,
                    None,
                    {},
                ),
                Event(
                    "container",
                    "workload.container",
                    "container",
                    "worker",
                    None,
                    None,
                    "test",
                    None,
                    None,
                    {},
                ),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("job", "pod", "owns", 1.0, {}),
                CausalEdge("pod", "container", "contains", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.lifecycle == ()


def test_kubernetes_lifecycle_requires_job_to_pod_ownership(tmp_path: Path) -> None:
    runpack = tmp_path / "unowned-pod.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("jobs", "jobs", 0, 100, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("job", "workload.job", "job", 10, 90),
                _event("pod", "workload.pod", "unrelated-pod", 20, 80),
                _event("container", "workload.container", "container", 30, 70),
            )
        )
        writer.add_causal_edge(CausalEdge("pod", "container", "contains", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert [(phase.name, phase.duration_seconds) for phase in analysis.lifecycle] == [
        ("executing", 100 / 1_000_000_000)
    ]


def test_batchscope_cli_emits_structured_json(tmp_path: Path) -> None:
    runpack = tmp_path / "batch.runpack"
    _write_batch(runpack)

    result = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.batchscope.cli",
            "inspect",
            str(runpack),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["name"] == "batch"
    assert payload["critical_path"]["certainty"] == "observed"
    assert payload["critical_path"]["event_ids"][:2] == ["process", "run"]
    assert payload["bottlenecks"][0]["classification"] == "serialized_stage"


def test_batchscope_cli_normalizes_overlong_runpack_paths() -> None:
    inspected = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.batchscope.cli",
            "inspect",
            "a" * 5000,
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert inspected.returncode == 2
    assert inspected.stdout == ""
    assert inspected.stderr.startswith("batchscope: could not resolve runpack path: ")
    assert "Traceback" not in inspected.stderr


def test_batchscope_labels_a_single_process_path_as_observed(tmp_path: Path) -> None:
    runpack = tmp_path / "local.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="local")

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "observed"


@pytest.mark.parametrize("missing_count", (1, "invalid"))
def test_missing_causal_evidence_makes_critical_path_inferred(
    tmp_path: Path,
    missing_count: int | str,
) -> None:
    runpack = tmp_path / "incomplete-causality.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "incomplete",
                "incomplete",
                0,
                100,
                (),
                str(tmp_path),
                0,
                None,
                {"otel": {"missing_parent_count": missing_count}},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("work", "operation", "work", 0, 100))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"


def test_ignored_annotation_evidence_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "ignored-annotations.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "incomplete",
                "incomplete",
                0,
                100,
                (),
                str(tmp_path),
                0,
                None,
                {"capture": {"annotation_error": "invalid annotation stream"}},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("work", "operation", "work", 0, 100))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"


def test_critical_path_does_not_subtract_sequential_sibling_intervals(tmp_path: Path) -> None:
    runpack = tmp_path / "siblings.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("siblings", "siblings", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("root", "run", "root", 0, 100_000_000))
        writer.add_event(_event("first", "stage", "first", 0, 40_000_000))
        writer.add_event(_event("second", "stage", "second", 40_000_000, 90_000_000))
        writer.add_causal_edge(CausalEdge("root", "first", "parent", 1.0, {}))
        writer.add_causal_edge(CausalEdge("root", "second", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.1
    assert analysis.critical_path.parallel_slack_seconds == 0.0


def test_critical_path_separates_active_time_waiting_and_parallel_slack(tmp_path: Path) -> None:
    runpack = tmp_path / "waiting.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("waiting", "waiting", 0, 50_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("produce", "operation", "produce", 0, 10_000_000))
        writer.add_event(_event("consume", "operation", "consume", 30_000_000, 40_000_000))
        writer.add_causal_edge(CausalEdge("produce", "consume", "follows", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.04
    assert analysis.critical_path.active_seconds == 0.02
    assert analysis.critical_path.waiting_seconds == 0.02
    assert analysis.critical_path.parallel_slack_seconds == pytest.approx(0.01)
    report = render_analysis(analysis, "text")
    assert "active execution: 20.0ms" in report
    assert "causal waiting: 20.0ms" in report


def test_batchscope_text_report_bounds_repeated_sections() -> None:
    names = tuple(f"event-{index}" for index in range(101))
    analysis = BatchAnalysis(
        execution_id="execution",
        name="bounded",
        total_seconds=1.0,
        lifecycle=tuple(LifecyclePhase(name, 0.01, "explicit") for name in names),
        critical_path=CriticalPath(1.0, 1.0, 0.0, 0.0, names, names, "observed", False),
        throughput=None,
        bottlenecks=tuple(Bottleneck("serialized_stage", name, 0.9) for name in names),
    )

    report = render_analysis(analysis, "text")

    assert "event-99" in report
    assert "event-100" not in report
    assert report.count("1 additional items omitted from text output") == 3


def test_batchscope_json_report_rejects_non_finite_facts() -> None:
    analysis = BatchAnalysis("run", "run", float("nan"), (), None, None, ())

    with pytest.raises(ValueError, match="Out of range float values are not JSON compliant"):
        render_analysis(analysis, "json")


def test_batchscope_text_preserves_submillisecond_duration_evidence() -> None:
    analysis = BatchAnalysis(
        "run",
        "run",
        0.0001,
        (LifecyclePhase("brief", 0.0001, "explicit"),),
        None,
        None,
        (),
    )

    report = render_analysis(analysis, "text")

    assert "total: 100.0µs" in report
    assert "brief                       100.0µs" in report


def _network_capture_metadata(connection_count: int) -> dict[str, JsonValue]:
    return {
        "capture": {
            "instrumentation": {
                "network_capture": {
                    "status": "complete",
                    "observer": "python-network-connection-wrapper",
                    "zero_code": True,
                    "process_count": 1,
                    "connection_count": connection_count,
                    "dropped_connection_count": 0,
                    "callback_error_count": 0,
                    "adapters": ["stdlib.socket.connect"],
                    "server_identity_policy": "redact",
                    "server_address_captured": False,
                    "path_captured": False,
                    "credentials_captured": False,
                    "caller_attribution": {
                        "status": "partial",
                        "caller_count": 0,
                        "attributed_connection_count": 0,
                        "unattributed_connection_count": connection_count,
                        "invalid_caller_count": 0,
                        "callback_error_count": 0,
                        "arguments_captured": False,
                        "locals_captured": False,
                    },
                }
            }
        }
    }


def _network_event(
    index: int,
    *,
    started_at_ns: int,
    duration_ns: int,
    failed: bool = False,
) -> Event:
    attributes: dict[str, JsonValue] = {
        "source": "python-network-connection-wrapper",
        "adapter": "stdlib.socket.connect",
        "transport": "tcp",
        "address_family": "ipv4",
        "server_address": None,
        "server_port": 5432,
        "server_identity_policy": "redact",
        "tls_requested": None,
        "parent_pid": 42,
        "role": "root",
        "outcome": "connect_error" if failed else "connected",
        "path_captured": False,
        "credentials_captured": False,
        "duration_boundary": "connection_ready",
    }
    if failed:
        attributes.update({"error": True, "error.type": "ConnectionRefusedError"})
    return _event(
        f"connection-{index}",
        "network.connect",
        "TCP connect",
        started_at_ns,
        started_at_ns + duration_ns,
        attributes,
    )


def _network_setup_capture_metadata(phase_count: int) -> dict[str, JsonValue]:
    return {
        "capture": {
            "instrumentation": {
                "network_setup_capture": {
                    "status": "complete",
                    "observer": "python-network-setup-wrapper",
                    "zero_code": True,
                    "process_count": 1,
                    "phase_count": phase_count,
                    "dropped_phase_count": 0,
                    "callback_error_count": 0,
                    "adapters": [
                        "stdlib.socket.getaddrinfo",
                        "stdlib.ssl.SSLObject.do_handshake",
                        "stdlib.ssl.SSLSocket.do_handshake",
                    ],
                    "hostname_captured": False,
                    "server_address_captured": False,
                    "sni_captured": False,
                    "certificate_captured": False,
                    "credentials_captured": False,
                    "caller_attribution": {
                        "status": "partial",
                        "caller_count": 0,
                        "attributed_phase_count": 0,
                        "unattributed_phase_count": phase_count,
                        "invalid_caller_count": 0,
                        "callback_error_count": 0,
                        "arguments_captured": False,
                        "locals_captured": False,
                    },
                }
            }
        }
    }


def _network_setup_event(
    index: int,
    *,
    phase: Literal["dns", "tls"],
    started_at_ns: int,
    duration_ns: int,
    failed: bool = False,
    hostname_captured: bool = False,
) -> Event:
    adapter = "stdlib.socket.getaddrinfo" if phase == "dns" else "stdlib.ssl.SSLSocket.do_handshake"
    attributes: dict[str, JsonValue] = {
        "source": "python-network-setup-wrapper",
        "phase": phase,
        "adapter": adapter,
        "parent_pid": 42,
        "role": "root",
        "outcome": "setup_error" if failed else "completed",
        "hostname_captured": hostname_captured,
        "server_address_captured": False,
        "sni_captured": False,
        "certificate_captured": False,
        "credentials_captured": False,
        "duration_boundary": phase,
    }
    if failed:
        attributes.update({"error": True, "error.type": "SSLError"})
    return _event(
        f"network-setup-{index}",
        "network.resolve" if phase == "dns" else "network.tls_handshake",
        "DNS resolution" if phase == "dns" else "TLS handshake",
        started_at_ns,
        started_at_ns + duration_ns,
        attributes,
    )


def _logical_operation_capture_metadata(operation_count: int) -> dict[str, JsonValue]:
    return {
        "capture": {
            "instrumentation": {
                "logical_operation_capture": {
                    "status": "complete",
                    "observer": "python-logical-operation-wrapper",
                    "zero_code": True,
                    "deep_only": True,
                    "process_count": 1,
                    "operation_count": operation_count,
                    "dropped_operation_count": 0,
                    "callback_error_count": 0,
                    "adapters": [
                        "stdlib.concurrent.futures.ThreadPoolExecutor",
                        "stdlib.queue.Queue",
                        "stdlib.sqlite3.Connection",
                        "stdlib.wsgiref",
                    ],
                    "statement_captured": False,
                    "parameters_captured": False,
                    "payload_captured": False,
                    "queue_item_captured": False,
                    "queue_identity_captured": False,
                    "callable_captured": False,
                    "awaitable_captured": False,
                    "task_name_captured": False,
                    "context_captured": False,
                    "arguments_captured": False,
                    "return_value_captured": False,
                    "exception_messages_captured": False,
                    "caller_attribution": {
                        "status": "partial",
                        "caller_count": 0,
                        "attributed_operation_count": 0,
                        "unattributed_operation_count": operation_count,
                        "invalid_caller_count": 0,
                        "callback_error_count": 0,
                        "arguments_captured": False,
                        "locals_captured": False,
                    },
                }
            }
        }
    }


def _logical_operation_event(
    index: int,
    *,
    category: Literal["database", "executor", "queue", "scheduler", "server"],
    operation: Literal["execute", "get", "request", "task"],
    started_at_ns: int,
    duration_ns: int,
    failed: bool = False,
    payload_captured: bool = False,
    callable_captured: bool = False,
    awaitable_captured: bool = False,
    route_captured: bool = False,
) -> Event:
    adapter = (
        "stdlib.sqlite3.Connection"
        if category == "database"
        else "stdlib.concurrent.futures.ThreadPoolExecutor"
        if category == "executor"
        else "stdlib.asyncio.create_task"
        if category == "scheduler"
        else "stdlib.wsgiref"
        if category == "server"
        else "stdlib.queue.Queue"
    )
    attributes: dict[str, JsonValue] = {
        "source": "python-logical-operation-wrapper",
        "category": category,
        "operation": operation,
        "adapter": adapter,
        "parent_pid": 42,
        "role": "root",
        "outcome": "operation_error" if failed else "completed",
        "statement_captured": False,
        "parameters_captured": False,
        "payload_captured": payload_captured,
        "queue_item_captured": False,
        "queue_identity_captured": False,
        "callable_captured": callable_captured,
        "awaitable_captured": awaitable_captured,
        "task_name_captured": False,
        "context_captured": False,
        "arguments_captured": False,
        "return_value_captured": False,
        "http_method_captured": False,
        "route_captured": route_captured,
        "url_captured": False,
        "headers_captured": False,
        "body_captured": False,
        "response_body_captured": False,
        "client_address_captured": False,
        "duration_boundary": (
            "submission_to_completion"
            if category == "executor"
            else "creation_to_completion"
            if category == "scheduler"
            else "request_to_response_completion"
            if category == "server"
            else "logical_operation"
        ),
    }
    if category == "server":
        attributes["status_code"] = 503 if failed else 200
    if failed:
        attributes.update(
            {
                "error": True,
                "error.type": "HTTPStatusError" if category == "server" else "OperationalError",
            }
        )
    name = (
        "Async task"
        if category == "scheduler"
        else {
            "execute": "Database execute",
            "get": "Queue get",
            "request": "Inbound HTTP request",
            "task": "Executor task",
        }[operation]
    )
    return _event(
        f"logical-operation-{index}",
        f"{category}.{operation}",
        name,
        started_at_ns,
        started_at_ns + duration_ns,
        attributes,
    )


def test_batchscope_classifies_connection_failures_setup_latency_and_churn(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "connection-diagnosis.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "connections",
                "connections",
                0,
                1_000_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _network_capture_metadata(12),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            tuple(
                _network_event(
                    index,
                    started_at_ns=index * 10_000_000,
                    duration_ns=400_000_000 if index == 0 else 1_000_000,
                    failed=index in {10, 11},
                )
                for index in range(12)
            )
        )

    analysis = analyze_runpack(runpack)

    findings = {finding.classification: finding for finding in analysis.bottlenecks}
    assert set(findings) == {
        "connection_failures",
        "connection_setup",
        "connection_churn",
    }
    assert findings["connection_failures"].evidence == (
        "2 of 12 retained outbound connection attempts failed"
    )
    assert findings["connection_setup"].evidence == (
        "tcp://<redacted>:5432 took 0.400s to become ready (40% of the run)"
    )
    assert findings["connection_churn"].evidence == (
        "12 outbound connection attempts occurred (12.0/s); inspect pooling or retry behavior"
    )
    assert len(analysis.network_connection_hotspots) == 1
    assert analysis.network_capture is not None
    assert analysis.network_capture.connection_hotspot_count == 1
    hotspot = analysis.network_connection_hotspots[0]
    assert hotspot.adapter == "stdlib.socket.connect"
    assert hotspot.caller is None
    assert hotspot.connection_count == 12
    assert hotspot.connected_connection_count == 10
    assert hotspot.failed_connection_count == 2
    assert hotspot.unfinished_connection_count == 0
    assert hotspot.total_duration_seconds == pytest.approx(0.411)
    assert hotspot.max_duration_seconds == pytest.approx(0.4)
    report = render_analysis(analysis, "text")
    assert "connection_failures (90%)" in report
    assert "connection_setup (80%)" in report
    assert "connection_churn (65%)" in report
    assert "Network connection hotspots" in report
    assert "12 attempts: 10 connected, 2 failed, 0 unfinished" in report


def test_batchscope_does_not_overstate_small_or_slow_connection_sets(tmp_path: Path) -> None:
    runpack = tmp_path / "ordinary-connections.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "connections",
                "connections",
                0,
                3_000_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _network_capture_metadata(10),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            tuple(
                _network_event(
                    index,
                    started_at_ns=index * 100_000_000,
                    duration_ns=49_000_000,
                )
                for index in range(10)
            )
        )

    analysis = analyze_runpack(runpack)

    assert not any(
        finding.classification.startswith("connection_") for finding in analysis.bottlenecks
    )


def test_batchscope_classifies_dns_latency_and_tls_failures(tmp_path: Path) -> None:
    runpack = tmp_path / "network-setup-diagnosis.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "network-setup",
                "network-setup",
                0,
                200_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _network_setup_capture_metadata(2),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _network_setup_event(
                    0,
                    phase="dns",
                    started_at_ns=10_000_000,
                    duration_ns=60_000_000,
                ),
                _network_setup_event(
                    1,
                    phase="tls",
                    started_at_ns=80_000_000,
                    duration_ns=5_000_000,
                    failed=True,
                ),
            )
        )

    analysis = analyze_runpack(runpack)

    findings = {finding.classification: finding for finding in analysis.bottlenecks}
    assert set(findings) == {"dns_latency", "tls_failures"}
    assert findings["dns_latency"].evidence == "DNS resolution took 0.060s (30% of the run)"
    assert findings["tls_failures"].evidence == ("1 of 1 retained tls handshake attempts failed")
    assert analysis.network_setup_capture is not None
    assert analysis.network_setup_capture.hotspot_count == 2
    assert len(analysis.network_setup_hotspots) == 2
    report = render_analysis(analysis, "text")
    assert "Automatic network setup capture" in report
    assert "Network setup hotspots" in report


def test_batchscope_rejects_network_setup_that_claims_to_capture_a_hostname(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unsafe-network-setup.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "unsafe-network-setup",
                "unsafe-network-setup",
                0,
                100_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _network_setup_capture_metadata(1),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _network_setup_event(
                0,
                phase="dns",
                started_at_ns=1,
                duration_ns=1,
                hostname_captured=True,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.network_setup_capture is not None
    assert analysis.network_setup_capture.status == "invalid"
    assert analysis.network_setup_capture.invalid_event_count == 1
    assert analysis.network_setup_phases == ()
    assert analysis.network_setup_hotspots == ()


def test_batchscope_classifies_queue_latency_and_database_failures(tmp_path: Path) -> None:
    runpack = tmp_path / "logical-operation-diagnosis.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "logical-operations",
                "logical-operations",
                0,
                200_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _logical_operation_capture_metadata(2),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _logical_operation_event(
                    0,
                    category="queue",
                    operation="get",
                    started_at_ns=10_000_000,
                    duration_ns=60_000_000,
                ),
                _logical_operation_event(
                    1,
                    category="database",
                    operation="execute",
                    started_at_ns=80_000_000,
                    duration_ns=5_000_000,
                    failed=True,
                ),
            )
        )

    analysis = analyze_runpack(runpack)

    findings = {finding.classification: finding for finding in analysis.bottlenecks}
    assert set(findings) == {"queue_operation_latency", "database_operation_failures"}
    assert findings["queue_operation_latency"].evidence == (
        "Queue get took 0.060s (30% of the run)"
    )
    assert findings["database_operation_failures"].evidence == (
        "1 of 1 retained database operations failed"
    )
    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.hotspot_count == 2
    assert len(analysis.logical_operation_hotspots) == 2
    report = render_analysis(analysis, "text")
    assert "Automatic logical operation capture" in report
    assert "Logical operation hotspots" in report


def test_batchscope_rejects_logical_operation_that_claims_to_capture_payload(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unsafe-logical-operation.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "unsafe-logical-operation",
                "unsafe-logical-operation",
                0,
                100_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _logical_operation_capture_metadata(1),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _logical_operation_event(
                0,
                category="database",
                operation="execute",
                started_at_ns=1,
                duration_ns=1,
                payload_captured=True,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "invalid"
    assert analysis.logical_operation_capture.invalid_event_count == 1
    assert analysis.logical_operations == ()
    assert analysis.logical_operation_hotspots == ()


def test_batchscope_rejects_executor_task_that_claims_to_capture_callable(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unsafe-executor-task.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "unsafe-executor-task",
                "unsafe-executor-task",
                0,
                100_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _logical_operation_capture_metadata(1),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _logical_operation_event(
                0,
                category="executor",
                operation="task",
                started_at_ns=1,
                duration_ns=1,
                callable_captured=True,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "invalid"
    assert analysis.logical_operation_capture.invalid_event_count == 1
    assert analysis.logical_operations == ()


def test_batchscope_rejects_async_task_that_claims_to_capture_awaitable(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unsafe-async-task.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "unsafe-async-task",
                "unsafe-async-task",
                0,
                100_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _logical_operation_capture_metadata(1),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _logical_operation_event(
                0,
                category="scheduler",
                operation="task",
                started_at_ns=1,
                duration_ns=1,
                awaitable_captured=True,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "invalid"
    assert analysis.logical_operation_capture.invalid_event_count == 1
    assert analysis.logical_operations == ()


def test_batchscope_rejects_server_request_that_claims_to_capture_route(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unsafe-server-request.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "unsafe-server-request",
                "unsafe-server-request",
                0,
                100_000_000,
                (),
                str(tmp_path),
                0,
                None,
                _logical_operation_capture_metadata(1),
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _logical_operation_event(
                0,
                category="server",
                operation="request",
                started_at_ns=1,
                duration_ns=1,
                route_captured=True,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.logical_operation_capture is not None
    assert analysis.logical_operation_capture.status == "invalid"
    assert analysis.logical_operation_capture.invalid_event_count == 1
    assert analysis.logical_operations == ()


def test_batchscope_classifies_dominant_external_dependency(tmp_path: Path) -> None:
    runpack = tmp_path / "external.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("external", "external", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("root", "run", "root", 0, 100_000_000))
        writer.add_event(_event("database", "client.request", "database", 20_000_000, 80_000_000))
        writer.add_causal_edge(CausalEdge("root", "database", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    finding = next(
        item for item in analysis.bottlenecks if item.classification == "external_dependency"
    )
    assert finding.evidence == "client operations occupy 0.060s of a 0.100s critical path"
    assert finding.confidence == 0.75


def test_batchscope_marks_external_dependency_inference_as_lower_confidence(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "inferred-external.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("external", "external", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("root", "run", "root", 0, 100_000_000))
        writer.add_event(_event("database", "client.request", "database", 20_000_000, 80_000_000))
        writer.add_causal_edge(CausalEdge("root", "database", "parent", 0.5, {}))

    analysis = analyze_runpack(runpack)

    finding = next(
        item for item in analysis.bottlenecks if item.classification == "external_dependency"
    )
    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"
    assert finding.evidence == "inferred client operations occupy 0.060s of a 0.100s critical path"
    assert finding.confidence == 0.5


def test_batchscope_classifies_failed_scheduling_as_capacity_starvation(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "capacity.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("capacity", "capacity", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "pod", "worker", None, {}))
        writer.add_event(
            _event(
                "failed-scheduling",
                "kubernetes.event",
                "FailedScheduling",
                10_000_000,
                10_000_000,
                {"message": "0/4 nodes are available: insufficient cpu"},
            )
        )

    analysis = analyze_runpack(runpack)

    finding = next(
        item for item in analysis.bottlenecks if item.classification == "capacity_starvation"
    )
    assert finding.evidence == "1 Kubernetes FailedScheduling event indicates placement failure"
    assert finding.confidence == 0.85


def _analyze_kubernetes_scheduling_failure(tmp_path: Path, involved_pod_uid: str) -> BatchAnalysis:
    source = tmp_path / "source.runpack"
    snapshot = tmp_path / "snapshot.json"
    output = tmp_path / "enriched.runpack"
    with RunpackWriter(source) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 10_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "pod", "target", None, {"k8s.pod.uid": "target-pod"}))
        writer.add_event(_event("root", "operation", "work", 2_000_000_000, 3_000_000_000))
    snapshot.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "kind": "Job",
                        "metadata": {
                            "uid": "target-job",
                            "name": "target-job",
                            "creationTimestamp": "1970-01-01T00:00:01Z",
                        },
                        "status": {},
                    },
                    {
                        "kind": "Pod",
                        "metadata": {
                            "uid": "target-pod",
                            "name": "target-pod",
                            "creationTimestamp": "1970-01-01T00:00:01Z",
                            "ownerReferences": [{"uid": "target-job"}],
                        },
                        "spec": {"containers": []},
                        "status": {"phase": "Running"},
                    },
                    {
                        "kind": "Pod",
                        "metadata": {
                            "uid": "unrelated-pod",
                            "name": "unrelated-pod",
                            "creationTimestamp": "1970-01-01T00:00:01Z",
                        },
                        "spec": {"containers": []},
                        "status": {"phase": "Pending"},
                    },
                    {
                        "kind": "Event",
                        "metadata": {
                            "uid": "failed-event",
                            "name": "failed-event",
                            "creationTimestamp": "1970-01-01T00:00:02Z",
                        },
                        "involvedObject": {"uid": involved_pod_uid},
                        "reason": "FailedScheduling",
                        "message": "pod has no capacity",
                        "type": "Warning",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    import_kubernetes_snapshot(source, snapshot, output)

    return analyze_runpack(output)


def test_batchscope_ignores_failed_scheduling_from_an_unrelated_pod(tmp_path: Path) -> None:
    analysis = _analyze_kubernetes_scheduling_failure(tmp_path, "unrelated-pod")

    assert not any(item.classification == "capacity_starvation" for item in analysis.bottlenecks)


def test_batchscope_keeps_failed_scheduling_from_the_correlated_pod(tmp_path: Path) -> None:
    analysis = _analyze_kubernetes_scheduling_failure(tmp_path, "target-pod")

    finding = next(
        item for item in analysis.bottlenecks if item.classification == "capacity_starvation"
    )
    assert finding.evidence == "1 Kubernetes FailedScheduling event indicates placement failure"


def test_batchscope_does_not_attribute_failed_scheduling_when_cohort_is_ambiguous(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "ambiguous-kubernetes-cohort.runpack"
    events = (
        _event("job-a", "workload.job", "job-a", 0, 1),
        _event("pod-a", "workload.pod", "pod-a", 0, 1),
        _event("local-a", "operation", "local-a", 0, 1),
        _event("job-b", "workload.job", "job-b", 0, 1),
        _event("pod-b", "workload.pod", "pod-b", 0, 1),
        _event("local-b", "operation", "local-b", 0, 1),
        _event("unrelated-job", "workload.job", "unrelated-job", 0, 1),
        _event("unrelated-pod", "workload.pod", "unrelated-pod", 0, 1),
        _event("unrelated-failure", "kubernetes.event", "FailedScheduling", 0, 1),
    )
    edges = (
        CausalEdge("job-a", "pod-a", "owns", 1.0, {}),
        CausalEdge("pod-a", "local-a", "correlates", 1.0, {}),
        CausalEdge("job-b", "pod-b", "owns", 1.0, {}),
        CausalEdge("pod-b", "local-b", "correlates", 1.0, {}),
        CausalEdge("unrelated-job", "unrelated-pod", "owns", 1.0, {}),
        CausalEdge("unrelated-pod", "unrelated-failure", "emits", 1.0, {}),
    )
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event_graph(events, edges)

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "capacity_starvation" for item in analysis.bottlenecks)


@pytest.mark.parametrize("event_kind", ("operation", "message.consume", "server.request"))
def test_batchscope_classifies_a_dominant_operation_straggler(
    tmp_path: Path, event_kind: str
) -> None:
    runpack = tmp_path / "straggler.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("straggler", "straggler", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            _event(
                f"task-{index}",
                event_kind,
                "task",
                index * 1_000_000,
                90_000_000 if index == 4 else 20_000_000,
            )
            for index in range(5)
        )

    analysis = analyze_runpack(runpack)

    finding = next(item for item in analysis.bottlenecks if item.classification == "straggler_tail")
    assert finding.evidence == ("task max duration 0.086s versus 0.019s median across 5 operations")
    assert finding.confidence == 0.8


def test_serialized_stage_uses_enclosing_logical_run_instead_of_process_startup(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "logical-window.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("logical", "logical", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("process", "process.run", "python", 0, 1_000_000_000))
        writer.add_event(_event("run", "run", "workload", 800_000_000, 900_000_000))
        writer.add_event(
            _event(
                "persist",
                "stage",
                "persist",
                850_000_000,
                890_000_000,
                {"concurrency": 1},
            )
        )

    analysis = analyze_runpack(runpack)

    assert {item.classification for item in analysis.bottlenecks} == {"serialized_stage"}


def test_queue_wait_uses_its_causal_run_as_the_comparison_window(tmp_path: Path) -> None:
    runpack = tmp_path / "queue-wait.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("queue", "queue", 0, 100_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_events(
            (
                Event(
                    "logical-run",
                    "run",
                    "workflow",
                    None,
                    50_000_000_000,
                    60_000_000_000,
                    "temporal.history",
                    None,
                    None,
                    {},
                ),
                Event(
                    "activity",
                    "temporal.activity",
                    "export",
                    None,
                    50_000_000_000,
                    59_000_000_000,
                    "temporal.history",
                    None,
                    None,
                    {},
                ),
                Event(
                    "queue",
                    "queue.wait",
                    "export queue",
                    None,
                    50_000_000_000,
                    54_000_000_000,
                    "temporal.history",
                    None,
                    None,
                    {"temporal.activity_type": "export"},
                ),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("logical-run", "activity", "parent", 1.0, {}),
                CausalEdge("activity", "queue", "parent", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    finding = next(item for item in analysis.bottlenecks if item.classification == "queue_wait")
    assert finding.evidence == "export waited 4.000s in queue (40% of its run)"


def test_serialized_stage_prefers_causal_run_over_shorter_unrelated_enclosure(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "causal-long-run.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("causal", "causal", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("owned-run", "run", "owned-run", 0, 1_000_000_000),
                _event("unrelated-run", "run", "unrelated-run", 400_000_000, 600_000_000),
                _event(
                    "persist",
                    "stage",
                    "persist",
                    450_000_000,
                    550_000_000,
                    {"concurrency": 1},
                ),
            )
        )
        writer.add_causal_edge(CausalEdge("owned-run", "persist", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "serialized_stage" for item in analysis.bottlenecks)


def test_serialized_stage_uses_short_causal_run_over_longer_unrelated_enclosure(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "causal-short-run.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("causal", "causal", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("unrelated-run", "run", "unrelated-run", 0, 1_000_000_000),
                _event("owned-run", "run", "owned-run", 400_000_000, 600_000_000),
                _event(
                    "persist",
                    "stage",
                    "persist",
                    450_000_000,
                    550_000_000,
                    {"concurrency": 1},
                ),
            )
        )
        writer.add_causal_edge(CausalEdge("owned-run", "persist", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert any(item.classification == "serialized_stage" for item in analysis.bottlenecks)


def test_serialized_stage_does_not_guess_between_multiple_causal_runs(tmp_path: Path) -> None:
    runpack = tmp_path / "ambiguous-causal-runs.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("causal", "causal", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("long-run", "run", "long-run", 0, 1_000_000_000),
                _event("short-run", "run", "short-run", 400_000_000, 600_000_000),
                _event(
                    "persist",
                    "stage",
                    "persist",
                    450_000_000,
                    550_000_000,
                    {"concurrency": 1},
                ),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("long-run", "persist", "parent", 1.0, {}),
                CausalEdge("short-run", "persist", "parent", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "serialized_stage" for item in analysis.bottlenecks)


@pytest.mark.parametrize("concurrency", (True, 10**1000))
def test_serialized_stage_ignores_invalid_concurrency_values(
    tmp_path: Path, concurrency: int | bool
) -> None:
    runpack = tmp_path / "invalid-concurrency.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("invalid", "invalid", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "persist",
                "stage",
                "persist",
                0,
                100_000_000,
                {"concurrency": concurrency},
            )
        )

    analysis = analyze_runpack(runpack)

    assert not any(item.classification == "serialized_stage" for item in analysis.bottlenecks)


def test_batchscope_does_not_sum_parallel_side_branch_clients_as_a_bottleneck(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "parallel-clients.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("parallel", "parallel", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("root", "run", "root", 0, 100_000_000))
        writer.add_event(_event("compute", "operation", "compute", 0, 90_000_000))
        writer.add_causal_edge(CausalEdge("root", "compute", "parent", 1.0, {}))
        for index in range(60):
            event_id = f"client-{index:02}"
            writer.add_event(_event(event_id, "client.request", "parallel-client", 0, 1_000_000))
            writer.add_causal_edge(CausalEdge("root", event_id, "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.1
    assert not any(item.classification == "external_dependency" for item in analysis.bottlenecks)


def test_batchscope_calculates_observed_post_compute_drain_rate(tmp_path: Path) -> None:
    runpack = tmp_path / "drain.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("drain", "drain", 0, 150_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("run", "run", "logical-run", 0, 100_000_000))
        writer.add_event(_event("compute", "stage", "compute", 0, 50_000_000))
        writer.add_event(
            _event(
                "progress-1",
                "progress",
                "progress",
                50_000_000,
                50_000_000,
                {"completed": 40, "total": 100, "series": "items"},
            )
        )
        writer.add_event(
            _event(
                "progress-2",
                "progress",
                "progress",
                70_000_000,
                70_000_000,
                {"completed": 70, "total": 100, "series": "items"},
            )
        )
        writer.add_event(
            _event(
                "progress-3",
                "progress",
                "progress",
                90_000_000,
                90_000_000,
                {"completed": 100, "total": 100, "series": "items"},
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("run", "compute", "parent", 1.0, {}),
                CausalEdge("run", "progress-1", "parent", 1.0, {}),
                CausalEdge("run", "progress-2", "parent", 1.0, {}),
                CausalEdge("run", "progress-3", "parent", 1.0, {}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.remaining_at_compute_completion == 60.0
    assert analysis.throughput.post_compute_seconds == 0.05
    assert analysis.throughput.post_compute_rate_per_second == 1500.0


def test_completed_progress_needs_no_rate_to_estimate_zero_drain(tmp_path: Path) -> None:
    runpack = tmp_path / "completed.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("completed", "completed", 0, 10_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "progress",
                "progress",
                "progress",
                10_000_000,
                10_000_000,
                {"completed": 100, "total": 100},
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.remaining == 0.0
    assert analysis.throughput.rate_per_second is None
    assert analysis.throughput.estimated_drain_seconds == 0.0


def test_throughput_ignores_progress_integers_outside_float_range(tmp_path: Path) -> None:
    runpack = tmp_path / "oversized-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "progress",
                "progress",
                "progress",
                1,
                1,
                {"completed": 10**1000, "total": 10**1000},
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_does_not_round_large_integer_progress_to_complete(tmp_path: Path) -> None:
    runpack = tmp_path / "imprecise-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            _event(
                "progress",
                "progress",
                "progress",
                1,
                1,
                {"completed": 2**53, "total": 2**53 + 1},
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_omits_overflowed_rates_from_json(tmp_path: Path) -> None:
    runpack = tmp_path / "overflowed-rate.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event(
                    "first",
                    "progress",
                    "progress",
                    0,
                    0,
                    {"completed": 0.0, "total": sys.float_info.max},
                ),
                _event(
                    "last",
                    "progress",
                    "progress",
                    1,
                    1,
                    {"completed": sys.float_info.max, "total": sys.float_info.max},
                ),
            )
        )

    analysis = analyze_runpack(runpack)
    rendered = render_analysis(analysis, "json")

    assert analysis.throughput is not None
    assert analysis.throughput.rate_per_second is None
    assert "Infinity" not in rendered


def test_throughput_omits_overflowed_drain_estimates(tmp_path: Path) -> None:
    runpack = tmp_path / "overflowed-drain.runpack"
    finished_at_ns = (1 << 63) - 1
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "progress",
                "progress",
                0,
                finished_at_ns,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event(
                    "first",
                    "progress",
                    "progress",
                    0,
                    0,
                    {"completed": 0.0, "total": sys.float_info.max},
                ),
                _event(
                    "last",
                    "progress",
                    "progress",
                    finished_at_ns,
                    finished_at_ns,
                    {"completed": 1.0, "total": sys.float_info.max},
                ),
            )
        )

    throughput = analyze_runpack(runpack).throughput

    assert throughput is not None
    assert throughput.rate_per_second is not None
    assert throughput.estimated_drain_seconds is None


def test_throughput_rejects_invalid_progress_and_does_not_infer_across_resets(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "ambiguous-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("boolean", "progress", "progress", 0, 0, {"completed": True, "total": 10}),
                _event("first", "progress", "progress", 10, 10, {"completed": 8, "total": 10}),
                _event("reset", "progress", "progress", 20, 20, {"completed": 2, "total": 10}),
                _event("invalid", "progress", "progress", 30, 30, {"completed": 11, "total": 10}),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.completed == 2
    assert analysis.throughput.total == 10
    assert analysis.throughput.remaining == 8
    assert analysis.throughput.rate_per_second is None
    assert analysis.throughput.estimated_drain_seconds is None


def test_throughput_rejects_conflicting_samples_at_the_same_timestamp(tmp_path: Path) -> None:
    runpack = tmp_path / "conflicting-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("first", "progress", "progress", 10, 10, {"completed": 2, "total": 10}),
                _event(
                    "conflict",
                    "progress",
                    "progress",
                    10,
                    10,
                    {"completed": 9, "total": 10},
                ),
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_collapses_identical_samples_at_the_same_timestamp(tmp_path: Path) -> None:
    runpack = tmp_path / "duplicate-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "progress",
                "progress",
                0,
                20_000_000,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event(
                    "first",
                    "progress",
                    "progress",
                    10_000_000,
                    10_000_000,
                    {"completed": 2, "total": 10},
                ),
                _event(
                    "second",
                    "progress",
                    "progress",
                    20_000_000,
                    20_000_000,
                    {"completed": 4, "total": 10},
                ),
                _event(
                    "duplicate",
                    "progress",
                    "progress",
                    20_000_000,
                    20_000_000,
                    {"completed": 4, "total": 10},
                ),
            )
        )

    throughput = analyze_runpack(runpack).throughput

    assert throughput is not None
    assert throughput.completed == 4
    assert throughput.rate_per_second == 200


def test_throughput_uses_source_sequence_when_uncertain_timestamps_are_reversed(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "sequenced-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 200, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                Event(
                    "first",
                    "progress",
                    "progress",
                    "worker",
                    100,
                    100,
                    "test",
                    20,
                    1,
                    {"completed": 20, "total": 100},
                ),
                Event(
                    "second",
                    "progress",
                    "progress",
                    "worker",
                    90,
                    90,
                    "test",
                    20,
                    2,
                    {"completed": 40, "total": 100},
                ),
            )
        )

    throughput = analyze_runpack(runpack).throughput

    assert throughput is not None
    assert throughput.completed == 40
    assert throughput.remaining == 60
    assert throughput.rate_per_second is None
    assert throughput.estimated_drain_seconds is None


def test_throughput_does_not_order_shared_series_by_cross_entity_sequences(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "multiple-sequence-sources.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 200, (), str(tmp_path), 0, None, {})
        )
        writer.add_entities(
            (
                Entity("worker-a", "worker", "worker-a", None, {}),
                Entity("worker-b", "worker", "worker-b", None, {}),
            )
        )
        writer.add_events(
            (
                Event(
                    "from-a",
                    "progress",
                    "progress",
                    "worker-a",
                    100,
                    100,
                    "test",
                    20,
                    1,
                    {"series": "shared", "completed": 20, "total": 100},
                ),
                Event(
                    "from-b",
                    "progress",
                    "progress",
                    "worker-b",
                    90,
                    90,
                    "test",
                    20,
                    2,
                    {"series": "shared", "completed": 40, "total": 100},
                ),
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_does_not_treat_unowned_sequences_as_one_source(tmp_path: Path) -> None:
    runpack = tmp_path / "unowned-sequence-sources.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 200, (), str(tmp_path), 0, None, {})
        )
        writer.add_events(
            (
                Event(
                    "first",
                    "progress",
                    "progress",
                    None,
                    100,
                    100,
                    "test",
                    20,
                    1,
                    {"completed": 20, "total": 100},
                ),
                Event(
                    "second",
                    "progress",
                    "progress",
                    None,
                    90,
                    90,
                    "test",
                    20,
                    2,
                    {"completed": 40, "total": 100},
                ),
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_does_not_merge_progress_from_multiple_entities(tmp_path: Path) -> None:
    runpack = tmp_path / "multiple-progress-series.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entities(
            (
                Entity("worker-a", "worker", "worker-a", None, {}),
                Entity("worker-b", "worker", "worker-b", None, {}),
            )
        )
        writer.add_events(
            (
                Event(
                    "progress-a",
                    "progress",
                    "progress",
                    "worker-a",
                    10,
                    10,
                    "test",
                    None,
                    None,
                    {"completed": 5, "total": 10},
                ),
                Event(
                    "progress-b",
                    "progress",
                    "progress",
                    "worker-b",
                    20,
                    20,
                    "test",
                    None,
                    None,
                    {"completed": 6, "total": 10},
                ),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is None


def test_throughput_does_not_infer_across_clock_domains(tmp_path: Path) -> None:
    runpack = tmp_path / "cross-clock-progress.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                Event(
                    "progress-a",
                    "progress",
                    "progress",
                    "worker",
                    0,
                    0,
                    "clock-a",
                    None,
                    None,
                    {"completed": 0, "total": 100},
                ),
                Event(
                    "progress-b",
                    "progress",
                    "progress",
                    "worker",
                    1_000_000_000,
                    1_000_000_000,
                    "clock-b",
                    None,
                    None,
                    {"completed": 100, "total": 100},
                ),
            )
        )

    assert analyze_runpack(runpack).throughput is None


def test_throughput_does_not_apply_a_cross_clock_compute_boundary(tmp_path: Path) -> None:
    runpack = tmp_path / "cross-clock-compute.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 1_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            Event(
                "compute",
                "stage",
                "compute",
                "worker",
                0,
                500_000_000,
                "compute-clock",
                None,
                None,
                {},
            )
        )
        writer.add_events(
            Event(
                f"progress-{index}",
                "progress",
                "progress",
                "worker",
                timestamp,
                timestamp,
                "progress-clock",
                None,
                index,
                {"completed": completed, "total": 100},
            )
            for index, (timestamp, completed) in enumerate(
                (
                    (100_000_000, 10),
                    (400_000_000, 40),
                    (700_000_000, 70),
                    (900_000_000, 90),
                ),
                1,
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.completed == 90
    assert analysis.throughput.remaining == 10
    assert analysis.throughput.rate_per_second == 100
    assert analysis.throughput.estimated_drain_seconds == 0.1
    assert analysis.throughput.compute_finished_at_ns is None
    assert analysis.throughput.remaining_at_compute_completion is None
    assert analysis.throughput.post_compute_seconds is None
    assert analysis.throughput.post_compute_rate_per_second is None
    assert "remaining at compute completion" not in render_analysis(analysis, "text")


def test_throughput_does_not_guess_between_multiple_parent_scopes(tmp_path: Path) -> None:
    runpack = tmp_path / "ambiguous-progress-parents.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 30, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                _event("scope-a", "run", "scope-a", 0, 30),
                _event("scope-b", "run", "scope-b", 0, 30),
                _event("progress-a", "progress", "progress", 10, 10, {"completed": 5, "total": 10}),
            )
        )
        writer.add_causal_edges(
            CausalEdge(parent, "progress-a", "parent", 1.0, {}) for parent in ("scope-a", "scope-b")
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is None


def test_throughput_uses_the_progress_entity_compute_boundary(tmp_path: Path) -> None:
    runpack = tmp_path / "scoped-compute.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("progress", "progress", 0, 100, (), str(tmp_path), 0, None, {})
        )
        writer.add_entities(
            (
                Entity("worker", "worker", "worker", None, {}),
                Entity("other", "service", "other", None, {}),
            )
        )
        writer.add_events(
            (
                Event("compute", "stage", "compute", "worker", 0, 40, "test", None, None, {}),
                Event(
                    "other-compute",
                    "stage",
                    "compute",
                    "other",
                    0,
                    80,
                    "test",
                    None,
                    None,
                    {},
                ),
                Event(
                    "progress-a",
                    "progress",
                    "progress",
                    "worker",
                    20,
                    20,
                    "test",
                    None,
                    None,
                    {"completed": 20, "total": 100},
                ),
                Event(
                    "progress-b",
                    "progress",
                    "progress",
                    "worker",
                    60,
                    60,
                    "test",
                    None,
                    None,
                    {"completed": 60, "total": 100},
                ),
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.throughput is not None
    assert analysis.throughput.compute_finished_at_ns == 40
    assert analysis.throughput.remaining_at_compute_completion == 80


def test_clock_inconsistency_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "clock-skew.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("skew", "skew", 0, 120_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("parent", "run", "parent", 10_000_000, 100_000_000))
        writer.add_event(_event("child", "operation", "child", 0, 120_000_000))
        writer.add_causal_edge(CausalEdge("parent", "child", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.12
    assert analysis.critical_path.certainty == "inferred"


def test_reversed_non_parent_causality_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "reversed-cause.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("reversed", "reversed", 0, 100_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("cause", "operation", "cause", 50_000_000, 60_000_000))
        writer.add_event(_event("effect", "operation", "effect", 10_000_000, 20_000_000))
        writer.add_causal_edge(CausalEdge("cause", "effect", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"


def test_clock_uncertainty_can_cover_apparent_parent_skew(tmp_path: Path) -> None:
    runpack = tmp_path / "uncertain-clock.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("uncertain", "uncertain", 0, 100, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(Event("parent", "run", "parent", "worker", 10, 90, "test", 5, None, {}))
        writer.add_event(Event("child", "operation", "child", "worker", 5, 95, "test", 5, None, {}))
        writer.add_causal_edge(CausalEdge("parent", "child", "parent", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "observed"


def test_causal_cycle_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "cycle.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("cycle", "cycle", 0, 20_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("first", "operation", "first", 0, 10_000_000))
        writer.add_event(_event("second", "operation", "second", 10_000_000, 20_000_000))
        writer.add_causal_edge(CausalEdge("first", "second", "follows", 1.0, {}))
        writer.add_causal_edge(CausalEdge("second", "first", "follows", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.cycle_detected is True
    assert analysis.critical_path.certainty == "inferred"
    assert "causal cycle detected; critical path is inferred" in render_analysis(analysis, "text")


def test_low_confidence_causal_edge_makes_critical_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "uncertain.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("uncertain", "uncertain", 0, 20_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("first", "operation", "first", 0, 10_000_000))
        writer.add_event(_event("second", "operation", "second", 10_000_000, 20_000_000))
        writer.add_causal_edge(CausalEdge("first", "second", "inferred", 0.4, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.certainty == "inferred"


def test_cross_clock_domain_critical_path_is_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "cross-clock.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("cross-clock", "cross-clock", 0, 20_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            Event(
                "first",
                "operation",
                "first",
                "worker",
                0,
                10_000_000,
                "producer-clock",
                None,
                None,
                {},
            )
        )
        writer.add_event(
            Event(
                "second",
                "operation",
                "second",
                "worker",
                10_000_000,
                20_000_000,
                "consumer-clock",
                None,
                None,
                {},
            )
        )
        writer.add_causal_edge(CausalEdge("first", "second", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == 0.02
    assert analysis.critical_path.certainty == "inferred"


def test_instant_events_preserve_causal_waiting_on_the_critical_path(tmp_path: Path) -> None:
    runpack = tmp_path / "instants.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("instants", "instants", 0, 50_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("publish", "event", "publish", 10_000_000, 10_000_000))
        writer.add_event(_event("consume", "event", "consume", 40_000_000, 40_000_000))
        writer.add_causal_edge(CausalEdge("publish", "consume", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.event_ids == ("publish", "consume")
    assert analysis.critical_path.duration_seconds == 0.03
    assert analysis.critical_path.active_seconds == 0.0
    assert analysis.critical_path.waiting_seconds == 0.03
    assert analysis.critical_path.certainty == "observed"


def test_critical_path_ties_prefer_observed_work_over_point_chains(tmp_path: Path) -> None:
    runpack = tmp_path / "active-tie.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("active-tie", "active-tie", 0, 30_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("work", "operation", "work", 0, 30_000_000))
        writer.add_event(_event("published", "event", "published", 0, 0))
        writer.add_event(_event("observed", "event", "observed", 30_000_000, 30_000_000))
        writer.add_causal_edge(CausalEdge("published", "observed", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.event_ids == ("work",)
    assert analysis.critical_path.duration_seconds == 0.03
    assert analysis.critical_path.active_seconds == 0.03
    assert analysis.critical_path.waiting_seconds == 0.0


def test_untimed_events_preserve_causality_but_make_the_path_inferred(tmp_path: Path) -> None:
    runpack = tmp_path / "partial-timing.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "partial-timing",
                "partial-timing",
                0,
                50_000_000,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(_event("produce", "operation", "produce", 0, 10_000_000))
        writer.add_event(
            Event(
                "handoff",
                "event",
                "handoff",
                "worker",
                None,
                None,
                "unknown",
                None,
                None,
                {},
            )
        )
        writer.add_event(_event("consume", "operation", "consume", 30_000_000, 40_000_000))
        writer.add_causal_edge(CausalEdge("produce", "handoff", "causes", 1.0, {}))
        writer.add_causal_edge(CausalEdge("handoff", "consume", "causes", 1.0, {}))

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.event_ids == ("produce", "handoff", "consume")
    assert analysis.critical_path.duration_seconds == 0.04
    assert analysis.critical_path.active_seconds == 0.02
    assert analysis.critical_path.waiting_seconds == 0.02
    assert analysis.critical_path.certainty == "inferred"


def test_batchscope_handles_causal_chains_beyond_python_recursion_limit(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "long-chain.runpack"
    chain_length = 10_000
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "long-chain",
                "long-chain",
                0,
                chain_length,
                (),
                str(tmp_path),
                0,
                None,
                {},
            )
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
    with sqlite3.connect(runpack) as connection:
        connection.executemany(
            """
            INSERT INTO events(
                id, kind, name, entity_id, started_at_ns, finished_at_ns,
                clock_domain, uncertainty_ns, sequence, attributes_json
            ) VALUES (?, 'operation', ?, 'worker', ?, ?, 'test', NULL, ?, '{}')
            """,
            (
                (f"event-{index}", f"event-{index}", index, index + 1, index)
                for index in range(chain_length)
            ),
        )
        connection.executemany(
            """
            INSERT INTO causal_edges(
                source_event_id, target_event_id, kind, confidence, attributes_json
            ) VALUES (?, ?, 'parent', 1.0, '{}')
            """,
            ((f"event-{index}", f"event-{index + 1}") for index in range(chain_length - 1)),
        )

    analysis = analyze_runpack(runpack)

    assert analysis.critical_path is not None
    assert analysis.critical_path.duration_seconds == pytest.approx(chain_length / 1e9)
    assert len(analysis.critical_path.event_ids) == chain_length


def test_invalid_subprocess_caller_edge_does_not_discard_the_boundary(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-caller.runpack"
    semantic: dict[str, JsonValue] = {
        "status": "complete",
        "observer": "python-subprocess-wrapper",
        "zero_code": True,
        "process_count": 1,
        "subprocess_count": 1,
        "dropped_subprocess_count": 0,
        "callback_error_count": 0,
        "arguments_captured": False,
        "environment_captured": False,
        "working_directory_captured": False,
        "caller_attribution": {
            "status": "complete",
            "caller_count": 1,
            "attributed_subprocess_count": 1,
            "unattributed_subprocess_count": 0,
            "invalid_caller_count": 0,
            "callback_error_count": 0,
            "arguments_captured": False,
            "locals_captured": False,
        },
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "invalid-caller",
                "invalid-caller",
                0,
                10_000_000,
                (),
                str(tmp_path),
                0,
                None,
                {"capture": {"instrumentation": {"semantic_capture": semantic}}},
            )
        )
        writer.add_entity(Entity("worker", "process", "python", None, {}))
        writer.add_event(
            _event(
                "subprocess",
                "subprocess.run",
                "python",
                1,
                2,
                {
                    "source": "python-subprocess-wrapper",
                    "parent_pid": 42,
                    "child_pid": 43,
                    "role": "root",
                    "shell": False,
                    "outcome": "exited",
                    "exit_code": 0,
                    "arguments_captured": False,
                    "caller_event_id": "caller",
                },
            )
        )
        writer.add_event(
            Event(
                "caller",
                "python.callsite",
                "application.launch",
                "worker",
                None,
                None,
                None,
                None,
                None,
                {
                    "source": "python-subprocess-wrapper",
                    "module": "application",
                    "qualname": "launch",
                    "filename": "/work/application.py",
                    "firstlineno": 4,
                    "scope": "application",
                    "subprocess_count": 2,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
            )
        )
        writer.add_causal_edge(
            CausalEdge(
                "caller",
                "subprocess",
                "launches",
                1.0,
                {"source": "python-subprocess-wrapper", "observation": "exact"},
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.semantic_capture is not None
    assert analysis.semantic_capture.status == "complete"
    assert analysis.semantic_capture.caller_attribution_status == "invalid"
    assert analysis.subprocess_calls[0].caller is None


def test_invalid_http_caller_edge_does_not_discard_the_boundary(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-http-caller.runpack"
    http_capture: dict[str, JsonValue] = {
        "status": "complete",
        "observer": "python-http-client-wrapper",
        "zero_code": True,
        "process_count": 1,
        "request_count": 1,
        "dropped_request_count": 0,
        "callback_error_count": 0,
        "server_identity_policy": "redact",
        "method_captured": True,
        "scheme_captured": True,
        "server_address_captured": False,
        "path_captured": False,
        "query_captured": False,
        "headers_captured": False,
        "body_captured": False,
        "response_body_captured": False,
        "caller_attribution": {
            "status": "complete",
            "caller_count": 1,
            "attributed_request_count": 1,
            "unattributed_request_count": 0,
            "invalid_caller_count": 0,
            "callback_error_count": 0,
            "arguments_captured": False,
            "locals_captured": False,
        },
    }
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "invalid-http-caller",
                "invalid-http-caller",
                0,
                10_000_000,
                (),
                str(tmp_path),
                0,
                None,
                {"capture": {"instrumentation": {"http_capture": http_capture}}},
            )
        )
        writer.add_entity(Entity("worker", "process", "python", None, {}))
        writer.add_event(
            _event(
                "request",
                "http.client.request",
                "HTTP GET",
                1,
                2,
                {
                    "source": "python-http-client-wrapper",
                    "method": "GET",
                    "scheme": "https",
                    "server_address": None,
                    "server_port": 443,
                    "server_identity_policy": "redact",
                    "parent_pid": 42,
                    "role": "root",
                    "outcome": "response",
                    "status_code": 200,
                    "headers_captured": False,
                    "body_captured": False,
                    "path_captured": False,
                    "query_captured": False,
                    "response_body_captured": False,
                    "duration_boundary": "response_headers",
                    "caller_event_id": "http-caller",
                },
            )
        )
        writer.add_event(
            Event(
                "http-caller",
                "python.callsite",
                "application.fetch",
                "worker",
                None,
                None,
                None,
                None,
                None,
                {
                    "source": "python-http-client-wrapper",
                    "module": "application",
                    "qualname": "fetch",
                    "filename": "/work/application.py",
                    "firstlineno": 4,
                    "scope": "application",
                    "http_request_count": 2,
                    "arguments_captured": False,
                    "locals_captured": False,
                },
            )
        )
        writer.add_causal_edge(
            CausalEdge(
                "http-caller",
                "request",
                "requests",
                1.0,
                {"source": "python-http-client-wrapper", "observation": "exact"},
            )
        )

    analysis = analyze_runpack(runpack)

    assert analysis.http_capture is not None
    assert analysis.http_capture.status == "complete"
    assert analysis.http_capture.caller_attribution_status == "invalid"
    assert analysis.http_requests[0].caller is None
