from __future__ import annotations

import json
from pathlib import Path

import pytest

import runtime_tools.inspect as inspect_module
from runtime_tools.inspect import inspect_runpack, render_causal_tree, render_summary
from runtime_tools.model import CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.storage import RunpackWriter


def test_text_inspection_labels_signaled_process_outcomes(tmp_path: Path) -> None:
    runpack = tmp_path / "signaled.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("signaled", "signaled", 0, 1, (), str(tmp_path), -15, None, {})
        )

    summary = render_summary(inspect_runpack(runpack), "text")

    assert "outcome:  failed (signal 15)" in summary


def test_text_inspection_keeps_unknown_runtime_distinct_from_open_execution(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "unknown-runtime.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("completed", "completed", 0, None, (), str(tmp_path), 0, None, {})
        )

    summary = inspect_runpack(runpack)
    rendered = render_summary(summary, "text")
    structured = json.loads(render_summary(summary, "json"))

    assert "outcome:  success (exit 0)" in rendered
    assert "runtime:  unknown" in rendered
    assert structured["finished_at_ns"] is None
    assert structured["exit_code"] == 0
    assert structured["wall_time_seconds"] is None


def test_text_inspection_reports_a_true_open_execution_in_progress(tmp_path: Path) -> None:
    runpack = tmp_path / "open.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("open", "open", 0, None, (), str(tmp_path), None, None, {}))

    rendered = render_summary(inspect_runpack(runpack), "text")

    assert "outcome:  in progress" in rendered
    assert "runtime:  in progress" in rendered


def test_causal_tree_handles_graphs_beyond_python_recursion_limit(tmp_path: Path) -> None:
    runpack = tmp_path / "deep.runpack"
    event_count = 1_500
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("deep", "deep", 0, event_count, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            Event(
                f"event-{index}",
                "operation",
                f"operation-{index}",
                "worker",
                0,
                1,
                "test",
                None,
                index,
                {},
            )
            for index in range(event_count)
        )
        writer.add_causal_edges(
            CausalEdge(
                f"event-{index - 1}",
                f"event-{index}",
                "parent",
                1.0,
                {},
            )
            for index in range(1, event_count)
        )

    tree = render_causal_tree(runpack)

    assert "operation-1499" in tree
    assert "… depth 1499 …" in tree
    assert "clock inconsistencies: 0" in tree


def test_causal_tree_bounds_human_readable_structure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runpack = tmp_path / "bounded-tree.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 3, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            Event(
                f"event-{index}",
                "operation",
                f"operation-{index}",
                "worker",
                index,
                index + 1,
                "test",
                None,
                index,
                {},
            )
            for index in range(3)
        )
    monkeypatch.setattr(inspect_module, "MAX_CAUSAL_TREE_ITEMS", 2)

    tree = render_causal_tree(runpack)

    assert "operation-0" in tree
    assert "operation-1" in tree
    assert "operation-2" not in tree
    assert "additional causal structure omitted from text output" in tree
    assert tree.endswith("clock inconsistencies: 0")


def test_causal_tree_distinguishes_shared_nodes_from_cycles(tmp_path: Path) -> None:
    runpack = tmp_path / "diamond.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("diamond", "diamond", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            Event(event_id, "operation", event_id, "worker", 0, 1, "test", None, None, {})
            for event_id in ("root", "left", "right", "shared", "cycle-a", "cycle-b")
        )
        writer.add_causal_edges(
            (
                CausalEdge("root", "left", "parent", 1.0, {}),
                CausalEdge("root", "right", "parent", 1.0, {}),
                CausalEdge("left", "shared", "parent", 1.0, {}),
                CausalEdge("right", "shared", "parent", 1.0, {}),
                CausalEdge("cycle-a", "cycle-b", "parent", 1.0, {}),
                CausalEdge("cycle-b", "cycle-a", "parent", 1.0, {}),
            )
        )

    tree = render_causal_tree(runpack)

    assert "shared [operation] 0.000ms (already shown)" in tree
    assert "cycle-a [operation] 0.000ms (cycle)" in tree


def test_causal_tree_orders_unknown_timestamps_before_the_unix_epoch(tmp_path: Path) -> None:
    runpack = tmp_path / "epoch-order.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("order", "order", 0, 1, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_events(
            (
                Event(
                    "unknown",
                    "operation",
                    "z-unknown",
                    "worker",
                    None,
                    None,
                    None,
                    None,
                    None,
                    {},
                ),
                Event(
                    "epoch",
                    "operation",
                    "a-epoch",
                    "worker",
                    0,
                    0,
                    "test",
                    None,
                    None,
                    {},
                ),
            )
        )

    tree = render_causal_tree(runpack)

    assert tree.index("z-unknown") < tree.index("a-epoch")


def test_causal_tree_groups_operations_by_callsite_and_reserves_raw_profile_graph(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "semantic-tree.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 10_000_000, (), str(tmp_path), 0, None, {}))
        writer.add_entity(Entity("worker", "process", "python", None, {}))
        writer.add_events(
            (
                Event(
                    "process", "process.run", "python", "worker", 0, 10_000_000, None, None, 0, {}
                ),
                Event(
                    "aggregate",
                    "python.call.aggregate",
                    "application.work",
                    "worker",
                    None,
                    None,
                    None,
                    None,
                    None,
                    {},
                ),
                Event(
                    "callsite",
                    "python.callsite",
                    "application.work",
                    "worker",
                    None,
                    None,
                    None,
                    None,
                    None,
                    {},
                ),
                Event(
                    "database",
                    "database.execute",
                    "Database execute",
                    "worker",
                    1_000_000,
                    2_000_000,
                    None,
                    None,
                    1,
                    {"error": True, "error.type": "OperationalError"},
                ),
            )
        )
        writer.add_causal_edges(
            (
                CausalEdge("aggregate", "callsite", "calls", 1.0, {}),
                CausalEdge("callsite", "database", "performs", 1.0, {}),
            )
        )

    tree = render_causal_tree(runpack)
    raw_tree = render_causal_tree(runpack, raw=True)

    assert "python :: application.work [application callsite]" in tree
    assert "  python :: Database execute [database.execute] 1.000ms, error OperationalError" in tree
    assert "python.call.aggregate" not in tree
    assert "[performs" not in tree
    assert "python :: application.work [python.call.aggregate] duration unknown" in raw_tree
    assert "application.work → Database execute [performs, confidence 1.00]" in raw_tree


def test_text_inspection_escapes_terminal_control_characters(tmp_path: Path) -> None:
    runpack = tmp_path / "controls.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("run", "unsafe\x1b[31m", 0, 1, (), str(tmp_path), 0, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            Event("event", "operation", "line\nbreak", "worker", 0, 1, "test", None, None, {})
        )

    summary = render_summary(inspect_runpack(runpack), "text")
    tree = render_causal_tree(runpack)

    assert "\x1b" not in summary
    assert r"unsafe\x1b[31m" in summary
    assert "line\nbreak" not in tree
    assert r"line\nbreak" in tree


def test_inspection_preserves_base_measurements_after_additive_enrichment(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "measurements.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
        writer.add_measurement(Measurement("process.memory.peak", 100.0, "By", 10, None, {}))
        writer.add_measurement(
            Measurement(
                "process.memory.peak",
                1.0,
                "By",
                10,
                None,
                {"source": "prometheus"},
            )
        )

    summary = inspect_runpack(runpack)

    assert summary.peak_memory_bytes == 100


def test_inspection_does_not_materialize_unrelated_measurement_series(tmp_path: Path) -> None:
    runpack = tmp_path / "many-measurements.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
        writer.add_measurements(
            Measurement("unrelated", float(index), "1", index, None, {}) for index in range(10_000)
        )
        writer.add_measurement(Measurement("process.memory.peak", 123.0, "By", 10, None, {}))

    summary = inspect_runpack(runpack)

    assert summary.peak_memory_bytes == 123
    assert summary.record_counts["measurements"] == 10_001


def test_inspection_uses_normalized_execution_bounds_over_process_wall_measurement(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "expanded.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("run", "run", 0, 10_000_000_000, (), str(tmp_path), 0, None, {})
        )
        writer.add_measurement(Measurement("process.wall_time", 1.0, "s", 1_000_000_000, None, {}))

    summary = inspect_runpack(runpack)

    assert summary.wall_time_seconds == 10.0


def test_inspection_does_not_mislabel_reserved_measurements_with_wrong_units(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "wrong-units.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
        writer.add_measurements(
            (
                Measurement("process.cpu.user", 5.0, "By", 10, None, {}),
                Measurement("process.cpu.system", 6.0, "By", 10, None, {}),
                Measurement("process.memory.peak", 7.0, "s", 10, None, {}),
            )
        )

    summary = inspect_runpack(runpack)

    assert summary.cpu_user_seconds is None
    assert summary.cpu_system_seconds is None
    assert summary.peak_memory_bytes is None


def test_inspection_keeps_invalid_peak_memory_measurements_unknown(tmp_path: Path) -> None:
    for index, value in enumerate((1.5, float(1 << 63))):
        runpack = tmp_path / f"invalid-peak-{index}.runpack"
        with RunpackWriter(runpack) as writer:
            writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
            writer.add_measurement(Measurement("process.memory.peak", value, "By", 10, None, {}))

        assert inspect_runpack(runpack).peak_memory_bytes is None


def test_inspection_keeps_invalid_summary_evidence_unknown(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-summary.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                0,
                10,
                (),
                str(tmp_path),
                0,
                None,
                {
                    "output": {
                        "stdout": {"bytes": True, "sha256": ""},
                        "stderr": {"bytes": 1 << 63, "sha256": "a" * 64},
                    }
                },
            )
        )
        writer.add_measurements(
            (
                Measurement("process.wall_time", -1.0, "s", 10, None, {}),
                Measurement("process.cpu.user", -1.0, "s", 10, None, {}),
                Measurement("process.cpu.system", -1.0, "s", 10, None, {}),
                Measurement("process.memory.peak", -1.0, "By", 10, None, {}),
            )
        )

    summary = inspect_runpack(runpack)

    assert summary.wall_time_seconds == 10 / 1_000_000_000
    assert summary.cpu_user_seconds is None
    assert summary.cpu_system_seconds is None
    assert summary.peak_memory_bytes is None
    assert summary.stdout_bytes is None
    assert summary.stdout_sha256 is None
    assert summary.stderr_bytes is None
    assert summary.stderr_sha256 == "a" * 64


def test_inspection_rejects_out_of_range_otlp_completeness_counts(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-completeness.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                0,
                10,
                (),
                str(tmp_path),
                0,
                None,
                {
                    "otel": {
                        "missing_parent_count": (1 << 63) - 1,
                        "missing_link_count": 1,
                        "dropped_attribute_count": 1 << 63,
                    }
                },
            )
        )

    summary = inspect_runpack(runpack)

    assert summary.missing_causal_references is None
    assert summary.dropped_attribute_count is None


def test_text_inspection_reports_invalid_output_completeness(tmp_path: Path) -> None:
    runpack = tmp_path / "invalid-output-completeness.runpack"
    digest = "a" * 64
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                0,
                10,
                (),
                str(tmp_path),
                0,
                None,
                {
                    "output": {
                        "stdout": {
                            "bytes": 5,
                            "sha256": digest,
                            "pipe_open_after_exit": "false",
                        }
                    }
                },
            )
        )

    summary = inspect_runpack(runpack)
    rendered = render_summary(summary, "text")

    assert summary.stdout_complete is None
    assert f"stdout:   5 B, sha256:{digest[:12]} (completeness unknown)" in rendered


def test_inspection_safely_reports_only_text_output_relay_errors(tmp_path: Path) -> None:
    runpack = tmp_path / "relay-errors.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                0,
                10,
                (),
                str(tmp_path),
                0,
                None,
                {
                    "output": {
                        "stdout": {"relay_error": True},
                        "stderr": {"relay_error": "bad\x1b[31m relay"},
                    }
                },
            )
        )

    summary = inspect_runpack(runpack)
    rendered = render_summary(summary, "text")

    assert summary.stdout_relay_error is None
    assert summary.stderr_relay_error == "bad\x1b[31m relay"
    assert "stdout relay:" not in rendered
    assert "\x1b" not in rendered
    assert r"stderr relay: failed (bad\x1b[31m relay)" in rendered
