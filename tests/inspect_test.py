from __future__ import annotations

from pathlib import Path

from runtime_tools.inspect import inspect_runpack, render_causal_tree, render_summary
from runtime_tools.model import CausalEdge, Entity, Event, Execution, Measurement
from runtime_tools.storage import RunpackWriter


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
