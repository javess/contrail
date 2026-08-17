from __future__ import annotations

from pathlib import Path

from runtime_tools.inspect import inspect_runpack, render_causal_tree
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
