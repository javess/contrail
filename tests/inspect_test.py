from __future__ import annotations

from pathlib import Path

from runtime_tools.inspect import render_causal_tree
from runtime_tools.model import CausalEdge, Entity, Event, Execution
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
