from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.model import Event, Execution
from runtime_tools.providers.builtins.otel import import_otlp_json
from runtime_tools.providers.builtins.temporal import (
    TemporalHistoryImportError,
    import_temporal_history,
)
from runtime_tools.providers.builtins.temporal import enrichment as temporal
from runtime_tools.storage import RunpackReader, RunpackWriter


def _minimal_history(*, scheduled_event_id: str = "2") -> dict[str, object]:
    return {
        "events": [
            {
                "eventId": "1",
                "eventTime": "2023-11-14T22:13:20Z",
                "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
                "workflowExecutionStartedEventAttributes": {
                    "workflowType": {"name": "ExampleWorkflow"},
                    "workflowId": "example-1",
                },
            },
            {
                "eventId": "2",
                "eventTime": "2023-11-14T22:13:21Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
                "activityTaskScheduledEventAttributes": {
                    "activityId": "activity-1",
                    "activityType": {"name": "do-work"},
                },
            },
            {
                "eventId": "3",
                "eventTime": "2023-11-14T22:13:22Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_STARTED",
                "activityTaskStartedEventAttributes": {
                    "scheduledEventId": scheduled_event_id,
                    "attempt": 1,
                },
            },
            {
                "eventId": "4",
                "eventTime": "2023-11-14T22:13:24Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": "2",
                    "startedEventId": "3",
                },
            },
            {
                "eventId": "5",
                "eventTime": "2023-11-14T22:13:25Z",
                "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED",
                "workflowExecutionCompletedEventAttributes": {},
            },
        ]
    }


def _empty_runpack(path: Path) -> None:
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution(
                "run",
                "run",
                1_700_000_000_000_000_000,
                1_700_000_000_000_000_001,
                (),
                str(path.parent),
                0,
                None,
                {},
            )
        )


def test_temporal_history_correlates_activity_lifecycle_with_otel_spans(
    tmp_path: Path,
) -> None:
    root = Path(__file__).parents[1]
    otel = root / "examples" / "otel" / "temporal-python-fanout.json"
    history = root / "examples" / "temporal" / "temporal-python-fanout-history.json"
    base = tmp_path / "temporal.runpack"
    enriched = tmp_path / "temporal-history.runpack"
    import_otlp_json(otel, base, name="temporal-fanout")

    result = import_temporal_history(base, history, enriched)

    assert result.activity_count == 6
    assert result.queue_wait_count == 6
    assert result.fallback_execution_count == 0
    assert result.event_count == 13
    assert result.edge_count == 18
    assert result.correlation_count == 6
    with RunpackReader(enriched) as reader:
        events = reader.events()
        edges = reader.causal_edges()
    workflow = next(event for event in events if event.kind == "run")
    aggregate = next(
        event
        for event in events
        if event.kind == "temporal.activity"
        and event.attributes["temporal.activity_id"] == "aggregate"
    )
    compute_two_queue = next(
        event
        for event in events
        if event.kind == "queue.wait" and event.attributes["temporal.activity_id"] == "compute-2"
    )
    assert workflow.name == "DailyExport"
    assert workflow.finished_at_ns is not None
    assert workflow.started_at_ns is not None
    assert workflow.finished_at_ns - workflow.started_at_ns == 44_600_000_000
    assert aggregate.attributes["temporal.attempt"] == 2
    assert aggregate.attributes["temporal.outcome"] == "completed"
    assert compute_two_queue.finished_at_ns is not None
    assert compute_two_queue.started_at_ns is not None
    assert compute_two_queue.finished_at_ns - compute_two_queue.started_at_ns == 290_000_000
    assert sum(edge.kind == "dispatches" for edge in edges) == 6

    analysis = analyze_runpack(enriched)
    assert {item.classification for item in analysis.bottlenecks} == {
        "retry_amplification",
        "straggler_tail",
    }
    retry = next(
        item for item in analysis.bottlenecks if item.classification == "retry_amplification"
    )
    assert retry.evidence == "result-aggregation completed on Temporal attempt 2"


def test_temporal_history_adds_execution_facts_when_otel_is_absent(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    history = tmp_path / "history.json"
    output = tmp_path / "output.runpack"
    _empty_runpack(source)
    history.write_text(json.dumps(_minimal_history()), encoding="utf-8")

    result = import_temporal_history(source, history, output)

    assert result.correlation_count == 0
    assert result.fallback_execution_count == 1
    with RunpackReader(output) as reader:
        operation = next(event for event in reader.events() if event.kind == "operation")
        execution = reader.execution()
    assert operation.name == "RunActivity:do-work"
    assert operation.finished_at_ns is not None
    assert operation.started_at_ns is not None
    assert operation.finished_at_ns - operation.started_at_ns == 2_000_000_000
    assert execution.started_at_ns == 1_700_000_000_000_000_000
    assert execution.finished_at_ns == 1_700_000_005_000_000_000


def test_temporal_history_does_not_guess_when_activity_ids_are_reused(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    history = tmp_path / "history.json"
    output = tmp_path / "output.runpack"
    _empty_runpack(source)
    with RunpackWriter.open_existing(source) as writer:
        writer.add_event(
            Event(
                "otel-activity",
                "server.request",
                "RunActivity:do-work",
                None,
                1_700_000_002_000_000_000,
                1_700_000_004_000_000_000,
                "otel:test",
                None,
                None,
                {
                    "temporalWorkflowID": "example-1",
                    "temporalActivityID": "activity-1",
                },
            )
        )
    document = _minimal_history()
    events = document["events"]
    assert isinstance(events, list)
    events[-1] = {
        "eventId": "5",
        "eventTime": "2023-11-14T22:13:25Z",
        "eventType": "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
        "activityTaskScheduledEventAttributes": {
            "activityId": "activity-1",
            "activityType": {"name": "do-work"},
        },
    }
    events.extend(
        (
            {
                "eventId": "6",
                "eventTime": "2023-11-14T22:13:26Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_STARTED",
                "activityTaskStartedEventAttributes": {
                    "scheduledEventId": "5",
                    "attempt": 1,
                },
            },
            {
                "eventId": "7",
                "eventTime": "2023-11-14T22:13:27Z",
                "eventType": "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
                "activityTaskCompletedEventAttributes": {
                    "scheduledEventId": "5",
                    "startedEventId": "6",
                },
            },
            {
                "eventId": "8",
                "eventTime": "2023-11-14T22:13:28Z",
                "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED",
                "workflowExecutionCompletedEventAttributes": {},
            },
        )
    )
    history.write_text(json.dumps(document), encoding="utf-8")

    result = import_temporal_history(source, history, output)

    assert result.activity_count == 2
    assert result.correlation_count == 0
    assert result.fallback_execution_count == 2


def test_temporal_history_rejects_unknown_activity_references_without_publishing(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    history = tmp_path / "history.json"
    output = tmp_path / "output.runpack"
    _empty_runpack(source)
    history.write_text(
        json.dumps(_minimal_history(scheduled_event_id="99")),
        encoding="utf-8",
    )

    with pytest.raises(
        TemporalHistoryImportError,
        match="activity started event 3 references unknown scheduled event 99",
    ):
        import_temporal_history(source, history, output)

    assert not output.exists()


def test_temporal_history_bounds_event_count_before_enrichment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.runpack"
    history = tmp_path / "history.json"
    output = tmp_path / "output.runpack"
    _empty_runpack(source)
    history.write_text(json.dumps(_minimal_history()), encoding="utf-8")
    monkeypatch.setattr(temporal, "MAX_TEMPORAL_HISTORY_EVENTS", 1)

    with pytest.raises(TemporalHistoryImportError, match="1-event input limit"):
        import_temporal_history(source, history, output)

    assert not output.exists()


def test_temporal_history_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    output = tmp_path / "output.runpack"
    history = tmp_path / "history.json"
    history.write_text('{"events": [], "events": []}', encoding="utf-8")

    with pytest.raises(TemporalHistoryImportError, match="duplicate JSON key: events"):
        import_temporal_history(tmp_path / "missing.runpack", history, output)

    assert not output.exists()


def test_temporal_history_rejects_invalid_identifier_unicode(tmp_path: Path) -> None:
    source = tmp_path / "source.runpack"
    history = tmp_path / "history.json"
    output = tmp_path / "output.runpack"
    _empty_runpack(source)
    document = _minimal_history()
    events = document["events"]
    assert isinstance(events, list)
    scheduled = events[1]
    assert isinstance(scheduled, dict)
    attributes = scheduled["activityTaskScheduledEventAttributes"]
    assert isinstance(attributes, dict)
    attributes["activityId"] = "invalid-\udcff"
    history.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(TemporalHistoryImportError, match="activityId must be valid UTF-8"):
        import_temporal_history(source, history, output)

    assert not output.exists()


def test_runtime_cli_enriches_with_temporal_history(tmp_path: Path) -> None:
    root = Path(__file__).parents[1]
    otel = root / "examples" / "otel" / "temporal-python-fanout.json"
    history = root / "examples" / "temporal" / "temporal-python-fanout-history.json"
    base = tmp_path / "temporal.runpack"
    output = tmp_path / "temporal-history.runpack"
    import_otlp_json(otel, base, name="temporal-fanout")

    completed = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "enrich-temporal-history",
            str(base),
            str(history),
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert completed.stdout == ""
    assert completed.stderr == (
        f"added 6 Temporal activities, 6 queue waits, and 6 OTLP correlations to {output}\n"
    )
