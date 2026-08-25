from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import cast

import pytest

import runtime_tools.process_observer as process_observer_module
from runtime_tools import CaptureError, record_process
from runtime_tools.batchscope import analyze_runpack
from runtime_tools.batchscope.report import render_analysis
from runtime_tools.process_observer import ProcessObserverError
from runtime_tools.proofline.verify import verify_contracts
from runtime_tools.rundiff.compare import compare_runpacks
from runtime_tools.storage import RunpackReader


def _child_workload(seconds: float = 0.35) -> str:
    return f"""
import subprocess
import sys
import time

child = subprocess.Popen([
    sys.executable,
    "-c",
    "import time; payload = bytearray(8 * 1024 * 1024); time.sleep({seconds})",
])
time.sleep({seconds})
child.wait()
"""


def test_process_tree_observation_captures_descendant_resources_without_injection(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "process-tree.runpack"

    exit_code = record_process(
        (sys.executable, "-c", _child_workload()),
        runpack,
        name="process-tree",
        observe_process_tree=True,
    )

    with RunpackReader(runpack) as reader:
        execution = reader.execution()
        entities = reader.entities()
        measurements = reader.measurements()
    capture = execution.metadata["capture"]
    assert isinstance(capture, dict)
    observer = capture["process_observer"]
    assert isinstance(observer, dict)
    assert exit_code == 0
    assert observer["requested"] is True
    assert observer["observer"] == "posix-process-table"
    assert observer["status"] == "complete"
    assert observer["process_count"] == 2
    assert observer["descendant_process_count"] == 1
    assert isinstance(observer["sample_count"], int) and observer["sample_count"] >= 4

    root = next(entity for entity in entities if entity.parent_entity_id is None)
    child = next(entity for entity in entities if entity.parent_entity_id is not None)
    assert child.parent_entity_id == root.id
    assert child.kind == "process"
    assert child.attributes["source"] == "process-observer"
    assert {measurement.name for measurement in measurements} >= {
        "process.memory.rss",
        "process.cpu.total",
    }
    observed = tuple(
        measurement
        for measurement in measurements
        if measurement.attributes.get("source") == "process-observer"
    )
    assert observed
    assert all(measurement.entity_id in {root.id, child.id} for measurement in observed)
    assert "bytearray" not in json.dumps(
        [entity.attributes for entity in entities]
        + [measurement.attributes for measurement in observed]
    )

    analysis = analyze_runpack(runpack)
    report = render_analysis(analysis, "text")
    assert analysis.process_observer is not None
    assert analysis.process_observer.status == "complete"
    assert len(analysis.process_hotspots) == 2
    assert "Process tree capture" in report
    assert "controller-side; no workload injection" in report
    assert "Process resource peaks" in report
    json_report = json.loads(render_analysis(analysis, "json"))
    assert json_report["process_observer"]["descendant_process_count"] == 1
    assert len(json_report["process_hotspots"]) == 2


def test_process_tree_observation_failure_does_not_change_workload_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_process_table(_process_group_id: int) -> tuple[object, ...]:
        raise ProcessObserverError("process table deliberately unavailable")

    monkeypatch.setattr(process_observer_module, "_read_process_table", fail_process_table)
    runpack = tmp_path / "unavailable.runpack"

    exit_code = record_process(
        (sys.executable, "-c", "print('still-ran')"),
        runpack,
        name="unavailable",
        observe_process_tree=True,
    )

    with RunpackReader(runpack) as reader:
        capture = reader.execution().metadata["capture"]
    assert isinstance(capture, dict)
    observer = capture["process_observer"]
    assert isinstance(observer, dict)
    assert exit_code == 0
    assert observer["status"] == "unavailable"
    assert observer["sample_count"] == 0
    assert observer["error"] == "process table deliberately unavailable"


def test_process_tree_observer_thread_start_failure_is_nonfatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnstartableThread:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            raise RuntimeError("thread limit reached")

    monkeypatch.setattr(process_observer_module.threading, "Thread", UnstartableThread)
    observer = process_observer_module.ProcessTreeObserver(
        process_group_id=123,
        execution_id="execution",
        root_entity_id="root",
        started_at_ns=1,
        started_monotonic_ns=1,
    )

    observer.start()
    result = observer.stop()

    assert result.as_metadata()["status"] == "unavailable"
    assert result.error == "could not start process observer: thread limit reached"


def test_process_tree_observation_composes_with_python_sampling(tmp_path: Path) -> None:
    runpack = tmp_path / "sampled-process-tree.runpack"

    record_process(
        (sys.executable, "-c", "import time\ndef useful(): time.sleep(0.25)\nuseful()"),
        runpack,
        name="sampled-process-tree",
        instrument="sample",
        observe_process_tree=True,
    )

    with RunpackReader(runpack) as reader:
        capture = reader.execution().metadata["capture"]
        event_kinds = {event.kind for event in reader.events()}
        measurement_names = {measurement.name for measurement in reader.measurements()}
    assert isinstance(capture, dict)
    instrumentation = capture["instrumentation"]
    observer = capture["process_observer"]
    assert isinstance(instrumentation, dict)
    assert isinstance(observer, dict)
    assert instrumentation["mode"] == "sample"
    assert instrumentation["status"] == "complete"
    assert observer["status"] == "complete"
    assert "python.stack.sample" in event_kinds
    assert "process.memory.rss" in measurement_names
    analysis = analyze_runpack(runpack)
    assert analysis.sample_profile is not None
    assert analysis.process_observer is not None


def test_process_tree_observation_marks_process_limit_truncation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(process_observer_module, "MAX_PROCESS_OBSERVER_PROCESSES", 1)
    runpack = tmp_path / "truncated.runpack"

    record_process(
        (sys.executable, "-c", _child_workload(0.25)),
        runpack,
        name="truncated",
        observe_process_tree=True,
    )

    with RunpackReader(runpack) as reader:
        capture = reader.execution().metadata["capture"]
    assert isinstance(capture, dict)
    observer = capture["process_observer"]
    assert isinstance(observer, dict)
    assert observer["status"] == "truncated"
    assert observer["truncated"] is True
    assert observer["process_count"] == 1
    dropped_process_count = observer["dropped_process_count"]
    assert isinstance(dropped_process_count, int) and dropped_process_count >= 1


def test_process_tree_observation_makes_mixed_timing_modes_unverifiable(
    tmp_path: Path,
) -> None:
    passive = tmp_path / "passive.runpack"
    observed = tmp_path / "observed.runpack"
    command = (sys.executable, "-c", "import time; time.sleep(0.15)")
    record_process(command, passive, name="passive")
    record_process(command, observed, name="observed", observe_process_tree=True)
    contract = tmp_path / "contract.yaml"
    contract.write_text(
        "name: timing\nassertions:\n"
        "  - type: max_runtime_regression\n"
        "    name: comparable-runtime\n"
        "    percent: 100\n",
        encoding="utf-8",
    )

    diff = compare_runpacks(passive, observed)
    verification = verify_contracts(contract, passive, observed)

    assert diff.baseline_instrumentation_mode == "passive"
    assert diff.candidate_instrumentation_mode == "process"
    assert diff.timing_comparable is False
    assert verification.results[0].status == "unverifiable"
    assert verification.results[0].observed == (
        "timing instrumentation differs (baseline=passive, candidate=process)"
    )


def test_record_process_rejects_non_boolean_process_observation_without_running_workload(
    tmp_path: Path,
) -> None:
    with pytest.raises(CaptureError, match="observe process tree must be a boolean"):
        record_process(
            (sys.executable, "-c", "raise RuntimeError('must not run')"),
            tmp_path / "invalid.runpack",
            name="invalid",
            observe_process_tree=cast(bool, 1),
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    (("00:01.25", 1.25), ("01:02:03", 3723.0), ("2-01:02:03", 176523.0)),
)
def test_process_cpu_clock_supports_qualified_posix_formats(
    value: str,
    expected: float,
) -> None:
    assert process_observer_module._cpu_seconds(value) == expected
