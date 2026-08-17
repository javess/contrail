from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_runtime_cli_records_then_inspects_json(tmp_path: Path) -> None:
    output = tmp_path / "cli.runpack"
    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--name",
            "cli-demo",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "print('captured')",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    inspected = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "inspect",
            str(output),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert recorded.stdout == "captured\n"
    assert f"recorded {output}" in recorded.stderr
    assert inspected.returncode == 0
    payload = json.loads(inspected.stdout)
    assert payload["name"] == "cli-demo"
    assert payload["exit_code"] == 0
    assert payload["stdout_bytes"] == 9
    assert payload["record_counts"] == {
        "causal_edges": 0,
        "entities": 1,
        "events": 1,
        "measurements": 6,
    }


def test_runtime_cli_imports_otlp_json_and_prints_causal_tree(tmp_path: Path) -> None:
    source = tmp_path / "trace.json"
    output = tmp_path / "trace.runpack"
    source.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": {"stringValue": "checkout"},
                                }
                            ]
                        },
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": "trace",
                                        "spanId": "span",
                                        "name": "charge",
                                        "startTimeUnixNano": "1000",
                                        "endTimeUnixNano": "3000",
                                    }
                                ]
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    imported = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "import-otel",
            str(source),
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    inspected = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "inspect",
            str(output),
            "--tree",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert imported.returncode == 0
    assert "imported 1 spans and 0 causal edges" in imported.stderr
    assert inspected.returncode == 0
    assert "outcome:  unknown (no exit status)" in inspected.stdout
    assert "runtime:  0.000s" in inspected.stdout
    assert "checkout :: charge [operation] 0.002ms" in inspected.stdout


def test_runtime_cli_reports_corrupt_runpack_without_traceback(tmp_path: Path) -> None:
    runpack = tmp_path / "corrupt.runpack"
    runpack.write_bytes(b"not sqlite")

    inspected = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "inspect",
            str(runpack),
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert inspected.returncode == 2
    assert inspected.stdout == ""
    assert inspected.stderr == f"runtime: invalid runpack: {runpack}\n"
    assert "Traceback" not in inspected.stderr
