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
            "--include-output",
            "--output-limit-bytes",
            "4",
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
    assert payload["schema_version"] == "1.1"
    assert payload["producer_version"] == "0.1.0"
    assert payload["exit_code"] == 0
    assert payload["stdout_bytes"] == 9
    assert payload["record_counts"] == {
        "causal_edges": 0,
        "entities": 1,
        "events": 1,
        "measurements": 6,
        "attachments": 2,
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


def test_runtime_cli_enriches_a_runpack_with_otlp_logs(tmp_path: Path) -> None:
    trace = tmp_path / "trace.json"
    logs = tmp_path / "logs.json"
    base = tmp_path / "base.runpack"
    output = tmp_path / "logs.runpack"
    trace.write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "traceId": "trace",
                                        "spanId": "span",
                                        "startTimeUnixNano": "1",
                                        "endTimeUnixNano": "10",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    logs.write_text(
        json.dumps(
            {
                "resourceLogs": [
                    {
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": "5",
                                        "body": {"stringValue": "hello"},
                                        "traceId": "trace",
                                        "spanId": "span",
                                    }
                                ]
                            }
                        ]
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
            str(trace),
            "--output",
            str(base),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    enriched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "enrich-otel-logs",
            str(base),
            str(logs),
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert imported.returncode == 0
    assert enriched.returncode == 0
    assert "added 1 OTLP log records and 1 span correlations" in enriched.stderr
    assert output.is_file()


def test_runtime_cli_warns_when_jsonl_query_output_is_truncated(tmp_path: Path) -> None:
    output = tmp_path / "query.runpack"
    record_process = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "pass",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    queried = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "query",
            str(output),
            "SELECT name FROM measurements ORDER BY id",
            "--limit",
            "1",
            "--format",
            "jsonl",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert record_process.returncode == 0
    assert queried.returncode == 0
    assert len(queried.stdout.splitlines()) == 1
    assert queried.stderr == "runtime: query result truncated at the requested row limit\n"


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


def test_runtime_cli_escapes_terminal_controls_in_errors(tmp_path: Path) -> None:
    runpack = tmp_path / "bad-\x1b[31m.runpack"
    runpack.write_bytes(b"not sqlite")

    inspected = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "inspect",
            str(runpack),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert inspected.returncode == 2
    assert "\x1b" not in inspected.stderr
    assert r"bad-\x1b[31m.runpack" in inspected.stderr


def test_runtime_cli_escapes_terminal_controls_in_status_paths(tmp_path: Path) -> None:
    output = tmp_path / "recorded-\x1b[31m.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "pass",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert "\x1b" not in recorded.stderr
    assert r"recorded-\x1b[31m.runpack" in recorded.stderr


def test_runtime_cli_reports_enrichment_output_collisions_without_traceback(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.runpack"
    snapshot = tmp_path / "snapshot.json"
    output = tmp_path / "existing.runpack"
    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--output",
            str(source),
            "--",
            sys.executable,
            "-c",
            "pass",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    snapshot.write_text('{"items": []}', encoding="utf-8")
    output.write_text("preserve me", encoding="utf-8")

    enriched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "enrich-kubernetes",
            str(source),
            str(snapshot),
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert enriched.returncode == 2
    assert enriched.stdout == ""
    assert enriched.stderr == f"runtime: refusing to overwrite existing runpack: {output}\n"
    assert "Traceback" not in enriched.stderr
    assert output.read_text(encoding="utf-8") == "preserve me"


def test_all_cli_entrypoints_report_the_package_version() -> None:
    modules = (
        "runtime_tools.cli",
        "runtime_tools.rundiff.cli",
        "runtime_tools.batchscope.cli",
        "runtime_tools.proofline.cli",
    )

    results = [
        subprocess.run(
            (sys.executable, "-m", module, "--version"),
            check=False,
            capture_output=True,
            text=True,
        )
        for module in modules
    ]

    assert [result.returncode for result in results] == [0, 0, 0, 0]
    assert [result.stdout for result in results] == [
        "runtime 0.1.0\n",
        "rundiff 0.1.0\n",
        "batchscope 0.1.0\n",
        "proofline 0.1.0\n",
    ]
