from __future__ import annotations

import hashlib
import json
import signal
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

import runtime_tools.cli as cli_module
from runtime_tools import __version__
from runtime_tools.capture import record_process
from runtime_tools.model import Event
from runtime_tools.storage import RunpackReader, RunpackWriter


def _trace_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-trace:{label}").hex


def _span_id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"test-span:{label}").hex[:16]


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
    assert payload["producer_version"] == __version__
    assert payload["exit_code"] == 0
    assert payload["stdout_bytes"] == 9
    assert payload["record_counts"] == {
        "causal_edges": 0,
        "entities": 1,
        "events": 1,
        "measurements": 6,
        "attachments": 2,
    }


@pytest.mark.parametrize(
    ("option", "keyword"),
    (("--contract", "contract"), ("--proofline-report", "proofline_report")),
)
def test_runtime_serve_forwards_proofline_debugging_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    option: str,
    keyword: str,
) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    proofline_input = tmp_path / "proofline-input"
    captured: dict[str, object] = {}

    def serve(*args: object, **kwargs: object) -> None:
        captured["args"] = args
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, "serve_runpacks", serve)

    status = cli_module.main(
        [
            "serve",
            str(baseline),
            "--compare",
            str(candidate),
            option,
            str(proofline_input),
            "--no-open",
        ]
    )

    assert status == 0
    assert captured["args"] == (baseline, candidate)
    assert captured[keyword] == proofline_input
    assert captured["open_browser"] is False


@pytest.mark.parametrize("option", ("--contract", "--proofline-report"))
def test_runtime_serve_requires_compare_for_proofline_debugging(
    tmp_path: Path,
    option: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    status = cli_module.main(
        ["serve", str(tmp_path / "baseline.runpack"), option, str(tmp_path / "input")]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert captured.err == f"runtime: {option} requires --compare\n"


def test_runtime_cli_rejects_an_output_limit_without_output_capture(tmp_path: Path) -> None:
    output = tmp_path / "ignored-limit.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--name",
            "ignored-limit",
            "--output",
            str(output),
            "--output-limit-bytes",
            "4",
            "--",
            sys.executable,
            "-c",
            "pass",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 2
    assert recorded.stdout == ""
    assert recorded.stderr == "runtime: --output-limit-bytes requires --include-output\n"
    assert "Traceback" not in recorded.stderr
    assert not output.exists()


def test_runtime_cli_handles_a_closed_output_pipe_without_a_traceback(tmp_path: Path) -> None:
    runpack = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="query")
    process = subprocess.Popen(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "query",
            str(runpack),
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM n "
            "WHERE x < 50000) SELECT x, printf('%0100d', x) AS payload FROM n",
            "--limit",
            "50000",
            "--format",
            "jsonl",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stderr is not None

    first_line = process.stdout.readline()
    process.stdout.close()
    stderr = process.stderr.read()
    return_code = process.wait()

    assert json.loads(first_line)["x"] == 1
    assert return_code == 1
    assert stderr == ""


def test_runtime_cli_records_in_an_explicit_working_directory(tmp_path: Path) -> None:
    working_directory = tmp_path / "work"
    working_directory.mkdir()
    output = tmp_path / "cwd.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--name",
            "cwd",
            "--cwd",
            str(working_directory),
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "from pathlib import Path; print(Path.cwd().name)",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert recorded.stdout == "work\n"
    with RunpackReader(output) as reader:
        assert reader.execution().working_directory == str(working_directory)


def test_runtime_cli_identifies_custom_environment_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "environment.runpack"
    monkeypatch.setenv("CONTRAIL_TEST_FEATURE_MODE", "experimental")

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--identify-env",
            "CONTRAIL_TEST_FEATURE_MODE",
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
    with RunpackReader(output) as reader:
        environment = reader.execution().metadata["environment"]
    assert isinstance(environment, dict)
    identities = environment["selected_value_sha256"]
    assert isinstance(identities, dict)
    assert identities["CONTRAIL_TEST_FEATURE_MODE"] == hashlib.sha256(b"experimental").hexdigest()


def test_runtime_cli_normalizes_child_signals_without_losing_artifact_evidence(
    tmp_path: Path,
) -> None:
    output = tmp_path / "signaled.runpack"

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
            "import os, signal; os.kill(os.getpid(), signal.SIGTERM)",
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    inspected = subprocess.run(
        (sys.executable, "-m", "runtime_tools.cli", "inspect", str(output)),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 128 + signal.SIGTERM
    assert inspected.returncode == 0
    assert "outcome:  failed (signal 15)" in inspected.stdout
    with RunpackReader(output) as reader:
        assert reader.execution().exit_code == -signal.SIGTERM


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
                                        "traceId": _trace_id("trace"),
                                        "spanId": _span_id("span"),
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


def test_runtime_cli_summary_and_tree_share_one_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="stable")
    real_close = RunpackReader.close
    mutated = False

    def mutate_after_snapshot(reader: RunpackReader) -> None:
        nonlocal mutated
        real_close(reader)
        if reader.path == runpack.resolve() and not mutated:
            mutated = True
            with RunpackWriter.open_existing(runpack) as writer:
                writer.add_event(
                    Event("late", "operation", "late event", None, 1, 2, "test", None, 99, {})
                )

    monkeypatch.setattr(RunpackReader, "close", mutate_after_snapshot)

    assert cli_module.main(["inspect", str(runpack), "--tree"]) == 0

    output = capsys.readouterr().out
    assert "records:  1 entities, 1 events" in output
    assert "late event" not in output
    with sqlite3.connect(runpack) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 2


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
                                        "traceId": _trace_id("trace"),
                                        "spanId": _span_id("span"),
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
                                        "traceId": _trace_id("trace"),
                                        "spanId": _span_id("span"),
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


def test_runtime_cli_emits_no_jsonl_record_for_an_empty_query(tmp_path: Path) -> None:
    output = tmp_path / "query.runpack"
    record_process((sys.executable, "-c", "pass"), output, name="query")

    queried = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "query",
            str(output),
            "SELECT id FROM events WHERE 0",
            "--format",
            "jsonl",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert queried.returncode == 0
    assert queried.stdout == ""
    assert queried.stderr == ""


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


def test_runtime_cli_normalizes_overlong_runpack_paths() -> None:
    inspected = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "inspect",
            "a" * 5000,
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert inspected.returncode == 2
    assert inspected.stdout == ""
    assert inspected.stderr.startswith("runtime: could not resolve runpack path: ")
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


@pytest.mark.parametrize(
    "subcommand",
    ("enrich-kubernetes", "enrich-prometheus", "enrich-otel-logs"),
)
def test_runtime_cli_rejects_existing_enrichment_outputs_before_reading_inputs(
    tmp_path: Path,
    subcommand: str,
) -> None:
    output = tmp_path / "existing.runpack"
    output.write_text("preserve me", encoding="utf-8")

    enriched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            subcommand,
            str(tmp_path / "missing.runpack"),
            str(tmp_path / "missing.json"),
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert enriched.returncode == 2
    assert enriched.stderr == f"runtime: refusing to overwrite existing runpack: {output}\n"
    assert output.read_text(encoding="utf-8") == "preserve me"


@pytest.mark.parametrize(
    "subcommand",
    ("enrich-kubernetes", "enrich-prometheus", "enrich-otel-logs"),
)
def test_runtime_cli_rejects_dangling_enrichment_outputs_before_reading_inputs(
    tmp_path: Path,
    subcommand: str,
) -> None:
    output = tmp_path / "existing.runpack"
    output.symlink_to(tmp_path / "missing-output-target.runpack")

    enriched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            subcommand,
            str(tmp_path / "missing.runpack"),
            str(tmp_path / "missing.json"),
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert enriched.returncode == 2
    assert enriched.stderr == f"runtime: refusing to overwrite existing runpack: {output}\n"
    assert output.is_symlink()


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
        f"runtime {__version__}\n",
        f"rundiff {__version__}\n",
        f"batchscope {__version__}\n",
        f"proofline {__version__}\n",
    ]
