from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from runtime_tools import __version__, record_process


def _contrail(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, "-m", "runtime_tools.contrail_cli", *arguments),
        check=False,
        capture_output=True,
        text=True,
    )


def test_contrail_root_help_and_version_present_one_product() -> None:
    help_result = _contrail("--help")
    no_argument_result = _contrail()
    version_result = _contrail("--version")

    assert help_result.returncode == 0
    assert help_result.stderr == ""
    assert "usage: contrail COMMAND" in help_result.stdout
    assert "demo" in help_result.stdout
    assert "record → verify → serve" in help_result.stdout
    assert no_argument_result.returncode == 0
    assert no_argument_result.stdout == help_result.stdout
    assert no_argument_result.stderr == ""
    assert version_result.returncode == 0
    assert version_result.stdout == f"contrail {__version__}\n"
    assert version_result.stderr == ""


def test_contrail_demo_is_a_self_contained_successful_walkthrough(tmp_path: Path) -> None:
    output = tmp_path / "installed demo"

    result = _contrail("demo", "--output-dir", str(output))

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith("CONTRAIL DEMO READY\n")
    assert "db.write operations:       3 → 30" in result.stdout
    assert "new metadata-db dependency: 0 → 1" in result.stdout
    assert {path.name for path in output.iterdir()} == {
        "baseline.runpack",
        "candidate.runpack",
        "contract.yaml",
        "proofline-report.json",
        "workload.py",
    }
    assert f"adaptable workload: {output / 'workload.py'}" in result.stdout
    verify_command = shlex.join(
        (
            "contrail",
            "verify",
            str(output / "contract.yaml"),
            "--baseline",
            str(output / "baseline.runpack"),
            "--candidate",
            str(output / "candidate.runpack"),
            "--explain",
        )
    )
    serve_command = shlex.join(
        (
            "contrail",
            "serve",
            str(output / "baseline.runpack"),
            "--compare",
            str(output / "candidate.runpack"),
            "--proofline-report",
            str(output / "proofline-report.json"),
        )
    )
    assert f"  {verify_command}\n" in result.stdout
    assert f"  {serve_command}\n" in result.stdout

    adaptation = result.stdout.split(
        "Adapt workload.py and contract.yaml, then capture the two variants again:\n", 1
    )[1].split("\n\n", 1)[0]
    commands = [shlex.split(line.strip()) for line in adaptation.splitlines()]
    assert [command[1] for command in commands] == ["record", "record", "verify"]
    replayed = [_contrail(*command[1:]) for command in commands]
    assert [command.returncode for command in replayed] == [0, 0, 1]
    assert (output / "recaptured-report.json").is_file()

    collision = _contrail("demo", "--output-dir", str(output))
    assert collision.returncode == 2
    assert collision.stdout == ""
    assert collision.stderr.startswith("contrail: refusing to reuse demo output directory:")


def test_contrail_demo_supports_its_default_relative_output_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = _contrail("demo")

    output = tmp_path / "contrail-demo"
    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith("CONTRAIL DEMO READY\n")
    assert {path.name for path in output.iterdir()} == {
        "baseline.runpack",
        "candidate.runpack",
        "contract.yaml",
        "proofline-report.json",
        "workload.py",
    }


def test_contrail_demo_commands_preserve_control_characters_in_paths(tmp_path: Path) -> None:
    output = tmp_path / "installed\ndemo\x1b"

    result = _contrail("demo", "--output-dir", str(output))

    assert result.returncode == 0
    assert result.stderr == ""
    command = result.stdout.split("Re-run the contract gate (expected exit 1):\n  ", 1)[1]
    command = command.split("\n", 1)[0]
    parsed = subprocess.run(
        ("bash", "-c", 'eval "set -- $1"; printf "%s\\0" "$@"', "bash", command),
        check=True,
        capture_output=True,
    )
    arguments = parsed.stdout.rstrip(b"\0").split(b"\0")
    assert arguments == [
        b"contrail",
        b"verify",
        bytes(output / "contract.yaml"),
        b"--baseline",
        bytes(output / "baseline.runpack"),
        b"--candidate",
        bytes(output / "candidate.runpack"),
        b"--explain",
    ]


def test_contrail_unknown_command_is_a_branded_usage_error() -> None:
    result = _contrail("unknown")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "contrail: unknown command: unknown\nTry 'contrail --help'.\n"


@pytest.mark.parametrize(
    "command",
    (
        "demo",
        "record",
        "inspect",
        "compare",
        "analyze",
        "serve",
        "query",
        "import-otel",
        "enrich-kubernetes",
        "enrich-prometheus",
        "enrich-otel-logs",
        "validate",
        "verify",
        "run",
        "search",
    ),
)
def test_contrail_subcommand_help_keeps_the_branded_command_name(command: str) -> None:
    result = _contrail(command, "--help")

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.startswith(f"usage: contrail {command} ")
    if command in {"run", "search"}:
        assert "--python" in result.stdout
    if command in {"verify", "run"}:
        assert "--report" in result.stdout


def test_contrail_run_reports_an_invalid_workload_python_as_a_branded_usage_error(
    tmp_path: Path,
) -> None:
    output = tmp_path / "results"

    result = _contrail(
        "run",
        str(tmp_path / "missing-contract.yaml"),
        "--baseline-ref",
        "main",
        "--candidate-ref",
        "HEAD",
        "--workload",
        "workload.py",
        "--python",
        str(tmp_path / "missing-python"),
        "--output-dir",
        str(output),
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr.startswith("contrail: workload Python does not exist:")
    assert not output.exists()


def test_contrail_record_preserves_child_status_and_double_dash_arguments(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "child.runpack"

    result = _contrail(
        "record",
        "--name",
        "branded-child",
        "--output",
        str(runpack),
        "--",
        sys.executable,
        "-c",
        "import sys; print('--kept'); raise SystemExit(7)",
    )

    assert result.returncode == 7
    assert result.stdout == "--kept\n"
    assert result.stderr == f"recorded {runpack}\n"
    assert runpack.is_file()


def test_contrail_preserves_machine_protocols_and_brands_domain_errors(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    recorded = _contrail(
        "record",
        "--name",
        "machine-output",
        "--output",
        str(runpack),
        "--",
        sys.executable,
        "-c",
        "pass",
    )

    inspected = _contrail("inspect", str(runpack), "--format", "json")
    compared = _contrail("compare", str(runpack), str(runpack), "--format", "json")
    analyzed = _contrail("analyze", str(runpack), "--format", "json")
    missing = _contrail("inspect", str(tmp_path / "missing.runpack"))

    assert recorded.returncode == 0
    assert json.loads(inspected.stdout)["document_type"] == "runtime.inspect"
    assert json.loads(compared.stdout)["document_type"] == "rundiff.compare"
    assert json.loads(analyzed.stdout)["document_type"] == "batchscope.inspect"
    assert [inspected.returncode, compared.returncode, analyzed.returncode] == [0, 0, 0]
    assert missing.returncode == 2
    assert missing.stdout == ""
    assert missing.stderr.startswith("contrail: runpack does not exist:")
    assert "runtime:" not in missing.stderr
    assert "batchscope:" not in missing.stderr


def test_contrail_explanation_keeps_the_debug_workflow_branded(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    record_process((sys.executable, "-c", "pass"), baseline, name="baseline")
    record_process(
        (sys.executable, "-c", "raise SystemExit(1)"),
        candidate,
        name="candidate",
    )
    contract.write_text(
        "name: branded-next-steps\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )

    result = _contrail(
        "verify",
        str(contract),
        "--baseline",
        str(baseline),
        "--candidate",
        str(candidate),
        "--explain",
    )

    assert result.returncode == 1
    assert f"contrail analyze {shlex.quote(str(candidate))}" in result.stdout
    assert f"contrail inspect {shlex.quote(str(candidate))} --tree" in result.stdout
    assert (
        f"contrail serve {shlex.quote(str(baseline))} "
        f"--compare {shlex.quote(str(candidate))} --contract {shlex.quote(str(contract))}"
        in result.stdout
    )
    assert "batchscope inspect" not in result.stdout
    assert "runtime serve" not in result.stdout


def test_contrail_report_next_step_replays_the_retained_failure(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    contract = tmp_path / "contract.yaml"
    report = tmp_path / "proofline report.json"
    record_process((sys.executable, "-c", "pass"), baseline, name="baseline")
    record_process(
        (sys.executable, "-c", "raise SystemExit(1)"),
        candidate,
        name="candidate",
    )
    contract.write_text(
        "name: retained\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )

    result = _contrail(
        "verify",
        str(contract),
        "--baseline",
        str(baseline),
        "--candidate",
        str(candidate),
        "--report",
        str(report),
    )

    assert result.returncode == 1
    assert result.stderr == ""
    assert json.loads(report.read_text(encoding="utf-8"))["artifact_bindings"]
    assert (
        f"contrail serve {shlex.quote(str(baseline))} "
        f"--compare {shlex.quote(str(candidate))} "
        f"--proofline-report {shlex.quote(str(report))}" in result.stdout
    )
    assert "runtime serve" not in result.stdout
