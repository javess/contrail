from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import venv
from pathlib import Path

import pytest

from runtime_tools.proofline import ContractError, ExperimentError, experiments, run_experiment
from runtime_tools.proofline import cli as proofline_cli
from runtime_tools.proofline.experiments import ExperimentResult
from runtime_tools.proofline.verify import ClaimResult, VerificationReport
from runtime_tools.storage import RunpackReader


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", *args),
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _git_repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Proofline Test")
    _git(repo, "config", "user.email", "proofline@example.invalid")
    return repo


def test_proofline_bounds_git_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def time_out(*args: object, **kwargs: object) -> None:
        timeout = kwargs["timeout"]
        assert isinstance(timeout, (int, float)) and not isinstance(timeout, bool)
        assert timeout == experiments.GIT_COMMAND_TIMEOUT_SECONDS
        raise subprocess.TimeoutExpired(("git", "status"), float(timeout))

    monkeypatch.setattr(subprocess, "run", time_out)

    with pytest.raises(ExperimentError, match="Git command timed out after 120 seconds"):
        experiments._git(tmp_path, "status")


def test_proofline_normalizes_git_launch_failures(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise OSError("argument vector is too large")

    monkeypatch.setattr(subprocess, "run", fail)

    with pytest.raises(ExperimentError, match="could not execute Git: argument vector"):
        experiments._git(tmp_path, "status")


def test_proofline_normalizes_invalid_git_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid byte")

    monkeypatch.setattr(subprocess, "run", fail)

    with pytest.raises(ExperimentError, match="Git output must be valid UTF-8"):
        experiments._git(tmp_path, "rev-parse", "--show-toplevel", capture=True)


def test_proofline_preserves_trailing_whitespace_in_repository_paths(tmp_path: Path) -> None:
    repo = tmp_path / "repo "
    repo.mkdir()
    _git(repo, "init", "-b", "main")

    assert experiments._repo_root(repo) == repo


def test_proofline_rejects_an_empty_git_repository_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(experiments, "_git", lambda *args, **kwargs: "")

    with pytest.raises(ExperimentError, match="Git repository root is empty"):
        experiments._repo_root(tmp_path)


def test_proofline_normalizes_temporary_worktree_allocation_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise PermissionError("temporary storage denied")

    monkeypatch.setattr(tempfile, "mkdtemp", fail)

    with pytest.raises(ExperimentError, match="could not create temporary worktree directory"):
        experiments._temporary_worktree_root()


def test_proofline_normalizes_annotation_fd_reservation_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> int:
        raise OSError("descriptor limit reached")

    monkeypatch.setattr("runtime_tools.proofline.experiments.os.open", fail)

    with pytest.raises(ExperimentError, match="could not reserve annotation transport fd"):
        experiments._reserve_annotation_fd()


def test_proofline_run_cli_emits_a_versioned_json_document(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = VerificationReport(
        "baseline",
        "candidate",
        (
            ClaimResult(
                "contract",
                "candidate-exit-success",
                "candidate_exit_success",
                "pass",
                "candidate exits successfully",
                "candidate=exit 0",
            ),
        ),
    )
    experiment = ExperimentResult(
        tmp_path / "baseline.runpack",
        tmp_path / "candidate.runpack",
        0,
        0,
        report,
    )
    monkeypatch.setattr(proofline_cli, "run_experiment", lambda *args, **kwargs: experiment)

    status = proofline_cli.main(
        [
            "run",
            str(tmp_path / "contract.yaml"),
            "--baseline-ref",
            "baseline-sha",
            "--candidate-ref",
            "candidate-sha",
            "--workload",
            "workload.py",
            "--format",
            "json",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert status == 0
    assert captured.err == ""
    assert set(payload) == {
        "baseline_exit_code",
        "baseline_runpack",
        "candidate_exit_code",
        "candidate_runpack",
        "document_type",
        "format_version",
        "verification",
    }
    assert payload["document_type"] == "proofline.experiment"
    assert payload["format_version"] == "2"
    assert payload["verification"]["document_type"] == "proofline.verification"
    assert payload["verification"]["format_version"] == "2"


def test_proofline_run_cli_forwards_the_selected_workload_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = VerificationReport("baseline", "candidate", ())
    experiment = ExperimentResult(
        tmp_path / "baseline.runpack",
        tmp_path / "candidate.runpack",
        0,
        0,
        report,
    )
    calls: list[dict[str, object]] = []

    def run(*args: object, **kwargs: object) -> ExperimentResult:
        calls.append(kwargs)
        return experiment

    monkeypatch.setattr(proofline_cli, "run_experiment", run)
    selected = tmp_path / "workload-python"

    status = proofline_cli.main(
        [
            "run",
            str(tmp_path / "contract.yaml"),
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "HEAD",
            "--workload",
            "workload.py",
            "--python",
            str(selected),
            "--capture-level",
            "process",
        ]
    )

    assert status == 0
    assert capsys.readouterr().err == ""
    assert calls[0]["python_executable"] == selected
    assert calls[0]["capture_level"] == "process"


@pytest.mark.parametrize(
    ("arguments", "operation"),
    [
        (
            [
                "run",
                "contract.yaml",
                "--baseline-ref",
                "main",
                "--candidate-ref",
                "HEAD",
                "--workload",
                "workload.py",
            ],
            "run",
        ),
        (
            [
                "search",
                "contract.yaml",
                "--parameters",
                "parameters.yaml",
                "--baseline-ref",
                "main",
                "--candidate-ref",
                "HEAD",
                "--workload",
                "workload.py",
            ],
            "search",
        ),
    ],
)
def test_proofline_execution_surfaces_forward_detached_json_launches(
    arguments: list[str],
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launches: list[tuple[tuple[str, ...], str, bool, str]] = []

    def launch(
        worker_arguments: tuple[str, ...],
        *,
        module: str,
        detached: bool = False,
        detached_format: str = "text",
    ) -> int:
        launches.append((worker_arguments, module, detached, detached_format))
        return 0

    monkeypatch.setattr(proofline_cli, "capture_worker_client_event", lambda: None)
    monkeypatch.setattr(proofline_cli, "run_capture_worker", launch)

    status = proofline_cli.main(
        [*arguments, "--detach", "--format", "json"],
        _launch_capture_worker=True,
    )

    assert status == 0
    assert launches == [
        (
            (*arguments, "--detach", "--format", "json"),
            "runtime_tools.proofline.cli",
            True,
            "json",
        )
    ]
    assert operation in launches[0][0]


def test_proofline_run_cli_reports_invalid_workload_python_without_creating_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "results"

    status = proofline_cli.main(
        [
            "run",
            str(tmp_path / "missing-contract.yaml"),
            "--baseline-ref",
            "bad-ref",
            "--candidate-ref",
            "bad-ref",
            "--workload",
            "missing.py",
            "--python",
            str(tmp_path / "missing-python"),
            "--output-dir",
            str(output),
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert captured.err.startswith("proofline: workload Python does not exist:")
    assert not output.exists()


def test_proofline_run_invalid_input_does_not_publish_a_report(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "results"
    report = tmp_path / "report.json"

    status = proofline_cli.main(
        [
            "run",
            str(tmp_path / "missing-contract.yaml"),
            "--baseline-ref",
            "bad-ref",
            "--candidate-ref",
            "bad-ref",
            "--workload",
            "missing.py",
            "--python",
            str(tmp_path / "missing-python"),
            "--output-dir",
            str(output),
            "--report",
            str(report),
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert captured.err.startswith("proofline: workload Python does not exist:")
    assert not output.exists()
    assert not report.exists()
    assert not tuple(tmp_path.glob(".contrail-report.tmp-*"))


def test_proofline_experiment_isolates_refs_and_preserves_runpacks(tmp_path: Path) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text("print('baseline')\n", encoding="utf-8")
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    workload.write_text("print('candidate')\n", encoding="utf-8")
    _git(repo, "commit", "-am", "candidate")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    output = tmp_path / "results"

    result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        workload_args=(),
        output_dir=output,
        cwd=repo,
        capture_level="process",
    )

    assert result.baseline_exit_code == 0
    assert result.candidate_exit_code == 0
    assert result.baseline_runpack.is_file()
    assert result.candidate_runpack.is_file()
    assert result.verification.passed is False
    assert result.verification.results[0].observed == "different"
    assert result.diff is not None
    assert result.diff.baseline.id == result.verification.baseline_id
    assert result.diff.candidate.id == result.verification.candidate_id
    assert result.diff.output_equivalent is False
    for runpack in (result.baseline_runpack, result.candidate_runpack):
        with RunpackReader(runpack) as reader:
            capture = reader.execution().metadata["capture"]
        assert isinstance(capture, dict)
        assert capture["level"] == "process"
        assert isinstance(capture["process_observer"], dict)
    worktrees = _git(repo, "worktree", "list", "--porcelain")
    assert worktrees.count("worktree ") == 1
    assert _git(repo, "status", "--short") == "?? contract.yaml"


def test_proofline_capture_worker_finishes_both_refs_after_cli_is_killed(
    tmp_path: Path,
) -> None:
    repo = _git_repository(tmp_path)
    ready = tmp_path / "baseline-ready"
    workload = repo / "workload.py"
    workload.write_text(
        "from pathlib import Path\n"
        "import time\n"
        f"Path({str(ready)!r}).write_text('ready', encoding='utf-8')\n"
        "time.sleep(0.4)\n"
        "print('survived client loss')\n",
        encoding="utf-8",
    )
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: completion\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )
    output = tmp_path / "results"
    retained_report = tmp_path / "proofline-report.json"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    client = subprocess.Popen(
        (
            sys.executable,
            "-m",
            "runtime_tools.proofline.cli",
            "run",
            str(contract),
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "main",
            "--workload",
            "workload.py",
            "--output-dir",
            str(output),
            "--report",
            str(retained_report),
        ),
        cwd=repo,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()

        os.kill(client.pid, signal.SIGKILL)
        assert client.wait(timeout=5) == -signal.SIGKILL
        deadline = time.monotonic() + 5
        job_id: str | None = None
        while time.monotonic() < deadline:
            listed = subprocess.run(
                (
                    sys.executable,
                    "-m",
                    "runtime_tools.contrail_cli",
                    "job",
                    "list",
                    "--format",
                    "json",
                ),
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            jobs = json.loads(listed.stdout)["jobs"]
            if jobs and jobs[0]["client_disconnected"]:
                job_id = jobs[0]["job_id"]
                assert jobs[0]["operation"] == "proofline run"
                break
            time.sleep(0.01)
        assert job_id is not None

        waited = subprocess.run(
            (
                sys.executable,
                "-m",
                "runtime_tools.contrail_cli",
                "job",
                "wait",
                job_id,
                "--format",
                "json",
            ),
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=15,
        )
        stdout, stderr = client.communicate(timeout=5)

        assert waited.returncode == 0
        waited_job = json.loads(waited.stdout)["job"]
        assert waited_job["state"] == "complete"
        assert waited_job["client_disconnected"] is True
        assert waited_job["artifacts"] == [
            str(output / "baseline.runpack"),
            str(output / "candidate.runpack"),
            str(retained_report),
        ]
        assert (output / "candidate.runpack").is_file()
        assert retained_report.is_file()
        assert stderr == ""
        assert "PROOFLINE" in stdout
        assert "baseline artifact:" in stdout
        assert "candidate artifact:" in stdout
        for name in ("baseline", "candidate"):
            with RunpackReader(output / f"{name}.runpack") as reader:
                capture = reader.execution().metadata["capture"]
            assert isinstance(capture, dict)
            assert capture["worker"] == {
                "format_version": 1,
                "mode": "separate-process",
                "client_disconnected": True,
            }
        assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1
    finally:
        if client.poll() is None:
            client.kill()
            client.wait(timeout=5)


def test_proofline_detached_json_run_replays_the_final_document_and_report(
    tmp_path: Path,
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text("print('detached proofline')\n", encoding="utf-8")
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: completion\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )
    output = tmp_path / "results"
    report = tmp_path / "proofline-report.json"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}

    launched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.proofline.cli",
            "run",
            str(contract),
            "--baseline-ref",
            "main",
            "--candidate-ref",
            "main",
            "--workload",
            "workload.py",
            "--output-dir",
            str(output),
            "--report",
            str(report),
            "--detach",
            "--format",
            "json",
        ),
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert launched.returncode == 0
    launch_document = json.loads(launched.stdout)
    assert launch_document["document_type"] == "runtime.capture_job"
    launch_job = launch_document["job"]
    assert launch_job["state"] == "starting"
    assert launch_job["detached"] is True
    job_id = launch_job["job_id"]
    waited = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.contrail_cli",
            "job",
            "wait",
            job_id,
            "--format",
            "json",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=15,
    )
    replayed = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "job",
            "output",
            job_id,
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert waited.returncode == 0
    waited_job = json.loads(waited.stdout)["job"]
    assert waited_job["artifacts"] == [
        str(output / "baseline.runpack"),
        str(output / "candidate.runpack"),
        str(report),
    ]
    assert waited_job["output"]["stdout_truncated"] is False
    assert replayed.returncode == 0
    replayed_document = json.loads(replayed.stdout)
    assert replayed_document["document_type"] == "proofline.experiment"
    assert replayed.stderr == ""
    assert report.read_text(encoding="utf-8") == replayed.stdout
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_proofline_can_run_both_refs_with_a_separate_workload_python(tmp_path: Path) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text(
        "import proofline_workload_dependency_a13c\n"
        "print(proofline_workload_dependency_a13c.VALUE)\n",
        encoding="utf-8",
    )
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: workload-environment\nassertions:\n  - type: candidate_exit_success\n",
        encoding="utf-8",
    )
    environment = tmp_path / "workload environment"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    scripts = environment / ("Scripts" if os.name == "nt" else "bin")
    workload_python = scripts / ("python.exe" if os.name == "nt" else "python")
    purelib = Path(
        subprocess.run(
            (
                str(workload_python),
                "-c",
                "import sysconfig; print(sysconfig.get_path('purelib'))",
            ),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    (purelib / "proofline_workload_dependency_a13c.py").write_text(
        "VALUE = 'workload dependency available'\n",
        encoding="utf-8",
    )

    default_result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="main",
        workload=Path("workload.py"),
        workload_args=(),
        output_dir=tmp_path / "default-results",
        cwd=repo,
    )
    selected_result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="main",
        workload=Path("workload.py"),
        workload_args=(),
        output_dir=tmp_path / "selected-results",
        cwd=repo,
        python_executable=workload_python,
        bind_artifacts=True,
    )

    assert default_result.baseline_exit_code != 0
    assert default_result.candidate_exit_code != 0
    assert default_result.verification.passed is False
    assert selected_result.baseline_exit_code == 0
    assert selected_result.candidate_exit_code == 0
    assert selected_result.verification.passed is True
    assert selected_result.artifact_bindings is not None
    assert selected_result.as_json_value(include_evidence=True)["artifact_bindings"] == {
        "baseline": {
            "size_bytes": selected_result.baseline_runpack.stat().st_size,
            "sha256": hashlib.sha256(selected_result.baseline_runpack.read_bytes()).hexdigest(),
        },
        "candidate": {
            "size_bytes": selected_result.candidate_runpack.stat().st_size,
            "sha256": hashlib.sha256(selected_result.candidate_runpack.read_bytes()).hexdigest(),
        },
    }
    selected_path = Path(os.path.abspath(workload_python))
    for runpack in (selected_result.baseline_runpack, selected_result.candidate_runpack):
        with RunpackReader(runpack) as reader:
            assert reader.execution().command[0] == str(selected_path)
    assert workload_python.is_symlink()
    assert selected_path != workload_python.resolve()


def test_proofline_pwd_reads_each_ref_from_the_isolated_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text(
        "import os\nfrom pathlib import Path\n"
        "print((Path(os.environ['PWD']) / 'version.txt').read_text().strip())\n",
        encoding="utf-8",
    )
    version = repo / "version.txt"
    version.write_text("baseline\n", encoding="utf-8")
    _git(repo, "add", "workload.py", "version.txt")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    version.write_text("candidate\n", encoding="utf-8")
    _git(repo, "commit", "-am", "candidate")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PWD", str(repo))

    result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        workload_args=(),
        output_dir=tmp_path / "results",
        cwd=repo,
    )

    assert result.baseline_exit_code == 0
    assert result.candidate_exit_code == 0
    assert result.verification.passed is False
    assert result.verification.results[0].observed == "different"


def test_proofline_experiment_verifies_the_contract_snapshot_loaded_before_workloads(
    tmp_path: Path,
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    contract = repo / "contract.yaml"
    mutated_contract = "name: changed\nassertions:\n  - type: exit_code_equivalent\n"
    workload.write_text(
        "from pathlib import Path\nimport sys\n"
        f"Path({str(contract)!r}).write_text({mutated_contract!r}, encoding='utf-8')\n"
        "print('baseline')\n",
        encoding="utf-8",
    )
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    workload.write_text(
        "from pathlib import Path\nimport sys\n"
        f"Path({str(contract)!r}).write_text({mutated_contract!r}, encoding='utf-8')\n"
        "print('candidate')\n",
        encoding="utf-8",
    )
    _git(repo, "commit", "-am", "candidate")
    contract.write_text(
        "name: original\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )

    result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        workload_args=(),
        output_dir=tmp_path / "results",
        cwd=repo,
    )

    assert contract.read_text(encoding="utf-8") == mutated_contract
    assert result.verification.passed is False
    assert [
        (claim.contract, claim.type, claim.status) for claim in result.verification.results
    ] == [("original", "output_equivalent", "fail")]


def test_proofline_identical_refs_expose_distinct_isolated_worktree_paths(tmp_path: Path) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text(
        "import os\n"
        "import sys\n"
        "from pathlib import Path\n"
        "from runtime_tools import runtime\n"
        "observed = {\n"
        "    'pwd': os.environ['PWD'],\n"
        "    'cwd': str(Path.cwd()),\n"
        "    'argv': str(Path(sys.argv[0])),\n"
        "    'annotations': os.environ['CONTRAIL_ANNOTATIONS_FILE'],\n"
        "    'annotation_fd': os.environ['_CONTRAIL_ANNOTATIONS_FD'],\n"
        "}\n"
        "for value in observed.values():\n"
        "    print(value)\n"
        "observed['fallback'] = os.environ['_CONTRAIL_ANNOTATIONS_FALLBACK']\n"
        "os.chdir('/')\n"
        "with runtime.stage('captured-stage'):\n"
        "    runtime.event('captured-event', **observed)\n",
        encoding="utf-8",
    )
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )

    result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="main",
        workload=Path("workload.py"),
        workload_args=(),
        output_dir=tmp_path / "results",
        cwd=repo,
    )

    assert result.verification.passed is False
    assert result.verification.results[0].observed == "different"
    with RunpackReader(result.baseline_runpack) as reader:
        baseline = reader.execution()
        baseline_events = reader.events()
        baseline_observed = next(
            event.attributes for event in baseline_events if event.name == "captured-event"
        )
        baseline_event_names = {event.name for event in baseline_events}
    with RunpackReader(result.candidate_runpack) as reader:
        candidate = reader.execution()
        candidate_events = reader.events()
        candidate_observed = next(
            event.attributes for event in candidate_events if event.name == "captured-event"
        )
        candidate_event_names = {event.name for event in candidate_events}
    assert baseline.command == candidate.command
    assert baseline.command[1] == candidate.command[1] == "./workload.py"
    assert baseline.working_directory != candidate.working_directory
    assert {key: value for key, value in baseline.metadata.items() if key != "output"} == {
        key: value for key, value in candidate.metadata.items() if key != "output"
    }
    assert {
        key: value
        for key, value in baseline_observed.items()
        if key not in {"cwd", "fallback", "pwd"}
    } == {
        key: value
        for key, value in candidate_observed.items()
        if key not in {"cwd", "fallback", "pwd"}
    }
    assert baseline_observed["fallback"] != candidate_observed["fallback"]
    baseline_fallback = Path(str(baseline_observed["fallback"]))
    candidate_fallback = Path(str(candidate_observed["fallback"]))
    assert baseline_fallback.parent == candidate_fallback.parent
    assert baseline_fallback.parent.name == "annotations"
    assert baseline_fallback.name.startswith(".baseline.runpack.annotations-")
    assert candidate_fallback.name.startswith(".candidate.runpack.annotations-")
    assert not baseline_fallback.exists()
    assert not candidate_fallback.exists()
    assert baseline_observed["pwd"] == baseline_observed["cwd"] == baseline.working_directory
    assert candidate_observed["pwd"] == candidate_observed["cwd"] == candidate.working_directory
    assert baseline_observed["pwd"] != candidate_observed["pwd"]
    assert baseline_observed["argv"] == candidate_observed["argv"] == "workload.py"
    annotation_path = Path(str(baseline_observed["annotations"]))
    assert annotation_path == Path(f"/dev/fd/{baseline_observed['annotation_fd']}")
    assert {"captured-stage", "captured-event"} <= baseline_event_names
    assert {"captured-stage", "captured-event"} <= candidate_event_names
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_proofline_isolates_late_annotations_from_a_detached_baseline_child(
    tmp_path: Path,
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    detached_child = (
        "import os, sys, time\n"
        "from pathlib import Path\n"
        "from runtime_tools import runtime\n"
        "ready, done, succeeded, after_cleanup, blocked = map(Path, sys.argv[1:])\n"
        "os.chdir('/')\n"
        "deadline = time.monotonic() + 5\n"
        "while not ready.exists() and time.monotonic() < deadline:\n"
        "    time.sleep(0.01)\n"
        "try:\n"
        "    runtime.event('baseline-late')\n"
        "    succeeded.write_text('succeeded', encoding='utf-8')\n"
        "finally:\n"
        "    done.write_text('done', encoding='utf-8')\n"
        "deadline = time.monotonic() + 5\n"
        "while not after_cleanup.exists() and time.monotonic() < deadline:\n"
        "    time.sleep(0.01)\n"
        "try:\n"
        "    runtime.event('baseline-after-cleanup')\n"
        "except OSError:\n"
        "    blocked.write_text('blocked', encoding='utf-8')\n"
    )
    workload.write_text(
        "import subprocess, sys\n"
        f"child = {detached_child!r}\n"
        "subprocess.Popen(\n"
        "    (sys.executable, '-c', child, *sys.argv[1:]),\n"
        "    stdin=subprocess.DEVNULL,\n"
        "    stdout=subprocess.DEVNULL,\n"
        "    stderr=subprocess.DEVNULL,\n"
        "    close_fds=True,\n"
        "    start_new_session=True,\n"
        ")\n",
        encoding="utf-8",
    )
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    workload.write_text(
        "import sys, time\n"
        "from pathlib import Path\n"
        "from runtime_tools import runtime\n"
        "ready, done, _, _, _ = map(Path, sys.argv[1:])\n"
        "ready.write_text('ready', encoding='utf-8')\n"
        "deadline = time.monotonic() + 5\n"
        "while not done.exists() and time.monotonic() < deadline:\n"
        "    time.sleep(0.01)\n"
        "runtime.event('candidate-event')\n",
        encoding="utf-8",
    )
    _git(repo, "commit", "-am", "candidate")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    ready = tmp_path / "candidate-ready"
    done = tmp_path / "baseline-done"
    succeeded = tmp_path / "baseline-late-succeeded"
    after_cleanup = tmp_path / "after-experiment-cleanup"
    blocked = tmp_path / "baseline-after-cleanup-blocked"

    result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        workload_args=(
            str(ready),
            str(done),
            str(succeeded),
            str(after_cleanup),
            str(blocked),
        ),
        output_dir=tmp_path / "results",
        cwd=repo,
    )

    assert result.verification.passed is True
    assert done.is_file()
    assert succeeded.is_file()
    after_cleanup.touch()
    deadline = time.monotonic() + 5
    while not blocked.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert blocked.is_file()
    with RunpackReader(result.candidate_runpack) as reader:
        candidate_event_names = {event.name for event in reader.events()}
    assert "candidate-event" in candidate_event_names
    assert "baseline-late" not in candidate_event_names
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_proofline_separate_roots_prevent_transient_cross_arm_contamination(
    tmp_path: Path,
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    version = repo / "version.txt"
    detached_child = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "version, ready, mutated, read, restored, unreachable = map(Path, sys.argv[1:])\n"
        "deadline = time.monotonic() + 5\n"
        "while not ready.exists() and time.monotonic() < deadline:\n"
        "    time.sleep(0.01)\n"
        "try:\n"
        "    original = version.read_text(encoding='utf-8')\n"
        "    version.write_text('baseline\\n', encoding='utf-8')\n"
        "    mutated.write_text('mutated', encoding='utf-8')\n"
        "except OSError:\n"
        "    unreachable.write_text('unreachable', encoding='utf-8')\n"
        "    raise SystemExit\n"
        "while not read.exists() and time.monotonic() < deadline:\n"
        "    time.sleep(0.01)\n"
        "version.write_text(original, encoding='utf-8')\n"
        "restored.write_text('restored', encoding='utf-8')\n"
    )
    workload.write_text(
        "import subprocess, sys\n"
        "from pathlib import Path\n"
        f"child = {detached_child!r}\n"
        "subprocess.Popen(\n"
        "    (sys.executable, '-c', child, str(Path('version.txt').resolve()), *sys.argv[1:]),\n"
        "    stdin=subprocess.DEVNULL,\n"
        "    stdout=subprocess.DEVNULL,\n"
        "    stderr=subprocess.DEVNULL,\n"
        "    close_fds=True,\n"
        "    start_new_session=True,\n"
        ")\n"
        "print(Path('version.txt').read_text(encoding='utf-8').strip())\n",
        encoding="utf-8",
    )
    version.write_text("baseline\n", encoding="utf-8")
    _git(repo, "add", "workload.py", "version.txt")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    workload.write_text(
        "import sys, time\n"
        "from pathlib import Path\n"
        "ready, mutated, read, restored, unreachable = map(Path, sys.argv[1:])\n"
        "ready.write_text('ready', encoding='utf-8')\n"
        "deadline = time.monotonic() + 5\n"
        "while not (mutated.exists() or unreachable.exists()) and time.monotonic() < deadline:\n"
        "    time.sleep(0.01)\n"
        "observed = Path('version.txt').read_text(encoding='utf-8').strip()\n"
        "if mutated.exists():\n"
        "    read.write_text('read', encoding='utf-8')\n"
        "    while not restored.exists() and time.monotonic() < deadline:\n"
        "        time.sleep(0.01)\n"
        "print(observed)\n",
        encoding="utf-8",
    )
    version.write_text("candidate\n", encoding="utf-8")
    _git(repo, "add", "workload.py", "version.txt")
    _git(repo, "commit", "-m", "candidate")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    ready = tmp_path / "candidate-ready"
    mutated = tmp_path / "candidate-mutated"
    read = tmp_path / "candidate-read"
    restored = tmp_path / "candidate-restored"
    unreachable = tmp_path / "baseline-path-unreachable"
    output = tmp_path / "results"

    result = run_experiment(
        contract,
        baseline_ref="main",
        candidate_ref="candidate",
        workload=Path("workload.py"),
        workload_args=tuple(str(path) for path in (ready, mutated, read, restored, unreachable)),
        output_dir=output,
        cwd=repo,
    )

    assert result.verification.passed is False
    assert result.verification.results[0].observed == "different"
    assert unreachable.is_file()
    assert not mutated.exists()
    assert (output / "baseline.runpack").is_file()
    assert (output / "candidate.runpack").is_file()
    assert _git(repo, "show", "candidate:version.txt") == "candidate"
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


@pytest.mark.parametrize("mutation", ("tracked", "untracked"))
def test_proofline_rejects_candidate_worktree_mutations(
    tmp_path: Path,
    mutation: str,
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text("print('baseline')\n", encoding="utf-8")
    tracked = repo / "tracked.txt"
    tracked.write_text("original\n", encoding="utf-8")
    _git(repo, "add", "workload.py", "tracked.txt")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "switch", "-c", "candidate")
    target = "tracked.txt" if mutation == "tracked" else "untracked.txt"
    workload.write_text(
        "from pathlib import Path\n"
        f"Path({target!r}).write_text('mutated\\n', encoding='utf-8')\n"
        "print('candidate')\n",
        encoding="utf-8",
    )
    _git(repo, "commit", "-am", "candidate")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )

    with pytest.raises(ExperimentError, match="candidate workload modified"):
        run_experiment(
            contract,
            baseline_ref="main",
            candidate_ref="candidate",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=tmp_path / "results",
            cwd=repo,
        )

    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_proofline_preserves_primary_error_when_worktree_cleanup_also_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outside = tmp_path / "outside.py"
    outside.write_text("print('outside')\n", encoding="utf-8")
    repo = _git_repository(tmp_path)
    (repo / "workload.py").symlink_to(outside)
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "symlink workload")
    contract = repo / "contract.yaml"
    contract.write_text(
        "name: output\nassertions:\n  - type: output_equivalent\n",
        encoding="utf-8",
    )
    git = experiments._git
    reserve_annotation_fd = experiments._reserve_annotation_fd
    reserved_descriptors: list[int] = []

    def track_annotation_fd() -> int:
        descriptor = reserve_annotation_fd()
        reserved_descriptors.append(descriptor)
        return descriptor

    def fail_after_worktree_removal(git_repo: Path, *args: str, capture: bool = False) -> str:
        result = git(git_repo, *args, capture=capture)
        if args[:2] == ("worktree", "remove"):
            raise ExperimentError("simulated cleanup failure")
        return result

    monkeypatch.setattr(experiments, "_git", fail_after_worktree_removal)
    monkeypatch.setattr(experiments, "_reserve_annotation_fd", track_annotation_fd)

    with pytest.raises(ExperimentError, match="must resolve inside the isolated worktree") as error:
        run_experiment(
            contract,
            baseline_ref="main",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=tmp_path / "results",
            cwd=repo,
        )

    assert error.value.__notes__ == [
        "Proofline worktree cleanup also failed: simulated cleanup failure"
    ]
    with pytest.raises(OSError):
        os.fstat(reserved_descriptors[0])
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1
    assert not (tmp_path / "results").exists()


def test_proofline_rejects_dangling_output_before_processing_inputs(tmp_path: Path) -> None:
    output = tmp_path / "results"
    output.symlink_to(tmp_path / "missing-output-target")

    with pytest.raises(ExperimentError, match="refusing to reuse output directory"):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="bad\0ref",
            candidate_ref="bad\0ref",
            workload=Path("bad\0workload.py"),
            workload_args=("bad\0argument",),
            output_dir=output,
            cwd=tmp_path,
        )

    assert output.is_symlink()


@pytest.mark.parametrize(
    ("kind", "message"),
    (
        ("missing", "workload Python does not exist"),
        ("directory", "workload Python must be a regular file"),
        ("non-executable", "workload Python is not executable"),
        ("nul", "workload Python path cannot contain NUL bytes"),
        ("unrepresentable", "workload Python path is not representable"),
    ),
)
def test_proofline_rejects_invalid_workload_python_before_other_inputs_or_side_effects(
    tmp_path: Path,
    kind: str,
    message: str,
) -> None:
    python_executable = tmp_path / "workload-python"
    if kind == "directory":
        python_executable.mkdir()
    elif kind == "non-executable":
        python_executable.write_text("#!/bin/sh\n", encoding="utf-8")
        python_executable.chmod(0o600)
    elif kind == "nul":
        python_executable = Path("bad\0python")
    elif kind == "unrepresentable":
        python_executable = Path("bad-\ud800-python")
    output = tmp_path / "results"

    with pytest.raises(ExperimentError, match=message):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="bad\0ref",
            candidate_ref="bad\0ref",
            workload=Path("bad\0workload.py"),
            workload_args=("bad\0argument",),
            output_dir=output,
            cwd=tmp_path,
            python_executable=python_executable,
        )

    assert not output.exists()


def test_proofline_rejects_option_like_git_refs_before_creating_outputs(
    tmp_path: Path,
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text("print('workload')\n", encoding="utf-8")
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")

    output = tmp_path / "results"
    with pytest.raises(ExperimentError, match="cannot start with"):
        run_experiment(
            repo / "contract.yaml",
            baseline_ref="--help",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=output,
            cwd=repo,
        )

    assert not output.exists()
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


@pytest.mark.parametrize(
    ("ref", "message"),
    (("bad\0ref", "cannot contain NUL bytes"), ("bad-\udcff", "must be valid UTF-8")),
)
def test_proofline_rejects_unrepresentable_git_refs_before_loading_contracts(
    tmp_path: Path, ref: str, message: str
) -> None:
    output = tmp_path / "results"

    with pytest.raises(ExperimentError, match=message):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref=ref,
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=output,
            cwd=tmp_path,
        )

    assert not output.exists()


def test_proofline_bounds_git_refs_before_loading_contracts(tmp_path: Path) -> None:
    with pytest.raises(ExperimentError, match="Git refs cannot exceed 4096 UTF-8 bytes"):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="a" * 4_097,
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=tmp_path / "results",
            cwd=tmp_path,
        )


@pytest.mark.parametrize(
    ("workload", "workload_args", "message"),
    (
        (Path("bad\0workload.py"), (), "workload path cannot contain NUL bytes"),
        (Path("bad-\udcff.py"), (), "workload path must be valid UTF-8"),
        (Path("workload.py"), ("bad\0argument",), "workload arguments cannot contain NUL bytes"),
        (Path("workload.py"), ("bad-\udcff",), "workload arguments must be valid UTF-8"),
    ),
)
def test_proofline_rejects_unrepresentable_workloads_before_loading_contracts(
    tmp_path: Path,
    workload: Path,
    workload_args: tuple[str, ...],
    message: str,
) -> None:
    output = tmp_path / "results"

    with pytest.raises(ExperimentError, match=message):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="main",
            candidate_ref="main",
            workload=workload,
            workload_args=workload_args,
            output_dir=output,
            cwd=tmp_path,
        )

    assert not output.exists()


@pytest.mark.parametrize(
    ("workload_args", "message"),
    (
        (("value",) * 1_025, "cannot contain more than 1024 entries"),
        (("x" * (1024 * 1024),), "cannot exceed 1048576 UTF-8 bytes"),
    ),
)
def test_proofline_bounds_workload_invocations_before_loading_contracts(
    tmp_path: Path, workload_args: tuple[str, ...], message: str
) -> None:
    with pytest.raises(ExperimentError, match=message):
        run_experiment(
            tmp_path / "missing-contract.yaml",
            baseline_ref="main",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=workload_args,
            output_dir=tmp_path / "results",
            cwd=tmp_path,
        )


def test_proofline_validates_contracts_before_creating_worktrees_or_outputs(
    tmp_path: Path,
) -> None:
    repo = _git_repository(tmp_path)
    workload = repo / "workload.py"
    workload.write_text("raise RuntimeError('must not execute')\n", encoding="utf-8")
    _git(repo, "add", "workload.py")
    _git(repo, "commit", "-m", "workload")
    contract = repo / "invalid.yaml"
    contract.write_text("name: invalid\nassertions:\n  - type: unknown\n", encoding="utf-8")
    output = tmp_path / "results"

    with pytest.raises(ContractError, match="unsupported assertion type: unknown"):
        run_experiment(
            contract,
            baseline_ref="main",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=output,
            cwd=repo,
        )

    assert not output.exists()
    assert _git(repo, "worktree", "list", "--porcelain").count("worktree ") == 1


def test_proofline_normalizes_contract_path_symlink_loops(tmp_path: Path) -> None:
    contract = tmp_path / "loop.yaml"
    contract.symlink_to(contract.name)
    output = tmp_path / "results"

    with pytest.raises(ExperimentError, match="could not resolve contract path"):
        run_experiment(
            contract,
            baseline_ref="main",
            candidate_ref="main",
            workload=Path("workload.py"),
            workload_args=(),
            output_dir=output,
            cwd=tmp_path,
        )

    assert not output.exists()
