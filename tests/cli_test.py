from __future__ import annotations

import hashlib
import json
import os
import select
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

import runtime_tools.capture as capture_module
import runtime_tools.cli as cli_module
from runtime_tools import CaptureError, __version__
from runtime_tools.capture import record_process
from runtime_tools.capture_jobs import (
    CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES,
    CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
    CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
)
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
    with RunpackReader(output) as reader:
        capture_metadata = reader.execution().metadata["capture"]
    assert isinstance(capture_metadata, dict)
    assert capture_metadata["worker"] == {
        "format_version": 1,
        "mode": "separate-process",
        "client_disconnected": False,
    }


def test_runtime_cli_detaches_and_replays_bounded_private_output(tmp_path: Path) -> None:
    output = tmp_path / "detached.runpack"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    launched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--detach",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import sys, time; print('detached stdout'); "
            "print('detached stderr', file=sys.stderr); time.sleep(0.2)",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert launched.returncode == 0
    assert launched.stderr == ""
    assert launched.stdout.startswith("CAPTURE JOB DETACHED\n\n")
    job_id = next(
        line.removeprefix("id:     ")
        for line in launched.stdout.splitlines()
        if line.startswith("id:     ")
    )
    waited = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
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
        timeout=10,
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
    job = json.loads(waited.stdout)["job"]
    assert job["state"] == "complete"
    assert job["detached"] is True
    assert job["client_disconnected"] is False
    assert job["artifacts"] == [str(output)]
    assert job["output"] == {
        "head_limit_bytes_per_stream": CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES,
        "limit_bytes_per_stream": CAPTURE_JOB_OUTPUT_LIMIT_BYTES,
        "retained": True,
        "retention_strategy": "head-tail",
        "stderr_head_size_bytes": len(f"detached stderr\nrecorded {output}\n".encode()),
        "stderr_omitted_bytes": 0,
        "stderr_omitted_bytes_truncated": False,
        "stderr_size_bytes": len(f"detached stderr\nrecorded {output}\n".encode()),
        "stderr_tail_size_bytes": 0,
        "stderr_truncated": False,
        "stdout_head_size_bytes": len(b"detached stdout\n"),
        "stdout_omitted_bytes": 0,
        "stdout_omitted_bytes_truncated": False,
        "stdout_size_bytes": len(b"detached stdout\n"),
        "stdout_tail_size_bytes": 0,
        "stdout_truncated": False,
        "tail_limit_bytes_per_stream": CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES,
    }
    assert replayed.returncode == 0
    assert replayed.stdout == "detached stdout\n"
    assert replayed.stderr == f"detached stderr\nrecorded {output}\n"
    assert output.is_file()


def test_runtime_cli_follows_detached_output_before_completion(tmp_path: Path) -> None:
    output = tmp_path / "followed.runpack"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    workload = (
        "import sys, time\n"
        "print('live stdout', flush=True)\n"
        "print('live stderr', file=sys.stderr, flush=True)\n"
        "time.sleep(2)\n"
        "print('final stdout', flush=True)\n"
        "print('final stderr', file=sys.stderr, flush=True)\n"
    )
    launched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--detach",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            workload,
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    job_id = next(
        line.removeprefix("id:     ")
        for line in launched.stdout.splitlines()
        if line.startswith("id:     ")
    )
    follower = subprocess.Popen(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "job",
            "output",
            job_id,
            "--follow",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    try:
        assert follower.stdout is not None
        readable, _, _ = select.select((follower.stdout,), (), (), 5)
        assert readable
        assert follower.stdout.readline() == b"live stdout\n"
        status = subprocess.run(
            (
                sys.executable,
                "-m",
                "runtime_tools.cli",
                "job",
                "status",
                job_id,
                "--format",
                "json",
            ),
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        remaining_stdout, followed_stderr = follower.communicate(timeout=10)
    finally:
        if follower.poll() is None:
            follower.kill()
            follower.wait(timeout=5)

    assert launched.returncode == 0
    assert status.returncode == 0
    assert json.loads(status.stdout)["job"]["state"] == "running"
    assert follower.returncode == 0
    assert remaining_stdout == b"final stdout\n"
    assert followed_stderr == (f"live stderr\nfinal stderr\nrecorded {output}\n".encode())
    assert output.is_file()


def test_interrupting_output_follow_does_not_cancel_the_capture(tmp_path: Path) -> None:
    output = tmp_path / "follow-interrupted.runpack"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    launched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--detach",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import time; print('still running', flush=True); time.sleep(30)",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    job_id = next(
        line.removeprefix("id:     ")
        for line in launched.stdout.splitlines()
        if line.startswith("id:     ")
    )
    follower = subprocess.Popen(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "job",
            "output",
            job_id,
            "--follow",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    try:
        assert follower.stdout is not None
        readable, _, _ = select.select((follower.stdout,), (), (), 5)
        assert readable
        assert follower.stdout.readline() == b"still running\n"
        follower.send_signal(signal.SIGINT)
        remaining_stdout, followed_stderr = follower.communicate(timeout=5)
        status = subprocess.run(
            (
                sys.executable,
                "-m",
                "runtime_tools.cli",
                "job",
                "status",
                job_id,
                "--format",
                "json",
            ),
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        cancelled = subprocess.run(
            (
                sys.executable,
                "-m",
                "runtime_tools.cli",
                "job",
                "cancel",
                job_id,
                "--format",
                "json",
            ),
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=10,
        )
    finally:
        if follower.poll() is None:
            follower.kill()
            follower.wait(timeout=5)

    assert launched.returncode == 0
    assert follower.returncode == 128 + signal.SIGINT
    assert remaining_stdout == b""
    assert followed_stderr == b""
    assert status.returncode == 0
    assert json.loads(status.stdout)["job"]["state"] == "running"
    assert cancelled.returncode == 0
    cancelled_job = json.loads(cancelled.stdout)["job"]
    assert cancelled_job["state"] == "complete"
    assert cancelled_job["exit_status"] == 128 + signal.SIGINT
    assert not output.exists()


def test_detached_capture_discards_output_beyond_each_private_stream_bound(
    tmp_path: Path,
) -> None:
    output = tmp_path / "bounded-detached.runpack"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    launched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--detach",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import os; "
            f"os.write(1, b'H' * {CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES} + "
            f"b'M' * 257 + b'T' * {CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES})",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    job_id = next(
        line.removeprefix("id:     ")
        for line in launched.stdout.splitlines()
        if line.startswith("id:     ")
    )
    waited = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
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
        timeout=10,
    )
    replayed = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "job",
            "output",
            job_id,
            "--follow",
        ),
        check=False,
        capture_output=True,
        env=environment,
    )

    assert launched.returncode == 0
    assert waited.returncode == 0
    job = json.loads(waited.stdout)["job"]
    assert job["output"]["stdout_size_bytes"] == CAPTURE_JOB_OUTPUT_LIMIT_BYTES
    assert job["output"]["stdout_head_size_bytes"] == CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES
    assert job["output"]["stdout_tail_size_bytes"] == CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES
    assert job["output"]["stdout_omitted_bytes"] == 257
    assert job["output"]["stdout_omitted_bytes_truncated"] is False
    assert job["output"]["stdout_truncated"] is True
    assert replayed.returncode == 0
    assert replayed.stdout == (
        b"H" * CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES + b"T" * CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES
    )
    assert (
        b"runtime: retained stdout was truncated; omitted 257 bytes between retained head and tail"
    ) in replayed.stderr
    job_directory = job_root / job_id
    assert (job_directory / "stdout.log").stat().st_size == CAPTURE_JOB_OUTPUT_HEAD_LIMIT_BYTES
    assert (job_directory / "stdout.tail.log").stat().st_size == CAPTURE_JOB_OUTPUT_TAIL_LIMIT_BYTES
    assert not tuple(job_directory.glob("*.tmp"))


def test_detached_capture_can_be_cancelled_and_reaped_without_a_frontend(
    tmp_path: Path,
) -> None:
    output = tmp_path / "cancelled-detached.runpack"
    workload_pid = tmp_path / "detached-workload.pid"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    workload = (
        "from pathlib import Path\n"
        "import os\n"
        "import time\n"
        f"Path({str(workload_pid)!r}).write_text(str(os.getpid()), encoding='utf-8')\n"
        "print('detached workload started', flush=True)\n"
        "time.sleep(30)\n"
    )
    launched = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--detach",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            workload,
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    job_id = next(
        line.removeprefix("id:     ")
        for line in launched.stdout.splitlines()
        if line.startswith("id:     ")
    )
    process_id: int | None = None
    try:
        deadline = time.monotonic() + 10
        while not workload_pid.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert workload_pid.exists()
        process_id = int(workload_pid.read_text(encoding="utf-8"))

        cancelled = subprocess.run(
            (
                sys.executable,
                "-m",
                "runtime_tools.cli",
                "job",
                "cancel",
                job_id,
                "--format",
                "json",
            ),
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=10,
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

        assert cancelled.returncode == 0
        job = json.loads(cancelled.stdout)["job"]
        assert job["state"] == "complete"
        assert job["exit_status"] == 128 + signal.SIGINT
        assert job["detached"] is True
        assert job["artifacts"] == []
        assert replayed.stdout == "detached workload started\n"
        assert not output.exists()
        with pytest.raises(ProcessLookupError):
            os.kill(process_id, 0)
    finally:
        if process_id is not None:
            try:
                os.kill(process_id, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_runtime_cli_recovers_a_post_exit_capture_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "recovered.runpack"

    def fail_publication(temporary: Path, destination: Path) -> None:
        raise OSError("simulated controller publication loss")

    monkeypatch.setattr(capture_module, "publish_without_overwrite", fail_publication)
    with pytest.raises(CaptureError, match="could not publish runpack"):
        record_process((sys.executable, "-c", "pass"), output, name="recovered")
    checkpoint = next(tmp_path.glob(".recovered.runpack.tmp-*"))
    monkeypatch.undo()

    recovered = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "recover",
            str(checkpoint),
            "--output",
            str(output),
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recovered.returncode == 0
    assert recovered.stdout == ""
    assert recovered.stderr == f"recovered {output}\n"
    with RunpackReader(output) as reader:
        capture_metadata = reader.execution().metadata["capture"]
    assert isinstance(capture_metadata, dict)
    recovery = capture_metadata["recovery"]
    assert isinstance(recovery, dict)
    assert recovery["controller_restart_recovered"] is True


def test_runtime_cli_exposes_expensive_deep_capture_as_an_opt_in_mode(
    tmp_path: Path,
) -> None:
    output = tmp_path / "deep-cli.runpack"
    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--instrument",
            "deep",
            "--name",
            "deep-cli",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "def useful(): return sum(range(1000))\nuseful()",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert recorded.stdout == ""
    assert "deep instrumentation is intrusive and can materially perturb timings" in (
        recorded.stderr
    )
    assert f"recorded {output}" in recorded.stderr
    with RunpackReader(output) as reader:
        assert "__main__.useful" in {event.name for event in reader.events()}


def test_runtime_cli_exposes_statistical_sampling_as_an_opt_in_mode(
    tmp_path: Path,
) -> None:
    output = tmp_path / "sample-cli.runpack"
    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--instrument",
            "sample",
            "--name",
            "sample-cli",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import time\ndef useful(): time.sleep(0.08)\nuseful()",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert recorded.stdout == ""
    assert "sampling estimates Python hotspots and may perturb timings" in recorded.stderr
    assert f"recorded {output}" in recorded.stderr
    with RunpackReader(output) as reader:
        useful = next(event for event in reader.events() if event.name == "__main__.useful")
    assert useful.kind == "python.stack.sample"
    leaf_sample_count = useful.attributes["leaf_sample_count"]
    assert isinstance(leaf_sample_count, int) and leaf_sample_count >= 1


def test_runtime_cli_exposes_controller_side_process_tree_observation(
    tmp_path: Path,
) -> None:
    output = tmp_path / "process-tree-cli.runpack"
    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--observe-process-tree",
            "--name",
            "process-tree-cli",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(0.25)",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert f"recorded {output}" in recorded.stderr
    with RunpackReader(output) as reader:
        capture = reader.execution().metadata["capture"]
    assert isinstance(capture, dict)
    observer = capture["process_observer"]
    assert isinstance(observer, dict)
    assert observer["status"] == "complete"
    assert observer["process_count"] == 1


def test_runtime_cli_sample_capture_level_combines_sampling_and_process_observation(
    tmp_path: Path,
) -> None:
    output = tmp_path / "sample-level.runpack"
    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--capture-level",
            "sample",
            "--name",
            "sample-level",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "import time\ndef useful(): time.sleep(0.22)\nuseful()",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 0
    assert "sampling estimates Python hotspots and may perturb timings" in recorded.stderr
    with RunpackReader(output) as reader:
        capture = reader.execution().metadata["capture"]
    assert isinstance(capture, dict)
    assert capture["level"] == "sample"
    assert isinstance(capture["instrumentation"], dict)
    assert isinstance(capture["process_observer"], dict)


def test_runtime_cli_rejects_mixed_capture_preset_and_expert_flags(
    tmp_path: Path,
) -> None:
    output = tmp_path / "conflict.runpack"

    recorded = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--capture-level",
            "sample",
            "--instrument",
            "deep",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            "raise RuntimeError('must not run')",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert recorded.returncode == 2
    assert recorded.stdout == ""
    assert recorded.stderr == (
        "runtime: --capture-level cannot be combined with --instrument or --observe-process-tree\n"
    )
    assert not output.exists()


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


def test_capture_worker_finishes_after_cli_is_killed_mid_run(tmp_path: Path) -> None:
    output = tmp_path / "client-loss.runpack"
    ready = tmp_path / "workload-ready"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    workload = (
        "from pathlib import Path\n"
        "import time\n"
        f"Path({str(ready)!r}).write_text('ready', encoding='utf-8')\n"
        "time.sleep(0.8)\n"
        "print('survived client loss')\n"
    )
    client = subprocess.Popen(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "record",
            "--capture-level",
            "sample",
            "--name",
            "client-loss",
            "--output",
            str(output),
            "--",
            sys.executable,
            "-c",
            workload,
        ),
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
        job_payload: dict[str, object] | None = None
        while time.monotonic() < deadline:
            listed = subprocess.run(
                (
                    sys.executable,
                    "-m",
                    "runtime_tools.cli",
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
            assert listed.returncode == 0
            jobs = json.loads(listed.stdout)["jobs"]
            if jobs and jobs[0]["client_disconnected"]:
                job_payload = jobs[0]
                break
            time.sleep(0.01)
        assert job_payload is not None
        assert job_payload["state"] == "running"
        assert job_payload["operation"] == "runtime record"
        job_id = job_payload["job_id"]
        assert isinstance(job_id, str)

        waited = subprocess.run(
            (
                sys.executable,
                "-m",
                "runtime_tools.cli",
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
            timeout=10,
        )
        stdout, stderr = client.communicate(timeout=5)

        assert waited.returncode == 0
        waited_job = json.loads(waited.stdout)["job"]
        assert waited_job["state"] == "complete"
        assert waited_job["exit_status"] == 0
        assert waited_job["client_disconnected"] is True
        assert waited_job["artifacts"] == [str(output)]
        assert output.is_file()
        assert stdout == "survived client loss\n"
        assert "sampling estimates Python hotspots" in stderr
        assert f"recorded {output}" in stderr
        with RunpackReader(output) as reader:
            execution = reader.execution()
        capture_metadata = execution.metadata["capture"]
        assert isinstance(capture_metadata, dict)
        worker = capture_metadata["worker"]
        assert isinstance(worker, dict)
        assert worker == {
            "format_version": 1,
            "mode": "separate-process",
            "client_disconnected": True,
        }
        instrumentation = capture_metadata["instrumentation"]
        assert isinstance(instrumentation, dict)
        assert instrumentation["status"] == "complete"
        assert not tuple(tmp_path.glob(".client-loss.runpack.tmp-*"))
        assert not tuple(tmp_path.glob(".contrail-sample-profile-*"))
    finally:
        if client.poll() is None:
            client.kill()
            client.wait(timeout=5)


def test_second_client_can_cancel_and_reap_a_disconnected_capture_job(tmp_path: Path) -> None:
    output = tmp_path / "cancelled-job.runpack"
    workload_pid = tmp_path / "cancelled-workload.pid"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
    workload = (
        "from pathlib import Path\n"
        "import os\n"
        "import time\n"
        f"Path({str(workload_pid)!r}).write_text(str(os.getpid()), encoding='utf-8')\n"
        "time.sleep(30)\n"
    )
    client = subprocess.Popen(
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
            workload,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
    )
    process_id: int | None = None
    try:
        deadline = time.monotonic() + 10
        while not workload_pid.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert workload_pid.exists()
        process_id = int(workload_pid.read_text(encoding="utf-8"))

        os.kill(client.pid, signal.SIGKILL)
        assert client.wait(timeout=5) == -signal.SIGKILL
        deadline = time.monotonic() + 5
        job_id: str | None = None
        while time.monotonic() < deadline:
            listed = subprocess.run(
                (
                    sys.executable,
                    "-m",
                    "runtime_tools.cli",
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
                break
            time.sleep(0.01)
        assert job_id is not None

        cancelled = subprocess.run(
            (
                sys.executable,
                "-m",
                "runtime_tools.cli",
                "job",
                "cancel",
                job_id,
                "--format",
                "json",
            ),
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=10,
        )
        stdout, stderr = client.communicate(timeout=5)

        assert cancelled.returncode == 0
        cancelled_job = json.loads(cancelled.stdout)["job"]
        assert cancelled_job["state"] == "complete"
        assert cancelled_job["exit_status"] == 128 + signal.SIGINT
        assert cancelled_job["client_disconnected"] is True
        assert cancelled_job["artifacts"] == []
        assert stdout == ""
        assert stderr == ""
        with pytest.raises(ProcessLookupError):
            os.kill(process_id, 0)
        assert not output.exists()
        assert not tuple(tmp_path.glob(".cancelled-job.runpack.tmp-*"))
    finally:
        if client.poll() is None:
            client.kill()
            client.wait(timeout=5)
        if process_id is not None:
            try:
                os.kill(process_id, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_runtime_cli_interrupt_stops_capture_worker_and_workload(tmp_path: Path) -> None:
    output = tmp_path / "interrupted-worker.runpack"
    workload_pid = tmp_path / "workload.pid"
    workload = (
        "from pathlib import Path\n"
        "import os\n"
        "import time\n"
        f"Path({str(workload_pid)!r}).write_text(str(os.getpid()), encoding='utf-8')\n"
        "time.sleep(30)\n"
    )
    client = subprocess.Popen(
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
            workload,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    process_id: int | None = None
    try:
        deadline = time.monotonic() + 10
        while not workload_pid.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert workload_pid.exists()
        process_id = int(workload_pid.read_text(encoding="utf-8"))

        client.send_signal(signal.SIGINT)
        stdout, stderr = client.communicate(timeout=10)

        assert client.returncode == 128 + signal.SIGINT
        assert stdout == ""
        assert stderr == ""
        with pytest.raises(ProcessLookupError):
            os.kill(process_id, 0)
        assert not output.exists()
        assert not tuple(tmp_path.glob(".interrupted-worker.runpack.tmp-*"))
    finally:
        if client.poll() is None:
            client.kill()
            client.wait(timeout=5)
        if process_id is not None:
            try:
                os.kill(process_id, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_capture_job_wait_returns_the_recorded_command_status(tmp_path: Path) -> None:
    output = tmp_path / "failed-job.runpack"
    job_root = tmp_path / "capture-jobs"
    environment = {**os.environ, "_CONTRAIL_CAPTURE_JOB_ROOT": str(job_root)}
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
            "raise SystemExit(7)",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    listed = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
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
    job_id = json.loads(listed.stdout)["jobs"][0]["job_id"]
    waited = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
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
    )

    assert recorded.returncode == 7
    assert listed.returncode == 0
    assert waited.returncode == 7
    assert json.loads(waited.stdout)["job"]["exit_status"] == 7


def test_capture_job_status_rejects_an_invalid_identity_without_traceback(tmp_path: Path) -> None:
    environment = {
        **os.environ,
        "_CONTRAIL_CAPTURE_JOB_ROOT": str(tmp_path / "capture-jobs"),
    }

    status = subprocess.run(
        (
            sys.executable,
            "-m",
            "runtime_tools.cli",
            "job",
            "status",
            "not-a-job",
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert status.returncode == 2
    assert status.stdout == ""
    assert status.stderr == (
        "runtime: capture job ID must be 32 lowercase hexadecimal characters\n"
    )
    assert "Traceback" not in status.stderr


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
    (
        "enrich-kubernetes",
        "enrich-prometheus",
        "enrich-otel-logs",
        "enrich-temporal-history",
    ),
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
    (
        "enrich-kubernetes",
        "enrich-prometheus",
        "enrich-otel-logs",
        "enrich-temporal-history",
    ),
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
