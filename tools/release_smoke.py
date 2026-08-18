#!/usr/bin/env python3
"""Install a wheel into isolated tool/workload environments and exercise every CLI."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import venv
from pathlib import Path
from typing import cast


def _run(command: tuple[str, ...], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, cwd=cwd, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        rendered = " ".join(command)
        raise RuntimeError(
            f"release smoke command failed ({completed.returncode}): {rendered}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def _json(command: tuple[str, ...], *, cwd: Path, document_type: str) -> dict[str, object]:
    value = json.loads(_run(command, cwd=cwd).stdout)
    if not isinstance(value, dict):
        raise RuntimeError(f"command did not emit a JSON object: {' '.join(command)}")
    if value.get("document_type") != document_type or value.get("format_version") != "1":
        raise RuntimeError(f"command emitted an unexpected JSON protocol: {' '.join(command)}")
    return cast(dict[str, object], value)


def _assert_artifact_bindings(
    document: dict[str, object],
    baseline: Path,
    candidate: Path,
) -> None:
    expected = {
        "baseline": {
            "size_bytes": baseline.stat().st_size,
            "sha256": hashlib.sha256(baseline.read_bytes()).hexdigest(),
        },
        "candidate": {
            "size_bytes": candidate.stat().st_size,
            "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
        },
    }
    if document.get("artifact_bindings") != expected:
        raise RuntimeError("installed Proofline report was not bound to its exact runpacks")


def _serve_payload(
    contrail: Path,
    baseline: Path,
    candidate: Path,
    report: Path,
    *,
    cwd: Path,
) -> dict[str, object]:
    process = subprocess.Popen(
        (
            str(contrail),
            "serve",
            str(baseline),
            "--compare",
            str(candidate),
            "--proofline-report",
            str(report),
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--no-open",
        ),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stderr, selectors.EVENT_READ)
    deadline = time.monotonic() + 10
    url: str | None = None
    try:
        while time.monotonic() < deadline and url is None:
            if process.poll() is not None:
                stderr = process.stderr.read()
                raise RuntimeError(
                    f"installed timeline server exited before startup ({process.returncode}): "
                    f"{stderr}"
                )
            for _ in selector.select(timeout=0.1):
                line = process.stderr.readline().strip()
                if line.startswith("runtime UI: http://"):
                    url = line.removeprefix("runtime UI: ")
                    break
        if url is None:
            raise RuntimeError("installed timeline server did not report its bound URL")
        with urllib.request.urlopen(f"{url}/api/data", timeout=5) as response:
            payload = json.loads(response.read())
        if not isinstance(payload, dict):
            raise RuntimeError("installed timeline server did not return a JSON object")
        return cast(dict[str, object], payload)
    finally:
        selector.close()
        if process.poll() is None:
            process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)


def _venv_executable(root: Path, name: str) -> Path:
    directory = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    return root / directory / f"{name}{suffix}"


def _install_wheel(
    python: Path,
    wheel: Path,
    *,
    constraints: Path | None,
    find_links: tuple[Path, ...],
    offline: bool,
    cwd: Path,
) -> None:
    command = [
        str(python),
        "-m",
        "pip",
        "--isolated",
        "install",
        "--disable-pip-version-check",
    ]
    if offline:
        command.append("--no-index")
    if constraints is not None:
        command.extend(("--constraint", str(constraints.resolve(strict=True))))
    for link in find_links:
        command.extend(("--find-links", str(link.resolve(strict=True))))
    command.append(str(wheel))
    _run(tuple(command), cwd=cwd)


def smoke(
    wheel: Path,
    *,
    constraints: Path | None,
    find_links: tuple[Path, ...],
    offline: bool,
) -> None:
    wheel = wheel.resolve(strict=True)
    if wheel.suffix != ".whl":
        raise ValueError(f"release smoke requires a wheel: {wheel}")
    with tempfile.TemporaryDirectory(prefix="contrail-wheel-smoke-") as directory:
        root = Path(directory)
        tool_environment = root / "tool-venv"
        workload_environment = root / "workload-venv"
        builder = venv.EnvBuilder(with_pip=True, clear=True)
        builder.create(tool_environment)
        builder.create(workload_environment)
        tool_python = _venv_executable(tool_environment, "python")
        workload_python = _venv_executable(workload_environment, "python")
        _install_wheel(
            tool_python,
            wheel,
            constraints=constraints,
            find_links=find_links,
            offline=offline,
            cwd=root,
        )

        contrail = _venv_executable(tool_environment, "contrail")
        runtime = _venv_executable(tool_environment, "runtime")
        rundiff = _venv_executable(tool_environment, "rundiff")
        batchscope = _venv_executable(tool_environment, "batchscope")
        proofline = _venv_executable(tool_environment, "proofline")
        for command in (contrail, runtime, rundiff, batchscope, proofline):
            version = _run((str(command), "--version"), cwd=root).stdout.strip()
            if not version.startswith(f"{command.stem} "):
                raise RuntimeError(f"unexpected version output from {command.name}: {version!r}")

        runpack = root / "smoke.runpack"
        _run(
            (
                str(runtime),
                "record",
                "--name",
                "wheel-smoke",
                "--output",
                str(runpack),
                "--",
                str(workload_python),
                "-I",
                "-c",
                "print('wheel-smoke')",
            ),
            cwd=root,
        )
        inspected = _json(
            (str(runtime), "inspect", str(runpack), "--format", "json"),
            cwd=root,
            document_type="runtime.inspect",
        )
        if inspected.get("name") != "wheel-smoke":
            raise RuntimeError("installed runtime did not inspect the captured execution")

        _install_wheel(
            workload_python,
            wheel,
            constraints=constraints,
            find_links=find_links,
            offline=offline,
            cwd=root,
        )
        annotated_runpack = root / "annotated.runpack"
        _run(
            (
                str(contrail),
                "record",
                "--name",
                "annotated-wheel-smoke",
                "--output",
                str(annotated_runpack),
                "--",
                str(workload_python),
                "-I",
                "-c",
                "from runtime_tools import runtime; "
                "runtime.event('db.write', kind='client.request')",
            ),
            cwd=root,
        )
        annotated_events = _json(
            (
                str(contrail),
                "query",
                str(annotated_runpack),
                "SELECT kind, name FROM events WHERE kind = 'client.request' AND name = 'db.write'",
                "--format",
                "json",
            ),
            cwd=root,
            document_type="runtime.query",
        )
        if annotated_events.get("columns") != ["kind", "name"] or annotated_events.get("rows") != [
            ["client.request", "db.write"]
        ]:
            raise RuntimeError("installed workload package did not emit its domain annotation")
        _run(
            (
                str(tool_python),
                "-I",
                "-c",
                "import sys\n"
                "from runtime_tools import RunpackError, open_runpack\n"
                "reader = open_runpack(sys.argv[1])\n"
                "with reader as runpack:\n"
                "    events = [(event.kind, event.name) for event in runpack.events() "
                "if event.kind == 'client.request' and event.name == 'db.write']\n"
                "assert events == [('client.request', 'db.write')], events\n"
                "try:\n"
                "    reader.events()\n"
                "except RunpackError:\n"
                "    pass\n"
                "else:\n"
                "    raise AssertionError('closed runpack remained readable')\n",
                str(annotated_runpack),
            ),
            cwd=root,
        )
        _json(
            (str(rundiff), "compare", str(runpack), str(runpack), "--format", "json"),
            cwd=root,
            document_type="rundiff.compare",
        )
        _json(
            (str(batchscope), "inspect", str(runpack), "--format", "json"),
            cwd=root,
            document_type="batchscope.inspect",
        )

        contract = root / "contract.yaml"
        contract.write_text(
            "name: wheel-smoke\nassertions:\n  - type: output_equivalent\n",
            encoding="utf-8",
        )
        verification_command = (
            str(proofline),
            "verify",
            str(contract),
            "--baseline",
            str(runpack),
            "--candidate",
            str(runpack),
            "--format",
            "json",
        )
        _json(
            verification_command,
            cwd=root,
            document_type="proofline.verification",
        )
        explanation = _json(
            (
                *verification_command,
                "--explain",
            ),
            cwd=root,
            document_type="proofline.verification",
        )
        diff = explanation.get("diff")
        if not isinstance(diff, dict):
            raise RuntimeError("installed Proofline explanation did not contain a runtime diff")
        if diff.get("document_type") != "rundiff.compare" or diff.get("format_version") != "1":
            raise RuntimeError("installed Proofline explanation contained an invalid runtime diff")
        _assert_artifact_bindings(explanation, runpack, runpack)
        results = explanation.get("results")
        if not isinstance(results, list) or not results:
            raise RuntimeError("installed Proofline explanation did not contain claim results")
        evidence = results[0].get("evidence") if isinstance(results[0], dict) else None
        if evidence != [
            {
                "fact": {"equivalent": True},
                "diff_path": "/output_equivalent",
                "selector": {},
            }
        ]:
            raise RuntimeError("installed Proofline claim did not reference its runtime diff fact")

        workload_site_packages = Path(
            _run(
                (
                    str(workload_python),
                    "-I",
                    "-c",
                    "import sysconfig; print(sysconfig.get_path('purelib'))",
                ),
                cwd=root,
            ).stdout.strip()
        )
        workload_dependency = "contrail_release_smoke_workload_dependency"
        (workload_site_packages / f"{workload_dependency}.py").write_text(
            "VALUE = 'workload environment selected'\n",
            encoding="utf-8",
        )
        tool_dependency_probe = subprocess.run(
            (str(tool_python), "-I", "-c", f"import {workload_dependency}"),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if tool_dependency_probe.returncode == 0:
            raise RuntimeError("workload-only smoke dependency leaked into the tool environment")
        experiment_repo = root / "proofline-repo"
        experiment_repo.mkdir()
        _run(("git", "init", "-b", "main"), cwd=experiment_repo)
        _run(("git", "config", "user.name", "Contrail release smoke"), cwd=experiment_repo)
        _run(
            ("git", "config", "user.email", "release-smoke@example.invalid"),
            cwd=experiment_repo,
        )
        (experiment_repo / "workload.py").write_text(
            f"import {workload_dependency}\nprint({workload_dependency}.VALUE)\n",
            encoding="utf-8",
        )
        _run(("git", "add", "workload.py"), cwd=experiment_repo)
        _run(("git", "commit", "-m", "workload"), cwd=experiment_repo)
        experiment_contract = root / "experiment-contract.yaml"
        experiment_contract.write_text(
            "name: workload-python\nassertions:\n  - type: candidate_exit_success\n",
            encoding="utf-8",
        )
        experiment_output = root / "proofline-results"
        experiment = _json(
            (
                str(contrail),
                "run",
                str(experiment_contract),
                "--baseline-ref",
                "main",
                "--candidate-ref",
                "main",
                "--workload",
                "workload.py",
                "--python",
                str(workload_python),
                "--output-dir",
                str(experiment_output),
                "--format",
                "json",
                "--explain",
            ),
            cwd=experiment_repo,
            document_type="proofline.experiment",
        )
        if experiment.get("baseline_exit_code") != 0 or experiment.get("candidate_exit_code") != 0:
            raise RuntimeError("installed Proofline did not use the selected workload Python")
        _assert_artifact_bindings(
            experiment,
            experiment_output / "baseline.runpack",
            experiment_output / "candidate.runpack",
        )
        selected_python = str(Path(os.path.abspath(workload_python)))
        for runpack_name in ("baseline.runpack", "candidate.runpack"):
            inspected_experiment = _json(
                (
                    str(contrail),
                    "inspect",
                    str(experiment_output / runpack_name),
                    "--format",
                    "json",
                ),
                cwd=root,
                document_type="runtime.inspect",
            )
            recorded_command = inspected_experiment.get("command")
            if not isinstance(recorded_command, list) or recorded_command[:1] != [selected_python]:
                raise RuntimeError("Proofline runpack did not record the selected workload Python")

        demo = root / "contrail-demo"
        demo_result = _run(
            (str(contrail), "demo", "--output-dir", str(demo)),
            cwd=root,
        )
        if not demo_result.stdout.startswith("CONTRAIL DEMO READY\n"):
            raise RuntimeError("installed Contrail demo did not report a ready walkthrough")
        demo_baseline = demo / "baseline.runpack"
        demo_candidate = demo / "candidate.runpack"
        demo_contract = demo / "contract.yaml"
        demo_workload = demo / "workload.py"
        if not demo_workload.is_file() or '"peer.service": "metadata-db"' not in (
            demo_workload.read_text(encoding="utf-8")
        ):
            raise RuntimeError("installed demo did not retain its adaptable workload")
        recaptured_baseline = demo / "recaptured-baseline.runpack"
        recaptured_candidate = demo / "recaptured-candidate.runpack"
        recaptured_report = demo / "recaptured-report.json"
        for name, variant, output in (
            ("demo-baseline", "baseline", recaptured_baseline),
            ("demo-candidate", "candidate", recaptured_candidate),
        ):
            _run(
                (
                    str(contrail),
                    "record",
                    "--name",
                    name,
                    "--output",
                    str(output),
                    "--",
                    str(tool_python),
                    str(demo_workload),
                    variant,
                ),
                cwd=root,
            )
        recaptured_gate = subprocess.run(
            (
                str(contrail),
                "verify",
                str(demo_contract),
                "--baseline",
                str(recaptured_baseline),
                "--candidate",
                str(recaptured_candidate),
                "--report",
                str(recaptured_report),
            ),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if recaptured_gate.returncode != 1:
            raise RuntimeError("installed demo template did not reproduce its failed gate")
        recaptured_document = json.loads(recaptured_report.read_text(encoding="utf-8"))
        _assert_artifact_bindings(
            recaptured_document,
            recaptured_baseline,
            recaptured_candidate,
        )
        report_path = root / "proofline-report.json"
        _json(
            (str(contrail), "inspect", str(demo_candidate), "--format", "json"),
            cwd=root,
            document_type="runtime.inspect",
        )
        _json(
            (
                str(contrail),
                "compare",
                str(demo_baseline),
                str(demo_candidate),
                "--format",
                "json",
            ),
            cwd=root,
            document_type="rundiff.compare",
        )
        _json(
            (str(contrail), "analyze", str(demo_candidate), "--format", "json"),
            cwd=root,
            document_type="batchscope.inspect",
        )
        explained_failure = subprocess.run(
            (
                str(contrail),
                "verify",
                str(demo_contract),
                "--baseline",
                str(demo_baseline),
                "--candidate",
                str(demo_candidate),
                "--report",
                str(report_path),
            ),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if explained_failure.returncode != 1:
            raise RuntimeError(
                "installed Proofline regression did not preserve its failed-gate status"
            )
        failed_report = json.loads(report_path.read_text(encoding="utf-8"))
        _assert_artifact_bindings(failed_report, demo_baseline, demo_candidate)
        failed_types = {
            result["type"]
            for result in failed_report.get("results", [])
            if isinstance(result, dict) and result.get("status") == "fail"
        }
        if failed_types != {"forbid_new_dependency", "max_operation_count"}:
            raise RuntimeError("installed Contrail demo did not preserve its expected violations")
        payload = _serve_payload(
            contrail,
            demo_baseline,
            demo_candidate,
            report_path,
            cwd=root,
        )
        proofline_payload = payload.get("proofline")
        if not isinstance(proofline_payload, dict) or proofline_payload.get("source") != "report":
            raise RuntimeError("installed timeline did not replay the retained CLI report")
        findings = proofline_payload.get("findings")
        selections = proofline_payload.get("selections")
        if not isinstance(findings, list) or not isinstance(selections, dict):
            raise RuntimeError("installed timeline did not expose Proofline evidence")
        if {
            finding.get("report_assurance") for finding in findings if isinstance(finding, dict)
        } != {"artifact_bound_policy_replayed"}:
            raise RuntimeError("installed timeline did not verify exact artifact bindings")
        by_type = {finding["type"]: finding for finding in findings if isinstance(finding, dict)}
        dependency = selections[by_type["forbid_new_dependency"]["selection_id"]]
        operations = selections[by_type["max_operation_count"]["selection_id"]]
        if not isinstance(dependency, dict) or not isinstance(operations, dict):
            raise RuntimeError("installed timeline selections were malformed")
        if len(dependency["candidate_event_ids"]) != 1 or dependency["truncated"]:
            raise RuntimeError("installed dependency selection was incomplete")
        if len(operations["candidate_event_ids"]) != 30 or operations["truncated"]:
            raise RuntimeError("installed operation selection was incomplete")

        substituted_candidate = root / "substituted-candidate.runpack"
        shutil.copyfile(demo_candidate, substituted_candidate)
        with sqlite3.connect(substituted_candidate) as connection:
            connection.execute(
                "UPDATE executions SET working_directory = ?",
                ("/substituted/evidence",),
            )
        rejected = subprocess.run(
            (
                str(contrail),
                "serve",
                str(demo_baseline),
                "--compare",
                str(substituted_candidate),
                "--proofline-report",
                str(report_path),
                "--no-open",
                "--port",
                "0",
            ),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if rejected.returncode != 2 or "artifact binding" not in rejected.stderr:
            raise RuntimeError("installed timeline accepted substituted runpack evidence")
        _run(
            (
                str(tool_python),
                "-c",
                "from importlib.resources import files; "
                "p=files('runtime_tools.ui').joinpath('static'); "
                "assert all(p.joinpath(n).is_file() for n in "
                "('index.html','app.js','styles.css'))",
            ),
            cwd=root,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument(
        "--constraints",
        type=Path,
        help="pip constraints exported from the lockfile",
    )
    parser.add_argument(
        "--find-links",
        action="append",
        type=Path,
        default=[],
        help="directory containing dependency wheels; repeat as needed",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="disable package indexes; dependencies must be available via --find-links",
    )
    args = parser.parse_args(argv)
    smoke(
        args.wheel,
        constraints=args.constraints,
        find_links=tuple(args.find_links),
        offline=args.offline,
    )
    print(f"installed-wheel smoke passed: {args.wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
