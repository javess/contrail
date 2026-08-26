#!/usr/bin/env python3
"""Install a wheel and exercise one complete Contrail workflow."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import cast


def _run(
    command: tuple[str, ...],
    *,
    cwd: Path,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if completed.returncode:
        raise RuntimeError(
            f"release smoke command failed ({completed.returncode}): {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def _document(
    command: tuple[str, ...],
    *,
    cwd: Path,
    environment: dict[str, str],
    document_type: str,
) -> dict[str, object]:
    value: object = json.loads(_run(command, cwd=cwd, environment=environment).stdout)
    if not isinstance(value, dict):
        raise RuntimeError(f"command did not emit a JSON object: {' '.join(command)}")
    document = cast(dict[str, object], value)
    if document.get("document_type") != document_type or document.get("format_version") != "2":
        raise RuntimeError(f"command emitted an unexpected JSON protocol: {' '.join(command)}")
    return document


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
        "uv",
        "pip",
        "install",
        "--python",
        str(python),
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
        tool_environment = root / "tool"
        workload_environment = root / "workload"
        for environment in (tool_environment, workload_environment):
            _run(
                ("uv", "venv", "--python", sys.executable, str(environment)),
                cwd=root,
            )

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
        for removed_name in ("runtime", "rundiff", "batchscope", "proofline"):
            if _venv_executable(tool_environment, removed_name).exists():
                raise RuntimeError(f"wheel still installs removed executable: {removed_name}")

        environment = os.environ.copy()
        environment["_CONTRAIL_CAPTURE_JOB_ROOT"] = str(root / "capture-jobs")
        version = _run((str(contrail), "--version"), cwd=root, environment=environment)
        if not version.stdout.startswith("contrail "):
            raise RuntimeError("installed Contrail returned an unexpected version")

        isolated_import = _run(
            (
                str(workload_python),
                "-I",
                "-c",
                "import importlib.util; print(importlib.util.find_spec('runtime_tools'))",
            ),
            cwd=root,
        )
        if isolated_import.stdout.strip() != "None":
            raise RuntimeError("workload environment unexpectedly contains Contrail")

        runpack = root / "smoke.runpack"
        _run(
            (
                str(contrail),
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
            environment=environment,
        )
        if not runpack.is_file():
            raise RuntimeError("installed Contrail did not create a runpack")

        inspected = _document(
            (str(contrail), "inspect", str(runpack), "--format", "json"),
            cwd=root,
            environment=environment,
            document_type="runtime.inspect",
        )
        if inspected.get("name") != "wheel-smoke" or inspected.get("exit_code") != 0:
            raise RuntimeError("installed Contrail did not inspect its captured run")

        compared = _document(
            (
                str(contrail),
                "compare",
                str(runpack),
                str(runpack),
                "--format",
                "json",
            ),
            cwd=root,
            environment=environment,
            document_type="rundiff.compare",
        )
        if compared.get("outcome") != "equivalent":
            raise RuntimeError("installed Contrail did not compare an identical run as equivalent")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument(
        "--constraints", type=Path, help="pip constraints exported from the lockfile"
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
