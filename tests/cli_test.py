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
