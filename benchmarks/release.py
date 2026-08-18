#!/usr/bin/env python3
"""Run deterministic, dependency-free release performance gates."""

from __future__ import annotations

import argparse
import json
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import cast

from generate import (
    write_causal_chain,
    write_finished_runpack,
    write_kubernetes_snapshot,
    write_otlp_trace,
    write_prometheus_response,
)

from runtime_tools.batchscope import analyze_runpack
from runtime_tools.kubernetes import import_kubernetes_snapshot
from runtime_tools.otel import import_otlp_json
from runtime_tools.prometheus import import_prometheus_response
from runtime_tools.proofline.verify import verify_contracts_with_artifact_bindings
from runtime_tools.storage import RunpackReader
from runtime_tools.ui import build_timeline_payload


def _peak_rss_mib() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / (1024 * 1024) if sys.platform == "darwin" else value / 1024


def _worker(case: str, count: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="contrail-release-benchmark-") as directory:
        root = Path(directory)
        output: Path | None = None
        if case == "otel_import":
            source = root / "trace.json"
            output = root / "trace.runpack"
            write_otlp_trace(source, count)
            start = time.perf_counter()
            observed = import_otlp_json(source, output, name="benchmark").event_count
        elif case == "prometheus_import":
            runpack = root / "base.runpack"
            source = root / "prometheus.json"
            output = root / "prometheus.runpack"
            write_finished_runpack(runpack, finished_at_ns=(count + 1) * 1_000_000_000)
            write_prometheus_response(source, count)
            start = time.perf_counter()
            observed = import_prometheus_response(runpack, source, output).sample_count
        elif case == "kubernetes_import":
            runpack = root / "base.runpack"
            source = root / "kubernetes.json"
            output = root / "kubernetes.runpack"
            write_finished_runpack(runpack, finished_at_ns=1)
            write_kubernetes_snapshot(source, count)
            start = time.perf_counter()
            observed = import_kubernetes_snapshot(runpack, source, output).event_count
        elif case == "batchscope_analyze":
            runpack = root / "chain.runpack"
            write_causal_chain(runpack, count)
            start = time.perf_counter()
            analysis = analyze_runpack(runpack)
            observed = len(analysis.critical_path.event_ids) if analysis.critical_path else 0
        elif case == "ui_payload":
            runpack = root / "timeline.runpack"
            write_causal_chain(runpack, count)
            start = time.perf_counter()
            payload = build_timeline_payload(runpack)
            runs = payload["runs"]
            assert isinstance(runs, list) and isinstance(runs[0], dict)
            events = runs[0]["events"]
            assert isinstance(events, list)
            observed = len(events)
        elif case == "retained_report":
            baseline = root / "baseline.runpack"
            candidate = root / "candidate.runpack"
            contract = root / "contract.yaml"
            report_path = root / "proofline-report.json"
            write_causal_chain(baseline, count)
            write_causal_chain(candidate, count)
            contract.write_text(
                "name: retained-report\nassertions:\n  - type: output_equivalent\n",
                encoding="utf-8",
            )
            start = time.perf_counter()
            report, diff, bindings = verify_contracts_with_artifact_bindings(
                contract,
                baseline,
                candidate,
            )
            document = report.as_json_value(
                include_evidence=True,
                artifact_bindings=bindings,
            )
            document["diff"] = diff.as_json_value()
            report_path.write_text(
                json.dumps(document, allow_nan=False, sort_keys=True),
                encoding="utf-8",
            )
            payload = build_timeline_payload(
                baseline,
                candidate,
                proofline_report=report_path,
            )
            runs = payload["runs"]
            assert isinstance(runs, list) and isinstance(runs[1], dict)
            events = runs[1]["events"]
            assert isinstance(events, list)
            observed = len(events)
        else:
            raise ValueError(f"unknown benchmark case: {case}")
        elapsed = time.perf_counter() - start
        if observed != count:
            raise RuntimeError(f"{case} produced {observed} records; expected {count}")
        if case.endswith("import"):
            assert output is not None
            with RunpackReader(output) as reader:
                reader.execution()
        return {
            "case": case,
            "count": count,
            "seconds": round(elapsed, 6),
            "peak_rss_mib": round(_peak_rss_mib(), 3),
        }


def _load_profile(path: Path, profile: str) -> dict[str, dict[str, float | int]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    profiles = document.get("profiles")
    if not isinstance(profiles, dict) or profile not in profiles:
        raise ValueError(f"benchmark profile is not defined: {profile}")
    selected = profiles[profile]
    if not isinstance(selected, dict):
        raise ValueError(f"benchmark profile must be an object: {profile}")
    return cast(dict[str, dict[str, float | int]], selected)


def _run_case(
    case: str,
    budget: dict[str, float | int],
) -> tuple[dict[str, object], tuple[str, ...]]:
    count = int(budget["count"])
    maximum_seconds = float(budget["max_seconds"])
    timeout = max(30.0, maximum_seconds * 4)
    completed = subprocess.run(
        (sys.executable, str(Path(__file__).resolve()), "--worker", case, str(count)),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"{case} worker failed:\n{completed.stderr or completed.stdout}")
    result = json.loads(completed.stdout)
    if not isinstance(result, dict):
        raise RuntimeError(f"{case} worker did not emit a JSON object")
    failures = []
    if float(result["seconds"]) > maximum_seconds:
        failures.append(f"time {result['seconds']}s > {maximum_seconds}s")
    maximum_rss = float(budget["max_rss_mib"])
    if float(result["peak_rss_mib"]) > maximum_rss:
        failures.append(f"RSS {result['peak_rss_mib']} MiB > {maximum_rss} MiB")
    result["max_seconds"] = maximum_seconds
    result["max_rss_mib"] = maximum_rss
    return cast(dict[str, object], result), tuple(failures)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="pr", choices=("pr", "release"))
    parser.add_argument("--case", action="append", dest="cases")
    parser.add_argument("--budgets", type=Path, default=Path(__file__).with_name("budgets.json"))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--worker", nargs=2, metavar=("CASE", "COUNT"), help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.worker:
        print(json.dumps(_worker(args.worker[0], int(args.worker[1])), sort_keys=True))
        return 0

    profile = _load_profile(args.budgets, args.profile)
    selected = args.cases or list(profile)
    unknown = sorted(set(selected) - set(profile))
    if unknown:
        parser.error(f"cases not in {args.profile} profile: {', '.join(unknown)}")
    results = []
    failed = False
    for case in selected:
        result, failures = _run_case(case, profile[case])
        result["passed"] = not failures
        result["failures"] = list(failures)
        results.append(result)
        failed = failed or bool(failures)
        if not args.json:
            status = "PASS" if not failures else "FAIL"
            print(
                f"{status} {case}: {result['count']} records, {result['seconds']}s, "
                f"{result['peak_rss_mib']} MiB RSS"
            )
            for failure in failures:
                print(f"  {failure}")
    if args.json:
        print(json.dumps({"profile": args.profile, "results": results}, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
