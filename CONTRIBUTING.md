# Contributing

Contrail changes should leave a failure easier to reproduce, explain, and
inspect. Start by running the same installed-style walkthrough used by the
release smoke test:

```bash
uv sync --locked
demo_root="$(mktemp -d)"
uv run contrail demo --output-dir "$demo_root/contrail-demo"
```

The demo is offline and exits 0 after proving its expected violations. It
prints commands for verifying the contract and opening the local evidence
timeline. Keep the temporary directory if you want to inspect its adaptable
`workload.py`, `baseline.runpack`, `candidate.runpack`, `contract.yaml`, and
`proofline-report.json`. The printed recapture commands turn those files into a
starting point for a new runtime contract.

## Report a bug with evidence

Use the bug report form for non-security defects. Include the smallest reliable
reproduction plus the Contrail version, Python version, operating system, and
installation method. Describe both the expected public outcome and the actual
outcome, including the command's exit status.

When a runtime or verification defect is safe to share, attach the baseline and
candidate runpacks, contract, and explained Proofline report. These artifacts
let another contributor inspect the exact inputs behind a claim instead of
trying to recreate a transient execution. They are helpful, not required.

Generate the report directly from the retained snapshots so its exit status is
still the contract gate status:

```bash
uv run contrail verify contract.yaml \
  --baseline baseline.runpack \
  --candidate candidate.runpack \
  --report proofline-report.json
```

The command exits 0 when every claim passes, 1 when a claim fails or cannot be
verified, and 2 when the input is invalid. A failed gate still atomically
publishes its complete artifact-bound JSON evidence before exiting 1. Invalid
input does not create a report, and an existing path is never overwritten.

Runpacks, reports, imported telemetry, command arguments, working-directory
paths, opt-in output attachments, and logs can contain sensitive data. Review
and redact evidence before sharing it publicly. Redaction can change the
evidence, so state what was removed and confirm that the redacted reproduction
still demonstrates the defect. Never publish credentials or exploit details.
Report suspected vulnerabilities through the private process in the
[security policy](SECURITY.md), not through a public issue.

## Make a change

Keep changes focused on a public outcome. Add or update tests that demonstrate
that outcome, and preserve unrelated compatibility behavior. Runtime behavior
changes should include evidence from a focused baseline/candidate reproduction
when that is practical. Documentation-only changes do not need runtime
artifacts.

Before opening a pull request, run the complete local checks:

```bash
uv run python tools/check_architecture.py
uv build
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
uv run python benchmarks/release.py --profile pr
```

The [Python development guide](docs/development.md) defines the module layers,
typing policy, package workflow, and testing expectations behind these checks.

The supported runpack boundary is documented in the
[runpack compatibility policy](docs/compatibility.md). Versioned JSON documents
and exit statuses are covered by the
[machine-readable output policy](docs/machine-output.md).
