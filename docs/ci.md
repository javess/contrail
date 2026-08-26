# Proofline in CI

Proofline turns runtime behavior into a normal failing build step while keeping
the runpacks needed to explain it.

## The gate

```bash
uv run contrail verify proofline.yaml \
  --baseline baseline.runpack \
  --candidate candidate.runpack \
  --explain \
  --format json \
  --report proofline-report.json
```

The command exits `0` when every claim passes, `1` when a claim fails or cannot
be verified, and `2` for invalid input. JSON goes to stdout; diagnostics go to
stderr. `--report` publishes an artifact-bound report atomically without
changing the gate status.

For an end-to-end Git comparison, use immutable commit SHAs:

```bash
uv run contrail run proofline.yaml \
  --baseline-ref "$BASE_SHA" \
  --candidate-ref "$HEAD_SHA" \
  --workload path/to/workload.py \
  --python .venv/bin/python \
  --output-dir proofline-results \
  --explain \
  --format json \
  --report proofline-report.json
```

`--python` selects one project interpreter for both refs. Omit it to use
Proofline's environment. The workload must keep both isolated checkouts clean
and write generated output elsewhere.

## GitHub Actions example

This compact job validates the contract, evaluates both immutable revisions,
and uploads evidence even when the gate fails:

```yaml
name: runtime-contract

on: [pull_request]

permissions:
  contents: read

jobs:
  proofline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2
        with:
          fetch-depth: 0
      - uses: astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9 # v9.0.0
        with:
          python-version: "3.12"
          version: "0.12.3"
      - run: uv sync --locked
      - run: uv run contrail validate proofline.yaml --format json
      - name: Evaluate runtime contract
        run: >-
          uv run contrail run proofline.yaml
          --baseline-ref "${{ github.event.pull_request.base.sha }}"
          --candidate-ref "${{ github.event.pull_request.head.sha }}"
          --workload path/to/workload.py
          --python .venv/bin/python
          --output-dir proofline-results
          --explain
          --format json
          --report proofline-report.json
      - name: Upload evidence
        if: always()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: proofline-evidence
          retention-days: 30
          if-no-files-found: warn
          path: |
            proofline-report.json
            proofline-results/*.runpack
```

On failure, download the artifact and inspect it locally:

```bash
uv run contrail analyze proofline-results/candidate.runpack
uv run contrail inspect proofline-results/candidate.runpack --tree
uv run contrail compare proofline-results/baseline.runpack \
  proofline-results/candidate.runpack
```

The retained report records the size and SHA-256 of both runpacks. Hash binding
detects stale or substituted files when checked; it is not authentication if
an actor can rewrite the entire bundle. Use a trusted artifact channel,
signing, or attestation for stronger provenance.

## Repository quality gate

Run these from a locked checkout:

```bash
uv run python tools/check_architecture.py
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
uv run python benchmarks/release.py --profile pr
```

Contrail's release matrix runs the source/static suite on Linux and macOS with
CPython 3.12 and 3.14. See [support](support.md) for the exact boundary.

## Build and installed-wheel qualification

```bash
uv build
uv export --locked --no-dev --no-emit-project --no-hashes \
  --output-file release-constraints.txt
uv run python tools/release_smoke.py \
  --constraints release-constraints.txt \
  dist/contrail_runtime_tools-0.10.0-py3-none-any.whl
```

The smoke test runs outside the checkout. It installs only the `contrail`
executable, captures against a separate workload environment, inspects the
result, and compares the runpack with itself through format-2 JSON.

For archive-path and metadata validation:

```bash
CONTRAIL_RELEASE_WHEEL=/absolute/path/to/contrail_runtime_tools-0.10.0-py3-none-any.whl \
CONTRAIL_RELEASE_SDIST=/absolute/path/to/contrail_runtime_tools-0.10.0.tar.gz \
uv run pytest -q tests/package_metadata_test.py
```

For offline wheel smoke, add `--offline --find-links /absolute/path/to/wheelhouse`.

## Release publication boundary

The release workflow builds wheel and source distribution twice with a stable
`SOURCE_DATE_EPOCH`, requires byte-for-byte equality, validates both archives,
runs installed-wheel smoke, and publishes only the qualified files plus
`SHA256SUMS`.

Before tagging, check identity:

```bash
uv run python tools/release_check.py --ref-type tag --ref-name v0.10.0
```

The first changelog release heading, project version, and `v<version>` tag must
match. Publication is enabled only when the repository variable
`PYPI_PUBLISH_REPOSITORY` exactly matches `owner/repository`, the protected
`pypi` environment approves deployment, and PyPI Trusted Publishing is
configured for this workflow. No API token is required.

Protect release tags, prevent self-review, enable immutable GitHub Releases,
and never move or reuse a published version. If PyPI accepted a broken release,
yank it and publish a newly versioned successor; do not overwrite files or use
`--skip-existing`.

See [machine-readable output](machine-output.md) for JSON contracts and
[Proofline execution](experiments.md) for checkout/environment isolation.
