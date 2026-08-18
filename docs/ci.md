# Proofline in CI

Proofline makes a failed runtime gate a retained debugging record. CI evaluates
the same explicit contract used locally. `--explain` attaches the RunDiff and
the SHA-256 plus byte size of the exact descriptor-bound snapshots behind that
result, while an `always()` upload keeps the JSON report and both runpacks
available after the job fails.

Validate inputs without opening runpacks, invoking Git, or executing a workload:

```bash
proofline validate proofline.yaml --format json
proofline validate proofline.yaml --parameters parameters.yaml --format json
```

`proofline verify` gates existing baseline and candidate runpacks. `proofline
run` captures the same repository-relative workload at two Git commits and then
applies the contract. A CI contract should normally include both candidate
health and behavioral equivalence:

```yaml
name: pull-request-runtime
assertions:
  - type: candidate_exit_success
  - type: exit_code_equivalent
  - type: output_equivalent
```

For `verify`, `run`, and `search`, `--format json` writes one JSON document to
stdout. The top-level `document_type` identifies its schema and
`format_version` is currently `"1"`. Diagnostics go to stderr. Exit status 0
means every evaluated claim passed, 1 means a claim failed or was
unverifiable, and 2 means the input or execution was invalid.

`--explain` is available on `verify` and `run`. It is opt-in, so existing text
and JSON consumers see no change when it is omitted. With `--format json
--explain`, the Proofline document adds a nested `diff` containing the complete
versioned `rundiff.compare` document from the same baseline and candidate
snapshots. Direct verification reports also add `artifact_bindings`; experiment
reports carry those bindings at their outer level. Every claim carries its exact
evaluated fact plus a pointer and optional selector for related RunDiff detail.
Human-readable explanation prints the same evidence plus copy-paste commands
for BatchScope, causal-tree inspection, and the local comparison timeline.
`--report PATH` implies explanation and atomically publishes that same
artifact-bound JSON without replacing an existing file. It preserves the gate
exit status while avoiding shell redirection, which can truncate an earlier
report before validation begins and can leave a zero-byte file after exit 2.

## Pull-request gate

The workflow below assumes Contrail is part of the project's locked development
environment, so the same tested version is used locally and in CI.

Use immutable pull-request commit SHAs rather than branch names. Let Proofline
publish the report with `--report` so validation errors leave no artifact and
the command's exit status remains the step status. On failure, a separate step
reruns only verification against the captured runpacks and publishes the
bounded human-readable explanation to the job summary; its expected nonzero
status cannot mask or replace the gate's status. Upload the report and runpacks
from a separate `always()` step so failed and unverifiable claims retain their
evidence.

```yaml
name: runtime-contract

on:
  pull_request:

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
      - name: Validate contract
        run: uv run proofline validate proofline.yaml --format json
      - name: Evaluate runtime contract
        id: proofline_gate
        run: >-
          uv run proofline run proofline.yaml
          --baseline-ref "${{ github.event.pull_request.base.sha }}"
          --candidate-ref "${{ github.event.pull_request.head.sha }}"
          --workload path/to/workload.py
          --python .venv/bin/python
          --output-dir proofline-results
          --explain
          --format json
          --report proofline-report.json
      - name: Publish Proofline failure summary
        if: failure() && steps.proofline_gate.conclusion == 'failure'
        shell: bash
        run: |
          {
            echo '## Proofline runtime contract'
            echo
            if [[ -f proofline-results/baseline.runpack && -f proofline-results/candidate.runpack ]]; then
              {
                uv run proofline verify proofline.yaml \
                  --baseline proofline-results/baseline.runpack \
                  --candidate proofline-results/candidate.runpack \
                  --explain 2>&1 || true
              } | sed 's/^/    /'
              echo
              echo 'Download the `proofline-evidence` artifact for the versioned JSON report and both runpacks.'
            else
              echo 'Proofline did not complete both captures. See the gate log for its diagnostic.'
            fi
          } >> "$GITHUB_STEP_SUMMARY"
      - name: Upload Proofline evidence
        if: always()
        uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1
        with:
          name: proofline-evidence
          if-no-files-found: warn
          retention-days: 30
          path: |
            proofline-report.json
            proofline-results/*.runpack
```

When the gate fails, download `proofline-evidence`. The report identifies the
violated claim, retains the canonical assertion policy and exact evaluated
fact, and carries the complete diff; the retained runpacks support deeper
inspection without trying to reproduce the CI execution:

```bash
batchscope inspect proofline-results/candidate.runpack
runtime inspect proofline-results/candidate.runpack --tree
runtime serve proofline-results/baseline.runpack \
  --compare proofline-results/candidate.runpack \
  --proofline-report proofline-report.json
```

For a current explained report, the server first verifies the retained
runpacks' exact SHA-256 and byte-size bindings. It then recomputes RunDiff and
replays every embedded assertion, rejecting any mismatch in policy, verdict,
expected/observed description, evaluated fact, or selector. The UI labels this
an artifact-bound report. Binding-less version-1 reports remain readable with
downgraded labels: assertion-bearing reports get legacy semantic replay, while
assertion-less policy and results remain report-authored but
runtime-consistent. Every reported result, including passes, remains visible.

Artifact binding catches stale or substituted evidence, including changes not
visible in RunDiff. It is not authentication when an actor can rewrite both the
report and runpacks. The trusted CI artifact channel in this example supplies
retention provenance; use signed or attested bundles when stronger portable
provenance is required. Claim actions use bounded, backend-resolved candidate
event IDs; browser code never reinterprets operation, error, or dependency
semantics.

This example retains evidence for 30 days. Adjust `retention-days` or the
repository policy to match the period in which a failed pull request may need
investigation.

The example's `--python` selects one project interpreter for both refs; the path
is validated and normalized before either workload runs. Omit it to use
Proofline's own Python environment. This matters when Contrail is installed as
an isolated tool but the workload has project dependencies or imports the
annotation API. The workload must keep each isolated checkout clean and write
caches or generated output outside the checkout. See
[Proofline execution](experiments.md) for the full isolation and environment
contract.

## Release qualification

The repository workflow runs the complete test suite, including loopback socket
tests, on Linux and macOS with Python 3.12 and 3.14. Its release-qualification
job derives `SOURCE_DATE_EPOCH` from the commit timestamp, builds the wheel and
source distribution twice into separate directories, and requires byte-for-byte
equality before recording SHA-256 checksums.

Archive validation is opt-in locally and mandatory in the
release-qualification job:

```bash
CONTRAIL_RELEASE_WHEEL=/absolute/path/to/contrail_runtime_tools-0.9.0-py3-none-any.whl \
CONTRAIL_RELEASE_SDIST=/absolute/path/to/contrail_runtime_tools-0.9.0.tar.gz \
uv run pytest -q tests/package_metadata_test.py
```

The installed-wheel smoke creates a temporary virtual environment, installs the
wheel without access to the source checkout, and exercises the primary
`contrail` command plus all four compatibility entry points, their versioned
JSON protocols, the complete demo-to-gate-to-timeline journey, and the packaged
UI resources:

```bash
uv export --locked --no-dev --no-emit-project --no-hashes \
  --output-file release-constraints.txt
uv run python tools/release_smoke.py --constraints release-constraints.txt \
  dist/contrail_runtime_tools-0.9.0-py3-none-any.whl
```

For an offline smoke, put the wheel and all transitive dependency wheels in a
local directory and add `--offline --find-links /absolute/path/to/wheelhouse`.
Third-party Actions and the uv release are pinned in the workflow; dependency
versions remain constrained by `uv.lock`.

## Publication boundary

`release-artifacts` is the only job that builds distributions. It first runs
the complete quality matrix, validates the project/changelog identity, builds
twice, compares the bytes, validates both archives, and exercises the installed
wheel. It then uploads only the qualified `dist-a` wheel and source distribution
plus `SHA256SUMS`. The job exposes the validated distribution `name`, `version`,
and expected `tag` (`v<version>`) as outputs. Both publication jobs download that
run-scoped artifact and verify its checksums; neither job checks out source or
rebuilds it.

The identity boundary is also runnable locally:

```bash
uv run python tools/release_check.py --ref-type tag --ref-name v0.9.0
```

Every run requires the first release heading in `CHANGELOG.md` to equal the
project version. A tag run additionally requires the tag to equal that exact
version with a `v` prefix. A mismatch exits 2 before the build.

Publication is deliberately inert unless a tag has passed qualification *and*
the Actions variable `PYPI_PUBLISH_REPOSITORY` exactly equals the current
`owner/repository` identity from `github.repository`. This repository-bound
value prevents an unrelated organization-wide boolean from enabling the jobs.
Do not set it until the release identity and controls below are configured, in
this order:

1. Reconfirm the configured identities immediately before release: PyPI
   distribution `contrail-runtime-tools`, GitHub owner `javess`, repository
   `contrail`, and public URL `https://github.com/javess/contrail`. The project
   metadata and documentation must continue to match them.
2. Create a protected GitHub environment named `pypi`. Require a reviewer who
   is not the release initiator, prevent self-review, and restrict deployment
   to protected release tags.
3. In PyPI's Publishing settings, add a pending Trusted Publisher for project
   `contrail-runtime-tools`, owner `javess`, repository `contrail`, workflow
   `ci.yml`, and environment `pypi`. A pending publisher lets the first OIDC
   release create the project, but it does not reserve the name before that
   release. Do not create or add an API token secret.
4. Protect the `v*` release-tag namespace with a repository ruleset. Restrict
   tag creation, update, and deletion to release maintainers; never move a
   published tag.
5. Enable immutable GitHub Releases in the repository settings so a published
   release and its qualified assets cannot be silently replaced.
6. Only after a dry review of all preceding identities and controls, create the
   repository Actions variable `PYPI_PUBLISH_REPOSITORY` with the exact
   canonical `owner/repository` value shown by GitHub.

On an enabled exact-version tag, `publish-pypi` receives only `contents: read`
and `id-token: write`, enters the protected `pypi` environment, and uses PyPA's
pinned Trusted Publishing action to publish the qualified files with PEP 740
provenance attestations. `publish-github` can run only after that succeeds; it
receives only `contents: write` and creates the verified-tag GitHub Release from
the same wheel, source distribution, and checksum file. A per-ref non-cancelling
concurrency group prevents two attempts for one release tag from overlapping.

PyPI publication is effectively irreversible: a published filename/version
must not be replaced or reused, even after deletion. If a bad release reaches
PyPI, yank it and publish a corrected, newly versioned successor. Never move the
tag, rebuild assets, add `--skip-existing`, or try to overwrite the release.

For a partial failure, first inspect PyPI and the workflow's retained artifact:

- If PyPI accepted nothing, correct the environment or trusted-publisher
  configuration and rerun the failed jobs from the same workflow run.
- If PyPI contains the complete release but GitHub publication failed, rerun
  only the failed `publish-github` job so it reuses the original run artifact.
- If PyPI accepted only part of the release, or the remote state is uncertain,
  stop. Yank that version, increment the project version, add the matching
  changelog heading and new protected tag, and let the full qualification chain
  produce a successor.
