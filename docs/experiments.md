# Proofline execution

`proofline run` turns a source change into a traceable runtime experiment. It
captures a repository-relative workload at two Git refs, evaluates the same
behavioral contract used in CI, and preserves both artifact snapshots for the
failure investigation. It never checks either ref out over the developer's
working tree.

```text
independently allocated detached worktree at baseline ref
  -> execute workload
  -> baseline.runpack

separately allocated detached worktree at candidate ref
  -> execute workload
  -> candidate.runpack

both runpacks -> deterministic contract verification
```

The runner rejects unsafe ref syntax and validates the contract before resolving
commits, creating outputs, or executing workloads. It uses uniquely named
temporary worktree storage and removes it even when capture or verification fails.
The parsed contract is held in memory for both workload runs, and for every
probe and replay in a counterexample search, so changing the source file during
execution cannot change the claims being evaluated.
Both refs are preflighted before the output directory is created. The baseline
and candidate are then executed sequentially in independently allocated,
unpredictable temporary roots, and the candidate root is not allocated until
the baseline worktree has been removed. This prevents a surviving baseline
descendant from reaching the candidate through a reused absolute pathname. The
workload argument remains the same repository-relative path for both arms, but
their absolute working directories and `PWD` values intentionally differ. A
workload that emits or otherwise depends on those absolute paths may therefore
produce a path-dependent comparison rather than equivalent output.
After each capture, Proofline checks the isolated Git worktree for tracked and
untracked changes before removing it or verifying the result. A workload that
changes either checkout makes the experiment fail instead of allowing mutated
source to support a comparison claim. Workloads must keep the checkout clean and
write generated files, caches, temporary data, and other outputs to paths outside
the isolated worktree.
The result directory is never reused or overwritten. Baseline and candidate
runpacks remain available for RunDiff, BatchScope, causal inspection, bounded
queries, and the local timeline.

## From a failed contract to its cause

Add `--report PATH` when the contract result should retain the complete RunDiff
and exact identities of the baseline and candidate snapshots it evaluated:

```bash
proofline run proofline.yaml \
  --baseline-ref main \
  --candidate-ref HEAD \
  --workload path/to/workload.py \
  --output-dir proofline-results \
  --report proofline-report.json
```

A violated or unverifiable claim still exits 1, but the text report now carries
the outcome, resource, operation, concurrency, timing, dependency, and causal
changes needed to start debugging. The runpacks remain the source evidence, so
the report prints these copy-paste next steps; they do not execute the workload
again:

```bash
batchscope inspect proofline-results/candidate.runpack
runtime inspect proofline-results/candidate.runpack --tree
runtime serve proofline-results/baseline.runpack \
  --compare proofline-results/candidate.runpack \
  --proofline-report proofline-report.json
```

`--report` implies explanation. Proofline reserves a private mode-0600 sibling
before execution, writes and fsyncs the complete versioned JSON, and publishes
it atomically without overwriting an existing path. Exit 0 and exit 1 publish;
invalid input exits 2 without leaving a report or partial file. Choose a new
report path for each evaluation. Omitting both `--report` and `--explain`
preserves the existing text and JSON output. With
`--format json --explain`, the Proofline result adds a nested
`diff` containing the versioned `rundiff.compare` document. If an earlier run
was captured without explanation, use the preserved snapshots directly:

```bash
proofline verify proofline.yaml \
  --baseline proofline-results/baseline.runpack \
  --candidate proofline-results/candidate.runpack \
  --report proofline-report.json
```

For an archived report, pass
`--proofline-report REPORT.json` instead of `--contract`. The UI verifies the
report's embedded diff and execution identities against the supplied runpacks,
then replays each embedded assertion and requires the complete claim result to
match before exposing its findings. The UI labels that result replay-verified.
Older assertion-less version-1 reports cannot reconstruct their original
policy; their selectors, limits, and statuses are explicitly shown as
report-authored but runtime-consistent, and pass results are not hidden.

By default the runner uses Proofline's current Python interpreter. Pass
`--python PATH` to execute the workload in a separate, already-provisioned
environment while keeping that environment constant across both refs:

```bash
proofline run contracts.yaml \
  --baseline-ref main \
  --candidate-ref HEAD \
  --workload benchmarks/retry.py \
  --python .venv/bin/python
```

Proofline validates the selected path as an existing, regular executable before
creating output or worktrees. It converts the path to an absolute lexical path
without dereferencing virtual-environment symlinks, and records that exact path
as the first command argument in both runpacks. This makes the workload
environment explicit and auditable while allowing the Proofline CLI itself to
remain installed independently. A single experiment intentionally cannot use
different interpreters for its baseline and candidate.

Workload arguments can be repeated:

```bash
proofline run contracts.yaml \
  --baseline-ref main \
  --candidate-ref HEAD \
  --workload benchmarks/retry.py \
  --workload-arg=--jobs \
  --workload-arg=100
```

## Counterexample search

`proofline search` uses bounded Hypothesis strategies and shrinking. The first
slice supports integer parameters with explicit bounds:

```yaml
parameters:
  retries:
    type: integer
    min: 0
    max: 10
  jobs:
    type: integer
    min: 1
    max: 100
```

Each generated value becomes `--parameter=value`. Non-violating case artifacts
are temporary; only the selected violating case is preserved after bounded
shrinking. Search is
deterministic, has no Hypothesis example database, and bounds distinct search
experiments with `--max-examples` (default 25, hard limit 1,000). Shrinking stays
within that experiment budget. One final preserved run then reproduces the
selected violation, so workload invocation count may be one greater than the
search budget. Parameter files contain at most 64 integer dimensions, and every
bound must fit a signed 64-bit integer.
When the budget prevents an uncached shrink candidate from being evaluated, the
result reports `shrink_budget_exhausted: true`; selected parameters are not a
claim of a global minimum.
Unknown parameter fields and ambiguous custom flags are rejected. The
final output directory is validated before the search begins and is never
reused. The final preserved run must reproduce an explicit failed claim or the
search reports an error. An unverifiable claim caused by missing evidence is
not shrunk or reported as a violation.

```bash
proofline search contracts.yaml \
  --parameters parameters.yaml \
  --baseline-ref main \
  --candidate-ref HEAD \
  --workload benchmarks/retry.py \
  --python .venv/bin/python
```

Search uses the same selected interpreter for every generated attempt and the
final preserved replay. The baseline and candidate runpacks therefore expose
the exact workload environment used to establish the counterexample.

Use `--format json` for a stable machine-readable result. The JSON contains the
configured search bound and a `counterexample` value that is either `null` or
the selected parameters, `shrink_budget_exhausted` status, preserved artifact
paths, exit codes, and complete verification report. As with text output, a
found counterexample exits with status 1, no counterexample exits with 0, and an
invalid search exits with 2.
