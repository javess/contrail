# Proofline execution

`proofline run` compares a repository-relative workload at two Git refs without
checking either ref out over the developer's working tree.

```text
temporary detached worktree at baseline ref
  -> execute workload
  -> baseline.runpack

temporary detached worktree at candidate ref
  -> execute workload
  -> candidate.runpack

both runpacks -> deterministic contract verification
```

The runner resolves refs before creating outputs, uses uniquely named temporary
worktrees, and removes those worktrees even when capture or verification fails.
The result directory is never reused or overwritten. Baseline and candidate
runpacks remain available for `runtime inspect`, RunDiff, BatchScope, or replay.

The initial runner deliberately uses Proofline's current Python interpreter and
dependency environment for both refs. This isolates source changes while
holding the execution environment constant. Workloads that require different
dependencies at each ref should supply a stable wrapper workload until a
declarative environment adapter is added.

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
are temporary; only the minimized violating case is preserved. Search is
deterministic, has no Hypothesis example database, and is bounded by
`--max-examples` (default 25, hard limit 1,000). The final output directory is
validated before the search begins and is never reused.

```bash
proofline search contracts.yaml \
  --parameters parameters.yaml \
  --baseline-ref main \
  --candidate-ref HEAD \
  --workload benchmarks/retry.py
```
