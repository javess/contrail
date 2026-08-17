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
