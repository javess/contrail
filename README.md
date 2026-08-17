# Contrail Runtime Tools

Software version control records what code changed. Contrail records what
runtime behavior changed.

Contrail is a local-first suite built around one portable representation of a
software execution:

- **RunDiff** answers: what changed in runtime behavior?
- **BatchScope** answers: where did the wall-clock time go?
- **Proofline** answers: did this change violate a behavioral contract?

The project is being built execution-first. See
[the architecture](docs/architecture.md),
[the execution model](docs/execution-model.md), and
[the delivery milestones](docs/milestones.md).

## Local process capture

Python 3.12 and [uv](https://docs.astral.sh/uv/) are required for development.

```bash
uv sync --locked
uv run runtime record --name demo -- python demo.py
uv run runtime inspect demo.runpack
```

Capture relays stdout and stderr to the terminal but stores only their byte
counts and SHA-256 identities. Environment values and output content are not
included in the artifact by default.

Output content can be included explicitly as bounded binary attachments. This
may capture secrets, so it is opt-in; each stream stores at most the configured
prefix plus its original byte count and truncation state:

```bash
uv run runtime record --name demo --include-output \
  --output-limit-bytes 1048576 -- python demo.py
```

Runpacks are versioned SQLite databases, so their normalized evidence remains
inspectable without Contrail or a hosted service:

```bash
sqlite3 demo.runpack '.tables'
```

Use `runtime inspect demo.runpack --format json` for machine-readable output.

Run bounded, read-only SQL directly against the portable artifact:

```bash
uv run runtime query demo.runpack \
  'SELECT kind, name, started_at_ns FROM events ORDER BY started_at_ns'
```

See [querying runpacks](docs/querying.md) for schema and output details.

Bounded OpenTelemetry trace exports in OTLP/JSON can be normalized into the
same artifact and inspected as a causal tree:

```bash
uv run runtime import-otel trace.json --name checkout
uv run runtime inspect trace.runpack --tree
```

Pass `--include-raw` only when the original OTLP JSON should travel with the
runpack. Normal inspection and the local UI expose attachment counts, not
attachment content; content remains available through an explicit SQL query.

Compare any two runpacks with RunDiff. The text report emphasizes changed
operation counts and runtime dependencies; JSON keeps the same structured facts
for automation.

```bash
uv run rundiff compare baseline.runpack candidate.runpack
uv run rundiff compare baseline.runpack candidate.runpack --format json
```

Open a read-only local timeline, optionally with the structured comparison
facts above it:

```bash
uv run runtime serve baseline.runpack --compare candidate.runpack
```

Bounded infrastructure evidence can enrich a captured execution without
modifying the original artifact:

```bash
uv run runtime enrich-kubernetes run.runpack snapshot.json --output run-k8s.runpack
uv run runtime enrich-prometheus run-k8s.runpack metrics.json --output run-full.runpack
```

Proofline evaluates explicit behavioral contracts without an LLM:

```bash
uv run proofline verify contracts.yaml \
  --baseline baseline.runpack \
  --candidate candidate.runpack
```

The dogfood example includes a contract that intentionally catches its
regression:

```bash
uv run proofline verify examples/local/contracts.yaml \
  --baseline baseline.runpack --candidate candidate.runpack
```

See [Proofline contracts](docs/contracts.md) for the supported assertion types
and exit-code behavior.

Proofline can also execute one repository-relative workload at two Git refs in
temporary detached worktrees, preserve both runpacks, and evaluate immediately:

```bash
uv run proofline run examples/local/contracts.yaml \
  --baseline-ref main --candidate-ref HEAD \
  --workload examples/local/pipeline.py
```

See [Proofline execution](docs/experiments.md) for isolation guarantees and the
initial fixed-environment tradeoff.

Domain work that cannot be inferred from process or OTel evidence can use the
small annotation API. Outside `runtime record` these calls are harmless no-ops.

```python
from runtime_tools import runtime

with runtime.run("inference", total_work=10_000):
    with runtime.stage("transform"):
        runtime.event("db.write", kind="client.request")
    runtime.progress(completed=10_000, total=10_000)
```

BatchScope explains lifecycle, critical path, throughput, and deterministic
bottleneck evidence for one finite run:

```bash
uv run batchscope inspect run.runpack
```

## End-to-end demonstration

The local pipeline example deliberately adds write amplification and a metadata
dependency while preserving its result:

```bash
uv run rundiff record baseline -- python examples/local/pipeline.py
uv run rundiff record candidate -- python examples/local/pipeline.py --regression
uv run rundiff compare baseline candidate
uv run runtime serve baseline.runpack --compare candidate.runpack
```
