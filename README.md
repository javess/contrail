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
included in the artifact by default. A small documented allowlist of
behavior-relevant environment variables is represented only by value hashes so
environment drift can be detected without storing the values.

Capture ends when the recorded process exits. If a detached descendant keeps an
inherited stdout or stderr pipe open, Contrail drains buffered output without
waiting for that descendant and records `pipe_open_after_exit` in the stream
metadata so the output identity is not overstated.

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

OpenTelemetry trace exports in OTLP/JSON, bounded to 64 MiB and 1,000,000 spans
per input, can be normalized into the same artifact and inspected as a causal
tree. Span links are independently limited to 1,000,000:

```bash
uv run runtime import-otel trace.json --name checkout
uv run runtime inspect trace.runpack --tree
```

Enrich that execution with OTLP/JSON log records without modifying the trace
artifact. Log inputs have the same 64 MiB byte limit and a 1,000,000-record
limit:

```bash
uv run runtime enrich-otel-logs trace.runpack logs.json \
  --output trace-logs.runpack
```

Known log timestamps outside the execution window are dropped. Service resource
attributes identify log owners, and trace/span IDs create causal `emits` edges
only when they match one known span. Timestamp-less records remain explicit
rather than receiving fabricated times. Unresolved log-to-span references are
retained as causal-completeness metadata for RunDiff and Proofline.

Pass `--include-raw` to either OTLP command only when the original JSON should
travel with the runpack. Normal inspection and the local UI expose attachment
counts, not attachment content; content remains available through an explicit
SQL query.

Compare any two runpacks with RunDiff. The text report emphasizes changed
entity counts, operation and explicit failure counts, observed max concurrency,
aggregate duration, CPU time, peak memory, and runtime dependencies; JSON keeps
the same structured facts for automation. Comparisons of artifacts carrying the
same execution ID are marked as exact identity matches; distinct executions use
structural matching when their entity, operation, and dependency key sets
align and their internal causal edges connect the same semantic operations,
then fall back to aggregate semantic matching when the observed shape changes.

```bash
uv run rundiff compare baseline.runpack candidate.runpack
uv run rundiff compare baseline.runpack candidate.runpack --format json
```

Open a read-only local timeline, optionally with the structured comparison
facts above it:

```bash
uv run runtime serve baseline.runpack --compare candidate.runpack
```

Infrastructure JSON evidence can enrich a captured execution without modifying
the original artifact. Inputs are limited to 64 MiB; Kubernetes snapshots also
allow at most 200,000 API items and 1,000,000 containers, while Prometheus
responses allow at most 1,000,000 samples:

```bash
uv run runtime enrich-kubernetes run.runpack snapshot.json --output run-k8s.runpack
uv run runtime enrich-prometheus run-k8s.runpack metrics.json --output run-full.runpack
```

Proofline evaluates explicit behavioral contracts without an LLM. Contract
files are limited to 1 MiB and 1,000 assertions:

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
Each captured annotation stream is limited to 64 MiB and 200,000 JSONL records.

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
uv run runtime record --name batch-drain -- python examples/local/batch_drain.py
uv run batchscope inspect batch-drain.runpack
```

The example finishes parallel compute with 20 items still outstanding, then
drains them through a concurrency-one stage so the post-compute backlog and
serialized constraint are visible in the report.

## End-to-end demonstration

The local pipeline example deliberately adds write amplification and a metadata
dependency while preserving its result:

```bash
uv run rundiff record baseline -- python examples/local/pipeline.py
uv run rundiff record candidate -- python examples/local/pipeline.py --regression
uv run rundiff compare baseline candidate
uv run runtime serve baseline.runpack --compare candidate.runpack
```
