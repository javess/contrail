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

Runpacks are versioned SQLite databases, so their normalized evidence remains
inspectable without Contrail or a hosted service:

```bash
sqlite3 demo.runpack '.tables'
```

Use `runtime inspect demo.runpack --format json` for machine-readable output.

Bounded OpenTelemetry trace exports in OTLP/JSON can be normalized into the
same artifact and inspected as a causal tree:

```bash
uv run runtime import-otel trace.json --name checkout
uv run runtime inspect trace.runpack --tree
```

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

Domain work that cannot be inferred from process or OTel evidence can use the
small annotation API. Outside `runtime record` these calls are harmless no-ops.

```python
from runtime_tools import runtime

with runtime.run("inference", total_work=10_000):
    with runtime.stage("transform"):
        runtime.event("db.write", kind="client.request")
    runtime.progress(completed=10_000, total=10_000)
```
