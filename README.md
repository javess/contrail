# Contrail Runtime Tools

Software version control records what code changed. Contrail records what
runtime behavior changed.

[Source](https://github.com/javess/contrail) ·
[Issues](https://github.com/javess/contrail/issues) ·
[Security policy](https://github.com/javess/contrail/security/policy)

Contrail is a local-first suite built around one portable representation of a
software execution:

- **RunDiff** answers: what changed in runtime behavior?
- **BatchScope** answers: where did the wall-clock time go?
- **Proofline** answers: did this change violate a behavioral contract?

The project is being built execution-first. See
[the architecture](docs/architecture.md),
[the execution model](docs/execution-model.md), and
[the delivery milestones](docs/milestones.md). Contributors can start with the
[Python development guide](docs/development.md).

The 0.9 compatibility promises are documented for
[runpacks](docs/compatibility.md) and
[machine-readable output](docs/machine-output.md). Platform and security
boundaries are covered by [support](docs/support.md), the
[threat model](docs/threat-model.md), and [the security policy](SECURITY.md).

## Install and see the workflow

Contrail requires CPython 3.12 through 3.14 on Linux or macOS. Install a release
wheel without cloning the repository:

```bash
uv tool install /path/to/contrail_runtime_tools-0.9.0-py3-none-any.whl
```

`uv tool install` gives the CLI its own isolated environment; it does not add
Contrail to the Python interpreter used by a captured workload. Plain process
capture needs no workload SDK. A Python workload only needs the package when it
imports the optional annotation API. For a local wheel, add that same wheel to
the workload project:

```bash
uv add --project /path/to/workload \
  /path/to/contrail_runtime_tools-0.9.0-py3-none-any.whl
```

After the first PyPI release, use the same released version in both environments:

```bash
uv tool install contrail-runtime-tools==0.9.0
uv add --project /path/to/workload contrail-runtime-tools==0.9.0
```

If the workload is not a uv project, replace `uv add` with `uv pip install
--python /path/to/workload/.venv/bin/python ...` to target the interpreter that
`contrail record` will execute. Then run the self-contained, offline walkthrough:

```bash
contrail demo
```

The demo exits 0 after creating `contrail-demo/` with an adaptable `workload.py`,
baseline and candidate runpacks, a contract, and an explained report. It
deliberately proves two violations while preserving the program result:
`db.write` grows from 3 to 30 and a new `metadata-db` dependency appears. The
command prints copyable recapture, gate, and local timeline commands. Edit the
generated workload and contract to turn the demonstration into a template for
your own runtime invariant.

`contrail` is the primary product command. The existing `runtime`, `rundiff`,
`batchscope`, and `proofline` executables remain compatibility entry points.

## Trace a regression from gate to evidence

Contrail makes a runtime regression reproducible instead of leaving a CI failure
as a disconnected assertion or dashboard link:

1. **Capture traceable evidence.** Record the baseline and candidate as portable,
   versioned SQLite `.runpack` snapshots.
2. **Enforce the behavior that matters.** Proofline evaluates an explicit
   contract over those snapshots and returns a CI-safe status.
3. **Explain the same failure.** Add `--explain` to retain the exact values used
   by every verdict, link them to the related RunDiff detail, and bind the report
   to the SHA-256 and byte size of both runpacks without collecting another pair
   of runs.
4. **Follow the evidence.** Use the preserved artifacts in BatchScope, the
   causal tree, bounded SQL, or the local comparison timeline.

For your own workload, capture the baseline and candidate as portable evidence:

```bash
contrail record --name baseline --output baseline.runpack -- python workload.py
contrail record --name candidate --output candidate.runpack -- python workload.py --new
```

Start with a mechanical contract that requires a healthy candidate, the same
exit status and output, and no more than 10% runtime regression:

```yaml
name: workload-regression
assertions:
  - type: candidate_exit_success
  - type: exit_code_equivalent
  - type: output_equivalent
  - type: max_runtime_regression
    percent: 10
```

Save that as `contracts.yaml`, then evaluate it and atomically retain the exact
debugging record. Verification exits 0 for a full pass, 1 for a violated or
unverifiable claim, and 2 for invalid input.

```bash
contrail verify contracts.yaml \
  --baseline baseline.runpack --candidate candidate.runpack \
  --report proofline-report.json
```

`--report` implies explanation, publishes only a complete artifact-bound JSON
document, and never overwrites an existing path. Invalid inputs leave no report.
Use a new report path for each evaluation. The terminal still shows the human
failure and its RunDiff. Follow the retained evidence without reproducing the
workload:

```bash
contrail analyze candidate.runpack
contrail inspect candidate.runpack --tree
contrail serve baseline.runpack --compare candidate.runpack \
  --proofline-report proofline-report.json
```

Python tests and debugging tools can read the same normalized evidence through
the supported read-only facade. For example, inspect the database writes from
your candidate without depending on Contrail's internal SQLite schema:

```python
from runtime_tools import open_runpack

with open_runpack("candidate.runpack") as runpack:
    db_writes = tuple(
        event
        for event in runpack.events()
        if event.kind == "client.request" and event.name == "db.write"
    )

assert len(db_writes) <= 10
```

This loop also works in one command across two Git revisions with `contrail
run`. In CI, retain its JSON report and both runpacks so every failed contract
remains independently inspectable. See [Proofline in CI](docs/ci.md).

Beyond local command capture, Contrail can normalize bounded OTLP/JSON traces
and logs, Kubernetes snapshots, and Prometheus responses into the same
execution model.

This is a bounded 0.9.0 beta release, not a hosted observability service.
The CLI requires Python 3.12, and local process capture currently uses POSIX
process primitives. Telemetry adapters consume exported JSON evidence rather
than running live collectors, and the UI is local and read-only. Output content
remains excluded unless explicitly requested.

## Local process capture

Python 3.12 is the minimum supported runtime. A source checkout additionally
uses [uv](https://docs.astral.sh/uv/) for locked development environments.

```bash
uv sync --locked
uv run contrail record --name demo -- python -c 'print("hello from Contrail")'
uv run contrail inspect demo.runpack
```

Use `--cwd PATH` when the command should run in another working directory; an
explicit `--output` remains relative to the directory where the CLI was
invoked. `rundiff record` accepts the same option.

Capture relays stdout and stderr to the terminal but stores only their byte
counts and SHA-256 identities. Environment values and output content are not
included in the artifact by default. A small documented allowlist of
behavior-relevant environment variables is represented only by value hashes so
environment drift can be detected without storing the values.

Repeat `--identify-env NAME` to include hashes for workload-specific variables
in that drift comparison. Missing variables are omitted, and original values
are never stored:

```bash
contrail record --name baseline --identify-env FEATURE_MODE -- python demo.py
```

The exact command arguments and resolved working directory are stored as
execution evidence. Do not put secrets in argv or directory names when the
runpack may be shared. Environment and output hashes identify changes but do
not conceal guessable, low-entropy values.

Capture ends when the recorded process exits. If a detached descendant keeps an
inherited stdout or stderr pipe open, Contrail drains buffered output without
waiting for that descendant and records `pipe_open_after_exit` in the stream
metadata so the output identity is not overstated.

Local workloads run in an isolated POSIX process group. If capture itself is
interrupted or fails, Contrail terminates descendants that remain in that group
before removing its temporary artifact and annotation stream.

Output content can be included explicitly as bounded binary attachments. This
may capture secrets, so it is opt-in; each stream stores at most the configured
prefix plus its original byte count and truncation state:

```bash
contrail record --name demo --include-output \
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
contrail query demo.runpack \
  'SELECT kind, name, started_at_ns FROM events ORDER BY started_at_ns'
```

See [querying runpacks](docs/querying.md) for schema and output details.

OpenTelemetry trace exports in OTLP/JSON, bounded to 64 MiB and 1,000,000 spans
per input, can be normalized into the same artifact and inspected as a causal
tree. Span links are independently limited to 1,000,000:

```bash
contrail import-otel trace.json --name checkout
contrail inspect trace.runpack --tree
```

Enrich that execution with OTLP/JSON log records without modifying the trace
artifact. Log inputs have the same 64 MiB byte limit and a 1,000,000-record
limit:

```bash
contrail enrich-otel-logs trace.runpack logs.json \
  --output trace-logs.runpack
```

Imported log bodies and attributes become normalized evidence and can appear in
queries and UI event details. Scrub sensitive log fields before importing or
sharing the enriched runpack.

Known log timestamps outside the execution window are dropped. Service resource
attributes identify log owners, and trace/span IDs create causal `emits` edges
only when they match one known span. Timestamp-less records remain explicit
rather than receiving fabricated times. Unresolved log-to-span references are
retained as causal-completeness metadata for RunDiff and Proofline.

Pass `--include-raw` to either OTLP command only when the original JSON should
travel with the runpack. Normal inspection and the local UI expose attachment
counts, not attachment content. Direct SQLite tooling can retrieve attachment
content; the bounded `runtime query` surface intentionally limits individual
cells to 4 MiB.

Compare any two runpacks with RunDiff. The text report emphasizes changed
entity counts, operation and explicit failure counts, observed max concurrency,
aggregate duration, CPU time, peak memory, and runtime dependencies; JSON keeps
the same structured facts for automation. Comparisons of artifacts carrying the
same execution ID are marked as exact identity matches; distinct executions use
structural matching when their entity, entity-parent, operation, and dependency
key sets align and their internal causal edges connect the same semantic
operations, then fall back to aggregate semantic matching when the observed
shape changes.

Successful comparisons exit 0 by default so reports remain easy to inspect and
pipe. Add `--require-equivalent-outcome` for an automation gate that exits 1
when the behavioral outcome is `different` or `unknown`, and 2 for invalid
input. This gate covers exit status, stdout, stderr, and explicit operation
failures; use Proofline when resource, timing, cardinality, or dependency
changes should fail a build.

```bash
contrail compare baseline.runpack candidate.runpack
contrail compare baseline.runpack candidate.runpack --format json
contrail compare baseline.runpack candidate.runpack --require-equivalent-outcome
```

Open a read-only local timeline, optionally with the structured comparison
facts above it:

```bash
contrail serve baseline.runpack --compare candidate.runpack
```

The default loopback server validates local `Host` headers to prevent DNS
rebinding. Passing a non-loopback `--host` intentionally exposes the read-only
evidence payload to clients that can reach that interface.

Infrastructure JSON evidence can enrich a captured execution without modifying
the original artifact. Inputs are limited to 64 MiB; Kubernetes snapshots also
allow at most 200,000 API items and 1,000,000 containers, while Prometheus
responses allow at most 1,000,000 samples:

```bash
contrail enrich-kubernetes run.runpack snapshot.json --output run-k8s.runpack
contrail enrich-prometheus run-k8s.runpack metrics.json --output run-full.runpack
```

Proofline evaluates explicit behavioral contracts without an LLM. Contract
files are limited to 1 MiB and 1,000 assertions:

```bash
contrail verify contracts.yaml \
  --baseline baseline.runpack \
  --candidate candidate.runpack
```

Add `--report PATH` when a pass/fail result should retain the exact RunDiff and
artifact identities computed from those same snapshots:

```bash
contrail verify contracts.yaml \
  --baseline baseline.runpack \
  --candidate candidate.runpack \
  --report proofline-report.json
```

`--report` implies `--explain`, writes through a private mode-0600 sibling, and
atomically publishes without replacing an existing file. Exit 0 and exit 1 both
publish complete evidence; invalid input exits 2 without creating a report.
Without `--explain` or `--report`, existing text and JSON output stay unchanged.
With `--format json --explain`, the Proofline document adds a nested
`diff`, the canonical assertion policy beside each exact claim fact, and
`artifact_bindings` containing the SHA-256 and byte size of the descriptor-bound
baseline and candidate snapshots. Reopening that report with its runpacks first
checks those exact byte identities, then replays the policy and rejects any
mismatch before showing the failure. The UI distinguishes this artifact-bound
replay from older binding-less reports, which remain readable with lower
assurance.

This binding detects a report paired with stale or substituted evidence; it
does not authenticate who produced the bundle if an attacker can rewrite both
the report and runpacks. Use trusted CI artifact retention or signing when
provenance matters. Text output includes copy-paste commands for BatchScope, the
causal tree, and a contract-aware comparison timeline. Selecting a failed
operation or dependency claim in the timeline focuses the backend-resolved
candidate events; scalar claims focus the candidate summary without inventing
interval attribution.

The dogfood example includes a contract that intentionally catches its
regression:

```bash
contrail verify examples/local/contracts.yaml \
  --baseline baseline.runpack --candidate candidate.runpack
```

See [Proofline contracts](docs/contracts.md) for the supported assertion types
and exit-code behavior.

Validate contract and optional counterexample parameter files without opening
runpacks or executing a workload:

```bash
contrail validate examples/local/contracts.yaml --format json
```

See [Proofline in CI](docs/ci.md) for the frozen JSON/exit-status contract and
an immutable-SHA pull-request workflow.

Proofline can also execute one repository-relative workload at two Git refs in
temporary detached worktrees, preserve both runpacks, and evaluate immediately:

```bash
contrail run examples/local/contracts.yaml \
  --baseline-ref main --candidate-ref HEAD \
  --workload examples/local/pipeline.py \
  --python .venv/bin/python \
  --report proofline-report.json
```

`--python` is optional; it lets an independently installed Contrail CLI run both
refs with one project environment, while each runpack records the exact
interpreter path. See [Proofline execution](docs/experiments.md) for the
isolation and environment guarantees.

Domain work that cannot be inferred from process or OTel evidence can use the
small annotation API. Outside `runtime record` these calls are harmless no-ops.
Each captured annotation stream is limited to 64 MiB and 200,000 JSONL records.
Because this import runs inside the workload, its interpreter or project must
contain the same Contrail wheel/package version as the isolated CLI environment;
installing the CLI with `uv tool install` does not make the API importable there.

```python
from runtime_tools import runtime

with runtime.run("inference", total_work=10_000):
    with runtime.stage("transform"):
        runtime.event("db.write", kind="client.request")
        runtime.event(
            "metadata.lookup",
            kind="client.request",
            **{"peer.service": "metadata-db"},
        )
    runtime.progress(completed=10_000, total=10_000)
```

Stable operation names drive cardinality/error contracts. A `client.request`
event with `peer.service` records dependency evidence even when no matching
server span is available.

Progress counters must be finite numbers satisfying
`0 <= completed <= total`; invalid instrumentation is rejected before it can
write misleading evidence.

BatchScope explains lifecycle, critical path, throughput, and deterministic
bottleneck evidence for one finite run:

```bash
contrail record --name batch-drain -- python examples/local/batch_drain.py
contrail analyze batch-drain.runpack
```

The example finishes parallel compute with 20 items still outstanding, then
drains them through a concurrency-one stage so the post-compute backlog and
serialized constraint are visible in the report.

Release qualification also runs deterministic malformed-input properties and
time/RSS budgets. See [performance qualification](docs/performance.md) for the
quick and release profiles.

## Contributing

Start with the installed-style demo, report runtime defects with preserved
evidence when it is safe to share, and run the repository checks described in
[the contribution guide](CONTRIBUTING.md). Report suspected vulnerabilities
privately through [the security policy](SECURITY.md).
