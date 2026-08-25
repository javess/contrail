# Machine-readable output

Contrail's JSON is the automation surface. Human terminal layout may change.

Every JSON document has:

```json
{
  "document_type": "batchscope.inspect",
  "format_version": "1"
}
```

Consumers should dispatch on `document_type`, reject unsupported format
versions, and ignore unknown fields. Format version `1` permits additive
fields but not removal, renaming, retyping, or incompatible semantic changes.

The authoritative schema is packaged at
`runtime_tools/schemas/contrail-output-v1.schema.json` in wheels and source
distributions. Deterministic examples live in `tests/fixtures/golden/`.

## Document types

| Command | `document_type` |
|---|---|
| `runtime inspect --format json` | `runtime.inspect` |
| `runtime query --format json` | `runtime.query` |
| `runtime job status/wait/cancel --format json` | `runtime.capture_job` |
| `runtime job list --format json` | `runtime.capture_jobs` |
| `rundiff compare --format json` | `rundiff.compare` |
| `batchscope inspect --format json` | `batchscope.inspect` |
| `proofline verify --format json` | `proofline.verification` |
| `proofline run --format json` | `proofline.experiment` |
| `proofline search --format json` | `proofline.search` |
| `proofline validate --format json` | `proofline.validation` |

The matching `contrail` aliases emit the same documents and exit statuses.

## Exit status and streams

For verification commands:

- `0`: every claim passed;
- `1`: a claim failed, evidence was unverifiable, or the workload failed;
- `2`: invalid command input, contract, or artifact.

JSON goes to stdout and diagnostics go to stderr. Prefer `--report PATH` when
retaining a Proofline report: publication is atomic, validation errors leave no
partial destination, and the command's gate status is preserved.

## Evidence completeness

Capture-family summaries use explicit states such as `complete`, `partial`,
`truncated`, `unavailable`, and `invalid`. Missing values are null; they are not
zero or success by implication.

RunDiff exposes the relevant baseline and candidate capture status. Proofline
makes an exact count, error, duration, or dependency claim unverifiable when
the required family is enabled but incomplete on either side. Comparing two
runs where a Deep-only family is unavailable remains valid.

Sample and Deep modes also make workload timing non-comparable when capture
levels differ. Profiler aggregate events remain inspectable but are excluded
from application operation facts.

## Capture families

BatchScope documents may include bounded detail arrays and corresponding
summary objects:

| Summary | Detail | Normalized operation |
|---|---|---|
| `sample_profile` | `python_sample_hotspots` | profiler evidence only |
| `deep_profile` | `python_hotspots` | profiler evidence only |
| `semantic_capture` | `subprocess_calls` | `subprocess.run` |
| `http_capture` | `http_requests` | `http.client.request` |
| `network_capture` | `network_connections` | `network.connect` |
| `network_setup_capture` | setup phases/hotspots | `network.resolve`, `network.tls_handshake` |
| `logical_operation_capture` | logical operations/hotspots | database, cache, queue, broker, executor, scheduler, server |

Public detail arrays are capped at 100 rows while summaries preserve normalized
totals and dropped counts. Exact logical capture retains at most 256 records per
interpreter and 2,000 in the controller.

Logical operations contain only category, generic operation, adapter, PID and
role, timing, safe outcome or exception class, and an optional caller. They do
not contain statements, parameters, cache keys, queue or broker payloads,
callables, task arguments, return values, or exception messages.

`server.request` uses `request_to_response_completion`. WSGI and Uvicorn
h11/httptools records contain adapter identity, timing, numeric status, safe 5xx
classification, PID/role, and exact application caller. Method, route, path,
URL, query, ASGI scope, headers, bodies, client address, arguments, locals, and
exception messages are absent. A completed request without a trustworthy
status makes logical evidence partial. WebSockets are not classified as HTTP.

## Python profiling

Deep summaries can include native-call, Python-exception,
`control_flow_filter`, and `observer_integrity` facts. Exception counts are per
propagated frame, not unique failures. Exact built-in `StopIteration`,
`StopAsyncIteration`, and `GeneratorExit` identities are excluded from the
diagnostic churn count, but type names, values, messages, traceback, arguments,
and locals are never retained.

If workload code calls a public trace/profile hook setter, Contrail downgrades
the affected evidence. It does not inspect the supplied hook.

Per-process profiler contributions include PID, root/descendant role, counts,
and timing. Executable and parent identity are added only when process-tree
evidence uniquely matches that PID.

## Capture jobs

`runtime.capture_job` is a snapshot of local control state, not a remote-job
protocol. It reports job identity, operation, state, worker PID, timestamps,
client disconnection, exit status, artifacts, detached state, and bounded
output metadata. States are `starting`, `running`, `complete`, `failed`, and
`lost`; `complete` can still contain a nonzero workload exit.

Detached jobs retain at most 1 MiB each of stdout and stderr using a bounded
head/tail strategy. `job output` is byte replay rather than JSON and never
increases that limit. Registry and checkpoint files are private formats.

## Python reader

Use the read-only API instead of querying SQLite internals:

```python
from runtime_tools import open_runpack

with open_runpack("candidate.runpack") as runpack:
    execution = runpack.execution()
    events = runpack.events()
    edges = runpack.causal_edges()
```

Supported methods are `manifest()`, `execution()`, `entities()`, `events()`,
`causal_edges()`, `measurements()`, and `attachments()`. Returned records are
frozen snapshots; mutating nested in-memory JSON does not change the runpack.

## Proofline explanations

Explained reports retain:

- canonical resolved assertions;
- verdict, expected/observed text, and evaluated facts;
- JSON-pointer paths and selectors into the embedded RunDiff document;
- byte size and SHA-256 bindings for both runpacks.

The local UI re-hashes the supplied artifacts, recomputes RunDiff, and replays
the assertions before labelling a report artifact-bound. This detects stale or
substituted evidence; it is not authentication if an attacker can replace both
report and runpacks.

See [compatibility](compatibility.md) for change rules and [contracts](contracts.md)
for the assertion vocabulary.
