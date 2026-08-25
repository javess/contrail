# Machine-readable output compatibility

Contrail 0.9 freezes format version `1` for these documented JSON outputs:

| Command | `document_type` |
| --- | --- |
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

Every document contains a `document_type` discriminator and
`format_version: "1"`. The packaged schema is available as
`runtime_tools/schemas/contrail-output-v1.schema.json` and is included in both
wheel and source distributions. The schema describes every required field and
nested value emitted in 0.9. It deliberately permits additional fields so a
format-version-1 consumer can remain compatible with additive releases.

The branded aliases—`contrail inspect`, `query`, `job`, `compare`, `analyze`,
`verify`, `run`, `search`, and `validate`—emit the exact same documents and exit
statuses as the component commands in the table. The command name is a user
interface choice; it does not create a second machine protocol.

Complete deterministic examples for all ten document types live under
`tests/fixtures/golden/`. The compatibility tests compare producer output with
those examples and validate them against the packaged schema without requiring
an optional JSON Schema library.

Within format version 1, fields will not be removed, renamed, retyped, or given
incompatible semantics. New optional fields may be added. Consumers should use
`document_type`, reject unsupported format versions, and ignore unknown fields.
A breaking change requires a new format major version and a documented
migration path.

The branded `contrail job` commands emit the same job documents. A capture-job
object contains `job_id`, `operation`, `state`, `worker_pid`,
`started_at_ns`, `updated_at_ns`, `client_disconnected`, `exit_status`, and
`artifacts`, plus additive `detached` and `output` fields. State is `starting`,
`running`, `complete`, `failed`, or `lost`.
`complete` describes a normally resolved capture command even when its exit
status is nonzero; `failed` is worker infrastructure failure, while `lost`
means the liveness lock disappeared without a terminal update. `wait` returns
the retained exit status when available. The JSON is a snapshot of private
local control state, not part of the runpack or a remote job protocol.
Artifact paths appear only after publication; a successful `proofline run
--report PATH` retains the two runpacks and the report path together.
`output.retained` is true only for an explicit detached launch. Its fixed limit
is 1,048,576 bytes per stream. New jobs use `retention_strategy: "head-tail"`
with 524,288-byte head and tail limits; legacy retained jobs may report
`"head"`. Per-stream size fields separate those segments. `*_omitted_bytes`
counts bytes between them, and `*_omitted_bytes_truncated` means that count is a
lower bound because the worker disappeared or the counter saturated. A stream
truncation flag means its middle was discarded or its final extent is unknown.

`job output` is a byte replay command rather than a JSON document: it writes
retained stdout to stdout and retained stderr to stderr, with omission details
on stderr. Both one-shot and following readers expose only append-only head
bytes while a job is active; `job output --follow` waits and then emits each
final tail after observing terminal state.
Neither command alters capture state or defines a new JSON document, and neither
increases the retention bound.

For `proofline run/search --detach --format json`, the launching frontend emits
a `runtime.capture_job` document in `starting` state. The eventual Proofline
document is retained in the detached stdout stream (or in the requested report
artifact) and can be obtained after `job wait`; asynchronous launch cannot emit
the final experiment or search document synchronously.

When a run was recorded with `--instrument sample`, `batchscope.inspect` adds a
`sample_profile` completeness object and a bounded `python_sample_hotspots`
array. Estimated durations are derived from sample counts and the recorded
sampling interval. With `--instrument deep`, it instead adds a `deep_profile`
completeness object and a bounded `python_hotspots` array containing call
aggregates. RunDiff adds an `instrumentation` object whenever either side is
not passive. Its `timing_comparable` field is false when capture modes differ;
Proofline runtime, CPU, and peak-memory regression claims then become
`unverifiable` instead of treating observer overhead as a workload regression.
Profiler and sampler aggregate events remain queryable in the runpack but are
excluded from RunDiff operation, dependency, duration, concurrency, and error
facts so capture mechanics do not appear as changed application behavior.

New Deep captures add `python_exception_capture` to the instrumentation and
BatchScope `deep_profile` objects. It reports completeness, bounded function
and propagation-event counts, drops, `per_propagated_frame` semantics, disabled
line/opcode events, and explicit false markers for exception type, value,
message, traceback, arguments, and locals. Python `python.call.aggregate`
events and their process contributions may carry `exception_count` greater than
`call_count` because one call can catch or propagate multiple exceptions.
New captures also add `control_flow_filter` to that summary and
`non_control_flow_exception_count` to Python aggregates and process
contributions. Filter format 1 uses `exact_type_identity` semantics and names
the static built-in policy types `GeneratorExit`, `StopAsyncIteration`, and
`StopIteration`; these names describe policy, not types observed in the
workload. It reports diagnostic, filtered, and dropped counts plus explicit
markers that type identity was inspected and exception types were not captured.
Legacy Deep artifacts omit the filter and continue to decode as available raw
counts with control-flow filtering unavailable.

When the Deep profile, Python-exception summary, observer-integrity summary,
and control-flow filter are all complete, no raw or diagnostic exception events
were dropped, and retained aggregate counts reconcile with both summaries,
BatchScope may add a
`python_exception_churn` bottleneck. The classifier considers application
Python functions across the complete retained aggregate set, including rows
beyond the 100-item `python_hotspots` display bound. It requires at least 10
non-control-flow propagation events and 0.5 events per call, emits at most one
strongest finding at 0.6 confidence, and states that built-in iterator
completion is excluded while per-frame events are not unique failures. Partial,
truncated, unavailable, invalid, unreconciled, legacy, or hook-displaced
evidence cannot produce this classification.

New Deep instrumentation and BatchScope summaries also add
`observer_integrity`. Its status is `complete`, `partial`, `unavailable`, or
`invalid`; it reports covered/missing process counts plus detected public
profile-hook and trace-hook setter calls and affected-process counts.
Explicit false markers state that hook arguments, values, and frame locals were
not captured. A detected profile-hook setter call makes the enclosing Deep
profile truncated. A detected trace-hook setter call makes
`python_exception_capture.status` partial while leaving unaffected exact-call
evidence available. Legacy artifacts omit this object or normalize it as
unavailable.

New sample and deep captures also add `semantic_capture` and a bounded
`subprocess_calls` array to `batchscope.inspect`. The summary status is
`complete`, `partial`, `truncated`, `unavailable`, or `invalid`; it contains
reporting-process, retained-boundary, dropped-boundary, callback-error, and
invalid-event counts plus explicit false privacy markers for arguments,
environment, and working directory. Each call contains its event identity,
sanitized executable name, parent and optional child PID, process role,
process-tree match, shell marker, outcome, optional exit or safe error type,
start time, and optional duration. At most 100 calls are rendered in a
BatchScope document while `subprocess_count` preserves the normalized total.
The summary's additive `caller_attribution` object reports its own completeness,
unique callsite count, attributed and unattributed boundary counts, invalid
caller records, and isolated callback errors. Each call's additive nullable
`caller` contains the callsite event ID, function identity, exact or sampled
observation method, and causal-edge confidence. Missing caller data does not
invalidate an otherwise valid subprocess boundary.
The shell marker is null when a caller supplies an opaque shell object; capture
does not invoke application-defined truthiness merely to classify it. A null
shell marker or unidentified executable makes the summary `partial`, preventing
operation contracts from treating the placeholder as complete identity evidence.

Unlike profiler aggregates, normalized `subprocess.run` events are application
operation evidence. RunDiff count, duration, concurrency, and explicit-error
facts therefore include them. Its version-1 document adds
`baseline_semantic_capture_status`, `candidate_semantic_capture_status`, and
the corresponding `*_dropped_subprocess_count` fields. Values are null for
legacy or non-injected captures. Proofline operation-count and operation-error
claims become `unverifiable` when either side exposes subprocess capture but
the two sides are not both complete; a bounded observer must not turn omitted
calls into a false pass.

The sibling `http_capture` summary and bounded `http_requests` array use the
same completeness model independently from subprocess capture. The summary
states the fixed `redact` server-identity policy, false markers for host, URL,
headers, bodies, and credentials, retained and dropped counts, callback and
validation errors, active `adapters`, and caller-attribution completeness. Each
request contains its event identity, `adapter`, safe method, scheme, numeric
port, explicitly null server address, start, duration through response headers,
outcome, status or safe error class, and optional exact or sampled caller. The
adapter fields are additive; missing fields read as `stdlib.http.client`. At
most 100 request details are rendered while the summary preserves the
normalized total.

Normalized `http.client.request` events are RunDiff and Proofline operation
evidence. RunDiff adds `baseline_http_capture_status`,
`candidate_http_capture_status`, and the matching
`*_dropped_http_request_count` fields. Exact operation-count and error claims
become `unverifiable` unless exposed HTTP evidence is complete on both sides.
Their `python.callsite` events and `requests` edges remain queryable provenance
and never become operation facts.

The sibling `network_capture` summary and bounded `network_connections` array
describe physical outbound stream connection attempts independently from HTTP
and subprocess evidence. The summary reports completeness, retained and dropped
counts, callback and validation errors, active adapters, caller attribution,
the fixed `redact` server-identity policy, and false markers for server address,
Unix path, and credentials. Each connection contains its event ID, adapter,
`tcp` or `unix` transport, address family, numeric TCP port when present,
nullable TLS request marker, explicitly null server address, start, duration
through connection readiness, safe outcome or exception class, and nullable
exact or sampled caller. At most 100 details are rendered while the summary
preserves the normalized total.

The additive `network_connection_hotspots` array aggregates the complete
retained set by validated caller plus adapter, with one adapter-only bucket for
unattributed evidence. Each item reports total, connected, failed, and
unfinished attempt counts, summed and maximum setup duration, adapter, and
nullable caller. At most 100 groups are emitted. These are derived presentation
facts; they do not introduce operation events, endpoint identity, or a claim
about logical database queries, cache commands, or queue messages.

Normalized `network.connect` events are RunDiff and Proofline operation
evidence. RunDiff adds `baseline_network_capture_status`,
`candidate_network_capture_status`, and matching
`*_dropped_network_connection_count` fields. Exact operation-count and error
claims become `unverifiable` unless exposed connection evidence is complete on
both sides. Their `python.callsite` events and `connects` edges remain queryable
provenance and never become operation facts.

The additive `network_setup_capture` summary plus bounded
`network_setup_phases` and `network_setup_hotspots` arrays describe DNS and TLS
setup independently from physical connections. A phase contains only
`dns`/`tls`, adapter, PID/role, start and duration, safe outcome or exception
class, and nullable caller. False privacy markers explicitly exclude hostname,
resolved address, SNI, certificate, credentials, and payload. Hotspots aggregate
the complete retained set by phase, caller, and adapter before at most 100 rows
are emitted; summary counts preserve the bounded total.

Normalized `network.resolve` and `network.tls_handshake` events are RunDiff and
Proofline operation evidence. RunDiff adds
`baseline_network_setup_capture_status`,
`candidate_network_setup_capture_status`, and matching
`*_dropped_network_setup_phase_count` fields. Exact operation-count and error
claims become `unverifiable` unless setup evidence is complete on both sides.
Their `python.callsite` events and `resolves`/`handshakes` edges remain
queryable provenance and never become operation facts.

Deep output may additionally include `logical_operation_capture`, bounded
`logical_operations`, and `logical_operation_hotspots`. A logical operation
contains database/cache/queue/broker/executor/scheduler/server category, supported generic operation
class, adapter, PID and role, timing, safe outcome or exception class, and
nullable caller. Explicit false markers exclude statements, parameters,
payloads, queue items, queue identity, executor callables/arguments, asyncio
awaitables/task names/context values, and return values; the observer also
does not copy cache command names or keys, broker destinations or messages, or
exception messages. Server operations add nullable `status_code`, use
`request_to_response_completion`, and carry false markers for HTTP method,
route, URL, headers, body, response body, and client address. Hotspots aggregate the complete retained set by category,
operation, caller, and adapter before limiting public detail to 100 rows.

An `executor.task` interval uses `submission_to_completion` duration semantics.
It begins immediately before standard-library executor `submit` and ends when
the Future callback observes success, cancellation, or a safe exception class.
It includes queueing and result relay and does not claim isolated worker
runtime. Application-originated `queue.*` boundaries remain visible while
library-only queue polling is omitted.

A `scheduler.task` interval uses `creation_to_completion` semantics. Explicit
`create_task`/`TaskGroup.create_task` intervals begin immediately before
delegation; `ensure_future` and implicit `gather` intervals begin as soon as
CPython returns the newly created task. All end when the completion callback
observes success, cancellation, or a safe exception class. Existing Futures
are not counted again. The interval includes event-loop scheduling and
suspended awaits, does not claim CPU time, and does not consume task exceptions.

A `server.request` interval uses `request_to_response_completion` semantics.
For `stdlib.wsgiref`, it begins at `BaseHandler.run` entry and ends when that
handler returns after iterating and transmitting the response. Statuses below
500 are `completed`; statuses from 500 through 999 are `operation_error` with
the synthetic safe class `HTTPStatusError`. A missing status is nullable and
makes the capture partial rather than invalid.

For `uvicorn.h11` and `uvicorn.httptools`, the interval begins when the HTTP
`RequestResponseCycle.run_asgi` coroutine first executes and ends after the
final `http.response.body` send returns. This includes streamed-body suspension
and protocol transmission work. Only `type`, numeric `status`, and `more_body`
are read transiently from ASGI send messages; the messages and ASGI scope are
never retained. Concurrent cycles are associated by transient object/frame
identity rather than a thread-local stack. WebSocket protocol lifecycles are
not `server.request` evidence. A completed boundary without a trustworthy
status remains timed but makes the family partial.

Normalized `database.*`, `cache.*`, `queue.*`, `broker.*`, `executor.task`, and
`scheduler.task` and `server.request` events are
operation evidence. RunDiff adds `baseline_logical_operation_capture_status`,
`candidate_logical_operation_capture_status`, and matching
`*_dropped_logical_operation_count` fields. Two runs where this Deep-only
family is unavailable are compared normally; once either side enables it,
exact operation-count and error claims require complete enabled evidence on
both sides. `python.callsite` and `performs` remain queryable provenance and do
not become operation facts.

Deep profile metadata may additionally contain `native_call_capture`. It
reports enabled/deep-only state, retained native function, call, and exception
counts, independent per-process function/edge limits, and explicit false
markers for arguments, return values, and exception messages. Older runpacks
without this object remain readable and BatchScope emits `null` for the nested
summary. A native `python.call.aggregate` event uses
`implementation: "native"`, `filename: "<native>"`, first line zero, and an
`exception_count` no larger than its call count. Python aggregates use
`implementation: "python"` and zero exceptions. Native aggregates and their
`calls` edges remain profiler facts: RunDiff and Proofline continue to exclude
them from logical operation counts.

Every Python hotspot also has `process_attribution_status`, with value
`complete`, `unavailable`, or `invalid`, and a bounded `processes` array.
Contributions include PID, root/descendant role, per-process counts and timing,
plus an executable basename and parent PID only when process-tree evidence
uniquely identifies that PID. An unmatched contribution remains in the array
with `observed_in_process_tree: false`; consumers must not infer an identity
from PID alone.
Deep hotspot and per-process entries add `implementation` and
`exception_count`; older Python-only evidence defaults to `python` and zero.

The `deep_profile` and `sample_profile` objects also contain
`process_coverage`. Its status is `complete`, `partial`, `unavailable`, or
`invalid`; it reports profiled, observed-Python, and matched process counts,
bounded `unprofiled_processes`, and reporting PIDs that process-tree polling did
not observe. A `complete` zero-of-zero result means the process observer saw no
Python or PyPy executable, so a non-Python workload is not mislabelled as a
capture failure. Older runpacks without reporting PIDs produce `unavailable`
coverage rather than an invented match.

Profile summaries use status `partial` when one or more process documents are
checkpoint-only. They add `checkpoint_process_count`,
`registration_only_process_count`, `first_checkpoint_delay_seconds`,
`checkpoint_interval_seconds`, `transport`, and `collector_error_count`; Deep
Capture also adds `open_call_count`. A registration-only process loaded the
observer but ended before the first 50-millisecond evidence checkpoint, so its
absence of hotspots is not evidence of an idle process. `transport` is
`controller-unix-socket` for the normal controller-side path, `workload-file`
when socket setup was unavailable, or `mixed` when a retained workload file
recovered a session that had a controller socket. Checkpoint evidence remains
queryable, but consumers must not treat it as a complete interval ending at
process exit.

Profile summaries add `dropped_profile_process_count` and
`dropped_profile_process_count_truncated`. The first counts process reports
omitted after the 128-process retention bound; the second means that count is a
bounded lower limit. Overflow is truncated evidence, not an invalid profile.

Within retained process reports, the merged function and relationship bounds
use deterministic exact aggregate ranking rather than report/PID order or the
largest single-process contribution. Deep functions rank by summed self and
total time, sampled functions by summed leaf and total counts; relationships
rank by their summed time/count signal. All contributions to a retained identity
are aggregated in a later pass. Existing `dropped_call_count`,
`dropped_frame_sample_count`, and edge-drop fields therefore count contributions
excluded by the final selected set, independent of report ordering. The
metadata `limits` object exposes the 256 MiB `max_ranking_bytes` controller
workspace ceiling additively.

Their additive `normalization_metrics` object has status `available`,
`unavailable`, or `invalid`. When available, `duration_seconds` measures from
collector shutdown through payload parsing, exact ranking, and normalized event
and edge construction. `ranking_database_peak_bytes` is the maximum primary
ranking-database page footprint across function and relationship selection, and
`ranking_database_limit_bytes` reports its ceiling. The database is the current
algorithm's only disk-backed ranking scratch; the size excludes its bounded
in-memory top-K heap and filesystem metadata, and the duration excludes final
runpack insertion. Legacy runpacks produce `unavailable`; malformed, negative,
out-of-range, or internally inconsistent values produce `invalid` independently
of the profile evidence.

Their additive `snapshot_metrics` object has status `available`, `unavailable`,
or `invalid`. It reports message count, total and maximum payload bytes, and
total and maximum workload serialization seconds. The corresponding
`checkpoint_*` fields include only evidence-bearing periodic checkpoints,
excluding startup registration and final shutdown, so consumers can assess
pauses that occurred while user work was active. Metrics are `unavailable` for
atomic-file fallback and legacy runpacks; inconsistent nested counts are
`invalid` rather than silently normalized. `available` describes messages the
controller received; it is not an attestation that a trusted workload never
used or altered its file fallback.

Their additive `publication_metrics` object also has status `available`,
`unavailable`, or `invalid`. It describes the latest retained snapshot for
each profile process, not overwritten history. `fallback_process_count` and
bounded `fallback_process_ids` identify retained workload-file snapshots;
`socket_attempted_process_count`, `socket_failure_seconds`, and
`max_socket_failure_seconds` describe failed configured-socket attempts before
those writes. A fallback with no configured socket has zero socket duration.
The metrics do not include the atomic-file write itself. New socket-only
captures report available zero counts; legacy runpacks report unavailable.
Malformed versions, counts, PIDs, kinds, or durations become invalid
independently of the Python profile. `transport` is `mixed` when a controller
collector existed but at least one retained process snapshot used workload-file
fallback.

When `--observe-process-tree` was requested, `batchscope.inspect` adds a
`process_observer` completeness object and bounded `process_hotspots`. The
observer status is `complete`, `truncated`, `partial`, `unavailable`, or
`invalid`; absence is never represented as an empty, complete process tree.
RunDiff capture modes use `process` for observer-only runs and compose names such
as `sample+process` when Python and process-tree observation are combined.
Timing comparisons require identical combined modes.

An execution recorded through `--capture-level` stores the selected preset as
`capture.level` in runpack execution metadata. This is additive provenance, not
a substitute for the concrete `instrumentation` and `process_observer` status
objects. RunDiff and Proofline continue to derive timing comparability from the
observers actually requested, including `sample+process` and `deep+process`.

Runpacks produced by command-line capture also contain additive
`capture.worker` provenance with `format_version: 1`,
`mode: "separate-process"`, and boolean `client_disconnected`. The boolean is
true when the worker observes frontend-pipe EOF before its final metadata
commit; it does not mean the captured workload disconnected or failed.
Proofline records the value independently for every retained arm or search
replay. Direct `record_process` API calls and older runpacks may omit the
object. Consumers must treat it as lifecycle provenance, not as an attestation
against same-UID interference.

Newly recorded runpacks also contain additive `capture.recovery` provenance.
Published artifacts use `format_version: 1`, `status: "complete"`,
`checkpoint: "post-exit"`, and `controller_restart_recovered`, which is true
when `runtime recover` or `contrail recover` completed or republished the
checkpoint. Private retained checkpoints may temporarily use `pending` or
`assembled` and carry local session locators; those files are recovery inputs,
not completed portable artifacts or a separate machine-readable CLI format.
The recover command writes no JSON document and exits `0` after publication or
`2` for invalid input or unsafe publication. It never returns the captured
workload's exit status as its own command status; that outcome remains in the
runpack.

## Python runpack reader

Python consumers can read normalized evidence without relying on the SQLite
schema or private analysis code:

```python
from runtime_tools import RunpackError, open_runpack

with open_runpack("candidate.runpack") as runpack:
    execution = runpack.execution()
    events = runpack.events()
```

`open_runpack` accepts a string or path-like filesystem path and returns the
supported read-only `Runpack` facade. Its stable methods are `execution()`,
`manifest()`, `entities()`, `events()`, `causal_edges()`, `measurements()`, and
`attachments()`. `Execution`, `Entity`, `Event`, `CausalEdge`, `Measurement`,
`Attachment`, `JsonScalar`, and `JsonValue` are exported from
`runtime_tools.runpack` for typed consumers.

The reader owns an open snapshot, so callers must use its context manager or
call `close()`. Closing more than once is harmless. Any read after close raises
`RunpackError`. Malformed, unsupported, or unreadable artifacts also fail with
`RunpackError` rather than exposing backend-specific exceptions.

Record instances are frozen and slotted normalized snapshots. Their nested JSON
values are not deeply frozen: metadata and attribute lists or dictionaries are
ordinary mutable Python containers. Changing one of those returned containers
does not write through to the runpack, but consumers that share a record should
copy or treat its nested values as immutable by convention.

## Proofline explanations

`proofline verify` and `proofline run` accept `--explain` when a contract result
must also carry the runtime changes behind it. Explanation is opt-in: without
the flag, their version-1 JSON documents are unchanged.

With `--format json --explain`, the Proofline document adds a `diff` member. Its
value is a complete nested `rundiff.compare` document, including its own
`document_type` and `format_version`. It also adds `artifact_bindings`, with
`baseline` and `candidate` members that each contain the exact snapshot's
non-negative `size_bytes` and lowercase 64-character `sha256` value. A
`proofline.verification` document carries this member at its top level. A
`proofline.experiment` document carries it at the experiment's outer level,
alongside `diff`; its nested `verification` remains the claim document.

```json
{
  "artifact_bindings": {
    "baseline": {
      "size_bytes": 123456,
      "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    },
    "candidate": {
      "size_bytes": 123789,
      "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  }
}
```

Every claim also adds an `assertion` object and an `evidence` array. `assertion`
is the canonical resolved policy: its type, resolved name, and every threshold
or selector field used by the evaluator. Each evidence item contains:

- `fact`, the exact structured values used to evaluate the claim;
- `diff_path`, an RFC 6901 JSON Pointer to related detail, relative to the
  top-level `diff` value;
- `selector`, an equality filter for a referenced array, or an empty object for
  a scalar or object fact.

For example, an operation-count claim can point to
`/operation_count_changes` with `{"operation_name": "db.write"}`, while its
`fact` records the aggregate baseline, candidate, and limit values used for the
verdict. For `proofline run`, the claims are nested under `verification` but
their pointers still resolve against the experiment document's top-level
`diff`.

The assertion, claim result, RunDiff, and artifact bindings are derived from the
same open, descriptor-bound baseline and candidate snapshots. The SHA-256 is
streamed from each descriptor and paired with its byte size. An automation
consumer can therefore retain the policy, exact runpack identities, evaluated
fact, and outcome, resource, operation, timing, or dependency context without
joining separate command outputs. RunDiff arrays list changed groups by
semantic identity, while some claims aggregate across identities. A selector
can therefore resolve to zero rows for an unchanged aggregate or multiple rows
for a split aggregate; `fact` remains the authoritative claim input and the
selected rows are diagnostic context.

When the local timeline opens a current explained report, it hashes the same
open descriptors used for the supplied runpack snapshots and requires both
artifact bindings to match before replay. It then parses the embedded
assertions, evaluates them again, and requires every policy, status,
expected/observed description, fact, and selector to match. The UI labels this
an artifact-bound replay.

Binding-less version-1 reports remain readable for compatibility. A report with
canonical assertions is replayed semantically against the current runpacks and
labelled with lower assurance because it is not byte-bound to them. An older
assertion-less report retains the still lower, explicitly labelled
report-authored policy/result assurance even when its runtime values are
consistent. The binding detects stale or substituted evidence, but does not
authenticate the bundle if the report and runpacks can all be rewritten;
trusted retention, attestation, or signing must supply provenance.

Consumers that do not need explanation can continue to omit `--explain`.
Consumers that request it should validate the outer Proofline document and the
nested RunDiff discriminator independently.

`proofline verify --report PATH` and `proofline run --report PATH` imply
explanation and atomically retain this same artifact-bound JSON document while
preserving human stdout and the 0/1/2 exit contract. A successful or violated
verification publishes a complete mode-0600 report. Invalid input publishes
nothing, and existing files or symlinks are never overwritten. With `--format
json`, stdout bytes and the retained report bytes are identical.

The following are intentionally outside this compatibility promise:

- human-readable terminal layout;
- `contrail report`, which is a human-only diagnostic composition with no JSON
  mode (a successfully produced report exits `0` even when included claims
  fail; `contrail verify` is the contract gate);
- `runtime query --format jsonl`, whose columns are selected by the caller;
- the browser-internal `/api/data` payload; and
- internal Python storage, writer, and analysis helpers outside the documented
  runpack reader and annotation API.

JSON never contains non-finite numeric literals. Unknown or incomplete evidence
is represented with `null`, an explicit completeness field, or an
`unverifiable` claim rather than an inferred success.
