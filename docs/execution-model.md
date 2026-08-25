# Execution model

## Records

An **execution** is one bounded observation. It has a stable identifier, name,
start and optional finish, command/revision metadata, and an outcome. A partial
artifact is valid while capture is in progress.

An **entity** is a participant. `kind` is an extensible string rather than a
closed enum so adapters can preserve new kinds without a schema migration.
Entities may have a parent entity, allowing both logical hierarchies (run,
stage, worker) and physical hierarchies (node, pod, container, process). Logical
and physical identity remain separate records connected by relationships.

An **event** is an occurrence. It has a start and optional finish, so the same
record supports instants and intervals. Events have a semantic `kind`, a
human-readable operation name, an optional owning entity, and attributes.
Adapter-specific identifiers such as trace and span IDs live in correlation
attributes. When an adapter must derive a core ID from multiple external
identifiers, each component is encoded independently; delimiter-bearing source
values therefore cannot make two distinct identity tuples alias.

A **causal edge** states that one event constrained or caused another. Causality
is stored separately from timestamps because clocks can disagree and async
relationships are not necessarily nested. Edge kinds include parent, follows,
publishes, consumes, schedules, and explicit links. The set is extensible.

A **measurement** is a numeric sample or aggregate with a name, value, unit,
timestamp, and optional entity. Measurements are separate from event attributes
because their volume and query patterns differ.

Controller-side process-tree observation adds `process.memory.rss` and
`process.cpu.total` measurements to observed process entities. CPU values are
the host process table's cumulative per-process clock; RSS is an instantaneous
sample. Entity first/last-seen attributes bound what Contrail observed and are
not claims about exact process creation or exit. Processes that detach from the
captured process group or live entirely between polls remain unobserved.

`capture.level` records an explicitly requested preset. `passive` adds no
observer, `process` enables the process-group observer, `sample` adds Python
stack sampling to process observation, and `deep` replaces sampling with exact
Python and CPython-visible native call profiling plus Python exception
propagation counts. Deep retains a compatibility-preserving raw propagation
count and, when filter metadata is complete, a diagnostic count that excludes
exact built-in iterator-control type identities. A missing level identifies a
run recorded through the
older or lower-level options; analyses derive comparability from the actual
instrumentation metadata rather than trusting this convenience label.

Sample and deep captures also normalize automatically observed Python
`subprocess.Popen` boundaries as `subprocess.run` events. A completed event has
an exact observer duration and exit code; a launch error retains only its safe
exception type, while a process whose completion was not observed remains an
open, explicitly `unknown` event. Attributes contain a sanitized executable
basename, parent and optional child PID, root/descendant role, and shell marker.
They never contain arguments, environment, cwd, or output. These events are
operation evidence for RunDiff and Proofline, but do not enter BatchScope's
critical path without an explicit causal edge.
The shell marker is null rather than guessed when reading it would execute
application-defined behavior.

An attributed boundary references an untimed `python.callsite` event through a
`launches` causal edge. The callsite contains module, qualified function name,
source filename, first line, and application/library/runtime scope, but never
arguments or locals. Deep attribution is exact. Sample attribution exists only
when the sampler observes the initiating thread in that active communication or
wait and therefore carries lower confidence. `python.callsite` is provenance,
not an operation or a critical-path interval.

The same modes normalize outbound HTTP boundaries as `http.client.request`
events. The always-on standard-library adapter covers `http.client` and
`urllib.request`; lazy optional adapters cover HTTPX's default `httpcore` sync
and async transports and aiohttp's session request boundary without importing
those packages. A request records its adapter, a safe method, scheme, numeric
port, start, duration through response headers, and response status or safe
exception class. Its server identity is always redacted, and no URL path,
query, header, body, credential, response content, or response-body timing is
captured. An attributed request references `python.callsite` through a
`requests` edge with exact or sampled confidence. Sampled attribution is
available only for synchronous waits; async callers remain explicitly
unattributed in Sample mode and are exact in Deep mode. HTTP operations remain
queryable and comparable, but both the HTTP event and its callsite are excluded
from BatchScope lifecycle and critical-path calculations to avoid duplicating
an application's own request spans.

Clients without a request-level adapter can still produce `network.connect`
events when they create a blocking Python stream socket or an asyncio TCP or
Unix transport. These are physical connection attempts, not protocol requests:
a connection pool may serve many database calls after one event. The record
contains transport, address family, numeric TCP port, optional asyncio TLS
request marker, timing through connect or transport readiness, safe outcome or
exception class, and optional caller. Server addresses and Unix paths are
always redacted. An attributed connection references `python.callsite` through
a `connects` edge. Deep callers are exact; synchronous Sample callers exist only
when a sampling tick observes the active connect, and async Sample callers are
unattributed. Supported HTTP adapters suppress their nested connection event.
Connection operations remain queryable and comparable but stay outside
BatchScope lifecycle and critical-path calculations.

DNS and TLS setup are separate `network.resolve` and
`network.tls_handshake` operations, not inferred slices of a connection event.
The standard-library wrappers observe forward name resolution plus blocking and
asyncio TLS handshakes. A record contains phase, adapter, timing, safe outcome
or exception class, and optional caller; hostname, returned address, SNI,
certificate, credentials, and payload stay outside the model. `resolves` and
`handshakes` edges link attributed setup to a `python.callsite`. Both client and
server handshakes may appear because the SSL boundary does not provide a
trustworthy direction marker. Setup operations are queryable and comparable but
stay outside BatchScope lifecycle and critical-path calculations.

Deep Capture additionally emits supported logical operations as
`database.execute`, `database.executemany`, `database.executescript`,
`database.commit`, `database.rollback`, `cache.command`, `cache.batch`,
`queue.put`, `queue.get`, `broker.publish`, `broker.consume`, or
`executor.task` or `scheduler.task`. Adapters cover
the standard `sqlite3.Connection`/`Cursor`, `queue.Queue`, and `asyncio.Queue`
boundaries, standard thread/process executor submission-to-completion, plus
explicit `asyncio.create_task` and `TaskGroup.create_task`, top-level
`asyncio.ensure_future`, and implicit coroutine scheduling by top-level
`asyncio.gather`, all with creation-to-completion timing, plus
documented public SQLAlchemy, Redis, Pika blocking-channel, and aiokafka methods
when those optional packages are present. The standard-library `wsgiref`
handler adds `server.request` from handler entry through response completion,
with its numeric status and exact first application caller. Each event contains
only operation category and class, adapter, PID/role, start and duration, safe
outcome or exception class, and optional exact caller. Statements, parameters,
cache command names and keys, broker destinations and messages, returned rows,
queue items and identity, executor callables and arguments, asyncio awaitables,
task names and context values, payloads, and return values are explicitly
absent. WSGI method, route, URL, headers, request/response bodies, and client
address are explicitly absent. Application-originated queue calls are retained; library-only queue
polling is excluded. A
task-local marker suppresses nested wrapped layers, and a `performs` edge links
an attributed operation to its `python.callsite`.
Logical-operation events remain RunDiff/Proofline operation facts but are
excluded from BatchScope lifecycle and critical-path calculations because an
application span may already describe the same call.

Sampling and Deep Capture metadata include the bounded PIDs of interpreters
that published a report. When process-tree evidence is also available,
BatchScope compares those PIDs with uniquely observed executables whose basename
starts with `python` or `pypy`. Coverage is complete only when process-table
observation is complete, every observed interpreter reported, and every
reporting PID was observed. This heuristic deliberately does not claim that an
arbitrarily renamed or embedded interpreter is Python unless it actually emits
a profile.

Each generated profile document is either a `checkpoint` or `final` snapshot.
The first checkpoint is an empty registration published synchronously before
the observer starts, the first evidence-bearing checkpoint follows after 50
milliseconds, and later checkpoints use a 500-millisecond cadence. The capture
controller retains the latest snapshot per PID. If an interpreter ends before
publishing `final`, its latest checkpoint still contributes bounded evidence,
while the profile summary becomes `partial` and reports how many processes are
checkpoint-only. A registration-only process proves capture loaded but makes no
hotspot claim. A later checkpoint can be up to one interval stale and Deep
Capture excludes calls that had not returned by that snapshot; both conditions
remain explicit rather than being treated as complete timing.

The controller retains at most 128 process reports. A larger pool keeps bounded
evidence, sets `dropped_profile_process_count`, and marks the profile truncated
or partial; it does not reject all reports merely because one more interpreter
arrived. Socket-backed captures also retain aggregate transport metrics for all
registration, checkpoint, and final messages, plus a checkpoint-only subset
that isolates serialization pauses incurred while user work may still be
running. Atomic-file fallback cannot recover historical message metrics and
marks them unavailable.

Deep Capture represents one observed Python or native C function identity as an
untimed `python.call.aggregate` event. Python identities retain module,
qualified name, source filename and line, and scope. A native identity retains
only bounded module and qualified name, a synthetic `<native>` filename, scope,
and `implementation: native`; call arguments and return values are absent. Both
retain call count, total time, self time, maximum call time, and observed
process count. Native aggregates also retain `exception_count` without an
exception value or message. Aggregated `calls` edges retain parent-to-child
call counts and total child time across Python/native boundaries. These are
profiler summaries, not real execution intervals, so they never participate in
critical-path, concurrency, or lifecycle timing.

Each deep aggregate may also contain a bounded `processes` array. Every entry
identifies the contributing PID, whether it was the capture root or a
descendant, and that process's call count, native exception count, and total,
self, and maximum time.
Python entries may also carry a raw exception-propagation count and an additive
non-control-flow count; neither is a unique failure count.
The entries must reconcile with the aggregate; malformed attribution is
discarded without discarding the otherwise valid hotspot.

Sampling Capture represents a function observed on a Python thread stack as an
untimed `python.stack.sample` event. It retains inclusive and leaf-frame
sample counts plus interval-derived time estimates. `stack_parent` edges retain
observed caller-to-child stack relationships and sample counts. These are
statistical observations rather than calls or execution intervals; they do not
participate in critical-path, concurrency, or lifecycle timing. Estimates are
nominal sample counts multiplied by the configured interval; across multiple
threads they are cumulative stack-residence estimates and may exceed wall time.

Each sample aggregate may likewise contain a bounded per-PID `processes` array
with root or descendant role, inclusive and leaf counts, and their nominal time
estimates. These contributions describe where samples were observed, not an
exact division of wall time between processes.

An **attachment** is optional opaque evidence such as a bounded log stream or
raw adapter input. It carries a kind, name, media type, bytes, and JSON-safe
attributes. Attachments are never required for normalized analysis and are not
rendered by default because they may contain secrets. Content is limited to 64
MiB per attachment and 256 MiB in aggregate per runpack.

Attributes are JSON objects at adapter boundaries. Values must be JSON-safe;
query-prominent concepts graduate to typed columns only after demonstrated use.
Normalized JSON fields are limited to 4 MiB so portable artifacts remain safe
to inspect; larger raw evidence belongs in an attachment.

## Time and uncertainty

Times are UTC Unix nanoseconds when known. Every timed record can include a
clock domain and an uncertainty in nanoseconds. Raw source timestamps may be
retained in attributes. A missing timestamp remains null rather than being
fabricated.

Process capture records the UTC start once and derives the finish from monotonic
elapsed time. Host clock adjustments during a command therefore cannot make its
execution interval run backward or disagree with its wall-time measurement.

Ordering uses the strongest available evidence:

1. explicit causal edges;
2. source-local sequence numbers;
3. timestamps whose uncertainty ranges do not overlap;
4. otherwise, unknown order.

Readers must not infer that a child happened after a parent solely because its
wall-clock timestamp is larger. Impossible timestamp orderings can be reported
as clock-skew evidence without deleting the causal relationship.

BatchScope labels a critical path as observed only when every selected interval
uses the same known clock domain and the artifact reports complete causal and
annotation evidence. Cross-domain or incomplete paths retain a best-effort
duration but are explicitly labelled inferred.

Observed maximum concurrency is calculated only among complete intervals in
the same known clock domain. RunDiff takes the maximum observed within any one
domain; it never sums overlap across clocks that may be skewed. If any event in
a semantic operation group lacks an interval or clock domain, concurrency for
that group remains unavailable rather than being reported as zero.

## Identity and repetition

Execution, entity, and event IDs are unique within an artifact. Generated IDs
are opaque. Repeatable logical identity belongs in semantic keys such as entity
kind/name, operation name, stage path, attributes, and source-local sequence.
RunDiff marks artifacts with the same execution ID as exact matches. Distinct
executions with the same entity, entity-parent, operation, and dependency key
sets are structural matches only when internal causal edges also connect the
same semantic operations. Changed shapes fall back to aggregate semantic
comparison; neither level assumes event or entity IDs survive repetition.

## Query implications

The artifact indexes event times, entity ownership, event kind/name, edge
endpoints, and measurement name/time. These support the initial questions:

- a time window scans event interval bounds;
- dependency traversal starts from indexed edge endpoints;
- active entities derive from their events and lifecycle intervals;
- lifecycle uses top-level explicit stages before heuristics, avoiding additive
  double-counting of nested stages;
- critical paths use a causally connected graph subset;
- throughput is inferred from one explicit series, logical parent, or entity at
  a time; unrelated counters are never merged into one rate, and an explicit
  series with one common causal run uses that run's completion boundary instead
  of extending drain time through process teardown;
- exported Temporal history contributes a workflow `run`, activity lifecycle,
  queue-wait intervals, terminal outcomes, and attempt counts. A uniquely
  matching Python `RunActivity` span remains the execution interval; when none
  exists, history start-to-terminal timing supplies a fallback operation;
- comparisons aggregate semantic keys rather than opaque IDs.

Confidence is part of derived analysis, not a replacement for evidence. Reports
must distinguish observed facts from inferred lifecycle phases or causal links.
