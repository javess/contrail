# Architecture

## Product boundary

Contrail captures a finite execution, normalizes evidence into a stable model,
and stores it in a portable `.runpack`. RunDiff, BatchScope, and Proofline are
different analyses of that same artifact; they do not own alternate execution
models.

```text
process / OTel / Kubernetes / Prometheus / Temporal / logs
                           |
                     capture adapters
                           |
                normalized execution records
                           |
                 versioned SQLite .runpack
                           |
           +---------------+----------------+
           |               |                |
        RunDiff        BatchScope        Proofline
```

Adapters describe how evidence was produced. The core describes what happened.
For example, an OTel span is adapter input, while a timed operation and its
causal parent are core concepts.

Opt-in Python sampling and Deep Capture are also adapters. Capture prepends a
private, standard-library-only `sitecustomize` bootstrap to the child
`PYTHONPATH`. Sampling inspects current Python thread stacks every 10
milliseconds; Deep Capture observes every Python and native C call with
`sys.setprofile` and Python exception propagation with `sys.settrace`. Its trace
callback disables line and opcode events on every frame. It retains the raw
per-function propagation count and a diagnostic count that transiently compares
the exception-type object by exact identity with the built-in `StopIteration`,
`StopAsyncIteration`, and `GeneratorExit` objects. It does not read or retain
the exception value, traceback, type name, or type object.
While the profile callback is active, it recognizes calls to the public
`sys.setprofile`, `sys.settrace`, `threading.setprofile`, and
`threading.settrace` replacement surfaces, including the CPython 3.12+
all-thread variants. It increments only per-process counters before a hook can
replace capture; no hook argument or replacement callable is inspected. Every
setter call is therefore conservatively treated as possible displacement. A
profile setter call marks call/caller evidence truncated, while a trace setter
call independently makes Python-exception coverage partial. Fork reset
clears inherited counters and reinstalls both child hooks.
Both aggregate in each workload process. During startup, each interpreter
synchronously sends one empty, bounded registration snapshot before sampling or
call profiling begins. A background thread sends the first evidence-bearing
aggregate after 50 milliseconds and then every 500 milliseconds over a private
Unix stream socket to the already separate Contrail controller process; no call
or sample performs IPC on its hot path. The controller atomically retains only
the latest snapshot for each PID, so evidence-bearing checkpoints and normal
final reports overwrite the registration. Registration and periodic-checkpoint
socket operations use a 100-millisecond timeout; final publication retains a
one-second timeout after user code has stopped. The collector also abandons a
stalled read after 100 milliseconds. If the socket cannot be created or used,
or any operation times out, the workload atomically writes the same snapshot
into the private session directory. Every new report carries a publication
metrics version. A fallback report also carries whether a controller socket was
configured, the failed socket duration, and its snapshot kind. This small fact
is appended to the already encoded bounded document before the atomic write, so
it does not repeat function/edge JSON serialization. The fixed transport header
carries the snapshot kind and the time spent constructing and serializing the
payload; the controller validates both before recording bounded message, byte,
and serialization metrics. It admits at most 128 distinct PIDs, still consumes
valid messages from later PIDs, and reports those omissions instead of creating
enough files to invalidate all evidence. File-fallback normalization applies
the same process bound and prioritizes the known root PID. The parent reads the
retained report bytes once, validates them, and performs exact aggregate top-K
selection in a temporary SQLite workspace owned by the controller. Deep
functions rank by fleet-wide self time and then total time; sampled functions
rank by fleet-wide leaf count and then total count. Deep edges rank by aggregate
total time and call count, and sampled edges by aggregate sample count, with a
stable SHA-256 identity tie-break in every case. The mode-0600 workspace is
created inside the private capture session, capped at 256 MiB, and uses a small
page cache. Selection scans its candidates through a bounded 20,000- or
50,000-entry controller heap instead of asking SQLite for a disk-backed sort, so
the database is the only on-disk ranking scratch. It is automatically deleted
when selection ends. Later passes
over the same captured bytes preserve all process contributions for every
selected identity. This prevents file or PID ordering and one-worker spikes
from choosing evidence at the merged 20,000-function and 50,000-edge bounds
without retaining every candidate in controller memory. No ranking or storage
runs in the workload process. The parent then normalizes sampling as untimed
`python.stack.sample` events plus `stack_parent` edges, or deep profiling as
untimed `python.call.aggregate` events plus `calls` edges. A checkpoint left by
`os._exit`, a fatal signal, or a crash is explicitly partial. Neither mode
participates in lifecycle or critical-path calculations. BatchScope renders
sampling as estimated hotspot evidence and Deep Capture as explicitly intrusive
call-timing evidence. A registration left by a process that ends before the
first 50-millisecond checkpoint proves the observer loaded but carries no
hotspot claim.

Deep handles `call`/`return` and `c_call`/`c_return`/`c_exception` as one nested
profile stack. Native identities use only the callable's bounded module and
qualified name, a synthetic `<native>` filename, and runtime/library scope;
the profiler never reads call arguments, return values, exception values, or
exception messages. Native elapsed time becomes child time of the Python
caller, so a blocking C wait is attributed to its real native boundary instead
of inflating the caller's Python self time. `calls` edges preserve both
Python-to-native and callback relationships. Python and native identities have
independent 2,000-function and 10,000-relationship budgets per interpreter,
then share the existing 20,000-function and 50,000-relationship controller
selection. A native aggregate additionally retains a bounded exception count.
Calls made inside the semantic observer remain under its ignored stack marker,
preventing observer bookkeeping or an already classified wrapper from becoming
generic native evidence.

BatchScope derives exception-churn diagnoses only after capture. It reconciles
the complete retained Python aggregate set with the Deep exception summary,
requires complete observer integrity and zero drops, then selects at most one
application function that crosses the conservative absolute and per-call
thresholds. This controller-side analysis adds no workload hook or artifact
field, and its evidence always labels counts as per-frame propagation events,
not unique failures.

Both injected modes also load one shared, standard-library-only semantic
observer beside `sitecustomize`. It wraps `subprocess.Popen` initialization,
communication, polling, and waiting without replacing the public class. Each Python process
retains at most 256 records containing a sanitized executable basename, PIDs,
shell use, wall-clock start, monotonic duration, and exit or launch outcome;
arguments, environment, cwd, and output are never read into evidence. Forked
interpreters discard inherited records. The observer snapshot rides in the
same registration, checkpoint, and final document as profiling evidence, so it
inherits the existing crash recovery and transport bounds without another
workload channel.

The observer wraps `http.client.HTTPConnection` request construction, header
completion, response-header receipt, and close methods without replacing the
public class. A narrow lazy-import hook also wraps
`httpcore.ConnectionPool.handle_request`,
`httpcore.AsyncConnectionPool.handle_async_request`, and
`aiohttp.ClientSession._request` when those optional modules appear. The hook
sits immediately before the standard `PathFinder`, delegates execution to the
real loader, restores the module's loader, and then patches the supported
method. Contrail does not import or depend on either client package, and custom
meta-path finders that resolve a module before `PathFinder` are deliberately
left alone.

A separate 256-record per-process budget retains only the adapter, safe HTTP
method, scheme, numeric port, start, monotonic duration through response
headers, status or safe error class, outcome, and caller identity. Server names,
URLs, paths, queries, headers, bodies, and credentials are never written to the
snapshot; server identity is explicitly redacted. `httpcore` gives HTTPX's
default sync and async transports one physical-request boundary for HTTP/1.1
and HTTP/2. aiohttp's session boundary represents one logical call and may
contain redirects.

A third, independent 256-record budget observes protocol-agnostic outbound
stream connections. The observer wraps blocking `socket.socket.connect` and
lazily wraps asyncio TCP and Unix transport creation. Records contain only the
adapter, TCP or Unix transport, address family, numeric TCP port, asyncio TLS
request marker, timing until connect or transport readiness, safe outcome or
exception class, and caller. Host or IP identity and Unix paths are never read
into evidence. Raw nonblocking socket connects are skipped because their
initial `EINPROGRESS` is not a terminal outcome; the higher-level asyncio
adapter owns those attempts instead.

A fourth independent 256-record budget observes network setup phases without
trying to infer them from connection duration. It wraps `socket.getaddrinfo`
for DNS plus `ssl.SSLSocket.do_handshake` and `ssl.SSLObject.do_handshake` for
blocking and event-loop TLS. A nonblocking `SSLObject` record remains open
across `SSLWantReadError` or `SSLWantWriteError` retries and finishes only when
the handshake succeeds or reaches a terminal exception. Setup records contain
only DNS/TLS phase, adapter, timing, safe outcome or exception class, PID, and
caller. Hostname, returned addresses, SNI, certificate, credentials, and
payload are never copied. Unlike physical connection suppression, setup phases
remain visible inside an HTTP boundary so one request can still be decomposed
without emitting a duplicate `network.connect` event. Normalization retains at
most 2,000 setup phases across processes and emits `network.resolve` or
`network.tls_handshake` events with `resolves` or `handshakes` provenance edges.

A fifth independent budget is enabled only by Deep Capture. Lazy
standard-library adapters observe SQLite connection/cursor execute,
executemany, script, commit, and rollback methods plus blocking and asyncio
queue put/get methods. SQLite's C extension types cannot be patched in place,
so the adapter replaces the module's default connection and cursor factories
with compatible subclasses only inside the already intrusive Deep workload.
Explicit custom factories and direct `_sqlite3` use are left untouched by this
semantic adapter, though CPython-visible methods still reach the generic native
profile. Queue classes are patched in place; `SimpleQueue` is not.

The same import hook recognizes explicit optional-client module and class
paths without importing or depending on those packages. It wraps SQLAlchemy
sync/async Connection and Session execution and transaction methods; Redis
sync/async command and pipeline execution; Pika BlockingChannel publish/get;
and aiokafka producer send-and-wait plus consumer get-one/get-many. A task-local
suppression marker makes the outer public boundary authoritative when an ORM,
async proxy, or pipeline calls another wrapped layer. Methods retain their
wrapped signatures, module loaders are restored after activation, and a module
with an unsupported shape increments the logical observer error count instead
of falling back to method-name heuristics.

The same lazy hook wraps standard `ThreadPoolExecutor.submit` and
`ProcessPoolExecutor.submit`. It records the submitting caller and start before
delegating unchanged, then adds a payload-free callback to the returned Future.
The callback reads only cancellation state or the completed exception object's
safe class name; it never calls `result()` or retains the callable, arguments,
successful value, or exception object/message. The interval therefore spans
submission through Future completion, including queueing and result relay.
Queue adapters retain only boundaries with an exact application caller when a
caller is available, preventing executor-manager polling and its expected
`queue.Empty` control flow from becoming application queue failures.

The hook also wraps the public `asyncio.create_task`, `asyncio.ensure_future`,
and `asyncio.gather` functions plus the `TaskGroup.create_task` method. It
records only calls with an exact application caller, delegates task creation
unchanged, and adds a payload-free completion callback. The gather wrapper uses
a synchronous thread-local origin while CPython converts coroutine arguments;
existing Futures are identified only by returned-object identity and are not
counted again. The thread-local label cannot be copied into the new task's
context. Cancellation is read through the public state predicate. On CPython,
failure classification reads only the task's stored exception object long
enough to derive its class name; it does not call `Task.exception()`, retain the
object, or suppress Python's unhandled-task warning. The interval spans creation
through completion, including scheduling delay and suspended awaits. Event-loop
bootstrap/shutdown tasks and task creation through lower-level APIs are not
classified. The wrapper deliberately does not set the logical-operation
suppression `ContextVar` while delegating: task creation copies the current
context, which would otherwise suppress real queue/database work inside the new
task.

Inbound `wsgiref` uses the existing Deep profile hook instead of replacing a
server or application callable. An exact call to
`wsgiref.handlers.BaseHandler.run` opens one request boundary; its
`start_response` call contributes only the parsed three-digit status; and the
`run` return closes the boundary. The first application-scope function entered
while that handler frame is active becomes the exact caller. Application calls
continue through the ordinary aggregate profiler, so semantic attribution does
not hide application hotspots. Frame locals other than the transient WSGI
status string are never read. A request that closes without a valid status is
retained with duration but makes logical-operation completeness partial.

Each process retains at most 256 records containing only database/cache/queue/
broker/executor/scheduler/server category, generic operation class, adapter, timing, safe outcome
or exception class, PID, and caller. Statements, parameters, cache commands and
keys, broker destinations and messages, rows, queue items and identity,
executor callables and arguments, asyncio awaitables, names, context values,
payloads, credentials, exception messages, and return values are never copied.
Server method, route, URL, headers, bodies, and client address are likewise
excluded; only a numeric WSGI response status may be retained.
Controller normalization retains at most 2,000 operations, prioritizing failed
or unfinished work and then duration. It emits `database.*`, `cache.*`,
`queue.*`, `broker.*`, `executor.task`, `scheduler.task`, or `server.request` events plus
`performs` provenance edges, with an independent completeness status.

HTTP adapters set a task-local `ContextVar` while executing their lower-level
transport work, suppressing nested connection records. The profile snapshot
sender uses the same suppression around its private Unix socket. This keeps
capture transport and one logical HTTP request from becoming application
connection evidence without using a global flag that could hide unrelated
threads or asyncio tasks.

Caller attribution reuses evidence each mode already owns rather than walking
frames synchronously in the wrapper. Deep Capture supplies the exact nearest
application call from its maintained profile stack. Sampling marks a concrete
`Popen` communication or wait, or a synchronous HTTP wait, active by thread;
when the existing sampler sees that stack, it attaches the nearest application
frame as a sampled caller. A short or otherwise unobserved wait remains
unattributed. Async HTTP adapters intentionally do not publish a sampled caller
because event-loop tasks share one thread; Deep Capture reads its exact active
profiler stack when the boundary begins. This avoids arguments, locals, extra
stack IPC, and Python frame-introspection audit events on the boundary path.

Controller normalization validates subprocess, HTTP, connection, DNS/TLS
setup, and logical-operation evidence independently from each other and from
the profile aggregates. Malformed
evidence in one family does not discard valid evidence in another. Across
accepted processes it retains at most 2,000 records of each family, prioritizing
failed or unfinished work and then duration; omissions remain counted. A valid
caller becomes one untimed `python.callsite` event and an explicit `launches`,
`requests`, `connects`, `resolves`, `handshakes`, or `performs` edge to its
attributed operation. Malformed caller
data invalidates attribution without discarding the boundary. Callsite events
are excluded from lifecycle, critical-path, and RunDiff operation facts.
All normalized boundary events remain normal operation evidence but are
excluded from BatchScope lifecycle and
critical-path inference because their injected spans can duplicate application
spans. Proofline treats partial, truncated, or invalid boundary evidence as
insufficient for exact operation-count and error claims.

BatchScope derives connection hotspots from the complete bounded normalized set
before limiting public detail to 100 rows. A hotspot groups one validated caller
event and adapter, or one unattributed adapter bucket, and aggregates connected,
failed, unfinished, total-duration, and maximum-duration facts. It does not
group by redacted endpoint or infer protocol operations. The aggregation adds no
work to the observed process and does not alter RunDiff or Proofline facts.

The controller measures the interval from collector shutdown through parsing,
exact selection, and normalized event and edge construction. It also records
the largest primary ranking-database page footprint across function and
relationship selection. These post-run metrics exclude workload execution,
the bounded in-memory top-K heap, and final insertion into the runpack. The
ranking database is the algorithm's only disk-backed scratch; these are not
workload-side capture-overhead measurements.

The command-line control plane separates the transient frontend from capture
ownership. `runtime record`, `contrail record`, and `rundiff record` start the
same installed command in a new session as a capture worker. `proofline run`
and `proofline search` do the same around their complete multi-run orchestration.
The frontend retains the write end of an anonymous pipe while the worker has a
daemon thread blocked on the read end. No command, event, profile, output, or
runpack data crosses this pipe: EOF only tells the worker that its client
disappeared. The worker owns workload launch and reaping, output relays,
collectors, temporary runpacks, Proofline worktrees, and publication. It
continues after abrupt frontend loss and records that fact before final commit.
For intentional Ctrl-C, the frontend signals the worker process group and the
worker unwinds the existing capture cleanup path. The direct `record_process`
Python API remains in-process and does not add this CLI ownership layer.

An explicit `--detach` launch omits the liveness pipe and gives the worker
`/dev/null` as stdin. Its stdout and stderr each target an anonymous pipe whose
read end is inherited by that same worker. Two controller threads continuously
drain those pipes into private files in the job directory. Each stream has a
512 KiB append-only head and a 512 KiB rolling tail, preserving the first and
most recent bytes under the same 1 MiB bound while discarding the middle. A
thread keeps its tail in a bounded chunk deque and checkpoints it at most every
500 milliseconds under an advisory file lock; rewriting the one mode-0600 tail
file never creates a second on-disk raw-output copy. The drains continue after the
retention bound so a verbose workload cannot block. At command completion the
worker redirects its own streams to `/dev/null`, gives inherited writers one
second to close, writes and fsyncs the final tails, marks a stream incomplete if
that deadline expires, joins both drains, and only then commits terminal job
state. No output is retained unless the operator selected `--detach`.

`job output --follow` is a second local reader of those same files, not another
capture transport. It reopens validated head files and reads only bytes after
its independent stdout and stderr offsets, polling job state every 100
milliseconds. Rolling tails cannot use append offsets, so both one-shot and
following readers emit them only after terminal state proves them final.
Exiting either reader does not signal or otherwise change the worker.

Each CLI worker also owns one private local job record, an advisory lock, and a
named cancellation pipe. A UUID job ID names a mode-0700 directory; its atomic
mode-0600 JSON state contains the operation, worker PID, timestamps,
client-disconnection flag, terminal exit status, and at most four artifact
paths. Detached records additionally expose the head/tail strategy, segment
sizes, omitted-middle counts, and whether those counts are lower bounds. The
state excludes the captured command and
arguments. The worker holds an exclusive lock for its entire lifetime. A reader
must acquire that same lock and re-read state before turning an unfinished
record into `lost`, so it cannot overwrite a concurrently committed terminal
result. A cancelling client writes one byte to the private pipe; the worker,
rather than a client acting on a reusable PID, signals itself and enters the
same bounded interrupt cleanup. No registry daemon participates. Terminal
records and their detached output are retained for at most seven days and
capped at 100.

Runpack assembly has a post-exit recovery boundary. Once the workload outcome,
resource measurements, process observations, annotations, optional output
attachments, and root process event are committed, the controller stops the
snapshot collector and records a versioned recovery checkpoint in its private
mode-0600 temporary runpack. The checkpoint binds the profile directory by
absolute path plus device and inode and retains the controller transport
metrics needed to reconstruct the same profile provenance. Profile events and
the execution metadata that describes them commit in one SQLite transaction.
The controller then removes the raw profile session, marks recovery complete,
and atomically publishes the runpack. `contrail recover` can resume a pending
checkpoint or publish an already assembled one after controller loss without
rerunning the workload. A pre-exit capture-worker loss remains unrecoverable
because the exit status, output digests, and resource outcome do not yet exist.
Frontend loss is survivable because the worker is the controller; the liveness
pipe adds no workload-side IPC.

Optional process-tree observation is controller-side rather than injected. A
bounded observer polls the host process table for members of the workload's
POSIX process group, normalizes observed descendants as process entities, and
stores RSS plus cumulative CPU measurements. First and last polls are evidence
windows, not inferred process start or exit times. The observer composes with
passive, sampling, and Deep Capture modes and never turns observation failure
into workload failure.

The public capture ladder is a configuration layer over those independent
adapters: `passive`, `process`, `sample`, and `deep`. The sample and deep presets
also enable process-tree observation; deep substitutes exact Python/native call
and Python exception-event profiling for statistical sampling. RunDiff,
Proofline experiments, and counterexample search
pass the same preset into the same capture adapter rather than reimplementing
collection. Existing low-level flags remain available for expert composition,
but a preset and low-level override cannot be mixed ambiguously.

BatchScope correlates a profile contribution with process-tree evidence only
when that PID identifies one observed process generation. Root and descendant
roles come from the capture boundary, not from a guessed name. If polling missed
the process or PID reuse made the identity ambiguous, the contribution remains
visible but unmatched. The aggregate hotspot stays stable even when its nested
process attribution is absent or invalid.

Profile metadata separately retains the bounded set of reporting PIDs.
BatchScope compares that set with uniquely observed process identities whose
executable basename starts with `python` or `pypy`. This produces a distinct
process-coverage fact: profile completeness describes the reports that arrived,
while process coverage identifies observed interpreters that never loaded the
bootstrap. Non-Python children are not treated as missing profile evidence, and
an incomplete process table cannot produce complete coverage.

## Smallest stable core

The first stable boundary consists of:

1. one execution record;
2. logical or physical entities that participated in it;
3. events, including optional intervals;
4. explicit causal edges independent of timestamp ordering;
5. measurements associated with an entity or the execution;
6. optional bounded attachments for selected logs or raw source evidence;
7. source and confidence metadata that preserve uncertainty.

The core deliberately does not contain Kubernetes, OTel, RunDiff, BatchScope,
or Proofline types. Those packages translate into or query the core records.

## Artifact decision

Version 1 `.runpack` files are SQLite databases. SQLite is already available in
Python, is a documented open format, supports incremental transactional writes,
and handles indexed time-range and aggregate queries without materializing all
events as Python objects. It also keeps the first end-to-end slice free of a
runtime dependency.

The tradeoffs are explicit:

- SQLite pages are not byte-for-byte deterministic even when logical rows are.
- A database is less compressible than columnar Parquet for large measurements.
- Concurrent writers require coordination through SQLite's locking model.

Those costs are preferable to committing now to a ZIP manifest, JSONL, Parquet,
and an embedded query engine simultaneously. The schema separates high-volume
measurements from events, so a future schema version can store measurement
partitions as Parquet without changing analysis concepts. Raw source telemetry
is optional and never required for core queries.

Every artifact contains a schema version, producer version, execution row, and
normalized tables. Readers reject unsupported major schema versions and accept
additive minor versions. Version 1.1 adds optional attachments while version 1
core tables remain readable. Runpacks use SQLite's DELETE journal mode so a
completed artifact is one portable file; readers reject WAL-mode databases that
may depend on unshipped sidecars. Executable schema triggers are also rejected
before inspection or enrichment. The exact read and write support rules are in
the [compatibility policy](compatibility.md).

Each logical inspection, comparison, UI payload, and contract verification uses
one SQLite read snapshot per artifact. Size checks, normalized facts, and final
verdicts therefore cannot combine different committed versions of one runpack.
Enrichment is held to the same rule: it copies one validated, descriptor-bound
source snapshot into a private artifact, then derives source-dependent adapter
facts from that copy before mutation. Replacing the source pathname cannot
combine one execution generation with correlations or time-window decisions
from another.

Explained Proofline generation also streams SHA-256 over that same open file
descriptor and records the byte size for both snapshots. When the retained UI
loads the report, it opens and hashes each supplied runpack once, verifies those
bindings, and uses the same descriptor-bound SQLite snapshots for RunDiff and
policy replay. A pathname replacement therefore cannot move validation,
identity, analysis, and replay onto different file generations. Ordinary
non-explained output does not compute or expose these byte identities.

## Package direction

The initial repository uses one Python distribution with internal packages. It
can be split into workspace distributions after package boundaries are proven:

```text
capture adapters --> core model/storage <-- analysis packages
                                         <-- CLI/API presentation
```

Core cannot import capture or analysis code. Analyses may share query helpers,
but they must return structured facts before rendering prose. LLMs and hosted
services are outside the core and are never required for capture or analysis.

The distribution enforces that direction as four internal layers:

```text
foundation <- adapters <- analyses <- presentation
```

Foundation owns the normalized model, artifact and storage safety,
serialization, and shared support. Adapters translate external evidence.
Analyses derive structured facts. Presentation owns the package facade, CLIs,
reports, demo, and local UI. A layer may import itself or anything to its left;
the package facade is for external consumers and is not an internal dependency.
`tools/check_architecture.py` checks both this direction and internal import
cycles in CI. See [Python development](development.md) for the concrete module
classification and validation workflow.

## Scale strategy

Small executions can be returned as immutable Python records. Medium and large
executions are queried through indexed SQL, with graph subsets materialized only
for algorithms such as critical-path analysis. Initial work targets tens of
thousands to low millions of records; the schema avoids an architectural need
to load 100 million events into memory.

## Security and privacy

Capture is local by default. Environment values, stdout, and stderr content are
not stored by default because they commonly contain secrets. Version 1 records
only selected non-sensitive environment metadata plus output byte counts and
hashes. Bounded output content and raw OTLP input require explicit CLI flags;
normal inspect and UI paths do not render their content.

The normalized execution deliberately stores the exact command arguments and
resolved working directory. Users must keep credentials out of argv and path
names before sharing a runpack. Selected environment and output hashes are
behavioral identities, not a secrecy mechanism; low-entropy values may be
guessable.

OTLP log enrichment is an explicit content import, not a redaction boundary.
Log bodies and attributes become normalized event evidence and may be rendered
by queries or UI details. Sensitive log fields must be scrubbed before import or
before the enriched runpack is shared.

Local capture identifies only these selected environment variables by SHA-256,
never plaintext: `CI`, `CUDA_VISIBLE_DEVICES`, locale/timezone settings,
thread-pool sizing variables, and `PYTHONHASHSEED`. The capture tool's Python
implementation and version are stored separately as non-secret controller
metadata; they are not presented as the workload runtime.

Text reports escape terminal control characters in artifact-supplied names,
attributes, commands, and paths. Machine-readable JSON preserves the normalized
values. Attachments remain opaque and are limited to 64 MiB each and 256 MiB in
aggregate per runpack.
