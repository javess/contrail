# Performance qualification

Contrail's input limits bound untrusted evidence; they are not throughput or
memory promises. A 64 MiB document can expand into very different normalized
graphs depending on its shape. Release qualification therefore uses fixed,
deterministic shapes and publishes both their record counts and resource budgets.

Validated runpack readers reject over-limit evidence before returning any
normalized records to Python. Each text or JSON field is limited to 4 MiB;
normalized text and JSON are each limited to 256 MiB per runpack. Record limits
are 1,024 manifest rows, one execution, 1,000,000 entities, 1,000,000 events,
2,000,000 causal edges, 1,000,000 measurements, and 100,000 attachments.
Attachment content remains limited to 64 MiB per item and 256 MiB in aggregate.
The complete SQLite file is limited to 2 GiB, including indexes, unused pages,
and trailing bytes. A 128 MiB SQLite value/encoded-row ceiling is installed
before structural and content validation as a backstop against values too large
to preflight safely. The bounded SQL surface may install a tighter cell limit
after this base guard.

Sampling is bounded to 2,000 function identities and 10,000 relationships per
observed Python process. Deep gives Python and native C identities independent
2,000-function and 10,000-relationship budgets. Both keep the existing 16 MiB
per-process report ceiling. Parent normalization accepts at most 128 reports,
64 MiB in aggregate, 20,000 merged functions, and 50,000 merged relationships
across both Deep identity kinds. Sampling runs at a fixed 10
millisecond interval and retains aggregate stack counts rather than individual
samples. Each merged function retains at most one contribution for each of the
128 accepted process reports, and profile metadata retains that same bounded
set of reporting PIDs. BatchScope emits at most 100 detailed unprofiled-process
coverage gaps while preserving the total gap count. Duplicate process reports
and contributions that do not reconcile with their aggregate are rejected.
Exceeding an in-process count budget produces explicit truncation; malformed or
byte/count-over-limit profile files are ignored with an `invalid`
instrumentation status. Sampling
still changes the observed process and has no zero-overhead guarantee. Deep
Capture can add substantially more time and memory overhead. Both modes are
excluded from ordinary benchmark promises.

Deep handles every CPython `c_call`, `c_return`, and `c_exception` callback in
addition to Python calls. It derives a bounded callable identity, updates the
thread's nested timing stack, and aggregates timing and exception count in
memory; it performs no hot-path IPC and never reads arguments, results, or
exception messages. Native child time is subtracted from Python self time,
which improves attribution but also changes hotspot ranking from older
Python-only Deep artifacts. The release harness compares a fixed-wait Passive
workload with the same Deep workload for 1,000 native compression calls in PR
and 5,000 in release, requires exact retained calls and privacy markers, and
uses a broad 5x whole-workload ratio ceiling. This intentionally qualifies the
most expensive preset rather than claiming isolated profiler-callback cost.

Deep also installs a CPython trace callback for Python exception propagation.
Every entered frame disables line and opcode events; exception events update
the same bounded function aggregate. The callback increments a raw count and
compares the supplied type object by exact identity with three static built-in
iterator-control types before incrementing a diagnostic count. It does not read
or retain the type name, type object, value, message, traceback, arguments, or
locals. A caught exception can produce multiple events in one call, and a
propagated exception produces one event in each crossed frame. The release
harness catches 1,000 exceptions in PR and 5,000 in release after a fixed wait,
then performs 200 ordinary awaits. It requires exact diagnostic counts, a
positive filtered-event count, zero drops, and message redaction, and applies
the same broad 5x whole-workload ceiling. The earlier PR reference run measured
1.565x Deep/passive workload time before the await qualification was added.
This is a regression guard, not a latency promise.

The same profile callback checks only call events for the public `sys` and
`threading` hook setters. It stores two bounded counters per process and performs
no frame walk, argument inspection, hot-path IPC, or replacement-hook
serialization. The full Deep benchmark cases remain the guardrail for this
additional integrity check rather than treating the check as free.

Exception-churn classification runs only in BatchScope after capture. It scans
the already bounded normalized Python aggregates, reconciles them with complete
exception and integrity metadata, and selects one candidate without adding a
workload callback or capture field. The release exception gate therefore checks
the diagnosis in addition to retention, privacy, completeness, and the existing
whole-preset overhead ceiling.

The shared semantic observer retains at most 256 subprocess boundaries in each
accepted Python process and adds them to the existing periodic profile
serialization. Initialization, communication, `poll`, and `wait` each perform
bounded Python bookkeeping under a re-entrant lock; no per-boundary IPC or
runpack write occurs. Deep caller attribution reads the profiler's existing
thread stack, while sampled attribution piggybacks on an existing sampling tick
during an active wait. Neither adds synchronous frame walking or another timer.
The controller retains at most 2,000 normalized boundaries across the same
128-process limit, selecting failures or unfinished boundaries before longer
successful calls. This makes memory and artifact growth fixed, but wrapping
`Popen` is not free and may be measurable in workloads that launch very large
numbers of very short processes. Dropped counts and callback errors remain
explicit in capture metadata.

HTTP boundaries use a separate 256-record budget in each accepted interpreter
and a separate 2,000-record controller limit. The wrapper performs bounded
bookkeeping at request start, header completion, response-header receipt, and
close; it does not read request or response bodies and does not time response
body consumption. Caller attribution reuses the same profiler stack or sampling
tick as subprocess attribution. The bound fixes memory and artifact growth, but
the method wrappers and lock are not free; sample and deep remain unsuitable
for claims that require observer-free HTTP latency.

Optional HTTP support adds one fixed meta-path finder with two exact module
targets. It does no polling and imports no client package; work occurs only when
the workload imports `httpcore` or `aiohttp.client`. The delegated loader is
restored before execution continues, and each supported boundary uses the same
bounded record path as `http.client`. Async adapters do not add a sampling
active-region marker, avoiding task-to-thread attribution errors.

Connection boundaries have their own 256-record per-interpreter and
2,000-record controller budgets. Blocking socket and asyncio transport wrappers
perform bounded allocation and lock-protected bookkeeping at connection start
and completion. Supported HTTP clients use task-local suppression so their
lower-level sockets do not consume both budgets. The snapshot transport is
suppressed as well. Asyncio duration can include name resolution and TLS setup
through transport readiness; raw blocking duration ends when `connect`
returns. These wrappers are not free and their timings are observer-affected,
especially in workloads that rapidly churn connections.

DNS/TLS setup has a separate 256-record per-interpreter and 2,000-record
controller budget. Name resolution adds one wrapper call and bounded record;
TLS adds one record per handshake, while asyncio reuses that record across
nonblocking want-read/want-write retries. No retry performs IPC. Setup records
share the existing periodic snapshot, so their payload and serialization cost
is included in reported publication metrics. The release connection benchmark
uses `socket.create_connection` and therefore exercises name-resolution capture
as well as connection capture; it is still a whole-preset comparison, not an
isolated wrapper-overhead claim.

Deep logical-operation capture has a fifth independent 256-record
per-interpreter and 2,000-record controller budget. Supported SQLite, queue,
executor, asyncio scheduler, WSGI server, SQLAlchemy, Redis, Pika, and aiokafka
boundaries allocate one bounded
record and take the observer lock at operation start and completion; no
operation performs IPC or inspects a statement, key, destination, message,
parameter, queue item, executor callable/argument, asyncio awaitable/name/context,
or successful result. A
`ContextVar` check suppresses nested adapters without serializing unrelated
asyncio tasks. Executor submission adds one Future callback; at completion it
reads only cancellation state or the exception object's safe class name.
Explicit and implicit asyncio task creation adds one callback that reads
cancellation state and CPython's stored exception slot without consuming the
exception or changing unhandled-task warnings. Gather uses a synchronous
thread-local origin only while converting coroutine arguments and does not
retain or inspect those arguments. WSGI uses the existing Deep call hook and
adds no application wrapper or completion callback; request start, status, and
completion each take the semantic observer lock once.
Blocking calls may receive caller attribution from the existing Deep stack,
while async calls never add a thread-local sampling marker.
This layer is deliberately absent from Sample mode because method replacement
and per-operation timing target the already expensive instrument-everything
preset. High-rate database, cache, queue, broker, executor, scheduler, or server microbenchmarks can
therefore see material additional distortion; retained operation timings are diagnostic
evidence, not observer-free latency measurements. The optional-client release
case gates exact bounded retention, secret omission, and a broad Passive/Deep
whole-workload ratio rather than claiming to isolate wrapper cost.
The executor release case submits 200 tasks in PR and the full 256-record bound
in release, requires exact completion/caller evidence and argument omission,
and applies the same broad 5x whole-workload ceiling. The first qualified PR run
measured 1.918x Deep/passive workload time.
The asyncio release case creates 200 tasks in PR and the full 256-record bound
in release across `create_task`, `TaskGroup`, `ensure_future`, and implicit
`gather` scheduling, requires exact application caller evidence and
awaitable/name/context omission, and uses the same 5x ceiling. The earlier
explicit-only workload measured 1.723x Deep/passive workload time; the broader
four-adapter PR workload measured 2.001x.
The WSGI gate drives 200 no-op requests in PR and 256 in release, requires
exact status/caller evidence and omission of route, header, body, and address
sentinels. The first 200-request run measured 9.276x Deep/passive whole-workload
time (0.560s versus 0.060s). Its deliberately broad 12x ceiling qualifies the
worst-case near-zero-work handler under the complete instrument-everything
preset; it is not a typical request-overhead promise.

When merged function or relationship cardinality exceeds its parent bound,
normalization retains an exact aggregate top-K instead of the first identities
encountered by filename/PID or the largest contribution from one worker.
Function ranking sums self/total time for Deep Capture or leaf/total sample
counts for sampling. Deep relationships sum total time and call count; sample
relationships sum sample count. A stable SHA-256 identity breaks ties. The
selected set is then aggregated in later passes so every retained identity
includes all reporting-process contributions, while dropped counters include
every omitted eligible contribution. Candidate scores spill into an
automatically deleted controller-side SQLite workspace capped at 256 MiB, with
an 8 MiB page cache. It is a mode-0600 file inside the private capture session.
Exact selection scans candidates through a bounded 20,000-function or
50,000-relationship heap, eliminating SQLite's separate sort scratch. The
controller also holds the at-most-64-MiB captured payload set, one decoded
report, and bounded selected maps. The cost is four JSON passes, temporary local
I/O, and post-run controller latency; no extra work is added to the profiled
call or sample hot path.

Profile metadata records the controller normalization interval from collector
shutdown through parsing, exact selection, and normalized event and edge
construction. It also records the maximum primary ranking-database page
footprint across function and relationship selection. The database is the only
disk-backed ranking scratch; the byte count excludes the bounded in-memory
top-K heap and filesystem metadata, and the duration excludes final runpack
insertion. These metrics describe post-workload controller cost, not workload
wall time.

Post-exit recovery reuses the incrementally written temporary runpack and does
not copy the database. Command-line execution does add one capture-worker
process per `record`, `proofline run`, or `proofline search` invocation. The
frontend and worker share only a one-way liveness pipe; one daemon worker thread
blocks on that pipe and no evidence payload crosses it. This adds process
startup, one file descriptor at each endpoint, and a blocked thread, but no
per-call, per-sample, or workload-side data path. The direct `record_process`
Python API remains in-process. After core evidence is committed, capture adds a
small pending metadata transaction. Profile events and completion metadata then
commit together, followed by one small complete-state update after raw snapshot
cleanup. These writes occur only after workload exit and are outside the
reported profile-normalization interval once event insertion begins, but they
do add final controller and filesystem latency before publication.

The CLI job registry adds small atomic JSON writes at worker start and finish,
when client loss is observed, and when artifact paths become available. Each
write is capped at 64 KiB and fsynced; each job also holds one advisory-lock
descriptor plus one named-pipe descriptor. A worker monitor blocks on that pipe
with a 100-millisecond stop check and otherwise wakes only for cancellation.
New-job cleanup scans at most 10,000 private registry entries and retains no
more than 100 valid terminal records for seven days. These are control-plane and
finalization costs, not workload hot-path instrumentation.

Explicit detached capture adds two anonymous pipes and two worker threads.
They continuously drain stdout and stderr, keep a 512 KiB append-only head and
512 KiB in-memory tail per stream, and discard the intervening bytes without
stopping the drain. A dirty tail is joined and rewritten under a file lock at
most twice per second and once at finalization; this caps checkpoint write
bandwidth near 1 MiB/s per active stream regardless of emitted volume. Normal
completion allows one second for inherited writers to close before marking the
affected stream incomplete. Drain and bounded-deque work still scale with
emitted output bytes after retention is full, but remain outside Python
call/sample observer hot paths.

An active `job output --follow` reader polls local state every 100 milliseconds
and reads only newly retained head bytes from independent stream offsets. It
reads the locked tail once after terminal state. It adds controller-side
filesystem opens and bounded reads but no workload descriptor, capture IPC,
extra retained byte, or profiler callback work.

Both Python modes synchronously serialize one empty registration snapshot at
interpreter startup, serialize their first aggregate after 50 milliseconds, and
then serialize every 500 milliseconds. The normal path sends snapshots over a
private Unix stream socket and the controller atomically retains only the
latest one for each PID; the workload performs no per-call or per-sample IPC
and no runpack writes. Socket setup or send failure falls back to an atomic
workload-side file. Registration and checkpoint socket operations use a
100-millisecond timeout before that fallback, while final publication retains a
one-second timeout after user code has stopped. The collector drops a stalled
read after 100 milliseconds so one incomplete peer cannot hold the receive loop
for a second. Serialization still pauses Python work under the GIL, socket
transfer still consumes CPU, and the fallback adds workload I/O, so moving
persistence to the controller does not make capture free. A session
accepts at most 1,000,000 snapshot messages, and each message shares the
existing 16 MiB per-report limit. The controller admits at most 128 reporting
PIDs and remembers at most 1,024 distinct omitted PIDs; higher cardinality is
bounded, partial evidence rather than an unbounded allocation or a wholly
invalid profile.

Socket transport metadata records aggregate message count, payload bytes, and
workload time spent constructing and JSON-serializing snapshots. A separate
checkpoint subset excludes startup registration and final shutdown, making the
reported maximum a closer bound on pauses imposed while user work was active.
It does not include socket send time or controller file I/O. File fallback and
legacy runpacks report these historical metrics as unavailable.

Each new workload report reserves 256 bytes for additive publication provenance.
When socket publication fails, the bootstrap appends a small object to the
already encoded report before its atomic file write. It records whether the
socket was configured and how long the failed socket attempt took without
re-serializing the function or edge arrays. BatchScope aggregates only the
latest retained snapshot per process. It does not claim that earlier overwritten
snapshots used no fallback, and it does not measure the subsequent atomic-file
write. An abrupt-collector test retained a complete Sample profile through this
path and recorded a 56.2-microsecond local connection failure.

Process-tree observation polls at a fixed 100 millisecond interval and retains
at most 2,000 process identities and 50,000 per-process resource samples. Each
sample produces one RSS and one cumulative CPU measurement. A host process-table
response is limited to 16 MiB and one second. Reaching a count limit is explicit
truncation; an unavailable or failed host process table is retained as observer
status rather than failing the workload. The controller launches `/bin/ps` for
each poll, so this opt-in mode consumes host CPU even though it injects nothing
into the workload.

The capture presets are progressive, not performance classes: `passive` enables
neither optional observer, `process` enables process-table polling, `sample`
adds the 10 millisecond Python sampler, and `deep` uses every-call profiling in
place of sampling. The `deep` preset is deliberately labelled expensive in
every execution CLI. Identical presets remove one source of measurement bias;
they do not make intrusive timing representative of an unobserved workload.

Run the quick gate with:

```bash
uv run python benchmarks/release.py --profile pr
```

Before a release, run the larger profile on a quiet Linux x86-64 host:

```bash
uv run python benchmarks/release.py --profile release --json \
  > release-benchmark.json
```

`benchmarks/budgets.json` is authoritative. Each case runs in a fresh subprocess
so peak resident memory is attributable to one workload. The harness normalizes
the different Linux and macOS `ru_maxrss` units. A timeout is four times the
declared time ceiling (and at least 30 seconds), so a severe regression fails
instead of hanging a release job.

The PR profile covers 10,000 OTLP spans, 20,000 Prometheus samples, 5,000
Kubernetes objects, 5,000 Temporal activities, a 5,000-function profile merge,
200 captured loopback connections, a 10,000-event causal chain, a 10,000-event
UI payload, and artifact-bound generation plus retained-report replay over two
10,000-event runpacks.
The release profile raises these to 100,000 spans/samples, 25,000 Kubernetes
objects, 50,000 Temporal activities, a truncating 22,000-input/20,000-retained
function merge with 60,000 input relationships, and 50,000-event
analysis/UI/report-replay cases. Its current hard
ceilings are 15–40 seconds and 1.5–2.5 GiB RSS per case. The deliberately
generous ceilings absorb shared-runner noise while still detecting accidental
quadratic work or unbounded materialization.

Artifact binding adds one linear, streaming SHA-256 pass over each baseline and
candidate runpack. It runs only when generating an explained report or loading
one for retained-report replay; non-explained verification and ordinary read
paths do not pay this cost. Hashing uses a fixed-size buffer, so auxiliary memory
does not grow with runpack size.

For calibration, the PR profile measured as follows on 2026-08-18 using macOS
Apple silicon and CPython 3.12. These observations are not promises; the JSON
budgets remain the pass/fail thresholds.

| Case | Records | Seconds | Peak RSS MiB |
| --- | ---: | ---: | ---: |
| OTLP import | 10,000 | 0.094 | 49.5 |
| Prometheus import | 20,000 | 0.090 | 43.9 |
| Kubernetes import | 5,000 | 0.092 | 50.1 |
| BatchScope analysis | 10,000 | 0.102 | 49.7 |
| UI payload | 10,000 | 0.192 | 63.3 |
| Artifact-bound retained report | 10,000 | 1.498 | 102.0 |

The same host measured the release profile at 0.971 seconds/180.0 MiB for
100,000 OTLP spans, 0.473 seconds/80.2 MiB for 100,000 Prometheus samples,
0.479 seconds/119.7 MiB for 25,000 Kubernetes objects, 0.741 seconds/119.4 MiB
for a 50,000-event BatchScope chain, and 1.301 seconds/163.4 MiB for the
50,000-event UI payload.

On 2026-08-19, the same class of macOS Apple silicon host measured Temporal
history enrichment at 0.226 seconds/70.5 MiB for 5,000 activities and 2.576
seconds/315.9 MiB for 50,000 activities (150,002 history events).

On 2026-08-20, a local five-run median calibration exercised one parent and one
Python child, 500,000 fixed arithmetic iterations, a 16 MiB child allocation,
and 350 milliseconds of deliberate wait time. Passive measured 416.6 ms;
process measured 409.8 ms (-1.6%, within run noise); sample measured 418.1 ms
(+0.4%); and deep measured 687.4 ms (+65.0%). This sleep-heavy shape shows the
capture-level ordering, not a general overhead promise: every-call overhead
grows with call rate and can be substantially higher on call-dense workloads.

A pre-registration five-run calibration on 2026-08-20 ran a 1.05-second
call-dense loop through one periodic checkpoint. Passive completed a median 17.44 million
iterations; sample with the controller socket completed 17.38 million (-0.4%)
and atomic-file fallback 17.49 million (+0.3%, run noise). Deep Capture completed
0.943 million iterations over the socket (-94.6%) and 0.936 million through the
file fallback (-94.6%). Socket-versus-file results were within 0.7% in both
modes. This supports the architectural boundary: controller-side persistence
improves crash survival and isolates file I/O, but cannot remove sampling,
serialization/GIL, or especially every-call profiling cost.

A later five-run paired calibration opened 200 sequential loopback TCP
connections. Passive capture had a 43.4-millisecond median workload interval;
Sample capture had a 74.0-millisecond median, and the median paired ratio was
1.592x. This short shape is dominated by the known Sample startup floor and
measures the complete preset—process observation, sampling, semantic wrappers,
snapshot publication, and shutdown—not the connection wrapper in isolation.
The release harness now requires every connection to normalize and applies a
broad 5x ratio ceiling so a catastrophic capture regression fails without
presenting this noisy micro-calibration as an end-user latency promise.

After adding synchronous registration and the 50-millisecond first checkpoint,
a 30-run interleaved empty-process calibration measured median execution times
of 17.3 milliseconds for process-only capture, 31.8 milliseconds for Sample,
and 30.2 milliseconds for Deep. Temporarily removing only the root registration
message measured 17.6, 30.8, and 32.0 milliseconds respectively; p95 changed by
less than 0.5 milliseconds. The Sample median moved by 1.0 millisecond while the
other differences reversed direction, so the registration cost is at or below
local run noise. The roughly 13–15 millisecond Sample/Deep startup premium is
the honest zero-touch floor for this empty-process shape and includes bootstrap,
observer-thread, final-report, and transport costs—not registration alone.

A 2026-08-20 cardinality calibration filled the workload-side aggregate near
its 2,000-function limit. Sampling a live 1,800-frame stack produced 1,801
functions, 1,800 relationships, and 371 KiB maximum payloads; its two periodic
checkpoints serialized in 6.4 milliseconds total with a 3.3-millisecond
maximum. Deep Capture over 1,900 generated functions produced 1,911 functions,
1,906 relationships, and 325 KiB maximum checkpoint payloads; two periodic
checkpoints serialized in 8.9 milliseconds total with a 4.7-millisecond
maximum. Independent monitor-thread runs observed median worst scheduling gaps
of 1.8 milliseconds for the sampling shape and 3.3 milliseconds for Deep,
versus 0.10 and 0.06 milliseconds without Python capture. These bounded pauses
do not yet justify a double-buffered aggregate, whose per-event synchronization
would tax the much hotter call/sample paths; revisit that decision if real
workloads approach the limits and exceed the recorded pause envelope.

The same host admitted 128 simultaneous Python reports in 0.9 seconds. Before
bounded PID admission, a 129th report made all Python evidence invalid. The
bounded controller retained 128 reports, omitted one explicitly, processed 131
snapshot messages (71.5 KiB), and recorded a 0.30-millisecond maximum
serialization time without collector errors.

The exact-aggregate merge gate on the same host normalized 5,000 functions and
15,000 relationships in 0.313 seconds at 74.6 MiB peak RSS. The embedded metric
recorded 0.311 seconds of controller normalization and a 2.289 MiB primary
ranking-database peak. Its release shape read 22,000 functions and 60,000
relationships, retained the documented 20,000/50,000 bounds, and completed in
1.400 seconds at 154.8 MiB peak RSS; its
embedded metric recorded 1.392 seconds and a 9.223 MiB ranking-database peak.
Compared with strongest-local in-memory ranking, the release shape traded about
0.27 seconds for a 20.7 MiB lower peak and fleet-wide-correct selection.

Two larger safety-shape calibrations stayed within the workspace ceiling. The
full 128-report/256,000-function candidate set occupied 46.4 MiB of input,
retained 20,000 functions, recorded 4.801 seconds of controller normalization
and a 31.508 MiB ranking-database peak, and peaked at 142.0 MiB RSS. Forty valid
2,000-function/10,000-edge reports occupied 38.5 MiB, considered 400,000
distinct relationships, retained 50,000, recorded 8.061 seconds and a 61.105
MiB ranking-database peak, and peaked at 168.1 MiB RSS. Both runs removed their
capture-local ranking files normally. These are controller normalization costs
after the workload exits, not capture hot-path overhead.

A deliberately stalled receiver held a maximum-size checkpoint send for 1.004
seconds before the timeout change and 0.104 seconds after it. The sender then
reported failure so normal publication could use its atomic-file fallback.

A small real Sample run recorded a 47.0-microsecond maximum periodic-checkpoint
serialization pause and 33.6 milliseconds of controller normalization. A Deep
run recorded 68.4 microseconds and 40.6 milliseconds respectively. Both used an
8 KiB primary ranking database. These one-off local observations illustrate the
separation between workload and controller cost; they are not performance
promises.

When changing a budget, attach before/after JSON results and explain the input
shape or algorithm change. Do not raise a budget solely to make a failing run
green. If a documented safety maximum cannot be processed within the supported
host envelope, preserve controlled rejection and lower the advertised limit
rather than relying on out-of-memory termination.
