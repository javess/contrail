# Contrail

**Capture one execution. See where time went, what changed, and which runtime contract broke.**

Contrail is a local-first toolkit for finite jobs and tests. It records execution
evidence in a portable `.runpack`, then answers three different questions over
the same artifact:

| Product | Input | Question |
|---|---|---|
| **BatchScope** | one run | Where did the wall-clock time go? |
| **RunDiff** | two runs | What changed in runtime behavior? |
| **Proofline** | two runs + a contract | Did the change violate an invariant? |

Here is an abridged BatchScope analysis of the built-in demo candidate; measured
durations vary by machine:

```text
BATCHSCOPE
run:   demo-candidate

Bottleneck
  serialized_stage (90%)
    result-aggregation ran at concurrency 1 for …

Observation
  compute completed with 20 / 100 work items remaining
  … of post-compute wall time followed
```

The workers can finish quickly while the logical job remains slow. BatchScope
keeps that downstream time visible instead of making you reconstruct it from a
trace, queue dashboard, worker logs, and infrastructure metrics.

[Source](https://github.com/javess/contrail) ·
[Issues](https://github.com/javess/contrail/issues) ·
[Security](SECURITY.md) ·
[Contributing](CONTRIBUTING.md)

## Try the complete workflow

Contrail is not on PyPI yet. From a source checkout, with Python 3.12–3.14 and
[`uv`](https://docs.astral.sh/uv/) installed:

```bash
git clone https://github.com/javess/contrail.git
cd contrail
uv sync --locked
uv run contrail demo
```

The offline demo exits successfully after creating `contrail-demo/` with an
adaptable Python workload, baseline and candidate runpacks, a contract, and an
artifact-bound Proofline report. The program result stays the same, but the
candidate performs 30 database writes instead of 3 and introduces a new
`metadata-db` dependency.

Use the commands printed by the demo, or run them directly:

```bash
# Review one integrated human-readable diagnostic.
uv run contrail report \
  contrail-demo/baseline.runpack contrail-demo/candidate.runpack \
  --contract contrail-demo/contract.yaml

# What changed between the two executions?
uv run contrail compare \
  contrail-demo/baseline.runpack contrail-demo/candidate.runpack

# Where did the candidate spend its time?
uv run contrail analyze contrail-demo/candidate.runpack

# Which runtime invariants failed? Expected exit status: 1.
uv run contrail verify contrail-demo/contract.yaml \
  --baseline contrail-demo/baseline.runpack \
  --candidate contrail-demo/candidate.runpack \
  --explain

# Inspect the retained evidence locally.
uv run contrail serve contrail-demo/baseline.runpack \
  --compare contrail-demo/candidate.runpack \
  --proofline-report contrail-demo/proofline-report.json
```

`contrail report` evaluates BatchScope, RunDiff, and optional Proofline claims
against the same opened runpack snapshots. It exits `0` when the diagnostic is
produced even when claims fail; use `contrail verify` when the exit status must
gate CI.

## Debug the job, not just the services

Distributed tracing answers what happened across observed spans and services.
That evidence is necessary, but one logical job may cross queue waits, trace
IDs, retries, workers, fan-out/fan-in, and post-compute processing.

BatchScope asks a narrower question: **what happened to this finite run?** It
derives an evidence-labelled lifecycle, critical path, causal waiting,
throughput, remaining work, and deterministic bottleneck classifications. It
does not replace OpenTelemetry or a workflow engine; it analyzes normalized
evidence from them.

When domain boundaries are absent from the source telemetry, the optional
Python annotation API makes the logical work explicit:

```python
from runtime_tools import runtime

with runtime.run("daily-export", total_work=100):
    with runtime.stage("compute", phase="compute", concurrency=16):
        runtime.progress(completed=80, total=100, series="rows")
    with runtime.stage("result-aggregation", phase="draining", concurrency=1):
        runtime.progress(completed=100, total=100, series="rows")
```

No LLM is required. Findings are derived from timings, causal edges, progress,
concurrency, failures, and dependency evidence preserved in the runpack.

## One artifact, three products

The integrated report leads with runtime outcome, the strongest candidate
bottleneck, and contract totals, then includes each product's detailed evidence.
Pass only the two runpacks to omit contract evaluation, or add `--contract` to
include Proofline failures and their supporting RunDiff facts.

### BatchScope: explain one execution

```bash
uv run contrail record --name batch-drain -- \
  python examples/local/batch_drain.py
uv run contrail analyze batch-drain.runpack
```

BatchScope currently classifies serialized stages, dominant external
dependencies, operation stragglers, queue waits, retry amplification,
connection failures, material connection setup, high-rate connection churn,
exception-heavy Python control flow, and Kubernetes scheduling starvation. It
marks critical paths as inferred when clocks or causal evidence are incomplete. The
[representative Temporal example](examples/temporal/README.md) combines an
official-style Python OTel trace with exported history: spans expose a worker
straggler, while history supplies workflow duration, activity queue waits,
terminal outcomes, and attempt counts.

Choose one progressive capture level. Every level is zero-code: the target
command does not import Contrail or add annotations. Higher levels collect more
evidence and impose more overhead.

| Level | Evidence added | Workload effect |
| --- | --- | --- |
| `passive` | outcome, wall/CPU time, peak RSS, output identities | no injected observer; default |
| `process` | bounded process tree, per-process RSS and CPU samples | controller-side polling only |
| `sample` | process evidence, Python stack samples, subprocess, HTTP, and outbound connection boundaries with sampled callers | injected statistical sampler and boundary observer |
| `deep` | process evidence, every Python/native C call and Python exception-propagation event, subprocess, HTTP, network, and supported database/cache/queue/broker/executor/scheduler operation boundaries with exact callers | expensive, intrusive exact profiling and broad boundary observer |

For any POSIX workload, the process level adds bounded per-process RSS and CPU
samples without modifying the workload:

```bash
uv run contrail record --capture-level process --name pipeline -- ./run-pipeline
uv run contrail analyze pipeline.runpack
```

This is useful for shell pipelines, test runners, compilers, native programs,
and Python jobs that launch worker processes. It observes only descendants that
remain in the captured process group and live long enough to reach a 100
millisecond sampling poll. Detached and short-lived processes may be absent.

For an uninstrumented Python workload, statistical sampling provides zero-code
hotspot discovery without tracing every call:

```bash
uv run contrail record --capture-level sample --name unknown-job -- python workload.py
uv run contrail analyze unknown-job.runpack
```

The included uninstrumented worker-pool example exercises one shared function
in a parent and three Python children:

```bash
uv run contrail record --capture-level sample --name worker-pool \
  -- python examples/local/worker_pool.py
uv run contrail analyze worker-pool.runpack
```

Sampling inspects Python thread stacks every 10 milliseconds and retains
bounded aggregate counts and stack relationships. Reported durations are
statistical estimates, and the sampler can still perturb the workload. Use the
more expensive Deep Capture mode when exact call counts and call timings are
worth that additional distortion:

```bash
uv run contrail record --capture-level deep --name unknown-job -- python workload.py
uv run contrail analyze unknown-job.runpack
```

Deep also consumes CPython's native call, return, and exception events. This
gives built-ins and C-extension clients a generic zero-touch floor even when no
semantic adapter exists: file I/O, compression, locks, sleeps, and direct
native database-driver calls retain aggregate call timing, exception counts,
and Python-to-native call edges. Arguments, return values, and exception
messages are never retained.

Deep also counts Python exception propagation with `sys.settrace`. One raised
exception can produce an event in each Python frame it crosses, so these are
propagation events rather than unique failures. Per-line and per-opcode tracing
are disabled. The compatibility-preserving raw count includes interpreter
control flow such as successful coroutine completion. A second diagnostic
count excludes only events whose transient type identity is exactly the
built-in `StopIteration`, `StopAsyncIteration`, or `GeneratorExit`; custom
subclasses are not filtered. Exception types, values, messages, tracebacks,
arguments, and locals are never retained. This adds more observer cost and is
one reason Deep remains the explicitly expensive fourth level.

Deep also reports observer integrity. If workload code calls `sys.setprofile`,
`sys.settrace`, `threading.setprofile`, `threading.settrace`, or their all-thread
variants, BatchScope identifies the affected processes and downgrades the
relevant exact-call or Python-exception evidence instead of silently claiming
complete coverage. Hook arguments, values, and frame locals are not inspected.

```bash
uv run contrail record --capture-level deep --name python-exceptions -- \
  python examples/local/python_exceptions.py
uv run contrail analyze python-exceptions.runpack
```

```text
Deep capture
  observer integrity complete: no tracing-hook setter calls detected
  Python exceptions: 8 propagation events across 4 functions
  exception diagnosis: 8 non-control-flow events across 4 functions; 0 built-in iterator-control events filtered
  type identity inspected only for control-flow filtering; types, values, messages, tracebacks, arguments, and locals not retained
  line and opcode tracing disabled; propagation may count one error in multiple frames
  native calls: 209 calls across 68 functions, 5 exceptions
  native arguments, return values, and exception messages omitted

Python hotspots (including native calls)
  __main__.parse_record [application, python]  10 calls, 4 exceptions
  time.sleep [runtime, native]  self 50.0ms, total 50.0ms, 1 calls
  sqlite3.Connection.execute [runtime, native]  4 calls, 1 exception
```

When complete Deep evidence shows sustained exception-heavy application code,
BatchScope adds a conservative diagnosis without treating propagation events as
failures. The workload remains unmodified:

```bash
uv run contrail record --capture-level deep --name retrying-batch -- \
  python examples/local/retrying_batch.py
uv run contrail analyze retrying-batch.runpack
```

```text
processed=16 attempts=48 transient_failures=32 checksum=240

Bottleneck
  python_exception_churn (60%)
    __main__.load_with_retry recorded 32 non-control-flow Python exception
    propagation events across 16 calls (2.00 per call); built-in iterator
    completion is excluded and events are not unique failures
```

The diagnosis requires complete profile, trace-integrity, and exception-count
evidence with the control-flow filter present and no drops. It intentionally
stays silent for legacy or partial evidence, when fewer than 10 diagnostic
events occur, or when the rate is below 0.5 events per call. Normal `asyncio`
await completion is therefore visible in the raw compatibility count but does
not become an exception-churn finding; real exceptions raised around awaits do.

The dependency-free example deliberately uses direct `_sqlite3`, native file
objects, zlib, and a lock so it exercises the generic floor rather than a
library-specific adapter:

```bash
uv run contrail record --capture-level deep --name native-calls -- \
  python examples/local/native_calls.py
uv run contrail analyze native-calls.runpack
```

Each interpreter has separate 2,000-native-function and
10,000-native-relationship limits in addition to the existing Python limits.
Deep remains the explicitly expensive option: use Sample when statistical
Python hotspots are sufficient.

The sample and deep presets also enable process-tree observation. They inject a
standalone startup observer and follow inherited `PYTHONPATH` into ordinary
Python subprocesses. Interpreters started with isolated or disabled site
initialization may not load them; the runpack and BatchScope report make that
absence explicit. Arguments, return values, locals, exception values or
messages, and source content are never captured. Existing
`--observe-process-tree` and
`--instrument sample|deep` flags remain available as lower-level expert options,
but cannot be combined with a preset.

Those same zero-code modes automatically observe Python `subprocess.Popen`
boundaries. They retain only a sanitized executable basename, parent and child
PIDs, shell use, duration, and observed exit or launch outcome. Deep Capture
links each boundary to its exact initiating Python function. Sampling links a
boundary only when its existing 10 millisecond sampler observes that thread in
the active wait; fast calls remain explicitly unattributed. Command arguments,
locals, environment values, working directories, and subprocess output are
never retained:

```text
Automatic subprocess capture
  complete: 2 boundaries across 2 Python processes
  caller attribution complete: 2 / 2 boundaries across 1 callsites

Subprocess boundaries
  missing-tool  1.1ms, launch error FileNotFoundError, parent pid 412 [root]
    called by pipeline.launch [exact, 100%] at pipeline.py:18
  python  54.5ms, exit 0, parent pid 412 [root], child pid 414
    called by pipeline.launch [exact, 100%] at pipeline.py:18
```

Each interpreter retains at most 256 boundaries and the controller normalizes
at most 2,000, prioritizing failures and then longer calls. Truncation and
unfinished calls are explicit. RunDiff compares retained boundaries as normal
`subprocess.run` operations, while Proofline refuses operation-count claims
when subprocess evidence is incomplete. Normalized `python.callsite` events and
`launches` edges remain queryable but are excluded from operation counts, so
capture mechanics cannot look like application work.

The same observer automatically captures outbound `http.client` requests,
including `urllib.request`, plus HTTPX's default sync and async transports and
aiohttp. Optional adapters activate only when `httpcore` or `aiohttp` is
imported; Contrail neither imports nor installs either package. It records the
adapter, a sanitized HTTP method, scheme, numeric port, duration through
response headers, status or safe error class, and optional caller. The server
name, URL path and query, headers, request and response bodies, and credentials
are never written to evidence. Server identity is always reported as
`<redacted>`:

```text
Automatic HTTP client capture
  complete: 1 request across 1 Python process
  active adapters: httpcore.sync, stdlib.http.client
  caller attribution complete: 1 / 1 requests across 1 callsite
  server identity redacted; durations end at response headers

Outbound HTTP requests
  POST https://<redacted>:443  82.4ms, status 202, adapter httpcore.sync
    called by client.publish [exact, 100%] at client.py:24
```

Each interpreter retains at most 256 HTTP requests and the controller
normalizes at most 2,000, independently from the subprocess limits. Truncation,
unfinished requests, malformed evidence, and unattributed sampled calls remain
explicit. Custom HTTPX transports and clients that bypass the supported
request boundaries fall back to connection evidence when they use Python's
socket or asyncio transport APIs. HTTPX HTTP/1.1 and HTTP/2 requests share the `httpcore` adapter;
aiohttp redirects are one logical request at its session boundary.
Response-body download time is not included. Sample mode does not attribute
async callers because thread-local sampling cannot safely distinguish
concurrent tasks; Deep mode does. RunDiff compares retained
`http.client.request` operations, while Proofline refuses exact operation-count
or error claims when HTTP evidence is incomplete. Their `python.callsite`
provenance and `requests` edges stay out of operation counts and BatchScope
lifecycle analysis.

Run the self-contained local example without changing or importing Contrail in
the workload:

```bash
uv run contrail record --capture-level deep --name http-client -- \
  python examples/local/http_client.py
uv run contrail analyze http-client.runpack
```

To exercise the optional clients without adding them to this project:

```bash
uv run --with httpx --with aiohttp contrail record --capture-level deep \
  --name optional-http -- python examples/local/optional_http_clients.py
uv run contrail analyze optional-http.runpack
```

For protocol clients without a request-level adapter, the same observer falls
back to outbound stream connections. It wraps blocking `socket.connect` plus
asyncio TCP and Unix transport creation. This provides useful zero-touch
evidence for Python-transport database drivers, queues, caches, RPC clients,
custom HTTP transports, and direct sockets without claiming to understand
their application protocol. A pooled database client may therefore show one
connection for many queries; this is connection churn and latency evidence,
not a query counter.

Only the adapter, TCP or Unix transport, address family, numeric TCP port, TLS
request marker when asyncio exposes it, duration until connect/transport ready,
safe outcome or exception class, and optional caller are retained. Host names,
IP addresses, Unix socket paths, credentials, and exception messages are never
written. A supported HTTP adapter suppresses its nested socket observation, so
one HTTP request does not appear again as a connection:

```text
Automatic network connection capture
  complete: 2 connections across 1 Python process
  active adapters: asyncio.create_connection, stdlib.socket.connect
  caller attribution complete: 2 / 2 connections across 2 callsites
  server identity and Unix paths redacted; durations end at connection ready

Outbound network connections
  tcp://<redacted>:51843  0.2ms, connected, adapter stdlib.socket.connect
    called by __main__.write_cache_entry [exact, 100%]
  tcp://<redacted>:51843  0.3ms, connected, adapter asyncio.create_connection
    called by __main__.publish_queue_message [exact, 100%]
```

Run the self-contained example without changing or importing Contrail in the
workload:

```bash
uv run contrail record --capture-level deep --name network-connections -- \
  python examples/local/network_connections.py
uv run contrail analyze network-connections.runpack
```

Each interpreter retains at most 256 connection attempts and the controller
normalizes at most 2,000, independently from the request and subprocess
budgets. Raw nonblocking socket state machines, native clients that connect in
C or Rust, alternative event loops that replace these asyncio methods, and
datagram traffic remain outside this observer. Sampling may miss the caller of
a fast connection; Deep Capture attributes it exactly. RunDiff treats retained
`network.connect` events as operation evidence, and Proofline refuses exact
operation-count or error claims when connection evidence is incomplete.

BatchScope promotes three conservative connection signals into its leading
bottleneck section: any retained failed attempt, a successful setup lasting at
least 50 milliseconds and 25% of the run, and at least 10 attempts occurring at
five or more per second. Churn is intentionally advisory—“inspect pooling or
retry behavior”—because redacted physical connections cannot prove whether the
application should have reused them.

It also aggregates the full retained connection set by initiating callsite and
adapter before bounding the displayed result to 100 rows. This makes lifecycle
shape visible without reading every attempt:

```text
Network connection hotspots
  __main__.connect_with_retry [exact, 100%], adapter stdlib.socket.connect
    3 attempts: 1 connected, 2 failed, 0 unfinished
  __main__.ReconnectingQueueClient.publish [exact, 100%], adapter stdlib.socket.connect
    12 attempts: 12 connected, 0 failed, 0 unfinished
  __main__.publish_one [exact, 100%], adapter asyncio.create_connection
    8 attempts: 8 connected, 0 failed, 0 unfinished
  __main__.PooledCacheClient.__init__ [exact, 100%], adapter stdlib.socket.connect
    1 attempt: 1 connected, 0 failed, 0 unfinished
```

The self-contained scenario uses one pooled connection for three logical cache
operations, reconnects for every queue publish, retries two failures, and opens
eight connections concurrently. It imports no Contrail code:

```bash
uv run contrail record --capture-level deep --name protocol-client-shapes -- \
  python examples/local/protocol_client_shapes.py
uv run contrail analyze protocol-client-shapes.runpack
```

Name resolution and TLS are captured as their own zero-touch setup phases rather
than guessed from total connection time. Contrail wraps `socket.getaddrinfo`,
blocking `SSLSocket.do_handshake`, and asyncio's nonblocking
`SSLObject.do_handshake`. It records the adapter, DNS/TLS phase, duration,
completion or safe exception class, and optional caller. It never records the
queried hostname, resolved addresses, SNI value, certificate, credentials, or
payload. HTTP adapters still suppress duplicate socket connects, but their DNS
and TLS work remains visible as setup evidence.

```text
Automatic network setup capture
  complete: 6 phases across 1 Python process
  caller attribution complete: 6 / 6 phases across 4 callsites

Network setup hotspots
  DNS __main__.connect_sync [exact, 100%], adapter stdlib.socket.getaddrinfo
    1 call: 1 completed, 0 failed, 0 unfinished
  TLS __main__.main [exact, 100%], adapter stdlib.ssl.SSLObject.do_handshake
    1 call: 1 completed, 0 failed, 0 unfinished
```

The repository example performs successful blocking and asyncio TLS connections
against a local test server and imports no Contrail code:

```bash
uv run contrail record --capture-level deep --name network-setup -- \
  python examples/local/network_setup.py
uv run contrail analyze network-setup.runpack
```

Each interpreter retains at most 256 setup phases and the controller normalizes
at most 2,000 independently from connection, request, and subprocess budgets.
BatchScope diagnoses retained DNS/TLS failures and setup phases taking at least
50 milliseconds and 25% of the run. RunDiff compares `network.resolve` and
`network.tls_handshake` operations; Proofline refuses exact operation-count or
error claims when setup evidence is incomplete.

Deep Capture adds an explicitly expensive logical-operation layer for supported
database, cache, queue, broker, executor, scheduler, and inbound server
boundaries. The standard-library
adapters observe `sqlite3` connection/cursor execute, executemany, script,
commit, and rollback calls; application-originated blocking and asyncio queue
put/get calls; and `ThreadPoolExecutor`/`ProcessPoolExecutor` tasks from
submission until their future completes. Explicit `asyncio.create_task` and
`TaskGroup.create_task` calls, top-level `asyncio.ensure_future` calls, and
coroutines scheduled implicitly by `asyncio.gather` are observed from task
creation until completion. Existing Futures passed to `ensure_future` or
`gather` are excluded rather than counted again.
Library-only queue polling, including normal process-pool `queue.Empty` control
flow, is excluded. Dependency-free lazy adapters recognize these optional
public client methods when their package is already installed:

- SQLAlchemy sync and asyncio `Connection`/`Session` execute, commit, and
  rollback boundaries;
- Redis sync and asyncio commands and pipeline execution;
- Pika `BlockingChannel` publish and get calls; and
- aiokafka producer `send_and_wait` plus consumer `getone`/`getmany` calls.

Contrail records only the generic operation class, adapter, duration, outcome
or safe exception class, PID, and exact caller. SQL statements and parameters,
cache command names and keys, broker destinations and messages, returned rows,
queue items and identities, payloads, return values, and exception messages are
never serialized. Executor callables, arguments, successful results, and
exception messages are likewise excluded. Asyncio awaitables, task names,
context values, arguments, successful results, and exception messages are also
excluded. The WSGI adapter retains only status, request-to-response duration,
and the exact application caller: HTTP method, route, URL, headers, request and
response bodies, and client address are excluded. Nested adapters are
task-locally suppressed, so an `AsyncSession` or
Redis pipeline remains one logical operation rather than also counting every
lower client layer it invokes.

```text
Automatic logical operation capture
  complete: 10 operations across 1 Python process
  caller attribution complete: 10 / 10 operations across 5 callsites

Logical operation hotspots
  DATABASE execute __main__.run_database [exact, 100%], adapter stdlib.sqlite3.Connection
    2 calls: 1 completed, 1 failed, 0 unfinished
  QUEUE get __main__.run_blocking_queue [exact, 100%], adapter stdlib.queue.Queue
    1 call: 1 completed, 0 failed, 0 unfinished; max 60.1ms
```

The self-contained example runs an in-memory SQLite workload, a blocking queue,
and an asyncio queue without importing Contrail:

```bash
uv run contrail record --capture-level deep --name logical-operations -- \
  python examples/local/logical_operations.py
uv run contrail analyze logical-operations.runpack
```

Executor capture is also zero-code. A separate example submits three thread
tasks and two process tasks, including one failure, without exposing the
callables or their private arguments:

```bash
uv run contrail record --capture-level deep --name executor-tasks -- \
  python examples/local/executor_tasks.py
uv run contrail analyze executor-tasks.runpack
```

```text
Bottlenecks
  executor_operation_failures (90%)
    1 of 5 retained executor operations failed

Logical operation hotspots
  EXECUTOR task __main__.run_thread_pool [exact, 100%]
    3 calls: 2 completed, 1 failed, 0 unfinished
  EXECUTOR task __main__.run_process_pool [exact, 100%]
    2 calls: 2 completed, 0 failed, 0 unfinished
```

An executor interval begins immediately before `submit()` and ends when the
returned future completes. It includes executor queueing, process serialization,
worker execution, and result relay; it is not an isolated worker-runtime
measurement. Rejected submissions and cancelled futures retain safe
`RuntimeError` and `CancelledError` outcomes.

Async task capture is zero-code as well. The example creates three standalone
tasks, two structured-concurrency tasks, one `ensure_future` task, and two
tasks from coroutine arguments passed directly to `gather`, including one
failure:

```bash
uv run contrail record --capture-level deep --name async-tasks -- \
  python examples/local/async_tasks.py
uv run contrail analyze async-tasks.runpack
```

```text
Bottlenecks
  scheduler_operation_failures (90%)
    1 of 8 retained scheduler operations failed
  scheduler_operation_latency (80%)
    Scheduler task took 0.062s (26% of the run)

Logical operation hotspots
  SCHEDULER task __main__.run_create_tasks [exact, 100%]
    3 calls: 2 completed, 1 failed, 0 unfinished
  SCHEDULER task __main__.run_implicit_gather [exact, 100%]
    2 calls: 2 completed, 0 failed, 0 unfinished
  SCHEDULER task __main__.run_task_group [exact, 100%]
    2 calls: 2 completed, 0 failed, 0 unfinished
  SCHEDULER task __main__.run_ensure_future [exact, 100%]
    1 call: 1 completed, 0 failed, 0 unfinished
```

The task observer adds a payload-free completion callback and reads CPython's
stored exception slot only for a safe class name. It does not call
`Task.exception()`, so unhandled-task warnings retain their normal behavior.
The interval includes event-loop queueing and suspended await time; it is not
CPU time. Direct loop scheduling, direct use of the `asyncio.tasks` submodule,
custom task factories, and alternate event loops remain outside this semantic
adapter.

Inbound WSGI request capture is zero-code and does not replace the application
or handler. Deep's existing call observer recognizes the standard-library
`wsgiref` request lifecycle, associates the first application function with the
request, and retains the numeric response status. The example serves one 200
and one deliberately slow 503 response:

```bash
uv run contrail record --capture-level deep --name wsgi-server -- \
  python examples/local/wsgi_server.py
uv run contrail analyze wsgi-server.runpack
```

```text
Bottleneck
  server_operation_failures (90%)
    1 of 2 retained server operations failed

Logical operation hotspots
  SERVER request __main__.application [exact, 100%], adapter stdlib.wsgiref
    2 calls: 1 completed, 1 failed, 0 unfinished; max 70.9ms

Logical operations
  SERVER request  70.9ms, status 503, error HTTPStatusError
  SERVER request  1.8ms, status 200, completed
```

These durations include response iteration and transmission through the WSGI
handler; they are not isolated application CPU time. The initial adapter covers
the standard-library `wsgiref` handler. Gunicorn, uWSGI, Waitress, ASGI servers,
custom gateways, and replaced handler methods remain generic Deep profiler
evidence until they receive explicit semantic adapters.

This family has its own 256-operation per-interpreter and 2,000-operation
controller limits. BatchScope diagnoses retained database, cache, queue, broker,
executor, scheduler, and server failures and a single operation consuming at least 50 milliseconds
and 25% of the run.
RunDiff compares the normalized operations, while Proofline refuses exact count
or error claims when enabled logical evidence is incomplete. The layer is not
enabled in Sample mode: method wrapping and SQLite factory substitution are
reserved for the already intrusive Deep preset. Custom SQLite factories,
direct `_sqlite3` use, `queue.SimpleQueue`, native drivers, and optional clients
outside the explicit adapter list remain unclassified. Their Python calls and
CPython-visible native calls still appear in Deep's generic profiler, but do not
receive logical-operation classification, per-operation records, or specialized
diagnoses. Optional adapters follow documented public method paths without
importing or depending on those packages; an unsupported package shape is
reported as incomplete logical-operation evidence instead of being guessed.

When more than one interpreter executes the same function, BatchScope preserves
the aggregate hotspot and breaks it down by process. A unique process-observer
match adds the executable name and parent; missed, detached, short-lived, or
ambiguously reused PIDs remain explicit unmatched evidence rather than being
silently assigned to the wrong process.

The sample and deep presets also compare reporting PIDs with the controller's
process-tree evidence. BatchScope says whether every observed Python or PyPy
process reported, names workers that did not load the observer, and distinguishes
those gaps from non-Python children that were never expected to report.

Sample and deep capture synchronously register each interpreter at startup,
publish the first bounded aggregate after 50 milliseconds, and then checkpoint
every 500 milliseconds to the Contrail controller. If a worker calls `os._exit`,
is killed by a signal, or crashes, BatchScope retains the latest snapshot and
labels it partial instead of presenting it as final evidence. A process that
ends before 50 milliseconds is explicitly registration-only: Contrail can prove
that capture loaded, but does not invent hotspot evidence.
Stalled registration and checkpoint socket operations time out after 100
milliseconds and use the same atomic-file fallback.
If the retained snapshot used that fallback, BatchScope reports the affected
processes and failed socket wait instead of still labelling the run socket-only.

BatchScope also exposes snapshot message volume, workload-side serialization
and failed-publication time, and controller-side normalization duration and
ranking-database size, so zero-touch capture cost is visible in the artifact
rather than hidden.
At most 128 interpreter reports are retained. Larger worker pools keep the root
and a bounded set of workers, report the omitted process count, and remain
partial evidence instead of invalidating the whole capture.

Command-line capture uses a transient frontend and a separate capture worker.
This applies to `runtime record`, `contrail record`, `rundiff record`, and the
multi-run `proofline run` and `proofline search` workflows. The frontend sends
no evidence over its one-way liveness pipe; the worker owns the workload,
output relays, temporary state, worktree cleanup, and final publication. If the
frontend is killed while its terminal and output descriptors remain usable,
the worker finishes independently and the resulting runpack records
`capture.worker.client_disconnected: true`. An ordinary Ctrl-C is still an
explicit cancellation: it is forwarded to the worker, which terminates and
reaps the workload instead of publishing an unfinished run.

For a long capture that should never own the launching terminal, detach it
explicitly:

```bash
contrail record --detach --output nightly.runpack -- python nightly.py
```

The frontend returns a job ID immediately. Detached capture uses `/dev/null`
for stdin and privately retains at most 1 MiB from each of stdout and stderr:
the first 512 KiB plus the most recent 512 KiB. After a stream crosses that
bound, its middle is drained and discarded so the workload cannot block on a
full output pipe, while late errors remain available. Replay reports the exact
omitted-middle byte count. This retention is opt-in because workload output may
contain secrets.

If the frontend disappears, another local client can discover and reattach to
the worker lifecycle without a daemon:

```bash
contrail job list
contrail job status JOB_ID
contrail job wait JOB_ID
contrail job output JOB_ID
contrail job output JOB_ID --follow
contrail job cancel JOB_ID
```

`wait` returns the completed capture command's exit status. `cancel` writes to
the worker-owned private cancellation channel; the worker then gives itself the
same interrupt as frontend Ctrl-C, performs bounded cleanup, and reaches a
terminal state without trusting a reusable PID. `--format json` is available
for lifecycle commands. `output` replays the two retained streams to their
original stdout/stderr destinations and reports truncation. Add `--follow` to
replay the append-only head immediately and flush new head bytes while the job
runs; once the job is terminal, follow appends the stable retained tail and
reports any omitted middle. A one-shot `output` call made while the job is
active also limits replay to the stable head. Ctrl-C stops only that observer;
use `job cancel` when the capture itself should stop. The private same-user
registry retains at most
100 terminal jobs for up to seven days. It
stores operation names, worker lifecycle, disconnect status, exit status, and
up to four published artifact paths, including a successfully published
Proofline `--report`; it never stores the captured command or its arguments.
The same `--detach` behavior applies to RunDiff record and Proofline run/search.

After the workload exits, record commits a valid private recovery checkpoint
before profile normalization and final publication. If the Contrail controller
is then killed or publication fails, recover the retained
`.NAME.runpack.tmp-ID` file without rerunning the workload:

```bash
uv run contrail recover .unknown-job.runpack.tmp-ID \
  --output unknown-job.runpack
```

Recovery validates the checkpoint, finishes Sample or Deep normalization from
the frozen per-process snapshots, and still refuses to overwrite an existing
output. It covers capture-worker loss after workload exit. Loss of the capture
worker itself while the workload is still running has no trustworthy exit
outcome and is not presented as a recoverable run; this is distinct from loss
of the transient frontend, which the worker now survives.

Proofline applies the same level to both Git refs—and to every bounded search
example—so comparisons do not silently mix observation costs:

```bash
uv run contrail run contract.yaml --baseline-ref main --candidate-ref HEAD \
  --workload workload.py --capture-level process
```

### RunDiff: compare runtime behavior

```text
Outcome
  equivalent

Operation count changes
  db.write                  3 → 30 (+900.0%)
  metadata.lookup           0 → 1 (new)

New runtime dependencies
  python → metadata-db [calls]: 0 → 1
```

RunDiff compares outcomes, runtime, resources, operation counts and durations,
concurrency, dependencies, and causal structure without requiring stable event
IDs across executions.

### Proofline: enforce explicit contracts

```yaml
name: workload-regression
assertions:
  - type: candidate_exit_success
  - type: output_equivalent
  - type: forbid_new_dependency
    from: python
    to: metadata-db
  - type: max_operation_count
    operation: db.write
    relative_to: baseline
    factor: 1.2
```

Proofline exits `0` when every claim passes, `1` when a claim fails or cannot be
verified, and `2` for invalid input. Explained reports bind every verdict to the
exact runpack bytes and include the RunDiff facts behind the decision.

## Evidence sources

Every adapter normalizes into the same versioned execution model:

| Source | Current support |
|---|---|
| Local processes | POSIX capture with outcome, timing, resources, output identities, and optional process-tree sampling |
| Python logical work | Optional `run`, `stage`, `progress`, `event`, and `link` annotations |
| OpenTelemetry | Bounded OTLP/JSON trace and log import |
| Kubernetes | Bounded exported snapshot enrichment |
| Prometheus | Bounded HTTP API JSON response enrichment |
| Temporal | Bounded exported History protojson enrichment with OTel activity correlation |

```text
process / annotations / OTLP / Kubernetes / Prometheus / Temporal history
                                  ↓
                      normalized execution evidence
                                  ↓
                      portable SQLite .runpack
                                  ↓
                  RunDiff     BatchScope     Proofline
```

Runpacks are ordinary SQLite files. Capture is local by default, no account is
required, and Contrail does not upload artifacts or telemetry. Environment
values and stdout/stderr content are excluded unless explicitly requested.

## Current limits

This is a bounded `0.9.0` beta, not a hosted observability service or a claim of
production readiness.

- There is no PyPI release yet; use a source checkout or a locally built wheel.
- Local process capture requires Linux or macOS and Python 3.12–3.14.
- OpenTelemetry, Kubernetes, Prometheus, and Temporal inputs are exported JSON,
  not live collectors or API clients.
- Temporal enrichment accepts one History protojson `events` object. It derives
  workflow and activity lifecycle facts and correlates uniquely identified
  Python `RunActivity` spans; it does not infer application-specific phases or
  progress.
- BatchScope analyzes a finite runpack; it is not fleet-wide run search or a
  generic tracing UI.
- Reliable logical reconstruction requires causal links or stable correlation
  identifiers. Missing evidence stays explicit instead of being invented.

See the [architecture](docs/architecture.md),
[execution model](docs/execution-model.md),
[compatibility policy](docs/compatibility.md), and
[machine-output contract](docs/machine-output.md) for the durable boundaries.

## Contributing

Start with `uv run contrail demo`, then follow
[the contribution guide](CONTRIBUTING.md). Security issues should be reported
privately through [SECURITY.md](SECURITY.md).

Contrail is available under the [MIT License](LICENSE).
