# Changelog

## 0.9.0

Contrail 0.9 is the beta compatibility-freeze release ahead of 1.0.

### Fixed

- `contrail demo` now resolves its generated workload before launching it, so
  the documented default relative output directory works from a fresh checkout.
- BatchScope scopes an explicit progress series to its single common causal run
  instead of counting process teardown as post-compute drain time.

### Changed

- Deep Capture now recognizes standard-library `wsgiref` inbound requests with
  zero application changes. It retains request-to-response duration, numeric
  status, safe 5xx classification, and exact application caller while omitting
  method, route, URL, headers, bodies, and client address. BatchScope, RunDiff,
  Proofline, the public schema, installed-wheel smoke, and release benchmarks
  carry the new `server.request` evidence.

- `--capture-level passive|process|sample|deep` now provides one progressive
  zero-code capture ladder across `contrail record`, `rundiff record`,
  `proofline run`, and `proofline search`. Sampling and deep presets also
  enable process-tree evidence for both comparison arms; deep is explicitly
  identified as expensive every-call instrumentation. Existing lower-level
  observer flags remain compatible when no preset is selected.
- Deep Capture now consumes CPython native call, return, and exception profile
  events in addition to Python calls. Independently bounded aggregates expose
  native database-driver, file-I/O, compression, lock, and wait hotspots plus
  Python/native call edges without retaining arguments, return values, or
  exception messages. Semantic adapters remain the richer classified layer.
- Deep Capture now counts Python exception propagation events by function with
  line and opcode tracing disabled. One exception may count in every frame it
  crosses and one call may contain many events. The existing raw count remains
  unchanged, while an additive diagnostic count excludes exact built-in
  `StopIteration`, `StopAsyncIteration`, and `GeneratorExit` type identities so
  normal generator and coroutine completion does not look like application
  exception churn. Types, values, messages, tracebacks, arguments, and locals
  are not retained; additive metadata exposes completeness and drops, and an
  explicit overhead gate keeps this in the expensive fourth capture level.
- BatchScope now turns complete, reconciled Deep exception evidence into one
  conservative `python_exception_churn` diagnosis for sustained application
  control flow. It stays silent on dropped, partial, hook-displaced, legacy, or
  low-rate evidence, requires the control-flow filter, and explicitly says
  built-in iterator completion is excluded and per-frame events are not unique
  failures.
- Deep Capture now reports observer integrity when workload code calls public
  `sys` or `threading` profile/trace hook setters. Exact call/caller evidence or
  Python-exception coverage is downgraded independently, affected processes are
  counted, fork/checkpoint behavior is preserved, and hook callables, values,
  arguments, and locals remain outside the artifact.
- Python sampling and Deep Capture aggregates now retain bounded per-process
  contributions. BatchScope uniquely correlates their PIDs with process-tree
  identities, labels root and descendant work, and leaves missed or ambiguous
  processes explicitly unmatched; duplicate or inconsistent profile evidence
  is rejected rather than guessed.
- Sampling and Deep Capture now report process coverage separately from profile
  completeness. Worker pools show how many observed Python or PyPy processes
  loaded the observer, identify workers that disabled startup, and do not treat
  non-Python children as missing profile evidence.
- Sampling and Deep Capture now synchronously register each interpreter,
  publish the first bounded aggregate after 50 milliseconds, and checkpoint
  every 500 milliseconds to the controller over a private Unix socket, with
  atomic-file fallback. Abrupt exit and fatal signals retain the latest
  per-process evidence as explicitly partial; sub-50ms exits are identified as
  registration-only instead of being confused with disabled capture. Hot-path
  calls and samples do not perform IPC.
- The snapshot controller now retains bounded message, payload, and
  workload-serialization metrics, including a periodic-checkpoint subset that
  excludes registration and final shutdown. Pools above 128 reporting
  interpreters retain bounded partial evidence and an explicit omitted-process
  count instead of invalidating the entire Python profile.
- Cross-worker Sample and Deep merges now retain functions and relationships by
  exact aggregate rank at the 20,000/50,000 parent bounds instead of filename,
  PID, or largest-worker order. A capped, automatically deleted controller-side
  SQLite workspace and bounded top-K heap avoid unmeasured sort scratch; every
  retained identity includes all process contributions, and release benchmarks
  exercise the truncating merge shape. BatchScope separately reports controller
  normalization duration and the ranking-database high-water mark, without
  presenting either as workload overhead.
- Registration and checkpoint socket operations now time out after 100
  milliseconds before atomic-file fallback, and the controller releases a
  stalled snapshot peer on the same bound. Final publication retains a longer
  one-second window after user code has stopped. Retained fallback snapshots
  identify their process and failed socket duration, and BatchScope reports a
  mixed transport instead of mislabelling recovery as socket-only.
- Capture now commits a self-describing post-exit recovery checkpoint before
  Sample or Deep normalization. `contrail recover CHECKPOINT --output RUNPACK`
  can finish or republish that run after controller loss without rerunning the
  workload; profile events and their completion metadata commit atomically,
  and pre-exit interruptions retain the existing cleanup behavior.
- CLI capture now runs in a separate per-command worker for `runtime record`,
  `contrail record`, `rundiff record`, `proofline run`, and `proofline search`.
  A payload-free liveness pipe lets the worker finish and publish after abrupt
  frontend loss, while Ctrl-C still cancels and reaps the workload. Runpacks
  retain additive worker-mode and client-disconnection provenance.
- `runtime job` and `contrail job` now list, inspect, wait for, and cancel those
  local workers without a daemon. A worker-held advisory lock serializes lost
  transitions with completion, while a worker-owned named pipe avoids
  PID-reuse races during cancellation. Bounded private state retains terminal
  status and published artifact paths, including Proofline reports, but never
  workload commands or arguments. Job JSON documents join machine-output
  format version 1.
- Worker-backed capture commands now accept explicit `--detach`. They return a
  local job identity immediately, use no terminal stdin, continuously drain
  output, and privately retain at most 1 MiB from each stream for `job output`.
  Job JSON reports retained sizes and truncation; attached capture retains no
  new output files.
- `runtime job output JOB_ID --follow` and its Contrail alias now replay the
  retained prefix and flush new retained stdout/stderr bytes until terminal job
  state. The observer reads by local stream offsets, does not expand the fixed
  retention bound, and can be interrupted without cancelling the capture.
- Detached output now divides the existing 1 MiB per-stream budget between a
  512 KiB append-only head and 512 KiB rolling tail. Late errors survive noisy
  starts, replay reports the omitted middle exactly when known, live follow
  appends stable tails at completion, and legacy head-only jobs remain readable.
- Sample and Deep capture now automatically retain bounded Python subprocess
  boundaries without workload code changes. Sanitized executable names,
  timing, PIDs, and exit or launch outcomes feed BatchScope, RunDiff, and
  Proofline, while arguments, environment, cwd, and output remain unrecorded;
  incomplete semantic evidence makes operation-count claims unverifiable.
- Subprocess boundaries now carry bounded caller causality. Deep Capture emits
  exact initiating functions; sampling attributes only waits observed by its
  existing timer. BatchScope renders the callsite, normalized `launches` edges
  remain queryable, and caller mechanics stay out of RunDiff operation facts.
- Sample and Deep capture now automatically retain bounded outbound HTTP
  boundaries with fixed server-identity redaction. The standard-library adapter
  covers `http.client` and `urllib.request`; dependency-free lazy adapters cover
  HTTPX's default sync/async `httpcore` transports and aiohttp. Safe adapter,
  method, scheme, port, response-header timing, outcome, status, and optional
  caller feed BatchScope, RunDiff, and Proofline without retaining hosts, URLs,
  headers, bodies, or credentials. Incomplete HTTP evidence makes exact
  operation-count and error claims unverifiable.
- Sample and Deep capture now add a protocol-agnostic outbound connection
  fallback for blocking Python stream sockets and asyncio TCP or Unix
  transports. Redacted transport, address family, numeric port, TLS request,
  connection-ready timing, outcome, and caller evidence cover Python-based
  database, queue, cache, RPC, and custom HTTP clients without retaining hosts,
  IP addresses, Unix paths, credentials, or exception messages. HTTP and
  Contrail snapshot sockets are suppressed to avoid duplicate or self-generated
  evidence; incomplete connection capture makes exact operation claims
  unverifiable.
- BatchScope now promotes retained connection failures, setup consuming at
  least 50 milliseconds and 25% of the run, and high-rate connection churn into
  explicit bottleneck findings. Churn remains an advisory pooling-or-retry
  signal because redacted connection evidence cannot identify a logical
  protocol operation. A release benchmark captures 200–256 real loopback
  connections and gates the complete Sample-to-passive workload-duration ratio.
- BatchScope now aggregates the full retained connection set by validated
  callsite and adapter before emitting at most 100 connection hotspots. Counts
  distinguish connected, failed, and unfinished attempts and retain total/max
  setup duration, making pooled reuse, per-operation reconnects, retries, and
  async fan-out visible without exposing endpoints or protocol payloads. A
  self-contained lifecycle example exercises all four shapes.
- Sample and Deep capture now retain independently bounded DNS resolution and
  blocking/async TLS handshake phases without hostnames, resolved addresses,
  SNI, certificates, credentials, or payloads. BatchScope aggregates callsites
  and diagnoses failures or material setup latency; RunDiff compares the new
  operations, and Proofline rejects exact claims when setup evidence is
  incomplete. A local TLS example covers both `SSLSocket` and asyncio
  `SSLObject` handshakes.
- Deep capture now adds an independently bounded, explicitly expensive logical
  operation family for standard `sqlite3`, `queue.Queue`, and `asyncio.Queue`
  boundaries, `ThreadPoolExecutor`/`ProcessPoolExecutor` future lifecycles,
  explicit `asyncio.create_task` and `TaskGroup.create_task` lifecycles,
  top-level `asyncio.ensure_future`, and implicit tasks created for coroutine
  arguments passed to top-level `asyncio.gather`, plus
  documented SQLAlchemy, Redis, Pika, and aiokafka public methods when those
  packages are present. BatchScope reports database, cache, queue, broker,
  executor, and scheduler hotspots, failures, and material latency; RunDiff and Proofline carry
  completeness. Library-only queue polling is excluded so process-pool
  `queue.Empty` control flow is not diagnosed as an application failure.
  Statements, parameters, command names, keys, destinations, messages, rows,
  queue items and identity, executor callables/arguments, asyncio
  awaitables/names/context, payloads, exception messages, and return values are
  never retained. Task-local suppression avoids
  double-counting ORM, async-proxy, and pipeline layers.
- `contrail record --observe-process-tree` adds bounded, controller-side RSS and
  cumulative CPU sampling for a workload's POSIX process group. Observed
  descendants become process entities without workload injection; missing,
  partial, and truncated evidence remain explicit, and combined observation
  modes are protected from misleading performance comparisons.
- `contrail record --instrument sample` adds zero-code Python stack sampling at
  a fixed 10 millisecond interval. Bounded aggregate samples and stack
  relationships feed a dedicated BatchScope hotspot section without claiming
  exact calls or altering critical-path intervals; cross-mode performance
  comparisons remain explicitly incomparable.
- `contrail record --instrument deep` adds zero-code, explicitly intrusive
  Python call profiling through a standalone startup bootstrap. Bounded
  function and call-edge aggregates feed a dedicated BatchScope hotspot section
  without altering critical-path intervals; mixed passive/deep performance
  comparisons are marked incomparable and related Proofline claims are
  unverifiable.
- `contrail report BASELINE CANDIDATE [--contract CONTRACT]` now composes the
  candidate BatchScope diagnosis, RunDiff changes, and optional explained
  Proofline results from one stable snapshot of each runpack. The intentionally
  human-only command is diagnostic; `contrail verify` remains the contract gate.
- The installed demo now includes compute and serialized result aggregation,
  prints direct RunDiff and BatchScope commands, and supports one coherent
  time/change/contract walkthrough over the same runpacks.
- BatchScope text output leads with its bottleneck and post-compute observation
  before showing detailed lifecycle, critical-path, and throughput evidence.
- BatchScope straggler analysis now covers repeated OTel server spans, with a
  representative Temporal Python fan-out export documenting what can and cannot
  be inferred from OTel alone.
- `enrich-temporal-history` adds bounded exported Temporal History protojson to
  a copied runpack, correlates uniquely identified Python activity spans, and
  preserves workflow duration, activity queue waits, outcomes, and attempt
  counts without storing raw payloads.
- BatchScope reports dominant queue waits and direct Temporal retry evidence.

### Stable candidate surfaces

- Portable SQLite runpack schema 1.1 and the documented 1.x read policy.
- JSON output format 1 for inspect, query, RunDiff, BatchScope, and Proofline.
- Documented CLI commands, exit statuses, and error-stream behavior.
- The additive `contrail` command, including the installed-wheel `contrail demo`
  walkthrough and flat capture, compare, verify, analyze, and debug workflow.
- Proofline contract parsing and deterministic verification semantics.
- The captured work annotation API: `run`, `stage`, `progress`, `event`, and
  `link`.
- The read-only `open_runpack` Python facade and normalized record types.

### Release scope

- Local POSIX capture on supported Linux and macOS Python versions.
- Bounded exported-JSON adapters for OTLP traces/logs, Kubernetes snapshots,
  Prometheus responses, and Temporal workflow history.
- Local read-only timeline UI.
- RunDiff, BatchScope, and Proofline over the same normalized artifact.
- Claim-to-evidence navigation from a local contract or a retained explained
  Proofline report, with policy replay and exact SHA-256/byte-size runpack
  bindings for newly generated explained JSON.
- A selectable, validated workload Python for Proofline experiments and
  counterexample search, held constant across both Git refs and every replay.
- Atomic, no-overwrite `verify/run --report PATH` publication so failed gates
  retain complete artifact-bound evidence without shell-redirection truncation.
- Pre-materialization runpack field, record, and aggregate limits shared by the
  public reader, analyses, contract verification, and timeline.
- A source-independent demo that creates and validates a complete failed-gate
  evidence bundle plus an adaptable workload/contract template without Git, a
  repository checkout, or network access.

Text presentation, the browser-internal payload, arbitrary query JSONL rows,
and undocumented Python internals are not frozen for 1.0.
