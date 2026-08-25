# Product hypotheses

These are adoption assumptions, not product claims. Confidence changes only
when an experiment produces evidence.

## Engineers debug asynchronous work as logical runs

- **Hypothesis:** Engineers naturally start with a business job or run ID and
  want its execution reconstructed across service and infrastructure boundaries.
- **Evidence for:** BatchScope can already turn explicit run, stage, progress,
  causal, OTel, and Kubernetes evidence into one finite analysis.
- **Evidence against:** No external user interview or real incident has yet
  shown that a run ID is the identifier users reach for first.
- **Confidence:** Medium-low.
- **Experiment:** Observe five investigations involving Temporal, Celery, or a
  queue-based pipeline; record the first identifier and dashboards used.
- **Result:** Pending.

## Post-compute latency is a painful missing explanation

- **Hypothesis:** “Compute finished, but the job remained slow” is common and
  valuable enough to motivate installation.
- **Evidence for:** The local demo now distinguishes compute completion from a
  serialized result-aggregation drain and reports the outstanding work.
- **Evidence against:** The demo is deliberately constructed; it proves the
  analysis, not the frequency or severity of the problem in real systems.
- **Confidence:** Medium-low.
- **Experiment:** Ask ten platform engineers for a recent job where elapsed time
  materially exceeded compute time, then replay one incident through a runpack.
- **Result:** Pending.

## Existing telemetry can reconstruct a useful run safely

- **Hypothesis:** OpenTelemetry plus workflow and infrastructure identifiers
  provide enough causal evidence for useful reconstruction without intrusive
  instrumentation.
- **Evidence for:** A representative Temporal Python OTel export plus History
  protojson now reconstructs workflow duration, six activity lifecycles and
  queue waits, six span correlations, a compute straggler, and an attempt-2
  aggregation without inventing identifiers.
- **Evidence against:** The paired fixtures follow official schemas but are not
  captured production telemetry. History still supplies no application-specific
  phase or logical progress facts.
- **Confidence:** Medium-low.
- **Experiment:** Run the same adapter against a captured Temporal fan-out/fan-in
  execution and measure correlation coverage, setup time, and missing facts.
- **Result:** Partial positive. OTel plus history covers causality, activity
  timing, queues, outcomes, and attempts; explicit annotations remain necessary
  for phase and progress semantics.

## Zero-touch capture provides useful first evidence

- **Hypothesis:** A user will get enough value from one unmodified command to
  justify deeper integration later.
- **Evidence for:** Controller-side process-tree observation can discover a
  parent and child, retain bounded RSS and cumulative CPU samples, and explain
  their resource peaks without importing Contrail into the workload. Optional
  Python stack sampling and exact call profiling add progressively richer
  evidence from the same record command, including each process's contribution
  when parent and child execute the same function identity. Process coverage
  distinguishes a complete worker pool from an observed interpreter that never
  loaded the observer. Synchronous registration distinguishes a sub-50ms crash
  from an observer that never loaded; an early checkpoint and periodic
  aggregates preserve bounded hotspot evidence when an interpreter exits
  without running shutdown handlers. Large worker pools retain 128 process
  reports and name bounded overflow instead of converting useful evidence into
  an invalid profile; embedded transport metrics make the resulting observer
  cost independently inspectable. At the parent merge limits, useful hotspot
  retention uses exact fleet-wide aggregate rank, is deterministic across
  worker/PID order, and keeps every process contribution for a retained
  identity. Workload checkpoint serialization and controller normalization are
  reported separately, so the first useful result also explains where the
  zero-touch observer spent its time. A stalled controller no longer holds a
  registration or periodic checkpoint socket operation for a full second; the
  bounded attempt preserves the existing atomic-file fallback. If that fallback
  is the retained snapshot, its affected PID and failed socket wait survive into
  BatchScope rather than disappearing behind a session-level transport label.
- **Evidence against:** Process-table polling cannot recover application phases,
  progress, queue semantics, short-lived processes between polls, or descendants
  that detach from the workload process group. Exact Python call tracing is too
  intrusive to be a safe default.
- **Confidence:** Medium-low.
- **Experiment:** Give five unfamiliar engineers only a target command and
  compare time-to-first-useful-fact and diagnostic usefulness across passive,
  process-tree, sampled, and annotated capture.
- **Result:** Partial positive. A controlled two-process workload yields useful
  topology and resource evidence with no workload changes. One four-level
  preset now applies consistently to direct capture, comparison experiments,
  and counterexample search. Sample and deep preserve root-versus-descendant
  hotspot attribution without workload changes. A four-process sampling run
  reports four-of-four worker coverage, while a `-S` worker is called out as an
  explicit one-of-two gap. `os._exit` and SIGTERM retain explicitly partial
  sample/deep evidence through the controller. Standard-library outbound HTTP
  now yields redacted method, status, response-header timing, and exact or
  sampled caller evidence without workload imports. The same evidence model now
  activates lazily for HTTPX's default sync/async `httpcore` transports and
  aiohttp; a real isolated run captured all three calls with exact Deep callers
  and no retained host, path, query, header, or body sentinels. The observer now
  also falls back to redacted blocking-socket and asyncio connection evidence,
  covering Python-transport database, queue, cache, RPC, and custom HTTP clients
  at the physical connection boundary. A local sync/async example reports the
  connection-ready timing and initiating function while omitting host/IP and
  Unix path identity. BatchScope now turns failures, material setup time, and
  high-rate churn into conservative diagnoses; a 200-connection gate measures
  the complete Sample preset against passive capture. Callsite aggregation now
  distinguishes one pooled connection serving three logical operations from 12
  per-operation reconnects, two failed retry attempts, and eight concurrent
  async connections without retaining any operation payload. The same
  zero-touch floor now decomposes name resolution and TLS setup: a real local
  workload retains two DNS phases plus blocking client/server and asyncio
  client/server handshakes, while omitting hostname, resolved address, SNI,
  certificate, and payload. RunDiff and Proofline carry setup completeness
  independently from physical connection completeness. Deep's expensive
  instrument-everything tier now also captures supported `sqlite3`, blocking
  queue, and asyncio queue operations with exact callers while excluding SQL,
  parameters, rows, items, identity, and return values; a real local workload
  distinguishes nine logical operations from zero network connections. The
  same tier now consumes CPython's generic native-call stream as a fallback for
  unadapted clients: direct `_sqlite3`, file I/O, zlib, locks, and waits produce
  bounded timings, exception counts, and Python/native edges without retaining
  arguments, results, or exception messages. A real no-adapter workload
  retained 209 native calls across 68 identities and localized its dominant
  wait to `time.sleep`; the first 1,000-call comparison measured 1.526x Deep
  versus Passive whole-workload time. Explicit dependency-free lazy adapters
  now layer database/cache/broker meaning onto documented SQLAlchemy, Redis,
  Pika, and aiokafka public methods; an API-shaped sync/async matrix retained 32
  logical operations across 11 adapters, classified six failures by safe class,
  preserved public signatures and loaders, suppressed nested layers, and
  retained none of its statement/key/destination/message sentinels.
  Standard thread and process executors now add another zero-code semantic
  layer: a real fan-out retained five Future lifecycles with exact submitting
  callsites, one safe failure, and submission-to-completion timing while
  omitting callables, arguments, results, and exception messages. RunDiff saw a
  controlled task increase from five to seven and Proofline rejected it against
  a 1.2x count contract. Library-only process-pool queue polling is filtered so
  expected `queue.Empty` control flow is not reported as application failure.
  Asyncio task fan-out now has the same zero-code semantic floor: a real
  workload retained eight `create_task`/`TaskGroup`/`ensure_future`/`gather`
  lifecycles with exact application callsites, one safe failure, and
  creation-to-completion timing. Existing Futures passed to the implicit APIs
  were not counted twice. RunDiff saw eight tasks become ten and Proofline rejected the amplification,
  while awaitables, task names, context values, results, and exception messages
  remained absent. The observer leaves Python's unhandled-task warning intact.
  The same existing Deep call hook now recognizes standard-library `wsgiref`
  inbound requests without wrapping the application: a real local server
  retained one 200 and one 503 boundary, exact application caller, and
  request-to-response duration while omitting path, headers, bodies, and client
  identity. RunDiff saw two requests become three and Proofline rejected that
  amplification. A 200-request no-op gate retained every boundary but measured
  9.276x Deep/passive workload time, confirming that semantic value can be
  zero-touch while the instrument-everything tier remains explicitly expensive.
  Deep now also counts Python exception propagation without line/opcode events
  or retained exception payloads. It preserves raw compatibility counts while
  a second count excludes exact built-in iterator-control type identities. A
  normal 200-await workload produces no exception-churn finding, while genuine
  async exceptions remain diagnostic. A threaded zero-code workload retained 8
  real events in one function called twice, and fork plus abrupt-exit
  checkpoints preserved both counter semantics. The 1,000-exception gate
  measured 1.565x Deep/passive whole-workload time before await qualification,
  with no retained message sentinel.
  Deep now also detects public `sys` and `threading` profile/trace hook
  replacement. Real workloads downgrade exact-call and exception coverage
  independently, an abrupt-exit checkpoint preserves the warning, and a forked
  child restarts cleanly instead of inheriting its parent's displacement. A
  dependency-free retrying batch then produced the first zero-touch diagnosis
  from that evidence: 32 non-control-flow propagation events in 16 retry calls,
  with built-in iterator completion excluded, explicitly not 32 unique
  failures, complete integrity, and no retained message payload.
  External adoption and incident value remain untested.

## Small annotations are an acceptable fallback

- **Hypothesis:** Teams will add `run`, `stage`, and `progress` annotations when
  generic telemetry cannot express logical work.
- **Evidence for:** The API is optional, small, no-op outside capture, and makes
  the built-in diagnosis deterministic.
- **Evidence against:** It requires the workload environment to contain the
  Contrail package, and no external user has accepted that integration cost.
- **Confidence:** Low.
- **Experiment:** Give an unfamiliar engineer a queue worker and measure whether
  they can instrument one useful run in under thirty minutes.
- **Result:** Pending.

## One artifact can support an adoptable product suite

- **Hypothesis:** BatchScope, RunDiff, and Proofline are clearer as three
  questions over one runpack than as unrelated tools or separate data models.
- **Evidence for:** The installed demo now generates one baseline/candidate pair
  that all three products can analyze without recapturing the workload.
- **Evidence against:** A broad suite can obscure the single strongest reason to
  install; the old README became dominated by Proofline details.
- **Confidence:** Medium.
- **Experiment:** Run a five-second comprehension test on the new README and ask
  readers to name each product’s input and question.
- **Result:** Pending external review.

## Local-first setup is lightweight enough

- **Hypothesis:** A source checkout, Python, and uv are acceptable for the first
  ten technical evaluators.
- **Evidence for:** The offline demo completes in under a second after the locked
  environment exists and needs no service, account, or network collector.
- **Evidence against:** There is no PyPI release, live collector, or one-command
  integration with an existing distributed workload.
- **Confidence:** Medium-low.
- **Experiment:** Time five clean-machine evaluations from clone to first useful
  diagnosis and record every prerequisite or abandoned step.
- **Result:** Pending.
