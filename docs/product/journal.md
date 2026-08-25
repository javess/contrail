# Product journal

## 2026-08-19 19:13 BST

### What changed

The installed demo now works from its documented default relative directory,
generates an execution with a compute-to-result-aggregation handoff, and prints
direct RunDiff and BatchScope next steps. BatchScope scopes explicit progress to
its single causal logical run and leads its terminal report with a bottleneck
and post-compute observation. The README now presents the three products as
different questions over one runpack.

### What we learned

Passing unit tests did not prove the copy-pasted first-run command: tests had
only exercised absolute output paths. We also found that an explicit progress
series could measure post-compute time through process teardown instead of its
logical run, which contradicted BatchScope’s primary abstraction.

### Evidence

- The original `contrail demo` exited 2 from a fresh temporary directory.
- The new CLI regression failed before workload path resolution was fixed.
- The exact default command now exits 0 and retains its five promised artifacts.
- The demo candidate reports 20 of 100 items outstanding at compute completion
  and classifies result aggregation as a serialized stage.
- The focused BatchScope, demo, CLI, and example suite passes.

### What remains uncertain

No external evidence yet shows that engineers will install Contrail for this
diagnosis, that exported OTel data can reliably preserve real job identity, or
that the annotation cost is acceptable. Temporal support remains a product
hypothesis rather than an implemented integration.

### Next highest-value action

Test reconstruction against one realistic Temporal + OTel fan-out/fan-in
fixture, then turn the verified terminal flow into a short recording and measure
clean-checkout setup time with an unfamiliar user.

## 2026-08-19 19:22 BST

### What changed

BatchScope now considers repeated OTel server spans when detecting a straggler.
A representative Temporal Python OTLP example exercises five parallel compute
activities, one slow attempt, and downstream aggregation without adding a
Temporal dependency.

### What we learned

Official Temporal tracing preserves a useful causal activity graph, but a
resumable workflow cannot be represented as one long-running span. The Python
interceptor emits point spans for workflow lifecycle checkpoints. OTel also has
no standard job-phase or progress convention, so a full post-compute diagnosis
cannot be derived safely from these spans alone.

### Evidence

- The 45.1-second representative export imports as 15 spans and 14 causal edges.
- BatchScope identifies the 18.8-second compute activity against a 6.85-second
  cohort median.
- The critical path is explicitly inferred, and throughput remains unavailable
  rather than being fabricated.

### What remains uncertain

The fixture follows official emitted semantics but is not a real production
trace. It does not establish whether Temporal history plus OTel can be
correlated with low setup cost or whether users will accept explicit progress
annotations.

### Next highest-value action

Prototype a read-only Temporal history input against this same activity graph,
starting with a documented fixture rather than a live service or new runtime
dependency.

## 2026-08-19 19:33 BST

### What changed

Contrail can now enrich an existing runpack from bounded exported Temporal
History protojson. The adapter adds workflow and activity lifecycles, queue
waits, outcomes, and attempt counts; it correlates uniquely identified official
Python `RunActivity` spans and falls back to history execution intervals when
OTel is absent. BatchScope recognizes dominant queue waits and retry evidence.

### What we learned

History and tracing are complementary rather than interchangeable. History
provides the durable workflow interval and activity state transitions; OTel
provides worker execution spans and service causality. Correlating by workflow
and activity IDs avoids name-and-time guessing and keeps duplicate execution
intervals out of straggler cohorts.

### Evidence

- The paired fixture reconstructs six activities and six queue waits with six
  OTel correlations.
- The enriched analysis retains the 18.8-second compute straggler and reports
  that result aggregation completed on attempt 2.
- A history-only test produces a fallback operation from the recorded activity
  start and terminal timestamps.
- Malformed references and event-limit violations publish no output artifact.

### What remains uncertain

The fixtures are representative, not captured from a production Temporal
cluster. The adapter does not establish real-world correlation coverage or
eliminate the need for application-specific phase and progress annotations.

### Next highest-value action

Capture one real Temporal Python workflow with its OTel export and CLI history,
then measure correlation coverage and document the clean-room integration path.

## 2026-08-20 10:37 BST

### What changed

Capture now has an opt-in controller-side process-tree observer. It follows the
workload's POSIX process group, retains bounded process identities plus RSS and
cumulative CPU samples, and gives BatchScope a resource-hotspot view. The mode
does not inject code into the workload and composes with optional Python stack
sampling or exact call profiling.

### What we learned

Moving collection to a side thread reduces workload coupling, but does not make
observation free: the controller still polls the host process table and stores
samples. The useful boundary is therefore a capture ladder, not one magical
mode: passive outcome evidence, controller-side process observation, sampled
Python stacks, exact Python calls, then explicit semantic annotations.

### Evidence

- A parent process launching one child is reconstructed as two related process
  entities with per-process CPU and RSS evidence.
- BatchScope reports the observation method, sample coverage, and resource
  peaks without claiming application phases or causal work.
- Observer failure leaves the workload exit outcome intact and is recorded as
  unavailable evidence.
- RunDiff and Proofline reject timing comparison when process-observer modes do
  not match.
- The focused capture, CLI, RunDiff, and Proofline slice passes 169 tests.

### What remains uncertain

Polling can miss short-lived or detached descendants, host process tables may
be unavailable, and no external incident has established that process topology
plus resource peaks are sufficient first value. The observer currently targets
POSIX hosts rather than providing an equivalent Windows backend.

### Next highest-value action

Exercise the capture ladder against a realistic multi-process batch workload
and measure both diagnostic gain and overhead at each level before choosing a
zero-touch default.

## 2026-08-20 10:45 BST

### What changed

The capture ladder is now an executable product surface rather than a design
description. `--capture-level passive|process|sample|deep` selects progressive
evidence across direct recording, RunDiff recording, Proofline two-ref runs,
and bounded counterexample search. Sample and deep also enable process-tree
evidence; every CLI calls deep the expensive every-call level.

### What we learned

Independent expert switches are useful for implementation, but they make the
first-run decision unnecessarily architectural. A progressive preset communicates
the value/cost tradeoff while retaining the underlying switches for advanced
composition. Applying one preset to both comparison arms also prevents a user
from accidentally comparing different observer costs.

### Evidence

- All four presets produce the expected runpack provenance and observer set.
- Conflicting preset and low-level options fail before the workload starts.
- Proofline passes the selected level to both Git refs and every search replay.
- Direct capture, RunDiff, Proofline, and counterexample focused tests pass.
- On one five-run local calibration, medians were 416.6 ms passive, 409.8 ms
  process, 418.1 ms sample, and 687.4 ms deep; the first three were within 1.6%
  while deep added 65.0% on that sleep-heavy workload.

### What remains uncertain

The names and ordering have not been tested with external users. `sample` and
`deep` are Python-specific even though their process-tree portion works for any
qualified POSIX workload, and no measured overhead envelope yet supports making
anything above passive the default.

### Next highest-value action

Run the same representative workload at all four levels, publish observed
evidence and overhead side by side, then test whether a new user chooses the
right level without reading architecture documentation.

## 2026-08-20 10:59 BST

### What changed

Sampling and Deep Capture now retain bounded per-process contributions when the
same Python function identity appears in more than one interpreter. BatchScope
labels root and descendant work and enriches it with process names and parent
relationships only when process-tree evidence uniquely identifies the PID.

### What we learned

Merging a multi-process hotspot is useful for ranking, but it hides ownership:
a slow parent wait and slow child operation can look like one opaque function.
Keeping the aggregate while exposing its contributing processes preserves the
simple ranking and makes the process boundary inspectable. PID-only joins are
not trustworthy enough; missed and ambiguously reused PIDs must stay unmatched.

### Evidence

- A zero-code parent/child Deep Capture reported `__main__.shared_hot` as 597.5
  ms of aggregate self time across two calls: 310.1 ms in the root and 287.4 ms
  in the descendant, with both processes uniquely correlated.
- Equivalent sampling coverage attributes inclusive and leaf samples to the
  same root and descendant identities.
- Malformed nested totals preserve the valid aggregate but surface `invalid`
  attribution; duplicate process documents are rejected.
- The full suite passes 1,009 tests with two skips, and the built wheel passes
  isolated multi-process capture, BatchScope JSON, and package smoke tests.

### What remains uncertain

Process-table polling can still miss short-lived or detached interpreters, and
the text report intentionally expands only the top ten hotspots and five
process contributors each. The attribution has not yet been exercised on a
large real worker pool or tested with external users.

### Next highest-value action

Run sampling capture on a representative worker pool with repeated function
identities, then measure whether process attribution points an unfamiliar user
to the responsible worker without requiring annotations.

## 2026-08-20 11:07 BST

### What changed

Sampling and Deep Capture now report process coverage separately from the
completeness of the profile files that arrived. Capture retains bounded reporter
PIDs; BatchScope compares them with uniquely observed Python and PyPy processes,
lists workers that did not load the bootstrap, and does not expect non-Python
children to report. The repository includes an uninstrumented worker-pool
example that exercises the same function in one parent and three children.

### What we learned

“Profile complete” and “workload completely profiled” are different claims. A
valid root report can coexist with a child started under `python -S`; without a
coverage join, the former hid the latter. Conversely, applying a Python capture
level to a shell workload should not manufacture a failure merely because no
Python report exists.

### Evidence

- The reusable worker-pool example completed in 405.1 ms and produced 96
  samples of `shared_hot` across four processes. BatchScope reported four-of-four
  process coverage and broke the hotspot into one root and three descendant
  contributions.
- A parent plus one `python -S` child produced valid root samples but explicit
  partial coverage: one of two observed Python processes reported, and the
  child PID was named as not loading the sampler.
- Equivalent deep-capture coverage identifies the missing profiler, and
  shell-only sample/deep runs report complete zero-of-zero Python coverage.
- Legacy metadata without reporter PIDs remains readable with `unavailable`
  coverage; duplicate or inconsistent reporter IDs produce `invalid` coverage.

### What remains uncertain

Executable basenames are a diagnostic heuristic and can miss renamed or
embedded interpreters. Process-table polling can still miss a short-lived
worker entirely, so coverage is only as complete as the controller-side
observation it cites.

### Next highest-value action

Exercise abrupt worker termination and interpreter isolation modes, then decide
whether crash-resilient incremental transport to a side process provides enough
additional evidence to justify its complexity and runtime cost.

## 2026-08-20 11:23 BST

### What changed

Sampling and Deep Capture now checkpoint bounded aggregates every 500
milliseconds. A background thread sends snapshots over a private Unix socket to
the already separate Contrail controller process, which atomically retains only
the latest document per PID. The workload performs no per-call or per-sample
IPC. If socket setup or transfer fails, the same snapshot is atomically written
from the workload as a fallback. Checkpoint-only processes are normalized as
explicitly partial evidence.

### What we learned

The side-process boundary is valuable for crash survival and for moving file
I/O out of the workload, but it cannot make instrumentation free. The workload
still aggregates and serializes under the GIL, while Deep Capture's dominant
cost remains the every-call hook. Periodic aggregate transfer is therefore a
better boundary than streaming individual calls.

### Evidence

- Before checkpointing, a 650 ms sampled function followed by `os._exit(0)`
  produced no Python evidence. It now retains 36 samples of `abrupt_work`, marks
  one checkpoint-only process, and reports complete one-of-one process coverage.
- A SIGTERM Deep Capture retains all 200 completed `completed_work` calls and
  identifies calls still open at the latest checkpoint instead of claiming a
  final profile.
- Malformed socket input is rejected without losing the normal final report;
  unavailable socket setup falls back to a crash-surviving atomic workload file.
- In a five-run, 1.05-second call-dense calibration, sample/socket throughput was
  -0.4% versus passive and sample/file was +0.3% (noise). Deep/socket and
  deep/file were both -94.6%. Socket-versus-file results stayed within 0.7%.

### What remains uncertain

An interpreter that dies before its first 500 ms checkpoint still has no Python
hotspot evidence. Snapshot serialization can pause large aggregate maps, and
the transport has not yet been measured on a large multi-process worker pool or
under sustained controller backpressure.

### Next highest-value action

Measure checkpoint size and pause time on a larger worker pool, then evaluate an
early lightweight registration/first checkpoint so short-lived crashes become
distinguishable without increasing steady-state snapshot frequency.

## 2026-08-20 11:34 BST

### What changed

Sample and Deep Capture now synchronously publish one empty registration before
entering user code, publish their first evidence-bearing aggregate after 50
milliseconds, and retain the existing 500-millisecond steady-state cadence.
Normal final reports and later checkpoints overwrite the registration. Forked
children perform the same reset and registration, and socket failure retains
the registration through the atomic-file fallback. BatchScope distinguishes
registration-only processes from evidence-bearing checkpoint-only processes in
text and additive JSON fields.

### What we learned

The previous cadence had two different blind spots: a process ending before 500
milliseconds looked as if capture never loaded, while a process ending after a
few samples but before the first checkpoint lost useful evidence. An empty
synchronous registration solves the provenance problem without inventing a
hotspot, and one early asynchronous checkpoint solves most of the evidence gap
without increasing long-running checkpoint frequency.

### Evidence

- Before the change, an immediate `os._exit(0)` completed in 31.2 milliseconds
  and BatchScope reported no observed Python profile. The same workload now
  reports one registration-only process with its PID and zero hotspot claims.
- A 120-millisecond abrupt Sample run retains `early_work` samples, and the Deep
  equivalent retains all 40 completed calls; both are evidence-bearing partial
  checkpoints rather than registrations.
- A burst of eight 120-millisecond worker interpreters produced eight early
  checkpoints plus the parent final report, with no collector errors.
- In a 30-run interleaved empty-process A/B, enabling registration changed the
  Sample median from 30.8 to 31.8 milliseconds; Deep moved from 32.0 to 30.2
  milliseconds and p95 changed by less than 0.5 milliseconds, indicating that
  the incremental registration cost is within roughly 1–2 milliseconds of
  local run noise.

### What remains uncertain

Synchronous registration can still encounter controller backlog in very large
simultaneous worker bursts, and the first evidence checkpoint can pause a large
aggregate under the GIL. The current eight-worker burst proves the basic path,
not the 128-process report limit or sustained backpressure behavior.

### Next highest-value action

Exercise the full 128-process boundary and measure snapshot byte size plus
serialization pause across increasing function-cardinality shapes, then decide
whether large snapshots need incremental or double-buffered aggregation.

## 2026-08-20 11:47 BST

### What changed

The controller now admits at most 128 profile PIDs, consumes and counts valid
messages from later PIDs without persisting them, and emits explicit bounded
overflow metadata. Atomic-file fallback applies the same limit while retaining
the known root report. The snapshot protocol also carries its kind and
workload-side construction/JSON serialization duration, allowing BatchScope to
separate evidence-checkpoint cost from registration and final shutdown.

### What we learned

The process bound was previously a cliff rather than a budget: 128 reports
worked, while report 129 caused the loader to reject every profile file. That
behavior destroyed more evidence precisely when the workload was most
distributed. Graceful omission is a better bounded contract. At maximum
workload-side function cardinality, periodic serialization pauses stayed below
five milliseconds on this host, so adding locks or double-buffer swaps to every
hot-path call/sample would currently cost more complexity than the measured
pause justifies.

### Evidence

- A parent plus 127 immediate-exit workers retained all 128 reports in 0.9
  seconds. Adding worker 128 previously produced `status: invalid` with
  `generated deep-profile evidence exceeds its file-count limit`.
- The bounded controller now retains 128 of 129 reports, marks one explicit
  omission, consumes 131 messages/71.5 KiB, and has zero collector errors.
- A sampling shape with a live 1,800-frame stack produced a 371 KiB maximum
  payload; two periodic checkpoints took 6.6 milliseconds total and 3.6
  milliseconds maximum to construct and serialize. A final rerun measured 6.4
  milliseconds total and 3.3 milliseconds maximum.
- Deep Capture with 1,900 generated functions produced 325 KiB periodic
  payloads; two checkpoints took 8.0 milliseconds total and 4.4 milliseconds
  maximum. A final rerun measured 8.9 milliseconds total and 4.7 milliseconds
  maximum. The full final snapshot was 446 KiB but is excluded from the
  in-workload checkpoint maximum.
- Legacy or file-fallback capture reports transport metrics as unavailable, and
  inconsistent metrics are invalidated independently of the underlying profile.

### What remains uncertain

The 129-process burst did not sustain backpressure for minutes, and controller
file writes are not part of workload serialization time. Function maps near
the merged 20,000-identity parent limit can still make normalization expensive
even though each individual process is capped at 2,000 identities.

### Next highest-value action

Exercise a heterogeneous pool whose retained reports collectively approach the
20,000-function and 50,000-edge merge bounds, then make merge overflow retain
the most useful functions deterministically rather than whichever identities
arrive first.

## 2026-08-20 12:02 BST

### What changed

Parent normalization now snapshots the bounded report bytes once, selects
functions and relationships with a bounded deterministic top-K, and aggregates
the selected identities in later passes. Deep functions use strongest local
self then total time; sampled functions use strongest local leaf then total
counts. Deep relationships use total time then calls and sampled relationships
use sample count. A canonical identity breaks ties. All contributions from all
retained reports are then attached to every selected identity.

### What we learned

The parent output limits were bounded but not semantically stable: the same
reports could retain a cold function or relationship merely because its PID
sorted first, and a later retained identity could lose contributions from an
earlier report. Exact global-sum ranking would require retaining every candidate
or spilling an aggregation index to disk. Ranking the strongest per-process
signal gives a bounded, order-independent answer and protects a hotspot that is
important in any worker, while the later aggregation pass preserves its full
cross-worker total. This is deliberately not presented as an exact global
aggregate top-K.

### Evidence

- Regression fixtures reverse root/PID priority at a one-function limit. Both
  Sample and Deep retain the same hot identity and aggregate its contributions
  from both processes; the old implementation retained the first cold identity.
- Equivalent relationship fixtures retain the later high-cost edge and its
  earlier contribution in both modes; the old implementation retained the
  first low-cost edge.
- The PR benchmark normalizes 5,000 functions and 15,000 relationships in
  0.247 seconds at 78.8 MiB peak RSS on this host.
- The release benchmark reads 22,000 functions and 60,000 relationships,
  retains 20,000 and 50,000 respectively, and completes in 1.130 seconds at
  175.5 MiB peak RSS. A traced-allocation run peaks at 121.9 MiB.

### What remains uncertain

Strongest-local ranking can prefer one intense worker over a function whose
smaller contributions sum to a larger fleet-wide total. Three JSON passes also
make post-run latency linear with a larger constant, and the 64 MiB safety
maximum has not been calibrated on the slowest supported host.

### Next highest-value action

Capture a heterogeneous real worker pool near the parent limit and compare
strongest-local retention with an offline exact aggregate ranking. Only add a
bounded disk-spill aggregation path if the exact ranking changes diagnosis
often enough to justify its I/O, cleanup, and security surface.

## 2026-08-20 12:13 BST

### What changed

Function and relationship selection now sums candidate evidence across every
retained process before applying the parent top-K. The controller spills those
scores into an automatically deleted SQLite database with a 256 MiB page limit,
8 MiB page cache, file-backed sorting, no journal, and no memory map. Candidate
data uses bound parameters. Selected identities are still re-read from the
bounded immutable payload snapshot so their final per-process contributions and
dropped counters remain complete.

### What we learned

Strongest-local ranking was deterministic but not fleet-correct. In both Sample
and Deep fixtures, one worker's score of 100 displaced a function or edge that
scored 60 in each of three workers (180 aggregate). That is a diagnosis-changing
error for distributed batches: the steady fleet-wide cost is the better hotspot.
Exact aggregate selection needs state for every candidate, but compact digest
keys plus bounded local disk move that state off the controller heap without
adding any work to the captured process.

### Evidence

- All four adversarial regressions failed under strongest-local selection and
  now retain the 180-point fleet-wide function/edge over the 100-point spike.
- Existing worker/PID-order fixtures still retain the same useful identity and
  all of its process contributions in Sample and Deep.
- The 5,000-function PR merge completes in 0.338 seconds at 76.5 MiB RSS. The
  22,000-function/60,000-edge release shape completes in 1.426 seconds at
  158.7 MiB RSS, about 0.30 seconds slower and 17 MiB lower peak than the
  strongest-local implementation.
- A full 128-report set with 256,000 distinct function candidates completes in
  4.316 seconds at 132.0 MiB RSS. A valid 40-report set with 400,000 distinct
  edge candidates completes in 5.804 seconds at 170.0 MiB RSS.
- Forced one-page workspace exhaustion and aggregate integer overflow produce
  bounded `DeepProfileError` failures rather than an unbounded fallback.

### What remains uncertain

The 256 MiB workspace ceiling has substantial headroom over the calibrated
valid shapes, but temp-filesystem throughput and quotas vary across supported
hosts. An abrupt controller kill can also prevent ordinary application-level
cleanup, although SQLite's anonymous temporary database is not a durable
artifact.

### Next highest-value action

Exercise capture under controller backpressure and a constrained temporary
filesystem, then expose post-run normalization duration/workspace high-water
metrics if operators need to distinguish workload overhead from controller
finalization cost.

## 2026-08-20 12:22 BST

### What changed

Sample and Deep metadata now record controller normalization duration from
collector shutdown through normalized event and edge construction, plus the
largest primary SQLite ranking-database footprint. BatchScope validates and
renders the fields independently of workload-side snapshot metrics. Older
runpacks remain explicitly unavailable, while malformed or inconsistent nested
metrics become invalid without discarding otherwise valid profile evidence.

### What we learned

Checkpoint serialization and controller finalization have very different cost
shapes. Tiny real runs paused the workload for tens of microseconds per periodic
checkpoint but spent tens of milliseconds normalizing after exit. Exact ranking
at release and stress cardinalities takes seconds, yet the primary database
remains far below its 256 MiB ceiling. One combined "capture overhead" number
would obscure both facts.

### Evidence

- A real Sample run recorded a 47.0-microsecond maximum periodic-checkpoint
  serialization pause, 33.6 milliseconds of controller normalization, and an
  8 KiB ranking-database peak.
- A real Deep run recorded 68.4 microseconds, 40.6 milliseconds, and 8 KiB
  respectively.
- The 5,000-function PR merge recorded 0.337 seconds of normalization and a
  2.289 MiB database peak; the 22,000-function/60,000-edge release merge
  recorded 1.423 seconds and 9.223 MiB.
- The 256,000-function and 400,000-edge safety shapes recorded 22.551 MiB and
  64.070 MiB primary database peaks respectively.
- Legacy absence remains `unavailable`; an impossible peak above its declared
  limit becomes `invalid` while the underlying profile remains inspectable.

### What remains uncertain

The primary database measurement does not include SQLite's separate temporary
sort file, so it is not total temporary-disk high-water. The duration also
excludes final runpack insertion. Performance on constrained or remote temporary
filesystems remains uncalibrated.

### Next highest-value action

Exercise exact ranking with an intentionally constrained temporary filesystem
and controller backpressure. If operators need a hard disk-capacity prediction,
add a separately bounded sort-scratch measurement rather than broadening the
meaning of the primary-database metric.

## 2026-08-20 12:33 BST

### What changed

Exact profile ranking now creates its mode-0600 SQLite database inside the
private capture session and scans candidates through a bounded top-K heap. It no
longer asks SQLite for an external sort, so the capped database is the only
disk-backed ranking scratch and is removed after selection. Registration and
periodic checkpoint socket operations now time out after 100 milliseconds
before atomic-file fallback; the controller applies the same bound to a stalled
read. Final publication retains a one-second window after user code has ended.

### What we learned

The prior 256 MiB database ceiling did not bound SQLite's separate `ORDER BY`
scratch: its query plan explicitly reported `USE TEMP B-TREE FOR ORDER BY`.
Replacing that sort with a 20,000/50,000-entry heap makes the disk claim honest
at the cost of bounded controller memory and additional time at the
400,000-relationship stress shape. Separately, a listener that accepted but did
not read held both zero-touch bootstraps for the full one-second socket timeout.
The same maximum-size send now returns to fallback in about a tenth of a second.

### Evidence

- A regression checks that the active ranking query plan has no temporary
  B-tree, the database resides in the capture directory with mode 0600, exact
  selection is preserved, and cleanup leaves no ranking file.
- Stalled maximum-size Sample and Deep sends failed in about 1.003 seconds
  before the change; the calibrated Sample send now fails in 0.104 seconds.
- A partial client connection is released by the collector within the new
  100-millisecond receive bound.
- The PR merge records 0.311 seconds of normalization at 74.6 MiB RSS; the
  release merge records 1.392 seconds at 154.8 MiB. Their ranking databases
  remain 2.289 MiB and 9.223 MiB.
- The 256,000-function shape records 4.801 seconds, 31.508 MiB of database, and
  142.0 MiB RSS. The 400,000-relationship shape records 8.061 seconds, 61.105
  MiB, and 168.1 MiB RSS. Both leave zero ranking files.

### What remains uncertain

Atomic-file fallback still depends on the capture filesystem and may block at
startup if that filesystem itself is severely congested. A hard controller kill
can bypass normal workspace cleanup. The bounded heap increases normalization
latency at the largest edge shape, although it remains post-workload and within
the release envelope.

### Next highest-value action

Expose whether socket publication fell back and how long transport—not just
serialization—took, without adding per-call work. Then exercise abrupt
controller death so BatchScope can distinguish a healthy file fallback from
missing transport evidence.

## 2026-08-20 12:43 BST

### What changed

Every new Sample and Deep report now carries a publication-metrics version. If
socket publication fails, the workload appends a bounded fact to the already
encoded snapshot before its atomic file write: whether a socket was configured,
the failed socket duration, and the registration/checkpoint/final kind. The
loader aggregates the latest retained fallback by PID. BatchScope exposes the
result in text and JSON and labels a live-controller session with retained file
evidence as `mixed` rather than socket-only.

### What we learned

Session setup cannot describe how the retained evidence actually arrived. A
collector may accept registration and then die; the final workload file is
valid even though the session still owns a collector object. Recording the fact
inside that fallback snapshot makes the artifact self-describing without an
acknowledgement protocol, sidecar coherence, or a second function/edge JSON
serialization. It intentionally describes retained evidence, not overwritten
publication history.

### Evidence

- Sample and Deep regressions close the collector after registration. Both
  retain complete final profiles through workload-file fallback and report
  `transport: mixed`.
- The real Sample example retained nine thread samples, named its one fallback
  PID, and recorded a 56.2-microsecond failed socket attempt.
- A collector-free immediate exit records one fallback process with no socket
  attempt, distinguishing unavailable setup from a failed configured socket.
- Legacy metadata remains `unavailable`. Impossible counts and malformed
  workload fields become `invalid` independently of otherwise valid profile
  evidence.
- The focused Deep/Sample and BatchScope suite passes 132 tests.

### What remains uncertain

The retained fact does not include the subsequent atomic-file write duration,
and an earlier fallback can be overwritten by a later socket-success snapshot.
As workload-authored evidence, the PID and timing are not trustworthy against a
hostile same-UID process. A hard kill of the entire capture controller after the
workload exits can still prevent runpack publication even when profile evidence
survived.

### Next highest-value action

Make runpack assembly recoverable from the private capture session after a
controller restart, or explicitly emit a recovery bundle before normalization.
First determine whether that lifecycle can preserve the current one-command
interface without leaving unbounded or secret-bearing state behind.

## 2026-08-20 12:59 BST

### What changed

Capture now establishes a versioned recovery boundary after workload exit and
before profile normalization. The private temporary runpack already contains a
finished execution, output identities and attachments, resource measurements,
process observations, annotations, and the root process event. Its pending
metadata binds the frozen Sample or Deep session by device and inode and retains
the stopped collector's transport counters. Profile events and the metadata
claiming them commit together. `contrail recover CHECKPOINT --output RUNPACK`
can resume normalization or republish an assembled checkpoint without rerunning
the workload.

### What we learned

The existing incremental SQLite writer and atomic per-PID snapshot files were
already most of a recovery bundle. The missing property was a trustworthy
commit point. A permanent side process is not needed for post-exit loss: once
the root outcome exists, a single transactional state machine—pending,
assembled, complete—makes restart safe. Stopping the collector before that
checkpoint also makes its files and metrics stable. A side process would only
be justified for the different problem of controller loss while the workload
is still running.

### Evidence

- A regression forks a real Sample controller, waits until its post-exit
  checkpoint is committed, sends `SIGKILL`, and recovers the run. The result
  preserves complete sample evidence, socket transport provenance, and the
  original workload exit code.
- A simulated final hard-link failure retains the temporary runpack; both the
  Python API and CLI publish it on retry without overwriting an existing target.
- A transaction rollback regression proves derived events cannot commit without
  their corresponding execution metadata.
- Recovery also exposed and fixed stale private `__pycache__` directories left
  by both normal and restarted profile sessions.
- A standalone recovered Sample run retained nine of nine thread samples,
  three socket messages, the original exit code, and
  `controller_restart_recovered: true`; BatchScope selected
  `__main__.work` as the leaf hotspot.
- The focused capture/profile/CLI/compatibility slice passes 313 tests. The
  full suite passes 1,060 tests with two skips, the installed-wheel smoke
  passes, and every PR benchmark remains within budget.

### What remains uncertain

The recovery boundary begins only after the root workload exits. A hard
controller kill during execution still loses exit status, output digests,
resource usage, and any collector-only snapshots. Users must currently locate
the hidden checkpoint pathname themselves. Same-UID mutation and unsupported
remote filesystems remain outside the recovery guarantee.

### Next highest-value action

Prototype a separate capture worker that owns the workload, output pipes, and
runpack checkpoint while the CLI becomes a transient client. Measure whether a
small control pipe can make mid-run CLI/controller loss survivable without
changing workload exit semantics or adding hot-path instrumentation work.

## 2026-08-20 13:13 BST

### What changed

Command-line capture now hands ownership to a separate per-invocation worker.
The frontend retains only the write end of an anonymous liveness pipe; the
worker owns workload launch and reaping, output relays, collectors, temporary
state, Proofline worktrees, and publication. Runtime, Contrail, RunDiff, and
both Proofline execution workflows use the same primitive. Abrupt frontend loss
sets additive runpack provenance and no longer ends an active capture. Ctrl-C
still signals the worker and follows the existing bounded workload cleanup.

### What we learned

The side process changes resilience only when it owns the stateful boundary.
Sending evidence through another pipe would duplicate buffering and
backpressure without protecting the controller that still owns exit status and
publication. A payload-free liveness pipe is enough: kernel EOF reports client
loss, while all capture data stays on the existing workload pipes, snapshot
socket, and temporary runpack paths. The cost is one process, one blocked daemon
thread, and one pipe per CLI workflow, with no per-call or per-sample work.

### Evidence

- Killing the Runtime frontend with `SIGKILL` during a Sample workload still
  publishes a complete runpack, preserves relayed stdout, removes capture
  scratch, and records `client_disconnected: true`.
- Sending Ctrl-C to the Runtime frontend returns 130, terminates and reaps the
  workload, and leaves no runpack or temporary checkpoint.
- Killing a real Git-backed Proofline frontend during its baseline still
  completes both refs, emits the verification report, publishes both runpacks
  with disconnection provenance, and removes both isolated worktrees.
- Normal Runtime and RunDiff records report the same worker format with
  `client_disconnected: false`; branded Contrail uses the same installed worker
  module and keeps its command naming.

### What remains uncertain

The worker cannot preserve capture if the worker itself is killed before the
post-exit checkpoint. There is no job registry or reattachment protocol, and a
frontend whose terminal also disappears may leave unusable inherited output
descriptors. The extra process and blocked monitor thread are architecturally
off the workload hot path but have not yet been isolated in a startup-cost
benchmark.

### Next highest-value action

Add a small local job identity and discover/status surface so a new client can
find an in-flight worker after terminal loss, without turning Contrail into a
persistent daemon or broadening the runpack trust boundary.

## 2026-08-20 13:31 BST

### What changed

Every CLI capture worker now owns a random local job identity, an atomic private
state record, and an advisory lock held for the worker lifetime. `runtime job`
and `contrail job` can list, inspect, wait for, or cancel a worker after the
original frontend disappears. Runtime and RunDiff publish their runpack path;
Proofline publishes both retained arm paths. Wait returns the original capture
command status, while cancel reuses the bounded Ctrl-C cleanup path.

### What we learned

Lifecycle reattachment does not require a daemon or an output transport. The
worker-held lock answers the hard question—whether the exact job owner is still
alive—without trusting a reusable PID. The JSON state is only a discovery
index; the runpack remains the evidence boundary. Avoiding command and argument
storage preserves the existing default secret posture, while retained artifact
paths provide enough information to find completed evidence.

### Evidence

- A second Runtime client discovers a Sample capture after frontend `SIGKILL`,
  observes live/disconnected state, waits for exit 0, and receives the exact
  published runpack path.
- A separate cancel client interrupts and reaps a 30-second workload after its
  frontend is killed; the job reaches `complete` with exit 130 and no partial
  artifact or temporary runpack.
- A new client waits for a Git-backed Proofline worker, then receives both
  completed arm paths and client-disconnection provenance.
- RunDiff records a completed `rundiff record` job with its artifact, and a
  nonzero Runtime record proves `job wait` returns the original exit status.
- Version-1 golden documents and the packaged JSON schema cover both single-job
  and job-list output.

### What remains uncertain

The registry cannot replay stdout or stderr after their inherited descriptors
are gone, and it is intentionally not authenticated against same-UID mutation.
It is local and ephemeral: terminal state expires after seven days or 100 jobs.
A hard worker kill before the post-exit recovery boundary is reported as
`lost`; it still cannot manufacture a trustworthy workload outcome.

### Next highest-value action

Add an explicit detached-capture option with bounded private output spooling so
long jobs can start without a persistent frontend and later replay only the
operator-requested control output, without silently retaining workload secrets.

## 2026-08-20 14:06 BST

### What changed

Every worker-backed execution surface now accepts explicit `--detach`.
Runtime/Contrail record, RunDiff record, and Proofline run/search return a local
job identity immediately with no terminal stdin. The worker continuously drains
stdout and stderr into private job files, retains the first 1 MiB of each, and
discards later bytes without backpressuring the workload. `job output` replays
the two streams on demand, while job JSON reports retained sizes and
truncation. Attached capture still creates no output files.

### What we learned

Detachment needs output ownership, not just background process launch. Directing
the worker at regular files would make retention unbounded; abandoning inherited
descriptors would either lose diagnostics or block verbose workloads. Having the
worker inherit its own pipe read ends keeps the controller as the bounded drain
owner without adding a daemon. Output finalization must complete before terminal
state, or `job wait` could claim completion while retained bytes were still
changing.

### Evidence

- A real detached Runtime capture returned immediately, completed with exit 0,
  published its runpack, and replayed separate workload/controller stdout and
  stderr with exact byte counts.
- A stream 257 bytes over the 1 MiB bound retained exactly 1 MiB, discarded the
  remainder, reached normal completion, and reported truncation.
- A detached 30-second workload was cancelled by job identity, reaped, retained
  its pre-cancel output, returned exit 130, and published no partial runpack.
- A real detached RunDiff record published its named artifact. A real
  Git-backed Proofline run completed both arms, published both runpacks and its
  requested report, then replayed the exact final JSON document.
- Detached acknowledgements for RunDiff and Proofline point operators to the
  shared Runtime job controls; every advertised status, wait, and output
  command is executable.
- The full suite passes 1,081 tests with two skips, strict lint and typing, the
  installed-wheel smoke, and all eight release performance scenarios.

### What remains uncertain

Detached files intentionally retain raw workload output and therefore may hold
secrets despite mode-0600 protection. Replay does not preserve cross-stream
interleaving, and the fixed head retention may omit a late failure after a very
verbose start. Worker loss still makes the execution outcome unknowable; both
streams are then marked incomplete rather than presented as full output.

### Next highest-value action

Add a bounded live-follow view over the existing private spool files so an
operator can inspect a long detached capture without waiting for completion.
Keep the initial surface offset-based and local: no daemon, remote transport, or
additional retention beyond the existing fixed per-stream limit.

## 2026-08-20 14:22 BST

### What changed

`runtime job output JOB_ID --follow` and `contrail job output JOB_ID --follow`
now replay the retained prefix, flush each newly retained stdout and stderr byte,
and stop after the capture reaches terminal state. Every detached launch prints
the exact follow command, including RunDiff and Proofline launches that use the
shared Runtime control surface. The reader resumes each file from an independent
offset and never increases the existing 1 MiB per-stream retention bound.

### What we learned

Live observation does not need another worker channel. The spool files are
already the controller-owned bounded handoff: once terminal state is visible,
worker finalization guarantees both are stable. An offset reader avoids
replaying bytes on each poll and keeps stdout and stderr independent. The
observer must also remain operationally separate from the capture—interrupting
follow returns control to the operator but leaves the worker and workload alone.

### Evidence

- A real detached capture exposed its first stdout line while job state was
  still `running`, then delivered the later stdout, stderr, controller
  publication message, and final artifact without duplicate bytes.
- Independent offset tests resume different positions in stdout and stderr and
  reject negative, boolean, oversized, or beyond-file offsets.
- A capped stream followed through terminal state retains exactly 1 MiB and
  reports truncation once; later bytes remain discarded.
- Interrupting a live follower returns 130 while the capture remains `running`;
  an explicit subsequent `job cancel` reaps it and records exit 130.
- Independent probes passed simultaneous per-stream caps, active cancellation,
  16 fast terminal transitions, and installed-wheel live following with exact
  stdout/stderr.
- The full suite passes 1,092 tests with two skips; architecture, formatting,
  lint, strict typing, package contracts, the built-wheel smoke, and all eight
  release performance scenarios are green.

### What remains uncertain

Follow is intentionally a view of retained output, not a bypass around the
bound. Once a stream fills its head-retention file, no later bytes are available
to either live or final readers. This keeps memory and disk fixed but can hide a
late error after a verbose start. Separate streams still cannot reconstruct
their original cross-stream ordering.

### Next highest-value action

Retain a small bounded tail as well as the existing head so late failures remain
diagnosable after noisy starts. Keep the total per-stream byte budget fixed,
make the omitted middle explicit, and preserve append-only live-follow offsets
for the head portion.

## 2026-08-20 14:45 BST

### What changed

Detached capture now divides the existing 1 MiB per-stream budget into a
512 KiB append-only head and a 512 KiB rolling tail. Drain threads keep the
tail in bounded memory, checkpoint it in place under a file lock at most every
500 milliseconds, and discard the middle without slowing the workload pipe.
Terminal replay emits both segments and an exact omitted-byte count when the
worker observed the whole stream. Existing head-only job records remain
readable.

### What we learned

A fixed budget can preserve both startup context and late diagnostics without a
second on-disk copy of raw output. Rolling tails cannot safely share the live
reader's append offsets, so active readers expose only the stable head and add
the final tail after terminal state. If output finalization cannot become
stable, the job is recorded as lost with a lower-bound omission count rather
than claiming a complete terminal result.

### Evidence

- A stream with distinct 512 KiB head, 257-byte middle, and 512 KiB tail
  replayed the exact head and tail, omitted the middle, and reported 257 bytes.
- Zero-byte, 512 KiB, and 1 MiB boundaries retain exact output without a false
  gap or truncation notice.
- Simultaneous stdout and stderr floods, cancellation during overflow, live
  follow, and concurrent checkpoint readers completed without deadlock,
  duplicate replay, or temporary raw-output files.
- Killing a worker after an idle checkpoint retained a stable diagnostic tail;
  the lost job explicitly marked the omitted count as a lower bound.
- The installed wheel passed its detached capture and replay smoke. The full
  suite passes 1,097 tests with two skips; architecture, formatting, lint,
  strict typing, package contracts, and all eight release performance scenarios
  are green.

### What remains uncertain

Separate stdout and stderr segments still cannot reconstruct their original
cross-stream ordering. Active readers do not see a rolling tail until terminal
state, and checkpointing adds bounded controller memory and periodic fsync work.
Worker loss before a final stream count makes the omitted-byte value a lower
bound rather than an exact total.

### Next highest-value action

Add a zero-touch semantic capture layer for common I/O boundaries, beginning
with subprocess execution, so passive and sampled captures explain external
work without requiring annotations or the full cost of every-call profiling.

## 2026-08-20 15:19 BST

### What changed

Sample and deep capture now install one shared, standard-library-only observer
for Python `subprocess.Popen` boundaries. It records bounded, privacy-safe
executable identities, parent and child PIDs, shell use, exact elapsed time, and
exit or launch outcome in the existing registration, checkpoint, and final
profile snapshots. BatchScope renders the calls, RunDiff compares them as
operations, and Proofline can enforce count and error budgets without workload
instrumentation.

### What we learned

Exact semantic boundaries add more operational value than undifferentiated call
frames, but zero-touch observers must be behaviorally invisible. Reading a
`PathLike` or testing an opaque shell object can execute application code, so
the observer only inspects exact built-in values and reports unknown identity
instead. Unknown identity makes semantic evidence partial, preventing a missing
operation name from producing a false contract pass. Preserving the public
`Popen` class and signature is part of the compatibility boundary.

### Evidence

- Real sample and deep workloads captured successful, nonzero, shell, failed
  launch, unfinished, forked, checkpoint, and crash-recovery paths without
  retaining arguments, environment, cwd, output, or secret sentinel values.
- Passive, sample, deep, and installed-wheel probes produced identical
  `PathLike.__fspath__` and opaque-shell truthiness call counts and identical
  `inspect.signature(subprocess.Popen)` results.
- A reproduced identity-loss scenario is now `partial`; RunDiff retains that
  status and Proofline returns `unverifiable` instead of a false pass.
- Per-process capture is capped at 256 boundaries and merged evidence at 2,000;
  failures and unfinished calls win the bounded global selection.
- Independent verification accepted the fix. The full suite passes 1,106 tests
  with two skips; architecture, format, lint, strict typing, schema/package
  contracts, installed-wheel smoke, and all eight release performance scenarios
  are green.

### What remains uncertain

The observer covers Python `subprocess.Popen`, not direct `fork`/`exec`, native
extensions, custom launchers, or isolated interpreters that skip site startup.
Method patching may conflict with another tool patching the same methods. A
boundary currently has no causal edge back to its initiating application
function, short-lived children may escape process-tree sampling, and even a
sanitized executable basename can be sensitive in some environments.

### Next highest-value action

Attach a bounded caller identity to each subprocess boundary and normalize it as
an explicit causal edge. That would let BatchScope attribute external wait time
to the initiating application function before the same semantic substrate is
extended to privacy-safe HTTP client boundaries.

## 2026-08-20 15:41 BST

### What changed

Subprocess boundaries now carry a bounded caller identity and an explicit
`launches` causal edge. Deep capture derives the exact nearest application
caller from its existing profile stack; sample capture attributes a caller only
when its existing sampler observes an active `communicate` or `wait`. The
normalized runpack contains an untimed `python.callsite` event, and BatchScope
renders the source location, observation method, confidence, and attribution
completeness without changing RunDiff or Proofline operation facts.

### What we learned

Caller attribution is useful only if its certainty and completeness remain
separate from the subprocess fact itself. Deep mode can promise exact callers,
while sampling will legitimately miss fast waits; malformed or missing caller
evidence must therefore degrade only caller attribution. Reusing the existing
profile stacks avoids a second instrumentation mechanism and avoids Python's
observable frame-audit hooks. Arguments, locals, command arguments, and output
remain outside the captured identity.

### Evidence

- A real long-wait example attributed `launch_worker` at its source line in
  both modes: sample reported `sampled, 90%` in 141.3 ms and deep reported
  `exact, 100%` in 148.6 ms, each with a normalized callsite and `launches`
  edge.
- Short unobserved sample calls remain explicitly unattributed; long observed
  calls, deep nearest-caller selection, partial checkpoints, and abrupt worker
  loss retain the expected evidence.
- Passive, sample, deep, and installed-wheel probes preserve the public
  `Popen`, `communicate`, `wait`, and `poll` signatures and application-visible
  `PathLike`, shell-truthiness, and frame-audit behavior.
- Secret sentinels, arguments, and locals are absent from runpack bytes.
  Malformed caller records leave valid subprocess boundaries intact and mark
  only caller attribution invalid.
- Independent verification accepted the change. The full suite passes 1,110
  tests with two skips; architecture, format, lint, strict typing,
  schema/package contracts, installed-wheel smoke, and all eight release
  performance scenarios are green.

### What remains uncertain

Sampling can never attribute a caller it does not observe, method patching can
conflict with another tool patching the same methods, and source filenames may
still be sensitive even though arguments and locals are excluded. Callsite
events are intentionally untimed and excluded from critical-path and operation
facts. Startup still depends on the isolated interpreter loading the capture
bootstrap.

### Next highest-value action

Extend the semantic substrate to common HTTP client boundaries with the same
exact-deep and observed-sample caller model. Define a privacy-safe server
identity first: omit headers, bodies, credentials, and query strings, and make
host retention an explicit policy rather than silently treating it as safe.

## 2026-08-20 16:08 BST

### What changed

Sample and deep capture now observe outbound standard-library HTTP requests
without workload code changes. The shared observer emits bounded
`http.client.request` operations with safe method, scheme, numeric port,
response-header timing, status or exception class, and exact or sampled caller
causality. BatchScope explains the requests, RunDiff compares them, and
Proofline makes exact operation-count and error claims unverifiable when HTTP
evidence is incomplete.

### What we learned

Zero-touch HTTP evidence is useful before retaining server identity. A method,
status, duration, and initiating function can reveal retry amplification,
failure, and external wait while a fixed redaction policy keeps host, path,
query, headers, bodies, and credentials outside the artifact. HTTP evidence
also needs a completeness contract independent from subprocess evidence: one
malformed or truncated family must not erase the other. Response-header timing
is a defensible boundary, but it does not represent response-body download.

### Evidence

- The self-contained uninstrumented example produced one `POST
  http://<redacted>:60933`, status 202, 88.4ms through response headers, with
  exact caller `__main__.publish_batch`.
- Real sample and deep tests cover successful POST requests, connection errors,
  truncation, malformed evidence, legacy reports, abrupt `os._exit`
  checkpoints, privacy sentinels in URLs, headers, and bodies, and preserved
  public method signatures.
- The installed wheel repeats a real deep HTTP capture and rejects any artifact
  containing request or response secret sentinels.
- RunDiff sees retained requests as operations; Proofline returns
  `unverifiable` rather than a false pass when HTTP capture is incomplete.
- Independent verification accepted the detached-job control surface and race
  fixes. The full suite passes 1,120 tests with two skips; architecture, format,
  lint, strict typing, package contracts, installed-wheel smoke, and all eight
  release performance scenarios are green.

### What remains uncertain

Coverage is limited to `http.client`, `urllib.request`, and clients that
delegate through those methods. Direct sockets, native transports, independent
HTTP/2 stacks, `httpx`, and `aiohttp` are not yet promised. Method patching can
conflict with another observer, sampling can miss short callers, source
filenames and numeric ports may still be sensitive, and measured duration
excludes response-body consumption.

### Next highest-value action

Add dependency-free, optional adapters for the most common Python HTTP client
stacks that bypass `http.client`, beginning with `httpx` and `aiohttp`. Reuse the
same redaction, bounds, caller, checkpoint, and completeness contracts rather
than creating a second HTTP evidence model.

## 2026-08-20 16:32 BST

### What changed

The shared zero-touch HTTP observer now activates dependency-free adapters when
a workload imports `httpcore` or aiohttp. HTTPX's default sync and async
transports and aiohttp therefore produce the same bounded, redacted
`http.client.request` evidence as `urllib.request`, including adapter identity,
response-header duration, status or safe exception class, and Deep caller
causality. Sample mode keeps async callers explicitly unattributed rather than
misassigning concurrent event-loop tasks to a thread-local sample.

### What we learned

A narrow lazy-import hook can extend coverage without making popular clients
runtime dependencies or importing them into workloads that do not use them.
Delegating to the real `PathFinder` loader and restoring it preserves normal
module identity and reload behavior. One adapter cannot claim identical
semantics everywhere: `httpcore` exposes physical HTTPX requests, while
aiohttp's session boundary represents a logical request that may include
redirects.

### Evidence

- A clean installed-wheel run with HTTPX 0.28.1, httpcore 1.0.9, and aiohttp
  3.14.3 retained three successful local requests and identified
  `httpcore.sync`, `httpcore.async`, and `aiohttp.async`.
- Deep Capture attributed all three requests exactly to
  `__main__.fetch_httpx` or `__main__.fetch_async_clients`; the durations through
  response headers were 64.1ms, 87.1ms, and 73.1ms in that run.
- Host, path, query, header, body, credential, and response sentinels produced
  no runpack match. Cancellation retained only `CancelledError`, and module
  reload produced one event per call without leaving the wrapper loader behind.
- An optional-client request survived abrupt `os._exit` through the shared
  checkpoint channel with explicitly partial capture status.
- The offline installed-wheel smoke now supplies API-compatible optional client
  modules and checks adapter identity, exact callers, and privacy from the built
  distribution. The full suite passes 1,125 tests with two skips; architecture,
  formatting, lint, strict typing, schema/package contracts, wheel smoke, and
  all eight release performance scenarios are green.

### What remains uncertain

Optional client internals can change independently because Contrail does not
pin them. Custom transports or importers can bypass the patch points, another
observer can replace the same methods, aiohttp redirects are not expanded into
physical attempts, and response-body time remains outside the interval. Sample
mode deliberately has no async caller attribution.

### Next highest-value action

Qualify a small client-version matrix and measure request-heavy overhead before
adding another boundary family. The next zero-touch candidate should be
database calls with the same rule: retain operation class, timing, outcome, and
caller while excluding statements, parameters, credentials, and server names.

## 2026-08-20 16:51 BST

### What changed

Sample and deep capture now add a protocol-agnostic fallback for outbound
blocking stream sockets and asyncio TCP or Unix transports. BatchScope renders
bounded `network.connect` evidence, RunDiff compares it, and Proofline treats
incomplete connection evidence as insufficient for exact count and error
claims. The observer retains transport, address family, numeric port, optional
TLS request, connection-ready timing, safe outcome, and caller while excluding
server addresses, Unix paths, credentials, and exception messages.

### What we learned

Connection evidence is a useful zero-touch floor, not a database or queue
operation model. It exposes connection churn, retries, failures, and setup
latency for unsupported protocol clients, but a pool can serve many logical
operations through one connection. Higher-level HTTP evidence must win when
available, so task-local suppression prevents one request from becoming both an
HTTP and connection operation. The same suppression is required for Contrail's
own snapshot socket so the observer never reports its control plane as workload
behavior.

### Evidence

- A self-contained workload produced one blocking and one asyncio TCP
  connection with exact Deep callers and redacted server identity.
- Real TCP and Unix success, refusal, missing-path, cancellation, truncation,
  RunDiff, Proofline, and abrupt-checkpoint tests retain only safe structured
  fields; failure and cancellation messages do not enter the runpack.
- A real `urllib.request` capture emits one HTTP request and zero connection
  events, proving duplicate suppression, while snapshot publication creates no
  self-observation.
- Each interpreter and controller use independent 256- and 2,000-record limits;
  malformed or legacy connection evidence does not discard other semantic
  families.
- The built wheel passed the installed-artifact smoke, all eight PR performance
  scenarios passed their budgets, and the complete package-qualified suite
  passes 1,135 tests. Architecture, formatting, lint, and strict typing are
  green.

### What remains uncertain

Native database drivers, datagrams, raw nonblocking socket state machines, and
alternative event loops can bypass these patch points. Asyncio timing can
include name resolution and TLS transport setup, while raw socket timing ends at
`connect`; these durations are intentionally labelled by their boundary rather
than presented as equivalent. Fast connections may be unattributed in Sample
mode, and numeric ports or caller filenames can still be sensitive.

### Next highest-value action

Measure the connection-heavy overhead and qualify representative pure-Python
database, cache, and queue clients. Add protocol-specific adapters only where a
safe operation identity materially improves on the connection fallback without
retaining statements, keys, payloads, credentials, or server names.

## 2026-08-20 17:01 BST

### What changed

BatchScope now converts the connection fallback into three conservative
diagnostic signals: retained connection failures, a successful setup lasting at
least 50 milliseconds and one quarter of the run, and high-rate churn of at
least 10 attempts at five per second. A new release benchmark opens 200–256 real
loopback connections, requires every attempt to survive Sample capture and
normalization, and gates the complete Sample-to-passive workload-duration ratio.

### What we learned

Physical connection evidence can support useful diagnosis without pretending
to know a database query or queue message. Failures and material setup time are
direct observations. Churn is weaker: even a high connection rate cannot prove
whether pooling is missing or retries are justified, especially after server
identity is redacted. The finding therefore stays advisory and lower confidence
instead of naming a cause. Measuring the full preset is similarly more honest
than attributing all overhead to the small connection wrapper.

### Evidence

- Synthetic normalized evidence deterministically exercises all three findings
  and proves 49-millisecond setup plus 10 attempts over three seconds remain
  below the thresholds.
- Real zero-code Sample capture opens 12 connections and produces the churn
  finding; real refused TCP and missing Unix connections produce the failure
  finding without retaining endpoints or error messages.
- Five paired 200-connection runs measured passive at a 43.4-millisecond median,
  Sample at 74.0 milliseconds, and a 1.592x median paired ratio. The release
  shape retained all 256 attempts at 1.727x in its observed run; both remain
  below the deliberately broad 5x cross-runner ceiling.
- All nine PR release scenarios pass. The rebuilt wheel's isolated smoke retains
  12 blocking/async connections and the churn diagnosis; the package-qualified
  source suite passes 1,138 tests, with architecture, formatting, lint, and
  strict typing green.

### What remains uncertain

The calibration is loopback, sequential, macOS, and startup-heavy; it is not a
database-driver matrix or a wrapper-only overhead claim. The diagnosis cannot
group by redacted server identity, and legitimate short-lived protocols can
produce churn. Native transports and alternative event loops still bypass the
fallback.

### Next highest-value action

Exercise representative protocol-client lifecycle shapes—pooled reuse,
per-operation reconnect, retry after refusal, and concurrent async connects—and
show which facts are visible without statements, keys, payloads, credentials,
or server names. Use those results to decide whether the next adapter should be
database/queue-specific or a more general DNS/TLS boundary.

## 2026-08-20 17:15 BST

### What changed

BatchScope now aggregates all retained connection evidence by validated caller
and adapter before limiting public output to 100 hotspots. Each group reports
total, connected, failed, and unfinished attempts plus total and maximum setup
duration. A self-contained protocol-client scenario exercises pooled reuse,
per-operation reconnects, retry after refusal, and concurrent asyncio connects
without importing Contrail or retaining endpoints and payloads.

### What we learned

The connection floor can distinguish lifecycle shape even when it cannot see
logical protocol operations. One pooled connection serving three cache reads is
visibly different from 12 queue publishes that reconnect every time. Two failed
attempts followed by success remain one retry callsite, while eight async
connections retain their own adapter and caller. Aggregating by caller rather
than redacted endpoint preserves that value without quietly reconstructing a
server identity.

### Evidence

- One real Deep run retained all 24 attempts across four exact callsites: pooled
  `PooledCacheClient.__init__` 1/1 connected, reconnecting
  `ReconnectingQueueClient.publish` 12/12, `connect_with_retry` 1 connected and
  2 failed, and async `publish_one` 8/8.
- The run produced both `connection_failures` and advisory `connection_churn`
  findings, while HTTP capture stayed at zero requests.
- A 101-connection regression proves hotspot aggregation consumes the full
  retained set before the individual connection list is capped at 100; the
  summary preserves the two-group total and text reports the omitted detail.
- Schema and JSON-contract tests type the additive hotspot array and its bounded
  summary count; synthetic output reconciles connection outcome and duration
  totals.
- A fresh public-CLI Deep run completed in 182.4 milliseconds, attributed all
  24 attempts across four callsites, and reported 2 failures plus 24-attempt
  churn without retaining server addresses. All nine PR release scenarios pass,
  the rebuilt wheel passes its isolated smoke, and the package-qualified suite
  passes 1,140 tests with architecture, formatting, lint, and strict typing
  green.

### What remains uncertain

Connection reuse alone cannot reveal how many real queries or messages used the
pool; the example's logical-operation counts come from workload output, not
capture. Grouping by caller separates code paths but cannot distinguish two
redacted servers reached from the same line. Sample mode may miss fast callers,
and native transports remain outside the observer.

### Next highest-value action

Add redacted name-resolution and TLS-handshake timing where they can decompose
connection setup for Python transports, or prototype a protocol-specific
operation adapter only if it can preserve the same no-statement/no-key/no-payload
privacy boundary. Prefer the general setup decomposition unless a real client
probe shows that connection lifecycle evidence is insufficient.

## 2026-08-20 17:41 BST

### What changed

Deep capture now observes Python DNS resolution and blocking or asyncio TLS
handshakes as a fourth, independently bounded evidence family. BatchScope
normalizes and groups these setup phases by validated caller, diagnoses material
DNS/TLS latency and failures, and reports capture completeness separately from
physical connections. RunDiff carries that completeness into comparisons, and
Proofline refuses exact operation-count or error-rate claims when retained setup
evidence is incomplete.

### What we learned

Connection setup is not one indivisible cost. A zero-touch observer can separate
name resolution, physical connection, and TLS negotiation without retaining the
queried hostname, resolved address, SNI, certificate, credentials, or payload.
The stdlib TLS boundary must treat nonblocking want-read/want-write retries as
one logical handshake, and client plus server handshakes are both legitimate
evidence. Keeping a separate budget and status prevents setup truncation from
silently degrading the connection observer's completeness claim.

### Evidence

- A real zero-code Deep run of the local TLS example completed in 91.0
  milliseconds, retained 2 connected physical attempts and all 6 setup phases
  across 4 exact callsites: 2 DNS resolutions and 4 client/server TLS
  handshakes. It reported no bottleneck and retained none of the sensitive setup
  values.
- Deterministic real failure probes classify DNS and TLS setup errors by safe
  exception class, while synthetic 60-millisecond DNS and failed-TLS evidence
  exercise the corresponding BatchScope findings.
- A one-phase controller-bound regression proves setup truncation is independent
  from complete connection capture, appears in RunDiff, and makes the affected
  Proofline claim unverifiable.
- The package-qualified suite passes 1,145 tests. All nine release benchmark
  scenarios pass, the rebuilt wheel passes isolated installed-package smoke,
  and architecture, formatting, lint, and strict typing are green.

### What remains uncertain

This captures Python stdlib boundaries, not native resolver or TLS
implementations, alternative event loops, QUIC, or logical database and queue
operations. Setup callsites can identify code paths but cannot safely group by
redacted server identity. The local certificate and loopback measurements prove
behavior, not representative production latency or overhead.

### Next highest-value action

Run representative real database and queue clients against local protocol
fixtures to quantify the gap between connection/setup evidence and logical
operation evidence. Add a protocol adapter only where that probe demonstrates
material missing value and the adapter can preserve the no-statement, no-key,
no-payload privacy boundary.

## 2026-08-20 18:00 BST

### What changed

Deep capture now observes supported logical database and queue operations as a
fifth, independently bounded evidence family. The first zero-touch adapters
cover SQLite connection and cursor execution, commit, and rollback plus
blocking and asyncio queue put/get calls. BatchScope attributes, aggregates,
and diagnoses the retained operations; RunDiff carries capture completeness
through comparisons; Proofline refuses exact count or error claims when enabled
evidence is incomplete.

### What we learned

Physical connection, DNS, and TLS evidence cannot recover logical work done
through a local database or a reused client. The logical boundary closes that
gap without retaining statements, parameters, rows, queue items or identities,
payloads, return values, or exception messages. Method substitution does add a
per-operation cost and can perturb identity-sensitive integrations, so this
belongs in the explicitly expensive Deep preset rather than the default Sample
path.

### Evidence

- A fresh zero-code public-CLI Deep run completed in 171.9 milliseconds and
  retained all 9 operations across 5 exact callsites: 5 database operations,
  4 blocking/async queue operations, and one intentional SQLite
  `OperationalError`.
- BatchScope diagnosed the database failure and a 67.5-millisecond blocking
  queue wait consuming 39% of the run. All four supported adapters were active,
  and no statement, row, or queue-item privacy sentinel appeared in the
  runpack.
- Reload regressions preserve capture through `queue`, `sqlite3.dbapi2`, and
  `sqlite3` reloads without duplicate evidence. The default connection also
  preserves the public `sqlite3.Connection` type contract.
- A controller-bound regression proves truncation is independent from the
  other semantic families, appears in RunDiff, and makes an affected Proofline
  exact-count claim unverifiable.
- All 10 PR release scenarios pass. The logical-operation scenario retained
  200 operations in 122 milliseconds at 47 MiB RSS, and the rebuilt wheel
  passes isolated installed-package smoke. The package-qualified suite passes
  1,149 tests with 2 skips; architecture, formatting, lint, strict typing,
  schema parsing, and diff hygiene are green.

### What remains uncertain

The first adapter set does not cover direct `_sqlite3` use, custom factories,
`queue.SimpleQueue`, native drivers, third-party database clients, or brokers.
Execution duration ends when `execute` returns and therefore excludes later row
fetching. The local in-memory and in-process measurements prove behavior and
privacy boundaries, not production overhead across client libraries.

### Next highest-value action

Qualify representative real database and broker clients through an isolated
optional adapter matrix, without making them core dependencies. Measure which
Python-level boundaries survive pooling, retry, async execution, module reload,
and native acceleration, then add only adapters that preserve bounded evidence,
public type compatibility, and the no-statement/no-key/no-payload contract.

## 2026-08-20 18:24 BST

### What changed

Deep capture now observes CPython-visible native calls as an independently
bounded generic evidence floor. BatchScope reports native function, call, and
exception counts alongside Python hotspots, distinguishes native from Python
implementations, and excludes observed native child time from Python self time.
The capture records callable identity and timing only: arguments, return values,
exception messages, and object representations remain outside the runpack.

### What we learned

The profiler's native call events close a useful part of the gap left by
library-specific adapters. Direct `_sqlite3`, file I/O, compression, locking,
and sleep calls remain observable even when no semantic adapter recognizes the
client. This is a generic timing and failure-count floor, not a replacement for
semantic adapters: it cannot identify a database operation kind or safely
retain statements, keys, destinations, or payloads. Native child accounting
also makes Python self time more useful by separating visible native waits.

### Evidence

- A fresh zero-code Deep run of the public native-call example completed in
  108.3 milliseconds and retained 209 native calls across 68 functions with 5
  native exceptions. It surfaced the 50.0-millisecond sleep, direct SQLite
  execution with 4 calls and 1 exception, file I/O, and zlib work without
  retaining any privacy sentinel.
- An adversarial contract regression rejects metadata that claims native
  arguments were captured, and parser regressions reject native/Python identity
  mismatches and impossible exception counts.
- All 11 PR release scenarios pass. The native scenario retained 1,000 exact
  compression calls at 1.412x Deep/passive workload time and 45.5 MiB peak RSS,
  below its explicit 5x overhead guardrail.
- The package-qualified suite passes 1,154 tests with 2 skips. The rebuilt wheel
  passes isolated installed-package smoke, including direct native SQLite, file,
  zlib, and sleep evidence.

### What remains uncertain

Coverage is limited to native calls surfaced by CPython's profile events. Work
that stays entirely inside native code, native-created threads that never enter
profiled Python, alternative interpreters, and native calls without a useful
callable identity can remain invisible. Exception counts can include benign
control-flow exceptions, and the local benchmark does not establish production
overhead for every extension workload.

### Next highest-value action

Use this generic native floor to qualify representative optional database,
cache, and broker clients without adding core dependencies. Promote only the
highest-value boundaries to semantic adapters where the generic evidence cannot
answer operation kind, queueing, retry, or error questions while preserving the
same bounded, payload-free contract.

## 2026-08-20 18:40 BST

### What changed

Deep logical-operation capture now recognizes documented public SQLAlchemy,
Redis, Pika, and aiokafka boundaries without importing or depending on those
packages. The normalized family adds cache command/batch and broker
publish/consume operations beside database and in-process queue operations.
A task-local suppression marker makes the outer ORM, async proxy, or pipeline
boundary authoritative instead of double-counting its wrapped lower layers.

### What we learned

Explicit module/class/method paths add substantially more meaning than generic
native timings without requiring argument inspection or heuristic method-name
matching. Import-time patching can preserve the real loader, reload behavior,
method signature, return value, and exception. Nested suppression must be
task-local: two concurrent async commands are independent operations, while one
AsyncSession or pipeline call through a lower wrapped client is still one.
Recognized but incompatible package shapes must downgrade completeness because
silently skipping them would make exact counts unsafe.

### Evidence

- A zero-code sync/async API-shape run retained all 32 logical operations across
  11 optional adapters and 6 exact application callsites: 16 database, 9 cache,
  and 7 broker operations. Six intentional failures retained only RuntimeError,
  TimeoutError, or OSError class names.
- The run exercised concurrent async Redis calls, nested SQLAlchemy Session and
  AsyncSession calls, sync/async Redis pipelines, Pika publish/get, and aiokafka
  publish/get-one/get-many. Public signatures, application results, exceptions,
  module loaders, and Redis reload behavior matched Passive capture.
- No statement, parameter, command/key, exchange/routing-key/message, topic,
  result, or exception-message sentinel appeared in the runpack. An adversarial
  adapter/category mismatch is rejected independently from the valid profile.
- A recognized Redis-shaped module missing Pipeline support retained its one
  observed command but reported truncated logical evidence with one observer
  error instead of claiming complete coverage.
- All 12 PR release scenarios pass. The optional-client scenario retained 200
  exact cache commands at 1.366x Deep/passive workload time and 48.3 MiB peak
  RSS, below its explicit 5x ceiling.
- The package-qualified suite passes 1,157 tests with 2 skips. The rebuilt wheel
  passes isolated installed-package smoke for the new cache category and privacy
  boundary as well as the existing capture families.

### What remains uncertain

The dependency-free matrix validates documented call shapes, not real upstream
package releases connected to real PostgreSQL, Redis, RabbitMQ, or Kafka
protocol fixtures. SQLAlchemy execution still ends before later result
iteration; Redis pipeline execution is one batch rather than one record per
queued command; Pika publish completion does not universally prove broker
delivery. Package internals, immutable extension classes, custom importers,
monkeypatching, and future version drift can bypass an adapter. Task-local nested
suppression can also hide a deliberately nested application client call made
inside an outer adapted operation.

### Next highest-value action

Build an isolated optional-package compatibility harness that can qualify pinned
upstream versions against local protocol fixtures without making them runtime
dependencies. Measure signature/loader preservation, pooling, retries,
concurrency, cancellation, reload, native acceleration, and per-client overhead;
then narrow or remove any adapter whose real package behavior does not match the
documented evidence boundary.

## 2026-08-20 18:49 BST

### What changed

Deep capture now aggregates Python exception propagation events by function.
The CPython trace callback disables line and opcode events and never inspects
its exception payload. BatchScope exposes completeness, event/function/drop
counts, per-frame semantics, and explicit false privacy markers; individual
Python hotspots can now have more exception events than calls.

### What we learned

This is useful zero-touch evidence, but it is deliberately not an error count.
One exception emits an event in each Python frame it crosses, and one function
call can catch many exceptions. Keeping that semantic explicit avoids false
failure claims while still locating exception-heavy control flow. Combining
`sys.settrace` with exact call profiling adds measurable cost even with line and
opcode events disabled, so the feature belongs only in the expensive Deep tier.

### Evidence

- A threaded workload called one function twice, caught eight exceptions, and
  retained exactly eight events for that function with one process
  contribution. Its exception-message sentinel was absent from the runpack.
- Fork reset retained a child-only exception count without inheriting the
  parent's aggregate. A process using `os._exit` retained all 40 caught events
  from its early checkpoint.
- The PR overhead gate caught 1,000 exceptions in one function call, retained
  the exact count with zero drops, and measured 1.565x Deep/passive whole-run
  time at 45.6 MiB peak RSS, below the explicit 5x ceiling. All 13 PR release
  scenarios pass.
- The package-qualified suite passes 1,159 tests with 2 skips. The rebuilt wheel
  passes isolated installed-package smoke for Python exception counts and
  privacy as well as the existing capture families. Formatting, lint, strict
  typing, schema parsing, privacy, fork, thread, and checkpoint checks are green.

### What remains uncertain

The observer uses CPython's tracing API and can conflict with debuggers,
coverage tools, profilers, or workload code that replaces `sys.settrace`.
Alternative interpreters are not qualified. Exception-heavy framework control
flow may be noisy, and the local fixed-wait benchmark does not predict overhead
for every application shape.

### Next highest-value action

Exercise a realistic failing batch workload to see whether per-function
propagation counts improve a diagnosis without being mistaken for unique or
unhandled errors.

## 2026-08-20 19:02 BST

### What changed

Deep capture now reports observer integrity instead of silently treating hook
displacement as complete evidence. The bootstrap counts public `sys` and
`threading` profile/trace setter calls before replacement takes effect. Profile
replacement marks exact call/caller evidence truncated; trace replacement
independently makes Python-exception evidence partial. BatchScope and JSON name
the affected processes and keep legacy artifacts explicitly unavailable.
Because hook arguments are intentionally not inspected, even a no-op setter
call is conservatively treated as possible displacement.

### What we learned

Zero-touch observation must explain when another zero-touch observer wins.
Debuggers, coverage tools, profilers, and application code legitimately use the
same CPython hooks. Fighting them by automatically reinstalling Contrail would
change workload behavior; retaining a small, payload-free integrity fact is the
safer product contract. The profile callback itself is the useful interception
point because it sees the public setter call before that call can remove it.

### Evidence

- Real direct `sys.settrace` replacement preserved exact call profiling while
  downgrading exception coverage; direct `sys.setprofile` replacement marked
  the Deep profile truncated.
- `threading.settrace` and `threading.setprofile` replacements were recorded
  before a newly started thread ran without Contrail's hooks.
- A forked child cleared the parent's trace-replacement counter, reinstalled
  its hooks, and retained the child-only exception event. The merged summary
  attributed one replacement to one of two reporting processes.
- A workload replaced `sys.setprofile` and then used `os._exit`; its 50 ms
  checkpoint retained the displacement warning and explicit partial status.
- Hook callable/value sentinels were absent from runpacks, and adversarial
  metadata claiming hook-value capture normalizes as invalid.
- All 13 PR release scenarios pass; the exact native and Python-exception gates
  also require complete integrity metadata and remain below their 5x Deep/
  Passive ceilings. The package-qualified suite passes 1,165 tests with 2
  skips, and the rebuilt wheel passes installed-package hook-displacement smoke.

### What remains uncertain

The detector covers documented public Python setters, not direct CPython C API
calls, interpreter-state mutation, custom runtimes, or malicious evidence
tampering. A hook can be temporarily displaced and restored between observable
setter calls only by bypassing those surfaces. The integrity counter explains
known loss; it is not an attestation mechanism.

### Next highest-value action

Use the now self-describing completeness boundary in a realistic failing batch
diagnosis.

## 2026-08-20 19:17 BST

### What changed

BatchScope now classifies sustained application-level Python exception churn
from complete Deep evidence. It requires at least 10 events and 0.5 events per
call, reconciles all retained Python aggregates with the exception summary,
requires intact observer integrity and zero drops, and emits at most one 60%
confidence finding. The evidence says the count is per-frame propagation, not
unique failures. Partial, truncated, legacy, hook-displaced, and unreconciled
evidence remains visible but cannot trigger the diagnosis.

### What we learned

The useful zero-touch product result is not merely “exceptions happened”; it is
“this application function is exception-heavy, here is the rate, and here is
why that count is not a failure total.” Completeness provenance is what makes
that claim safe. Applying the diagnosis across the full retained aggregate set,
before the 100-row display limit, also prevents a fast retry loop from vanishing
behind slower ordinary functions.

### Evidence

- An unmodified 16-record batch completed 48 attempts with 32 transient
  failures. BatchScope selected `__main__.load_with_retry`, reporting 32
  propagation events across 16 calls (2.00 per call) and explicitly stating
  that these were not unique failures.
- The complete profile retained 68 propagation events across five functions:
  64 application propagation events plus four runtime/import events. The
  diagnosis correctly selected application control flow rather than treating
  the execution-wide count as an error total.
- The trace-hook integrity summary was complete, no exception events were
  dropped, and the exception-message sentinel was absent from the runpack.
- Boundary tests reject 9 events, a rate below 0.5 per call, truncated capture,
  trace-hook displacement, and legacy evidence without integrity provenance.
  A 101-function case still diagnoses a candidate beyond the public 100-row
  hotspot limit.

### What remains uncertain

The absolute and per-call thresholds are conservative starting points, not yet
calibrated against production frameworks. Some frameworks use exceptions for
normal control flow, and the 60% confidence deliberately describes diagnostic
relevance rather than proving material runtime cost.

### Next highest-value action

Test the classifier on exception-heavy framework workloads and a real incident,
then decide whether rate, self time, or process concentration should refine its
confidence without weakening the completeness gate.

## 2026-08-20 19:34 BST

### What changed

Deep now preserves its raw Python exception-propagation count and adds a second,
versioned diagnostic count. The trace callback excludes only exact built-in
`StopIteration`, `StopAsyncIteration`, and `GeneratorExit` type identities from
that second count. BatchScope requires the filter metadata and reconciled
diagnostic totals before emitting `python_exception_churn`; older artifacts stay
readable but cannot trigger the classifier.

### What we learned

Successful coroutine awaits use `StopIteration` internally and looked like
sustained application exception churn in the raw trace stream. A broad
framework heuristic would be fragile and opaque. Exact identity filtering of
three language-level control-flow types removes that false positive while
preserving both the public raw evidence and genuine async exceptions. The
privacy boundary remains narrow: type identity is compared transiently against
a static policy, while no workload type, name, value, message, or traceback is
retained.

### Evidence

- A normal 200-step `asyncio` loop previously emitted 200 raw events in each of
  its coroutine frames and produced a false `python_exception_churn` finding;
  it now retains those raw events, reports zero diagnostic events for those
  frames, and emits no finding.
- A real async retry loop retained 20 diagnostic `RuntimeError` propagation
  events across 21 calls and still produced the conservative diagnosis.
- The unmodified retrying-batch example reports 32 diagnostic events across 16
  calls (2.00 per call), states that built-in iterator completion is excluded,
  and retains none of its exception-message sentinel.
- Function-budget drops, fork reset, abrupt-exit checkpoints, legacy artifacts,
  malformed policy metadata, and the public JSON schema all have focused
  regression coverage.

### What remains uncertain

The static policy intentionally does not filter subclasses or framework-defined
sentinel exceptions. Those may still be noisy, but adding them would require a
separate evidence-backed and privacy-reviewed policy rather than silently
changing the meaning of this filter version.

### Next highest-value action

Run the full release gate and installed-wheel smoke, then exercise the filter on
real exception-heavy async frameworks before considering any broader policy.

## 2026-08-20 19:50 BST

### What changed

Deep now observes standard `ThreadPoolExecutor` and `ProcessPoolExecutor` tasks
from submission until Future completion. Each bounded `executor.task` operation
contains only adapter, timing, safe outcome, PID, and exact submitting callsite;
callable identity, arguments, successful result, exception object, and message
are excluded. BatchScope diagnoses executor failures/material latency, RunDiff
compares task counts, and Proofline can enforce the same operation contracts.

Queue capture now retains application-originated callsites when exact caller
evidence is available. This removes executor-manager polling and its expected
`queue.Empty` control flow from application failure findings without hiding a
user queue call made from application code.

### What we learned

Generic every-call profiling can show executor internals, but it cannot answer
the product question “how many tasks were submitted, how long until each Future
completed, and which submitter amplified them?” A single Future callback adds
that semantic boundary without wrapping the callable or moving work. Testing a
real process pool also exposed why semantic filters must separate application
boundaries from library control flow: otherwise five normal empty-poll results
looked like queue failures.

### Evidence

- An unmodified example retained three thread tasks and two process tasks, with
  exact `run_thread_pool`/`run_process_pool` callsites and one safe
  `RuntimeError`. BatchScope emitted executor failure and material-latency
  findings.
- A candidate with two extra thread tasks appeared as `5 → 7` in RunDiff;
  Proofline failed a 1.2x `Executor task` count contract at the exact limit of
  six.
- Cancellation and submit-after-shutdown produce `CancelledError` and
  `RuntimeError` outcomes while the workload behavior remains unchanged.
- Reloaded concurrent-futures modules are re-patched, thread/process public
  behavior matches Passive capture, and callable/argument/result/error-message
  sentinels are absent from the runpack.
- The 200-task PR release case retained every task with complete caller
  attribution at 1.918x Deep/passive whole-workload time, below the 5x ceiling.

### What remains uncertain

Submission-to-completion includes executor queueing, process serialization,
worker execution, and result relay; it is intentionally not worker-only time.
Custom Executor/Future implementations and methods replaced after import remain
outside the adapter. Production pools may submit above the 256-record
per-process bound, where completeness—not silent sampling—will be reported.

### Next highest-value action

Qualify the installed wheel and full release suite, then evaluate whether
separate queue-wait versus execution timing can be added without wrapping user
callables or changing process-pool pickling behavior.

## 2026-08-20 20:08 BST

### What changed

Deep now observes explicit `asyncio.create_task` and `TaskGroup.create_task`
lifecycles from creation through completion. Each `scheduler.task` contains
only adapter, timing, safe outcome, PID, and exact application callsite.
Awaitables, task names, context values, arguments, results, exception objects,
and messages are excluded. BatchScope diagnoses scheduler failures/material
latency, RunDiff compares task counts, and Proofline can enforce the same
operation contracts.

### What we learned

Patching `BaseEventLoop.create_task` captured eight boundaries for a five-task
workload because `asyncio.run` creates its main and shutdown tasks while the
application's `main` frame remains on the stack. Restricting the adapter to the
two explicit public creation APIs preserves a trustworthy task-count meaning
without heuristics. Calling `Task.exception()` would also suppress Python's
unretrieved-exception warning; reading CPython's stored exception slot only long
enough to derive its class name preserves that observable behavior.
Likewise, setting the existing nested-operation suppression marker during task
creation copied `True` into the new task and hid its real queue operations. The
explicit creation wrappers do not nest another semantic adapter, so delegation
must occur without that marker.

### Evidence

- An unmodified example retains three standalone and two structured-concurrency
  tasks with exact `run_create_tasks`/`run_task_group` callsites and one safe
  `RuntimeError`.
- A candidate with two extra tasks appears as `5 → 7` in RunDiff; Proofline
  rejects a 1.2x `Async task` count contract at the exact limit of six.
- Cancelled and rejected task creation retain `CancelledError` and
  `RuntimeError`; module reloads reapply both public adapters.
- Deep and Passive both emit the normal “Task exception was never retrieved”
  warning for an intentionally abandoned failed task.
- The 200-task PR release case retains every task with complete callsites at
  1.723x Deep/passive whole-workload time, below the 5x ceiling.

### What remains uncertain

Creation-to-completion includes event-loop scheduling and suspended await time,
not CPU execution. Direct loop scheduling, `ensure_future`, coroutine arguments
passed directly to `gather`, custom task factories, and alternate event loops
remain generic Deep evidence rather than scheduler operations. The shared
256-record logical-operation bound can become incomplete in task-heavy services.

### Next highest-value action

Qualify the installed wheel and full release suite, then evaluate whether Deep's
maintained stack can distinguish application-requested implicit task creation
from `asyncio.run` bootstrap/shutdown without frame inspection or task wrapping.

## 2026-08-20 20:24 BST

### What changed

Deep now observes top-level `asyncio.ensure_future` calls and the tasks that
top-level `asyncio.gather` creates for coroutine arguments. Existing Futures
passed through either API are excluded by identity, so an explicit task passed
to `gather` remains one operation. The same bounded `scheduler.task` facts flow
through BatchScope, RunDiff, Proofline, the public schema, the installed-wheel
smoke, and the release performance gate.

### What we learned

The first implementation targeted CPython's historical private
`_ensure_future` helper. Current 3.12 folds that logic into the public
`ensure_future` function, so activation became incomplete and captured nothing.
Wrapping every call to the tasks-module function then captured `asyncio.run`'s
main and shutdown tasks because the application module frame was still active.
The reliable boundary is a split wrapper: top-level public entry points label
the synchronous scheduling call, while the tasks-module wrapper records only
when that label is present. A thread-local label is required here; a
`ContextVar` would be copied into the newly created task and could misclassify
later scheduling.

### Evidence

- A regression first observed only two explicit tasks in a five-task workload;
  it now retains two explicit, two gather-created, and one ensure-future task.
- Passing existing explicit tasks through both `gather` and `ensure_future`
  does not add operations, while Passive and Deep workload output remains
  identical.
- The broader repository example retains eight tasks across four adapters with
  exact application callers and one safe `RuntimeError`; RunDiff sees `8 → 10`
  and Proofline rejects the amplification at a 9.6 limit.
- The optional-client matrix now exposes the two scheduler tasks behind its
  pre-existing `gather(coro, coro)` call without hiding the two underlying
  Redis operations.
- The 200-task PR gate divides work evenly across all four adapters, retains all
  200 operations, and measured 2.001x Deep/passive whole-workload time under
  the existing 5x ceiling.
- The same installed observer retained eight complete tasks across all four
  adapters on local CPython 3.12, 3.13, and 3.14 interpreters.

### What remains uncertain

Direct loop scheduling, direct `asyncio.tasks` submodule calls, custom task
factories, and alternate event loops remain generic Deep evidence. Implicit
task timing begins just after CPython returns the new task, so it excludes the
small synchronous creation cost while retaining queueing and await time.

### Next highest-value action

Qualify the built wheel and full release suite, then evaluate server-side work
boundaries or direct event-loop scheduling without weakening the trustworthy
task-count meaning.

## 2026-08-20 20:47 BST

### What changed

Deep now recognizes standard-library `wsgiref` inbound requests without
application instrumentation or handler replacement. Each bounded
`server.request` spans handler entry through response completion and retains
only numeric status, safe 5xx classification, PID, duration, and the exact
first application function. BatchScope diagnoses failures, RunDiff compares
request counts, and Proofline enforces the existing operation contracts.

### What we learned

Wrapping `BaseHandler.run` would have placed application calls below the
semantic observer's ignored frames and hidden them from generic Deep profiling.
Recognizing the exact `run` and `start_response` frames inside the existing
profile hook preserves both semantic request evidence and ordinary application
hotspots. The status is the only transient WSGI local read. A missing status
retains duration but makes the logical family partial, because otherwise exact
failure claims could silently miss a 5xx response.

### Evidence

- An unmodified local WSGI example returns 200 and 503. Deep retains both
  request-to-response boundaries, the exact `__main__.application` caller, and
  the same two calls in the ordinary Python hotspot.
- BatchScope emits `server_operation_failures`; RunDiff observes `2 → 3`; and
  Proofline rejects a 1.0x request-count contract at the exact limit of two.
- The runpack contains none of the route, request/response header, body, or
  client-address sentinels; schema and adversarial parser tests enforce the
  explicit false privacy markers.
- The 200-request no-op PR workload retains every boundary with no drops in
  0.560s of Deep workload time versus 0.060s Passive, a 9.276x ratio under the
  deliberately explicit 12x worst-case ceiling.

### What remains uncertain

The first adapter covers only standard-library `wsgiref`. Gunicorn, uWSGI,
Waitress, ASGI servers, custom gateways, and replaced handlers remain generic
Deep evidence. WSGI duration includes response iteration and transmission, not
isolated application CPU time. The shared 256-record per-process budget can
truncate a request-heavy service quickly.

### Next highest-value action

Qualify the installed wheel and full suite, then add one production-relevant
WSGI or ASGI server adapter using the same control-flow recognition pattern
without collecting routes, headers, or bodies.

## 2026-08-25 15:53 BST

### What changed

Deep now recognizes Uvicorn's h11 and httptools inbound HTTP request cycles
without importing Uvicorn, installing middleware, or changing the application.
Each bounded operation reuses `server.request` and
`request_to_response_completion`, ending only after the final response-body
send returns. It retains adapter, numeric status, safe 5xx classification,
PID/role, timing, and the exact first application function. WebSocket protocol
lifecycles are deliberately ignored.

### What we learned

The WSGI thread-local active-frame design cannot be copied into asyncio:
concurrent request tasks interleave on one event-loop thread. Exact Uvicorn
`run_asgi` frame and transient request-cycle identities provide the stable
boundary, while ancestor frames identify the first application call directly.
The send message needs only three transient fields—`type`, numeric `status`,
and `more_body`—to distinguish response start from final-body completion. No
ASGI scope or message needs to cross the observer boundary.

Coroutine profile callbacks also report `return` when an await suspends. A
terminal-return check is therefore required before closing either `send` or
`run_asgi`; otherwise the first streamed-body await truncates timing and can
drop the cycle before status normalization.

### Evidence

- Unmodified Uvicorn 0.52.4 h11 and httptools applications produced identical
  Passive and Deep output for concurrent streamed 200 and 503 requests.
- Each engine retained exactly two HTTP operations with exact
  `__main__.application` callers, one `HTTPStatusError`, and no duplicate lower
  adapter. The streamed 200 remained open for at least the deliberate 50ms
  delay.
- A real WebSocket upgrade exercised the same application but produced no
  `server.request`. A completed synthetic record without status retained its
  interval and made logical evidence partial.
- BatchScope diagnosed the 5xx; RunDiff observed `2 → 3`; Proofline rejected a
  1.0x request-count contract; and the format-version-1 schema accepted both
  additive adapter identities.
- Method, path/query, ASGI scope-derived header/body/client sentinels, response
  headers/bodies, arguments, locals, and the application exception-message
  sentinel were absent from the runpack.
- The 200-cycle dependency-free PR gate retained both engine identities with no
  drops at 0.216s Deep versus 0.103s Passive, a 2.109x whole-workload ratio
  under the explicit 15x expensive-tier ceiling.
- Real Uvicorn 0.30.6 and 0.52.4 focused suites pass for both engines. The
  latest suite also passes on managed CPython 3.12.12, 3.13.11, and 3.14.2;
  installed-wheel smoke exercises both adapter identities from a clean
  workload environment.

### What remains uncertain

The adapter qualifies Uvicorn's HTTP/1.1 h11 and httptools request-cycle shape,
not other ASGI servers, HTTP/2 implementations, custom protocol classes, or
methods replaced after import. Request duration includes application awaits,
streaming, and Uvicorn protocol send work; it is not application CPU time or a
network-flush latency guarantee. The shared 256-record process budget can still
truncate a busy service quickly.

### Next highest-value action

Exercise the adapter against real production middleware stacks and sustained
concurrency before considering another server. Keep WebSocket semantics as a
separate future contract rather than treating them as HTTP requests.

## 2026-08-25 16:34 BST

### What changed

Evidence integrations now share a typed provider contract and lazy registry.
OpenTelemetry, Kubernetes, Prometheus, and Temporal each have one canonical
package under `runtime_tools.providers.builtins`; the core CLI discovers their
commands instead of branching on provider names. External distributions can
publish `contrail.providers` entry points and remain disabled and unimported
until explicitly selected.

The public documentation was reorganized around the product journey: see the
failure, run the demo, capture evidence, enforce a contract, then extend the
tool. The README now uses real local-UI screenshots, while long architecture,
capture, compatibility, machine-output, support, security, CI, and performance
material is split into concise task and reference pages.

### What we learned

An extensible provider boundary needs more than a protocol. Selection,
discoverability without import, command ownership, collision failure, safe
errors, packaging, and an installed-wheel example all belong to the contract.
Keeping integrations behind a host-side boundary lets contributors normalize
new evidence without weakening the standard-library-only workload observer.

Maintaining top-level compatibility modules would create two obvious import
paths and obscure the structure contributors should copy. During the beta, one
clean canonical surface is more valuable than preserving those internal-era
module locations. Artifact and structured-output compatibility remain separate
public commitments.

### Evidence

- A temporary installed distribution is discoverable without importing its
  module, becomes executable only when enabled, and adds a CLI command without
  modifying the core parser or dispatcher.
- Registry tests reject unknown selections, key conflicts, duplicate commands,
  malformed providers, and collisions with core commands. Unexpected provider
  failures expose only a safe exception class, never the message.
- Built-ins can be disabled independently while core capture and analysis stay
  available.
- The external hello-provider template installs through a standard entry point;
  installed-wheel smoke exercises the same boundary outside the checkout.
- The architecture checker enforces the provider layer, and focused provider,
  CLI, artifact, and packaging suites pass before full qualification.

### What remains uncertain

The first extension boundary adds host-side commands only. Providers do not yet
contribute UI panels, new normalized record schemas, workload-side capture
hooks, or persistent configuration. Those are separate product and trust
decisions, not incidental follow-ons to command discovery.

### Next highest-value action

Validate the modular boundary against a real third-party evidence source and
use contributor feedback to decide whether providers need shared importer
utilities beyond the current atomic enrichment helpers.
