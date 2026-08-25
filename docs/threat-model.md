# Threat model

## Security goals

Contrail should preserve the user's existing files, avoid publishing partial
artifacts, bound work performed on imported evidence, keep local evidence local
by default, and represent missing or ambiguous evidence without inventing facts.
Completed runpacks are opened read-only for analysis. Enrichment copies a
validated snapshot and publishes a new artifact without overwriting an existing
filesystem entry. Adapter correlations and time-window decisions are derived
from that copied snapshot, so source-path replacement cannot publish a hybrid
of two valid runpack generations.

## Trust boundaries

Exported OTLP, Kubernetes, Prometheus, Temporal history, YAML, JSONL, Proofline report, and
`.runpack` files may be untrusted. Parsers enforce byte/count/depth limits,
reject duplicate object keys where ambiguity matters, normalize malformed input
to public domain errors, and must not leave a partial destination. A runpack is
still a SQLite database: it is parsed by the SQLite library bundled with
CPython. Keep CPython security updates current when processing evidence from an
untrusted source.

Runpack connections install a SQLite value-length ceiling before validation.
After the required schema is established, validation counts every normalized
table and computes per-field plus whole-artifact text, JSON, and attachment byte
bounds inside SQLite. Python record access, Proofline, RunDiff, BatchScope, and
the timeline therefore receive rows only after the same snapshot passes this
preflight. Oversized table detection stops before content aggregation, and
descriptor-bound report hashing rejects an over-limit complete file before
reading its bytes. The exact limits are listed in the
[performance policy](performance.md).

The command passed to `runtime record`, and workloads executed by Proofline, are
trusted arbitrary code. Isolation is for reproducible evidence and cleanup, not
a security sandbox. A workload has the invoking user's filesystem, network, and
credential access. Git refs and repository contents used by Proofline must be
trusted to execute.

CLI capture starts a separate worker under the same user and with the same
environment and standard streams as the frontend. Its anonymous pipe carries
only client liveness. It is not a privilege boundary, daemon, authentication
channel, or durable remote handoff. Abrupt frontend loss intentionally lets the
worker and trusted workload continue; a closed or unusable inherited output
descriptor can still prevent output relay. An explicit Ctrl-C is forwarded as
cancellation. Same-UID processes remain able to inspect, signal, or interfere
with both processes.

`--detach` is also an explicit output-retention choice. It stores the first
512 KiB and most recent 512 KiB of workload and controller stdout and stderr in
mode-0600 files and may therefore retain credentials, personal data, or
application payloads. Middle bytes are discarded, not redacted. Tail
checkpoints rewrite one locked bounded file in place so no second raw-output
snapshot is retained on disk. `job output` replays the raw retained
bytes and should be treated like replaying the original process output.
`job output --follow` repeatedly exposes new retained bytes with the same risk;
it adds neither redaction nor retention. Interrupting that reader does not
cancel the worker. Attached capture does not create these files. Registry
expiry removes them with the job, but same-UID processes and host administrators
remain able to read or alter them within the existing trust boundary.

Capture-job discovery uses a mode-0700 same-user directory, atomic mode-0600
state files, and advisory locks. Records deliberately omit workload commands
and arguments but retain operation names and up to four absolute artifact
paths, which may still reveal sensitive filesystem structure. `job cancel`
writes to a private named pipe opened by the live worker; the worker signals
itself, so a liveness-check-to-signal race cannot target a reused PID or process
group. A reader holds the liveness lock while re-reading and committing a
`lost` transition, preventing it from overwriting a concurrent terminal write.
These controls do not defend against a hostile same-UID process that can replace
state or pipe entries, inherit or acquire locks, or send signals itself.
Terminal state is ephemeral local convenience, not authenticated audit
evidence.

Sampling and Deep Capture deliberately change Python startup by prepending a
private `sitecustomize` bootstrap through `PYTHONPATH`. They are opt-in and may
interact with an application's own startup customization. Python `-I`, `-E`, or
`-S` can prevent injection; missing output remains explicit rather than being
reported as complete. A trusted workload can read or alter its private profile
environment and files, so these modes provide diagnostic evidence, not an
attestation boundary.

Those modes also wrap the standard-library `subprocess.Popen` class methods in
each observed interpreter. The public class identity is preserved, and observer
errors are isolated from subprocess outcomes, but this remains workload
modification and may interact with code that replaces the same methods. The
observer deliberately omits argument vectors, environment mappings, working
directories, and output. It retains executable basenames, PIDs, timing, shell
use, exit codes, safe exception class names, and optional caller module,
qualified name, filename, and first line; executable and source names themselves
may still be sensitive. Caller capture never retains arguments or locals. Deep
callers come from the existing profiler stack, and sampled callers only from an
existing sampler observation of an active wait, avoiding a new synchronous
frame-audit surface. The evidence has the same trusted-workload and same-UID
tampering limits as profile snapshots.

Deep Capture's Python exception observer receives CPython's transient
`(exception type, value, traceback)` trace payload. It increments the raw count,
then compares only the type object's exact identity with three static built-in
objects: `StopIteration`, `StopAsyncIteration`, and `GeneratorExit`. The type
object, type name, value, message, and traceback are not copied or retained;
arguments and locals are also untouched, and line/opcode events are disabled.
The resulting diagnostic count excludes routine iterator/coroutine completion
but is not a general exception classifier: subclasses and all other types stay
counted. Function and source-file names plus exception frequency can still be
sensitive. A trusted workload or another debugger/coverage/profiling tool can
replace `sys.settrace`, suppress evidence, or change callback ordering. Neither
counter is a security monitor or proof that an exception was unique or
unhandled.

Observer-integrity detection records only that a public `sys` or `threading`
trace/profile setter was called and which process reported it. It does not read
the replacement callable, arguments, closure, globals, or locals. Hook-use
frequency and the fact that a process uses a debugger or coverage tool may
still be sensitive. A trusted workload can bypass this diagnostic through the
CPython C API, interpreter-state mutation, evidence-file tampering, or a custom
runtime; this is conservative completeness evidence, not tamper resistance.

The observer wraps selected `http.client`, `httpcore`, and aiohttp methods. It
retains adapter, method, scheme, numeric port, timing through response headers,
status or safe exception class, and optional caller. For aiohttp, a supplied URL
is transiently parsed only to extract scheme and port. Server names and
addresses, URL paths and queries, all request and response headers, request and
response bodies, and credentials are neither retained nor exposed to
normalization; the published server identity is always `<redacted>`. Method,
port, adapter, exception class, and caller source filename can themselves be
sensitive. The lazy import hook delegates to the standard loader and restores
it, but another tool that replaces the same client methods can change which
wrapper is outermost. The observer does not provide content redaction for
application spans, logs, annotations, or other explicit imports, and a trusted
workload can still tamper with its evidence.

The connection fallback wraps blocking CPython stream-socket connects and
asyncio TCP and Unix transport creation. It retains adapter, transport, address
family, numeric TCP port, asyncio TLS-request marker, timing, safe outcome or
exception class, and optional caller. It does not retain or publish the host or
IP address, Unix path, credentials, or exception message. Numeric ports,
adapter, exception class, and caller source filename can still be sensitive.
HTTP wrappers suppress their nested connection observation with task-local
state, and the snapshot sender suppresses its own private Unix socket. Native
extensions, raw nonblocking state machines, alternate event loops, or another
tool replacing the same methods can bypass or change this evidence. This is
diagnostic observation, not a network policy or exfiltration detector.

The setup observer also wraps standard-library name resolution and TLS
handshake methods. It retains phase, adapter, timing, safe outcome or exception
class, PID, and optional caller. It does not retain queried or returned names
and addresses, SNI values, certificates, credentials, TLS records, or
application payload. DNS/TLS exception class and caller filename can still be
sensitive. Both client and server TLS handshakes are visible because the
standard SSL objects do not expose a reliable direction marker at this
boundary. Native resolvers/TLS stacks, custom extension methods, or another
tool replacing the same methods can bypass or alter the evidence.

Deep logical-operation adapters receive SQL arguments, cache keys, broker
destinations and messages, queue items, and executor callables/arguments only
because the original Python call already does; the observer neither inspects
nor copies them. The executor callback reads Future cancellation state and, for
a completed failure, only the exception object's safe class name. It never
calls `result()` or retains the exception object/message. Published
evidence contains only operation category/class, adapter, timing, safe outcome
or exception class, PID, and caller. Statements, parameters, command names,
keys, destinations, messages, rows, queue items and identity, executor
callables/arguments, asyncio awaitables/task names/context values, payloads,
credentials, exception messages, and return values are excluded. The asyncio
callback uses public cancellation state and transiently reads CPython's stored
exception object only to derive a class name; it never calls `Task.exception()`
or changes the task's unhandled-warning state. Gather identifies newly created
tasks only by object identity; it never reads coroutine contents or existing
Future results. The `wsgiref` adapter observes handler control flow and reads
only the transient status argument long enough to parse a three-digit integer.
It does not inspect the WSGI environment, method, route, URL, headers, request
or response body, or client address. Status, duration, safe failure class,
application caller identity, and the fact that a request occurred may still be
sensitive. Operation
class, adapter, exception class, frequency, timing, and caller filename can
still be sensitive. The SQLite adapter substitutes standard subclasses for the
module's default connection and cursor factories; queue and optional-client
adapters replace mutable class methods. A task-local suppression marker avoids
duplicate evidence from nested wrapped layers but can also hide an application
operation deliberately invoked inside one outer client call. Application
monkeypatching or another observer can change wrapper order or bypass the
adapters. Custom SQLite factories, `_sqlite3`, `SimpleQueue`, native drivers,
unsupported client versions, custom Executor/Future implementations, custom
importers, and dynamically synthesized modules are coverage gaps, not proof
that no operation occurred. Executor timing spans submission through Future
completion and therefore includes queueing, serialization, execution, and
result relay; treating it as worker-only runtime would be unsafe.

Process-tree observation invokes the qualified host's `/bin/ps` from the
controller and filters results to the workload's POSIX process group. It stores
PIDs, parent PIDs, executable basenames, observation timestamps, RSS, and
cumulative CPU time; it does not retain descendant argument vectors or
environment values. Same-UID processes and host administrators can manipulate
the process table and remain outside the trust boundary. Detached processes are
not followed.

Processes running as the same operating-system user are outside the isolation
boundary. Descriptor identity checks, private directories, randomized names,
and inode-aware cleanup prevent accidental aliasing and common pathname races,
but cannot defend against a hostile peer with the same effective UID that can
inspect or manipulate the process. Root and host administrators are also out of
scope.

## Local UI and queries

The UI binds to loopback by default and validates loopback `Host` headers to
reduce DNS-rebinding exposure. Binding a non-loopback address is an explicit
choice to expose normalized evidence to reachable clients. The server has no
authentication or TLS and must not be placed directly on an untrusted network.
It is read-only, but event attributes and log bodies can contain secrets.

An imported Proofline report is size-bounded. Current explained reports bind
the SHA-256 and byte size of the exact descriptor-bound baseline and candidate
snapshots. The retained UI checks both identities before comparing the embedded
RunDiff, parsing assertions, and replaying every claim. This detects stale or
substituted runpacks even when their execution IDs and policy-visible facts are
unchanged; it also covers attachment and other whole-file byte changes.

Binding-less version-1 reports remain readable with downgraded assurance.
Assertion-bearing reports are replayed semantically against the current
evidence, while assertion-less reports retain the report-authored policy/result
label; reported passes remain visible rather than being upgraded to
artifact-bound assurance. None of these checks authenticates who created the
evidence. An actor able to rewrite both report and runpacks can make a new
internally consistent bundle. Use trusted CI retention, attestations, or
signatures when provenance matters.

`runtime query` uses a read-only SQLite connection, an authorizer, row/cell/result
limits, and a virtual-machine step budget. It is a bounded inspection surface,
not a general database shell. Direct `sqlite3` access bypasses those controls.

## Sensitive evidence

Command arguments and resolved working directories are always evidence. Do not
put credentials in argv or path names when a runpack may be shared. Selected
environment values and output streams are hashed by default; hashes of
low-entropy secrets can be guessed. Output attachments, raw adapter input, OTLP
log bodies, and normalized attributes are opt-in or imported content and may
contain plaintext secrets. Contrail is not a redaction or data-loss-prevention
system.

Temporal histories can contain workflow and activity payloads. The adapter
persists only selected identifiers, event timestamps, states, outcomes, and
attempt counts, but the exported source file remains sensitive evidence and
must be protected separately.

Sampling and Deep Capture store module names, qualified function names, source
filenames, and first-line numbers. These can reveal application structure and
local paths. Neither captures arguments, return values, local variables,
exception values or messages, or source content. Deep additionally stores the
bounded module and qualified name of CPython-visible built-ins and extension
functions, aggregate call timing, and native exception counts. These names can
reveal installed extension modules and method structure. Native identities are
derived from the callable object supplied by CPython's profile event; the
observer never retains the bound instance, arguments, result, or raised
exception.

Their merged aggregates also retain contributing PIDs, root/descendant roles,
and per-process counts and timing. When a PID uniquely matches process-tree
evidence, BatchScope may expose that process's executable basename and parent
PID. Reused or multiply observed PID generations are treated as ambiguous and
are not correlated.

Process coverage also retains the complete bounded set of reporting PIDs and
can list observed Python or PyPy executable basenames that did not report.
Executable-name recognition is a diagnostic heuristic, not an attestation: a
workload can rename a process, spoof a name, disable startup, or alter its
private profile environment.

Profile registration and periodic snapshots use a randomized, mode-0600 Unix
socket owned by the capture controller. The controller accepts a fixed header
and bounded payload, validates the claimed snapshot kind and a serialization
duration capped at one hour, atomically keeps only the latest document per
admitted PID, and reuses the normal JSON validation before normalization. At
most 128 distinct PIDs are admitted and at most 1,024 omitted PID identities are
remembered for diagnostics; further omissions are explicitly a lower bound.
Registration is empty and bounded, but it is synchronous so an interpreter
cannot enter user code before attempting to publish its capture provenance.
Socket failure falls back to a private atomic file. A trusted workload can
connect to or spoof this transport, including its timing metrics, and a same-UID
peer remains outside the isolation boundary; checkpointing improves crash
resilience, not evidence authenticity. New workload reports carry a publication
version, and a retained fallback adds its PID, snapshot kind, configured-socket
state, and failed socket duration. The loader bounds and validates these fields
independently, but they remain workload-authored claims; their presence proves
what the retained document says, not that a hostile workload attempted the
reported transport.

After collection, exact cross-process function and relationship ranking uses a
separate mode-0600 SQLite database inside the private capture session.
Candidate identities and scores are bound parameters, not executable SQL. The
database has journaling and memory mapping disabled, an 8 MiB page cache, and a
256 MiB page-count ceiling. Exact selection scans it with a bounded
20,000-function or 50,000-relationship heap instead of creating a separate
SQLite sort file. The database is deleted when the ranking connection closes.
A disk-full condition, unsupported capture storage, integer overflow,
identity-digest conflict, workspace-limit failure, or cleanup failure
invalidates the generated profile evidence instead of falling back to an
unbounded aggregation. This workspace is diagnostic scratch state and is never
included in the runpack. Its high-water mark covers the algorithm's only
disk-backed ranking scratch, but not the bounded in-memory heap or filesystem
allocation metadata. As with profile snapshots, the trusted workload knows the
private session path and remains capable of altering same-UID scratch state.

Capture presets do not change these trust boundaries. `sample` and `deep`
include process-table observation, so they combine the process metadata exposed
by `process` with the Python function identity exposed by their injected
observer. `passive` is the only preset that enables neither optional observer.

Process-tree executable basenames and parent relationships can reveal workload
structure even though full descendant command lines are excluded.

Runpacks inherit source permissions during enrichment. Users remain responsible
for destination directory permissions, backups, and secure deletion.

## Availability and filesystem behavior

Input bounds limit parser work but cannot guarantee a fixed expansion ratio or
memory use for every valid shape. Release benchmarks cover deterministic shapes;
malformed and over-limit inputs must fail in a controlled way. Deliberately
adversarial resource exhaustion beyond documented bounds is out of scope.

Atomic no-clobber publication depends on local POSIX hard-link semantics. The
temporary and destination entries are created on the same qualified filesystem.
Unsupported network or emulated filesystems may weaken locking, link, permission,
or durability behavior and should not be used for security-sensitive evidence.

Capture terminates descendants that remain in its POSIX process group after an
interruption. A workload can deliberately detach, retain the user's privileges,
and outlive capture. Proofline uses independent randomized worktree roots and
isolated annotation backing files so a surviving baseline descendant cannot
accidentally write into the candidate through a reused path. This does not make
the detached process untrusted-code-safe.

After workload exit, capture may retain a mode-0600 temporary runpack and a
mode-0700 Sample or Deep session when the capture worker is killed or final
publication fails. The temporary runpack already contains the command, working
directory, output identities, and any explicitly captured output attachments;
the profile session contains function identities and paths. Treat both as
sensitive evidence. Recovery validates the runpack, its versioned state, the
profile directory's device and inode, all persisted transport counters, and the
normal profile bounds before mutation. It publishes by the same atomic
no-clobber hard link as normal capture. Same-UID replacement remains outside the
threat boundary. Capture-worker loss before the post-exit checkpoint cannot be
recovered and does not cause an unfinished execution to be published. Loss of
the transient frontend is a different event: the worker continues and records
the observed disconnect in completed runpacks.

## Non-goals

Contrail does not sandbox workloads, collect live cluster telemetry, authenticate
UI clients, encrypt runpacks, redact secrets, guarantee secure deletion, defend
against same-UID attackers, or provide durable behavior on unqualified remote
filesystems.
