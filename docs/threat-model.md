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

Exported OTLP, Kubernetes, Prometheus, YAML, JSONL, Proofline report, and
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

## Non-goals

Contrail does not sandbox workloads, collect live cluster telemetry, authenticate
UI clients, encrypt runpacks, redact secrets, guarantee secure deletion, defend
against same-UID attackers, or provide durable behavior on unqualified remote
filesystems.
