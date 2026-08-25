# Threat model

Contrail is a local evidence tool, not a sandbox or data-loss-prevention
system. Its goals are to preserve existing files, publish artifacts atomically,
bound work on untrusted evidence, keep capture private by default, and never
turn missing evidence into a confident fact.

## Trust boundaries

Treat these as untrusted input:

- `.runpack`, OTLP, Kubernetes, Prometheus, Temporal, JSON, JSONL, and YAML;
- Proofline contracts and explained reports;
- installed third-party provider entry points;
- browser-visible event attributes and log bodies.

Parsers enforce byte, count, depth, field, and archive-path limits; reject
ambiguous duplicate keys where relevant; and normalize failures to bounded
public errors. Runpacks are SQLite databases parsed by the SQLite library in
the selected CPython, so keep CPython security updates current.

Workloads and Git refs executed by Proofline are trusted arbitrary code. They
run with the invoking user's filesystem, network, environment, and credentials.
Contrail isolates evidence and cleanup, not authority.

Providers run in the Contrail host process after explicit enablement. Install
and enable only trusted distributions. Provider metadata can be discovered
without importing the implementation, and third-party providers are disabled
by default, but execution is not sandboxed.

## Filesystem safety

Capture and enrichment write to private staging paths, validate the completed
snapshot, and publish with no-clobber semantics. Existing destinations are not
overwritten. Completed runpacks are opened read-only for analysis.

Enrichment copies one validated source snapshot before correlation, preventing
path replacement from combining two source generations. Temporary session,
checkpoint, ranking, and job-registry files use same-user private locations and
bounded formats. Recovery is local and requires the original retained files.

No-clobber publication relies on a qualified local filesystem with atomic hard
links. Network, synchronized, FUSE, and object-backed mounts are outside the
release boundary.

## Capture privacy

Default capture does not store output content or environment values. Explicit
attachments can contain anything the workload wrote and must be reviewed
before sharing.

Zero-touch semantic capture is intentionally lossy. It may retain timing,
bounded adapter and operation classes, process identity/role, numeric ports or
HTTP status where documented, safe outcome/exception class, and exact or
sampled application caller. It does not retain:

- function arguments, locals, return values, or exception messages;
- subprocess arguments, environment, or working directory;
- SQL, parameters, cache keys, queue items, broker destinations, or payloads;
- executor callables/arguments or asyncio awaitables, names, and context;
- hostnames, resolved addresses, certificate content, credentials, or payloads;
- inbound HTTP method, route/path, URL/query, headers, bodies, ASGI scope,
  client address, arguments, or locals.

Deep's exception observer transiently receives CPython exception objects but
retains only counts and completeness. It does not read or store the type name,
value, message, traceback, arguments, or locals. The control-flow filter checks
only exact built-in type identity.

For outbound HTTP/network evidence, a numeric port and library adapter can
still be sensitive in a particular environment. Generic Python/native function
identity can reveal dependency or code structure. Privacy-minimal does not mean
anonymous.

Inbound WSGI/Uvicorn capture retains only adapter, timing, numeric status, safe
5xx classification, PID/role, and exact application caller. Uvicorn send
messages are read transiently only for `type`, numeric `status`, and
`more_body`; scope and messages are never copied into evidence. WebSockets are
ignored.

## Imported and explicit evidence

Imported telemetry is not automatically redacted. OTLP attributes and log
bodies, Kubernetes object names, Prometheus label values, Temporal payloads,
annotation values, output attachments, and raw adapter inputs may contain
secrets. Contrail preserves selected evidence; it does not discover or remove
all sensitive material.

Runpacks are plaintext SQLite files. Hash binding detects byte substitution but
does not encrypt, authenticate, or establish custody. Use operating-system
permissions, encrypted storage, trusted artifact retention, and signing or
attestation where the risk requires them.

## Local UI and queries

The UI binds only to loopback, serves immutable snapshots, and does not load
remote assets. Values are rendered as text rather than injected HTML. Loopback
does not protect against another same-user process or a malicious browser
extension; stop the server when finished.

Query input is parsed into a restricted AST with allowlisted fields/operators,
row limits, expression depth limits, and a virtual-machine step budget. It is
not interpolated as SQL. This is a bounded inspection surface, not a general
database console.

Explained reports bind to runpack byte size and SHA-256 and are semantically
replayed by the local server. If an attacker can replace both report and
runpacks, binding alone provides no authenticity. CI retention, signatures, or
attestations supply stronger provenance.

## Availability and completeness

All public inputs and observer families have hard budgets. Limit exhaustion,
collector loss, malformed snapshots, hook displacement, missing status, and
unsupported adapter shapes become `partial`, `truncated`, `invalid`, or
`unavailable`. Exact Proofline claims then become unverifiable.

Process observation invokes the host's `/bin/ps`; Sample and Deep inject a
standard-library-only bootstrap into supported CPython children. A hostile
workload can disable startup, replace hooks, kill controllers, tamper with
private files, emit extreme output, or terminate before checkpointing. Contrail
records detectable loss but does not defend against same-user hostile code.

Detached output is drained continuously and retains at most 1 MiB per stream.
The capture registry and cleanup scans are bounded. These controls limit storage
growth but do not prevent a workload from consuming CPU, memory, disk, network,
or inherited resources.

## Non-goals

Contrail does not sandbox workloads or providers, authenticate UI users,
encrypt artifacts, guarantee redaction or secure deletion, defend against a
hostile local administrator, capture native programs comprehensively, or
provide remote collection and multi-tenant isolation.

Report vulnerabilities privately using [SECURITY.md](../SECURITY.md).
