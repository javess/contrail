# Compatibility policy

This policy defines the `.runpack` compatibility boundary for the 0.9 release
line. Package versions and runpack schema versions are independent: a newer
Contrail package does not imply a new artifact schema.

## Runpack schema 1

Contrail 0.9 writers create schema 1.1 runpacks. Read-only commands accept a
schema with major version 1 when the required core tables, columns, keys,
relationships, value bounds, and single-file SQLite safety rules remain valid.
Readers ignore additional tables and columns, so a structurally valid additive
future 1.x schema remains inspectable.

Read compatibility does not grant write compatibility. Enrichment and other
writer operations accept only the schemas whose mutation semantics this release
knows: `1`, `1.0`, and `1.1`. They reject an unknown future 1.x schema before
writing, even when the read-only validator can inspect it. This prevents an old
writer from discarding or contradicting requirements introduced by a newer
producer.

Schema 1.1 adds the optional `attachments` table. Adding attachments to a
schema `1` or `1.0` artifact atomically creates the table and upgrades the
manifest to `1.1`. If attachment validation or insertion fails, both the table
creation and manifest change roll back.

Post-exit capture recovery does not change the SQLite schema. Its state is an
additive object in the existing execution metadata JSON, so legacy schema-1
readers continue to inspect recovered artifacts and may ignore that provenance.
The temporary `pending` and `assembled` states are controller checkpoints, not
published runpack states; current writers publish only `complete` recovery
metadata.

The CLI capture-worker lifecycle is also additive execution metadata.
`capture.worker` may be absent in older artifacts and direct Python API output;
schema-1 readers must ignore it when unknown. Its version-1 fields identify the
separate-process mode and whether frontend loss was observed before final
commit. They do not change execution outcome semantics or the runpack schema.

Capture-job registry files are private ephemeral control state, not portable
runpack schema. Their internal integer format version is not a public exchange
format. The `runtime.capture_job` and `runtime.capture_jobs` CLI documents are
instead additive members of machine-output format version 1 and follow its
ignore-unknown-fields rule. Their job states and exit-status semantics are
documented in `docs/machine-output.md`.
Detached jobs add `detached` and a bounded `output` summary to those documents.
Both are additive format-version-1 fields; consumers of earlier job documents
must continue to tolerate their absence.
Head-and-tail retention adds strategy, segment-size, and omitted-byte fields
inside that existing summary. These are additive format-version-1 fields;
`stdout_size_bytes`, `stderr_size_bytes`, and both truncation flags retain their
meaning as total retained size and incomplete-output indicators. The private
reader accepts earlier head-only job records and does not require tail files for
them.
The optional `job output --follow` flag changes only when raw retained bytes are
replayed; it adds no field or document type and preserves the existing output
command's stream and exit-status contract.

Automatic subprocess capture adds `subprocess.run` events and a nested
`capture.instrumentation.semantic_capture` metadata object to newly produced
sample and deep runpacks. Both are additive schema-1 evidence. Older runpacks
without that object remain readable and report semantic capture as unavailable.
RunDiff format version 1 adds per-side semantic-capture status and dropped-count
fields; existing consumers must ignore them when unknown. BatchScope likewise
adds optional `semantic_capture` and `subprocess_calls` members. No existing
field changes meaning.

New writers may also add `python.callsite` events and `launches` causal edges.
The nested BatchScope semantic summary adds optional `caller_attribution`, and
each subprocess call adds an optional nullable `caller`. Older runpacks and
format-version-1 documents without these additions remain readable and report
caller attribution as unavailable. Callsite events are excluded from RunDiff
operation facts, preserving existing operation semantics.

Automatic HTTP capture is another additive schema-1 family. New runpacks may
contain `http.client.request` events, `requests` edges, and an independent
`capture.instrumentation.http_capture` metadata object. BatchScope adds optional
`http_capture` and `http_requests` members. RunDiff format version 1 adds
nullable per-side HTTP status and dropped-request fields. Existing runpacks and
documents remain readable and report HTTP capture as unavailable. HTTP events
are operation facts; their callsites are provenance and remain excluded from
operation facts.

The HTTP summary may add an `adapters` array and each request may add an
`adapter` string. Current writers use `stdlib.http.client`, `httpcore.sync`,
`httpcore.async`, or `aiohttp.async`; readers continue to interpret either
missing field as the legacy standard-library adapter. These are additive
schema-1 fields and do not change the `http.client.request` operation identity.

Automatic connection capture is a third additive schema-1 family. New runpacks
may contain `network.connect` events, `connects` edges, and an independent
`capture.instrumentation.network_capture` metadata object. BatchScope adds
optional `network_capture` and `network_connections` members. RunDiff format
version 1 adds nullable per-side connection status and dropped-connection
fields. Older evidence remains readable and reports connection capture as
unavailable. Connection events are operation facts; their callsites are
provenance and remain excluded from operation facts.

BatchScope may additionally emit `network_connection_hotspots`, a bounded
derived array grouped by caller and adapter. It is additive format-version-1
presentation data over existing connection events; older consumers can ignore
it, and its absence does not change the meaning of `network_connections`.

Network setup capture is a fourth additive schema-1 family. New runpacks may
contain `network.resolve` and `network.tls_handshake` events, `resolves` and
`handshakes` edges, and a `capture.instrumentation.network_setup_capture`
metadata object. BatchScope adds optional `network_setup_capture`,
`network_setup_phases`, and `network_setup_hotspots` members. RunDiff format
version 1 adds nullable per-side setup status and dropped-phase fields. Older
runpacks and reports remain readable and treat the family as unavailable. Setup
events are operation facts; setup callsites are provenance and stay excluded
from operation facts.

Deep logical-operation capture is a fifth additive schema-1 family. New
runpacks may contain `database.*`, `cache.*`, `queue.*`, `broker.*`,
`executor.task`, `scheduler.task`, or `server.request` events,
`performs` edges, and a `capture.instrumentation.logical_operation_capture`
metadata object. The cache and broker categories, their generic operation
classes, the executor/scheduler categories and
`submission_to_completion`/`creation_to_completion` duration boundaries, and
the SQLAlchemy/Redis/Pika/aiokafka/concurrent-futures/asyncio adapter
identifiers—including the additive top-level `ensure_future` and `gather`
identifiers—plus the server category, request operation,
`request_to_response_completion`, nullable `status_code`, `stdlib.wsgiref`,
`uvicorn.h11`, and `uvicorn.httptools` adapters, and server privacy markers
extend the same family; older readers that
only understand the initial
database/queue values may reject those new events rather than misclassify them.
Additive false privacy markers state that callables, awaitables, task names,
context values, arguments, HTTP methods, routes, URLs, headers, bodies, and
client addresses were not captured. Existing artifacts without those markers
remain readable.
BatchScope adds optional `logical_operation_capture`, `logical_operations`, and
`logical_operation_hotspots` members. RunDiff format version 1 adds nullable
per-side logical-operation status and dropped-operation fields. Older runpacks
and reports remain readable and treat this Deep-only family as unavailable.
Logical operations are operation facts; their callsites are provenance and
stay excluded from operation facts.

Deep native-call profiling extends the existing `python.call.aggregate` shape
additively. New events may use a synthetic `<native>` filename and add
`implementation` plus `exception_count`; per-process contributions add the same
exception count, and instrumentation/BatchScope summaries may add
`native_call_capture`. Older runpacks omit those fields and continue to decode
as Python implementation with zero native exceptions and unavailable native
summary. `python.call.aggregate` and `calls` remain profiler facts excluded from
RunDiff/Proofline operation counts. Python `self_seconds` keeps its meaning as
time outside observed children; newer writers observe native children that an
older profiler attributed to the Python parent, so cross-version hotspot
distribution is not an observer-free performance comparison.

Python exception profiling extends the same aggregate additively. New Python
events may have nonzero `exception_count`, including a count greater than
`call_count`; per-process contributions follow the same rule. Newer writers may
also add `non_control_flow_exception_count` to Python aggregates and process
contributions, plus a `control_flow_filter` inside
`python_exception_capture`. Version 1 excludes exact built-in iterator-control
type identities while retaining the raw `exception_count` unchanged. Older
runpacks retain zero Python exception counts and omit the summary; intermediate
runpacks may have raw counts but no filter. Both remain readable. These events
remain profiler facts excluded from RunDiff and Proofline operation/error
counts; they are not unique application failures.

BatchScope may add the additive `python_exception_churn` bottleneck
classification when new Deep exception evidence and observer integrity are
complete, the versioned control-flow filter is present, and raw and diagnostic
counts are internally reconciled. The evidence string explicitly says built-in
iterator completion is excluded and propagation events are not unique failures.
Legacy artifacts, which lack the required filter provenance, remain readable
and do not produce this diagnosis. The runpack schema and the generic
bottleneck JSON shape are unchanged.

New Deep metadata may add `observer_integrity` with format version 1, process
coverage, and detected profile/trace-hook setter-call counts. Older internal
profile documents omit the object and normalize it as unavailable; older
runpacks omit the public summary and remain readable. A replacement changes
completeness, not the meaning of already retained `python.call.aggregate`
facts. Hook call counts are diagnostic provenance and remain excluded from
RunDiff and Proofline operation facts.

All schema versions remain subject to the artifact safety rules: readers reject
unknown major versions, missing required structure, invalid relationships,
executable triggers, and WAL-mode databases that may depend on unshipped
sidecars. Additive compatibility does not weaken those checks.

## Change rules

- Adding an optional table or nullable column may use a new schema 1.x minor.
- Changing the meaning or type of an existing field, making optional evidence
  required, or removing a field requires a new schema major.
- A producer must update the manifest in the same transaction as the first
  change that requires a newer schema minor.
- Runpack compatibility is logical. SQLite page layout and file bytes are not a
  stable serialization contract.

Historical schema 1 and 1.1 fixtures live in
`tests/fixtures/compat/`. Their provenance and SHA-256 identities are checked in
with the fixtures, and the compatibility tests materialize fresh databases from
the immutable SQL sources.

## Explained-report bindings

Explained Proofline verification and experiment documents may carry
`artifact_bindings` for the exact baseline and candidate runpack bytes. Each
binding is a SHA-256 digest plus byte size. This is an additive format-version-1
field: legacy binding-less version-1 reports remain readable. The retained UI
labels assertion-bearing legacy reports as semantic replay against current
evidence, and gives assertion-less report-authored policy/results a still lower
assurance. Non-explained JSON and text output are unchanged.

A binding is a file identity, not a logical runpack identity. SQLite `VACUUM`,
enrichment, attachment changes, or any other reserialization can change the
digest or byte size even when selected normalized facts remain equivalent. Such
an artifact is intentionally a different identity and requires a newly
generated explained report. This does not change the schema compatibility rule
above: compatible runpacks need not have stable bytes.
