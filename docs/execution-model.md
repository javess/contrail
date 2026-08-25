# Execution model

Contrail normalizes every evidence source into a small set of records. Providers
may add attributes and new string kinds, but analyses share this model.

## Records

| Record | Meaning |
|---|---|
| execution | one bounded observation with identity, time, command/revision metadata, outcome, and capture provenance |
| entity | a logical or physical participant; entities may form parent hierarchies |
| event | an instant or interval with semantic kind, name, optional entity, timing, and attributes |
| causal edge | a directed constraint or relationship between events, independent of timestamps |
| measurement | a numeric sample or aggregate with unit, optional time, entity, and attributes |
| attachment | optional bounded opaque bytes; never required for normalized analysis |

IDs are unique within one artifact. Repeatable identity belongs in semantic
keys—entity kind/name, operation kind/name, relationships, and selected
attributes—not opaque IDs.

String kinds are extensible. Adapter-specific correlation identifiers belong in
attributes. Composite generated IDs encode each component independently so
delimiter-bearing values cannot alias.

Attributes must be JSON-safe. Query-prominent concepts use typed columns;
provider detail stays in bounded JSON until it demonstrates a stable cross-source
meaning.

## Time and causality

Known times are UTC Unix nanoseconds. A timed event may also declare a clock
domain and uncertainty. Missing time stays null.

Ordering uses, strongest first:

1. explicit causal edges;
2. source-local sequence;
3. non-overlapping timestamp uncertainty;
4. unknown order.

Wall-clock order alone does not prove causality. BatchScope labels a critical
path observed only when selected intervals share a known clock domain and the
required evidence is complete; otherwise it labels the result inferred.

Concurrency is calculated only from complete intervals within one clock
domain. Missing time or mixed domains produce unavailable evidence, not zero.

## Normalized capture evidence

Capture presets are provenance. Analyses inspect the concrete observer metadata
rather than trusting the convenience label alone.

Sample profiles become untimed `python.stack.sample` events and `stack_parent`
edges. Counts are statistical observations; nominal time is count multiplied by
the sample interval and may exceed wall time across threads.

Deep profiles become untimed `python.call.aggregate` events and `calls` edges.
Python and native aggregates retain bounded identity, counts, total/self/max
time, process contribution, and safe native exception counts. They are summaries,
not execution intervals, and never enter critical-path or concurrency math.

Semantic boundaries normalize as operation events:

| Family | Event kinds | Caller edge |
|---|---|---|
| subprocess | `subprocess.run` | `launches` |
| outbound HTTP | `http.client.request` | `requests` |
| physical connection | `network.connect` | `connects` |
| DNS/TLS | `network.resolve`, `network.tls_handshake` | `resolves`, `handshakes` |
| database/cache/queue/broker | category plus generic operation | `performs` |
| executor/scheduler | `executor.task`, `scheduler.task` | `performs` |
| inbound HTTP | `server.request` | `performs` |

Caller provenance is an untimed `python.callsite` event containing bounded
module, function, filename, first line, and application/library/runtime scope.
Deep callers are exact. Sample callers exist only when the sampler observes the
active synchronous boundary and carry lower confidence.

Semantic events remain queryable and comparable. They stay outside BatchScope’s
application lifecycle and critical path unless explicit causal evidence makes
them part of the logical job; this avoids counting an application span and its
nested adapter twice.

`server.request` uses `request_to_response_completion`. A 5xx status is a safe
operation error with `HTTPStatusError`; missing trustworthy status makes the
family partial. Uvicorn streaming completes after the final body send returns.

The exact retained/omitted fields and supported boundaries are in
[capture and evidence](capture.md).

## Completeness

Observer metadata distinguishes:

- complete final snapshots;
- periodic or crash checkpoints;
- registration-only processes;
- unavailable or malformed families;
- truncated record/process bounds;
- missing caller, process-tree, status, or trace-hook integrity evidence.

Analyses carry these states forward. RunDiff reports capture status for both
sides. Proofline marks exact count or error claims unverifiable when the
required family is incomplete.

Per-process contribution arrays are bounded and reconcile to their aggregate.
A PID receives executable/parent metadata only when process-tree evidence
uniquely identifies it; PID alone is not trusted identity.

## Attachments

Attachments hold optional raw exports or opt-in output. They carry kind, name,
media type, bytes, and JSON-safe attributes. The current limits are 64 MiB per
attachment and 256 MiB per runpack.

Attachments may contain secrets and are not rendered by default. Normalized
analysis must not depend on them.

## Repetition and comparison

RunDiff uses exact matching only when execution IDs match. Distinct runs may use
structural matching when entity, operation, dependency, and causal shapes agree;
otherwise comparison falls back to aggregate semantic keys. Stable event IDs
across executions are never required.

Lifecycle selection favors top-level explicit stages before heuristics, avoiding
double-counting nested work. Throughput uses one explicit series, logical
parent, or entity at a time; unrelated counters are not merged.

## Query surface

Runpacks index event time, entity ownership, event kind/name, edge endpoints,
and measurement name/time. Use `contrail query` for bounded read-only SQL or the
public Python reader for normalized records. See [querying](querying.md) and
[machine output](machine-output.md).
