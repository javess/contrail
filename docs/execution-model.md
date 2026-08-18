# Execution model

## Records

An **execution** is one bounded observation. It has a stable identifier, name,
start and optional finish, command/revision metadata, and an outcome. A partial
artifact is valid while capture is in progress.

An **entity** is a participant. `kind` is an extensible string rather than a
closed enum so adapters can preserve new kinds without a schema migration.
Entities may have a parent entity, allowing both logical hierarchies (run,
stage, worker) and physical hierarchies (node, pod, container, process). Logical
and physical identity remain separate records connected by relationships.

An **event** is an occurrence. It has a start and optional finish, so the same
record supports instants and intervals. Events have a semantic `kind`, a
human-readable operation name, an optional owning entity, and attributes.
Adapter-specific identifiers such as trace and span IDs live in correlation
fields or attributes and do not become core identity.

A **causal edge** states that one event constrained or caused another. Causality
is stored separately from timestamps because clocks can disagree and async
relationships are not necessarily nested. Edge kinds include parent, follows,
publishes, consumes, schedules, and explicit links. The set is extensible.

A **measurement** is a numeric sample or aggregate with a name, value, unit,
timestamp, and optional entity. Measurements are separate from event attributes
because their volume and query patterns differ.

An **attachment** is optional opaque evidence such as a bounded log stream or
raw adapter input. It carries a kind, name, media type, bytes, and JSON-safe
attributes. Attachments are never required for normalized analysis and are not
rendered by default because they may contain secrets.

Attributes are JSON objects at adapter boundaries. Values must be JSON-safe;
query-prominent concepts graduate to typed columns only after demonstrated use.

## Time and uncertainty

Times are UTC Unix nanoseconds when known. Every timed record can include a
clock domain and an uncertainty in nanoseconds. Raw source timestamps may be
retained in attributes. A missing timestamp remains null rather than being
fabricated.

Process capture records the UTC start once and derives the finish from monotonic
elapsed time. Host clock adjustments during a command therefore cannot make its
execution interval run backward or disagree with its wall-time measurement.

Ordering uses the strongest available evidence:

1. explicit causal edges;
2. source-local sequence numbers;
3. timestamps whose uncertainty ranges do not overlap;
4. otherwise, unknown order.

Readers must not infer that a child happened after a parent solely because its
wall-clock timestamp is larger. Impossible timestamp orderings can be reported
as clock-skew evidence without deleting the causal relationship.

Observed maximum concurrency is calculated only among complete intervals in
the same clock domain. RunDiff takes the maximum observed within any one domain;
it never sums overlap across clocks that may be skewed. If any event in a
semantic operation group lacks an interval, concurrency for that group remains
unavailable rather than being reported as zero.

## Identity and repetition

Execution, entity, and event IDs are unique within an artifact. Generated IDs
are opaque. Repeatable logical identity belongs in semantic keys such as entity
kind/name, operation name, stage path, attributes, and source-local sequence.
RunDiff currently compares aggregate semantic keys rather than assuming IDs
survive repeated executions. Exact and structural matching remain future layers
over the same identity model.

## Query implications

The artifact indexes event times, entity ownership, event kind/name, edge
endpoints, and measurement name/time. These support the initial questions:

- a time window scans event interval bounds;
- dependency traversal starts from indexed edge endpoints;
- active entities derive from their events and lifecycle intervals;
- stages use explicit ownership/attributes before heuristics;
- critical paths use a causally connected graph subset;
- comparisons aggregate semantic keys rather than opaque IDs.

Confidence is part of derived analysis, not a replacement for evidence. Reports
must distinguish observed facts from inferred lifecycle phases or causal links.
