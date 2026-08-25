# Compatibility policy

Contrail has separate compatibility surfaces for runpacks, structured CLI
documents, contracts/reports, and Python APIs. Package and artifact versions do
not advance together.

This is the policy for the `0.9` beta. Human terminal layout and browser-internal
payloads are not stable contracts.

## Runpack schema 1

Contrail `0.9` writes schema `1.1`.

Read-only commands accept a structurally valid schema with major version `1`.
Readers ignore additional tables and columns after validating required core
tables, columns, relationships, bounds, manifest keys, and single-file SQLite
safety. Unknown major versions are rejected.

Write compatibility is narrower. Enrichment accepts only `1`, `1.0`, and `1.1`;
an older writer must not mutate an unknown future `1.x` artifact. Schema `1.1`
adds optional attachments. Adding an attachment to `1`/`1.0` creates the table
and upgrades the manifest atomically.

Additive execution metadata—capture level, worker lifecycle, recovery,
observer completeness, transport metrics, and provider provenance—does not
change the SQLite schema. Older readers may ignore unknown metadata keys.

Private checkpoints, capture-job registry files, ranking databases, and worker
control files are not portable runpack formats.

## Structured output version 1

Every JSON document has `document_type` and string `format_version`. Current
documents use format version `1`.

Within version `1`:

- fields and enum values may be added;
- consumers must ignore unknown fields;
- existing field meaning and type do not change;
- unknown or incomplete evidence uses null or explicit completeness/status;
- non-finite JSON numbers are forbidden.

Document-specific schemas live in
`runtime_tools/schemas/contrail-output-v1.schema.json`. See
[machine-readable output](machine-output.md) for discriminators and exit codes.

The `runtime.capture_job` and `runtime.capture_jobs` documents are public; the
on-disk job registry that backs them is not.

## Capture additions

New capture adapters may add normalized events, relationships, metadata
summaries, and derived analysis arrays without changing schema major version.
The compatibility requirement is evidence monotonicity:

- existing normalized fields keep their meaning;
- privacy false-markers remain false;
- legacy absence becomes unavailable, never complete;
- malformed new-family data degrades that family without discarding unrelated
  valid evidence;
- exact Proofline claims require complete evidence on both compared sides once
  either side enables that family.

Adapter identifiers and operation kinds are extensible strings. Consumers
must not treat an unknown provider or adapter as malformed solely because it is
new.

## Contracts and explained reports

Proofline contract YAML is versioned by its validated assertion vocabulary.
Unknown assertion types or fields are rejected rather than ignored.

Explained JSON reports include canonical resolved assertions, evidence facts,
JSON-pointer paths into a nested RunDiff document, and bindings for the exact
baseline/candidate runpack size and SHA-256. The local UI reopens and hashes the
runpacks, re-evaluates the assertions, and requires the retained facts/results
to match before labelling the report artifact-bound.

Binding proves that a report refers to supplied bytes; it does not authenticate
an attacker-controlled bundle. Signing and trusted retention are separate
concerns.

Older binding-less version-1 reports remain readable at explicitly lower
assurance when their available policy/result can be replayed.

## Python APIs

The supported read API is:

```python
from runtime_tools import open_runpack

with open_runpack("candidate.runpack") as runpack:
    execution = runpack.execution()
    events = runpack.events()
```

The stable `Runpack` methods are `manifest()`, `execution()`, `entities()`,
`events()`, `causal_edges()`, `measurements()`, and `attachments()`. Normalized
record types are exported from `runtime_tools.runpack`.

Provider extension types are exported from `runtime_tools.providers`. Built-in
implementations have one canonical path under
`runtime_tools.providers.builtins`; this beta does not retain earlier
top-level provider-module aliases.

Internal storage, writer, analysis, bootstrap, and command helpers are not
public APIs merely because Python can import them.

## Change rules

An additive change needs tests, schema/document updates where applicable, and a
changelog entry.

A breaking artifact or structured-output change needs:

1. a new major format/schema discriminator;
2. explicit reader and writer behavior;
3. a migration or coexistence story;
4. fixtures for old and new versions;
5. release notes and user approval.

Human prose, spacing, color, and ordering may change without a format version.
Automation must use JSON or the Python reader, not parse terminal output.
