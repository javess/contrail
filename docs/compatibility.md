# Compatibility policy

Contrail has separate compatibility surfaces for runpacks, structured CLI
documents, contracts/reports, and Python APIs. Package and artifact versions do
not advance together.

This is the policy for the `0.10` beta. Human terminal layout and private
Python modules are not compatibility contracts.

## Runpack schema 1.1

Contrail `0.10` writes schema `1.1`.

Readers and writers accept exactly schema `1.1`. Earlier and later versions are
rejected; this beta keeps one current artifact layout instead of carrying
migration branches. Required tables, columns, relationships, bounds, manifest
keys, and single-file SQLite safety are still validated before use.

Additive execution metadata—capture level, worker lifecycle, recovery,
observer completeness, transport metrics, and provider provenance—does not
change the SQLite schema.

Private checkpoints, capture-job registry files, ranking databases, and worker
control files are not portable runpack formats.

## Structured output version 2

Every JSON document has `document_type` and string `format_version`. Current
documents use format version `2`.

Within version `2`:

- fields and enum values may be added;
- consumers must ignore unknown fields;
- existing field meaning and type do not change;
- unknown or incomplete evidence uses null or explicit completeness/status;
- non-finite JSON numbers are forbidden.

Pydantic generates schemas directly from the typed output models; there is no
separate handwritten aggregate schema. See [machine-readable
output](machine-output.md) for discriminators and exit codes.

The `runtime.capture_job` and `runtime.capture_jobs` documents are public; the
on-disk job registry that backs them is not.

## Capture additions

New capture adapters may add normalized events, relationships, metadata
summaries, and derived analysis arrays while schema `1.1` remains current.
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
baseline/candidate runpack size and SHA-256. Report producers calculate those
bindings from the runpacks used for verification.

Binding proves that a report refers to supplied bytes; it does not authenticate
an attacker-controlled bundle. Signing and trusted retention are separate
concerns.

## 0.10 terminal-only transition

The `contrail serve` command and the importable
`runtime_tools.ui` package were beta surfaces and are removed in `0.10` with
explicit user approval. Use `compare`, `analyze`, `inspect`, `verify
--explain`, `report`, and `query` instead. Existing runpacks and explained JSON
reports do not require runpack migration. Structured JSON moved to format 2
during the same explicitly breaking consolidation. A future UI must consume
those public artifacts rather than revive
the removed internal browser payload.

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

Evidence integrations are fixed built-ins under
`runtime_tools.providers.builtins`. Third-party provider discovery and its
public extension protocol were removed during consolidation.

Internal storage, writer, analysis, bootstrap, and command helpers are not
public APIs merely because Python can import them.

## Change rules

An additive change needs tests, schema/document updates where applicable, and a
changelog entry.

A breaking artifact or structured-output change needs an explicit discriminator,
reader/writer behavior, documentation, tests, and release notes. During the
beta, Contrail does not carry migrations or coexistence code unless a concrete
user need justifies it.

Human prose, spacing, color, and ordering may change without a format version.
Automation must use JSON or the Python reader, not parse terminal output.
