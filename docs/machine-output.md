# Machine-readable output compatibility

Contrail 0.9 freezes format version `1` for these documented JSON outputs:

| Command | `document_type` |
| --- | --- |
| `runtime inspect --format json` | `runtime.inspect` |
| `runtime query --format json` | `runtime.query` |
| `rundiff compare --format json` | `rundiff.compare` |
| `batchscope inspect --format json` | `batchscope.inspect` |
| `proofline verify --format json` | `proofline.verification` |
| `proofline run --format json` | `proofline.experiment` |
| `proofline search --format json` | `proofline.search` |
| `proofline validate --format json` | `proofline.validation` |

Every document contains a `document_type` discriminator and
`format_version: "1"`. The packaged schema is available as
`runtime_tools/schemas/contrail-output-v1.schema.json` and is included in both
wheel and source distributions. The schema describes every required field and
nested value emitted in 0.9. It deliberately permits additional fields so a
format-version-1 consumer can remain compatible with additive releases.

The branded aliases—`contrail inspect`, `query`, `compare`, `analyze`,
`verify`, `run`, `search`, and `validate`—emit the exact same documents and exit
statuses as the component commands in the table. The command name is a user
interface choice; it does not create a second machine protocol.

Complete deterministic examples for all eight document types live under
`tests/fixtures/golden/`. The compatibility tests compare producer output with
those examples and validate them against the packaged schema without requiring
an optional JSON Schema library.

Within format version 1, fields will not be removed, renamed, retyped, or given
incompatible semantics. New optional fields may be added. Consumers should use
`document_type`, reject unsupported format versions, and ignore unknown fields.
A breaking change requires a new format major version and a documented
migration path.

## Python runpack reader

Python consumers can read normalized evidence without relying on the SQLite
schema or private analysis code:

```python
from runtime_tools import RunpackError, open_runpack

with open_runpack("candidate.runpack") as runpack:
    execution = runpack.execution()
    events = runpack.events()
```

`open_runpack` accepts a string or path-like filesystem path and returns the
supported read-only `Runpack` facade. Its stable methods are `execution()`,
`manifest()`, `entities()`, `events()`, `causal_edges()`, `measurements()`, and
`attachments()`. `Execution`, `Entity`, `Event`, `CausalEdge`, `Measurement`,
`Attachment`, `JsonScalar`, and `JsonValue` are exported from
`runtime_tools.runpack` for typed consumers.

The reader owns an open snapshot, so callers must use its context manager or
call `close()`. Closing more than once is harmless. Any read after close raises
`RunpackError`. Malformed, unsupported, or unreadable artifacts also fail with
`RunpackError` rather than exposing backend-specific exceptions.

Record instances are frozen and slotted normalized snapshots. Their nested JSON
values are not deeply frozen: metadata and attribute lists or dictionaries are
ordinary mutable Python containers. Changing one of those returned containers
does not write through to the runpack, but consumers that share a record should
copy or treat its nested values as immutable by convention.

## Proofline explanations

`proofline verify` and `proofline run` accept `--explain` when a contract result
must also carry the runtime changes behind it. Explanation is opt-in: without
the flag, their version-1 JSON documents are unchanged.

With `--format json --explain`, the Proofline document adds a `diff` member. Its
value is a complete nested `rundiff.compare` document, including its own
`document_type` and `format_version`. It also adds `artifact_bindings`, with
`baseline` and `candidate` members that each contain the exact snapshot's
non-negative `size_bytes` and lowercase 64-character `sha256` value. A
`proofline.verification` document carries this member at its top level. A
`proofline.experiment` document carries it at the experiment's outer level,
alongside `diff`; its nested `verification` remains the claim document.

```json
{
  "artifact_bindings": {
    "baseline": {
      "size_bytes": 123456,
      "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    },
    "candidate": {
      "size_bytes": 123789,
      "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  }
}
```

Every claim also adds an `assertion` object and an `evidence` array. `assertion`
is the canonical resolved policy: its type, resolved name, and every threshold
or selector field used by the evaluator. Each evidence item contains:

- `fact`, the exact structured values used to evaluate the claim;
- `diff_path`, an RFC 6901 JSON Pointer to related detail, relative to the
  top-level `diff` value;
- `selector`, an equality filter for a referenced array, or an empty object for
  a scalar or object fact.

For example, an operation-count claim can point to
`/operation_count_changes` with `{"operation_name": "db.write"}`, while its
`fact` records the aggregate baseline, candidate, and limit values used for the
verdict. For `proofline run`, the claims are nested under `verification` but
their pointers still resolve against the experiment document's top-level
`diff`.

The assertion, claim result, RunDiff, and artifact bindings are derived from the
same open, descriptor-bound baseline and candidate snapshots. The SHA-256 is
streamed from each descriptor and paired with its byte size. An automation
consumer can therefore retain the policy, exact runpack identities, evaluated
fact, and outcome, resource, operation, timing, or dependency context without
joining separate command outputs. RunDiff arrays list changed groups by
semantic identity, while some claims aggregate across identities. A selector
can therefore resolve to zero rows for an unchanged aggregate or multiple rows
for a split aggregate; `fact` remains the authoritative claim input and the
selected rows are diagnostic context.

When the local timeline opens a current explained report, it hashes the same
open descriptors used for the supplied runpack snapshots and requires both
artifact bindings to match before replay. It then parses the embedded
assertions, evaluates them again, and requires every policy, status,
expected/observed description, fact, and selector to match. The UI labels this
an artifact-bound replay.

Binding-less version-1 reports remain readable for compatibility. A report with
canonical assertions is replayed semantically against the current runpacks and
labelled with lower assurance because it is not byte-bound to them. An older
assertion-less report retains the still lower, explicitly labelled
report-authored policy/result assurance even when its runtime values are
consistent. The binding detects stale or substituted evidence, but does not
authenticate the bundle if the report and runpacks can all be rewritten;
trusted retention, attestation, or signing must supply provenance.

Consumers that do not need explanation can continue to omit `--explain`.
Consumers that request it should validate the outer Proofline document and the
nested RunDiff discriminator independently.

`proofline verify --report PATH` and `proofline run --report PATH` imply
explanation and atomically retain this same artifact-bound JSON document while
preserving human stdout and the 0/1/2 exit contract. A successful or violated
verification publishes a complete mode-0600 report. Invalid input publishes
nothing, and existing files or symlinks are never overwritten. With `--format
json`, stdout bytes and the retained report bytes are identical.

The following are intentionally outside this compatibility promise:

- human-readable terminal layout;
- `runtime query --format jsonl`, whose columns are selected by the caller;
- the browser-internal `/api/data` payload; and
- internal Python storage, writer, and analysis helpers outside the documented
  runpack reader and annotation API.

JSON never contains non-finite numeric literals. Unknown or incomplete evidence
is represented with `null`, an explicit completeness field, or an
`unverifiable` claim rather than an inferred success.
