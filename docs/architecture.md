# Architecture

## Product boundary

Contrail captures a finite execution, normalizes evidence into a stable model,
and stores it in a portable `.runpack`. RunDiff, BatchScope, and Proofline are
different analyses of that same artifact; they do not own alternate execution
models.

```text
process / OTel / Kubernetes / Prometheus / logs
                       |
                 capture adapters
                       |
            normalized execution records
                       |
             versioned SQLite .runpack
                       |
       +---------------+----------------+
       |               |                |
    RunDiff        BatchScope        Proofline
```

Adapters describe how evidence was produced. The core describes what happened.
For example, an OTel span is adapter input, while a timed operation and its
causal parent are core concepts.

## Smallest stable core

The first stable boundary consists of:

1. one execution record;
2. logical or physical entities that participated in it;
3. events, including optional intervals;
4. explicit causal edges independent of timestamp ordering;
5. measurements associated with an entity or the execution;
6. optional bounded attachments for selected logs or raw source evidence;
7. source and confidence metadata that preserve uncertainty.

The core deliberately does not contain Kubernetes, OTel, RunDiff, BatchScope,
or Proofline types. Those packages translate into or query the core records.

## Artifact decision

Version 1 `.runpack` files are SQLite databases. SQLite is already available in
Python, is a documented open format, supports incremental transactional writes,
and handles indexed time-range and aggregate queries without materializing all
events as Python objects. It also keeps the first end-to-end slice free of a
runtime dependency.

The tradeoffs are explicit:

- SQLite pages are not byte-for-byte deterministic even when logical rows are.
- A database is less compressible than columnar Parquet for large measurements.
- Concurrent writers require coordination through SQLite's locking model.

Those costs are preferable to committing now to a ZIP manifest, JSONL, Parquet,
and an embedded query engine simultaneously. The schema separates high-volume
measurements from events, so a future schema version can store measurement
partitions as Parquet without changing analysis concepts. Raw source telemetry
is optional and never required for core queries.

Every artifact contains a schema version, producer version, execution row, and
normalized tables. Readers reject unsupported major schema versions and accept
additive minor versions. Version 1.1 adds optional attachments while version 1
core tables remain readable. Runpacks use SQLite's DELETE journal mode so a
completed artifact is one portable file; readers reject WAL-mode databases that
may depend on unshipped sidecars. Executable schema triggers are also rejected
before inspection or enrichment. The exact read and write support rules are in
the [compatibility policy](compatibility.md).

Each logical inspection, comparison, UI payload, and contract verification uses
one SQLite read snapshot per artifact. Size checks, normalized facts, and final
verdicts therefore cannot combine different committed versions of one runpack.
Enrichment is held to the same rule: it copies one validated, descriptor-bound
source snapshot into a private artifact, then derives source-dependent adapter
facts from that copy before mutation. Replacing the source pathname cannot
combine one execution generation with correlations or time-window decisions
from another.

Explained Proofline generation also streams SHA-256 over that same open file
descriptor and records the byte size for both snapshots. When the retained UI
loads the report, it opens and hashes each supplied runpack once, verifies those
bindings, and uses the same descriptor-bound SQLite snapshots for RunDiff and
policy replay. A pathname replacement therefore cannot move validation,
identity, analysis, and replay onto different file generations. Ordinary
non-explained output does not compute or expose these byte identities.

## Package direction

The initial repository uses one Python distribution with internal packages. It
can be split into workspace distributions after package boundaries are proven:

```text
capture adapters --> core model/storage <-- analysis packages
                                         <-- CLI/API presentation
```

Core cannot import capture or analysis code. Analyses may share query helpers,
but they must return structured facts before rendering prose. LLMs and hosted
services are outside the core and are never required for capture or analysis.

The distribution enforces that direction as four internal layers:

```text
foundation <- adapters <- analyses <- presentation
```

Foundation owns the normalized model, artifact and storage safety,
serialization, and shared support. Adapters translate external evidence.
Analyses derive structured facts. Presentation owns the package facade, CLIs,
reports, demo, and local UI. A layer may import itself or anything to its left;
the package facade is for external consumers and is not an internal dependency.
`tools/check_architecture.py` checks both this direction and internal import
cycles in CI. See [Python development](development.md) for the concrete module
classification and validation workflow.

## Scale strategy

Small executions can be returned as immutable Python records. Medium and large
executions are queried through indexed SQL, with graph subsets materialized only
for algorithms such as critical-path analysis. Initial work targets tens of
thousands to low millions of records; the schema avoids an architectural need
to load 100 million events into memory.

## Security and privacy

Capture is local by default. Environment values, stdout, and stderr content are
not stored by default because they commonly contain secrets. Version 1 records
only selected non-sensitive environment metadata plus output byte counts and
hashes. Bounded output content and raw OTLP input require explicit CLI flags;
normal inspect and UI paths do not render their content.

The normalized execution deliberately stores the exact command arguments and
resolved working directory. Users must keep credentials out of argv and path
names before sharing a runpack. Selected environment and output hashes are
behavioral identities, not a secrecy mechanism; low-entropy values may be
guessable.

OTLP log enrichment is an explicit content import, not a redaction boundary.
Log bodies and attributes become normalized event evidence and may be rendered
by queries or UI details. Sensitive log fields must be scrubbed before import or
before the enriched runpack is shared.

Local capture identifies only these selected environment variables by SHA-256,
never plaintext: `CI`, `CUDA_VISIBLE_DEVICES`, locale/timezone settings,
thread-pool sizing variables, and `PYTHONHASHSEED`. The capture tool's Python
implementation and version are stored separately as non-secret controller
metadata; they are not presented as the workload runtime.

Text reports escape terminal control characters in artifact-supplied names,
attributes, commands, and paths. Machine-readable JSON preserves the normalized
values. Attachments remain opaque and are limited to 64 MiB each and 256 MiB in
aggregate per runpack.
