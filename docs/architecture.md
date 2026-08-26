# Architecture

Contrail turns evidence from one finite execution into one normalized,
portable artifact. BatchScope, RunDiff, and Proofline are consumers of that
artifact; they do not own alternate execution models.

```text
local process / annotations / exported telemetry
                       │
                 evidence providers
                       │
           normalized execution records
                       │
             versioned SQLite .runpack
                       │
          ┌────────────┼────────────┐
       BatchScope    RunDiff     Proofline
```

## Design rules

1. Core records describe what happened; providers describe where evidence came
   from.
2. Human output is rendered from structured facts. It is never parsed back into
   behavior.
3. Missing, ambiguous, or truncated evidence stays explicit.
4. Workload observers are bounded and privacy-minimal.
5. A runpack is sufficient for later analysis; investigation does not require
   the original workload or a Contrail service.

## Package direction

Importable code lives under `src/runtime_tools` and follows one enforced
direction:

```text
foundation → providers/adapters → analyses → presentation
```

| Layer | Owns | May import |
|---|---|---|
| foundation | model, storage, artifacts, serialization, terminal safety | foundation |
| providers/adapters | process capture, annotations, profiling, and built-in integrations | foundation, providers/adapters |
| analyses | inspection, query, BatchScope, RunDiff, Proofline | foundation, providers/adapters, analyses |
| presentation | CLIs, reports, capture workers, demo | every layer |

Package internals import concrete modules, never the `runtime_tools` facade.
`tools/check_architecture.py` classifies every module, rejects reverse imports
and cycles, and enforces a 1,000-line production-module ceiling.

Large concerns use narrow package façades. Storage is owned by reader, writer,
validation, and snapshot modules; capture separates orchestration, process I/O,
metadata, and recovery; profiling separates sessions, parsing, evidence
families, ranking, and loading; BatchScope separates contracts from lifecycle,
profile, boundary, resource, and hotspot analyses. These are ownership
boundaries, not compatibility wrappers.

## Provider architecture

Host-side evidence integrations live under one canonical tree:

```text
runtime_tools/providers/
  contracts.py          typed public protocol and immutable specs
  registry.py           fixed built-in command catalog
  enrichment.py         shared atomic runpack enrichment
  builtins/
    otel/
    kubernetes/
    prometheus/
    temporal/
```

Each built-in package separates normalization from `commands.py`. The CLI asks
one deterministic registry for the bundled commands. Command collisions fail
before argument parsing.

This integration boundary is intentionally host-side. Providers translate input
into the existing model; they do not define private record schemas or inject
arbitrary code into captured workloads. See [provider development](providers.md).

## Capture process model

The CLI frontend starts a separate capture worker. The worker owns the workload,
temporary state, output relay, observer collector, checkpoint, and final
publication. The frontend can detach or disappear without becoming the owner of
evidence integrity.

Passive capture waits for the process and records bounded outcome/resource
facts. Process capture polls the process group from the controller. Sample and
Deep create a private session directory and prepend a standalone
`sitecustomize.py` to child `PYTHONPATH`; the target application does not import
Contrail.

The workload hot path aggregates in memory. Each interpreter registers and
periodically sends its latest bounded snapshot over a private Unix socket. A
short-timeout atomic-file fallback handles unavailable or stalled collectors.
The controller retains one latest snapshot per PID, validates it, performs
bounded cross-process ranking in private SQLite scratch space, and writes
normalized events and edges only after the workload stops.

Crash checkpoints remain partial evidence. Process limits, snapshot kinds,
transport statistics, selection bounds, and observer-integrity facts are stored
so analyses can distinguish absence from completeness.

The semantic observer is a standard-library-only package copied beside the
profile bootstrap. One state object owns its bounded records while focused
subprocess, HTTP, network, asyncio, and logical-operation modules install the
adapters. Optional adapters activate lazily when supported modules appear;
Contrail does not import or depend on those clients. Deep’s existing profile
hook recognizes supported WSGI/Uvicorn server cycles without replacing the
application or protocol object.

Detailed capture boundaries and privacy rules live in
[capture and evidence](capture.md).

## Runpack boundary

A `.runpack` is a versioned SQLite database containing:

- one execution and its capture metadata;
- entities, timed or untimed events, causal edges, and measurements;
- bounded attachments when explicitly requested;
- normalized provider and observer facts.

Writers use private staging and publish without overwrite. Readers validate
schema compatibility, shapes, bounds, archive paths, and safe SQLite behavior
before returning typed values. See the [execution model](execution-model.md) and
[compatibility policy](compatibility.md).

## Analysis boundary

BatchScope explains one execution using normalized lifecycle, causal,
throughput, resource, process, and semantic evidence. Diagnoses are
deterministic and evidence-labelled.

RunDiff compares two opened snapshots. Aggregate matching does not require
stable event IDs across runs.

Proofline evaluates explicit assertions over the same snapshots. Explained
reports bind results to the exact runpack bytes and retain structured RunDiff
facts. Unavailable evidence produces an unverifiable result rather than a pass.

## Presentation boundary

The branded `contrail` command composes focused Typer command modules into one
flat command tree. Command functions receive typed paths, enums, and options;
they delegate behavior to the analysis and adapter layers rather than parsing
arguments themselves. Rich owns help, usage errors, tables, status, and other
human-only presentation through one console boundary.

Structured documents have versioned `document_type` and `format_version`
fields. JSON and JSONL bypass Rich and are written directly to stdout, while
human reports render the same structured facts in the terminal.

## Scale and trust

Contrail is bounded by design: input bytes, record counts, attachment sizes,
process reports, observer records, query rows, and analysis details all have
hard limits. Exact limits belong in [support](support.md),
[machine output](machine-output.md), and [performance](performance.md), not in
the module graph.

Workload code, imported telemetry, runpacks, YAML, JSON, SQLite, and provider
entry points cross distinct trust boundaries. Validation
belongs at those boundaries; internal domain values remain precisely typed.
See the [threat model](threat-model.md).
