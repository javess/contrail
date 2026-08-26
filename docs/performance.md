# Performance qualification

Contrail treats performance as a tested boundary, not a blanket overhead
claim. `benchmarks/budgets.json` is authoritative; this page explains what the
gate protects and how to interpret it.

## Run the gates

Use the bounded pull-request profile while developing:

```bash
uv run python benchmarks/release.py --profile pr
```

Before release, use a quiet Linux x86-64 host:

```bash
uv run python benchmarks/release.py --profile release --json \
  > release-benchmark.json
```

Every case runs in a fresh subprocess. The harness normalizes Linux/macOS peak
RSS units and times out severe regressions. A passing local run is useful
evidence, not a replacement for the release host.

## Capture levels

| Level | Cost model |
|---|---|
| `passive` | process wait plus bounded resource/outcome collection |
| `process` | adds 100 ms process-table polling |
| `sample` | adds a 10 ms Python sampler and semantic boundary wrappers |
| `deep` | profiles every Python/native call and exact supported logical boundaries |

Deep is intentionally expensive. Retained operation timings are diagnostic
evidence, not observer-free latency measurements. Compare runtime and memory
only between runs captured with the same level.

Sample and Deep aggregate in memory and publish periodic snapshots; they do not
write a runpack or send IPC for every call. Startup registration is synchronous,
the first checkpoint follows after 50 ms, and later checkpoints occur every
500 ms. Socket operations fall back after 100 ms to an atomic local file.

## Expensive-tier gates

The PR and release profiles bound synthetic near-zero-work operations. Ratio
ceilings are deliberately wider than application-level performance targets:

| Case | PR count | Release count | Deep/passive ceiling | Absolute ceiling |
|---|---:|---:|---:|---:|
| outbound connection | 200 | 256 | 5x | 5 s |
| optional clients | 200 | 256 | 5x | 5 s |
| executor tasks | 200 | 256 | 5x | 5 s |
| asyncio tasks | 200 | 256 | 5x | 5 s |
| native calls | 1,000 | 5,000 | 5x | 5 s |
| Python exceptions | 1,000 | 5,000 | 5x | 5 s |
| WSGI requests | 200 | 256 | 12x | 5 s |
| Uvicorn-shaped ASGI requests | 200 | 256 | **15x** | **5 s** |

The first qualified ASGI PR run measured 0.216 s Deep versus 0.103 s passive,
or 2.109x, across h11 and httptools request-cycle shapes. The 15x gate is the
explicit expensive-tier ceiling, not a promise that production HTTP latency
will rise by that amount. Real Uvicorn behavior is covered separately by h11
and httptools integration tests.

The first WSGI gate measured 0.560 s versus 0.060 s, or 9.276x. Its near-empty
handler makes fixed Deep overhead unusually visible.

Both server gates require exact caller/status evidence, final streamed-body
completion, privacy-sentinel omission, and no duplicate records. An overhead
pass without those correctness checks does not qualify the adapter.

## Bounded evidence

Key workload/controller bounds include:

- 2,000 functions and 10,000 relationships per Python report;
- 16 MiB per process report;
- 128 reporting PIDs, with omitted PIDs counted explicitly;
- 256 semantic or logical records per interpreter;
- 2,000 logical records after controller normalization;
- 20,000 merged functions and 50,000 merged relationships;
- a 256 MiB controller ranking database with an 8 MiB page cache;
- 2,000 process identities and 50,000 resource samples;
- 1,000,000 accepted snapshot messages per capture session.

Cross-process selection uses exact aggregate top-K ranking with stable hashes
for ties. Candidate scores spill to a private SQLite workspace rather than
materializing unbounded maps. The workspace is removed after normalization.

Reaching a count limit produces explicit partial or truncated evidence.
Malformed input is rejected. Neither case is silently reported as complete.

## Runpack and import envelope

Validation applies before analysis. The supported maximum shape is bounded at
1,000,000 entities, 2,000,000 events, 2,000,000 causal edges, 1,000,000
measurements, and 100,000 attachments, with per-field, aggregate text/JSON,
attachment, encoded-row, SQLite page-count, and file-size ceilings.

These are safety rejection limits, not a claim that every combined maximum
shape fits on the minimum supported host. Release qualification assumes at
least two CPU cores and 4 GiB available memory.

The PR profile covers representative OTLP, Prometheus, Kubernetes, Temporal,
profile-merge, capture, BatchScope, and retained-report shapes. The release
profile raises imported data to 25,000–100,000 records and analysis/report
cases to 50,000 events. Current hard per-case ceilings range from 15–40 seconds
and 1.5–2.5 GiB RSS for those large cases.

## What the numbers mean

- Whole-preset ratios include bootstrap, process observation, profiling,
  semantic wrappers, snapshot publication, shutdown, and workload time.
- Very short workloads are dominated by startup cost.
- Sleep-heavy workloads understate every-call profiling cost.
- Call-dense workloads can make Deep dramatically slower.
- Controller normalization occurs after workload exit and is not workload
  latency, though it delays final artifact publication.
- Artifact binding adds one streaming SHA-256 pass over each runpack only when
  creating or replaying an explained report.

Detached output retention, capture-job registry writes, and recovery
checkpoints are bounded control-plane costs. They are outside call/sample hot
paths but still add finalization and filesystem work.

## Changing a budget

Attach before/after JSON results and explain the input or algorithm change. Do
not raise a ceiling solely to turn a red run green. If a supported safety shape
cannot fit the documented host envelope, retain controlled rejection and lower
the advertised limit.
