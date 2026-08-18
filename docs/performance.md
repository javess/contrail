# Performance qualification

Contrail's input limits bound untrusted evidence; they are not throughput or
memory promises. A 64 MiB document can expand into very different normalized
graphs depending on its shape. Release qualification therefore uses fixed,
deterministic shapes and publishes both their record counts and resource budgets.

Validated runpack readers reject over-limit evidence before returning any
normalized records to Python. Each text or JSON field is limited to 4 MiB;
normalized text and JSON are each limited to 256 MiB per runpack. Record limits
are 1,024 manifest rows, one execution, 1,000,000 entities, 1,000,000 events,
2,000,000 causal edges, 1,000,000 measurements, and 100,000 attachments.
Attachment content remains limited to 64 MiB per item and 256 MiB in aggregate.
The complete SQLite file is limited to 2 GiB, including indexes, unused pages,
and trailing bytes. A 128 MiB SQLite value/encoded-row ceiling is installed
before structural and content validation as a backstop against values too large
to preflight safely. The bounded SQL surface may install a tighter cell limit
after this base guard.

Run the quick gate with:

```bash
uv run python benchmarks/release.py --profile pr
```

Before a release, run the larger profile on a quiet Linux x86-64 host:

```bash
uv run python benchmarks/release.py --profile release --json \
  > release-benchmark.json
```

`benchmarks/budgets.json` is authoritative. Each case runs in a fresh subprocess
so peak resident memory is attributable to one workload. The harness normalizes
the different Linux and macOS `ru_maxrss` units. A timeout is four times the
declared time ceiling (and at least 30 seconds), so a severe regression fails
instead of hanging a release job.

The PR profile covers 10,000 OTLP spans, 20,000 Prometheus samples, 5,000
Kubernetes objects, a 10,000-event causal chain, a 10,000-event UI payload, and
artifact-bound generation plus retained-report replay over two 10,000-event
runpacks.
The release profile raises these to 100,000 spans/samples, 25,000 Kubernetes
objects, and 50,000-event analysis/UI/report-replay cases. Its current hard
ceilings are 15–40 seconds and 1.5–2.5 GiB RSS per case. The deliberately
generous ceilings absorb shared-runner noise while still detecting accidental
quadratic work or unbounded materialization.

Artifact binding adds one linear, streaming SHA-256 pass over each baseline and
candidate runpack. It runs only when generating an explained report or loading
one for retained-report replay; non-explained verification and ordinary read
paths do not pay this cost. Hashing uses a fixed-size buffer, so auxiliary memory
does not grow with runpack size.

For calibration, the PR profile measured as follows on 2026-08-18 using macOS
Apple silicon and CPython 3.12. These observations are not promises; the JSON
budgets remain the pass/fail thresholds.

| Case | Records | Seconds | Peak RSS MiB |
| --- | ---: | ---: | ---: |
| OTLP import | 10,000 | 0.094 | 49.5 |
| Prometheus import | 20,000 | 0.090 | 43.9 |
| Kubernetes import | 5,000 | 0.092 | 50.1 |
| BatchScope analysis | 10,000 | 0.102 | 49.7 |
| UI payload | 10,000 | 0.192 | 63.3 |
| Artifact-bound retained report | 10,000 | 1.498 | 102.0 |

The same host measured the release profile at 0.971 seconds/180.0 MiB for
100,000 OTLP spans, 0.473 seconds/80.2 MiB for 100,000 Prometheus samples,
0.479 seconds/119.7 MiB for 25,000 Kubernetes objects, 0.741 seconds/119.4 MiB
for a 50,000-event BatchScope chain, and 1.301 seconds/163.4 MiB for the
50,000-event UI payload.

When changing a budget, attach before/after JSON results and explain the input
shape or algorithm change. Do not raise a budget solely to make a failing run
green. If a documented safety maximum cannot be processed within the supported
host envelope, preserve controlled rejection and lower the advertised limit
rather than relying on out-of-memory termination.
