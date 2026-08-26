# Make a terrible sort disappear

This dependency-free example replaces worst-case bubble sort with Python's
built-in Timsort. Both programs sort the same 4,000 descending integers and emit
byte-identical JSON; Contrail shows where the time went and proves the result did
not change.

From the repository root:

```bash
sorting_demo="$(mktemp -d)"

uv run contrail record \
  --capture-level deep \
  --name bubble-sort \
  --output "$sorting_demo/before.runpack" \
  -- \
  python examples/sorting/before.py

uv run contrail record \
  --capture-level deep \
  --name timsort \
  --output "$sorting_demo/after.runpack" \
  -- \
  python examples/sorting/after.py
```

Both commands print the same result:

```json
{"checksum": 8002000, "count": 4000, "first": 1, "last": 4000}
```

## Find the wasted time

```bash
uv run contrail analyze "$sorting_demo/before.runpack"
```

Representative output:

```text
Bottleneck
  python_hotspot (60%)
    __main__.bubble_sort accumulated 0.780s self time

Python hotspots (including native calls)
  __main__.bubble_sort [application, python]  self 780.5ms, total 780.5ms
```

Deep capture is intrusive, so these are observed diagnostic timings rather than
unmodified application latency. Both sides use the same capture level, keeping
their comparison meaningful.

## See the replacement pay off

```bash
uv run contrail compare \
  "$sorting_demo/before.runpack" \
  "$sorting_demo/after.runpack"
```

One local run produced:

```text
Outcome
  equivalent
  exit status: exit 0 → exit 0 (equivalent)
  stdout:      equivalent

Runtime
  871.4ms → 90.3ms (-89.6%)

CPU time
  863.7ms → 83.1ms (-90.4%)
```

Exact timings vary by machine; the example test requires the candidate to take
less than half the observed wall and CPU time.

## Make it a contract

```bash
uv run contrail verify examples/sorting/contract.yaml \
  --baseline "$sorting_demo/before.runpack" \
  --candidate "$sorting_demo/after.runpack" \
  --explain
```

Proofline checks candidate success, exit and output equivalence, and zero-percent
runtime/CPU regression allowances. The expected result is five passing claims.

Inspect the same artifacts more deeply in the terminal:

```bash
uv run contrail compare "$sorting_demo/before.runpack" \
  "$sorting_demo/after.runpack"
uv run contrail analyze "$sorting_demo/after.runpack"
uv run contrail inspect "$sorting_demo/after.runpack" --tree
```

The workload itself never imports Contrail or adds instrumentation. Read
[`before.py`](before.py), [`after.py`](after.py), and the shared
[`workload.py`](workload.py) to see the complete code change.
