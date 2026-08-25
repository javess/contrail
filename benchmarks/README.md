# Release benchmarks

The harness generates deterministic evidence in a private temporary directory,
runs each operation in a fresh subprocess, and enforces elapsed-time and peak-RSS
ceilings from `budgets.json`. It uses only the Python standard library and the
installed Contrail package.

```bash
uv run python benchmarks/release.py --profile pr
uv run python benchmarks/release.py --profile release --json
```

The PR profile catches accidental algorithmic regressions quickly. The release
profile covers the largest interactive UI payload and representative six-figure
adapter inputs. It also records the same local connection-heavy workload under
passive and Sample capture, requires every connection to survive normalization,
requires every corresponding DNS phase to survive its independent bound, and
gates the observed workload-duration ratio with a deliberately broad
cross-runner ceiling. This measures the complete Sample preset rather than
claiming to isolate the connection wrapper. A separate Deep workload executes
200 operations in PR and the full
256-operation per-process bound in release, requires every SQLite operation to
normalize with no drops, and rejects a runpack containing its SQL sentinel.
This qualifies bounded retention and privacy, not observer-free query latency.
An additional optional-client case exercises the Redis public method shape with
200 calls in PR and the full 256-record bound in release. It requires exact
cache-command retention, rejects its key sentinel, and compares Passive with
Deep under the same broad 5x whole-workload ceiling. The generated client keeps
the release gate dependency-free; compatibility with upstream client packages
is covered separately from this overhead and retention gate.
The executor-task case submits 200 standard-library thread-pool tasks in PR and
the full 256-record bound in release. It requires exact
submission-to-completion records, complete caller attribution, explicit
callable/argument/result privacy markers, and omission of its argument
sentinel, then applies the broad 5x Passive/Deep whole-workload ceiling.
The asyncio-task case creates 200 public tasks in PR and the full 256-record
bound in release, divided across `asyncio.create_task`, `TaskGroup.create_task`,
`asyncio.ensure_future`, and coroutine arguments to `asyncio.gather`. It requires exact
creation-to-completion records with application callsites, explicit
awaitable/name/context/result privacy markers, and omission of its task payload
and name sentinels, then applies the same broad 5x ceiling.
The inbound WSGI case drives 200 no-op requests in PR and the full 256-record
bound in release. It requires exact request-to-response records, status codes,
application-callsite attribution, and explicit route/URL/header/body/address
privacy markers, then rejects all request and response sentinels. Because this
is a deliberately near-zero-work application measured under the complete Deep
preset, its 12x Passive/Deep ceiling is an explicit worst-case qualification
bound rather than a typical request-overhead claim; the absolute gate remains
five seconds.
The native-call case compares Passive with Deep while running 1,000 (PR) or
5,000 (release) native compression calls after a fixed startup wait. It
requires the exact aggregate call count, native identity, zero exceptions, and
argument redaction, and applies the same broad 5x whole-workload ratio ceiling.
The Python-exception case catches 1,000 (PR) or 5,000 (release) exceptions in a
single Python call, then performs 200 ordinary `asyncio` awaits. It requires the
exact raw and diagnostic count for the real exceptions, a positive built-in
iterator-control filtered count for normal await completion, message redaction,
no drops, and a conservative `python_exception_churn` diagnosis that excludes
built-in iterator completion and does not call propagation events unique
failures. It then applies the same broad 5x whole-workload ratio ceiling. Line
and opcode events stay disabled during this Deep-only gate.
The native and Python-exception gates also require complete observer-integrity
metadata with no false hook-setter report, so the hot-path integrity check
is covered by the same Deep/passive overhead ceilings.
These are release qualification ceilings, not end-user latency promises. Input safety limits remain rejection
bounds and do not promise that every maximum-size input fits every machine.
