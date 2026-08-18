# Changelog

## 0.9.0

Contrail 0.9 is the beta compatibility-freeze release ahead of 1.0.

### Stable candidate surfaces

- Portable SQLite runpack schema 1.1 and the documented 1.x read policy.
- JSON output format 1 for inspect, query, RunDiff, BatchScope, and Proofline.
- Documented CLI commands, exit statuses, and error-stream behavior.
- The additive `contrail` command, including the installed-wheel `contrail demo`
  walkthrough and flat capture, compare, verify, analyze, and debug workflow.
- Proofline contract parsing and deterministic verification semantics.
- The captured work annotation API: `run`, `stage`, `progress`, `event`, and
  `link`.
- The read-only `open_runpack` Python facade and normalized record types.

### Release scope

- Local POSIX capture on supported Linux and macOS Python versions.
- Bounded exported-JSON adapters for OTLP traces/logs, Kubernetes snapshots,
  and Prometheus responses.
- Local read-only timeline UI.
- RunDiff, BatchScope, and Proofline over the same normalized artifact.
- Claim-to-evidence navigation from a local contract or a retained explained
  Proofline report, with policy replay and exact SHA-256/byte-size runpack
  bindings for newly generated explained JSON.
- A selectable, validated workload Python for Proofline experiments and
  counterexample search, held constant across both Git refs and every replay.
- Atomic, no-overwrite `verify/run --report PATH` publication so failed gates
  retain complete artifact-bound evidence without shell-redirection truncation.
- Pre-materialization runpack field, record, and aggregate limits shared by the
  public reader, analyses, contract verification, and timeline.
- A source-independent demo that creates and validates a complete failed-gate
  evidence bundle plus an adaptable workload/contract template without Git, a
  repository checkout, or network access.

Text presentation, the browser-internal payload, arbitrary query JSONL rows,
and undocumented Python internals are not frozen for 1.0.
