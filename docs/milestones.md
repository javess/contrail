# Milestones

Each milestone ends in a demonstrable vertical slice over the same `.runpack`.
Later work may refine earlier schemas only through explicit versioning.

## 0. Architecture

- Record the stable core, adapter boundary, artifact choice, time uncertainty,
  identity, and scale tradeoffs.
- Avoid package scaffolding for components with no demonstrated behavior.

## 1. Local capture

- `runtime record --name demo -- python demo.py` writes a `.runpack`.
- Capture process lifecycle, exit code, wall time, CPU, peak memory, Git revision,
  selected platform metadata, and stdout/stderr hashes and byte counts.
- Allow explicitly requested, bounded stdout/stderr attachments without changing
  the privacy-preserving default.
- `runtime inspect demo.runpack` renders the captured facts.

## 2. OpenTelemetry import

- Import a bounded OTLP trace fixture into the same artifact.
- Normalize services, operations, intervals, parent edges, and trace correlation.
- Print the causal structure and report clock inconsistencies.

## 3. RunDiff

- Compare outcome, wall time, operation counts, and aggregate graph edges.
- Provide concise terminal and stable JSON reports.
- Drive the implementation with a local pipeline regression example.

## 4. Local timeline

- Serve one local, read-only timeline and compare view.
- Add zoom, filtering, and critical-path highlighting without becoming a generic
  dashboard.

## 5. Work annotations

- Add the minimal `run`, `stage`, `progress`, `event`, and `link` API only where
  capture evidence cannot express domain work.
- Represent annotations compatibly with OTel spans/events when OTel is present.

## 6. BatchScope

- Derive an evidence-labelled lifecycle.
- Calculate observed/inferred critical paths, throughput, remaining drain time,
  and deterministic bottleneck classifications.

## 7. Kubernetes and metrics adapters

- Correlate finite workloads, owners, pods, containers, nodes, and bounded
  Prometheus samples with logical entities.
- Keep Kubernetes and Prometheus concepts out of the core.

## 8. Proofline contracts

- Parse explicit contracts and evaluate deterministic outcome, runtime, resource,
  structure, and cardinality assertions over existing artifacts.

## 9. Proofline execution

- Capture comparable baseline and candidate workloads in isolated Git worktrees.
- Reuse RunDiff facts for contract evaluation and preserve both artifacts.

## 10. Counterexample search

- Add parameterized workloads and shrinking only after deterministic contracts
  and execution isolation are reliable.

## Release gate

The first public demonstration is ready when a baseline/candidate local pipeline
shows equivalent output, a meaningful wall-time regression, amplified database
operations, and a new dependency in both terminal and visual reports.
