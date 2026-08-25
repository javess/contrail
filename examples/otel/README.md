# Exported OpenTelemetry examples

`temporal-python-fanout.json` is a small representative OTLP/JSON document for
the span names and attributes emitted by the official Temporal Python
OpenTelemetry interceptor. It is deterministic test evidence, not a captured
production trace.

Import and inspect it without installing Temporal:

```bash
uv run contrail import-otel examples/otel/temporal-python-fanout.json \
  --name temporal-fanout --output temporal.runpack
uv run contrail inspect temporal.runpack --tree
uv run contrail analyze temporal.runpack
```

The fixture represents five parallel `RunActivity:compute-chunk` attempts, one
straggler, and a later result-aggregation activity. BatchScope can safely
classify the repeated activity straggler from the server-span cohort. It labels
the cross-resource critical path as inferred and does not invent progress or
semantic lifecycle phases that are absent from the export.

This limitation is structural. Temporal workflows are resumable, so its Python
interceptor records workflow lifecycle checkpoints such as `RunWorkflow` and
`CompleteWorkflow` as point spans while activity executions have ordinary
durations. OpenTelemetry currently has no general semantic convention for job
phases or logical progress. A fuller BatchScope diagnosis therefore needs either:

- explicit `run`, `stage`, and `progress` evidence from the workload; or
- exported Temporal history correlated with the OTel activity spans. See the
  [paired example](../temporal/README.md).

The example follows the official
[Temporal Python interceptor](https://github.com/temporalio/sdk-python/blob/main/temporalio/contrib/opentelemetry/_interceptor.py)
and [OpenTelemetry span model](https://opentelemetry.io/docs/concepts/signals/traces/).
