# Exported Temporal history example

`temporal-python-fanout-history.json` is representative Temporal History
protojson paired with the official-style Python OTel export in
[`../otel/temporal-python-fanout.json`](../otel/temporal-python-fanout.json).
Both files are deterministic test evidence, not captured production telemetry.

Run the complete enrichment path:

```bash
uv run contrail import-otel examples/otel/temporal-python-fanout.json \
  --name temporal-fanout --output temporal.runpack
uv run contrail enrich-temporal-history temporal.runpack \
  examples/temporal/temporal-python-fanout-history.json \
  --output temporal-history.runpack
uv run contrail analyze temporal-history.runpack
```

The history adapter accepts one protobuf-JSON `History` object with an `events`
array. It reads workflow start/terminal events and activity scheduled, started,
completed, failed, timed-out, and canceled events. Input is limited to 64 MiB
and 200,000 history events, duplicate JSON keys and ambiguous references are
rejected, and the source runpack is never modified.

For each activity, Contrail preserves queue wait, terminal outcome, and the
recorded attempt number. A unique official Python `RunActivity:<type>` server
span with matching `temporalWorkflowID` and `temporalActivityID` remains the
execution interval. When no such span exists, the history start-to-terminal
interval becomes a fallback operation. Raw history and payloads are not stored
in the enriched runpack.

This example yields six activity correlations, one compute straggler, and direct
evidence that result aggregation completed on attempt 2. It still has no
application-specific phase or progress facts: those require explicit workload
annotations or another evidence source.

The accepted field names follow Temporal's
[History protobuf](https://github.com/temporalio/api/blob/main/temporal/api/history/v1/message.proto).
