# Built-in evidence integrations

Contrail ships OpenTelemetry, Kubernetes, Prometheus, and Temporal import or
enrichment commands. They are fixed parts of the application rather than a
third-party plug-in API:

```bash
contrail import-otel trace.json
contrail enrich-otel-logs run.runpack logs.json --output enriched.runpack
contrail enrich-kubernetes run.runpack snapshot.json --output enriched.runpack
contrail enrich-prometheus run.runpack response.json --output enriched.runpack
contrail enrich-temporal-history run.runpack history.json --output enriched.runpack
```

`contrail providers` lists the bundled integrations and commands. They are
always enabled, so results do not depend on installed entry points or selection
environment variables.

## Implementation boundary

Each integration lives under `runtime_tools.providers.builtins`, with input
normalization separate from `commands.py`. A command is a small typed value
containing its name, help text, argument configuration callback, and executor.
The fixed registry combines those command tuples and protects the CLI from
unexpected executor exceptions.

Integrations translate bounded input into the existing normalized model and
runpack format. They do not add private record schemas or inject code into a
captured workload. Use `runtime_tools.providers.enrichment` for atomic runpack
enrichment.

To add another built-in integration:

1. Add its normalizer under `runtime_tools/providers/builtins/<name>/`.
2. Expose a `COMMANDS` tuple from `commands.py`.
3. Add that tuple and its display metadata to `providers/registry.py`.
4. Test valid, malformed, bounded, privacy, and CLI behavior through public
   outcomes.
