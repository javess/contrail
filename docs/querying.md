# Querying runpacks

Version 1 runpacks are SQLite databases with a documented normalized schema.
The query command opens artifacts in read-only mode and caps returned rows:

```bash
runtime query run.runpack \
  'SELECT name, kind, started_at_ns FROM events ORDER BY started_at_ns' \
  --format table
```

Machine-readable formats preserve column order and JSON-safe values:

```bash
runtime query run.runpack \
  "SELECT timestamp_ns, value FROM measurements WHERE name = 'queue_depth'" \
  --format json
```

The primary tables are `executions`, `entities`, `events`, `causal_edges`, and
`measurements`. JSON attributes use the `attributes_json` column. SQLite's JSON
functions can inspect those attributes when the local SQLite build enables
them. Queries are single statements; writes fail because the connection itself
is read-only rather than relying on SQL text filtering.
