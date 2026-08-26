# Querying runpacks

Version 1 runpacks are SQLite databases with a documented normalized schema.
The query command opens artifacts in read-only mode and caps returned rows:

```bash
contrail query run.runpack \
  'SELECT name, kind, started_at_ns FROM events ORDER BY started_at_ns' \
  --format table
```

The default output cap is 1,000 rows. `--limit` can raise it to at most 100,000.
SQL statement text is limited to 1 MiB and must be valid UTF-8.
Artifact validation and the requested statement share a limit of 25 million
SQLite virtual-machine steps. This bounds expensive validation, aggregation,
and recursive queries even when they produce few rows. Individual SQLite values
are limited to 4 MiB and the encoded
result, including column metadata and row framing, is limited to 16 MiB, so a
small row count cannot create unbounded output. Each output renderer enforces
the same limit after formatting, preventing repeated JSONL keys or table padding
from expanding a bounded result. Table columns are truncated to 60 characters.

Machine-readable formats preserve column order and JSON-safe values. Repeated
SQL column labels receive deterministic `_2`, `_3`, and later suffixes so JSONL
objects do not discard cells. BLOBs and non-finite SQLite floats use tagged
objects rather than non-standard JSON. Because JSONL contains only result rows,
the CLI writes a truncation warning to stderr when its row limit is reached:

```bash
contrail query run.runpack \
  "SELECT timestamp_ns, value FROM measurements WHERE name = 'queue_depth'" \
  --format json
```

The primary tables are `executions`, `entities`, `events`, `causal_edges`, and
`measurements`. Schema 1.1 runpacks can also contain `attachments`; its `content`
column is a BLOB and may contain secrets. JSON attributes use the
`attributes_json` column. SQLite's JSON functions can inspect those attributes
when the local SQLite build enables them. Queries are single statements; both
the read-only connection and a SQLite authorizer block writes, `ATTACH`, and
other side effects. The authorizer also denies `load_extension` even if a host
SQLite connection has extension loading enabled.
