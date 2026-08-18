# Runpack compatibility fixtures

These immutable SQL fixtures reproduce runpacks emitted before the 0.9
compatibility freeze. Tests verify each source file's SHA-256 before loading it
into SQLite.

| Fixture | Source revision | Producer | Source runpack SHA-256 | SQL fixture SHA-256 |
| --- | --- | --- | --- | --- |
| `schema-1.sql` | `23c488641a1e06ca89ea30fae636daf8d20c6741` (`Accept additive runpack schema minors`) | 0.1.0 | `0f514a1e562bb56a094835e9c4834389665c4eedd94fe2d7da2930b484d04ac6` | `bf30b574629d4d3081b690d209475457a6a6613099a27155480dbc394c073a3f` |
| `schema-1.1.sql` | `102ea8596ab1fc615190b07751611753533a22ff` (`Add opt-in runpack attachments`) | 0.1.0 | `95fe7bef490ac5e80fd7567fc42544b382f37238b1e24f35703c23f458899964` | `2d46fd9f8be122aa0c8d4c2a0e6106d4e84bec2cf66ec020d2b6d541d89f2d9c` |

Each source revision was exported with `git archive`, then its public
`RunpackWriter` created the fixed execution, entity, event, and measurement.
The 1.1 fixture also includes one attachment. SQLite `.dump` produced the SQL;
the runpack application ID and DELETE journal mode are retained at the top.
