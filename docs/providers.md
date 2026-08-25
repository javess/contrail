# Provider development

Providers add evidence import or enrichment commands without changing the core
CLI dispatcher. Built-ins live under `runtime_tools.providers.builtins`;
external distributions use the standard `contrail.providers` entry-point group.

## Contract

Export one `ProviderSpec` containing a stable lowercase key and one or more
`ProviderCommand` values. Each command configures its own argparse parser and
returns a `ProviderResult` when it executes. Use `ProviderExecutionError` for a
safe user-facing failure. Unexpected exception messages are not printed.

```python
from runtime_tools.providers import ProviderCommand, ProviderResult, ProviderSpec

PROVIDER = ProviderSpec(
    key="acme",
    display_name="Acme evidence",
    commands=(ProviderCommand("import-acme", "import Acme evidence", configure, execute),),
)
```

Declare it in the provider distribution:

```toml
[project.entry-points."contrail.providers"]
acme = "acme_contrail:PROVIDER"
```

The entry-point name and `ProviderSpec.key` must match. Names and command
collisions fail before command parsing.

## Loading policy

Built-ins are enabled by default. Installed external providers are discovered
but not imported until explicitly enabled:

```bash
contrail providers
CONTRAIL_ENABLE_PROVIDERS=acme contrail import-acme evidence.json
CONTRAIL_DISABLE_PROVIDERS=otel contrail providers
```

Both variables accept comma-separated keys. A key cannot appear in both.
Selection is process-local and does not write configuration files.

## Provider boundary

Providers translate input into the existing normalized model and runpack
format. They do not add private record schemas or change analysis semantics.
Validate JSON, files, subprocess data, and other untrusted input before creating
typed records. Use the shared atomic helpers in
`runtime_tools.providers.enrichment` when enriching a runpack.

Third-party providers run in the Contrail controller process. They are not
allowed into the zero-touch workload bootstrap: injected capture adapters have
stricter behavior-preservation and privacy requirements and remain built-in.

## Add a built-in

1. Add a package under `runtime_tools/providers/builtins/<key>/`.
2. Keep normalization separate from `commands.py`.
3. Add one lazy declaration to `builtins/catalog.py`.
4. Test valid, malformed, bounded, privacy, CLI, and disabled-provider behavior.
5. Run the architecture, typing, full pytest, build, and installed-wheel gates.

The [hello provider](../examples/providers/hello-provider/) is an installable
external template. It intentionally does not write evidence; replace its
executor with a bounded importer or enricher and test the resulting runpack
through public readers.
