# Hello provider

This tiny distribution is the minimum external Contrail provider. Install it in
the same environment as Contrail, then enable its entry-point key explicitly:

```bash
uv pip install -e examples/providers/hello-provider
CONTRAIL_ENABLE_PROVIDERS=hello contrail hello-provider contributor
```

The example only demonstrates discovery, configuration, and execution. A real
evidence provider should validate untrusted input, normalize it into Contrail's
existing model, publish artifacts atomically, and add focused privacy and
malformed-input tests.
