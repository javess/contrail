# Security policy

## Supported versions

Security fixes are made for the current 0.10 release series. Development snapshots
and older pre-release series are not guaranteed backports. Platform and artifact
compatibility details are in `docs/support.md`; security boundaries are in
`docs/threat-model.md`.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability or include
sensitive runpacks, logs, credentials, or exploit details in public discussions.
Use [the repository security page](https://github.com/javess/contrail/security)
and its private vulnerability-reporting channel when available. If that channel
is unavailable, contact a maintainer privately through the address or private
contact method published by the repository host and ask for a secure reporting
channel before sending evidence.

Include the affected version, operating system and Python version, the smallest
reproduction you can safely share, expected and observed impact, and whether the
issue requires a malicious workload, malicious evidence file, network access,
or a same-UID local process. Remove unrelated secrets and personal data.

Maintainers should acknowledge receipt privately, reproduce and assess the
report, coordinate a fix and compatibility tests, and agree on disclosure timing
with the reporter. No fixed response-time promise is made by this developer
project. Credit is offered when requested and safe.

## Scope

Examples of in-scope reports include arbitrary file overwrite or deletion,
publication of partial or attacker-substituted artifacts, mutation through the
read-only query surface, parser behavior that escapes documented resource
bounds, secret exposure contrary to documented defaults, or cross-run evidence
contamination.

The arbitrary effects of an explicitly executed workload, a hostile process
already running as the same user, secrets
placed in argv/imported logs/opt-in attachments, and behavior on unsupported
filesystems are documented limitations rather than vulnerabilities by
themselves. Reports that show an additional boundary crossing remain welcome.
