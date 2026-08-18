# Compatibility policy

This policy defines the `.runpack` compatibility boundary for the 0.9 release
line. Package versions and runpack schema versions are independent: a newer
Contrail package does not imply a new artifact schema.

## Runpack schema 1

Contrail 0.9 writers create schema 1.1 runpacks. Read-only commands accept a
schema with major version 1 when the required core tables, columns, keys,
relationships, value bounds, and single-file SQLite safety rules remain valid.
Readers ignore additional tables and columns, so a structurally valid additive
future 1.x schema remains inspectable.

Read compatibility does not grant write compatibility. Enrichment and other
writer operations accept only the schemas whose mutation semantics this release
knows: `1`, `1.0`, and `1.1`. They reject an unknown future 1.x schema before
writing, even when the read-only validator can inspect it. This prevents an old
writer from discarding or contradicting requirements introduced by a newer
producer.

Schema 1.1 adds the optional `attachments` table. Adding attachments to a
schema `1` or `1.0` artifact atomically creates the table and upgrades the
manifest to `1.1`. If attachment validation or insertion fails, both the table
creation and manifest change roll back.

All schema versions remain subject to the artifact safety rules: readers reject
unknown major versions, missing required structure, invalid relationships,
executable triggers, and WAL-mode databases that may depend on unshipped
sidecars. Additive compatibility does not weaken those checks.

## Change rules

- Adding an optional table or nullable column may use a new schema 1.x minor.
- Changing the meaning or type of an existing field, making optional evidence
  required, or removing a field requires a new schema major.
- A producer must update the manifest in the same transaction as the first
  change that requires a newer schema minor.
- Runpack compatibility is logical. SQLite page layout and file bytes are not a
  stable serialization contract.

Historical schema 1 and 1.1 fixtures live in
`tests/fixtures/compat/`. Their provenance and SHA-256 identities are checked in
with the fixtures, and the compatibility tests materialize fresh databases from
the immutable SQL sources.

## Explained-report bindings

Explained Proofline verification and experiment documents may carry
`artifact_bindings` for the exact baseline and candidate runpack bytes. Each
binding is a SHA-256 digest plus byte size. This is an additive format-version-1
field: legacy binding-less version-1 reports remain readable. The retained UI
labels assertion-bearing legacy reports as semantic replay against current
evidence, and gives assertion-less report-authored policy/results a still lower
assurance. Non-explained JSON and text output are unchanged.

A binding is a file identity, not a logical runpack identity. SQLite `VACUUM`,
enrichment, attachment changes, or any other reserialization can change the
digest or byte size even when selected normalized facts remain equivalent. Such
an artifact is intentionally a different identity and requires a newly
generated explained report. This does not change the schema compatibility rule
above: compatible runpacks need not have stable bytes.
