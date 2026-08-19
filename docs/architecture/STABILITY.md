# Schema stability policy

v1.0 (spec.md §71, "Portable Contract" exit criterion) requires the workload,
environment, dataset, and artifact specs to be **stable**. This document is
that promotion: as of v1.0, the schemas previously tagged `v1alpha1` are
promoted to `v1` under the field shapes already in place — this is a rename,
not a redesign. Every field, type, and validation rule in
`spec/workload/v1alpha1.py`, `spec/environment/v1alpha1.py`,
`spec/dataset/v1alpha1.py`, and `spec/artifact/v1alpha1.py` is unchanged by
this promotion.

(The modules themselves keep their `v1alpha1.py` filenames/import paths —
only the `apiVersion` string a document declares changes. Renaming the
Python modules/packages would be a needless breaking change to every
existing `from spec.workload.v1alpha1 import ...` in this codebase and any
external caller, for zero benefit.)

## What changed

Every schema's `apiVersion` field now accepts two strings:

- `runtime/v1` — the stable, canonical name going forward. New workload,
  environment, dataset, and artifact documents should use this.
- `runtime/v1alpha1` — still accepted, for backward compatibility. Parsing
  a document with this `apiVersion` now emits a `DeprecationWarning`
  (`spec/stability.py::warn_if_deprecated`) instead of failing.

Nothing that already targets `v1alpha1` breaks. Every example under
`examples/` and this repository's own reconciler-internal constructions
have already been updated to `runtime/v1`; only test fixtures that
deliberately exercise the deprecated string still use it.

## Deprecation policy going forward

- A deprecated `apiVersion` string continues to parse, with a warning, for
  at least the next two minor releases after the version that deprecates
  it.
- A deprecated string is only removed (turned into a hard validation
  error) in a major version bump, never a minor or patch release.
- Any future schema change follows the same pattern used here: additive
  fields with safe defaults parse unchanged under the old `apiVersion`
  (e.g. `RequirementsSpec`'s all-`False` defaults, `ApplicationSpec`'s
  `spark-declarative-pipeline` variant); a field-shape-breaking change is
  never made silently under an existing `apiVersion` name — it requires a
  new one (e.g. a hypothetical future `runtime/v2`), following the same
  promotion mechanics documented here.

## What "stable" means

- No field is removed or has its type/semantics changed without a new
  `apiVersion`.
- New optional fields may be added freely (as `RequirementsSpec` already
  demonstrates) without changing `apiVersion` at all, since they parse as
  a no-op default for any document that predates them.
- The JSON Schema exported by `spec.workload.v1alpha1.json_schema()`
  (spec §43 — publicly documented) is the authoritative machine-readable
  shape of the workload spec, kept in sync with this document by the same
  test suite that already validates every example fixture parses.

## Scope

This promotion covers the four portable-contract schemas: `SparkWorkload`,
`Environment`, `Dataset`, `Artifact`. It does not cover the REST API's own
versioning (`/v1/...` routes, unrelated to this `apiVersion` field) or the
provider extension surface, which is documented separately in
[PROVIDER_SDK.md](PROVIDER_SDK.md).
