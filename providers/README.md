# providers

All infrastructure-specific behavior lives here, behind the `ExecutionProvider` and storage provider interfaces (`docs/architecture/spec.md` §13, §47). Nothing outside `providers/` should contain provider-specific branching (`if provider == "databricks"`) — the rest of the system is capability-driven (§20, ADR 0005).

- `execution/` — translates a resolved portable workload into a provider-native submission (Kubernetes, Databricks).
- `storage/` — resolves logical dataset identifiers into provider-native URIs and Spark configuration (VAST, S3, ADLS).
- `identity/` — OIDC and machine-identity integration (§33–34).

A provider is not "supported" merely because it compiles — it must pass the provider conformance suite (§43, `tests/contract/`).
