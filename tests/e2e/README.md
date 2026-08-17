# tests/e2e

End-to-end portability tests (`docs/architecture/spec.md` §55): the canonical fixture workload (read table → Spark transform → write table) run across every available combination of execution provider and storage provider, with semantic result comparison. This is intended to become the project's most important CI artifact.

The Phase 0 exit demonstration — Kubernetes+S3 and Databricks+S3 with the same artifact — is the seed of this suite.
