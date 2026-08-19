# docs/providers

Per-provider documentation: configuration schema, capability matrix, supported Spark/runtime versions, and known semantic differences from upstream Spark, for each execution and storage provider under `providers/`. One file per provider, added as that provider ships.

- [`kubernetes.md`](kubernetes.md) — live-verified during Phase 0 against a real cluster: exact versions, the SparkApplication CRD shape, and what it actually took (RBAC, operator restart) to get a real job running.
- [`s3.md`](s3.md) — live-verified during Phase 0 against MinIO as an S3-API-compatible backend.
- [`query.md`](query.md) — v0.5's Iceberg REST Catalog / Trino / Kyuubi query plane: table dataset bindings, direct JDBC connection strings, and the real Iceberg/Spark/S3 compatibility gaps found live-verifying it.
- [`databricks.md`](databricks.md) — live-verified since v0.3 against a real workspace: Serverless-only findings, the Spark<->Databricks Runtime compatibility table (confirmed live, including v1.0.0's Spark 4.2/DBR 19 addition), and real end-to-end run results (see also `providers/execution/databricks/README.md`).
