# providers/storage/adls

Azure Data Lake Storage (ABFS) provider, mirroring the S3 provider's responsibilities: logical mapping (`abfss://<container>@<account>.dfs.core.windows.net/<path>`), identity, filesystem/account configuration, Spark connector configuration, and health checks (`docs/architecture/spec.md` §50), via the real `azure-storage-file-datalake`/`azure-identity` SDKs.

Workload identity is preferred over static storage account keys (spec §50): `AdlsStorageProvider.spark_config()` emits `spark.hadoop.fs.azure.account.key.*` when `AdlsCredentials.account_key` is set, or the OAuth/MSI token-provider config (`spark.hadoop.fs.azure.account.auth.type.*` = `OAuth`) when it's `None` — `control_plane/credentials.py`'s `resolve_adls_credentials()` treats a missing `{reference}_ACCOUNT_KEY` env var as "use workload identity," not an error.

**Translation-layer prototype only** — no self-hostable open-source ADLS substitute and no real Azure subscription is available to this project, so this is tested against a fake `AdlsClientLike` client (`tests/unit/test_adls_provider.py`), never a real Azure account. Same pattern, same caveat, as the Databricks execution provider.
