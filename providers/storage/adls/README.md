# providers/storage/adls

Azure Data Lake Storage (ABFS) provider, mirroring the S3 provider's responsibilities: logical mapping, identity, filesystem/account configuration, Spark connector configuration, and health checks (`docs/architecture/spec.md` §50). Workload identity is preferred over static storage account keys.

Not yet implemented — v0.2 milestone.
