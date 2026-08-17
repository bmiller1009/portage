# providers/storage

Storage providers resolve logical dataset identifiers (`claims.raw`) into environment-specific URIs/table identifiers and generate the Spark configuration needed to read/write them (`docs/architecture/spec.md` §47). Every storage provider implements: configuration validation, URI/path resolution, credential resolution, health/connectivity checks, Spark configuration generation, diagnostics, and capability declaration.

Storage providers must not pretend NFS, S3, and ADLS have identical semantics (§74.1, §74.6) — they normalize configuration and logical addressing, not behavior.
