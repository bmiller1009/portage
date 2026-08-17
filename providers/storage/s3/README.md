# providers/storage/s3

S3 storage provider: URI resolution, credential generation/federation, Spark Hadoop S3 configuration, endpoint/path-style configuration, and connectivity tests (`docs/architecture/spec.md` §49). AWS S3 is the initial supported target; S3-compatible vendors are handled through configuration rather than as separate core providers unless behavior genuinely differs.

`provider.py` implements `S3StorageProvider`. Unit-tested against `moto` (`tests/unit/test_s3_provider.py`); `tests/integration/test_s3_provider_minio.py` runs the same code against a real MinIO endpoint (deployed on the Phase 0 remote cluster as an S3-API-compatible stand-in — see `docs/providers/s3.md`). The `endpoint_url` config is what makes MinIO and real AWS S3 interchangeable through configuration alone, per spec §49.
