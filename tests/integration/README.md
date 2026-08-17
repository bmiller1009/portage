# tests/integration

Tests against real infrastructure — mocks are insufficient for this layer (`docs/architecture/spec.md` §53). Two different CI stories, depending on what the infrastructure actually is:

- `db/` — real PostgreSQL. **Runs in CI** (`persistence-tests` job, `.github/workflows/ci.yml`) using GitHub Actions' built-in `postgres:16` service container — no self-hosted runner needed.
- Everything else (`test_s3_provider_minio.py`, and eventually real Spark-on-Kubernetes runs) needs a live Kubernetes cluster / MinIO deployment that only exists on the project's remote dev box — **excluded from CI**, run manually over SSH. See `docs/providers/kubernetes.md` and `docs/providers/s3.md` for how that infrastructure is stood up and what's been verified against it.
