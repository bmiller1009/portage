# providers/storage/vast

VAST storage provider. Supports two distinct, explicitly-configured access modes — VAST NFS (via Kubernetes CSI) and VAST S3 — and does not treat them as equivalent internally (`docs/architecture/spec.md` §48), via two separate classes rather than one class with a mode flag.

**S3 mode — implemented and live-verified.** `VastS3StorageProvider` delegates to `S3StorageProvider` (`providers/storage/s3/`), since VAST's S3 mode is genuinely S3-API-compatible — only `capabilities()` differs (`protocol="vast-s3"`), so there's no logic to duplicate or drift. No real VAST appliance is available to this project, but because the wire protocol really is S3, this was live-verified against the real MinIO already running on the remote dev box, configured as a `StorageProfile` with `provider="vast"`, `config.protocol="s3"` — a real `plane run` succeeded through it end-to-end.

**NFS mode — not yet implemented.** Kubernetes-CSI-backed NFS access isn't expressible as Spark config at all (no `spark.hadoop.*` equivalent) — it needs pod volume mounts on the SparkApplication CRD, a cross-cutting concern with the Kubernetes execution provider that doesn't exist yet. Tracked separately.
