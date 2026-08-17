# tests/chaos

Failure injection (`docs/architecture/spec.md` §56–57): kill Spark executor/driver, kill a Kubernetes worker, restart the control-plane API/reconciler/PostgreSQL connection, expire credentials, inject storage latency, inject API 429/5xx, interrupt network during/after submission. The control-plane recovery test (§57) — destroy the entire control plane while a job is running, confirm the job continues and reconciliation converges on restart — is a mandatory v1.0 acceptance test.

Not yet implemented — v0.4 milestone.
