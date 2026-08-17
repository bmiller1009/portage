# providers/execution/kubernetes

Translates a `ResolvedWorkload` into an Apache Spark Kubernetes Operator `SparkApplication` resource and submits it via the official Kubernetes Python client (`docs/architecture/spec.md` §14, ADR 0004). Certified target: Kubernetes 1.34+, Spark Kubernetes Operator 1.0+, Apache Spark 4.2 (§46).

This package must never implement its own Spark scheduler — pod lifecycle is entirely owned by the upstream operator. Default `spark.local.dir` is node-local ephemeral storage, not networked/persistent storage (§15) — that must remain explicit opt-in.

Not yet implemented — Phase 0 milestone (`Kubernetes execution provider prototype`).
