# providers/execution/kubernetes

Translates a `ResolvedWorkload` into an Apache Spark Kubernetes Operator `SparkApplication` resource and submits it via the official Kubernetes Python client (`docs/architecture/spec.md` §14, ADR 0004). Certified target: Kubernetes 1.34+, Spark Kubernetes Operator 1.0+, Apache Spark 4.2 (§46).

This package must never implement its own Spark scheduler — pod lifecycle is entirely owned by the upstream operator. Default `spark.local.dir` is node-local ephemeral storage, not networked/persistent storage (§15) — that must remain explicit opt-in.

`provider.py` implements `KubernetesExecutionProvider`, translated against the CRD shape of the official Spark Kubernetes Operator (chart 1.8.0 / app 1.0.0) confirmed live on a kind cluster running Kubernetes 1.34, and unit-tested against a fake `CustomObjectsApi` (`tests/unit/test_kubernetes_provider.py`). The `image/` directory holds the framework-managed Spark base image (spec §17): a generic entry-point launcher layered onto `apache/spark:4.2.0-python3`. See `docs/providers/kubernetes.md` for exact versions and live-run results.

`KubernetesProfile.runtime_profiles` (spec §18) maps a named runtime profile to a `nodeSelector` hint, merged into `driverSpec`/`executorSpec`'s `podTemplateSpec` alongside any VAST NFS volume mounts (`_pod_spec()` builds both from the same mechanism). Live-verified: a workload declaring `runtime.profile: high-memory` against an environment whose `runtimeProfiles.high-memory.nodeSelector` targeted one specific real node — the driver pod actually scheduled there, confirmed via `kubectl get pod -o wide`, not just a manifest-shape assertion.
