# deploy/dev

Manifests `scripts/dev-up.sh` applies to bring up the local `make dev` stack (spec §45) — nothing here is meant for a real deployment, only a local `kind` cluster.

- `kind-config.yaml` — the 1-control-plane/2-worker cluster shape used throughout Phase 0/v0.1 development.
- `rbac-deletecollection.yaml` — supplementary `ClusterRole`/`RoleBinding` for a gap in the Spark Kubernetes Operator Helm chart's default RBAC: it grants `delete` but not `deletecollection` on pods/configmaps/persistentvolumeclaims, which Spark's own Kubernetes scheduler backend needs for bulk executor/PVC cleanup on shutdown. Without it, a driver's *shutdown* can throw even though the actual computation succeeded — see `docs/providers/kubernetes.md`. Additive, not an edit to the chart-owned `spark-workload-clusterrole`.
- `minio.yaml` — MinIO `Deployment`/`Service` in namespace `portage-storage`, with dev-only credentials (never used outside the local `kind` cluster).
