# charts

Helm charts for deploying Portage's own control plane to Kubernetes (`docs/architecture/spec.md` §46). The control plane itself remains container-portable — this is the deployment packaging, not a dependency of it.

- [`portage/`](portage/) — the API + reconciler chart. Live-verified (v1.0): 2+2 HA replicas, a real PostgreSQL-backed migration hook, and TLS termination at an `Ingress`. Does not bundle PostgreSQL (spec §46: "external/HA PostgreSQL," bring your own) — see the chart's own README.
