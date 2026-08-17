# control_plane

Domain services called by `api/`: authentication/authorization, workload validation, environment resolution, dataset resolution, the provider registry, run lifecycle management, audit logging, reconciliation coordination, and portability/conformance checks (`docs/architecture/spec.md` §6).

This package must stay thin (§4.4): it coordinates workloads and never becomes a data warehouse, distributed scheduler, secret vault, identity provider, object store, notebook environment, ETL designer, proprietary catalog, or query optimizer.

Not yet implemented — v0.1 milestone.
