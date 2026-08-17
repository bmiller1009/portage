# control_plane

Domain services called by `api/`: authentication/authorization, workload validation, environment resolution, dataset resolution, the provider registry, run lifecycle management, audit logging, reconciliation coordination, and portability/conformance checks (`docs/architecture/spec.md` §6).

This package must stay thin (§4.4): it coordinates workloads and never becomes a data warehouse, distributed scheduler, secret vault, identity provider, object store, notebook environment, ETL designer, proprietary catalog, or query optimizer.

Two pieces exist so far, both pure domain modeling with no I/O: `run_state.py` (the canonical `RunState` enum, spec §23) and `execution_provider.py` (the `ExecutionProvider` protocol and its supporting dataclasses, spec §13 — implemented by `providers/execution/kubernetes` and `providers/execution/databricks`). Everything else listed above — auth, environment/dataset *persistence*, the provider registry, run lifecycle storage, audit, reconciliation — is still v0.1 milestone scope; see `cli/environments.py` for the Phase-0 stand-in.
