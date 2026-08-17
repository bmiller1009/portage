# control_plane

Domain services called by `api/`: authentication/authorization, workload validation, environment resolution, dataset resolution, the provider registry, run lifecycle management, audit logging, reconciliation coordination, and portability/conformance checks (`docs/architecture/spec.md` §6).

This package must stay thin (§4.4): it coordinates workloads and never becomes a data warehouse, distributed scheduler, secret vault, identity provider, object store, notebook environment, ETL designer, proprietary catalog, or query optimizer.

`run_state.py` (the canonical `RunState` enum, spec §23) and `execution_provider.py` (the `ExecutionProvider` protocol and its supporting dataclasses, spec §13) are pure domain modeling with no I/O. `db.py`/`models.py`/`repositories.py` are the real persistence layer (PostgreSQL via async SQLAlchemy, migrated with Alembic — `alembic/`): `Environment`, `ExecutionProfile`, `StorageProfile`, `DatasetBinding`, `WorkloadDefinition`, replacing the Phase-0 file/env-var stand-in in `cli/environments.py` (which the CLI's `plane run` still uses — the REST-backed `plane environment list`/`plane dataset list` and the persistence layer are separate from run submission for now). Auth, run lifecycle storage, audit, and reconciliation are still open v0.1 backlog items.
