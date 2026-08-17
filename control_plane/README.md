# control_plane

Domain services called by `api/`: authentication/authorization, workload validation, environment resolution, dataset resolution, the provider registry, run lifecycle management, audit logging, reconciliation coordination, and portability/conformance checks (`docs/architecture/spec.md` §6).

This package must stay thin (§4.4): it coordinates workloads and never becomes a data warehouse, distributed scheduler, secret vault, identity provider, object store, notebook environment, ETL designer, proprietary catalog, or query optimizer.

`run_state.py` (the canonical `RunState` enum, spec §23) and `execution_provider.py` (the `ExecutionProvider` protocol and its supporting dataclasses, spec §13) are pure domain modeling with no I/O. `db.py`/`models.py`/`repositories.py` are the real persistence layer (PostgreSQL via async SQLAlchemy, migrated with Alembic — `alembic/`): config/definition tables (`Environment`, `ExecutionProfile`, `StorageProfile`, `DatasetBinding`, `WorkloadDefinition`) and run lifecycle tables (`Run`, `ProviderRun`, `RunEvent`, `IdempotencyKey`).

`provider_factory.py` and `credentials.py` are the database-backed equivalent of `cli/environments.py`'s file/env-var translation — turning a persisted `ExecutionProfile`/`StorageProfile` row into a live `ExecutionProvider`/Spark storage config, used by `reconciler/`. `run_service.py` is the shared run-lifecycle logic (idempotent creation, state transitions that always log a `run_event`) both `api/routers/runs.py` and `reconciler/service.py` call.

`plane run` (the CLI) still uses the older `cli/environments.py` file/env-var path directly — migrating it onto this real persistence + async submission path is the next v0.1 backlog item. Auth and audit logging remain open.
