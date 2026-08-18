# control_plane

Domain services called by `api/`: authentication/authorization, workload validation, environment resolution, dataset resolution, the provider registry, run lifecycle management, audit logging, reconciliation coordination, and portability/conformance checks (`docs/architecture/spec.md` §6).

This package must stay thin (§4.4): it coordinates workloads and never becomes a data warehouse, distributed scheduler, secret vault, identity provider, object store, notebook environment, ETL designer, proprietary catalog, or query optimizer.

`run_state.py` (the canonical `RunState` enum, spec §23) and `execution_provider.py` (the `ExecutionProvider` protocol and its supporting dataclasses, spec §13) are pure domain modeling with no I/O. `execution_provider.py` also has `match_capabilities()` (spec §20-21) — fail-fast capability matching shared by every provider's `validate()`, so both providers and `POST /v1/validate` run the exact same check. `db.py`/`models.py`/`repositories.py` are the real persistence layer (PostgreSQL via async SQLAlchemy, migrated with Alembic — `alembic/`): config/definition tables (`Environment`, `ExecutionProfile`, `StorageProfile`, `DatasetBinding`, `WorkloadDefinition`) and run lifecycle tables (`Run`, `ProviderRun`, `RunEvent`, `IdempotencyKey`).

`provider_factory.py` and `credentials.py` turn a persisted `ExecutionProfile`/`StorageProfile` row into a live `ExecutionProvider`/Spark storage config, used by `reconciler/`. `run_service.py` is the shared run-lifecycle logic (idempotent creation, state transitions that always log a `run_event`, cancellation, log-reference lookup, static capability validation via `validate_workload()`) that `api/routers/runs.py`/`api/routers/validate.py` and `reconciler/service.py` both call.

`plane` (the CLI) is now a pure REST client (spec §31) — `cli/main.py`'s `run`/`status`/`cancel`/`logs` all go through `/v1/runs`, never through this package or a provider directly; the file/env-var direct-provider path (`cli/environments.py`) has been retired. Auth and audit logging remain open.
