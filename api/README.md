# api

The FastAPI application: routers, request/response schemas, and the OpenAPI surface described in `docs/architecture/spec.md` §30. This is the only HTTP entry point into the control plane — the CLI, SDK, and UI are all clients of this API, not privileged callers.

Currently implemented: `GET /health`, `GET /ready` (`main.py`); CRUD for `/v1/execution-profiles`, `/v1/storage-profiles`, `/v1/environments`, `/v1/datasets`, `/v1/workloads`; and `POST /v1/runs`/`GET /v1/runs/{id}`/`GET /v1/runs/{id}/events` (`routers/`) — all backed by real PostgreSQL persistence (`control_plane/db.py`/`models.py`/`repositories.py`/`run_service.py`, `alembic/`). `POST /v1/runs` only ever writes `ACCEPTED` and returns — `reconciler/` is what actually submits to a provider and advances the state, verified live against the remote Kubernetes cluster. Cancel/logs endpoints and migrating `plane run` off its current direct-provider path are the next v0.1 backlog item (`cli/main.py`).

Business logic belongs in `control_plane/`, not here — routers should stay thin and delegate.
