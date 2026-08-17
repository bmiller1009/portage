# api

The FastAPI application: routers, request/response schemas, and the OpenAPI surface described in `docs/architecture/spec.md` §30. This is the only HTTP entry point into the control plane — the CLI, SDK, and UI are all clients of this API, not privileged callers.

Currently implemented: `GET /health`, `GET /ready` (`main.py`), and CRUD for `/v1/execution-profiles`, `/v1/storage-profiles`, `/v1/environments`, `/v1/datasets`, `/v1/workloads` (`routers/`) — backed by real PostgreSQL persistence, see `control_plane/db.py`/`models.py`/`repositories.py` and `alembic/`. `POST /v1/runs` and the run status/cancel/logs endpoints are the next v0.1 backlog item — `plane run` still calls execution providers directly until then (see `cli/main.py`).

Business logic belongs in `control_plane/`, not here — routers should stay thin and delegate.
