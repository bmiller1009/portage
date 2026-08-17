# api

The FastAPI application: routers, request/response schemas, and the OpenAPI surface described in `docs/architecture/spec.md` §30. This is the only HTTP entry point into the control plane — the CLI, SDK, and UI are all clients of this API, not privileged callers.

Currently implemented: `GET /health`, `GET /ready` (`main.py`). The `/v1/runs`, `/v1/workloads`, `/v1/environments`, `/v1/datasets`, `/v1/providers`, `/v1/validate`, and `/v1/conformance` routes are v0.1+ scope — see the v0.1 milestone.

Business logic belongs in `control_plane/`, not here — routers should stay thin and delegate.
