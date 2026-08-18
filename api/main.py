"""FastAPI control-plane app (docs/architecture/spec.md §30). Health
endpoints, CRUD for environments/profiles/datasets/workloads (persistence
layer — v0.1 issue #17), run submission/cancel/logs (issue #18/#20) — async:
POST /v1/runs only ever writes ACCEPTED and returns; reconciler/service.py
is what actually submits to an execution provider and advances the state.
POST /v1/validate (issue #24) is the one exception to "the API never talks
to providers" for a mutating action — validate() is read-only against the
provider, so it's answered synchronously rather than deferred."""

from fastapi import FastAPI

from api.routers import (
    datasets,
    environments,
    execution_profiles,
    runs,
    storage_profiles,
    validate,
    workloads,
)

app = FastAPI(title="Portage Control Plane")

app.include_router(execution_profiles.router)
app.include_router(storage_profiles.router)
app.include_router(environments.router)
app.include_router(datasets.router)
app.include_router(workloads.router)
app.include_router(runs.router)
app.include_router(validate.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ok"}
