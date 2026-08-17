"""FastAPI control-plane app (docs/architecture/spec.md §30). Health
endpoints, CRUD for environments/profiles/datasets/workloads (persistence
layer — v0.1 issue #17), and run submission (POST /v1/runs, issue #18) —
async: this only ever writes ACCEPTED and returns; reconciler/service.py
is what actually submits to an execution provider and advances the state.
Cancel/logs endpoints and migrating `plane run` off its current
direct-provider path are issue #20."""

from fastapi import FastAPI

from api.routers import (
    datasets,
    environments,
    execution_profiles,
    runs,
    storage_profiles,
    workloads,
)

app = FastAPI(title="Portage Control Plane")

app.include_router(execution_profiles.router)
app.include_router(storage_profiles.router)
app.include_router(environments.router)
app.include_router(datasets.router)
app.include_router(workloads.router)
app.include_router(runs.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ok"}
