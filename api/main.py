"""FastAPI control-plane app (docs/architecture/spec.md §30). Health
endpoints plus CRUD for environments/profiles/datasets/workloads
(persistence layer — v0.1 backlog issue #17). Run submission/status/cancel/
logs (§24, §30) are a later v0.1 issue — plane run still calls execution
providers directly for now (see cli/main.py)."""

from fastapi import FastAPI

from api.routers import datasets, environments, execution_profiles, storage_profiles, workloads

app = FastAPI(title="Portage Control Plane")

app.include_router(execution_profiles.router)
app.include_router(storage_profiles.router)
app.include_router(environments.router)
app.include_router(datasets.router)
app.include_router(workloads.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ok"}
