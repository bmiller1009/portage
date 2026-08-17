"""FastAPI control-plane app. Currently exposes only the health endpoints
from docs/architecture/spec.md §30 — everything else is v0.1 milestone scope."""

from fastapi import FastAPI

app = FastAPI(title="Portage Control Plane")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ok"}
