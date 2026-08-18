"""FastAPI control-plane app (docs/architecture/spec.md §30). Health
endpoints, CRUD for environments/profiles/datasets/workloads (persistence
layer — v0.1 issue #17), run submission/cancel/logs (issue #18/#20) — async:
POST /v1/runs only ever writes ACCEPTED and returns; reconciler/service.py
is what actually submits to an execution provider and advances the state.
POST /v1/validate (issue #24) is the one exception to "the API never talks
to providers" for a mutating action — validate() is read-only against the
provider, so it's answered synchronously rather than deferred.

GET /metrics (issue #22, spec §29) exposes this process's own OpenTelemetry/
Prometheus registry (control_plane/metrics.py) — run-creation counts, some
terminal-state counts, and API error rates. Submission/queue/execution
latencies, provider errors, and reconciliation lag live on the reconciler's
own :9091/metrics instead (reconciler/main.py) — separate process, separate
memory, see control_plane/metrics.py's docstring for why."""

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST

from api.routers import (
    artifacts,
    audit,
    conformance,
    datasets,
    environments,
    execution_profiles,
    providers,
    runs,
    storage_profiles,
    validate,
    workloads,
)
from control_plane import metrics

app = FastAPI(title="Portage Control Plane")

# ui/ (spec §32) is a separate Vite dev server on its own port, so it's a
# cross-origin caller of this API by construction — allowing all origins
# is a dev-only stance (no cookies/credentials are ever sent) — auth
# itself is real (api/auth.py, spec §33) but its own enforcement is
# opt-in (PORTAGE_AUTH_MODE), so this stays permissive by default too,
# not something to carry into a real deployment unreviewed.
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

app.include_router(execution_profiles.router)
app.include_router(storage_profiles.router)
app.include_router(environments.router)
app.include_router(datasets.router)
app.include_router(artifacts.router)
app.include_router(workloads.router)
app.include_router(runs.router)
app.include_router(validate.router)
app.include_router(audit.router)
app.include_router(providers.router)
app.include_router(conformance.router)


def _route_path(request: Request) -> str:
    route = request.scope.get("route")
    return route.path if route is not None else request.url.path


@app.middleware("http")
async def count_api_errors(request: Request, call_next):
    try:
        response = await call_next(request)
    except Exception:
        # An unhandled exception never reaches the status-code check below —
        # Starlette's BaseHTTPMiddleware re-raises rather than returning a
        # response for it — so it needs its own count before re-raising.
        metrics.api_errors_total.add(
            1, {"method": request.method, "route": _route_path(request), "status_code": "500"}
        )
        raise
    if response.status_code >= 400:
        metrics.api_errors_total.add(
            1,
            {
                "method": request.method,
                "route": _route_path(request),
                "status_code": str(response.status_code),
            },
        )
    return response


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics_endpoint() -> Response:
    return Response(content=metrics.render_prometheus_text(), media_type=CONTENT_TYPE_LATEST)
