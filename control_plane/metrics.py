"""Shared metric instrument definitions (docs/architecture/spec.md §29).
OpenTelemetry Metrics API for instrumentation, exported as Prometheus text
via opentelemetry-exporter-prometheus's PrometheusMetricReader — matching
§29's "OpenTelemetry should be the primary telemetry standard" while still
producing the Prometheus exposition format `GET /metrics` implies.

Imported by both api/main.py and reconciler/main.py — each is a separate
process (spec §4.5, ADR 0007) with its own Python interpreter, so each gets
its own independent MeterProvider/registry purely from process isolation;
no cross-process plumbing is needed. Every instrument here is recorded
synchronously at the exact call site that observes it (run creation in
run_service.create_run(), state transitions in transition_run_state(),
latencies in reconciler/service.py) rather than computed at scrape time
from Postgres — OTel has no asynchronous/observable histogram instrument,
and even the counters are simpler as plain synchronous increments than as
observable callbacks that would need to run a DB query from inside the
SDK's synchronous collection path.

Because of this, run-outcome counts reflect whichever process actually
made the transition: reconciler/service.py accounts for nearly all
SUCCEEDED/FAILED/CANCELED transitions, but api/routers/runs.py's cancel_run
finalizes an ACCEPTED run straight to CANCELED without the reconciler ever
seeing it — that one increments on the API process's own counter, not the
reconciler's. A dashboard that only scrapes one target undercounts by
exactly that edge case; summing both scrape targets (the normal multi-target
Prometheus pattern) gives the true total.
"""

import time

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider

_reader = PrometheusMetricReader()
_provider = MeterProvider(metric_readers=[_reader])
metrics.set_meter_provider(_provider)
_meter = metrics.get_meter("portage")

runs_created_total = _meter.create_counter(
    "portage_runs_created_total",
    description="Runs created via POST /v1/runs (excludes idempotency-key replays).",
)
runs_terminal_total = _meter.create_counter(
    "portage_runs_terminal_total",
    description="Runs reaching a terminal state, labeled by state (SUCCEEDED/FAILED/CANCELED/LOST).",
)
api_errors_total = _meter.create_counter(
    "portage_api_errors_total",
    description="Non-2xx HTTP responses from the API, labeled by method/route/status_code.",
)

submission_latency_seconds = _meter.create_histogram(
    "portage_run_submission_latency_seconds",
    unit="s",
    description="Time from a run being accepted (ACCEPTED) to being submitted to a provider (QUEUED).",
)
queue_latency_seconds = _meter.create_histogram(
    "portage_run_queue_latency_seconds",
    unit="s",
    description="Time a run spent queued at the provider before it started running.",
)
execution_duration_seconds = _meter.create_histogram(
    "portage_run_execution_duration_seconds",
    unit="s",
    description="Time a run spent actually running (RUNNING) before reaching a terminal state.",
)
provider_errors_total = _meter.create_counter(
    "portage_provider_errors_total",
    description=(
        "Exceptions caught while submitting/polling/canceling a run at a provider "
        "(reconciler/service.py's except blocks). Also covers storage-resolution "
        "and portability-validation failures (spec §29) — both currently surface "
        "the same way, as a caught exception turned into a FAILED transition; "
        "finer-grained categorization is a known simplification, not yet needed "
        "by any real consumer."
    ),
)

_last_successful_reconcile: float | None = None


def record_successful_reconcile() -> None:
    global _last_successful_reconcile
    _last_successful_reconcile = time.monotonic()


def _reconciliation_lag_callback(_options: CallbackOptions):
    if _last_successful_reconcile is None:
        return []
    return [Observation(time.monotonic() - _last_successful_reconcile)]


reconciliation_lag_seconds = _meter.create_observable_gauge(
    "portage_reconciliation_lag_seconds",
    callbacks=[_reconciliation_lag_callback],
    unit="s",
    description="Seconds since this process last completed a reconcile_once() loop successfully.",
)


def render_prometheus_text() -> bytes:
    """Renders this process's own registry as Prometheus exposition text —
    used by the API's GET /metrics route. The reconciler doesn't need this;
    it uses start_metrics_server() instead (a dedicated port, not a FastAPI
    route)."""
    from prometheus_client import generate_latest

    return generate_latest()


def start_metrics_server(port: int) -> None:
    """Exposes this process's registry at :port/metrics via
    prometheus_client's own HTTP server — PrometheusMetricReader already
    registers its collector with the global prometheus_client REGISTRY, so
    this one call is enough. Used by the reconciler, which has no other
    HTTP surface of its own."""
    from prometheus_client import start_http_server

    start_http_server(port)
