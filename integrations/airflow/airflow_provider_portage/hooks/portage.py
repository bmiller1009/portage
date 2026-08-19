"""PortageHook -- a thin REST client over the Portage control-plane API
(docs/architecture/spec.md §31/§39: cross-system orchestrators call the
public job API, never the database or a provider directly). Deliberately
self-contained (its own httpx calls, not a dependency on the still-empty
sdk/python/), mirroring cli/main.py's own hand-rolled approach."""

from __future__ import annotations

import os
from typing import Any

import httpx
from airflow.sdk import BaseHook


class PortageHook(BaseHook):
    conn_name_attr = "portage_conn_id"
    default_conn_name = "portage_default"
    conn_type = "portage"
    hook_name = "Portage"

    def __init__(self, portage_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.portage_conn_id = portage_conn_id

    def _api_base_url(self) -> str:
        """Connection host/schema/port if a real Airflow Connection named
        portage_conn_id is configured; otherwise $PORTAGE_API_URL,
        matching the CLI's own convention, so this hook works with zero
        Airflow-side setup beyond that one env var."""
        try:
            conn = self.get_connection(self.portage_conn_id)
        except Exception:  # noqa: BLE001 - no connection configured is a normal, expected case
            conn = None
        if conn is not None and conn.host:
            scheme = conn.schema or "http"
            port = f":{conn.port}" if conn.port else ""
            return f"{scheme}://{conn.host}{port}"
        return os.environ.get("PORTAGE_API_URL", "http://localhost:8000")

    def register_workload(self, workload: dict[str, Any]) -> dict[str, Any]:
        """Idempotent from the caller's point of view -- a 409 just means
        someone (possibly a previous DAG run) already registered this
        exact (name, version), same as cli/main.py's `plane run`."""
        resp = httpx.post(f"{self._api_base_url()}/v1/workloads", json=workload)
        if resp.status_code not in (201, 409):
            resp.raise_for_status()
        return resp.json() if resp.status_code == 201 else workload

    def submit_run(
        self, *, workload_name: str, workload_version: str, environment_name: str, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
        resp = httpx.post(
            f"{self._api_base_url()}/v1/runs",
            json={
                "workload_name": workload_name,
                "workload_version": workload_version,
                "environment_name": environment_name,
            },
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()

    def get_run(self, run_id: str) -> dict[str, Any]:
        resp = httpx.get(f"{self._api_base_url()}/v1/runs/{run_id}")
        resp.raise_for_status()
        return resp.json()

    def cancel_run(self, run_id: str) -> dict[str, Any]:
        resp = httpx.delete(f"{self._api_base_url()}/v1/runs/{run_id}")
        resp.raise_for_status()
        return resp.json()
