"""PortageRunOperator -- register a workload, submit a run, and poll it
to a terminal state, the same sequence cli/main.py's `plane run` already
does (spec §31: the CLI/every integration is a pure REST client, so this
operator is that same sequence, not a reimplementation of run
submission logic)."""

from __future__ import annotations

import time
from typing import Any

import yaml
from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from airflow_provider_portage.hooks.portage import PortageHook

# Mirrors control_plane/run_state.py's RunState/TERMINAL_STATES -- not
# imported directly, so this package stays installable without the main
# portage backend package (spec §31's "pure REST client" boundary
# applied to what this package depends on, not just how it talks to the
# API).
_TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "LOST"}
_SUCCESS_STATE = "SUCCEEDED"


class PortageRunOperator(BaseOperator):
    """Submits examples/*.yaml-shaped portable workload definitions to
    Portage and blocks until the run reaches a terminal state, raising
    on anything but SUCCEEDED."""

    template_fields = ("workload_file", "environment_name")

    def __init__(
        self,
        *,
        workload_file: str,
        environment_name: str,
        portage_conn_id: str = PortageHook.default_conn_name,
        poll_interval_seconds: float = 5,
        timeout_seconds: float = 600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.workload_file = workload_file
        self.environment_name = environment_name
        self.portage_conn_id = portage_conn_id
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds

    def execute(self, context) -> dict[str, Any]:
        hook = PortageHook(self.portage_conn_id)

        with open(self.workload_file) as f:
            workload = yaml.safe_load(f)
        workload_name = workload["metadata"]["name"]
        workload_version = workload["metadata"]["version"]

        hook.register_workload(workload)
        run = hook.submit_run(
            workload_name=workload_name,
            workload_version=workload_version,
            environment_name=self.environment_name,
            idempotency_key=f"{context['dag'].dag_id}-{context['run_id']}-{context['task'].task_id}",
        )
        run_id = run["id"]
        self.log.info("submitted portage run %s (%s/%s on %s)", run_id, workload_name, workload_version, self.environment_name)

        deadline = time.monotonic() + self.timeout_seconds
        while True:
            run = hook.get_run(run_id)
            state = run["state"]
            if state in _TERMINAL_STATES:
                if state != _SUCCESS_STATE:
                    raise AirflowException(f"portage run {run_id} ended in {state}: {run}")
                self.log.info("portage run %s succeeded", run_id)
                return run
            if time.monotonic() > deadline:
                raise AirflowException(f"timed out waiting for portage run {run_id} (last state: {state})")
            time.sleep(self.poll_interval_seconds)
