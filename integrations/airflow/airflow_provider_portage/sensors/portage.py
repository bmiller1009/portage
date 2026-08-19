"""PortageRunSensor -- polls an already-submitted run (e.g. one another
task submitted and passed via XCom) without owning submission itself,
for DAGs that want Airflow's own deferred/sensor scheduling rather than
PortageRunOperator's blocking poll loop."""

from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseSensorOperator

from airflow_provider_portage.hooks.portage import PortageHook

_FAILURE_STATES = {"FAILED", "CANCELED", "LOST"}
_SUCCESS_STATE = "SUCCEEDED"


class PortageRunSensor(BaseSensorOperator):
    template_fields = ("run_id",)

    def __init__(self, *, run_id: str, portage_conn_id: str = PortageHook.default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.run_id = run_id
        self.portage_conn_id = portage_conn_id

    def poke(self, context) -> bool:
        hook = PortageHook(self.portage_conn_id)
        run = hook.get_run(self.run_id)
        state = run["state"]
        if state in _FAILURE_STATES:
            raise AirflowException(f"portage run {self.run_id} ended in {state}: {run}")
        return state == _SUCCESS_STATE
