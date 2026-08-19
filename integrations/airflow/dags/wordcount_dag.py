"""Example/live-verification fixture for the Portage Airflow provider
(v0.6.4). Submits examples/wordcount.yaml against a real Portage
environment and blocks until it succeeds -- run with:

    PORTAGE_WORKLOAD_FILE=/path/to/portage/examples/wordcount.yaml \
    PORTAGE_ENVIRONMENT_NAME=k8s-remote-v2 \
    PORTAGE_API_URL=http://127.0.0.1:8123 \
    airflow dags test portage_wordcount_demo 2026-08-19
"""

import os
from datetime import UTC, datetime

from airflow.sdk import DAG
from airflow_provider_portage.operators.portage import PortageRunOperator

with DAG(
    dag_id="portage_wordcount_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
) as dag:
    submit_wordcount = PortageRunOperator(
        task_id="submit_wordcount",
        workload_file=os.environ.get("PORTAGE_WORKLOAD_FILE", "examples/wordcount.yaml"),
        environment_name=os.environ.get("PORTAGE_ENVIRONMENT_NAME", "k8s-remote-v2"),
        timeout_seconds=180,
    )
