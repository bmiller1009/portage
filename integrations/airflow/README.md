# airflow-provider-portage

An Apache Airflow provider for Portage (docs/architecture/spec.md §69) --
submits and polls Portage runs from Airflow DAGs. Per spec §31/§39,
cross-system orchestrators call the public job API and never own workflow
semantics themselves, so this package is a thin, self-contained REST
client: it does not depend on the main `portage` backend package (`spec`,
`control_plane`, etc.) or on the still-empty `sdk/python/` -- it hand-rolls
its own `httpx` calls the same way `cli/main.py` does.

## Components

- **`PortageHook`** -- connection-config wrapper around the REST calls
  (`register_workload`, `submit_run`, `get_run`, `cancel_run`). Resolves
  its base URL from an Airflow Connection named `portage_default` (or
  whatever `portage_conn_id` is passed) if one is configured, otherwise
  falls back to `$PORTAGE_API_URL` -- so the provider works with zero
  Airflow-side setup beyond that one env var.
- **`PortageRunOperator`** -- registers a workload file, submits a run,
  and blocks (polling on `poll_interval_seconds`) until it reaches a
  terminal state, raising `AirflowException` on anything but `SUCCEEDED`
  or on timeout. Mirrors the sequence `plane run` already performs.
- **`PortageRunSensor`** -- poll-only, for DAGs where a separate task
  already submitted the run (e.g. via XCom) and just want Airflow's own
  sensor/deferred scheduling instead of the operator's blocking loop.

## Install

```
cd integrations/airflow
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

`apache-airflow` is a large, independent dependency tree, so this package
intentionally has its own `.venv` rather than sharing the main project's --
same reasoning as `ui/` having its own `node_modules`.

## Configuring the connection

Either set `PORTAGE_API_URL` (matches the CLI's own convention):

```
export PORTAGE_API_URL=http://localhost:8000
```

or configure a real Airflow Connection named `portage_default` with
`conn_type=portage`, `host`, `schema`, and `port` set to the API's
location.

## Tests

```
pytest tests/ -v
ruff check .
```

All unit tests monkeypatch `httpx`/`PortageHook` directly -- no real
network calls, no real Airflow metadata database required.

## Live-verifying a DAG locally

```
export AIRFLOW_HOME="$(pwd)/.airflow-home"
export PORTAGE_API_URL=http://127.0.0.1:8123
export PORTAGE_WORKLOAD_FILE=/path/to/portage/examples/wordcount.yaml
export PORTAGE_ENVIRONMENT_NAME=k8s-remote-v2
airflow standalone &   # or just: airflow db migrate
airflow dags test portage_wordcount_demo 2026-08-19
```

`dags/wordcount_dag.py` is the fixture used for this -- a single-task DAG
running `PortageRunOperator` against a real Portage environment.
