"""`plane` CLI (docs/architecture/spec.md §31). A pure REST client of the
control-plane API — no direct database access, no direct provider access
(§31). `run`/`status`/`cancel`/`logs` all talk to /v1/runs; the async
submit/poll/cancel work itself happens in reconciler/service.py, not here.
"""

import os
import time
import uuid

import httpx
import typer
from pydantic import ValidationError

from control_plane.run_state import TERMINAL_STATES, RunState
from spec.workload.v1alpha1 import parse_workload

app = typer.Typer(name="plane", no_args_is_help=True)
workload_app = typer.Typer(no_args_is_help=True)
app.add_typer(workload_app, name="workload")
environment_app = typer.Typer(no_args_is_help=True)
app.add_typer(environment_app, name="environment")
dataset_app = typer.Typer(no_args_is_help=True)
app.add_typer(dataset_app, name="dataset")


def _api_base_url() -> str:
    return os.environ.get("PORTAGE_API_URL", "http://localhost:8000")


@environment_app.command("list")
def environment_list() -> None:
    """List environments registered with the control plane (REST client —
    spec §31: the CLI talks to the API, never the database directly)."""
    resp = httpx.get(f"{_api_base_url()}/v1/environments")
    resp.raise_for_status()
    for env in resp.json():
        typer.echo(
            f"{env['name']}\t{env['execution_provider']}/{env['execution_profile_name']}"
            f"\t{env['storage_provider']}/{env['storage_profile_name']}"
        )


@dataset_app.command("list")
def dataset_list(dataset_name: str = typer.Option(None, "--dataset")) -> None:
    """List dataset bindings registered with the control plane."""
    params = {"dataset_name": dataset_name} if dataset_name else {}
    resp = httpx.get(f"{_api_base_url()}/v1/datasets", params=params)
    resp.raise_for_status()
    for binding in resp.json():
        typer.echo(f"{binding['dataset_name']}\t{binding['environment_name']}\t{binding['uri']}")


@workload_app.command("validate")
def workload_validate(
    file: str, environment: str | None = typer.Option(None, "--environment")
) -> None:
    """Validate a portable workload definition against the v1alpha1 schema
    (local, offline — no network call). With --environment, additionally
    checks it against that environment's provider capabilities (spec §20-21)
    via POST /v1/validate — the one REST call this otherwise-local command
    makes, since capability matching needs the environment's registered
    provider, which only the control plane knows about."""
    try:
        workload = parse_workload(file)
    except ValidationError as e:
        typer.echo(f"FAIL: {file} is not a valid workload\n{e}")
        raise typer.Exit(code=1) from e
    typer.echo(f"PASS: {file} ({workload.metadata.name}, spark {workload.runtime.spark})")

    if environment is None:
        return

    resp = httpx.post(
        f"{_api_base_url()}/v1/validate",
        json={"workload": workload.model_dump(mode="json"), "environment_name": environment},
    )
    if resp.status_code == 422:
        typer.echo(f"FAIL: {resp.json()['detail']}")
        raise typer.Exit(code=1)
    resp.raise_for_status()
    result = resp.json()
    if not result["valid"]:
        for error in result["errors"]:
            typer.echo(f"CAPABILITY MISMATCH: {error}")
        raise typer.Exit(code=1)
    typer.echo(f"PASS: compatible with environment '{environment}'")


@app.command()
def run(
    workload_file: str,
    environment: str = typer.Option(..., "--environment"),
    timeout_seconds: int = typer.Option(600, "--timeout"),
    poll_interval_seconds: float = typer.Option(5, "--poll-interval"),
) -> None:
    """Submit a workload run against the named environment (spec §24) and
    poll until it reaches a terminal state (spec §23)."""
    workload = parse_workload(workload_file)
    base_url = _api_base_url()

    # Registering is idempotent from the CLI's point of view — a 409 just
    # means someone (possibly this same command, last time) already did it.
    register_resp = httpx.post(f"{base_url}/v1/workloads", json=workload.model_dump(mode="json"))
    if register_resp.status_code not in (201, 409):
        register_resp.raise_for_status()

    create_resp = httpx.post(
        f"{base_url}/v1/runs",
        json={
            "workload_name": workload.metadata.name,
            "workload_version": workload.metadata.version,
            "environment_name": environment,
        },
        headers={"Idempotency-Key": uuid.uuid4().hex},
    )
    create_resp.raise_for_status()
    run_id = create_resp.json()["id"]
    typer.echo(f"submitted: run_id={run_id}")

    deadline = time.monotonic() + timeout_seconds
    last_state = None
    while time.monotonic() < deadline:
        status_resp = httpx.get(f"{base_url}/v1/runs/{run_id}")
        status_resp.raise_for_status()
        run_state = status_resp.json()["state"]
        if run_state != last_state:
            typer.echo(f"status: {run_state}")
            last_state = run_state
        if RunState(run_state) in TERMINAL_STATES:
            if run_state == RunState.SUCCEEDED.value:
                typer.echo(f"SUCCEEDED: {run_id}")
                return
            typer.echo(f"FAILED: {run_id} ({run_state})")
            raise typer.Exit(code=1)
        time.sleep(poll_interval_seconds)

    typer.echo("TIMED OUT waiting for terminal state")
    raise typer.Exit(code=1)


@app.command()
def status(run_id: str) -> None:
    """Show current run status and its event history."""
    base_url = _api_base_url()
    resp = httpx.get(f"{base_url}/v1/runs/{run_id}")
    if resp.status_code == 404:
        typer.echo(f"run not found: {run_id}")
        raise typer.Exit(code=1)
    resp.raise_for_status()
    run = resp.json()
    typer.echo(
        f"{run['id']}\t{run['state']}\t{run['workload_name']}@{run['workload_version']}"
        f"\t{run['environment_name']}"
    )

    events_resp = httpx.get(f"{base_url}/v1/runs/{run_id}/events")
    events_resp.raise_for_status()
    for event in events_resp.json():
        typer.echo(f"  {event['from_state']} -> {event['to_state']}: {event['message'] or ''}")


@app.command()
def cancel(run_id: str) -> None:
    """Request cancellation of a run (spec §23 — CANCELING/CANCELED)."""
    base_url = _api_base_url()
    resp = httpx.delete(f"{base_url}/v1/runs/{run_id}")
    if resp.status_code == 404:
        typer.echo(f"run not found: {run_id}")
        raise typer.Exit(code=1)
    if resp.status_code == 409:
        typer.echo(f"cannot cancel: {resp.json()['detail']}")
        raise typer.Exit(code=1)
    resp.raise_for_status()
    run = resp.json()
    typer.echo(f"{run['state']}: {run_id}")


@app.command()
def logs(run_id: str) -> None:
    """Show a reference to the run's provider-side logs (not fetched log
    content — a kubectl command or a Databricks run URL, depending on the
    provider)."""
    base_url = _api_base_url()
    resp = httpx.get(f"{base_url}/v1/runs/{run_id}/logs")
    if resp.status_code == 404:
        typer.echo(f"run not found: {run_id}")
        raise typer.Exit(code=1)
    if resp.status_code == 409:
        typer.echo(resp.json()["detail"])
        raise typer.Exit(code=1)
    resp.raise_for_status()
    ref = resp.json()
    typer.echo(f"{ref['description']}: {ref['uri']}")


if __name__ == "__main__":
    app()
