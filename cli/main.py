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
conformance_app = typer.Typer(no_args_is_help=True)
app.add_typer(conformance_app, name="conformance")


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
        typer.echo(
            f"{binding['dataset_name']}\t{binding['environment_name']}\t{binding['kind']}\t{binding['uri']}"
        )


@workload_app.command("validate")
def workload_validate(
    file: str, environment: list[str] = typer.Option([], "--environment")
) -> None:
    """Validate a portable workload definition against the v1alpha1 schema
    (local, offline — no network call). With one or more --environment
    flags, additionally checks it against each named environment's
    provider capabilities (spec §20-21) via POST /v1/validate — the one
    REST call this otherwise-local command makes, since capability
    matching needs each environment's registered provider, which only the
    control plane knows about. Passing --environment more than once is a
    static, offline-safe way to check portability across providers before
    ever attempting a live run (spec §66's "same workload, no application
    changes" claim) — exits 1 if the workload fails against any of them."""
    try:
        workload = parse_workload(file)
    except ValidationError as e:
        typer.echo(f"FAIL: {file} is not a valid workload\n{e}")
        raise typer.Exit(code=1) from e
    typer.echo(f"PASS: {file} ({workload.metadata.name}, spark {workload.runtime.spark})")

    any_failed = False
    for env_name in environment:
        resp = httpx.post(
            f"{_api_base_url()}/v1/validate",
            json={"workload": workload.model_dump(mode="json"), "environment_name": env_name},
        )
        if resp.status_code == 422:
            typer.echo(f"FAIL: {env_name}: {resp.json()['detail']}")
            any_failed = True
            continue
        resp.raise_for_status()
        result = resp.json()
        if not result["valid"]:
            any_failed = True
            for error in result["errors"]:
                typer.echo(f"CAPABILITY MISMATCH: {env_name}: {error}")
            continue
        typer.echo(f"PASS: compatible with environment '{env_name}'")

    if any_failed:
        raise typer.Exit(code=1)


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


@conformance_app.command("test")
def conformance_test(
    workload_file: str,
    environment: list[str] = typer.Option(..., "--environment"),
    output: str | None = typer.Option(None, "--output"),
    timeout_seconds: int = typer.Option(600, "--timeout"),
    poll_interval_seconds: float = typer.Option(5, "--poll-interval"),
) -> None:
    """Dynamic conformance test (spec §21): submits the SAME workload to
    every named environment, polls each to a terminal state, then
    compares their outputs semantically (spec §22) via
    POST /v1/conformance/compare — the actual comparison happens
    server-side, since it needs storage credentials this CLI never
    touches directly (spec §31). Reports PASS/FAIL/BLOCKED per
    environment pair; exits 1 unless every pair PASSes."""
    workload = parse_workload(workload_file)
    base_url = _api_base_url()

    register_resp = httpx.post(f"{base_url}/v1/workloads", json=workload.model_dump(mode="json"))
    if register_resp.status_code not in (201, 409):
        register_resp.raise_for_status()

    run_ids: dict[str, str] = {}
    for env_name in environment:
        create_resp = httpx.post(
            f"{base_url}/v1/runs",
            json={
                "workload_name": workload.metadata.name,
                "workload_version": workload.metadata.version,
                "environment_name": env_name,
            },
            headers={"Idempotency-Key": uuid.uuid4().hex},
        )
        create_resp.raise_for_status()
        run_ids[env_name] = create_resp.json()["id"]
        typer.echo(f"{env_name}: submitted run_id={run_ids[env_name]}")

    deadline = time.monotonic() + timeout_seconds
    pending = dict(run_ids)
    while pending and time.monotonic() < deadline:
        for env_name, run_id in list(pending.items()):
            status_resp = httpx.get(f"{base_url}/v1/runs/{run_id}")
            status_resp.raise_for_status()
            run_state = status_resp.json()["state"]
            if RunState(run_state) in TERMINAL_STATES:
                typer.echo(f"{env_name}: {run_state}")
                del pending[env_name]
        if pending:
            time.sleep(poll_interval_seconds)
    for env_name in pending:
        typer.echo(f"{env_name}: TIMED OUT waiting for terminal state")

    compare_resp = httpx.post(
        f"{base_url}/v1/conformance/compare",
        json={"run_ids": list(run_ids.values()), "output_name": output},
    )
    compare_resp.raise_for_status()
    report = compare_resp.json()

    any_failed = False
    for pair in report["results"]:
        typer.echo(f"{pair['left_environment']} <-> {pair['right_environment']}: {pair['status']}")
        if pair["status"] != "PASS":
            any_failed = True
            for mismatch in pair["mismatches"]:
                typer.echo(f"  {mismatch}")

    if any_failed:
        raise typer.Exit(code=1)


@conformance_app.command("report")
def conformance_report() -> None:
    """Provider certification report (spec §78) — every registered
    execution+storage combination, PASS/FAIL/BLOCKED, in the format
    spec §78 itself shows. BLOCKED rows are an honest "never live-
    verified" (e.g. Databricks, ADLS, VAST NFS — see issues #8/#27/#28),
    not a fabricated PASS. Exits 1 if any row is FAIL (a real, current
    defect); BLOCKED rows don't affect the exit code."""
    base_url = _api_base_url()
    resp = httpx.get(f"{base_url}/v1/conformance/report")
    resp.raise_for_status()
    report = resp.json()

    typer.echo("Portable Runtime Conformance")
    typer.echo()
    any_failed = False
    for row in report["rows"]:
        label = f"{row['execution_provider']} + {row['storage_protocol']}"
        typer.echo(f"{label:<40}{row['status']}")
        if row["status"] == "FAIL":
            any_failed = True
            if row["detail"]:
                typer.echo(f"  {row['detail']}")

    if any_failed:
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
