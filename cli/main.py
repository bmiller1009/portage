"""`plane` CLI (docs/architecture/spec.md §31). Most subcommands are stubs
until the REST API (v0.1 milestone) exists and the CLI can become a pure
API client per §31's own rule ("no direct database access, no hidden
Kubernetes access"). `plane run` is a deliberate, documented Phase 0
exception: it calls execution providers directly and synchronously,
because proving portability doesn't require the async reconciler (spec
§24) yet — see cli/environments.py for how environment names resolve to
providers."""

import asyncio
import time
import uuid

import typer
from pydantic import ValidationError

from cli.environments import (
    build_execution_provider,
    build_storage_config,
    load_datasets_for_workload,
    load_environment,
)
from control_plane.execution_provider import ResolvedWorkload, RunRequest
from control_plane.run_state import TERMINAL_STATES, RunState
from spec.dataset.v1alpha1 import resolve_dataset_config
from spec.workload.v1alpha1 import parse_workload

app = typer.Typer(name="plane", no_args_is_help=True)
workload_app = typer.Typer(no_args_is_help=True)
app.add_typer(workload_app, name="workload")


@workload_app.command("validate")
def workload_validate(file: str) -> None:
    """Validate a portable workload definition against the v1alpha1 schema."""
    try:
        workload = parse_workload(file)
    except ValidationError as e:
        typer.echo(f"FAIL: {file} is not a valid workload\n{e}")
        raise typer.Exit(code=1) from e
    typer.echo(f"PASS: {file} ({workload.metadata.name}, spark {workload.runtime.spark})")


@app.command()
def run(
    workload_file: str,
    environment: str = typer.Option(..., "--environment"),
    timeout_seconds: int = typer.Option(600, "--timeout"),
    poll_interval_seconds: float = typer.Option(5, "--poll-interval"),
) -> None:
    """Submit a workload run against the named environment and poll until it
    reaches a terminal state (spec §23)."""
    workload = parse_workload(workload_file)
    environment_def = load_environment(environment)
    datasets = load_datasets_for_workload(workload)

    resolved = ResolvedWorkload(
        workload=workload,
        dataset_config=resolve_dataset_config(workload, datasets, environment),
        storage_config=build_storage_config(environment_def),
        environment_name=environment,
    )
    provider = build_execution_provider(environment_def)
    request = RunRequest(run_id=uuid.uuid4().hex[:8], resolved=resolved)

    validation = asyncio.run(provider.validate(resolved))
    if not validation.valid:
        for error in validation.errors:
            typer.echo(f"VALIDATION FAILED: {error}")
        raise typer.Exit(code=1)

    provider_run = asyncio.run(provider.submit(request))
    typer.echo(f"submitted: provider_run_id={provider_run.provider_run_id}")

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        run_status = asyncio.run(provider.status(provider_run.provider_run_id))
        typer.echo(f"status: {run_status.state.value} ({run_status.provider_native_status})")
        if run_status.state in TERMINAL_STATES:
            if run_status.state == RunState.SUCCEEDED:
                typer.echo(f"SUCCEEDED: {provider_run.provider_run_id}")
                return
            typer.echo(f"FAILED: {provider_run.provider_run_id} ({run_status.state.value})")
            raise typer.Exit(code=1)
        time.sleep(poll_interval_seconds)

    typer.echo("TIMED OUT waiting for terminal state")
    raise typer.Exit(code=1)


@app.command()
def status(run_id: str) -> None:
    """Show run status. Not yet implemented — depends on the REST API (v0.1)."""
    typer.echo("not yet implemented — plane status depends on the v0.1 REST API")
    raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
