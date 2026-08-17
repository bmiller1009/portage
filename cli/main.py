"""`plane` CLI (docs/architecture/spec.md §31). Talks to the public REST API
only — no direct database or Kubernetes access. Most subcommands are stubs
until the REST API (v0.1 milestone) exists; `workload validate` is real
today since it only needs the local schema."""

import typer
from pydantic import ValidationError

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
def run(workload: str, environment: str = typer.Option(..., "--environment")) -> None:
    """Submit a workload run. Not yet implemented — depends on the REST API (v0.1)."""
    typer.echo("not yet implemented — plane run depends on the v0.1 REST API")
    raise typer.Exit(code=1)


@app.command()
def status(run_id: str) -> None:
    """Show run status. Not yet implemented — depends on the REST API (v0.1)."""
    typer.echo("not yet implemented — plane status depends on the v0.1 REST API")
    raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
