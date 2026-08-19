# Contributing to Portage

Thanks for considering a contribution. Portage is a small, actively-maintained open-source project — this document covers how to get a working dev environment, what's expected of a PR, and where the project's boundaries are.

## Development setup

Requires Docker, `kind`, `helm`, and `kubectl` on `PATH`. No cloud account, and no Databricks/AWS/Azure/VAST access, is needed for the basic development/test suite.

```bash
git clone https://github.com/bmiller1009/portage.git
cd portage
make dev       # stand up kind + PostgreSQL + MinIO + Spark Operator + the control plane, idempotently
```

Once `make dev` finishes:

```bash
PORTAGE_API_URL=http://127.0.0.1:8124 plane run examples/wordcount.yaml --environment local
```

should succeed. `make dev-down` tears down only what `make dev` created.

## Running tests and checks

```bash
make test      # pytest tests/unit -v
make lint      # ruff check . && pyright && cd ui && npm run lint && npm run build
```

`tests/unit` and `tests/integration/db` (against a real PostgreSQL) are what CI runs on every PR — see [`tests/README.md`](tests/README.md) for the full suite breakdown, including which suites need live infrastructure this project's CI doesn't have and are run manually/on a schedule instead.

Before opening a PR:

```bash
ruff check .
pyright
pytest tests/unit -v
```

should all be clean. If your change touches the FastAPI app's routes/schemas, also run `python scripts/export_openapi.py` and commit the resulting `openapi.json` diff — CI fails on undocumented drift.

## PR conventions

- Keep PRs scoped to one change. A bug fix doesn't need surrounding cleanup; a new feature doesn't need an unrelated refactor riding along.
- Write commit messages and PR descriptions that explain *why*, not just *what* — the diff already shows what changed.
- If your change is architecturally significant, check [`docs/adr/`](docs/adr/) first — an ADR may already cover the relevant decision, or your change may need one.
- Live-infrastructure claims (a provider now works against real Kubernetes/Databricks/etc.) should be backed by an actual run, not assumed from reading code — this project's own discipline throughout its history has been "confirm live, never assume."

## Project boundaries

Portage is a portable workload runtime/control plane, not a lakehouse platform — see the README's Non-goals section before proposing anything that would turn it into one. Adding a new execution or storage provider is welcome, but requires implementing the stable `ExecutionProvider`/`StorageProvider` Protocol and passing the shared contract-test suite — see [`docs/architecture/PROVIDER_SDK.md`](docs/architecture/PROVIDER_SDK.md).

## Reporting bugs and requesting features

Open a GitHub issue. For security vulnerabilities, see [`SECURITY.md`](SECURITY.md) instead — please don't file those as public issues.
