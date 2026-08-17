# examples

Example portable workload definitions, used by tests, the CLI, and the Phase 0 exit demonstration.

- `wordcount.yaml` / `claims-normalization.yaml` — schema fixtures used by `tests/unit/test_workload_schema.py` and `plane workload validate`.
- `wordcount_app/` — a real PySpark package (not just a fixture) with a wheel-buildable `wordcount` module, used by `plane run` and the Phase 0 exit demonstration. See its own `pyproject.toml`/`Dockerfile`.
- `environments/` — `Environment` definitions (spec §8), resolved by name in `plane run --environment <name>` via `cli/environments.py`.
- `datasets/` — `Dataset` bindings (spec §9), resolved by dataset name (e.g. `wordcount.raw.yaml` binds the `wordcount.raw` dataset) via `cli/environments.py`.
