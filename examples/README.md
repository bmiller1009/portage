# examples

Example portable workload definitions, used by tests, the CLI, and the Phase 0 exit demonstration.

- `wordcount.yaml` / `claims-normalization.yaml` — schema fixtures used by `tests/unit/test_workload_schema.py` and `plane workload validate`.
- `wordcount_app/` — a real PySpark package (not just a fixture) with a wheel-buildable `wordcount` module, used by `plane run` and the Phase 0 exit demonstration. See its own `pyproject.toml`/`Dockerfile`.
- `environments/` — `Environment` definitions (spec §8) as schema fixtures. As of v0.1 the CLI no longer reads these directly (`plane run --environment <name>` resolves the environment from the database via the REST API) — they remain useful as reference shapes for seeding `ExecutionProfile`/`StorageProfile`/`Environment` rows.
- `datasets/` — `Dataset` bindings (spec §9) fixtures, same status: `DatasetBinding` rows now live in the database, resolved by the reconciler, not read from these files at run time.
- `hello-pipeline.yaml` / `hello_pipeline/` — a minimal Spark Declarative Pipeline (spec §39, v0.6.5) schema fixture, used by `tests/unit/test_workload_schema.py` and `test_kubernetes_provider.py`. **Not runnable end-to-end yet**: `build_spark_application()`'s translation is confirmed live to correctly bootstrap `org.apache.spark.deploy.SparkPipelines`, but `spark-pipelines run` itself requires a Spark Connect session, and bootstrapping one in-process inside a plain `spark-submit` driver hits a classloader wall — see the confirmed-live finding in `providers/execution/kubernetes/provider.py`'s module docstring.
