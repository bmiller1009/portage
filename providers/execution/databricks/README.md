# providers/execution/databricks

Translates a `ResolvedWorkload` into a Databricks Jobs API 2.2 one-time submission (`docs/architecture/spec.md` §16, ADR 0004). Python workloads map to a Python wheel task; JVM workloads map to a JAR task. Notebooks and the legacy `spark_submit_task` are explicitly avoided as canonical translation targets (§16, §73).

Initial conformance target: Databricks Runtime 19 (Spark 4.2 compatibility class); a Databricks Runtime 18 LTS / Spark 4.1 profile follows for customers prioritizing long support windows (§46).

`provider.py` implements `DatabricksExecutionProvider` — a translation-layer prototype only. No live Databricks workspace was available during Phase 0, so this is unit-tested against a hand-written fake `WorkspaceClient` (`tests/unit/test_databricks_provider.py`), verified against the real `databricks-sdk` package's actual method signatures and enum values (not guessed), but never exercised against a real workspace. The exact Jobs API `spark_version` cluster identifier for Databricks Runtime 19/18 LTS is a documented placeholder — see `_SPARK_TO_DBR_CLUSTER_VERSION` in `provider.py` — to be confirmed via `WorkspaceClient().clusters.spark_versions()` before any live submission.

`DatabricksProfile.runtime_profiles` (spec §18) maps a named runtime profile to a `node_type_id` override, applied in `build_run_submission()` when `workload.runtime.profile` is set and found in the environment's config — falls back to the profile's default `cluster_node_type_id` otherwise, not an error (only unit-tested here, same live-verification gap as the rest of this provider).
