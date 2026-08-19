# providers/execution/databricks

Translates a `ResolvedWorkload` into a Databricks Jobs API 2.2 one-time submission (`docs/architecture/spec.md` §16, ADR 0004). Python workloads map to a Python wheel task; JVM workloads map to a JAR task. Notebooks and the legacy `spark_submit_task` are explicitly avoided as canonical translation targets (§16, §73).

`provider.py` implements `DatabricksExecutionProvider`. **Live-verified, not a translation-layer prototype** — real OAuth M2M-authenticated runs against a real workspace since v0.3, including both classic and Serverless compute, Python wheel and JVM JAR artifacts, and (as of v1.0.0's Databricks Runtime 19 support) Spark 4.2. `tests/unit/test_databricks_provider.py` still unit-tests the translation logic itself against a hand-written fake `WorkspaceClient` (fast, no network) — the live-verification claim above is about `submit()`/`status()`/`cancel()` having actually run against a real workspace at various points across this project's history, not about the unit test suite's own fixtures.

The Spark <-> Databricks Runtime compatibility table lives in `compatibility.py`, along with an explicit-override escape hatch (`DatabricksProfile.dbr_cluster_version_overrides`) for workspaces running a newer Databricks Runtime than the curated table currently knows about, and a bounded, best-effort live cross-check against a real workspace's `w.clusters.spark_versions()`.

`DatabricksProfile.runtime_profiles` (spec §18) maps a named runtime profile to a `node_type_id` override, applied in `build_run_submission()` when `workload.runtime.profile` is set and found in the environment's config — falls back to the profile's default `cluster_node_type_id` otherwise, not an error.
