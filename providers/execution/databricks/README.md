# providers/execution/databricks

Translates a `ResolvedWorkload` into a Databricks Jobs API 2.2 one-time submission (`docs/architecture/spec.md` §16, ADR 0004). Python workloads map to a Python wheel task; JVM workloads map to a JAR task. Notebooks and the legacy `spark_submit_task` are explicitly avoided as canonical translation targets (§16, §73).

Initial conformance target: Databricks Runtime 19 (Spark 4.2 compatibility class); a Databricks Runtime 18 LTS / Spark 4.1 profile follows for customers prioritizing long support windows (§46).

Not yet implemented — Phase 0 milestone (`Databricks execution provider prototype`).
