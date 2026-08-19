# Databricks execution provider — live-run results

Real workspace, real jobs, real output — live-verified across v0.3 (first real credentials) and v1.0.0's release-hardening pass (Databricks Runtime 19 / Spark 4.2 support), not a translation-layer prototype.

## What was actually running

| Component | Version/detail, confirmed live |
|---|---|
| Workspace | Real Databricks workspace (hostname withheld from the public verification record — adds no verification value), OAuth M2M (client ID/secret) authentication |
| Compute mode | Serverless (this workspace administratively forbids classic job clusters — "Only serverless compute is supported") |
| Databricks Runtime, Spark 4.0 | 17.3 LTS, cluster-version key `17.3.x-scala2.13` |
| Databricks Runtime, Spark 4.1 | 18.2, cluster-version key `18.2.x-scala2.13` |
| Databricks Runtime, Spark 4.2 | 19 (GA 2026-07-23), cluster-version key `19.x-scala2.13` |
| Storage | Unity Catalog Volumes (`/Volumes/workspace/default/portage_artifacts/...`) — not S3; the on-prem MinIO this project's Kubernetes leg uses isn't network-reachable from Databricks' cloud compute |

All three Spark<->Databricks Runtime mappings were confirmed by querying the real workspace's `WorkspaceClient().clusters.spark_versions()` directly (not read off release notes alone) — see `providers/execution/databricks/compatibility.py` for the curated table this produced.

## Findings from real live runs, not hypothetical

1. **Serverless-only workspace.** This workspace's own policy forbids `new_cluster` submissions outright. `DatabricksExecutionProvider` gained a `serverless` mode (`DatabricksProfile.serverless`) that submits via a job-level `environment_key`/`JobEnvironment` instead.
2. **The example wheel's own `pyspark==4.2.0` pip dependency broke Serverless's library installer** (no `--no-deps` equivalent available). Moved to an optional dev-only dependency in the example app — it was only ever needed for local unit testing, not for execution on either provider.
3. **`spark.conf.get()` is rejected outright for non-Spark-builtin keys under Serverless's Spark Connect session** (`[CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION]`), and the Jobs API's own `environment_variables_key` confirmed live *not* to actually reach a serverless Python-wheel task's process environment either. What works, confirmed end-to-end: `PythonWheelTask.parameters` as `--key=value` strings, read back via `sys.argv` — see `examples/wordcount_app`'s `_portable_config()` fallback.
4. **Databricks Runtime 19 / Spark 4.2 support (v1.0.0):** a workload declaring `spark: "4.2"` used to be rejected at `validate()` with "no Databricks Runtime ships Spark 4.2 yet" — that stopped being true once DBR 19 reached GA. Confirmed by re-running the real hero-demo workload (`examples/claims-normalization.yaml`, version-bumped, `spark: "4.2"`) end-to-end through the real API → reconciler → real workspace: `ACCEPTED → RUNNING → SUCCEEDED`, with freshly-written Parquet output confirmed in the target Unity Catalog volume.

## Result

```
$ plane run examples/wordcount-dbx.yaml --environment azure-dbx-v1
submitted: run_id=...
status: ACCEPTED
status: RUNNING
status: SUCCEEDED
SUCCEEDED: ...
```

Output confirmed correct in the target Unity Catalog volume, and semantically equivalent (Portage's formal portability guarantee, ADR 0011 — schema, row counts, null counts, partition coverage, deterministic hashes, never physical file identity) to the same workload's Kubernetes-side output for `examples/claims_app` — the actual "same workload, same artifact, no application changes" proof spec §66 describes. In this specific v1.0.0 run the output also happened to be byte-identical across both providers, an observed bonus, not something the contract itself requires.
