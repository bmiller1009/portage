# Query plane — Iceberg REST Catalog, Trino, Kyuubi (v0.5)

## Portage does not own the query plane

Per spec §38: *"A federated SQL/query plane is useful but not part of core v0.1... Do not write a custom SQL engine."* v0.5 stands up Trino and (optionally, per spec §68) Kyuubi behind the same portability boundary as every other composed system (Kubernetes, Databricks, S3, VAST, ADLS) — Portage deploys and configures them, and resolves logical table names into fully-qualified identifiers through its own dataset bindings, but it never proxies a query or owns the SQL engine itself. Any JDBC-speaking client — a BI tool, a notebook, `trino`/`kyuubi-beeline` CLIs — connects to Trino or Kyuubi *directly*, using the connection strings below, not through the Portage API.

```
JDBC/SQL → Trino / Kyuubi → Iceberg REST Catalog → S3-compatible storage (MinIO/VAST/S3)
                                     ↑
                    Portage's own Spark jobs also read/write here,
                    via table dataset bindings (spec §11)
```

## Table dataset bindings (spec §11)

A `Dataset` binding can now be `kind: table` instead of `kind: path`:

```yaml
metadata:
  name: claims.normalized
kind: table
bindings:
  onprem-prod:
    identifier: analytics.claims.normalized
```

`spec/dataset/v1alpha1.py::resolve_dataset_config()` turns this into `spark.portable.dataset.<name>.identifier=portage_iceberg.<identifier>` (a workload reads it via `spark.conf.get(...)`, same contract as the existing `.uri` key for path bindings) plus, once per run, `resolve_iceberg_catalog_config()` registers the environment's Iceberg REST catalog with Spark (`spark.sql.catalog.portage_iceberg.*`) — sourced from two new `StorageProfile.config` keys, `iceberg_catalog_uri` and `iceberg_warehouse`. Both functions are no-ops for path-only workloads, so nothing about v0.1–v0.4 behavior changes.

The same catalog is what Trino's `iceberg.properties` and Kyuubi's `kyuubi-defaults.conf` point at (see below) — a table Portage's own Spark jobs write is immediately visible to both, and vice versa. Live-verified: a table written by a Portage-submitted Spark job, read back by a second, independent Portage-submitted Spark job purely through the logical dataset name, then queried by both Trino and Kyuubi — four independent readers, one consistent table.

## Connecting directly

| Service | Connection string | CLI |
|---|---|---|
| Trino | `jdbc:trino://<host>:8080/iceberg/<schema>` | `trino --server <host>:8080 --catalog iceberg --schema <schema>` |
| Kyuubi | `jdbc:kyuubi://<host>:10009/` | `kyuubi-beeline -u 'jdbc:kyuubi://<host>:10009/' -n <user>` |

Both see the same tables under the `iceberg` (Trino) / `portage_iceberg` (Kyuubi, Spark SQL catalog naming) catalog — same REST catalog, same warehouse, different client-side catalog name because Trino's and Spark's own catalog-registration conventions are independent of each other and of Iceberg's.

## Real gotchas found via live verification, not assumed

None of this worked on the first attempt — each of these was a genuine defect or a genuinely undocumented-until-tried compatibility gap, found by actually running it, per this project's own discipline (spec §67) of never claiming something works without proving it live.

- **The Iceberg REST Catalog fixture's `CATALOG_*` env var translation is the opposite of what its own naming suggests.** It's prefix-strip, then `__` (double underscore) → `-` (hyphen), `_` (single underscore) → `.` (dot) — so `CATALOG_S3_ACCESS__KEY__ID` → `s3.access-key-id`. Getting this backwards (as an early version of `deploy/dev/iceberg-rest.yaml` did) doesn't error — it silently produces an unrecognized property name, so credentials were configured but never actually applied.
- **The REST catalog fixture's own S3 client needs an explicit `io-impl` override.** Without `CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO`, the server tries to load Hadoop's `S3AFileSystem`, which its classpath doesn't include, and returns a 500.
- **AWS SDK v2 always resolves a region, even against a non-AWS endpoint.** Both the REST catalog server and any Spark client touching Iceberg's native S3 client need `AWS_REGION`/`spark.sql.catalog.<name>.client.region` set to *something* syntactically valid — the value itself is never used to pick an endpoint, since the endpoint is configured separately.
- **The warehouse URI scheme has to be `s3a://`, not `s3://`** — Spark's `hadoop-aws` classpath only registers a `FileSystem` for `s3a`, so an `s3://`-prefixed warehouse throws `UnsupportedFileSystemException` the moment a job writes through `org.apache.iceberg.hadoop.HadoopFileIO`.
- **`org.apache.iceberg:iceberg-spark-runtime-4.0` (the newest published Iceberg-Spark runtime as of this writing — no dedicated 4.2 build exists yet) is genuinely binary-incompatible with Spark 4.2.0**, throwing `java.lang.IncompatibleClassChangeError` against `org.apache.spark.sql.connector.catalog.View`. `examples/iceberg_demo_app` and the Kyuubi image are both built on `apache/spark:4.1.3-python3` instead — Spark 4.1 is already a supported version on the Kubernetes execution provider, so this is a legitimate compatible target, not a workaround pin.
- **`iceberg-aws-bundle` is needed on the Spark *client* too, not just the catalog server** — even a read-only query that never touches Hadoop's S3A path still probes `Class.forName("org.apache.iceberg.aws.s3.S3FileIO")` internally (`ResolvingFileIO.ioClass()`, used for locality hints), and without the jar on the classpath that throws a hard `NoClassDefFoundError` rather than a caught exception.
- **The Kubernetes Spark Operator mounts its own generated ConfigMap over `/opt/spark/conf` at runtime**, silently shadowing anything an application image bakes in at that path — so `spark.sql.extensions` and the Iceberg catalog registration have to flow as real Spark config (the `SparkApplication` CRD's `spec.sparkConf`, via `ResolvedWorkload.dataset_config`), not a static `spark-defaults.conf`. Kyuubi doesn't have this problem — it manages its own pod directly, no Operator involved, so its `kyuubi-defaults.conf` (baked into the image, overridable via a mounted ConfigMap) works as documented.

## Deployment

`deploy/dev/iceberg-rest.yaml`, `deploy/dev/trino.yaml`, `deploy/dev/kyuubi.yaml` — all idempotent `kubectl apply` + `rollout status` steps in `scripts/dev-up.sh`, same pattern as MinIO's. `providers/query/kyuubi/image/` holds the custom Kyuubi image (no official prebuilt Kyuubi+Spark image exists), built and `kind load`-ed the same way `portage/wordcount:0.1.0` already is.
