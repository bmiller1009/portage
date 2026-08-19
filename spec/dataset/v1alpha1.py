"""Dataset binding schema (docs/architecture/spec.md §9/§11). Stable as of
v1.0 (docs/architecture/STABILITY.md) — apiVersion "runtime/v1"; the
"runtime/v1alpha1" name still parses but is deprecated.

Path bindings point at a plain URI; table bindings (v0.5) point at a
fully-qualified identifier resolved through the Iceberg REST catalog
(docs/architecture/spec.md §11 — "Iceberg is the preferred neutral table
format... It should not require Iceberg for every workload")."""

from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from spec.stability import warn_if_deprecated
from spec.workload.v1alpha1 import SparkWorkload


class DatasetMetadata(BaseModel):
    name: str


class PathBinding(BaseModel):
    kind: Literal["path"] = "path"
    uri: str


class TableBinding(BaseModel):
    kind: Literal["table"] = "table"
    identifier: str


Binding = Annotated[PathBinding | TableBinding, Field(discriminator="kind")]

# The Iceberg catalog Portage registers with Spark for every table binding
# — one fixed name is enough since a single environment has exactly one
# storage profile, and therefore exactly one Iceberg REST catalog.
ICEBERG_CATALOG_NAME = "portage_iceberg"


class Dataset(BaseModel):
    apiVersion: Literal["runtime/v1", "runtime/v1alpha1"]
    kind: Literal["Dataset"]
    metadata: DatasetMetadata
    bindings: dict[str, Binding]

    @model_validator(mode="after")
    def _warn_deprecated_api_version(self) -> "Dataset":
        warn_if_deprecated(self.apiVersion, "Dataset")
        return self


def parse_dataset(path: str | Path) -> Dataset:
    """Load and validate a Dataset binding definition from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Dataset.model_validate(raw)


class DatasetResolutionError(Exception):
    """Raised when a workload references a dataset with no binding for the
    target environment (spec §9's resolver) rather than data movement,
    per §12 — dataset mapping is not data replication."""


def resolve_dataset_config(
    workload: SparkWorkload,
    datasets: dict[str, Dataset],
    environment: str,
) -> dict[str, str]:
    """Produce the spark.portable.dataset.<name>.{uri,identifier} Spark
    configuration entries for every input/output dataset a workload
    references, per ADR 0006. A path binding resolves to a .uri key; a
    table binding (§11) resolves to a .identifier key naming the table
    fully-qualified under the environment's Iceberg REST catalog — see
    resolve_iceberg_catalog_config() for the catalog-level config the
    latter also requires.

    The key is prefixed with "spark." — confirmed necessary by running this
    live during Phase 0: Spark 4.2's RuntimeConfig silently drops any
    --conf key that isn't spark.*-namespaced ("Ignoring non-Spark config
    property"), so a bare "portable.dataset.*.uri" key is never visible to
    spark.conf.get() at all, even though it's accepted into sparkConf by
    the Kubernetes Operator and Databricks Jobs API without complaint.
    """
    config: dict[str, str] = {}
    refs = list(workload.datasets.inputs.values()) + list(workload.datasets.outputs.values())
    for ref in refs:
        dataset = datasets.get(ref.dataset)
        if dataset is None:
            raise DatasetResolutionError(f"no Dataset definition found for '{ref.dataset}'")
        binding = dataset.bindings.get(environment)
        if binding is None:
            raise DatasetResolutionError(
                f"dataset '{ref.dataset}' has no binding for environment '{environment}'"
            )
        if isinstance(binding, TableBinding):
            config[f"spark.portable.dataset.{ref.dataset}.identifier"] = (
                f"{ICEBERG_CATALOG_NAME}.{binding.identifier}"
            )
        else:
            config[f"spark.portable.dataset.{ref.dataset}.uri"] = binding.uri
    return config


def resolve_iceberg_catalog_config(
    workload: SparkWorkload,
    datasets: dict[str, Dataset],
    environment: str,
    *,
    iceberg_catalog_uri: str | None,
    iceberg_warehouse: str | None,
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
    s3_path_style_access: str | None = None,
) -> dict[str, str]:
    """Produce the spark.sql.catalog.<name>.* config registering the
    environment's Iceberg REST catalog with Spark, plus the Iceberg SQL
    extension every table-binding operation needs — needed once per run
    (not once per dataset) whenever any referenced dataset resolves to a
    table binding. Returns {} when no table binding is present, so calling
    this unconditionally is safe for path-only workloads (the common case
    through v0.4).

    Deliberately returned as Spark *config* (flows through
    ResolvedWorkload.dataset_config -> the execution provider's sparkConf,
    e.g. the SparkApplication CRD's spec.sparkConf) rather than a static
    spark-defaults.conf baked into the application image: the Kubernetes
    Spark Operator mounts its own generated ConfigMap over
    /opt/spark/conf, silently shadowing anything an image bakes in at
    that path — confirmed live in examples/iceberg_demo_app's first
    verification run, which failed for exactly this reason.

    The s3_* keywords are optional because they're only needed when a
    Spark job actually exercises Iceberg's *native* S3 client
    (org.apache.iceberg.aws.s3.S3FileIO) rather than the Hadoop S3A path
    the storage provider's own spark.hadoop.fs.s3a.* config already
    covers — confirmed live that reading a table (unlike writing one)
    does exercise it, so both write- and read-only callers should pass
    these when available (the caller's already-resolved S3 connection
    details — see reconciler/service.py's _resolve_dataset_config, which
    reuses provider_factory.build_storage_config()'s output rather than
    re-resolving credentials itself).
    """
    refs = list(workload.datasets.inputs.values()) + list(workload.datasets.outputs.values())
    has_table_binding = any(
        isinstance(datasets[ref.dataset].bindings.get(environment), TableBinding)
        for ref in refs
        if ref.dataset in datasets
    )
    if not has_table_binding:
        return {}
    if not iceberg_catalog_uri or not iceberg_warehouse:
        raise DatasetResolutionError(
            "a table dataset binding is present but the environment's storage profile has no "
            "iceberg_catalog_uri/iceberg_warehouse configured"
        )
    config = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type": "rest",
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.uri": iceberg_catalog_uri,
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse": iceberg_warehouse,
        # AWS SDK v2's S3 client builder unconditionally resolves a
        # region even against a non-AWS S3-compatible endpoint (MinIO,
        # VAST) — confirmed live: without this, a read that touches
        # Iceberg's native S3 client fails with SdkClientException
        # "Unable to load region from any of the providers in the
        # chain". The region value itself is never actually used to pick
        # an endpoint (the endpoint is set separately below), so any
        # syntactically valid AWS region name is fine here.
        f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.client.region": "us-east-1",
    }
    if s3_endpoint:
        config[f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.s3.endpoint"] = s3_endpoint
    if s3_access_key:
        config[f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.s3.access-key-id"] = s3_access_key
    if s3_secret_key:
        config[f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.s3.secret-access-key"] = s3_secret_key
    if s3_path_style_access:
        config[f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.s3.path-style-access"] = s3_path_style_access
    return config
