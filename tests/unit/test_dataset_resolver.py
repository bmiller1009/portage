import pytest

from spec.dataset.v1alpha1 import (
    Dataset,
    DatasetResolutionError,
    PathBinding,
    TableBinding,
    resolve_dataset_config,
    resolve_iceberg_catalog_config,
)
from spec.workload.v1alpha1 import parse_workload

EXAMPLES_DIR = __import__("pathlib").Path(__file__).resolve().parents[2] / "examples"


def _dataset(name: str, **bindings: str) -> Dataset:
    return Dataset.model_validate(
        {
            "apiVersion": "runtime/v1alpha1",
            "kind": "Dataset",
            "metadata": {"name": name},
            "bindings": {env: {"kind": "path", "uri": uri} for env, uri in bindings.items()},
        }
    )


def _table_dataset(name: str, environment: str, identifier: str) -> Dataset:
    return Dataset.model_validate(
        {
            "apiVersion": "runtime/v1alpha1",
            "kind": "Dataset",
            "metadata": {"name": name},
            "bindings": {environment: {"kind": "table", "identifier": identifier}},
        }
    )


def test_resolves_portable_dataset_config_for_environment():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _dataset("wordcount.raw", k8s_remote="s3a://portage-phase0/wordcount/input.txt"),
        "wordcount.counts": _dataset("wordcount.counts", k8s_remote="s3a://portage-phase0/wordcount/output"),
    }

    config = resolve_dataset_config(workload, datasets, "k8s_remote")

    assert config == {
        "spark.portable.dataset.wordcount.raw.uri": "s3a://portage-phase0/wordcount/input.txt",
        "spark.portable.dataset.wordcount.counts.uri": "s3a://portage-phase0/wordcount/output",
    }


def test_raises_when_no_binding_for_environment():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _dataset("wordcount.raw", other_env="s3a://bucket/input.txt"),
        "wordcount.counts": _dataset("wordcount.counts", other_env="s3a://bucket/output"),
    }

    with pytest.raises(DatasetResolutionError, match="no binding for environment"):
        resolve_dataset_config(workload, datasets, "k8s_remote")


def test_raises_when_dataset_definition_missing():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")

    with pytest.raises(DatasetResolutionError, match="no Dataset definition found"):
        resolve_dataset_config(workload, {}, "k8s_remote")


def test_discriminated_union_parses_path_and_table_bindings():
    dataset = Dataset.model_validate(
        {
            "apiVersion": "runtime/v1alpha1",
            "kind": "Dataset",
            "metadata": {"name": "claims.normalized"},
            "bindings": {
                "onprem": {"kind": "path", "uri": "s3a://bucket/claims"},
                "azure": {"kind": "table", "identifier": "analytics.claims.normalized"},
            },
        }
    )

    assert isinstance(dataset.bindings["onprem"], PathBinding)
    assert isinstance(dataset.bindings["azure"], TableBinding)
    assert dataset.bindings["azure"].identifier == "analytics.claims.normalized"


def test_resolve_dataset_config_emits_identifier_key_for_table_binding():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _table_dataset("wordcount.raw", "k8s_remote", "analytics.wordcount.raw"),
        "wordcount.counts": _dataset("wordcount.counts", k8s_remote="s3a://portage-phase0/wordcount/output"),
    }

    config = resolve_dataset_config(workload, datasets, "k8s_remote")

    assert config == {
        "spark.portable.dataset.wordcount.raw.identifier": "portage_iceberg.analytics.wordcount.raw",
        "spark.portable.dataset.wordcount.counts.uri": "s3a://portage-phase0/wordcount/output",
    }


def test_resolve_iceberg_catalog_config_empty_when_no_table_binding():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _dataset("wordcount.raw", k8s_remote="s3a://bucket/input.txt"),
        "wordcount.counts": _dataset("wordcount.counts", k8s_remote="s3a://bucket/output"),
    }

    config = resolve_iceberg_catalog_config(
        workload, datasets, "k8s_remote", iceberg_catalog_uri=None, iceberg_warehouse=None
    )

    assert config == {}


def test_resolve_iceberg_catalog_config_emits_catalog_keys_for_table_binding():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _table_dataset("wordcount.raw", "k8s_remote", "analytics.wordcount.raw"),
        "wordcount.counts": _dataset("wordcount.counts", k8s_remote="s3a://bucket/output"),
    }

    config = resolve_iceberg_catalog_config(
        workload,
        datasets,
        "k8s_remote",
        iceberg_catalog_uri="http://iceberg-rest:8181",
        iceberg_warehouse="s3://portage-local/iceberg-warehouse",
    )

    assert config == {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.portage_iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.portage_iceberg.type": "rest",
        "spark.sql.catalog.portage_iceberg.uri": "http://iceberg-rest:8181",
        "spark.sql.catalog.portage_iceberg.warehouse": "s3://portage-local/iceberg-warehouse",
        "spark.sql.catalog.portage_iceberg.client.region": "us-east-1",
    }


def test_resolve_iceberg_catalog_config_emits_s3_client_config_when_provided():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _table_dataset("wordcount.raw", "k8s_remote", "analytics.wordcount.raw"),
        "wordcount.counts": _dataset("wordcount.counts", k8s_remote="s3a://bucket/output"),
    }

    config = resolve_iceberg_catalog_config(
        workload,
        datasets,
        "k8s_remote",
        iceberg_catalog_uri="http://iceberg-rest:8181",
        iceberg_warehouse="s3://portage-local/iceberg-warehouse",
        s3_endpoint="http://minio.local:9000",
        s3_access_key="access-key",
        s3_secret_key="secret-key",
        s3_path_style_access="true",
    )

    assert config["spark.sql.catalog.portage_iceberg.s3.endpoint"] == "http://minio.local:9000"
    assert config["spark.sql.catalog.portage_iceberg.s3.access-key-id"] == "access-key"
    assert config["spark.sql.catalog.portage_iceberg.s3.secret-access-key"] == "secret-key"
    assert config["spark.sql.catalog.portage_iceberg.s3.path-style-access"] == "true"


def test_resolve_iceberg_catalog_config_raises_when_table_binding_but_catalog_unconfigured():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    datasets = {
        "wordcount.raw": _table_dataset("wordcount.raw", "k8s_remote", "analytics.wordcount.raw"),
        "wordcount.counts": _dataset("wordcount.counts", k8s_remote="s3a://bucket/output"),
    }

    with pytest.raises(DatasetResolutionError, match="iceberg_catalog_uri"):
        resolve_iceberg_catalog_config(
            workload, datasets, "k8s_remote", iceberg_catalog_uri=None, iceberg_warehouse=None
        )
