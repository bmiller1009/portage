from pathlib import Path

import pytest
from pydantic import ValidationError

from spec.workload.v1alpha1 import SparkWorkload, json_schema, parse_workload

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


def test_parses_valid_workload():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")

    assert workload.metadata.name == "wordcount"
    assert workload.apiVersion == "runtime/v1"
    assert workload.runtime.spark == "4.2"
    assert workload.application.entryPoint == "wordcount.jobs.count"
    assert workload.datasets.inputs["text"].dataset == "wordcount.raw"
    assert workload.datasets.outputs["counts"].dataset == "wordcount.counts"
    assert workload.resources.scaling.maxExecutors == 2


def _minimal_workload_kwargs(api_version: str) -> dict:
    return {
        "apiVersion": api_version,
        "kind": "SparkWorkload",
        "metadata": {"name": "w", "version": "1.0"},
        "runtime": {"spark": "4.2"},
        "application": {"type": "python-wheel", "artifact": "artifact://w/1.0", "entryPoint": "w.jobs.run"},
        "datasets": {},
        "resources": {
            "driver": {"cores": 1, "memory": "1Gi"},
            "executor": {"cores": 1, "memory": "1Gi"},
            "scaling": {"minExecutors": 1, "maxExecutors": 1},
        },
        "execution": {"timeout": "1h"},
    }


def test_stable_api_version_parses_without_warning(recwarn):
    SparkWorkload.model_validate(_minimal_workload_kwargs("runtime/v1"))

    assert not any(issubclass(w.category, DeprecationWarning) for w in recwarn.list)


def test_deprecated_api_version_still_parses_but_warns():
    with pytest.warns(DeprecationWarning, match="runtime/v1alpha1.*deprecated"):
        workload = SparkWorkload.model_validate(_minimal_workload_kwargs("runtime/v1alpha1"))

    assert workload.apiVersion == "runtime/v1alpha1"


def test_parses_multi_dataset_workload():
    workload = parse_workload(EXAMPLES_DIR / "claims-normalization.yaml")

    assert workload.metadata.name == "claims-normalization"
    assert set(workload.datasets.inputs) == {"claims", "reference"}
    assert workload.datasets.inputs["claims"].dataset == "claims.raw"
    assert workload.datasets.inputs["reference"].dataset == "provider.reference"
    assert workload.datasets.outputs["normalized"].dataset == "claims.normalized"


def test_json_schema_export_describes_required_top_level_fields():
    schema = json_schema()

    assert schema["title"] == "SparkWorkload"
    assert set(schema["required"]) >= {
        "apiVersion",
        "kind",
        "metadata",
        "runtime",
        "application",
        "datasets",
        "resources",
        "execution",
    }


def test_rejects_workload_missing_resources(tmp_path):
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text(
        """
apiVersion: runtime/v1alpha1
kind: SparkWorkload

metadata:
  name: no-resources
  version: "0.0.1"

runtime:
  spark: "4.2"

application:
  type: python-wheel
  artifact: artifact://no-resources/0.0.1
  entryPoint: no_resources.jobs.run

datasets:
  inputs: {}
  outputs: {}

execution:
  timeout: 10m
"""
    )

    with pytest.raises(ValidationError):
        parse_workload(invalid)


def test_parses_declarative_pipeline_workload():
    workload = parse_workload(EXAMPLES_DIR / "hello-pipeline.yaml")

    assert workload.application.type == "spark-declarative-pipeline"
    assert workload.application.pipelineSpec == "local:///opt/portage/pipelines/hello/spark-pipeline.yml"
    assert workload.application.artifact is None
    assert workload.application.entryPoint is None


def test_rejects_declarative_pipeline_missing_pipeline_spec(tmp_path):
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text(
        """
apiVersion: runtime/v1alpha1
kind: SparkWorkload

metadata:
  name: broken-pipeline
  version: "0.0.1"

runtime:
  spark: "4.2"

application:
  type: spark-declarative-pipeline

datasets: {}

resources:
  driver:
    cores: 1
    memory: 2Gi
  executor:
    cores: 1
    memory: 2Gi
  scaling:
    minExecutors: 1
    maxExecutors: 1

execution:
  timeout: 10m
"""
    )

    with pytest.raises(ValidationError):
        parse_workload(invalid)


def test_rejects_workload_missing_entry_point(tmp_path):
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text(
        """
apiVersion: runtime/v1alpha1
kind: SparkWorkload

metadata:
  name: broken
  version: "0.0.1"

runtime:
  spark: "4.2"

application:
  type: python-wheel
  artifact: artifact://broken/0.0.1

datasets:
  inputs: {}
  outputs: {}

resources:
  driver:
    cores: 1
    memory: 2Gi
  executor:
    cores: 1
    memory: 2Gi
  scaling:
    minExecutors: 1
    maxExecutors: 1

execution:
  timeout: 10m
"""
    )

    with pytest.raises(ValidationError):
        parse_workload(invalid)
