from pathlib import Path

import pytest
from pydantic import ValidationError

from spec.workload.v1alpha1 import parse_workload

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


def test_parses_valid_workload():
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")

    assert workload.metadata.name == "wordcount"
    assert workload.runtime.spark == "4.2"
    assert workload.application.entryPoint == "wordcount.jobs.count"
    assert workload.datasets.inputs["text"].dataset == "wordcount.raw"
    assert workload.datasets.outputs["counts"].dataset == "wordcount.counts"
    assert workload.resources.scaling.maxExecutors == 2


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
