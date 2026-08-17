"""Dataset binding schema, v1alpha1 (docs/architecture/spec.md §9). Path
bindings only for Phase 0 — table bindings (§11) are a later milestone."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel

from spec.workload.v1alpha1 import SparkWorkload


class DatasetMetadata(BaseModel):
    name: str


class PathBinding(BaseModel):
    kind: Literal["path"] = "path"
    uri: str


class Dataset(BaseModel):
    apiVersion: Literal["runtime/v1alpha1"]
    kind: Literal["Dataset"]
    metadata: DatasetMetadata
    bindings: dict[str, PathBinding]


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
    """Produce the portable.dataset.<name>.uri Spark configuration entries
    for every input/output dataset a workload references, per ADR 0006."""
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
        config[f"portable.dataset.{ref.dataset}.uri"] = binding.uri
    return config
