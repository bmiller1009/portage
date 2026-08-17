"""Environment schema, v1alpha1 (docs/architecture/spec.md §8) — Phase-0-scoped.

This is intentionally minimal: enough for the CLI to pick an execution
provider and a storage provider by name. The full v0.1 environment
resolver (persisted execution/storage *profiles* — connection details,
credentials references, catalog config) is out of scope here; for Phase 0
the "profile" a provider needs (kubeconfig context, S3 endpoint/credentials)
is resolved from environment variables by the provider construction code
in cli/main.py, not by this model.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel


class EnvironmentMetadata(BaseModel):
    name: str


class ExecutionRef(BaseModel):
    provider: Literal["kubernetes", "databricks"]
    profile: str


class DataRef(BaseModel):
    provider: Literal["s3", "vast", "adls"]
    profile: str


class Environment(BaseModel):
    apiVersion: Literal["runtime/v1alpha1"]
    kind: Literal["Environment"]
    metadata: EnvironmentMetadata
    execution: ExecutionRef
    data: DataRef


def parse_environment(path: str | Path) -> Environment:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Environment.model_validate(raw)
