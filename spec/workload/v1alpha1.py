"""Portable Workload schema, v1alpha1 (docs/architecture/spec.md §7.1)."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class WorkloadMetadata(BaseModel):
    name: str
    version: str


class RuntimeSpec(BaseModel):
    spark: str


class ApplicationSpec(BaseModel):
    type: Literal["python-wheel", "jvm-jar"]
    artifact: str
    entryPoint: str


class DatasetRef(BaseModel):
    dataset: str


class DatasetsSpec(BaseModel):
    inputs: dict[str, DatasetRef] = Field(default_factory=dict)
    outputs: dict[str, DatasetRef] = Field(default_factory=dict)


class DriverResources(BaseModel):
    cores: int
    memory: str


class ExecutorResources(BaseModel):
    cores: int
    memory: str


class ScalingSpec(BaseModel):
    minExecutors: int
    maxExecutors: int


class ResourcesSpec(BaseModel):
    driver: DriverResources
    executor: ExecutorResources
    scaling: ScalingSpec


class ExecutionPolicy(BaseModel):
    timeout: str
    retries: int = 0


class RequirementsSpec(BaseModel):
    """Explicit capability requirements (spec §20-21) a provider must
    support for this workload to be schedulable there — matched against a
    provider's CapabilitySet by control_plane/execution_provider.py's
    match_capabilities(). All default False: most workloads need none of
    these, and existing workload definitions parse unchanged."""

    dynamicAllocation: bool = False
    gpu: bool = False
    streaming: bool = False
    localDisk: bool = False
    sparkConnect: bool = False


class SparkWorkload(BaseModel):
    apiVersion: Literal["runtime/v1alpha1"]
    kind: Literal["SparkWorkload"]
    metadata: WorkloadMetadata
    runtime: RuntimeSpec
    application: ApplicationSpec
    arguments: list[str] = Field(default_factory=list)
    datasets: DatasetsSpec
    resources: ResourcesSpec
    execution: ExecutionPolicy
    requirements: RequirementsSpec = Field(default_factory=RequirementsSpec)


def parse_workload(path: str | Path) -> SparkWorkload:
    """Load and validate a portable workload definition from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return SparkWorkload.model_validate(raw)


def json_schema() -> dict:
    """Export the v1alpha1 JSON Schema, per the spec's own recommendation
    that the portable workload schema be publicly documented (§43)."""
    return SparkWorkload.model_json_schema()
