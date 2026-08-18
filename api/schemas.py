"""Request/response schemas for the CRUD routers. These are API-layer
concerns, not part of the portable spec — contrast with workload creation,
which reuses spec/workload/v1alpha1.py's SparkWorkload directly since the
request body *is* a portable workload definition.
"""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from spec.workload.v1alpha1 import SparkWorkload


class ExecutionProfileCreate(BaseModel):
    name: str
    provider: str
    config: dict


class ExecutionProfileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    provider: str
    config: dict


class StorageProfileCreate(BaseModel):
    name: str
    provider: str
    config: dict
    credential_reference: dict


class StorageProfileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    provider: str
    config: dict
    credential_reference: dict


class EnvironmentCreate(BaseModel):
    name: str
    execution_provider: str
    execution_profile_name: str
    storage_provider: str
    storage_profile_name: str


class EnvironmentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    execution_provider: str
    execution_profile_name: str
    storage_provider: str
    storage_profile_name: str


class DatasetBindingCreate(BaseModel):
    dataset_name: str
    environment_name: str
    kind: str = "path"
    uri: str


class DatasetBindingOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    dataset_name: str
    environment_name: str
    kind: str
    uri: str


class ArtifactBindingCreate(BaseModel):
    artifact_name: str
    artifact_version: str
    environment_name: str
    kind: str = "path"
    uri: str


class ArtifactBindingOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    artifact_name: str
    artifact_version: str
    environment_name: str
    kind: str
    uri: str


class WorkloadDefinitionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    version: str
    definition: dict


class RunCreate(BaseModel):
    workload_name: str
    workload_version: str | None = None
    environment_name: str


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workload_name: str
    workload_version: str
    environment_name: str
    state: str
    # Always set on a real (DB-persisted) Run — server_default NOT NULL
    # columns — optional here only so router-layer tests can construct a
    # bare Run(...) in memory without a real session and still serialize.
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RunEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    from_state: str | None
    to_state: str
    message: str | None


class RunLogsOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    description: str
    uri: str | None


class ProviderOut(BaseModel):
    name: str
    kind: Literal["execution", "storage"]
    provider: str


class ProviderCapabilitiesOut(BaseModel):
    name: str
    kind: Literal["execution", "storage"]
    provider: str
    capabilities: dict


class ValidateRequest(BaseModel):
    workload: SparkWorkload
    environment_name: str


class ValidateResponseOut(BaseModel):
    valid: bool
    errors: list[str]
