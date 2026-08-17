"""Request/response schemas for the CRUD routers. These are API-layer
concerns, not part of the portable spec — contrast with workload creation,
which reuses spec/workload/v1alpha1.py's SparkWorkload directly since the
request body *is* a portable workload definition.
"""

from pydantic import BaseModel, ConfigDict


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


class WorkloadDefinitionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    version: str
    definition: dict
