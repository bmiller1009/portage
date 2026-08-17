"""Phase-0 environment -> provider construction (docs/architecture/spec.md §8).

Full profile persistence (execution_profiles/storage_profiles tables) is
v0.1 scope (see spec/environment/README.md). For Phase 0, the connection
details behind an Environment's named "profile" are resolved from
environment variables here, and Environment/Dataset definitions are
resolved by filename convention under examples/ rather than a registry.
"""

import os
from pathlib import Path
from typing import cast

from control_plane.execution_provider import ExecutionProvider
from providers.execution.databricks.provider import (
    DatabricksExecutionProvider,
    DatabricksProfile,
    WorkspaceClientLike,
)
from providers.execution.kubernetes.provider import (
    KubernetesExecutionProvider,
    KubernetesProfile,
)
from providers.storage.s3.provider import S3ConnectionProfile, S3StorageProvider
from spec.dataset.v1alpha1 import Dataset, parse_dataset
from spec.environment.v1alpha1 import Environment, parse_environment
from spec.workload.v1alpha1 import SparkWorkload

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"


def load_environment(name: str) -> Environment:
    path = EXAMPLES_DIR / "environments" / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"no Environment definition found at {path}")
    return parse_environment(path)


def load_datasets_for_workload(workload: SparkWorkload) -> dict[str, Dataset]:
    refs = list(workload.datasets.inputs.values()) + list(workload.datasets.outputs.values())
    datasets: dict[str, Dataset] = {}
    for ref in refs:
        path = EXAMPLES_DIR / "datasets" / f"{ref.dataset}.yaml"
        if not path.exists():
            raise FileNotFoundError(f"no Dataset definition found at {path}")
        datasets[ref.dataset] = parse_dataset(path)
    return datasets


def build_execution_provider(environment: Environment) -> ExecutionProvider:
    if environment.execution.provider == "kubernetes":
        profile = KubernetesProfile(
            namespace=os.environ.get("PORTAGE_K8S_NAMESPACE", "default"),
            service_account=os.environ.get("PORTAGE_K8S_SERVICE_ACCOUNT", "spark"),
            image=os.environ["PORTAGE_K8S_IMAGE"],
            kubeconfig_path=os.environ.get("PORTAGE_KUBECONFIG"),
            context=os.environ.get("PORTAGE_K8S_CONTEXT"),
        )
        return KubernetesExecutionProvider(profile)

    if environment.execution.provider == "databricks":
        from databricks.sdk import WorkspaceClient

        profile = DatabricksProfile(
            host=os.environ["DATABRICKS_HOST"],
            cluster_node_type_id=os.environ.get("PORTAGE_DBX_NODE_TYPE", "i3.xlarge"),
        )
        # WorkspaceClient's real type is broader than the JobsAPILike Protocol
        # this provider depends on (its .jobs is a property, not a plain
        # attribute) — cast rather than widen the Protocol just to satisfy it.
        return DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, WorkspaceClient()))

    raise ValueError(f"unsupported execution provider: {environment.execution.provider}")


def build_storage_config(environment: Environment) -> dict[str, str]:
    if environment.data.provider == "s3":
        profile = S3ConnectionProfile(
            access_key=os.environ["S3_ACCESS_KEY"],
            secret_key=os.environ["S3_SECRET_KEY"],
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        )
        return S3StorageProvider(profile).spark_config()

    raise ValueError(f"unsupported storage provider: {environment.data.provider}")
