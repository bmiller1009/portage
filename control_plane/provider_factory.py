"""Builds a live ExecutionProvider (and a storage Spark config dict) from
already-fetched, persisted ExecutionProfile/StorageProfile rows. Pure
translation, no DB access — mirrors what cli/environments.py does from
files/env vars, but for the reconciler's database-backed path instead.
"""

from typing import cast

from control_plane.credentials import resolve_s3_credentials
from control_plane.execution_provider import ExecutionProvider
from control_plane.models import ExecutionProfile, StorageProfile
from providers.execution.databricks.provider import (
    DatabricksExecutionProvider,
    DatabricksProfile,
    WorkspaceClientLike,
)
from providers.execution.kubernetes.provider import KubernetesExecutionProvider, KubernetesProfile
from providers.storage.s3.provider import S3ConnectionProfile, S3StorageProvider


class UnsupportedProviderError(Exception):
    pass


def build_execution_provider(execution_profile: ExecutionProfile) -> ExecutionProvider:
    config = execution_profile.config

    if execution_profile.provider == "kubernetes":
        profile = KubernetesProfile(
            namespace=config["namespace"],
            service_account=config["service_account"],
            image=config["image"],
            kubeconfig_path=config.get("kubeconfig_path"),
            context=config.get("context"),
        )
        return KubernetesExecutionProvider(profile)

    if execution_profile.provider == "databricks":
        from databricks.sdk import WorkspaceClient

        profile = DatabricksProfile(
            host=config["host"],
            cluster_node_type_id=config["cluster_node_type_id"],
            num_workers=config.get("num_workers", 1),
        )
        return DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, WorkspaceClient()))

    raise UnsupportedProviderError(f"unsupported execution provider: {execution_profile.provider}")


def build_storage_config(storage_profile: StorageProfile) -> dict[str, str]:
    if storage_profile.provider == "s3":
        access_key, secret_key = resolve_s3_credentials(storage_profile.credential_reference)
        config = storage_profile.config
        profile = S3ConnectionProfile(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=config.get("endpoint_url"),
            region=config.get("region", "us-east-1"),
            path_style_access=config.get("path_style_access", True),
        )
        return S3StorageProvider(profile).spark_config()

    raise UnsupportedProviderError(f"unsupported storage provider: {storage_profile.provider}")
