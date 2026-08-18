"""Builds a live ExecutionProvider (and a storage Spark config dict) from
already-fetched, persisted ExecutionProfile/StorageProfile rows. Pure
translation, no DB access — used by reconciler/service.py and
control_plane/run_service.py (cancel/logs), the only two callers that ever
turn persisted config into a live provider.
"""

from typing import cast

from control_plane.credentials import (
    resolve_adls_credentials,
    resolve_s3_credentials,
    resolve_vast_credentials,
)
from control_plane.execution_provider import ExecutionProvider
from control_plane.models import ExecutionProfile, StorageProfile
from control_plane.storage_provider import StorageProvider
from providers.execution.databricks.provider import (
    DatabricksExecutionProvider,
    DatabricksProfile,
    WorkspaceClientLike,
)
from providers.execution.kubernetes.provider import KubernetesExecutionProvider, KubernetesProfile
from providers.storage.adls.provider import AdlsConnectionProfile, AdlsStorageProvider
from providers.storage.s3.provider import S3ConnectionProfile, S3StorageProvider
from providers.storage.vast.provider import (
    VastNfsConnectionProfile,
    VastNfsStorageProvider,
    VastS3ConnectionProfile,
    VastS3StorageProvider,
)


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


def _build_storage_provider(storage_profile: StorageProfile) -> StorageProvider:
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
        return S3StorageProvider(profile)

    if storage_profile.provider == "vast":
        config = storage_profile.config
        protocol = config.get("protocol")
        if protocol == "s3":
            access_key, secret_key = resolve_vast_credentials(storage_profile.credential_reference)
            vast_profile = VastS3ConnectionProfile(
                access_key=access_key,
                secret_key=secret_key,
                endpoint_url=config["endpoint_url"],
                region=config.get("region", "us-east-1"),
                path_style_access=config.get("path_style_access", True),
            )
            return VastS3StorageProvider(vast_profile)
        if protocol == "nfs":
            mount = config["mount"]
            nfs_profile = VastNfsConnectionProfile(
                mount_path=mount["path"], server=config["server"], export_path=config["exportPath"]
            )
            return VastNfsStorageProvider(nfs_profile)
        raise UnsupportedProviderError(f"VAST storage profile missing/invalid 'protocol': {protocol!r}")

    if storage_profile.provider == "adls":
        credentials = resolve_adls_credentials(storage_profile.credential_reference)
        config = storage_profile.config
        adls_profile = AdlsConnectionProfile(
            account_name=config["account_name"], container=config["container"], credentials=credentials
        )
        return AdlsStorageProvider(adls_profile)

    raise UnsupportedProviderError(f"unsupported storage provider: {storage_profile.provider}")


def build_storage_config(storage_profile: StorageProfile) -> dict[str, str]:
    return _build_storage_provider(storage_profile).spark_config()


def build_storage_volume_mounts(storage_profile: StorageProfile) -> list[dict] | None:
    """Pod volume mounts the storage provider needs, if any (spec §48 —
    VAST NFS). Kept separate from build_storage_config() rather than
    changing that function's return shape: every existing caller only
    ever needs the flat spark_config dict, and constructing the provider
    twice for NFS mode is cheap (no I/O in any storage provider's
    constructor)."""
    return _build_storage_provider(storage_profile).volume_mounts()
