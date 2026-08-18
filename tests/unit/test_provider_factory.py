"""Pure translation-logic tests — no database, no live cluster/workspace.
Kubernetes's real client construction (kubeconfig loading) is monkeypatched
out so this stays fast; Databricks needs no such monkeypatching since
build_execution_provider() never constructs a live WorkspaceClient — that
now happens lazily inside the provider itself, only on submit/status/cancel
(see providers/execution/databricks/provider.py's _get_client()). The
actual K8s translation shape is already covered by
tests/unit/test_kubernetes_provider.py."""

import pytest

from control_plane.credentials import CredentialResolutionError
from control_plane.models import ExecutionProfile, StorageProfile
from control_plane.provider_factory import (
    UnsupportedProviderError,
    build_execution_provider,
    build_storage_config,
    build_storage_volume_mounts,
)
from providers.execution.databricks.provider import DatabricksExecutionProvider
from providers.execution.kubernetes import provider as kubernetes_provider_module
from providers.execution.kubernetes.provider import KubernetesExecutionProvider


def test_build_execution_provider_kubernetes(monkeypatch):
    monkeypatch.setattr(kubernetes_provider_module.k8s_config, "load_kube_config", lambda **kwargs: None)
    monkeypatch.setattr(kubernetes_provider_module.k8s_client, "CustomObjectsApi", lambda: object())

    profile = ExecutionProfile(
        name="phase0-remote",
        provider="kubernetes",
        config={"namespace": "default", "service_account": "spark", "image": "portage/wordcount:0.1.0"},
    )

    provider = build_execution_provider(profile)

    assert isinstance(provider, KubernetesExecutionProvider)
    assert provider.profile.namespace == "default"
    assert provider.profile.image == "portage/wordcount:0.1.0"
    assert provider.profile.runtime_profiles == {}


def test_build_execution_provider_kubernetes_passes_through_runtime_profiles(monkeypatch):
    monkeypatch.setattr(kubernetes_provider_module.k8s_config, "load_kube_config", lambda **kwargs: None)
    monkeypatch.setattr(kubernetes_provider_module.k8s_client, "CustomObjectsApi", lambda: object())

    profile = ExecutionProfile(
        name="phase0-remote",
        provider="kubernetes",
        config={
            "namespace": "default",
            "service_account": "spark",
            "image": "portage/wordcount:0.1.0",
            "runtimeProfiles": {"high-memory": {"nodeSelector": {"workload-type": "memory-optimized"}}},
        },
    )

    provider = build_execution_provider(profile)

    assert isinstance(provider, KubernetesExecutionProvider)
    assert provider.profile.runtime_profiles == {
        "high-memory": {"nodeSelector": {"workload-type": "memory-optimized"}}
    }


def test_build_execution_provider_databricks_does_not_construct_a_client():
    """Regression test: build_execution_provider() used to eagerly
    construct a real WorkspaceClient(), which raises at construction time
    whenever no Databricks credentials are configured — crashing even a
    translation-only caller like `plane workload validate`. No monkeypatch
    of databricks.sdk is needed here specifically because that no longer
    happens: if it did, this test would fail with a real auth error."""
    profile = ExecutionProfile(
        name="dbx",
        provider="databricks",
        config={"host": "https://example.databricks.com", "cluster_node_type_id": "i3.xlarge"},
    )

    provider = build_execution_provider(profile)

    assert isinstance(provider, DatabricksExecutionProvider)
    assert provider.profile.host == "https://example.databricks.com"
    assert provider._client is None


def test_build_execution_provider_databricks_passes_through_credential_reference():
    profile = ExecutionProfile(
        name="dbx",
        provider="databricks",
        config={
            "host": "https://example.databricks.com",
            "cluster_node_type_id": "i3.xlarge",
            "credential_reference": {"provider": "env", "reference": "PORTAGE_DATABRICKS"},
        },
    )

    provider = build_execution_provider(profile)

    assert isinstance(provider, DatabricksExecutionProvider)
    assert provider.profile.credential_reference == {
        "provider": "env",
        "reference": "PORTAGE_DATABRICKS",
    }


def test_build_execution_provider_unsupported():
    profile = ExecutionProfile(name="x", provider="not-a-real-provider", config={})
    with pytest.raises(UnsupportedProviderError):
        build_execution_provider(profile)


def test_build_storage_config_resolves_credentials_from_env(monkeypatch):
    monkeypatch.setenv("PORTAGE_TEST_ACCESS_KEY", "AKIA123")
    monkeypatch.setenv("PORTAGE_TEST_SECRET_KEY", "supersecret")
    profile = StorageProfile(
        name="phase0-minio",
        provider="s3",
        config={"endpoint_url": "http://minio.local:9000"},
        credential_reference={"provider": "env", "reference": "PORTAGE_TEST"},
    )

    config = build_storage_config(profile)

    assert config["spark.hadoop.fs.s3a.access.key"] == "AKIA123"
    assert config["spark.hadoop.fs.s3a.secret.key"] == "supersecret"
    assert config["spark.hadoop.fs.s3a.endpoint"] == "http://minio.local:9000"


def test_build_storage_config_missing_credentials_raises(monkeypatch):
    monkeypatch.delenv("PORTAGE_MISSING_ACCESS_KEY", raising=False)
    monkeypatch.delenv("PORTAGE_MISSING_SECRET_KEY", raising=False)
    profile = StorageProfile(
        name="x",
        provider="s3",
        config={},
        credential_reference={"provider": "env", "reference": "PORTAGE_MISSING"},
    )

    with pytest.raises(CredentialResolutionError):
        build_storage_config(profile)


def test_build_storage_config_unsupported():
    profile = StorageProfile(name="x", provider="not-a-real-provider", config={}, credential_reference={})
    with pytest.raises(UnsupportedProviderError):
        build_storage_config(profile)


def test_build_storage_config_vast_s3_mode(monkeypatch):
    monkeypatch.setenv("PORTAGE_TEST_ACCESS_KEY", "AKIA123")
    monkeypatch.setenv("PORTAGE_TEST_SECRET_KEY", "supersecret")
    profile = StorageProfile(
        name="vast-prod",
        provider="vast",
        config={"protocol": "s3", "endpoint_url": "http://vast.local:9000"},
        credential_reference={"provider": "env", "reference": "PORTAGE_TEST"},
    )

    config = build_storage_config(profile)

    assert config["spark.hadoop.fs.s3a.access.key"] == "AKIA123"
    assert config["spark.hadoop.fs.s3a.endpoint"] == "http://vast.local:9000"


def _vast_nfs_profile() -> StorageProfile:
    return StorageProfile(
        name="vast-nfs",
        provider="vast",
        config={"protocol": "nfs", "mount": {"path": "/vast"}, "server": "vast.example.com", "exportPath": "/export/portage"},
        credential_reference={},
    )


def test_build_storage_config_vast_nfs_mode_is_empty():
    # Nothing NFS-specific is expressible as sparkConf — the real
    # translation is build_storage_volume_mounts(), not this function.
    assert build_storage_config(_vast_nfs_profile()) == {}


def test_build_storage_volume_mounts_vast_nfs_mode():
    mounts = build_storage_volume_mounts(_vast_nfs_profile())

    assert mounts == [
        {
            "name": "vast-nfs-data",
            "volume": {"nfs": {"server": "vast.example.com", "path": "/export/portage"}},
            "mount_path": "/vast",
        }
    ]


def test_build_storage_volume_mounts_none_for_s3(monkeypatch):
    monkeypatch.setenv("PORTAGE_TEST_ACCESS_KEY", "AKIA123")
    monkeypatch.setenv("PORTAGE_TEST_SECRET_KEY", "supersecret")
    profile = StorageProfile(
        name="phase0-minio",
        provider="s3",
        config={},
        credential_reference={"provider": "env", "reference": "PORTAGE_TEST"},
    )

    assert build_storage_volume_mounts(profile) is None


def test_build_storage_config_vast_missing_protocol():
    profile = StorageProfile(name="vast-bad", provider="vast", config={}, credential_reference={})
    with pytest.raises(UnsupportedProviderError, match="protocol"):
        build_storage_config(profile)


def test_build_storage_config_adls_static_key(monkeypatch):
    monkeypatch.setenv("PORTAGE_TEST_ACCOUNT_KEY", "supersecret")
    profile = StorageProfile(
        name="adls-prod",
        provider="adls",
        config={"account_name": "portageadls", "container": "raw"},
        credential_reference={"provider": "env", "reference": "PORTAGE_TEST"},
    )

    config = build_storage_config(profile)

    assert config["spark.hadoop.fs.azure.account.key.portageadls.dfs.core.windows.net"] == "supersecret"


def test_build_storage_config_adls_workload_identity(monkeypatch):
    monkeypatch.delenv("PORTAGE_NOKEY_ACCOUNT_KEY", raising=False)
    profile = StorageProfile(
        name="adls-wi",
        provider="adls",
        config={"account_name": "portageadls", "container": "raw"},
        credential_reference={"provider": "env", "reference": "PORTAGE_NOKEY"},
    )

    config = build_storage_config(profile)

    assert config["spark.hadoop.fs.azure.account.auth.type.portageadls.dfs.core.windows.net"] == "OAuth"
