"""Pure translation-logic tests — no database, no live cluster/workspace.
Real client construction (kubeconfig loading, WorkspaceClient auth) is
monkeypatched out so this stays fast; the actual K8s translation shape is
already covered by tests/unit/test_kubernetes_provider.py."""

import pytest

from control_plane.credentials import CredentialResolutionError
from control_plane.models import ExecutionProfile, StorageProfile
from control_plane.provider_factory import (
    UnsupportedProviderError,
    build_execution_provider,
    build_storage_config,
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


def test_build_execution_provider_databricks(monkeypatch):
    import databricks.sdk as databricks_sdk_module

    monkeypatch.setattr(databricks_sdk_module, "WorkspaceClient", lambda: object())

    profile = ExecutionProfile(
        name="dbx",
        provider="databricks",
        config={"host": "https://example.databricks.com", "cluster_node_type_id": "i3.xlarge"},
    )

    provider = build_execution_provider(profile)

    assert isinstance(provider, DatabricksExecutionProvider)
    assert provider.profile.host == "https://example.databricks.com"


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
    profile = StorageProfile(name="x", provider="vast", config={}, credential_reference={})
    with pytest.raises(UnsupportedProviderError):
        build_storage_config(profile)
