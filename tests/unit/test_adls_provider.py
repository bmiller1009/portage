"""ADLS provider tests — no live Azure account available (ADR: mocked-only
until real credentials exist, same as the Databricks execution provider).
FakeDataLakeServiceClient stands in for azure.storage.filedatalake's real
client, mirroring test_databricks_provider.py's FakeJobsAPI pattern."""

import pytest
from azure.core.exceptions import HttpResponseError

from control_plane.credentials import AdlsCredentials
from providers.storage.adls.provider import AdlsConnectionProfile, AdlsStorageProvider


class FakeFileSystemClient:
    def __init__(self, *, healthy: bool):
        self._healthy = healthy

    def get_file_system_properties(self):
        if not self._healthy:
            raise HttpResponseError("simulated failure")
        return {"name": "raw"}


class FakeDataLakeServiceClient:
    def __init__(self, *, healthy: bool = True):
        self._healthy = healthy
        self.requested_file_system: str | None = None

    def get_file_system_client(self, file_system: str):
        self.requested_file_system = file_system
        return FakeFileSystemClient(healthy=self._healthy)


def _profile(account_key: str | None = "static-key-value") -> AdlsConnectionProfile:
    return AdlsConnectionProfile(
        account_name="portageadls", container="raw", credentials=AdlsCredentials(account_key=account_key)
    )


def test_resolve_uri_accepts_matching_abfss_uri():
    provider = AdlsStorageProvider(_profile())
    uri = "abfss://raw@portageadls.dfs.core.windows.net/claims/input.parquet"
    assert provider.resolve_uri(uri) == uri


def test_resolve_uri_rejects_mismatched_container_or_account():
    provider = AdlsStorageProvider(_profile())
    with pytest.raises(ValueError, match="cannot resolve URI outside"):
        provider.resolve_uri("abfss://other@portageadls.dfs.core.windows.net/x")


def test_spark_config_static_key_path():
    config = AdlsStorageProvider(_profile(account_key="supersecret")).spark_config()
    assert config["spark.hadoop.fs.azure.account.key.portageadls.dfs.core.windows.net"] == "supersecret"


def test_spark_config_workload_identity_path_when_no_key():
    config = AdlsStorageProvider(_profile(account_key=None)).spark_config()
    assert config["spark.hadoop.fs.azure.account.auth.type.portageadls.dfs.core.windows.net"] == "OAuth"
    assert "account.key" not in " ".join(config.keys())


def test_health_check_without_client_returns_false():
    assert AdlsStorageProvider(_profile()).health_check() is False


def test_health_check_true_when_client_reachable():
    fake_client = FakeDataLakeServiceClient(healthy=True)
    provider = AdlsStorageProvider(_profile(), client=fake_client)
    assert provider.health_check() is True
    assert fake_client.requested_file_system == "raw"


def test_health_check_false_on_azure_error():
    provider = AdlsStorageProvider(_profile(), client=FakeDataLakeServiceClient(healthy=False))
    assert provider.health_check() is False


def test_capabilities_declares_adls_protocol():
    caps = AdlsStorageProvider(_profile()).capabilities()
    assert caps.protocol == "adls"
    assert caps.path_bindings is True
    assert caps.table_bindings is False
