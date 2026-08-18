"""Azure Data Lake Storage (ABFS) provider (docs/architecture/spec.md
§50). Responsibilities mirror S3: logical mapping, identity, filesystem/
account configuration, Spark connector configuration, health checks.
Workload identity is preferred to static storage keys — see
control_plane.credentials.AdlsCredentials's account_key=None case, which
switches spark_config() to the OAuth/MSI path instead of a static key.

No self-hostable open-source ADLS substitute and no real Azure
subscription is available to this project — translation-layer-only,
tested against a fake client, never live-verified against a real Azure
account. Same pattern as the Databricks execution provider (ADR: mocked-
only until real credentials exist).
"""

from dataclasses import dataclass
from typing import Any, Protocol

from azure.core.exceptions import AzureError

from control_plane.credentials import AdlsCredentials
from control_plane.storage_provider import StorageCapabilitySet


@dataclass
class AdlsConnectionProfile:
    account_name: str
    container: str
    credentials: AdlsCredentials


class AdlsClientLike(Protocol):
    """The subset of azure.storage.filedatalake.DataLakeServiceClient this
    provider uses — a Protocol so unit tests can inject a fake without
    subclassing the real (heavily generated) SDK client."""

    def get_file_system_client(self, file_system: str) -> Any: ...


class AdlsStorageProvider:
    def __init__(self, profile: AdlsConnectionProfile, client: AdlsClientLike | None = None):
        self.profile = profile
        # Testing seam, same pattern as DatabricksExecutionProvider's
        # `client` — None is only valid for translation-only use
        # (resolve_uri/spark_config); health_check requires a real client.
        self._client = client

    def _account_host(self) -> str:
        return f"{self.profile.account_name}.dfs.core.windows.net"

    def resolve_uri(self, binding_uri: str) -> str:
        prefix = f"abfss://{self.profile.container}@{self._account_host()}/"
        if not binding_uri.startswith(prefix):
            raise ValueError(f"ADLS provider cannot resolve URI outside {prefix}: {binding_uri}")
        return binding_uri

    def spark_config(self) -> dict[str, str]:
        account_host = self._account_host()
        if self.profile.credentials.account_key is not None:
            return {f"spark.hadoop.fs.azure.account.key.{account_host}": self.profile.credentials.account_key}
        # Workload identity path (spec §50's preferred option) — OAuth via
        # Azure AD's managed-identity token provider, no static secret
        # ever touches Spark config. Confirmed field names against
        # Hadoop's own ABFS connector docs, not assumed.
        return {
            f"spark.hadoop.fs.azure.account.auth.type.{account_host}": "OAuth",
            f"spark.hadoop.fs.azure.account.oauth.provider.type.{account_host}": (
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
            ),
        }

    def health_check(self) -> bool:
        if self._client is None:
            return False
        try:
            self._client.get_file_system_client(self.profile.container).get_file_system_properties()
            return True
        except AzureError:
            return False

    def capabilities(self) -> StorageCapabilitySet:
        return StorageCapabilitySet(protocol="adls", path_bindings=True, table_bindings=False)
