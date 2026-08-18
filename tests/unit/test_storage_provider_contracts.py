"""Provider contract tests (spec §52.2): "Every StorageProvider similarly
runs: resolve logical dataset, reject invalid mapping, resolve identity,
health check, generate Spark configuration, never expose secrets." Same
shared-suite spirit as test_execution_provider_contracts.py — a new
storage provider added later has to satisfy this exact suite too.

Every test function calls _cases() itself, so each test gets fresh
provider instances — no state leaks between tests sharing a parametrize id.
"""

from dataclasses import dataclass
from typing import Any

import pytest
from moto import mock_aws

from control_plane.credentials import AdlsCredentials
from control_plane.storage_provider import StorageProvider
from providers.storage.adls.provider import AdlsConnectionProfile, AdlsStorageProvider
from providers.storage.s3.provider import S3ConnectionProfile, S3StorageProvider
from providers.storage.vast.provider import (
    VastNfsConnectionProfile,
    VastNfsStorageProvider,
    VastS3ConnectionProfile,
    VastS3StorageProvider,
)
from tests.unit.test_adls_provider import FakeDataLakeServiceClient


@dataclass
class StorageProviderCase:
    name: str
    provider: StorageProvider
    valid_uri: str
    invalid_uri: str
    identity_marker: str  # a configured, non-secret identity value (e.g. account name) that must appear in spark_config()
    secret_value: str | None  # a configured secret that must NEVER appear in capabilities()
    # Health check behavior is provider-specific by design (VAST NFS is
    # honestly always False, no live server reachable) — each case
    # supplies its own context manager for the "reachable" scenario and
    # its own expected result, rather than forcing one shared shape.
    health_check_context: Any
    expected_healthy: bool


def _s3_case() -> StorageProviderCase:
    # moto's interception needs a resolvable hostname (confirmed during
    # v0.2's VAST-S3 work) — a real AWS hostname still never leaves this
    # process, moto intercepts every request before any socket is opened.
    profile = S3ConnectionProfile(
        access_key="contract-test-access",
        secret_key="contract-test-secret",
        endpoint_url="https://s3.amazonaws.com",
    )
    provider = S3StorageProvider(profile)
    return StorageProviderCase(
        name="s3",
        provider=provider,
        valid_uri="s3a://bucket/path",
        invalid_uri="abfss://bucket/path",
        identity_marker="contract-test-access",
        secret_value="contract-test-secret",
        health_check_context=mock_aws,
        expected_healthy=True,
    )


def _vast_s3_case() -> StorageProviderCase:
    # Same moto-needs-a-resolvable-hostname reasoning as the s3 case above.
    profile = VastS3ConnectionProfile(
        access_key="contract-test-access",
        secret_key="contract-test-secret",
        endpoint_url="https://s3.amazonaws.com",
    )
    provider = VastS3StorageProvider(profile)
    return StorageProviderCase(
        name="vast-s3",
        provider=provider,
        valid_uri="s3a://bucket/path",
        invalid_uri="abfss://bucket/path",
        identity_marker="contract-test-access",
        secret_value="contract-test-secret",
        health_check_context=mock_aws,
        expected_healthy=True,
    )


def _vast_nfs_case() -> StorageProviderCase:
    profile = VastNfsConnectionProfile(
        mount_path="/vast", server="vast.example.com", export_path="/export/portage"
    )
    provider = VastNfsStorageProvider(profile)

    from contextlib import contextmanager

    @contextmanager
    def _no_op_context():
        yield

    return StorageProviderCase(
        name="vast-nfs",
        provider=provider,
        valid_uri="/vast/claims/input.parquet",
        invalid_uri="s3a://bucket/path",
        identity_marker="vast.example.com",  # not spark-config-expressible; checked via volume_mounts() instead
        secret_value=None,
        health_check_context=_no_op_context,
        expected_healthy=False,  # honestly always False — no live NFS server reachable (see provider docstring)
    )


def _adls_case() -> StorageProviderCase:
    from contextlib import contextmanager

    fake_client = FakeDataLakeServiceClient(healthy=True)
    profile = AdlsConnectionProfile(
        account_name="contracttest", container="raw", credentials=AdlsCredentials(account_key="contract-test-key")
    )
    provider = AdlsStorageProvider(profile, client=fake_client)

    @contextmanager
    def _client_already_wired():
        yield

    return StorageProviderCase(
        name="adls",
        provider=provider,
        valid_uri="abfss://raw@contracttest.dfs.core.windows.net/claims/input.parquet",
        invalid_uri="s3a://bucket/path",
        identity_marker="contracttest.dfs.core.windows.net",
        secret_value="contract-test-key",
        health_check_context=_client_already_wired,
        expected_healthy=True,
    )


def _cases() -> list[StorageProviderCase]:
    return [_s3_case(), _vast_s3_case(), _vast_nfs_case(), _adls_case()]


_CASE_IDS = [c.name for c in _cases()]


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_capabilities_are_well_formed(case: StorageProviderCase):
    caps = case.provider.capabilities()
    assert caps.protocol


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_resolve_uri_accepts_a_matching_uri(case: StorageProviderCase):
    assert case.provider.resolve_uri(case.valid_uri) == case.valid_uri


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_resolve_uri_rejects_an_invalid_mapping(case: StorageProviderCase):
    with pytest.raises(ValueError):
        case.provider.resolve_uri(case.invalid_uri)


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_spark_config_reflects_configured_identity(case: StorageProviderCase):
    if case.name == "vast-nfs":
        pytest.skip("VAST NFS identity isn't spark-config-expressible — see docs/architecture/spec.md §74.6")
    config = case.provider.spark_config()
    # S3/VAST-S3 carry identity as a config *value* (the access key);
    # ADLS carries it as part of a config *key* (the account host is
    # embedded in the Hadoop property name itself) — check both rather
    # than assuming one shape fits every provider.
    assert case.identity_marker in config.values() or any(
        case.identity_marker in key for key in config
    )


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_health_check_returns_a_bool(case: StorageProviderCase):
    with case.health_check_context():
        result = case.provider.health_check()
    assert isinstance(result, bool)


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_health_check_matches_expected_behavior(case: StorageProviderCase):
    with case.health_check_context():
        if case.name in ("s3", "vast-s3"):
            import boto3

            # Both cases share the same real-hostname endpoint_url (see
            # _s3_case()/_vast_s3_case()'s comments) — hardcoded here
            # rather than reaching into case.provider's internals, which
            # StorageProvider (a Protocol) doesn't expose.
            boto3.client(
                "s3", region_name="us-east-1", endpoint_url="https://s3.amazonaws.com"
            ).create_bucket(Bucket="contract-test-bucket")
        assert case.provider.health_check() is case.expected_healthy


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_capabilities_never_expose_secrets(case: StorageProviderCase):
    """Spec §52.2's "never expose secrets" — capabilities() is what GET
    /v1/providers/{name}/capabilities returns to any authenticated Viewer
    (api/routers/providers.py), so it must never leak a configured
    secret, even though spark_config() legitimately does (Spark itself
    needs the secret to authenticate — a different, non-public surface)."""
    if case.secret_value is None:
        pytest.skip(f"{case.name} has no static secret configured in this case")
    caps = case.provider.capabilities()
    assert case.secret_value not in repr(caps)
