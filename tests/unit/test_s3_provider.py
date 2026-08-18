import boto3
import pytest
from moto import mock_aws

from providers.storage.s3.provider import S3ConnectionProfile, S3StorageProvider


@pytest.fixture
def profile() -> S3ConnectionProfile:
    return S3ConnectionProfile(access_key="test-access", secret_key="test-secret")


def test_resolve_uri_passes_through_valid_s3_uris(profile):
    provider = S3StorageProvider(profile)
    assert provider.resolve_uri("s3a://bucket/key") == "s3a://bucket/key"
    assert provider.resolve_uri("s3://bucket/key") == "s3://bucket/key"


def test_resolve_uri_rejects_non_s3_uris(profile):
    provider = S3StorageProvider(profile)
    with pytest.raises(ValueError, match="cannot resolve non-S3 URI"):
        provider.resolve_uri("/vast/data/claims/raw")


def test_spark_config_includes_endpoint_when_set():
    profile = S3ConnectionProfile(
        access_key="a", secret_key="b", endpoint_url="http://minio.local:9000"
    )
    config = S3StorageProvider(profile).spark_config()

    assert config["spark.hadoop.fs.s3a.endpoint"] == "http://minio.local:9000"
    assert config["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert config["spark.hadoop.fs.s3a.path.style.access"] == "true"


def test_spark_config_omits_endpoint_for_real_aws(profile):
    config = S3StorageProvider(profile).spark_config()
    assert "spark.hadoop.fs.s3a.endpoint" not in config


@mock_aws
def test_health_check_against_simulated_s3(profile):
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="portage-test")
    provider = S3StorageProvider(profile)
    assert provider.health_check() is True


def test_health_check_returns_false_for_unreachable_endpoint():
    # Port 1 is not listening, so connection is refused immediately —
    # deliberately not wrapped in @mock_aws, since moto would intercept
    # the request and defeat the point of this test.
    profile = S3ConnectionProfile(
        access_key="a", secret_key="b", endpoint_url="http://127.0.0.1:1"
    )
    assert S3StorageProvider(profile).health_check() is False


def test_capabilities_declares_path_bindings_only(profile):
    caps = S3StorageProvider(profile).capabilities()
    assert caps.protocol == "s3"
    assert caps.path_bindings is True
    assert caps.table_bindings is False
