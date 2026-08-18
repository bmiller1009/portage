"""VAST S3 mode delegates to S3StorageProvider (spec §48), so these tests
focus on the delegation itself and the capability relabeling — the
underlying S3 logic (URI validation, spark_config shape, health checks) is
already covered by tests/unit/test_s3_provider.py."""

import boto3
import pytest
from moto import mock_aws

from providers.storage.vast.provider import (
    VastNfsConnectionProfile,
    VastNfsStorageProvider,
    VastS3ConnectionProfile,
    VastS3StorageProvider,
)


def _profile() -> VastS3ConnectionProfile:
    return VastS3ConnectionProfile(
        access_key="test-access", secret_key="test-secret", endpoint_url="http://vast.local:9000"
    )


def test_resolve_uri_delegates_to_s3():
    provider = VastS3StorageProvider(_profile())
    assert provider.resolve_uri("s3a://bucket/key") == "s3a://bucket/key"


def test_spark_config_includes_vast_endpoint():
    config = VastS3StorageProvider(_profile()).spark_config()
    assert config["spark.hadoop.fs.s3a.endpoint"] == "http://vast.local:9000"
    assert config["spark.hadoop.fs.s3a.access.key"] == "test-access"


@mock_aws
def test_health_check_against_simulated_s3():
    # moto intercepts at botocore's transport layer but still needs a
    # resolvable host to route the request to — "vast.local" isn't one, so
    # this uses AWS's own endpoint (still fully mocked by moto, not a real
    # call) rather than the fixture's placeholder hostname.
    profile = VastS3ConnectionProfile(
        access_key="test-access", secret_key="test-secret", endpoint_url="https://s3.amazonaws.com"
    )
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="portage-test")
    assert VastS3StorageProvider(profile).health_check() is True


def test_capabilities_reports_vast_s3_protocol():
    caps = VastS3StorageProvider(_profile()).capabilities()
    assert caps.protocol == "vast-s3"
    assert caps.path_bindings is True
    assert caps.table_bindings is False


def test_vast_s3_reports_no_volume_mounts():
    assert VastS3StorageProvider(_profile()).volume_mounts() is None


def _nfs_profile() -> VastNfsConnectionProfile:
    return VastNfsConnectionProfile(mount_path="/vast", server="vast.example.com", export_path="/export/portage")


def test_nfs_resolve_uri_accepts_paths_under_mount():
    provider = VastNfsStorageProvider(_nfs_profile())
    assert provider.resolve_uri("/vast/claims/raw") == "/vast/claims/raw"


def test_nfs_resolve_uri_rejects_paths_outside_mount():
    provider = VastNfsStorageProvider(_nfs_profile())
    with pytest.raises(ValueError, match="cannot resolve URI outside"):
        provider.resolve_uri("s3a://bucket/key")


def test_nfs_spark_config_is_empty():
    # Nothing NFS-specific is expressible as sparkConf — see volume_mounts().
    assert VastNfsStorageProvider(_nfs_profile()).spark_config() == {}


def test_nfs_health_check_always_false():
    # No real VAST NFS server is reachable from this project — honest
    # False rather than faking a positive result.
    assert VastNfsStorageProvider(_nfs_profile()).health_check() is False


def test_nfs_capabilities_reports_vast_nfs_protocol():
    caps = VastNfsStorageProvider(_nfs_profile()).capabilities()
    assert caps.protocol == "vast-nfs"


def test_nfs_volume_mounts_shape():
    mounts = VastNfsStorageProvider(_nfs_profile()).volume_mounts()
    assert mounts == [
        {
            "name": "vast-nfs-data",
            "volume": {"nfs": {"server": "vast.example.com", "path": "/export/portage"}},
            "mount_path": "/vast",
        }
    ]
