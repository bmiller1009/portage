"""VAST storage provider (docs/architecture/spec.md §48). VAST supports two
conceptual access modes — VAST NFS (Kubernetes-CSI-backed) and VAST S3 —
and this module deliberately does not pretend they're equivalent
internally: NFS mode needs pod volume mounts, not Spark config, so it gets
its own class rather than a `protocol` flag on one shared class.
"""

from dataclasses import asdict, dataclass

from control_plane.storage_provider import StorageCapabilitySet
from providers.storage.s3.provider import S3ConnectionProfile, S3StorageProvider


@dataclass
class VastS3ConnectionProfile:
    access_key: str
    secret_key: str
    endpoint_url: str  # required — unlike S3's optional one, VAST is never "real AWS"
    region: str = "us-east-1"
    path_style_access: bool = True


class VastS3StorageProvider:
    """VAST's S3 mode is genuinely S3-API-compatible (spec §48), so this
    delegates to S3StorageProvider rather than duplicating its logic —
    only capabilities() differs, to report the true protocol rather than
    claiming to be S3 itself."""

    def __init__(self, profile: VastS3ConnectionProfile):
        self.profile = profile
        self._delegate = S3StorageProvider(S3ConnectionProfile(**asdict(profile)))

    def resolve_uri(self, binding_uri: str) -> str:
        return self._delegate.resolve_uri(binding_uri)

    def spark_config(self) -> dict[str, str]:
        return self._delegate.spark_config()

    def health_check(self) -> bool:
        return self._delegate.health_check()

    def capabilities(self) -> StorageCapabilitySet:
        return StorageCapabilitySet(protocol="vast-s3", path_bindings=True, table_bindings=False)

    def volume_mounts(self) -> list[dict] | None:
        return None


@dataclass
class VastNfsConnectionProfile:
    mount_path: str  # where the export appears inside driver/executor containers
    server: str
    export_path: str  # the path exported by the NFS server, e.g. /export/portage


class VastNfsStorageProvider:
    """VAST NFS mode is Kubernetes-CSI-backed (spec §48) — not expressible
    as spark.hadoop.* config at all, so spark_config() is empty and the
    real translation happens in volume_mounts(), consumed by
    KubernetesExecutionProvider.build_spark_application() to add an actual
    pod volume + volumeMount rather than a Spark config key. No real VAST
    NFS server is reachable from this project (no self-hostable
    substitute exists), so health_check() is honestly always False rather
    than faking a positive result."""

    def __init__(self, profile: VastNfsConnectionProfile):
        self.profile = profile

    def resolve_uri(self, binding_uri: str) -> str:
        if not binding_uri.startswith(self.profile.mount_path):
            raise ValueError(
                f"VAST NFS provider cannot resolve URI outside {self.profile.mount_path}: {binding_uri}"
            )
        return binding_uri

    def spark_config(self) -> dict[str, str]:
        return {}

    def health_check(self) -> bool:
        return False

    def capabilities(self) -> StorageCapabilitySet:
        return StorageCapabilitySet(protocol="vast-nfs", path_bindings=True, table_bindings=False)

    def volume_mounts(self) -> list[dict]:
        return [
            {
                "name": "vast-nfs-data",
                "volume": {"nfs": {"server": self.profile.server, "path": self.profile.export_path}},
                "mount_path": self.profile.mount_path,
            }
        ]
