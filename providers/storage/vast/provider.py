"""VAST storage provider (docs/architecture/spec.md §48). VAST supports two
conceptual access modes — VAST NFS (Kubernetes-CSI-backed) and VAST S3 —
and this module deliberately does not pretend they're equivalent
internally: NFS mode needs pod volume mounts, not Spark config, so it gets
its own class (added in a later slice) rather than a `protocol` flag on
one shared class.
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
