"""S3 (and S3-compatible, e.g. MinIO/VAST-S3) storage provider
(docs/architecture/spec.md §49). The same code path is used against real
AWS S3 and any S3-API-compatible endpoint via a configurable endpoint_url —
spec §49's "S3-compatible implementations through configuration" guidance."""

from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from control_plane.storage_provider import StorageCapabilitySet


@dataclass
class S3ConnectionProfile:
    access_key: str
    secret_key: str
    endpoint_url: str | None = None
    region: str = "us-east-1"
    path_style_access: bool = True


class S3StorageProvider:
    def __init__(self, profile: S3ConnectionProfile):
        self.profile = profile

    def _client(self, *, connect_timeout: float = 60, read_timeout: float = 60):
        return boto3.client(
            "s3",
            endpoint_url=self.profile.endpoint_url,
            aws_access_key_id=self.profile.access_key,
            aws_secret_access_key=self.profile.secret_key,
            region_name=self.profile.region,
            config=Config(
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                retries={"max_attempts": 1},
            ),
        )

    def resolve_uri(self, binding_uri: str) -> str:
        """Dataset path bindings for this provider are already s3a:// URIs —
        resolution is a validating pass-through, not a rewrite."""
        if not binding_uri.startswith(("s3a://", "s3://")):
            raise ValueError(f"S3 provider cannot resolve non-S3 URI: {binding_uri}")
        return binding_uri

    def spark_config(self) -> dict[str, str]:
        """Spark Hadoop S3A configuration for this connection profile (spec §49).
        Includes the hadoop-aws package coordinates needed for s3a:// support,
        since Spark's official images don't bundle it — same version pin the
        Spark Kubernetes Operator's own dfs-read-write.yaml example uses."""
        config = {
            "spark.hadoop.fs.s3a.access.key": self.profile.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.profile.secret_key,
            "spark.hadoop.fs.s3a.path.style.access": str(self.profile.path_style_access).lower(),
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.4.2",
            "spark.jars.ivy": "/tmp/.ivy2",
        }
        if self.profile.endpoint_url:
            config["spark.hadoop.fs.s3a.endpoint"] = self.profile.endpoint_url
            config["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(
                self.profile.endpoint_url.startswith("https://")
            ).lower()
        return config

    def health_check(self) -> bool:
        try:
            self._client(connect_timeout=5, read_timeout=5).list_buckets()
            return True
        except (ClientError, BotoCoreError):
            return False

    def capabilities(self) -> StorageCapabilitySet:
        return StorageCapabilitySet(protocol="s3", path_bindings=True, table_bindings=False)

    def volume_mounts(self) -> list[dict] | None:
        return None
