"""The StorageProvider protocol every storage provider implements
(docs/architecture/spec.md §47, ADR 0005: "storage providers follow the
same pattern" as execution providers). Formalizes what was, until now, an
informal contract only S3StorageProvider happened to follow.
"""

from dataclasses import dataclass
from typing import Protocol


@dataclass
class StorageCapabilitySet:
    """Storage provider capability declaration (spec §47's "capability
    declaration" responsibility)."""

    protocol: str
    path_bindings: bool
    table_bindings: bool


class StorageProvider(Protocol):
    def resolve_uri(self, binding_uri: str) -> str: ...
    def spark_config(self) -> dict[str, str]: ...
    def health_check(self) -> bool: ...
    def capabilities(self) -> StorageCapabilitySet: ...
    def volume_mounts(self) -> list[dict] | None:
        """Pod volume mounts this provider needs, if any (spec §48 — VAST
        NFS access is Kubernetes-CSI-backed, not expressible as Spark
        config). None for every provider except VAST NFS mode. Protocol
        conformance is structural, not inherited — every concrete
        provider still implements this itself (trivially, returning None,
        for everything except VastNfsStorageProvider)."""
        ...
