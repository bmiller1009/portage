"""The StorageProvider protocol every storage provider implements
(docs/architecture/spec.md §47, ADR 0005: "storage providers follow the
same pattern" as execution providers). Formalizes what was, until now, an
informal contract only S3StorageProvider happened to follow.
"""

from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass
class StorageCapabilitySet:
    """Storage provider capability declaration (spec §47's "capability
    declaration" responsibility).

    `verification`: "live_verified" (S3, VAST S3-mode) has actually moved
    real data against real infrastructure; "translation_layer_only" (ADLS,
    VAST NFS-mode) is tested against fakes/unit tests only — no Azure
    subscription or VAST hardware available to this project. Same two-tier
    model as CapabilitySet.verification (control_plane/execution_provider.py)
    for the same reason — this project's own honest evidence only supports
    two tiers today, not a finer-grained certification scale.
    """

    protocol: str
    path_bindings: bool
    table_bindings: bool
    verification: Literal["live_verified", "translation_layer_only"] = "live_verified"


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
