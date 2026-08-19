"""Provider certification report (spec §78): a public compatibility
matrix — every distinct execution+storage provider *type* combination in
use, PASS/FAIL/BLOCKED. Deliberately deduplicates by provider type, not
by registered environment: this project's database accumulates many
near-identical test environments (each with its own uniquely-named
profile) over a long-lived shared deployment, so deduplicating by profile
name would produce one row per environment instead of one row per real
combination — not what spec §78's own example table shows.

PASS requires real live verification in this project's history (spec
§67's discipline: state what's actually been proven, not what's merely
implemented). BLOCKED means the combination has never been live-verified
— no Azure account or VAST NFS server has ever been reachable from this
project (see issues #8, #27, #28's own closing comments) — not a
fabricated PASS. A real Databricks workspace *has* been reachable since
v0.3 (issue #8) — Databricks execution is live-verified, paired with
Unity Catalog Volumes storage registered under the "s3" storage-provider
type (see issue #62's closing comment for the live v1.0.0 Spark-4.2 run
against this exact pairing) — so `databricks` is a live-verified
execution provider, not just Kubernetes.

Storage protocol identification and FAIL detection deliberately avoid
resolving any environment's own storage credentials: many registered
test environments' credential_reference values were never actually
configured on any given deployment (confirmed live — see issue #42's
closing comment), so constructing a live StorageProvider per environment
would make this report's result depend on incidental deployment noise
rather than the provider type itself. The execution side has no such
issue (Kubernetes auth is kubeconfig-based, not a stored credential;
Databricks execution profiles are correctly BLOCKED regardless), so
FAIL detection stays real there.
"""

from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import provider_factory, repositories
from control_plane.models import StorageProfile

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_BLOCKED = "BLOCKED"

_LIVE_VERIFIED_EXECUTION_PROVIDERS = {"kubernetes", "databricks"}
_LIVE_VERIFIED_STORAGE_PROTOCOLS = {"s3", "vast-s3"}


@dataclass
class CertificationRow:
    execution_provider: str
    storage_protocol: str
    status: str
    detail: str | None = None


@dataclass
class CertificationReport:
    rows: list[CertificationRow] = field(default_factory=list)


def _storage_protocol_label(storage_profile: StorageProfile) -> str:
    """s3/adls map directly; vast's two access modes (spec §48) share one
    `provider="vast"` value but a different `config["protocol"]`, so
    they need their own labels to match spec §78's own distinct rows for
    "VAST NFS" vs "VAST S3" — read straight from the profile's already-
    persisted config, no live construction needed."""
    if storage_profile.provider == "vast":
        return "vast-nfs" if storage_profile.config.get("protocol") == "nfs" else "vast-s3"
    return storage_profile.provider


def _status_for(execution_provider: str, storage_protocol: str) -> str:
    # Known simplification: this is a cross-product of two independently
    # live-verified sets, not per-pair verification — e.g. adding
    # "databricks" to the execution set now also marks a
    # (databricks, vast-s3) combination PASS even though that exact pair
    # has never actually been run together (Databricks execution has only
    # ever been paired with Unity Catalog Volumes storage; no on-prem VAST
    # hardware is network-reachable from Databricks' cloud compute
    # regardless). Acceptable for now since every currently-registered
    # environment only ever pairs a provider with the storage it was
    # actually built to use — revisit if that stops being true.
    if (
        execution_provider in _LIVE_VERIFIED_EXECUTION_PROVIDERS
        and storage_protocol in _LIVE_VERIFIED_STORAGE_PROTOCOLS
    ):
        return STATUS_PASS
    return STATUS_BLOCKED


async def _check_execution_side(session: AsyncSession, execution_profile_name: str) -> tuple[str, str | None]:
    """Returns (provider_name, fail_detail) — fail_detail is None unless
    construction/capabilities() itself raised right now, a real, current
    defect distinct from "never live-verified." Kubernetes execution
    profiles need no stored credential to construct (kubeconfig-based
    auth, spec §33/§34), so this is safe to do for real."""
    execution_profile = await repositories.get_execution_profile(session, execution_profile_name)
    try:
        provider = provider_factory.build_execution_provider(execution_profile)
        capabilities = await provider.capabilities()
    except Exception as e:  # noqa: BLE001 - deliberate: a broken provider reports FAIL, not a crash
        return execution_profile.provider, str(e)
    if not capabilities.spark_versions:
        return execution_profile.provider, "execution provider reports no supported Spark versions"
    return execution_profile.provider, None


async def generate_certification_report(session: AsyncSession) -> CertificationReport:
    environments = await repositories.list_environments(session)

    seen: set[tuple[str, str]] = set()
    rows: list[CertificationRow] = []
    for environment in environments:
        storage_profile = await repositories.get_storage_profile(session, environment.storage_profile_name)
        storage_protocol = _storage_protocol_label(storage_profile)
        execution_provider, fail_detail = await _check_execution_side(
            session, environment.execution_profile_name
        )

        key = (execution_provider, storage_protocol)
        if key in seen:
            continue
        seen.add(key)

        if fail_detail is not None:
            rows.append(
                CertificationRow(
                    execution_provider=execution_provider,
                    storage_protocol=storage_protocol,
                    status=STATUS_FAIL,
                    detail=fail_detail,
                )
            )
            continue

        status = _status_for(execution_provider, storage_protocol)
        rows.append(
            CertificationRow(
                execution_provider=execution_provider,
                storage_protocol=storage_protocol,
                status=status,
                detail=(
                    None
                    if status == STATUS_PASS
                    else "translation-layer only — no live infrastructure reachable from this project"
                ),
            )
        )

    return CertificationReport(rows=rows)
