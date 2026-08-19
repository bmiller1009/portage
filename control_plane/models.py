"""SQLAlchemy models for the persistence layer (docs/architecture/spec.md
§27): config/definition CRUD (Environment/ExecutionProfile/StorageProfile/
DatasetBinding/WorkloadDefinition) plus run lifecycle persistence
(Run/ProviderRun/RunEvent/IdempotencyKey) plus the identity-bearing
privileged-action audit trail (AuditEvent, spec §36) plus outbound webhook
subscriptions and their delivery log (WebhookSubscription/WebhookDelivery,
spec §39).
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from control_plane.db import Base


class ExecutionProfile(Base):
    __tablename__ = "execution_profiles"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    provider: Mapped[str] = mapped_column(String(64))
    config: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class StorageProfile(Base):
    __tablename__ = "storage_profiles"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    provider: Mapped[str] = mapped_column(String(64))
    config: Mapped[dict] = mapped_column(JSON)
    # Never a raw secret (spec §35) — a reference to resolve one, e.g.
    # {"provider": "env", "reference": "PORTAGE_MINIO_CREDENTIALS"}.
    credential_reference: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Environment(Base):
    __tablename__ = "environments"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    execution_provider: Mapped[str] = mapped_column(String(64))
    execution_profile_name: Mapped[str] = mapped_column(
        ForeignKey("execution_profiles.name", ondelete="RESTRICT")
    )
    storage_provider: Mapped[str] = mapped_column(String(64))
    storage_profile_name: Mapped[str] = mapped_column(
        ForeignKey("storage_profiles.name", ondelete="RESTRICT")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class DatasetBinding(Base):
    __tablename__ = "dataset_bindings"
    __table_args__ = (UniqueConstraint("dataset_name", "environment_name"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    dataset_name: Mapped[str] = mapped_column(String(255), index=True)
    environment_name: Mapped[str] = mapped_column(
        ForeignKey("environments.name", ondelete="CASCADE"), index=True
    )
    kind: Mapped[str] = mapped_column(String(32), default="path")
    uri: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ArtifactBinding(Base):
    """Artifact repository binding (spec §51) — logical (name, version)
    resolving to a per-environment physical URI, the same shape of
    problem as DatasetBinding above but versioned like
    WorkloadDefinition, since an artifact is a versioned build output."""

    __tablename__ = "artifact_bindings"
    __table_args__ = (UniqueConstraint("artifact_name", "artifact_version", "environment_name"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    artifact_name: Mapped[str] = mapped_column(String(255), index=True)
    artifact_version: Mapped[str] = mapped_column(String(64))
    environment_name: Mapped[str] = mapped_column(
        ForeignKey("environments.name", ondelete="CASCADE"), index=True
    )
    kind: Mapped[str] = mapped_column(String(32), default="path")
    uri: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class WorkloadDefinition(Base):
    __tablename__ = "workload_definitions"
    __table_args__ = (UniqueConstraint("name", "version"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), index=True)
    version: Mapped[str] = mapped_column(String(64))
    # The full validated SparkWorkload (spec/workload/v1alpha1.py), as JSON.
    definition: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# --- Run lifecycle (spec §23-25) -------------------------------------------


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["workload_name", "workload_version"],
            ["workload_definitions.name", "workload_definitions.version"],
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    workload_name: Mapped[str] = mapped_column(String(255))
    workload_version: Mapped[str] = mapped_column(String(64))
    environment_name: Mapped[str] = mapped_column(ForeignKey("environments.name", ondelete="RESTRICT"))
    # Canonical RunState (control_plane/run_state.py), stored as its string value.
    state: Mapped[str] = mapped_column(String(32), index=True)
    # Count of provider.submit() attempts that failed with a
    # RetryableProviderError (spec §26/§67 reconciliation hardening) —
    # bounds retries so a persistently-broken provider doesn't retry
    # forever; incremented, never reset.
    submission_attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ProviderRun(Base):
    """One row per submission attempt for a run — spec §27 keeps this
    separate from `runs` so the canonical run record stays provider-agnostic
    while still keeping a full history of what was actually submitted."""

    __tablename__ = "provider_runs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    provider_run_id: Mapped[str] = mapped_column(String(255))
    provider: Mapped[str] = mapped_column(String(64))
    raw: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RunEvent(Base):
    __tablename__ = "run_events"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    from_state: Mapped[str | None] = mapped_column(String(32), nullable=True)
    to_state: Mapped[str] = mapped_column(String(32))
    message: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class WebhookSubscription(Base):
    """A registered outbound webhook (spec §39 — "very small primitives...
    webhooks/events", deliberately not a general event-bus). event_types
    holds a list of run-lifecycle event names (e.g. "run.succeeded",
    "run.failed") or the single wildcard "run.state_changed"."""

    __tablename__ = "webhook_subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    url: Mapped[str] = mapped_column(String(2048))
    event_types: Mapped[list] = mapped_column(JSON)
    # Used to HMAC-sign delivery payloads (X-Portage-Signature) — never
    # logged or returned by the API after creation.
    secret: Mapped[str] = mapped_column(String(255))
    enabled: Mapped[bool] = mapped_column(default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class WebhookDelivery(Base):
    """One row per (subscription, run-event) match — persisted the same
    way AuditEvent persists outcomes, so a delivery's fate (delivered,
    still pending, or failed past the retry limit) is always inspectable
    rather than a fire-and-forget black box."""

    __tablename__ = "webhook_deliveries"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("webhook_subscriptions.id", ondelete="CASCADE"), index=True
    )
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)
    # "pending" | "delivered" | "failed" (spec §26/§67's own
    # retry-classification discipline, applied here too).
    status: Mapped[str] = mapped_column(String(32), default="pending", server_default="pending", index=True)
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AuditEvent(Base):
    """Every privileged operation (spec §36) — deliberately separate from
    RunEvent, which is a run's own state-transition log, not a general
    identity-bearing audit trail across every kind of privileged action."""

    __tablename__ = "audit_events"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # The caller's subject (email if the token has one, else sub) — never
    # a raw token or any other credential material.
    identity: Mapped[str] = mapped_column(String(255), index=True)
    action: Mapped[str] = mapped_column(String(64), index=True)
    resource: Mapped[str] = mapped_column(String(512))
    environment_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    result: Mapped[str] = mapped_column(String(32))
    # api.auth.Identity.source ("oidc" or "unauthenticated" when
    # PORTAGE_AUTH_MODE=disabled) — never fabricated.
    source: Mapped[str] = mapped_column(String(32))
    correlation_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
