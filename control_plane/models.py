"""SQLAlchemy models for the persistence layer (docs/architecture/spec.md
§27), scoped to what's needed for Environment/ExecutionProfile/
StorageProfile/Dataset/Workload CRUD. Run/reconciliation tables
(runs, run_events, provider_runs, idempotency_keys) are a later migration —
see the v0.1 backlog issue that adds POST /v1/runs.
"""

import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, String, UniqueConstraint, func
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
