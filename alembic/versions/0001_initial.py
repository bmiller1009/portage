"""initial schema: execution_profiles, storage_profiles, environments, dataset_bindings, workload_definitions

Revision ID: ac76bb3cee0c
Revises:
Create Date: 2026-08-17

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "ac76bb3cee0c"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "execution_profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_execution_profiles_name", "execution_profiles", ["name"])

    op.create_table(
        "storage_profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("credential_reference", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_storage_profiles_name", "storage_profiles", ["name"])

    op.create_table(
        "environments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("execution_provider", sa.String(64), nullable=False),
        sa.Column(
            "execution_profile_name",
            sa.String(255),
            sa.ForeignKey("execution_profiles.name", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("storage_provider", sa.String(64), nullable=False),
        sa.Column(
            "storage_profile_name",
            sa.String(255),
            sa.ForeignKey("storage_profiles.name", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_environments_name", "environments", ["name"])

    op.create_table(
        "dataset_bindings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_name", sa.String(255), nullable=False),
        sa.Column(
            "environment_name",
            sa.String(255),
            sa.ForeignKey("environments.name", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(32), nullable=False, server_default="path"),
        sa.Column("uri", sa.String(2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("dataset_name", "environment_name"),
    )
    op.create_index("ix_dataset_bindings_dataset_name", "dataset_bindings", ["dataset_name"])
    op.create_index("ix_dataset_bindings_environment_name", "dataset_bindings", ["environment_name"])

    op.create_table(
        "workload_definitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("version", sa.String(64), nullable=False),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", "version"),
    )
    op.create_index("ix_workload_definitions_name", "workload_definitions", ["name"])


def downgrade() -> None:
    op.drop_table("workload_definitions")
    op.drop_table("dataset_bindings")
    op.drop_table("environments")
    op.drop_table("storage_profiles")
    op.drop_table("execution_profiles")
