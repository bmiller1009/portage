"""artifact bindings (spec §51)

Revision ID: d4a1f2b8c3e6
Revises: c898248d6fe3
Create Date: 2026-08-18

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d4a1f2b8c3e6"
down_revision: str | None = "c898248d6fe3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "artifact_bindings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("artifact_name", sa.String(255), nullable=False),
        sa.Column("artifact_version", sa.String(64), nullable=False),
        sa.Column(
            "environment_name",
            sa.String(255),
            sa.ForeignKey("environments.name", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(32), nullable=False, server_default="path"),
        sa.Column("uri", sa.String(2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("artifact_name", "artifact_version", "environment_name"),
    )
    op.create_index("ix_artifact_bindings_artifact_name", "artifact_bindings", ["artifact_name"])
    op.create_index("ix_artifact_bindings_environment_name", "artifact_bindings", ["environment_name"])


def downgrade() -> None:
    op.drop_table("artifact_bindings")
