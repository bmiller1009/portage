"""run_events failure taxonomy (v1.0.0 release-hardening — normalized
failure diagnostics)

Revision ID: b1c9e4f7a208
Revises: 30a0e524318b
Create Date: 2026-08-19

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b1c9e4f7a208"
down_revision: str | None = "30a0e524318b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("run_events", sa.Column("category", sa.String(length=32), nullable=True))
    op.add_column("run_events", sa.Column("disposition", sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column("run_events", "disposition")
    op.drop_column("run_events", "category")
