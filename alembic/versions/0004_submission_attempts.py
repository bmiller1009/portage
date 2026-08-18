"""run submission_attempts (spec §26/§67 reconciliation hardening)

Revision ID: e5b2a9c7f104
Revises: d4a1f2b8c3e6
Create Date: 2026-08-18

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e5b2a9c7f104"
down_revision: str | None = "d4a1f2b8c3e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runs", sa.Column("submission_attempts", sa.Integer(), nullable=False, server_default="0")
    )


def downgrade() -> None:
    op.drop_column("runs", "submission_attempts")
