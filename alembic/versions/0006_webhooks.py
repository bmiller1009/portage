"""webhook subscriptions + deliveries (spec §39/§69)

Revision ID: 30a0e524318b
Revises: f6c3b0d8e215
Create Date: 2026-08-19

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "30a0e524318b"
down_revision: str | None = "f6c3b0d8e215"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_subscriptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("url", sa.String(2048), nullable=False),
        sa.Column("event_types", postgresql.JSON, nullable=False),
        sa.Column("secret", sa.String(255), nullable=False),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "webhook_deliveries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "subscription_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("webhook_subscriptions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSON, nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_attempted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_webhook_deliveries_subscription_id", "webhook_deliveries", ["subscription_id"])
    op.create_index("ix_webhook_deliveries_run_id", "webhook_deliveries", ["run_id"])
    op.create_index("ix_webhook_deliveries_status", "webhook_deliveries", ["status"])


def downgrade() -> None:
    op.drop_table("webhook_deliveries")
    op.drop_table("webhook_subscriptions")
