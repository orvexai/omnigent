"""Persist runner-to-host attribution independently of live tunnel leases."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "orvex4a5b6c7"
down_revision: str | None = "orvex3a4b5c6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the durable runner-to-host attribution table."""
    op.create_table(
        "runner_tunnel_hosts",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("runner_id", sa.String(64), nullable=False),
        sa.Column("host_id", sa.String(64), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("workspace_id", "runner_id"),
    )


def downgrade() -> None:
    """Drop durable runner-to-host attribution."""
    op.drop_table("runner_tunnel_hosts")
