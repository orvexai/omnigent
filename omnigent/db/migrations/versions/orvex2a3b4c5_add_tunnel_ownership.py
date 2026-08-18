"""Add durable host and runner tunnel ownership records.

Revision ID: orvex2a3b4c5
Revises: orvex1a2b3c4

The records are additive and nullable/inert for old binaries.  A runner
tunnel is keyed by workspace and runner because runner ids are session
affinity keys, not host ids.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "orvex2a3b4c5"
down_revision: str | None = "orvex1a2b3c4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the nullable host owner and runner tunnel ownership table."""
    op.add_column("hosts", sa.Column("owner_addr", sa.String(64), nullable=True))
    op.create_table(
        "runner_tunnels",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False, server_default="0"),
        # Runner ids are opaque wire identifiers; token-bound runners are
        # ``runner_token_<32 hex>`` rather than UUIDs.
        sa.Column("runner_id", sa.String(64), nullable=False),
        sa.Column("owner_addr", sa.String(64), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("workspace_id", "runner_id"),
    )


def downgrade() -> None:
    """Remove the ownership records added by this migration."""
    op.drop_table("runner_tunnels")
    with op.batch_alter_table("hosts") as batch_op:
        batch_op.drop_column("owner_addr")
