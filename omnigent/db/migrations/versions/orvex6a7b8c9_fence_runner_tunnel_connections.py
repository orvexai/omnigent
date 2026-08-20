"""Fence runner tunnel ownership by server-issued connection generations."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "orvex6a7b8c9"
down_revision: str | None = "orvex5a6b7c8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the ownership generation and its durable allocator."""
    op.add_column(
        "runner_tunnels",
        sa.Column(
            "connection_generation",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
    )
    op.create_table(
        "runner_tunnel_generations",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("runner_id", sa.String(64), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("workspace_id", "runner_id"),
    )


def downgrade() -> None:
    """Remove the generation allocator and ownership fence."""
    op.drop_table("runner_tunnel_generations")
    with op.batch_alter_table("runner_tunnels") as batch_op:
        batch_op.drop_column("connection_generation")
