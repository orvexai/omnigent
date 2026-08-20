"""Add the durable cross-replica session-list announcement stream."""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "orvex5a6b7c8"
down_revision: str | None = "orvex4a5b6c7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the shared announcement transport used by all replicas."""
    op.create_table(
        "omnigent_user_session_stream_events",
        sa.Column("sequence", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("workspace_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.String(32), nullable=False),
        sa.Column("publisher_id", sa.String(32), nullable=False),
        sa.Column("user_key", sa.String(512), nullable=False),
        sa.Column("published_at", sa.BigInteger(), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("sequence"),
    )
    op.create_index(
        "ix_omnigent_user_session_stream_events_workspace_sequence",
        "omnigent_user_session_stream_events",
        ["workspace_id", "sequence"],
    )
    op.create_index(
        "ix_omnigent_user_session_stream_events_published_at",
        "omnigent_user_session_stream_events",
        ["published_at"],
    )


def downgrade() -> None:
    """Drop the shared announcement transport."""
    op.drop_index(
        "ix_omnigent_user_session_stream_events_published_at",
        table_name="omnigent_user_session_stream_events",
    )
    op.drop_index(
        "ix_omnigent_user_session_stream_events_workspace_sequence",
        table_name="omnigent_user_session_stream_events",
    )
    op.drop_table("omnigent_user_session_stream_events")
