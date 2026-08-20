"""Add durable server-wide settings.

Revision ID: orvex3a4b5c6
Revises: orvex2a3b4c5
Create Date: 2026-08-20 00:00:00.000000

Adds the shared key/value table used for admin-controlled sharing settings and
the default OIDC allowed-domain additions. The table is additive and starts
empty; the application imports values from the legacy files when a key is
first read or written.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "orvex3a4b5c6"
down_revision: str | None = "orvex2a3b4c5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the empty server-wide settings table."""
    op.create_table(
        "omnigent_server_settings",
        sa.Column("key", sa.String(128), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("key"),
    )


def downgrade() -> None:
    """Drop the server-wide settings table."""
    op.drop_table("omnigent_server_settings")
