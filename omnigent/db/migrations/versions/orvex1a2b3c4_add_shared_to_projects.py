"""Orvex: add ``shared`` to projects.

Revision ID: orvex1a2b3c4
Revises: za2b3c4d5e6f
Create Date: 2026-08-17 00:00:00.000000

Adds a NOT NULL ``shared`` boolean to ``projects``, defaulting to false.

Upstream projects are unconditionally owner-private. This fork lets a project
opt in to being *readable* by non-owners (sidebar, project list, ``get``, the
per-project session query, and filing a session into it) while writes stay
owner-only. The flag is the whole of that opt-in.

Additive, and the default is what makes it safe: the server-side default
backfills every pre-existing row to ``false``, so nothing that existed before
this migration becomes visible to anyone it was not already visible to. There
is no data migration and no backfill statement — a column default is the
backfill, and it is the only correct one.

The revision id is deliberately outside upstream's ``<letter>1a2b3c4d5e6``
sequence so an upstream sync that adds its own head cannot collide with it.
Rebasing this onto a newer upstream head means changing ``down_revision``
only.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "orvex1a2b3c4"
down_revision: str | None = "za2b3c4d5e6f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add ``projects.shared``, NOT NULL, defaulting to false."""
    op.add_column(
        "projects",
        # server_default (not just a Python-side default) so the ALTER can add
        # a NOT NULL column to a populated table on both SQLite and Postgres,
        # and so any writer that predates the ORM change still lands ``false``
        # rather than failing the NOT NULL.
        sa.Column("shared", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    """Drop ``projects.shared``.

    Batch mode because SQLite cannot ``ALTER TABLE ... DROP COLUMN`` before
    3.35 — see ``tests/db/test_migrations_sqlite_safe.py``, which fails
    statically on a raw ``op.drop_column``.
    """
    with op.batch_alter_table("projects") as batch_op:
        batch_op.drop_column("shared")
