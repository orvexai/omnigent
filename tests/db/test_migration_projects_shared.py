"""Orvex — tests for ``orvex1a2b3c4``, which adds ``projects.shared``.

The migration is the whole of AC1: the column must exist, be NOT NULL, default
to false, and — the part that actually matters for security — leave every row
that existed before it strictly private. A migration that defaulted the column
to true, or left it NULL and let a downstream ``bool(None)`` decide, would
publish every project on the server the moment it ran.

The downgrade is exercised too, because an Orvex-only revision has to be
removable when the branch rebases onto a new upstream head.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.engine import Engine

from omnigent.db.utils import (
    _build_alembic_config,
    clear_engine_cache,
    get_or_create_engine,
)

# The upstream head this revision sits on top of — i.e. the schema as it was
# before projects could be shared at all.
_BEFORE_SHARED = "za2b3c4d5e6f"

_PROJECT_ID = "beefbeefbeefbeefbeefbeefbeefbeef"


@pytest.fixture
def db_engine(tmp_path: Path) -> Iterator[Engine]:
    """Fresh SQLite database migrated to head."""
    db_path = tmp_path / "shared.db"
    engine = get_or_create_engine(f"sqlite:///{db_path}")
    try:
        yield engine
    finally:
        clear_engine_cache()


def test_shared_column_exists_at_head(db_engine: Engine) -> None:
    """``projects.shared`` is present, boolean and NOT NULL at head."""
    columns = {c["name"]: c for c in sa.inspect(db_engine).get_columns("projects")}
    assert "shared" in columns, f"Expected projects.shared; found {set(columns)}"
    shared = columns["shared"]
    assert isinstance(shared["type"], sa.Boolean), (
        f"projects.shared should be BOOLEAN, got {shared['type']!r}"
    )
    assert not shared["nullable"], "projects.shared must be NOT NULL — NULL is not 'private'"


def test_shared_defaults_to_false_for_a_row_that_names_no_value(db_engine: Engine) -> None:
    """An INSERT that never mentions ``shared`` lands private.

    This is the server-side default doing its job. A writer that predates the
    ORM change — or a hand-written INSERT — must not be able to create a
    project that is visible to the whole workspace by omission.
    """
    with db_engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO projects (workspace_id, id, name, user_id, created_at, updated_at) "
                f"VALUES (0, X'{_PROJECT_ID}', 'legacy', 'alice@example.com', 1700000000, NULL)"
            )
        )
    with db_engine.connect() as conn:
        shared = conn.execute(
            sa.text(f"SELECT shared FROM projects WHERE id = X'{_PROJECT_ID}'")
        ).scalar_one()
    assert not shared, f"a project created without naming `shared` must be private; got {shared!r}"


def test_pre_existing_projects_are_private_after_the_upgrade(tmp_path: Path) -> None:
    """A project that existed BEFORE the migration reads back ``shared = false``.

    Seeds the row at the previous head — a schema with no ``shared`` column at
    all — then upgrades. This is the AC1 clause that protects every project
    already on the live server: the migration must not publish any of them.
    """
    uri = f"sqlite:///{tmp_path / 'backfill.db'}"
    engine = get_or_create_engine(uri)
    config = _build_alembic_config(uri)

    # Rewind to before the column existed, then seed as the old code would have.
    with engine.begin() as conn:
        config.attributes["connection"] = conn
        command.downgrade(config, _BEFORE_SHARED)

    inspector = sa.inspect(engine)
    assert "shared" not in {c["name"] for c in inspector.get_columns("projects")}, (
        "precondition: the previous head must have no `shared` column"
    )

    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO projects (workspace_id, id, name, user_id, created_at, updated_at) "
                f"VALUES (0, X'{_PROJECT_ID}', 'pre-existing', 'alice@example.com', "
                "1700000000, NULL)"
            )
        )

    with engine.begin() as conn:
        config.attributes["connection"] = conn
        command.upgrade(config, "head")

    with engine.connect() as conn:
        row = conn.execute(
            sa.text(f"SELECT name, shared FROM projects WHERE id = X'{_PROJECT_ID}'")
        ).one()
    assert row.name == "pre-existing", "the row must survive the upgrade"
    assert not row.shared, (
        f"a project that predates the migration must be private; got shared={row.shared!r}"
    )

    engine.dispose()
    clear_engine_cache()


def test_downgrade_drops_the_column_and_re_upgrade_restores_it(tmp_path: Path) -> None:
    """The revision round-trips, so the branch can be rebased or rolled back."""
    uri = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    engine = get_or_create_engine(uri)
    config = _build_alembic_config(uri)

    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO projects "
                "(workspace_id, id, name, user_id, created_at, updated_at, shared) "
                f"VALUES (0, X'{_PROJECT_ID}', 'fleet', 'alice@example.com', 1700000000, NULL, 1)"
            )
        )

    with engine.begin() as conn:
        config.attributes["connection"] = conn
        command.downgrade(config, _BEFORE_SHARED)

    cols = {c["name"] for c in sa.inspect(engine).get_columns("projects")}
    assert "shared" not in cols, f"downgrade must drop projects.shared; found {cols}"
    with engine.connect() as conn:
        name = conn.execute(
            sa.text(f"SELECT name FROM projects WHERE id = X'{_PROJECT_ID}'")
        ).scalar_one()
    assert name == "fleet", "the row itself must survive the batch table rebuild"

    with engine.begin() as conn:
        config.attributes["connection"] = conn
        command.upgrade(config, "head")

    with engine.connect() as conn:
        shared = conn.execute(
            sa.text(f"SELECT shared FROM projects WHERE id = X'{_PROJECT_ID}'")
        ).scalar_one()
    # The flag itself does NOT survive a round trip — the column was dropped —
    # and the re-upgrade must land on the safe side of that loss.
    assert not shared, "re-upgrade must restore the column private, not shared"

    engine.dispose()
    clear_engine_cache()
