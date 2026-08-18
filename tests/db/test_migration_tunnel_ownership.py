"""Reversible migration coverage for durable tunnel ownership records."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from omnigent.db.utils import _build_alembic_config

_REVISION = "orvex2a3b4c5"
_DOWN_REVISION = "orvex1a2b3c4"


def test_tunnel_ownership_migration_round_trips(tmp_path) -> None:
    """Upgrade, observe, downgrade, observe removal, then upgrade again."""
    uri = f"sqlite:///{tmp_path / 'ownership.db'}"
    config: Config = _build_alembic_config(uri)

    command.upgrade(config, _REVISION)
    engine = sa.create_engine(uri)
    try:
        inspector = sa.inspect(engine)
        assert "owner_addr" in {column["name"] for column in inspector.get_columns("hosts")}
        assert "runner_tunnels" in inspector.get_table_names()

        command.downgrade(config, _DOWN_REVISION)
        inspector = sa.inspect(engine)
        assert "owner_addr" not in {column["name"] for column in inspector.get_columns("hosts")}
        assert "runner_tunnels" not in inspector.get_table_names()

        command.upgrade(config, _REVISION)
        inspector = sa.inspect(engine)
        assert "owner_addr" in {column["name"] for column in inspector.get_columns("hosts")}
        assert "runner_tunnels" in inspector.get_table_names()
    finally:
        engine.dispose()
