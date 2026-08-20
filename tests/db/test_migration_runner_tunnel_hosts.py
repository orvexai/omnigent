"""Migration coverage for durable runner-to-host attribution."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from omnigent.db.utils import _build_alembic_config

_REVISION = "orvex4a5b6c7"
_DOWN_REVISION = "orvex3a4b5c6"


def test_runner_tunnel_hosts_migration_round_trips(tmp_path) -> None:
    """Upgrade and downgrade create and remove the attribution table."""
    uri = f"sqlite:///{tmp_path / 'runner-tunnel-hosts.db'}"
    config: Config = _build_alembic_config(uri)

    command.upgrade(config, _REVISION)
    engine = sa.create_engine(uri)
    try:
        assert "runner_tunnel_hosts" in sa.inspect(engine).get_table_names()
        command.downgrade(config, _DOWN_REVISION)
        assert "runner_tunnel_hosts" not in sa.inspect(engine).get_table_names()
    finally:
        engine.dispose()
