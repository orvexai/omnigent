"""Durable ownership records for live runner WebSocket tunnels."""

from __future__ import annotations

from sqlalchemy import Engine, delete, update

from omnigent.db.db_models import SqlRunnerTunnel, current_workspace_id
from omnigent.db.utils import make_named_managed_session_maker, now_epoch


class RunnerTunnelStore:
    """Read and compare-and-set runner tunnel ownership in Postgres/SQLite."""

    def __init__(self, engine: Engine) -> None:
        """Create a store sharing the host store's database engine."""
        self._session = make_named_managed_session_maker(
            engine,
            query_name_prefix="omnigent.runner_tunnel_store",
        )

    def claim(self, runner_id: str, owner_addr: str | None) -> None:
        """Claim a runner on connect; ``None`` keeps Stage 1 inert."""
        if owner_addr is None:
            return
        with self._session("register_runner_tunnel") as session:
            row = session.get(SqlRunnerTunnel, (current_workspace_id(), runner_id))
            if row is None:
                session.add(
                    SqlRunnerTunnel(
                        runner_id=runner_id,
                        owner_addr=owner_addr,
                        updated_at=now_epoch(),
                    )
                )
            else:
                row.owner_addr = owner_addr
                row.updated_at = now_epoch()

    def heartbeat(self, runner_id: str, owner_addr: str | None) -> None:
        """Refresh only the row still owned by this pod."""
        if owner_addr is None:
            return
        with self._session("heartbeat_runner_tunnel") as session:
            session.execute(
                update(SqlRunnerTunnel)
                .where(
                    SqlRunnerTunnel.workspace_id == current_workspace_id(),
                    SqlRunnerTunnel.runner_id == runner_id,
                    SqlRunnerTunnel.owner_addr == owner_addr,
                )
                .values(updated_at=now_epoch())
            )

    def release(self, runner_id: str, owner_addr: str | None) -> None:
        """Delete only this pod's runner row; stale pods are no-ops."""
        if owner_addr is None:
            return
        with self._session("release_runner_tunnel") as session:
            session.execute(
                delete(SqlRunnerTunnel).where(
                    SqlRunnerTunnel.workspace_id == current_workspace_id(),
                    SqlRunnerTunnel.runner_id == runner_id,
                    SqlRunnerTunnel.owner_addr == owner_addr,
                )
            )

    def owner(self, runner_id: str) -> str | None:
        """Return the durable owner address for a runner, if claimed."""
        with self._session("lookup_runner_tunnel") as session:
            row = session.get(SqlRunnerTunnel, (current_workspace_id(), runner_id))
            return row.owner_addr if row is not None else None
