"""Durable ownership records for live runner WebSocket tunnels."""

from __future__ import annotations

import logging
import math
import os
from typing import Any

from sqlalchemy import (
    BigInteger,
    Column,
    Engine,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    select,
    update,
)

from omnigent.db.db_models import SqlConversationMetadata, current_workspace_id
from omnigent.db.utils import make_named_managed_session_maker, now_epoch

_logger = logging.getLogger(__name__)


def _positive_float_env(name: str, default: float) -> float:
    """Read a positive finite duration, falling back for invalid settings."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if math.isfinite(value) and value > 0 else default


RUNNER_TUNNEL_HEARTBEAT_INTERVAL_S = _positive_float_env(
    "OMNIGENT_RUNNER_TUNNEL_HEARTBEAT_INTERVAL_S",
    10.0,
)
RUNNER_TUNNEL_TTL_S = _positive_float_env("OMNIGENT_RUNNER_TUNNEL_TTL_S", 30.0)

_runner_tunnel_hosts = Table(
    "runner_tunnel_hosts",
    MetaData(),
    Column("workspace_id", BigInteger, nullable=False),
    Column("runner_id", String(64), nullable=False),
    Column("host_id", String(64), nullable=False),
    Column("updated_at", Integer, nullable=False),
)

_runner_tunnels = Table(
    "runner_tunnels",
    MetaData(),
    Column("workspace_id", BigInteger, nullable=False),
    Column("runner_id", String(64), nullable=False),
    Column("owner_addr", String(64), nullable=False),
    Column("updated_at", Integer, nullable=False),
    Column("connection_generation", BigInteger, nullable=False),
)

_runner_tunnel_generations = Table(
    "runner_tunnel_generations",
    MetaData(),
    Column("workspace_id", BigInteger, nullable=False),
    Column("runner_id", String(64), nullable=False),
    Column("generation", BigInteger, nullable=False),
)


class RunnerTunnelStore:
    """Read and compare-and-set runner tunnel ownership in Postgres/SQLite."""

    def __init__(self, engine: Engine) -> None:
        """Create a store sharing the host store's database engine."""
        self._session = make_named_managed_session_maker(
            engine,
            query_name_prefix="omnigent.runner_tunnel_store",
        )

    def allocate_generation(self, runner_id: str) -> int:
        """Allocate the next durable connection generation for a runner."""
        with self._session("allocate_runner_tunnel_generation") as session:
            workspace_id = current_workspace_id()
            dialect = session.bind.dialect.name if session.bind is not None else ""
            if dialect in ("sqlite", "postgresql"):
                stmt: Any
                if dialect == "sqlite":
                    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                    stmt = sqlite_insert(_runner_tunnel_generations)
                else:
                    from sqlalchemy.dialects.postgresql import insert as pg_insert

                    stmt = pg_insert(_runner_tunnel_generations)
                stmt = stmt.values(
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    generation=1,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["workspace_id", "runner_id"],
                    set_={
                        "generation": _runner_tunnel_generations.c.generation + 1,
                    },
                )
                return int(
                    session.execute(
                        stmt.returning(_runner_tunnel_generations.c.generation)
                    ).scalar_one()
                )

            row = session.execute(
                select(_runner_tunnel_generations.c.generation).where(
                    _runner_tunnel_generations.c.workspace_id == workspace_id,
                    _runner_tunnel_generations.c.runner_id == runner_id,
                )
            ).scalar_one_or_none()
            generation = 1 if row is None else int(row) + 1
            if row is None:
                session.execute(
                    _runner_tunnel_generations.insert().values(
                        workspace_id=workspace_id,
                        runner_id=runner_id,
                        generation=generation,
                    )
                )
            else:
                session.execute(
                    update(_runner_tunnel_generations)
                    .where(
                        _runner_tunnel_generations.c.workspace_id == workspace_id,
                        _runner_tunnel_generations.c.runner_id == runner_id,
                    )
                    .values(generation=generation)
                )
            return generation

    def claim(
        self,
        runner_id: str,
        owner_addr: str | None,
        host_id: str | None = None,
        *,
        generation: int | None = None,
    ) -> None:
        """Atomically claim a runner, fenced by its connection generation."""
        if owner_addr is None:
            _logger.warning(
                "runner tunnel ownership claim skipped: runner_id=%s owner_addr=None",
                runner_id,
            )
            return
        if generation is None:
            # Compatibility for callers predating durable fencing. The live
            # WebSocket route always allocates and supplies its token first.
            generation = self.allocate_generation(runner_id)
        with self._session("register_runner_tunnel") as session:
            now = now_epoch()
            workspace_id = current_workspace_id()
            dialect = session.bind.dialect.name if session.bind is not None else ""
            if dialect in ("sqlite", "postgresql"):
                stmt: Any
                if dialect == "sqlite":
                    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                    stmt = sqlite_insert(_runner_tunnels)
                else:
                    from sqlalchemy.dialects.postgresql import insert as pg_insert

                    stmt = pg_insert(_runner_tunnels)
                stmt = stmt.values(
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    owner_addr=owner_addr,
                    updated_at=now,
                    connection_generation=generation,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["workspace_id", "runner_id"],
                    set_={
                        "owner_addr": stmt.excluded.owner_addr,
                        "updated_at": stmt.excluded.updated_at,
                        "connection_generation": stmt.excluded.connection_generation,
                    },
                    where=(
                        stmt.excluded.connection_generation
                        > _runner_tunnels.c.connection_generation
                    ),
                )
                result: Any = session.execute(stmt)
                changed = result.rowcount > 0
            else:
                row = (
                    session.execute(
                        select(_runner_tunnels).where(
                            _runner_tunnels.c.workspace_id == workspace_id,
                            _runner_tunnels.c.runner_id == runner_id,
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    session.execute(
                        _runner_tunnels.insert().values(
                            workspace_id=workspace_id,
                            runner_id=runner_id,
                            owner_addr=owner_addr,
                            updated_at=now,
                            connection_generation=generation,
                        )
                    )
                    changed = True
                elif generation > row["connection_generation"]:
                    session.execute(
                        update(_runner_tunnels)
                        .where(
                            _runner_tunnels.c.workspace_id == workspace_id,
                            _runner_tunnels.c.runner_id == runner_id,
                        )
                        .values(
                            owner_addr=owner_addr,
                            updated_at=now,
                            connection_generation=generation,
                        )
                    )
                    changed = True
                else:
                    changed = False
            if changed and host_id is None:
                host_id = session.execute(
                    select(SqlConversationMetadata.host_id)
                    .where(
                        SqlConversationMetadata.workspace_id == workspace_id,
                        SqlConversationMetadata.runner_id == runner_id,
                        SqlConversationMetadata.host_id.is_not(None),
                    )
                    .limit(1)
                ).scalar_one_or_none()
            if changed and host_id is not None:
                self._upsert_host_attribution(session, runner_id, host_id, now)
        _logger.info(
            "runner tunnel ownership claim applied: runner_id=%s owner_addr=%s "
            "generation=%s host_id=%s changed=%s",
            runner_id,
            owner_addr,
            generation,
            host_id,
            changed,
        )

    def heartbeat(
        self,
        runner_id: str,
        owner_addr: str | None,
        *,
        generation: int | None = None,
    ) -> None:
        """Refresh only the row still owned by this connection generation."""
        if owner_addr is None:
            return
        with self._session("heartbeat_runner_tunnel") as session:
            workspace_id = current_workspace_id()
            if generation is None:
                generation = session.execute(
                    select(_runner_tunnels.c.connection_generation).where(
                        _runner_tunnels.c.workspace_id == workspace_id,
                        _runner_tunnels.c.runner_id == runner_id,
                        _runner_tunnels.c.owner_addr == owner_addr,
                    )
                ).scalar_one_or_none()
            if generation is None:
                return
            session.execute(
                update(_runner_tunnels)
                .where(
                    _runner_tunnels.c.workspace_id == workspace_id,
                    _runner_tunnels.c.runner_id == runner_id,
                    _runner_tunnels.c.owner_addr == owner_addr,
                    _runner_tunnels.c.connection_generation == generation,
                )
                .values(updated_at=now_epoch())
            )

    def release(
        self,
        runner_id: str,
        owner_addr: str | None,
        *,
        generation: int | None = None,
    ) -> None:
        """Delete only this connection generation's runner row."""
        if owner_addr is None:
            return
        with self._session("release_runner_tunnel") as session:
            workspace_id = current_workspace_id()
            if generation is None:
                generation = session.execute(
                    select(_runner_tunnels.c.connection_generation).where(
                        _runner_tunnels.c.workspace_id == workspace_id,
                        _runner_tunnels.c.runner_id == runner_id,
                        _runner_tunnels.c.owner_addr == owner_addr,
                    )
                ).scalar_one_or_none()
            if generation is None:
                return
            session.execute(
                delete(_runner_tunnels).where(
                    _runner_tunnels.c.workspace_id == workspace_id,
                    _runner_tunnels.c.runner_id == runner_id,
                    _runner_tunnels.c.owner_addr == owner_addr,
                    _runner_tunnels.c.connection_generation == generation,
                )
            )

    def owner(self, runner_id: str) -> str | None:
        """Return a fresh durable owner address for a runner, if claimed."""
        with self._session("lookup_runner_tunnel") as session:
            row = session.execute(
                select(_runner_tunnels.c.owner_addr, _runner_tunnels.c.updated_at).where(
                    _runner_tunnels.c.workspace_id == current_workspace_id(),
                    _runner_tunnels.c.runner_id == runner_id,
                )
            ).one_or_none()
            if row is None or row.updated_at < now_epoch() - RUNNER_TUNNEL_TTL_S:
                return None
            return row.owner_addr

    def owner_addr(self, runner_id: str) -> str | None:
        """Return the recorded owner, including a lease that needs review."""
        with self._session("lookup_runner_tunnel_owner") as session:
            owner_addr = session.execute(
                select(_runner_tunnels.c.owner_addr).where(
                    _runner_tunnels.c.workspace_id == current_workspace_id(),
                    _runner_tunnels.c.runner_id == runner_id,
                )
            ).scalar_one_or_none()
        _logger.info(
            "runner tunnel ownership lookup: runner_id=%s owner_addr=%s",
            runner_id,
            owner_addr,
        )
        return owner_addr

    def host_id(self, runner_id: str) -> str | None:
        """Return the durable host that launched or advertised a runner."""
        with self._session("lookup_runner_tunnel_host") as session:
            return session.execute(
                select(_runner_tunnel_hosts.c.host_id).where(
                    _runner_tunnel_hosts.c.workspace_id == current_workspace_id(),
                    _runner_tunnel_hosts.c.runner_id == runner_id,
                )
            ).scalar_one_or_none()

    def _upsert_host_attribution(
        self,
        session: Any,
        runner_id: str,
        host_id: str,
        updated_at: int,
    ) -> None:
        """Persist host attribution without coupling it to the ORM model."""
        dialect = session.bind.dialect.name if session.bind is not None else ""
        values = {
            "workspace_id": current_workspace_id(),
            "runner_id": runner_id,
            "host_id": host_id,
            "updated_at": updated_at,
        }
        if dialect in ("sqlite", "postgresql"):
            stmt: Any
            if dialect == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                stmt = sqlite_insert(_runner_tunnel_hosts)
            else:
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = pg_insert(_runner_tunnel_hosts)
            stmt = stmt.values(**values).on_conflict_do_update(
                index_elements=["workspace_id", "runner_id"],
                set_={
                    "host_id": stmt.excluded.host_id,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            session.execute(stmt)
            return
        changed = session.execute(
            update(_runner_tunnel_hosts)
            .where(
                _runner_tunnel_hosts.c.workspace_id == current_workspace_id(),
                _runner_tunnel_hosts.c.runner_id == runner_id,
            )
            .values(host_id=host_id, updated_at=updated_at)
        ).rowcount
        if not changed:
            session.execute(_runner_tunnel_hosts.insert().values(**values))
