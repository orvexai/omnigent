"""Per-user fan-out for session-list discovery events.

The ``WS /v1/sessions/updates`` push stream is *client-driven*: a browser
watches only the session ids it already has cached, so it can keep those rows
fresh but can never learn about a session created somewhere else (another tab,
the CLI, or one shared with the user) — that id was never in its watch-set.

This module closes that gap with a push instead of a poll. It is a fan-out
broadcaster keyed by a *user key* (the authenticated user id, or a shared
sentinel in single-user mode): when a session becomes accessible to a user, the
HTTP route :func:`publish`es a ``session_added`` event, and every one of that
user's connected updates streams (each an async :func:`subscribe`) wakes and
pushes the new session to its browser. The short-lived announcement queue is
stored in the shared application database, so a subscriber on another replica
observes the same event.

Mirrors :mod:`omnigent.runtime.session_stream` (the per-conversation SSE
broadcaster) but is deliberately minimal: no replay buffer, no end-of-stream
sentinel, no snapshot hooks, and no side-channels. Events emitted while a user
has no stream connected are deleted with the short retention window — that
user's next page load fetches the list over HTTP anyway. When runtime database
wiring is unavailable, the in-process path remains available for embedded and
unit-test deployments.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import threading
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    delete,
    insert,
    select,
)

from omnigent.db.db_models import current_workspace_id
from omnigent.db.utils import get_or_create_engine, make_named_managed_session_maker

_logger = logging.getLogger(__name__)

_SHARED_EVENT_RETENTION_S = 300.0
_SHARED_EVENT_POLL_INTERVAL_S = 0.25

_shared_metadata = MetaData()
_shared_events = Table(
    "omnigent_user_session_stream_events",
    _shared_metadata,
    Column("sequence", Integer, primary_key=True, autoincrement=True),
    Column("workspace_id", Integer, nullable=False),
    Column("event_id", String(32), nullable=False),
    Column("user_key", String(512), nullable=False),
    Column("published_at", BigInteger, nullable=False),
    Column("payload", Text, nullable=False),
)


@dataclass(frozen=True)
class _SharedEventBus:
    """Database-backed announcement transport shared by all replicas."""

    session_maker: Any

    def cursor(self, user_key: str, workspace_id: int) -> int:
        with self.session_maker("cursor_user_session_events") as session:
            row = session.execute(
                select(_shared_events.c.sequence)
                .where(
                    and_(
                        _shared_events.c.workspace_id == workspace_id,
                        _shared_events.c.user_key == user_key,
                    )
                )
                .order_by(_shared_events.c.sequence.desc())
                .limit(1)
            ).first()
        return 0 if row is None else int(row[0])

    def append(self, user_key: str, event: dict[str, Any], workspace_id: int) -> None:
        now = time.time_ns()
        event_id = secrets.token_hex(16)
        payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
        with self.session_maker("publish_user_session_event") as session:
            session.execute(
                insert(_shared_events).values(
                    workspace_id=workspace_id,
                    event_id=event_id,
                    user_key=user_key,
                    published_at=now,
                    payload=payload,
                )
            )
            session.execute(
                delete(_shared_events).where(
                    _shared_events.c.published_at
                    < now - int(_SHARED_EVENT_RETENTION_S * 1_000_000_000),
                )
            )

    def read_after(
        self,
        user_key: str,
        workspace_id: int,
        cursor: int,
    ) -> tuple[list[dict[str, Any]], int]:
        with self.session_maker("read_user_session_events") as session:
            rows = session.execute(
                select(_shared_events.c.sequence, _shared_events.c.payload)
                .where(
                    and_(
                        _shared_events.c.workspace_id == workspace_id,
                        _shared_events.c.user_key == user_key,
                        _shared_events.c.sequence > cursor,
                    )
                )
                .order_by(_shared_events.c.sequence)
                .limit(128)
            ).all()
        events = [json.loads(str(row[1])) for row in rows]
        if not rows:
            return events, cursor
        return events, int(rows[-1][0])


_bus_lock = threading.Lock()
_buses: dict[str, _SharedEventBus] = {}


def _shared_bus() -> _SharedEventBus | None:
    """Return the shared event transport when runtime database wiring exists."""
    try:
        from omnigent.runtime import get_agent_store

        storage_location = get_agent_store().storage_location
    except (AttributeError, RuntimeError):
        return None
    if not isinstance(storage_location, str) or not storage_location:
        return None
    with _bus_lock:
        bus = _buses.get(storage_location)
        if bus is not None:
            return bus
        try:
            engine = get_or_create_engine(storage_location)
            _shared_events.create(engine, checkfirst=True)
            bus = _SharedEventBus(
                make_named_managed_session_maker(
                    engine,
                    query_name_prefix="omnigent.runtime.user_session_stream",
                )
            )
        except Exception:
            _logger.debug("shared user-session event bus unavailable", exc_info=True)
            return None
        _buses[storage_location] = bus
        return bus


# Subscriber registry: user_key -> set of (queue, event_loop) pairs. The loop
# reference lets a publisher running on a different thread/loop deliver into the
# queue's owning loop via ``call_soon_threadsafe`` (matches session_stream).
_subscribers: dict[
    str,
    set[tuple[asyncio.Queue[dict[str, Any]], asyncio.AbstractEventLoop]],
] = {}
_lock = threading.Lock()


def publish(user_key: str, event: dict[str, Any]) -> None:
    """
    Broadcast an event to every active subscriber for ``user_key``.

    No-op when that user has no stream connected (the common case), so callers
    can fire this unconditionally after a grant without checking for listeners.

    :param user_key: The target user's discovery key — the authenticated user
        id (e.g. ``"alice@example.com"``) in multi-user mode, or the shared
        single-user sentinel the updates route also subscribes under.
    :param event: The event dict to deliver, e.g.
        ``{"type": "session_added", "session_id": "conv_abc123"}``.
    """
    bus = _shared_bus()
    if bus is not None:
        try:
            bus.append(user_key, event, current_workspace_id())
            return
        except Exception:
            _logger.warning(
                "shared user-session event publish failed; using local delivery",
                exc_info=True,
            )
    with _lock:
        subs = list(_subscribers.get(user_key, ()))
    for queue, loop in subs:
        loop.call_soon_threadsafe(queue.put_nowait, event)


async def subscribe(user_key: str) -> AsyncIterator[dict[str, Any]]:
    """
    Subscribe to discovery events for ``user_key`` until cancelled.

    Creates an ephemeral queue, registers it, and yields events as they arrive
    from :func:`publish`. Live-tail only — events emitted before this call are
    not replayed. The ``finally`` block always unregisters the slot, so a
    disconnected stream cannot leak a queue. Must be called from the event loop
    the caller iterates on.

    :param user_key: The user's discovery key to subscribe under (see
        :func:`publish`).
    :returns: An async iterator of event dicts, each yielded verbatim.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    loop = asyncio.get_running_loop()
    entry = (queue, loop)
    bus = _shared_bus()
    workspace_id = current_workspace_id()
    cursor = bus.cursor(user_key, workspace_id) if bus is not None else None
    with _lock:
        _subscribers.setdefault(user_key, set()).add(entry)
    try:
        while True:
            if bus is None or cursor is None:
                yield await queue.get()
                continue
            try:
                event = await asyncio.wait_for(
                    queue.get(),
                    timeout=_SHARED_EVENT_POLL_INTERVAL_S,
                )
            except asyncio.TimeoutError:
                try:
                    events, cursor = bus.read_after(user_key, workspace_id, cursor)
                except Exception:
                    _logger.debug("shared user-session event poll failed", exc_info=True)
                    continue
                for event in events:
                    queue.put_nowait(event)
                continue
            yield event
    finally:
        with _lock:
            subs = _subscribers.get(user_key)
            if subs is not None:
                subs.discard(entry)
                if not subs:
                    _subscribers.pop(user_key, None)
