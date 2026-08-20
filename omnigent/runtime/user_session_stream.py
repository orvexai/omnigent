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
unit-test deployments. A configured database whose schema is unavailable is an
error, not a reason to silently fall back to per-pod delivery.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import threading
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, cast

from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    delete,
    func,
    insert,
    select,
)
from sqlalchemy.exc import ProgrammingError

from omnigent.db.db_models import current_workspace_id
from omnigent.db.utils import get_or_create_engine, make_named_managed_session_maker

_logger = logging.getLogger(__name__)

_SHARED_EVENT_RETENTION_S = 300.0
_SHARED_EVENT_POLL_INTERVAL_S = 0.25
_SHARED_EVENT_POLL_BACKOFF_MAX_S = 5.0

_shared_metadata = MetaData()
_shared_events = Table(
    "omnigent_user_session_stream_events",
    _shared_metadata,
    Column("sequence", Integer, primary_key=True, autoincrement=True),
    Column("workspace_id", Integer, nullable=False),
    Column("event_id", String(32), nullable=False),
    Column("publisher_id", String(32), nullable=False),
    Column("user_key", String(512), nullable=False),
    Column("published_at", BigInteger, nullable=False),
    Column("payload", Text, nullable=False),
)


@dataclass(frozen=True)
class _SharedEventBus:
    """Database-backed announcement transport shared by all replicas."""

    session_maker: Any
    storage_location: str
    publisher_id: str

    def cursor(self, user_key: str, workspace_id: int) -> int:
        with self.session_maker("cursor_user_session_events") as session:
            row = session.execute(
                select(func.max(_shared_events.c.sequence)).where(
                    _shared_events.c.workspace_id == workspace_id
                )
            ).first()
        return 0 if row is None or row[0] is None else int(row[0])

    def append(self, user_key: str, event: dict[str, Any], workspace_id: int) -> str:
        now = time.time_ns()
        event_id = secrets.token_hex(16)
        payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
        with self.session_maker("publish_user_session_event") as session:
            session.execute(
                insert(_shared_events).values(
                    workspace_id=workspace_id,
                    event_id=event_id,
                    publisher_id=self.publisher_id,
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
        return event_id

    def read_after_all(
        self,
        cursor: int,
    ) -> tuple[list[tuple[int, int, str, str, dict[str, Any]]], int]:
        with self.session_maker("read_user_session_events") as session:
            rows = session.execute(
                select(
                    _shared_events.c.sequence,
                    _shared_events.c.workspace_id,
                    _shared_events.c.publisher_id,
                    _shared_events.c.user_key,
                    _shared_events.c.payload,
                )
                .where(_shared_events.c.sequence > cursor)
                .order_by(_shared_events.c.sequence)
                .limit(128)
            ).all()
        events = [
            (int(row[0]), int(row[1]), str(row[2]), str(row[3]), json.loads(str(row[4])))
            for row in rows
        ]
        if not rows:
            return events, cursor
        return events, int(rows[-1][0])


_bus_lock = threading.Lock()
_buses: dict[str, _SharedEventBus] = {}
_pollers: dict[tuple[str, asyncio.AbstractEventLoop], asyncio.Task[None]] = {}


@dataclass(frozen=True)
class _Subscriber:
    queue: asyncio.Queue[dict[str, Any]]
    loop: asyncio.AbstractEventLoop
    workspace_id: int
    storage_location: str | None


def _shared_bus() -> _SharedEventBus | None:
    """Return the shared event transport when runtime database wiring exists."""
    if not os.environ.get("OMNIGENT_POD_ADDR"):
        return None
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
        engine = get_or_create_engine(storage_location)
        bus = _SharedEventBus(
            make_named_managed_session_maker(
                engine,
                query_name_prefix="omnigent.runtime.user_session_stream",
            ),
            storage_location,
            secrets.token_hex(16),
        )
        _buses[storage_location] = bus
        return bus


# Subscriber registry: user_key -> queue/event-loop records. The loop reference
# lets a publisher on a different thread/loop use ``call_soon_threadsafe``.
_subscribers: dict[
    str,
    set[_Subscriber],
] = {}
_lock = threading.Lock()


def _bus_storage_location(bus: object) -> str:
    storage_location = getattr(bus, "storage_location", None)
    if isinstance(storage_location, str) and storage_location:
        return storage_location
    return f"bus:{id(bus)}"


def _is_schema_error(exc: BaseException) -> bool:
    """Return whether *exc* means the shared announcement schema is invalid."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ProgrammingError) or type(current).__name__ in {
            "SchemaError",
            "UndefinedColumn",
            "UndefinedTable",
            "NoSuchTableError",
        }:
            return True
        message = str(current).lower()
        if "no such table" in message or "does not exist" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _deliver_local(
    user_key: str,
    event: dict[str, Any],
    workspace_id: int,
    storage_location: str | None,
) -> None:
    with _lock:
        subscribers = list(_subscribers.get(user_key, ()))
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None
    for subscriber in subscribers:
        if (
            subscriber.workspace_id != workspace_id
            or subscriber.storage_location != storage_location
        ):
            continue
        if subscriber.loop is running_loop:
            subscriber.queue.put_nowait(event)
        else:
            subscriber.loop.call_soon_threadsafe(subscriber.queue.put_nowait, event)


async def _poll_shared_events(
    bus: Any,
    storage_location: str,
    initial_cursor: int,
) -> None:
    """Read the shared stream once per interval for all local subscribers."""
    loop = asyncio.get_running_loop()
    key = (storage_location, loop)
    cursor = initial_cursor
    try:
        read_after_all = getattr(bus, "read_after_all", None)
        if not callable(read_after_all):
            return
        read_after_all = cast(
            Callable[
                [int],
                tuple[list[tuple[int, int, str, str, dict[str, Any]]], int],
            ],
            read_after_all,
        )
        backoff_s = _SHARED_EVENT_POLL_INTERVAL_S
        while True:
            await asyncio.sleep(_SHARED_EVENT_POLL_INTERVAL_S)
            with _lock:
                active = any(
                    subscriber.storage_location == storage_location
                    for subscribers in _subscribers.values()
                    for subscriber in subscribers
                )
            if not active:
                return
            try:
                rows, cursor = await asyncio.to_thread(read_after_all, cursor)
            except Exception as exc:
                if _is_schema_error(exc):
                    _logger.exception("shared user-session event schema is unavailable")
                    raise
                _logger.exception("shared user-session event poll failed")
                await asyncio.sleep(backoff_s)
                backoff_s = min(_SHARED_EVENT_POLL_BACKOFF_MAX_S, backoff_s * 2)
                continue
            backoff_s = _SHARED_EVENT_POLL_INTERVAL_S
            publisher_id = getattr(bus, "publisher_id", None)
            for _sequence, workspace_id, row_publisher_id, user_key, event in rows:
                if row_publisher_id == publisher_id:
                    continue
                _deliver_local(user_key, event, workspace_id, storage_location)
    finally:
        if _pollers.get(key) is asyncio.current_task():
            _pollers.pop(key, None)


def _ensure_shared_poller(bus: Any, initial_cursor: int) -> None:
    loop = asyncio.get_running_loop()
    storage_location = _bus_storage_location(bus)
    key = (storage_location, loop)
    task = _pollers.get(key)
    if task is None or task.done():
        _pollers[key] = asyncio.create_task(
            _poll_shared_events(bus, storage_location, initial_cursor),
            name="user-session-shared-event-poller",
        )


async def _append_shared_event(
    user_key: str,
    event: dict[str, Any],
    workspace_id: int,
    local_delivery_done: bool,
) -> None:
    """Append an announcement without running database work on the loop."""
    bus = await asyncio.to_thread(_shared_bus)
    if bus is None:
        if not local_delivery_done:
            _deliver_local(user_key, event, workspace_id, None)
        return
    storage_location = _bus_storage_location(bus)
    if not local_delivery_done:
        _deliver_local(user_key, event, workspace_id, storage_location)
    await asyncio.to_thread(bus.append, user_key, event, workspace_id)


def _report_shared_append_failure(task: asyncio.Task[None]) -> None:
    """Log a fire-and-forget announcement failure without killing the loop."""
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        _logger.exception("shared user-session event append failed")


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
    workspace_id = current_workspace_id()
    if not os.environ.get("OMNIGENT_POD_ADDR"):
        _deliver_local(user_key, event, workspace_id, None)
        return

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # Synchronous producers already run outside the server event loop.
        bus = _shared_bus()
        storage_location = _bus_storage_location(bus) if bus is not None else None
        _deliver_local(user_key, event, workspace_id, storage_location)
        if bus is not None:
            bus.append(user_key, event, workspace_id)
        return

    with _lock:
        subscribers = list(_subscribers.get(user_key, ()))
    matching_subscribers = [
        subscriber for subscriber in subscribers if subscriber.workspace_id == workspace_id
    ]
    storage_locations = {subscriber.storage_location for subscriber in matching_subscribers}
    local_delivery_done = bool(matching_subscribers) and len(storage_locations) == 1
    storage_location = next(iter(storage_locations), None)
    _deliver_local(user_key, event, workspace_id, storage_location)
    task = asyncio.create_task(
        _append_shared_event(user_key, event, workspace_id, local_delivery_done),
        name="user-session-shared-event-append",
    )
    task.add_done_callback(_report_shared_append_failure)


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
    workspace_id = current_workspace_id()
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    loop = asyncio.get_running_loop()
    bus = await asyncio.to_thread(_shared_bus) if os.environ.get("OMNIGENT_POD_ADDR") else None
    storage_location = _bus_storage_location(bus) if bus is not None else None
    cursor = (
        await asyncio.to_thread(bus.cursor, user_key, workspace_id) if bus is not None else None
    )
    entry = _Subscriber(queue, loop, workspace_id, storage_location)
    with _lock:
        _subscribers.setdefault(user_key, set()).add(entry)
    if bus is not None and cursor is not None:
        _ensure_shared_poller(bus, cursor)
    try:
        while True:
            yield await queue.get()
    finally:
        with _lock:
            subs = _subscribers.get(user_key)
            if subs is not None:
                subs.discard(entry)
                if not subs:
                    _subscribers.pop(user_key, None)
