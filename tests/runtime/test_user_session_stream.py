"""Coverage for cross-replica session-list announcements."""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from omnigent.runtime import user_session_stream


class _SharedBus:
    def __init__(self) -> None:
        self.rows: list[tuple[int, int, str, str, dict[str, Any]]] = []
        self.cursor_calls = 0
        self.poll_calls = 0
        self.cursor_threads: list[int] = []
        self.poll_threads: list[int] = []
        self.append_threads: list[int] = []
        self.storage_location = "test-shared-db"
        self.publisher_id = "local"

    def cursor(self, _user_key: str, workspace_id: int) -> int:
        self.cursor_calls += 1
        self.cursor_threads.append(threading.get_ident())
        if not self.rows:
            return 0
        return max(row[0] for row in self.rows if row[1] == workspace_id)

    def append(self, user_key: str, event: dict[str, Any], workspace_id: int) -> str:
        self.append_threads.append(threading.get_ident())
        sequence = len(self.rows) + 1
        self.rows.append((sequence, workspace_id, self.publisher_id, user_key, event))
        return str(sequence)

    def read_after_all(
        self,
        cursor: int,
    ) -> tuple[list[tuple[int, int, str, str, dict[str, Any]]], int]:
        self.poll_calls += 1
        self.poll_threads.append(threading.get_ident())
        rows = [row for row in self.rows if row[0] > cursor]
        if not rows:
            return [], cursor
        return rows, rows[-1][0]


async def _run_in_thread(callable_: Any, *args: Any) -> Any:
    """Test replacement for ``asyncio.to_thread`` with an owned worker."""
    loop = asyncio.get_running_loop()
    result: asyncio.Future[Any] = loop.create_future()

    def finish(value: Any = None, error: BaseException | None = None) -> None:
        if result.done():
            return
        if error is None:
            result.set_result(value)
        else:
            result.set_exception(error)

    def run() -> None:
        try:
            value = callable_(*args)
        except BaseException as exc:
            loop.call_soon_threadsafe(finish, None, exc)
        else:
            loop.call_soon_threadsafe(finish, value)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        return await result
    finally:
        thread.join()


async def _close_streams(streams: list[Any]) -> None:
    await asyncio.gather(*(stream.aclose() for stream in streams), return_exceptions=True)


@pytest.mark.asyncio
async def test_shared_announcement_reaches_subscribers_on_each_replica(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = _SharedBus()
    monkeypatch.setenv("OMNIGENT_POD_ADDR", "pod-a:8000")
    monkeypatch.setattr(user_session_stream, "_shared_bus", lambda: bus)
    monkeypatch.setattr(user_session_stream.asyncio, "to_thread", _run_in_thread)
    user_session_stream._subscribers.clear()

    stream_a = user_session_stream.subscribe("alice")
    stream_b = user_session_stream.subscribe("alice")
    task_a = asyncio.create_task(stream_a.__anext__())
    task_b = asyncio.create_task(stream_b.__anext__())
    try:
        for _ in range(100):
            if len(user_session_stream._subscribers.get("alice", ())) == 2:
                break
            await asyncio.sleep(0.001)
        bus.rows.append(
            (
                1,
                0,
                "remote-pod",
                "alice",
                {"type": "session_added", "session_id": "conv_shared"},
            )
        )
        await asyncio.sleep(0.35)
        assert await asyncio.wait_for(task_a, timeout=2.0) == {
            "type": "session_added",
            "session_id": "conv_shared",
        }
        assert await asyncio.wait_for(task_b, timeout=2.0) == {
            "type": "session_added",
            "session_id": "conv_shared",
        }
    finally:
        for task in (task_a, task_b):
            if not task.done():
                task.cancel()
        await asyncio.gather(task_a, task_b, return_exceptions=True)
        await _close_streams([stream_a, stream_b])
        user_session_stream._subscribers.clear()
        await asyncio.sleep(user_session_stream._SHARED_EVENT_POLL_INTERVAL_S * 2)


def test_shared_event_table_is_migration_owned() -> None:
    migration = Path(
        "omnigent/db/migrations/versions/orvex5a6b7c8_add_user_session_stream_events.py"
    ).read_text()

    assert 'revision: str = "orvex5a6b7c8"' in migration
    assert 'down_revision: str | None = "orvex4a5b6c7"' in migration
    assert '"omnigent_user_session_stream_events"' in migration
    assert ".create(" not in inspect.getsource(user_session_stream._shared_bus)


@pytest.mark.asyncio
async def test_missing_announcement_schema_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OMNIGENT_POD_ADDR", "pod-a:8000")

    def missing_schema() -> Any:
        raise RuntimeError("omnigent_user_session_stream_events is missing")

    monkeypatch.setattr(user_session_stream, "_shared_bus", missing_schema)
    monkeypatch.setattr(user_session_stream.asyncio, "to_thread", _run_in_thread)
    stream = user_session_stream.subscribe("alice")

    with pytest.raises(RuntimeError, match="is missing"):
        await stream.__anext__()
    await _close_streams([stream])
    assert not user_session_stream._subscribers


@pytest.mark.asyncio
async def test_subscriber_database_work_runs_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = _SharedBus()
    monkeypatch.setenv("OMNIGENT_POD_ADDR", "pod-a:8000")
    monkeypatch.setattr(user_session_stream, "_shared_bus", lambda: bus)
    monkeypatch.setattr(user_session_stream.asyncio, "to_thread", _run_in_thread)
    monkeypatch.setattr(user_session_stream, "_SHARED_EVENT_POLL_INTERVAL_S", 0.01)
    user_session_stream._subscribers.clear()
    event_loop_thread = threading.get_ident()
    stream = user_session_stream.subscribe("alice")
    task = asyncio.create_task(stream.__anext__())
    try:
        for _ in range(100):
            if len(user_session_stream._subscribers.get("alice", ())) == 1:
                break
            await asyncio.sleep(0.001)
        assert bus.cursor_threads
        assert all(thread_id != event_loop_thread for thread_id in bus.cursor_threads)

        bus.rows.append((1, 0, "remote-pod", "alice", {"type": "hosts_changed"}))
        await asyncio.sleep(0.35)
        assert await asyncio.wait_for(task, timeout=2.0) == {"type": "hosts_changed"}
        await asyncio.sleep(0)
        assert bus.poll_threads
        assert all(thread_id != event_loop_thread for thread_id in bus.poll_threads)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close_streams([stream])
        user_session_stream._subscribers.clear()
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_publish_database_work_runs_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Announcement INSERT/retention work never runs on the event loop."""
    bus = _SharedBus()
    monkeypatch.setenv("OMNIGENT_POD_ADDR", "pod-a:8000")
    monkeypatch.setattr(user_session_stream, "_shared_bus", lambda: bus)
    monkeypatch.setattr(user_session_stream.asyncio, "to_thread", _run_in_thread)
    event_loop_thread = threading.get_ident()

    user_session_stream.publish("alice", {"type": "hosts_changed"})
    await asyncio.sleep(0.05)

    assert bus.append_threads
    assert all(thread_id != event_loop_thread for thread_id in bus.append_threads)


@pytest.mark.asyncio
async def test_oss_announcement_path_has_no_periodic_database_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default single-replica path remains purely in-process."""
    monkeypatch.delenv("OMNIGENT_POD_ADDR", raising=False)

    def unexpected_shared_bus() -> Any:
        raise AssertionError("OSS announcement path must not initialize the shared bus")

    monkeypatch.setattr(user_session_stream, "_shared_bus", unexpected_shared_bus)
    user_session_stream._subscribers.clear()
    stream = user_session_stream.subscribe("alice")
    task = asyncio.create_task(stream.__anext__())
    try:
        for _ in range(100):
            if len(user_session_stream._subscribers.get("alice", ())) == 1:
                break
            await asyncio.sleep(0.001)
        user_session_stream.publish("alice", {"type": "hosts_changed"})
        assert await asyncio.wait_for(task, timeout=1.0) == {"type": "hosts_changed"}
        print("announcement query rate bus=disabled rate=0.0/s calls=0")
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close_streams([stream])
        user_session_stream._subscribers.clear()


@pytest.mark.asyncio
async def test_shared_poller_retries_transient_error_and_delivers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient poll failure does not strand existing subscribers."""

    class _FlakyBus(_SharedBus):
        def __init__(self) -> None:
            super().__init__()
            self.failures = 1

        def read_after_all(
            self,
            cursor: int,
        ) -> tuple[list[tuple[int, int, str, str, dict[str, Any]]], int]:
            if self.failures:
                self.failures -= 1
                raise RuntimeError("connection refused")
            return super().read_after_all(cursor)

    bus = _FlakyBus()
    bus.rows.append((1, 0, "remote-pod", "alice", {"type": "hosts_changed"}))
    monkeypatch.setattr(user_session_stream, "_SHARED_EVENT_POLL_INTERVAL_S", 0.001)
    monkeypatch.setattr(user_session_stream.asyncio, "to_thread", _run_in_thread)
    user_session_stream._subscribers.clear()
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    user_session_stream._subscribers["alice"] = {
        user_session_stream._Subscriber(queue, loop, 0, bus.storage_location)
    }
    poller = asyncio.create_task(
        user_session_stream._poll_shared_events(bus, bus.storage_location, 0)
    )
    try:
        assert await asyncio.wait_for(queue.get(), timeout=1.0) == {"type": "hosts_changed"}
        assert bus.failures == 0
        assert bus.poll_calls >= 1
        assert not poller.done()
    finally:
        poller.cancel()
        await asyncio.gather(poller, return_exceptions=True)
        user_session_stream._subscribers.clear()


@pytest.mark.asyncio
async def test_shared_poller_schema_error_still_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing announcement schema remains a loud, non-retryable failure."""

    class SchemaError(RuntimeError):
        pass

    class _BrokenBus(_SharedBus):
        def read_after_all(
            self,
            cursor: int,
        ) -> tuple[list[tuple[int, int, str, str, dict[str, Any]]], int]:
            del cursor
            raise SchemaError("announcement schema is missing")

    bus = _BrokenBus()
    monkeypatch.setattr(user_session_stream, "_SHARED_EVENT_POLL_INTERVAL_S", 0.001)
    monkeypatch.setattr(user_session_stream.asyncio, "to_thread", _run_in_thread)
    user_session_stream._subscribers.clear()
    loop = asyncio.get_running_loop()
    user_session_stream._subscribers["alice"] = {
        user_session_stream._Subscriber(asyncio.Queue(), loop, 0, bus.storage_location)
    }
    poller = asyncio.create_task(
        user_session_stream._poll_shared_events(bus, bus.storage_location, 0)
    )
    try:
        with pytest.raises(SchemaError, match="schema is missing"):
            await poller
    finally:
        if not poller.done():
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
        user_session_stream._subscribers.clear()


@pytest.mark.asyncio
@pytest.mark.parametrize("subscriber_count", [1, 3, 100])
async def test_shared_poller_query_rate_is_constant(
    monkeypatch: pytest.MonkeyPatch,
    subscriber_count: int,
) -> None:
    bus = _SharedBus()
    monkeypatch.setenv("OMNIGENT_POD_ADDR", "pod-a:8000")
    interval = user_session_stream._SHARED_EVENT_POLL_INTERVAL_S
    monkeypatch.setattr(user_session_stream, "_shared_bus", lambda: bus)

    async def run_without_default_executor(callable_: Any, *args: Any) -> Any:
        # Frequency is measured independently from the event-loop blocking
        # check below; the sandbox's default executor is not deterministic.
        await asyncio.sleep(0)
        return callable_(*args)

    monkeypatch.setattr(user_session_stream.asyncio, "to_thread", run_without_default_executor)
    user_session_stream._subscribers.clear()
    loop = asyncio.get_running_loop()
    entries = {
        user_session_stream._Subscriber(asyncio.Queue(), loop, 0, bus.storage_location)
        for _ in range(subscriber_count)
    }
    user_session_stream._subscribers["alice"] = entries
    poller = asyncio.create_task(
        user_session_stream._poll_shared_events(bus, bus.storage_location, 0)
    )
    started = time.monotonic()
    try:
        await asyncio.sleep(interval * 8)
        elapsed = time.monotonic() - started
        before_rate = (1.0 / interval) * subscriber_count
        after_rate = bus.poll_calls / elapsed
        print(
            f"announcement query rate subscribers={subscriber_count}: "
            f"before={before_rate:.1f}/s after={after_rate:.1f}/s "
            f"calls={bus.poll_calls} elapsed={elapsed:.2f}s"
        )
        assert 1 <= bus.poll_calls <= 8
    finally:
        poller.cancel()
        await asyncio.gather(poller, return_exceptions=True)
        user_session_stream._subscribers.clear()
