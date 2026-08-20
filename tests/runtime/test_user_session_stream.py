"""Coverage for cross-replica session-list announcements."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from omnigent.runtime import user_session_stream


class _SharedBus:
    def __init__(self) -> None:
        self.rows: list[tuple[int, dict[str, Any]]] = []

    def cursor(self, _user_key: str, _workspace_id: int) -> int:
        if not self.rows:
            return 0
        sequence, _event = self.rows[-1]
        return sequence

    def append(self, _user_key: str, event: dict[str, Any], _workspace_id: int) -> None:
        self.rows.append((len(self.rows) + 1, event))

    def read_after(
        self,
        _user_key: str,
        _workspace_id: int,
        cursor: int,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [row for row in self.rows if row[0] > cursor]
        if not rows:
            return [], cursor
        return [row[1] for row in rows], rows[-1][0]


@pytest.mark.asyncio
async def test_shared_announcement_reaches_subscribers_on_each_replica(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = _SharedBus()
    monkeypatch.setattr(user_session_stream, "_shared_bus", lambda: bus)
    monkeypatch.setattr(user_session_stream, "_SHARED_EVENT_POLL_INTERVAL_S", 0.01)
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
        user_session_stream.publish(
            "alice",
            {"type": "session_added", "session_id": "conv_shared"},
        )

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
        await stream_a.aclose()
        await stream_b.aclose()
        user_session_stream._subscribers.clear()
