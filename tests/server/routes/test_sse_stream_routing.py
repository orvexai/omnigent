"""Coverage for session-stream ownership and reconnect classification."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from omnigent.errors import ErrorCode, OmnigentError
from omnigent.runtime import session_stream
from omnigent.server.routes._sessions import helpers
from omnigent.server.routes.sessions import _stream_live_events
from omnigent.server.routes.sessions.routes_events import _ensure_stream_owner


class _RunnerRouter:
    def runner_owner_addr(self, runner_id: str) -> str:
        assert runner_id == "runner_remote"
        return "10.0.0.7:8000"


class _OfflineRunnerRouter:
    def runner_owner_addr(self, runner_id: str) -> None:
        assert runner_id == "runner_offline"
        return


def _request() -> Request:
    app = SimpleNamespace(state=SimpleNamespace(pod_addr="10.0.0.9:8000"))
    return Request({"type": "http", "app": app})


def _offline_request() -> Request:
    state = SimpleNamespace(
        pod_addr="10.0.0.9:8000",
        host_registry=SimpleNamespace(get=lambda _host_id: None),
        host_store=SimpleNamespace(get_host=lambda _host_id: None),
    )
    return Request({"type": "http", "app": SimpleNamespace(state=state)})


class _ConnectedRequest:
    async def is_disconnected(self) -> bool:
        return False


@pytest.mark.asyncio
async def test_non_owner_session_stream_is_wrong_replica_not_hollow_200() -> None:
    conversation = SimpleNamespace(runner_id="runner_remote", host_id="host_local")

    with pytest.raises(OmnigentError) as exc_info:
        await _ensure_stream_owner(
            _request(),
            conversation,
            runner_client=None,
            runner_router=_RunnerRouter(),  # type: ignore[arg-type]
        )

    assert exc_info.value.code == ErrorCode.WRONG_REPLICA
    assert exc_info.value.owner_addr == "10.0.0.7:8000"


@pytest.mark.asyncio
async def test_offline_runner_stream_opens_and_replays_history_without_done() -> None:
    """An offline host does not block history reconciliation for its sessions."""
    conversation = SimpleNamespace(runner_id="runner_offline", host_id="host_offline")

    await _ensure_stream_owner(
        _offline_request(),
        conversation,
        runner_client=None,
        runner_router=_OfflineRunnerRouter(),  # type: ignore[arg-type]
    )

    async def history_snapshot() -> list[dict[str, str]]:
        return [
            {
                "type": "session.changed_files.invalidated",
                "session_id": "conv_offline",
                "environment_id": "default",
            }
        ]

    session_stream._subscribers.clear()
    stream = _stream_live_events(
        _ConnectedRequest(),
        "conv_offline",
        history_snapshot,
    )
    try:
        ready = await asyncio.wait_for(stream.__anext__(), timeout=2.0)
        history = await asyncio.wait_for(stream.__anext__(), timeout=2.0)

        assert "session.heartbeat" in ready
        assert "session.changed_files.invalidated" in history
        assert "[DONE]" not in ready
        assert "[DONE]" not in history
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_runner_rehome_ends_stream_without_done_for_client_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = {"addr": "10.0.0.9:8000"}

    def binding(_conversation_id: str) -> tuple[str, str]:
        return "runner_rehomed", owner["addr"]

    monkeypatch.setattr(session_stream, "_runner_binding_for", binding)
    monkeypatch.setattr(helpers, "_SESSION_STREAM_HEARTBEAT_INTERVAL_S", 0.001)
    session_stream._subscribers.clear()

    stream = _stream_live_events(_ConnectedRequest(), "conv_rehomed")
    first = await asyncio.wait_for(stream.__anext__(), timeout=2.0)
    assert "session.heartbeat" in first

    owner["addr"] = "10.0.0.7:8000"
    frames = [frame async for frame in stream]

    assert all("[DONE]" not in frame for frame in frames)
    assert "conv_rehomed" not in session_stream._subscribers


@pytest.mark.asyncio
async def test_managed_launch_remote_binding_ends_stream_for_client_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = {"value": None}

    monkeypatch.setattr(
        session_stream, "_runner_binding_for", lambda _conversation_id: binding["value"]
    )
    monkeypatch.setattr(
        "omnigent.runtime.get_runner_router",
        lambda: SimpleNamespace(_pod_addr="10.0.0.9:8000"),
    )
    monkeypatch.setattr(helpers, "_SESSION_STREAM_HEARTBEAT_INTERVAL_S", 0.001)
    session_stream._subscribers.clear()

    stream = _stream_live_events(_ConnectedRequest(), "conv_managed")
    first = await asyncio.wait_for(stream.__anext__(), timeout=2.0)
    assert "session.heartbeat" in first

    # Managed provisioning commits the runner after the browser subscribed,
    # and this replica is not the durable owner.
    binding["value"] = ("runner_managed", "10.0.0.7:8000")
    frames = [frame async for frame in stream]

    assert all("[DONE]" not in frame for frame in frames)
    assert "conv_managed" not in session_stream._subscribers


@pytest.mark.asyncio
async def test_unbound_session_uses_host_owner_fallback() -> None:
    host = SimpleNamespace(owner_addr="10.0.0.7:8000", updated_at=10**18, status="online")
    conversation = SimpleNamespace(runner_id=None, host_id="host_remote")
    state = SimpleNamespace(
        pod_addr="10.0.0.9:8000",
        host_registry=SimpleNamespace(get=lambda _host_id: None),
        host_store=SimpleNamespace(get_host=lambda _host_id: host),
    )
    request = Request({"type": "http", "app": SimpleNamespace(state=state)})

    with pytest.raises(OmnigentError) as exc_info:
        await _ensure_stream_owner(request, conversation, None, None)

    assert exc_info.value.code == ErrorCode.WRONG_REPLICA
