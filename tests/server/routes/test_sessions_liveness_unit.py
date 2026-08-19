"""Focused tests for cache-backed session liveness decisions."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, get_args

import pytest


@pytest.fixture()
def two_pod_status_cache() -> Iterator[Any]:
    """Switch the shared cache in place between two simulated pods."""
    from omnigent.server.routes._sessions import common

    global_cache = common._session_status_cache
    pod_caches: dict[str, dict[str, str]] = {"a": {}, "b": {}}
    pod_a_written_keys: set[str] = set()

    @contextmanager
    def enter_pod(name: str) -> Iterator[None]:
        assert name in pod_caches
        global_cache.clear()
        global_cache.update(pod_caches[name])
        if name == "b":
            assert not pod_a_written_keys.intersection(global_cache)
        try:
            yield
        finally:
            pod_caches[name].clear()
            pod_caches[name].update(global_cache)
            if name == "a":
                pod_a_written_keys.update(global_cache)
            global_cache.clear()

    try:
        yield SimpleNamespace(
            cache=global_cache,
            pods=pod_caches,
            enter_pod=enter_pod,
        )
    finally:
        global_cache.clear()


def test_two_pod_status_cache_fixture_isolation(two_pod_status_cache: Any) -> None:
    """The fixture models a cold cache on pod B rather than a patched alias."""
    from omnigent.server.routes._sessions import common, helpers, orchestration

    assert common._session_status_cache is helpers._session_status_cache
    assert common._session_status_cache is orchestration._session_status_cache

    with two_pod_status_cache.enter_pod("a"):
        helpers._publish_status("fixture-meta-session", "running")

    with two_pod_status_cache.enter_pod("b"):
        assert "fixture-meta-session" not in orchestration._session_status_cache
        assert orchestration._session_status_cache == {}


def test_reconciliation_publish_does_not_complete_scheduled_run(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server import session_live_state
    from omnigent.server.routes import sessions

    scheduled_calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        session_live_state,
        "persist_scheduled_run_completion",
        lambda *args, **kwargs: scheduled_calls.append((*args, *kwargs.values())),
    )

    with two_pod_status_cache.enter_pod("a"):
        sessions._publish_status("reconciliation-session", "idle", origin="reconciliation")
        assert scheduled_calls == []

    with two_pod_status_cache.enter_pod("b"):
        assert two_pod_status_cache.cache == {}


def test_edge_idle_still_completes_scheduled_run(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server import session_live_state
    from omnigent.server.routes import sessions

    scheduled_calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        session_live_state,
        "persist_scheduled_run_completion",
        lambda *args, **kwargs: scheduled_calls.append((*args, *kwargs.values())),
    )

    with two_pod_status_cache.enter_pod("a"):
        sessions._publish_status("edge-session", "idle")

    assert scheduled_calls == [("edge-session", "succeeded")]


@pytest.mark.asyncio
async def test_recovery_clears_failed_status_without_scheduled_completion(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server import session_live_state
    from omnigent.server.routes import sessions
    from omnigent.server.routes._sessions import orchestration

    session_id = "recovery-origin-session"
    scheduled_calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        session_live_state,
        "persist_scheduled_run_completion",
        lambda *args, **kwargs: scheduled_calls.append((*args, *kwargs.values())),
    )

    async def no_persist_error(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", no_persist_error)
    conversation = SimpleNamespace(labels={}, live_status="failed")

    with two_pod_status_cache.enter_pod("a"):
        sessions._session_status_cache[session_id] = "failed"
        await sessions._publish_runner_recovered_status(
            session_id,
            SimpleNamespace(),
            conversation=conversation,
        )
        assert sessions._session_status_cache[session_id] == "idle"
        assert scheduled_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runner_status", "expected_cache"),
    [("bogus", None), ("launching", "launching")],
)
async def test_snapshot_validates_runner_status_at_ingress(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
    runner_status: str,
    expected_cache: str | None,
) -> None:
    from omnigent.server import session_live_state
    from omnigent.server.routes import sessions
    from omnigent.server.routes._sessions import orchestration

    class FakeResponse:
        status_code = 200

        def json(self) -> dict[str, str]:
            return {"status": runner_status}

    class FakeRunnerClient:
        async def get(self, url: str, *, timeout: float) -> FakeResponse:
            del url, timeout
            return FakeResponse()

    class FakeStore:
        def get_conversation(self, session_id: str) -> Any:
            return SimpleNamespace(
                id=session_id,
                labels={},
                live_status="idle",
                runner_id=None,
                host_id=None,
            )

        def list_items(self, **kwargs: Any) -> Any:
            del kwargs
            return SimpleNamespace(data=[])

    async def empty_runner_skills(*args: Any) -> list[Any]:
        del args
        return []

    async def empty_model_options(*args: Any) -> list[Any]:
        del args
        return []

    monkeypatch.setattr("omnigent.runtime.get_runner_router", lambda: None)
    monkeypatch.setattr("omnigent.runtime.get_runner_client", FakeRunnerClient)
    monkeypatch.setattr(session_live_state, "persist_live_status", lambda *args: None)
    monkeypatch.setattr(sessions, "session_stream", SimpleNamespace(publish=lambda *args: None))
    monkeypatch.setattr(orchestration, "_fetch_runner_skills", empty_runner_skills)
    monkeypatch.setattr(orchestration, "_fetch_model_options", empty_model_options)
    monkeypatch.setattr(orchestration, "load_session_usage", lambda *args: None)
    monkeypatch.setattr(
        orchestration,
        "_pending_elicitation_snapshot_for_session",
        lambda *args: [],
    )
    monkeypatch.setattr(
        orchestration,
        "_build_session_response",
        lambda conv, items, status, *args, **kwargs: SimpleNamespace(status=status),
    )

    session_id = "snapshot-bogus-status"
    with two_pod_status_cache.enter_pod("b"):
        snapshot = await orchestration._get_session_snapshot(FakeStore(), session_id)
        assert snapshot.status == "idle"
        assert two_pod_status_cache.cache.get(session_id) == expected_cache
        if expected_cache is None:
            assert two_pod_status_cache.cache == {}


@pytest.mark.asyncio
async def test_relay_cache_miss_declines_without_store_read(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server.routes import sessions
    from omnigent.server.routes._sessions import orchestration

    class RaisingStore:
        get_conversation_calls = 0
        set_labels_calls = 0

        def get_conversation(self, session_id: str) -> Any:
            del session_id
            self.get_conversation_calls += 1
            raise AssertionError("disconnect verdict must not read the row")

        def set_labels(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs
            self.set_labels_calls += 1

    async def dropped_stream(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise orchestration._RelayTransportLost(intentional=False)

    monkeypatch.setattr(orchestration, "_relay_runner_stream_once", dropped_stream)
    monkeypatch.setattr(orchestration, "RUNNER_DISCONNECT_GRACE_S", 0.0)
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(
        sessions,
        "session_stream",
        SimpleNamespace(
            publish=lambda session_id, payload: events.append({"id": session_id, **payload})
        ),
    )
    store = RaisingStore()
    session_id = "relay-cache-miss"

    with two_pod_status_cache.enter_pod("b"):
        await orchestration._relay_runner_stream(
            session_id,
            SimpleNamespace(),
            store,  # type: ignore[arg-type]
        )
        assert two_pod_status_cache.cache.get(session_id) is None
        assert two_pod_status_cache.cache == {}

    assert store.get_conversation_calls == 0
    assert store.set_labels_calls == 0
    assert events == []


def _offline_conversation(session_id: str, row_status: str | None) -> Any:
    return SimpleNamespace(
        id=session_id,
        kind="root",
        live_status=row_status,
        labels={},
    )


async def _run_offline_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
    conversation: Any,
    *,
    fail_idle_top_level: bool = False,
) -> tuple[list[dict[str, Any]], list[Any]]:
    from omnigent.server import session_live_state
    from omnigent.server.routes._sessions import orchestration
    from omnigent.server.schemas import ErrorDetail

    events: list[dict[str, Any]] = []
    persisted_errors: list[Any] = []
    from omnigent.server.routes import sessions

    monkeypatch.setattr(
        sessions,
        "session_stream",
        SimpleNamespace(
            publish=lambda session_id, payload: events.append({"id": session_id, **payload})
        ),
    )
    monkeypatch.setattr(session_live_state, "persist_live_status", lambda *args: None)
    monkeypatch.setattr(
        session_live_state,
        "persist_scheduled_run_completion",
        lambda *args, **kwargs: None,
    )

    async def persist_error(session_id: str, error: Any, store: Any) -> None:
        persisted_errors.append((session_id, error, store))

    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", persist_error)
    await orchestration._mark_runner_sessions_offline_impl(
        [conversation],
        ErrorDetail(code="runner_disconnected", message="Runner disconnected unexpectedly."),
        SimpleNamespace(),
        fail_idle_top_level=fail_idle_top_level,
    )
    return events, persisted_errors


@pytest.mark.asyncio
async def test_offline_verdict_idle_cache_does_not_fail_row_running(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server.routes._sessions import orchestration

    session_id = "offline-idle-cache"
    with two_pod_status_cache.enter_pod("a"):
        two_pod_status_cache.cache[session_id] = "idle"
        events, persisted = await _run_offline_reconciliation(
            monkeypatch,
            _offline_conversation(session_id, "running"),
        )
        assert orchestration._session_status_cache.get(session_id) == "idle"
    assert events == []
    assert persisted == []


def test_status_publisher_rejects_invalid_status(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic import ValidationError

    from omnigent.server import session_live_state
    from omnigent.server.routes import sessions
    from omnigent.server.routes._sessions import helpers, orchestration
    from omnigent.server.schemas import SessionStatusEvent

    assert (
        frozenset(get_args(SessionStatusEvent.model_fields["status"].annotation))
        == helpers._SESSION_STATUS_VALUES
    )
    assert orchestration._SESSION_STATUS_VALUES is helpers._SESSION_STATUS_VALUES

    persisted: list[tuple[Any, ...]] = []
    events: list[Any] = []
    monkeypatch.setattr(
        session_live_state,
        "persist_live_status",
        lambda *args: persisted.append(args),
    )
    monkeypatch.setattr(
        sessions,
        "session_stream",
        SimpleNamespace(publish=lambda *args: events.append(args)),
    )
    session_id = "invalid-status-publisher"
    with two_pod_status_cache.enter_pod("a"):
        with pytest.raises(ValidationError):
            sessions._publish_status(session_id, "bogus")
        assert session_id not in two_pod_status_cache.cache

    assert persisted == []
    assert events == []


def test_status_publisher_accepts_launching_status(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server import session_live_state
    from omnigent.server.routes import sessions

    persisted: list[tuple[Any, ...]] = []
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(
        session_live_state,
        "persist_live_status",
        lambda *args: persisted.append(args),
    )
    monkeypatch.setattr(
        sessions,
        "session_stream",
        SimpleNamespace(
            publish=lambda session_id, payload: events.append({"id": session_id, **payload})
        ),
    )

    session_id = "launching-status-publisher"
    with two_pod_status_cache.enter_pod("a"):
        sessions._publish_status(session_id, "launching")
        assert two_pod_status_cache.cache[session_id] == "launching"

    assert persisted == [(session_id, "launching")]
    assert events[0]["status"] == "launching"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runner_status", "expected_cache"),
    [("bogus", None), ("launching", "launching")],
)
async def test_relay_validates_runner_status_at_ingress(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    runner_status: str,
    expected_cache: str | None,
) -> None:
    from omnigent.server import session_live_state
    from omnigent.server.routes import sessions
    from omnigent.server.routes._sessions import orchestration

    class FakeResponse:
        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def aiter_text(self) -> Any:
            yield 'data: {"type": "session.heartbeat"}\n\n'
            yield f'data: {{"type": "session.status", "status": "{runner_status}"}}\n\n'
            yield "data: [DONE]\n\n"

    class FakeRunnerClient:
        def stream(self, *args: Any, **kwargs: Any) -> FakeResponse:
            del args, kwargs
            return FakeResponse()

    persisted: list[tuple[Any, ...]] = []
    events: list[Any] = []
    monkeypatch.setattr(
        session_live_state,
        "persist_live_status",
        lambda *args: persisted.append(args),
    )
    monkeypatch.setattr(
        sessions,
        "session_stream",
        SimpleNamespace(publish=lambda *args: events.append(args)),
    )
    caplog.set_level(logging.WARNING, logger="omnigent.server.routes.sessions")
    session_id = "relay-bogus-status"

    with two_pod_status_cache.enter_pod("b"):
        await orchestration._relay_runner_stream(
            session_id,
            FakeRunnerClient(),  # type: ignore[arg-type]
            SimpleNamespace(),  # type: ignore[arg-type]
        )
        assert two_pod_status_cache.cache.get(session_id) == expected_cache
        if expected_cache is None:
            assert two_pod_status_cache.cache == {}

    if expected_cache is None:
        assert persisted == []
        assert events == []
    else:
        assert persisted
        assert events[0][1]["status"] == expected_cache
    if expected_cache is None:
        assert f"Ignoring invalid runner session status='{runner_status}'" in caplog.text
    else:
        assert f"Ignoring invalid runner session status='{runner_status}'" not in caplog.text


@pytest.mark.asyncio
async def test_offline_verdict_running_cache_fails_row_idle_with_cause(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "offline-running-cache"
    with two_pod_status_cache.enter_pod("a"):
        two_pod_status_cache.cache[session_id] = "running"
        events, persisted = await _run_offline_reconciliation(
            monkeypatch,
            _offline_conversation(session_id, "idle"),
        )
        assert two_pod_status_cache.cache[session_id] == "failed"
    assert events[0]["error"]["code"] == "runner_disconnected"
    assert persisted[0][1].code == "runner_disconnected"


@pytest.mark.asyncio
async def test_tunnel_hook_cache_only_table_and_crash_path(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="omnigent.server.routes.sessions")
    session_id = "offline-tunnel-hook"
    with two_pod_status_cache.enter_pod("b"):
        events, persisted = await _run_offline_reconciliation(
            monkeypatch,
            _offline_conversation(session_id, "running"),
        )
        assert two_pod_status_cache.cache.get(session_id) is None
        assert two_pod_status_cache.cache == {}
    assert events == []
    assert persisted == []
    assert (
        "Offline reconciliation has no local status for session=offline-tunnel-hook; "
        "declining verdict"
    ) in caplog.text

    caplog.clear()
    with two_pod_status_cache.enter_pod("a"):
        two_pod_status_cache.cache[session_id] = "running"
        events, persisted = await _run_offline_reconciliation(
            monkeypatch,
            _offline_conversation(session_id, None),
        )
    assert events[0]["error"]["code"] == "runner_disconnected"
    assert persisted[0][1].code == "runner_disconnected"

    crash_id = "offline-crash-top-level"
    with two_pod_status_cache.enter_pod("b"):
        events, persisted = await _run_offline_reconciliation(
            monkeypatch,
            _offline_conversation(crash_id, "idle"),
            fail_idle_top_level=True,
        )
        assert two_pod_status_cache.cache[crash_id] == "failed"
    assert events[0]["id"] == crash_id
    assert persisted[0][1].code == "runner_disconnected"
    assert "Offline reconciliation has no local status" not in caplog.text


@pytest.mark.asyncio
async def test_tunnel_hook_does_not_repeat_relay_failure(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "offline-already-failed"
    with two_pod_status_cache.enter_pod("a"):
        two_pod_status_cache.cache[session_id] = "failed"
        events, persisted = await _run_offline_reconciliation(
            monkeypatch,
            _offline_conversation(session_id, "running"),
        )
    assert events == []
    assert persisted == []
