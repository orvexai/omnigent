"""Focused tests for cache-backed session liveness decisions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

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
    from omnigent.server.routes._sessions import common

    with two_pod_status_cache.enter_pod("a"):
        common._session_status_cache["fixture-meta-session"] = "running"

    with two_pod_status_cache.enter_pod("b"):
        assert "fixture-meta-session" not in common._session_status_cache
        assert common._session_status_cache == {}


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

    session_id = "recovery-origin-session"
    scheduled_calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        session_live_state,
        "persist_scheduled_run_completion",
        lambda *args, **kwargs: scheduled_calls.append((*args, *kwargs.values())),
    )
    monkeypatch.setattr(
        sessions,
        "_persist_session_status_error_labels",
        lambda *args, **kwargs: None,
    )
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
async def test_snapshot_drops_unvalidated_runner_status(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server.routes._sessions import orchestration

    class FakeResponse:
        status_code = 200

        def json(self) -> dict[str, str]:
            return {"status": "bogus"}

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
        assert session_id not in two_pod_status_cache.cache
        assert two_pod_status_cache.cache == {}


@pytest.mark.asyncio
async def test_relay_cache_miss_declines_without_store_read(
    two_pod_status_cache: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        orchestration.session_stream,
        "publish",
        lambda session_id, payload: events.append({"id": session_id, **payload}),
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
    monkeypatch.setattr(
        orchestration.session_stream,
        "publish",
        lambda session_id, payload: events.append({"id": session_id, **payload}),
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
) -> None:
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
