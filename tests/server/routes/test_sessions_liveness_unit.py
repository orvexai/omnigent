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
