"""Discriminating tests for durable liveness decisions."""

from __future__ import annotations

import inspect
import threading
from types import SimpleNamespace

import pytest

from omnigent.server.routes import sessions as sessions_facade
from omnigent.server.routes._sessions import common, helpers, orchestration
from omnigent.stores.conversation_store import ConversationStore


def test_frozen_status_signatures() -> None:
    assert str(inspect.signature(ConversationStore.set_session_live_status)) == (
        "(self, conversation_id: str, status: str) -> None"
    )
    assert str(inspect.signature(helpers._session_status_from_cache)) == (
        "(conversation_id: 'str', db_status: 'str | None' = None) -> "
        "\"Literal['idle', 'running', 'failed']\""
    )
    assert str(inspect.signature(helpers._session_status_with_child_rollup)) == (
        "(conversation_id: 'str', child_session_ids: 'list[str]', "
        "db_status: 'str | None' = None) -> "
        "\"Literal['idle', 'running', 'failed']\""
    )


@pytest.mark.parametrize(
    ("cached", "row", "expected"),
    [
        (None, None, "decline"),
        (None, "idle", "decline"),
        (None, "running", "fail"),
        (None, "waiting", "fail"),
        (None, "failed", "decline"),
        ("idle", None, "decline"),
        ("idle", "idle", "decline"),
        ("idle", "running", "decline"),
        ("idle", "waiting", "decline"),
        ("idle", "failed", "decline"),
        ("running", None, "fail"),
        ("running", "idle", "adopt_row"),
        ("running", "running", "fail"),
        ("running", "waiting", "fail"),
        ("running", "failed", "adopt_row"),
        ("waiting", None, "fail"),
        ("waiting", "idle", "adopt_row"),
        ("waiting", "running", "fail"),
        ("waiting", "waiting", "fail"),
        ("waiting", "failed", "adopt_row"),
        ("failed", None, "decline"),
        ("failed", "idle", "decline"),
        ("failed", "running", "decline"),
        ("failed", "waiting", "decline"),
        ("failed", "failed", "decline"),
        ("launching", None, "decline"),
        ("launching", "idle", "decline"),
        ("launching", "running", "decline"),
        ("launching", "waiting", "decline"),
        ("launching", "failed", "decline"),
    ],
)
def test_liveness_resolver_truth_table(cached, row, expected) -> None:
    assert helpers._resolve_liveness_decision(cached, row) == expected


@pytest.mark.parametrize(
    ("cached", "durable_status", "code", "expected"),
    [
        (None, None, None, "accept"),
        (None, None, "runner_disconnected", "accept"),
        (None, None, "runner_failed_to_start", "accept"),
        (None, None, "codex_turn_error", "accept"),
        (None, "idle", None, "accept"),
        (None, "idle", "runner_disconnected", "accept"),
        (None, "idle", "runner_failed_to_start", "accept"),
        (None, "idle", "codex_turn_error", "accept"),
        (None, "failed", None, "suppress"),
        (None, "failed", "runner_disconnected", "accept"),
        (None, "failed", "runner_failed_to_start", "suppress"),
        (None, "failed", "codex_turn_error", "suppress"),
        ("idle", None, None, "accept"),
        ("idle", None, "runner_disconnected", "accept"),
        ("idle", None, "runner_failed_to_start", "accept"),
        ("idle", None, "codex_turn_error", "accept"),
        ("idle", "idle", None, "accept"),
        ("idle", "idle", "runner_disconnected", "accept"),
        ("idle", "idle", "runner_failed_to_start", "accept"),
        ("idle", "idle", "codex_turn_error", "accept"),
        ("idle", "failed", None, "suppress"),
        ("idle", "failed", "runner_disconnected", "accept"),
        ("idle", "failed", "runner_failed_to_start", "suppress"),
        ("idle", "failed", "codex_turn_error", "suppress"),
        ("failed", None, None, "suppress"),
        ("failed", None, "runner_disconnected", "accept"),
        ("failed", None, "runner_failed_to_start", "suppress"),
        ("failed", None, "codex_turn_error", "suppress"),
        ("failed", "idle", None, "suppress"),
        ("failed", "idle", "runner_disconnected", "accept"),
        ("failed", "idle", "runner_failed_to_start", "suppress"),
        ("failed", "idle", "codex_turn_error", "suppress"),
        ("failed", "failed", None, "suppress"),
        ("failed", "failed", "runner_disconnected", "accept"),
        ("failed", "failed", "runner_failed_to_start", "suppress"),
        ("failed", "failed", "codex_turn_error", "suppress"),
    ],
)
def test_idle_after_failure_resolver_truth_table(cached, durable_status, code, expected) -> None:
    assert helpers._resolve_idle_after_failure(cached, durable_status, code) == expected


@pytest.mark.asyncio
async def test_relay_uses_row_when_cache_is_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    session_id = "conv-stale"
    common._session_status_cache[session_id] = "running"
    events: list[str] = []
    labels: list[object] = []

    class Store:
        def __init__(self) -> None:
            self.get_conversation_calls = 0

        def get_conversation(self, conversation_id: str):
            self.get_conversation_calls += 1
            return SimpleNamespace(id=conversation_id, live_status="idle")

    store = Store()

    async def lost(*args, **kwargs):
        raise orchestration._RelayTransportLost(intentional=False)

    monkeypatch.setattr(orchestration, "_relay_runner_stream_once", lost)
    monkeypatch.setattr(orchestration, "RUNNER_DISCONNECT_GRACE_S", 0.0)
    monkeypatch.setattr(
        orchestration, "_publish_status", lambda *args, **kwargs: events.append("status")
    )

    async def record_labels(*args, **kwargs):
        labels.append(args)

    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", record_labels)

    try:
        await orchestration._relay_runner_stream(session_id, object(), store)  # type: ignore[arg-type]
        assert store.get_conversation_calls == 1
        assert events == []
        assert labels == []
        assert common._session_status_cache[session_id] == "idle"
    finally:
        common._session_status_cache.pop(session_id, None)


@pytest.mark.asyncio
async def test_lost_running_write_is_silent_until_runner_resync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lost running write leaves a quiet row-terminal veto until resync."""
    session_id = "conv-lost-running"
    write_seen = threading.Event()
    events: list[object] = []

    class Store:
        def set_session_live_status(self, conversation_id: str, status: str) -> None:
            # The write reaches the store call but is lost before changing the
            # durable idle row.
            del conversation_id, status
            write_seen.set()

        def get_conversation(self, conversation_id: str):
            return SimpleNamespace(id=conversation_id, live_status="idle")

        def set_session_live_status_if_busy(self, conversation_id: str, status: str) -> bool:
            raise AssertionError("idle row should veto the failure claim")

    store = Store()
    helpers.session_live_state.configure(store)  # type: ignore[arg-type]
    monkeypatch.setattr(
        sessions_facade,
        "session_stream",
        SimpleNamespace(publish=lambda *args: None),
    )
    monkeypatch.setattr(
        helpers.session_live_state,
        "persist_scheduled_run_completion",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        orchestration, "_publish_status", lambda *args, **kwargs: events.append(args)
    )
    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", _record_async)
    monkeypatch.setattr(orchestration, "RUNNER_DISCONNECT_GRACE_S", 0.0)

    async def lost(*args, **kwargs):
        raise orchestration._RelayTransportLost(intentional=False)

    monkeypatch.setattr(orchestration, "_relay_runner_stream_once", lost)
    common._session_status_cache[session_id] = "idle"
    try:
        helpers._publish_status(session_id, "running")
        assert write_seen.wait(1.0)
        assert common._session_status_cache[session_id] == "running"
        await orchestration._relay_runner_stream(session_id, object(), store)  # type: ignore[arg-type]
        assert events == []
        assert common._session_status_cache[session_id] == "idle"
    finally:
        helpers.session_live_state.configure(None)
        common._session_status_cache.pop(session_id, None)


@pytest.mark.asyncio
async def test_reconciler_adopts_durable_idle_over_stale_running_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "conv-reconciler"
    common._session_status_cache[session_id] = "running"
    events: list[object] = []
    labels: list[object] = []

    async def record_labels(*args, **kwargs):
        labels.append(args)

    monkeypatch.setattr(
        orchestration, "_publish_status", lambda *args, **kwargs: events.append(args)
    )
    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", record_labels)
    try:
        await orchestration._mark_runner_sessions_offline_impl(
            [SimpleNamespace(id=session_id, kind="sub_agent", live_status="idle")],
            SimpleNamespace(code="runner_disconnected", message="gone"),
            object(),  # type: ignore[arg-type]
        )
        assert events == []
        assert labels == []
        assert common._session_status_cache[session_id] == "idle"
    finally:
        common._session_status_cache.pop(session_id, None)


@pytest.mark.asyncio
async def test_relay_busy_failure_claim_loses_to_idle_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "conv-relay-barrier"
    common._session_status_cache[session_id] = "running"
    events: list[object] = []

    class Store:
        row_status = "running"

        def get_conversation(self, conversation_id: str):
            return SimpleNamespace(live_status=self.row_status)

        def set_session_live_status_if_busy(self, conversation_id: str, status: str) -> bool:
            # The durable idle commit lands after the relay read and before its
            # conditional failure write.
            self.row_status = "idle"
            return False

    async def lost(*args, **kwargs):
        raise orchestration._RelayTransportLost(intentional=False)

    monkeypatch.setattr(orchestration, "_relay_runner_stream_once", lost)
    monkeypatch.setattr(orchestration, "RUNNER_DISCONNECT_GRACE_S", 0.0)
    monkeypatch.setattr(
        orchestration, "_publish_status", lambda *args, **kwargs: events.append(args)
    )
    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", _record_async)
    store = Store()
    try:
        await orchestration._relay_runner_stream(session_id, object(), store)  # type: ignore[arg-type]
        assert events == []
        assert store.row_status == "idle"
    finally:
        common._session_status_cache.pop(session_id, None)


@pytest.mark.asyncio
async def test_reconciler_busy_failure_claim_loses_to_idle_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "conv-reconcile-barrier"
    events: list[object] = []

    class Store:
        row_status = "running"

        def set_session_live_status_if_busy(self, conversation_id: str, status: str) -> bool:
            # The completed idle commit wins between snapshot materialization
            # and this stale failure attempt.
            self.row_status = "idle"
            return False

    monkeypatch.setattr(
        orchestration, "_publish_status", lambda *args, **kwargs: events.append(args)
    )
    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", _record_async)
    store = Store()
    await orchestration._mark_runner_sessions_offline_impl(
        [SimpleNamespace(id=session_id, kind="sub_agent", live_status="running")],
        SimpleNamespace(code="runner_disconnected", message="gone"),
        store,  # type: ignore[arg-type]
    )
    assert events == []
    assert store.row_status == "idle"


@pytest.mark.asyncio
async def test_reconciler_dead_on_arrival_still_fails_idle_top_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "conv-dead-on-arrival"
    events: list[object] = []
    monkeypatch.setattr(
        orchestration, "_publish_status", lambda *args, **kwargs: events.append(args)
    )
    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", _record_async)
    await orchestration._mark_runner_sessions_offline_impl(
        [SimpleNamespace(id=session_id, kind="default", live_status="idle")],
        SimpleNamespace(code="runner_failed_to_start", message="crashed"),
        object(),  # type: ignore[arg-type]
        fail_idle_top_level=True,
    )
    assert len(events) == 1


@pytest.mark.asyncio
async def test_relay_store_failure_falls_through_to_cache_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "conv-store-error"
    common._session_status_cache[session_id] = "running"
    events: list[object] = []

    class Store:
        def get_conversation(self, conversation_id: str):
            raise RuntimeError("database unavailable")

    async def lost(*args, **kwargs):
        raise orchestration._RelayTransportLost(intentional=False)

    monkeypatch.setattr(orchestration, "_relay_runner_stream_once", lost)
    monkeypatch.setattr(orchestration, "RUNNER_DISCONNECT_GRACE_S", 0.0)
    monkeypatch.setattr(
        orchestration, "_publish_status", lambda *args, **kwargs: events.append(args)
    )
    monkeypatch.setattr(orchestration, "_persist_session_status_error_labels", _record_async)
    try:
        await orchestration._relay_runner_stream(session_id, object(), Store())  # type: ignore[arg-type]
        assert len(events) == 1
    finally:
        common._session_status_cache.pop(session_id, None)


def test_task_failure_stays_sticky_but_disconnect_idle_is_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "conv-sticky"
    published: list[object] = []
    monkeypatch.setattr(helpers.session_live_state, "persist_live_status", lambda *args: None)
    monkeypatch.setattr(
        helpers.session_live_state,
        "persist_scheduled_run_completion",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        sessions_facade,
        "session_stream",
        SimpleNamespace(publish=lambda *args: published.append(args)),
    )
    common._session_status_cache.pop(session_id, None)
    try:
        assert not helpers._publish_status(
            session_id,
            "idle",
            durable_status="failed",
            durable_error_code="codex_turn_error",
        )
        assert helpers._publish_status(
            session_id,
            "idle",
            durable_status="failed",
            durable_error_code="runner_disconnected",
        )
        assert len(published) == 1
    finally:
        common._session_status_cache.pop(session_id, None)


@pytest.mark.asyncio
async def test_disconnect_label_clear_is_bound_to_current_code() -> None:
    session_id = "conv-labels"
    labels = {
        "omnigent.last_task_error_code": "runner_disconnected",
        "omnigent.last_task_error_message": "gone",
    }

    class Store:
        def clear_disconnect_error_labels_if_current(self, conversation_id: str) -> bool:
            if labels.get("omnigent.last_task_error_code") != "runner_disconnected":
                return False
            labels.clear()
            return True

        def set_labels(self, conversation_id: str, updates: dict[str, str]) -> None:
            labels.update(updates)

    await helpers._persist_session_status_error_labels(
        session_id,
        None,
        Store(),  # type: ignore[arg-type]
        only_if_disconnect=True,
    )
    assert labels == {}

    labels["omnigent.last_task_error_code"] = "codex_turn_error"
    await helpers._persist_session_status_error_labels(
        session_id,
        None,
        Store(),  # type: ignore[arg-type]
        only_if_disconnect=True,
    )
    assert labels == {"omnigent.last_task_error_code": "codex_turn_error"}


@pytest.mark.asyncio
async def test_external_idle_accepts_disconnect_failure_and_clears_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An external idle clears only the durable disconnect classification."""
    session_id = "conv-external-idle"
    labels = {"omnigent.last_task_error_code": "runner_disconnected"}

    class Store:
        def clear_disconnect_error_labels_if_current(self, conversation_id: str) -> bool:
            del conversation_id
            if labels.get("omnigent.last_task_error_code") != "runner_disconnected":
                return False
            labels.clear()
            return True

    monkeypatch.setattr(helpers.session_live_state, "persist_live_status", lambda *args: None)
    monkeypatch.setattr(
        sessions_facade,
        "session_stream",
        SimpleNamespace(publish=lambda *args: None),
    )
    common._session_status_cache[session_id] = "failed"
    try:
        assert helpers._publish_status(
            session_id,
            "idle",
            durable_status="failed",
            durable_error_code="runner_disconnected",
        )
        await helpers._persist_session_status_error_labels(
            session_id,
            None,
            Store(),  # type: ignore[arg-type]
            only_if_disconnect=True,
        )
        assert labels == {}
    finally:
        common._session_status_cache.pop(session_id, None)


async def _record_async(*args, **kwargs) -> None:
    del args, kwargs
