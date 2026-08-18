"""Idle-monitor active-work accounting for background runner work.

Pins that ``app.state.has_active_work`` keeps the inactivity watchdog from
shutting down while ``sys_call_async`` tools, scheduled timers, or parked
approvals are still live — and that completion / cancel / failure release
the pin so a short idle timeout can shut down.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from omnigent.errors import OmnigentError
from omnigent.runner import app as runner_app
from omnigent.runner import create_runner_app, pending_approvals
from omnigent.runner._entry import _run_inactivity_monitor
from omnigent.runner.app import (
    _SESSION_MESSAGE_BUFFER_CAP,
    _has_live_async_tasks,
    _session_timers,
    register_timer,
    unregister_timer,
)
from omnigent.runner.tool_dispatch import execute_tool
from omnigent.runtime import _globals, set_runner_client
from omnigent.server.routes.sessions import create_sessions_router
from tests.runner.conftest import _FakeProcessManager, _runner_client, _ScriptedHarnessClient
from tests.runner.helpers import NullServerClient
from tests.runner.test_runner_dispatch import (
    _recovery_runner_client,
    _RecoveryFakeProcessManager,
    _RecoveryScriptedHarnessClient,
)
from tests.server.routes.test_session_resources import _ConversationStore


class _AckAgentStore:
    def get(self, agent_id: str) -> None:
        del agent_id
        return


@pytest.fixture(autouse=True)
def _clean_global_active_work_state() -> None:
    """
    Reset module-global timer / approval registries between tests.

    :returns: None.
    """
    pending_approvals.reset_for_tests()
    for session_timers in list(_session_timers.values()):
        for task in list(session_timers.values()):
            task.cancel()
    _session_timers.clear()
    yield
    pending_approvals.reset_for_tests()
    for session_timers in list(_session_timers.values()):
        for task in list(session_timers.values()):
            task.cancel()
    _session_timers.clear()


def _scaffold_app() -> FastAPI:
    """
    Build a scaffold runner app (no harness process manager).

    :returns: Fresh FastAPI runner app.
    """
    return create_runner_app(server_client=NullServerClient())  # type: ignore[arg-type]


def _buffered_app() -> FastAPI:
    """Build a runner whose message endpoint has a live process manager."""
    return create_runner_app(
        process_manager=_FakeProcessManager(_ScriptedHarnessClient([])),  # type: ignore[arg-type]
        server_client=NullServerClient(),  # type: ignore[arg-type]
    )


def _register_async_handle(
    registry: dict[str, dict[str, tuple[asyncio.Task[str], asyncio.Event]]],
    *,
    session_id: str,
    handle_id: str,
    task: asyncio.Task[str],
) -> None:
    """
    Insert a live ``sys_call_async`` registry entry for idle-monitor tests.

    :param registry: Async-tool registry to mutate.
    :param session_id: Session key, e.g. ``"conv_async"``.
    :param handle_id: Async handle id, e.g. ``"handle_test"``.
    :param task: Background task standing in for the async tool.
    :returns: None.
    """
    registry.setdefault(session_id, {})[handle_id] = (task, asyncio.Event())


async def _assert_monitor_blocked_then_shuts_down(
    *,
    has_active_work: Any,
    release: Any,
) -> None:
    """
    Prove a short idle timeout waits for active work, then shuts down.

    :param has_active_work: Callback matching ``app.state.has_active_work``.
    :param release: Awaitable that clears the active-work pin.
    :returns: None.
    """
    loop = asyncio.get_running_loop()
    shutdowns: list[str] = []
    monitor = asyncio.create_task(
        _run_inactivity_monitor(
            idle_timeout_s=0.01,
            get_last_activity=lambda: loop.time() - 1.0,
            has_active_work=has_active_work,
            request_shutdown=lambda: shutdowns.append("shutdown"),
            poll_interval_s=0.005,
        )
    )
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(monitor), timeout=0.03)
    assert shutdowns == []
    assert not monitor.done()

    await release()
    await asyncio.wait_for(monitor, timeout=0.2)
    assert shutdowns == ["shutdown"]


@pytest.mark.asyncio
async def test_running_async_tool_blocks_idle_shutdown() -> None:
    """A live ``sys_call_async`` task prevents idle shutdown.

    :returns: None.
    """
    registry: dict[str, dict[str, tuple[asyncio.Task[str], asyncio.Event]]] = {}
    started = asyncio.Event()
    finish = asyncio.Event()

    async def _bg() -> str:
        started.set()
        await finish.wait()
        return "ok"

    task = asyncio.create_task(_bg(), name="async-handle_live")
    _register_async_handle(registry, session_id="conv_async", handle_id="handle_live", task=task)
    await started.wait()
    assert _has_live_async_tasks(registry) is True

    async def _release() -> None:
        finish.set()
        await task
        registry["conv_async"].pop("handle_live", None)

    await _assert_monitor_blocked_then_shuts_down(
        has_active_work=lambda: _has_live_async_tasks(registry),
        release=_release,
    )
    assert _has_live_async_tasks(registry) is False


@pytest.mark.asyncio
async def test_completed_async_tool_is_idle_eligible() -> None:
    """After an async tool finishes, the runner is idle-eligible.

    :returns: None.
    """
    registry: dict[str, dict[str, tuple[asyncio.Task[str], asyncio.Event]]] = {}

    async def _bg() -> str:
        return "done"

    task = asyncio.create_task(_bg(), name="async-handle_done")
    _register_async_handle(registry, session_id="conv_async", handle_id="handle_done", task=task)
    await task
    # Stale registry entry must not count once the task is done.
    assert _has_live_async_tasks(registry) is False

    loop = asyncio.get_running_loop()
    shutdowns: list[str] = []
    await asyncio.wait_for(
        _run_inactivity_monitor(
            idle_timeout_s=0.01,
            get_last_activity=lambda: loop.time() - 1.0,
            has_active_work=lambda: _has_live_async_tasks(registry),
            request_shutdown=lambda: shutdowns.append("shutdown"),
            poll_interval_s=0.001,
        ),
        timeout=0.2,
    )
    assert shutdowns == ["shutdown"]


@pytest.mark.asyncio
async def test_cancelled_async_tool_releases_active_work() -> None:
    """Cancellation clears active-work status for idle shutdown.

    :returns: None.
    """
    registry: dict[str, dict[str, tuple[asyncio.Task[str], asyncio.Event]]] = {}
    gate = asyncio.Event()

    async def _bg() -> str:
        await gate.wait()
        return "never"

    task = asyncio.create_task(_bg(), name="async-handle_cancel")
    _register_async_handle(registry, session_id="conv_async", handle_id="handle_cancel", task=task)
    assert _has_live_async_tasks(registry) is True

    async def _release() -> None:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        registry["conv_async"].pop("handle_cancel", None)

    await _assert_monitor_blocked_then_shuts_down(
        has_active_work=lambda: _has_live_async_tasks(registry),
        release=_release,
    )
    assert _has_live_async_tasks(registry) is False


@pytest.mark.asyncio
async def test_failed_async_tool_releases_active_work() -> None:
    """A failed async tool releases active-work status.

    :returns: None.
    """
    registry: dict[str, dict[str, tuple[asyncio.Task[str], asyncio.Event]]] = {}
    gate = asyncio.Event()

    async def _bg() -> str:
        await gate.wait()
        raise RuntimeError("async tool boom")

    task = asyncio.create_task(_bg(), name="async-handle_fail")
    _register_async_handle(registry, session_id="conv_async", handle_id="handle_fail", task=task)
    assert _has_live_async_tasks(registry) is True

    async def _release() -> None:
        gate.set()
        with pytest.raises(RuntimeError, match="async tool boom"):
            await task
        # Leave the stale entry; done() must still make the runner idle-eligible.
        assert "handle_fail" in registry["conv_async"]

    await _assert_monitor_blocked_then_shuts_down(
        has_active_work=lambda: _has_live_async_tasks(registry),
        release=_release,
    )
    assert _has_live_async_tasks(registry) is False


@pytest.mark.asyncio
async def test_live_timer_blocks_idle_shutdown() -> None:
    """A registered timer task pins the runner until it completes.

    :returns: None.
    """
    app = _scaffold_app()
    finish = asyncio.Event()

    async def _timer() -> None:
        await finish.wait()

    task = asyncio.create_task(_timer(), name="timer-pin")
    register_timer("conv_timer", "timer_pin", task)
    assert app.state.has_active_work() is True

    async def _release() -> None:
        finish.set()
        await task
        unregister_timer("conv_timer", "timer_pin")

    await _assert_monitor_blocked_then_shuts_down(
        has_active_work=app.state.has_active_work,
        release=_release,
    )
    assert app.state.has_active_work() is False


@pytest.mark.asyncio
async def test_parked_approval_blocks_idle_shutdown() -> None:
    """A parked ASK Future keeps the runner alive until resolved.

    :returns: None.
    """
    app = _scaffold_app()
    fut = pending_approvals.register("elicit_idle_pin")
    assert app.state.has_active_work() is True

    async def _release() -> None:
        fut.set_result(True)
        pending_approvals.cleanup("elicit_idle_pin")

    await _assert_monitor_blocked_then_shuts_down(
        has_active_work=app.state.has_active_work,
        release=_release,
    )
    assert app.state.has_active_work() is False


@pytest.mark.asyncio
async def test_done_approval_future_does_not_pin_runner() -> None:
    """A completed approval Future left in the registry is not active work.

    :returns: None.
    """
    app = _scaffold_app()
    fut = pending_approvals.register("elicit_stale")
    fut.set_result(False)
    assert fut.done()
    assert app.state.has_active_work() is False


@pytest.mark.asyncio
async def test_f1_message_buffer_cap_refuses_without_growing() -> None:
    """The tool-to-server-to-runner path preserves a full-buffer refusal."""
    app = _buffered_app()
    session_id = "b460374fc8e697b296708f52dc9d8179"
    parent_id = "46b658cc1407206c877965810133b32f"
    app.state.active_turns[session_id] = None
    app.state.session_message_buffers[session_id] = [
        {"type": "message", "content": []} for _ in range(_SESSION_MESSAGE_BUFFER_CAP)
    ]
    store = _ConversationStore()
    server = FastAPI()

    @server.exception_handler(OmnigentError)
    async def _handle_error(request: Any, exc: OmnigentError) -> Any:
        del request
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    server.include_router(create_sessions_router(store, _AckAgentStore()), prefix="/v1")
    runner_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://runner"
    )
    prior_runner = _globals._runner_client
    set_runner_client(runner_client)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=server), base_url="http://server"
        ) as client:
            output = await execute_tool(
                tool_name="sys_session_send",
                arguments=json.dumps({"session_id": session_id, "args": "x", "if_busy": "queue"}),
                server_client=client,
                conversation_id=parent_id,
                agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
                session_inbox=asyncio.Queue(),
            )
        assert json.loads(output)["error"] == "queue_full"
        assert len(app.state.session_message_buffers[session_id]) == _SESSION_MESSAGE_BUFFER_CAP
        labels = store.get_conversation(session_id).labels
        assert "omnigent.last_task_error_code" not in labels
        assert "omnigent.last_task_error_message" not in labels
    finally:
        set_runner_client(prior_runner)
        await runner_client.aclose()
        app.state.active_turns.pop(session_id, None)
        app.state.session_message_buffers.pop(session_id, None)


@pytest.mark.asyncio
async def test_f2_failed_turn_flushes_buffer_and_reports_discard_count() -> None:
    """A failed turn drops buffered continuations and reports the count once."""
    app = _buffered_app()
    session_id = "conv_failed_buffer"
    parent_id = "conv_failed_parent"
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    runner_app._session_inboxes_ref[parent_id] = inbox
    runner_app.register_subagent_work(
        parent_session_id=parent_id, child_session_id=session_id, agent="worker", title="task"
    )
    app.state.active_turns[session_id] = None
    app.state.session_message_buffers[session_id] = [{"type": "message"}, {"type": "message"}]
    try:
        app.state.on_proxy_stream_end(session_id, error={"message": "boom"})
        result = inbox.get_nowait()
        assert result["status"] == "failed"
        assert "Buffered message(s) discarded: 2." in result["output"]
        assert session_id not in app.state.session_message_buffers
    finally:
        runner_app.unregister_subagent_work(session_id)
        runner_app._session_inboxes_ref.pop(parent_id, None)
        app.state.active_turns.pop(session_id, None)
        app.state.session_message_buffers.pop(session_id, None)


@pytest.mark.asyncio
async def test_f3_cancelled_turn_flushes_buffer_and_reports_discard_count() -> None:
    """A cancelled turn drops buffered continuations instead of starting them."""
    app = _buffered_app()
    session_id = "conv_cancelled_buffer"
    parent_id = "conv_cancelled_parent"
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    runner_app._session_inboxes_ref[parent_id] = inbox
    runner_app.register_subagent_work(
        parent_session_id=parent_id, child_session_id=session_id, agent="worker", title="task"
    )
    app.state.active_turns[session_id] = None
    app.state.session_message_buffers[session_id] = [{"type": "message"}]
    app.state.interrupted_sessions.add(session_id)
    try:
        app.state.on_proxy_stream_end(session_id)
        result = inbox.get_nowait()
        assert result["status"] == "cancelled"
        assert "Buffered message(s) discarded: 1." in result["output"]
        assert session_id not in app.state.session_message_buffers
    finally:
        app.state.interrupted_sessions.discard(session_id)
        runner_app.unregister_subagent_work(session_id)
        runner_app._session_inboxes_ref.pop(parent_id, None)
        app.state.active_turns.pop(session_id, None)
        app.state.session_message_buffers.pop(session_id, None)


@pytest.mark.asyncio
async def test_f4_connection_error_preserves_buffer_for_rebind() -> None:
    """A clean rebind keeps buffered messages instead of dropping them."""
    app = _buffered_app()
    session_id = "conv_connection_buffer"
    app.state.active_turns[session_id] = None
    app.state.session_message_buffers[session_id] = [{"type": "message"}]
    runner_app._session_histories_ref.pop(session_id, None)
    try:
        app.state.on_proxy_stream_end(
            session_id, error={"code": "connection_error", "message": "rebind"}
        )
        assert app.state.session_message_buffers[session_id] == [{"type": "message"}]
        assert session_id in app.state.desynced_sessions
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert runner_app._session_histories_ref[session_id] == [
            {"type": "message", "role": "user", "content": []}
        ]
    finally:
        app.state.active_turns.pop(session_id, None)
        app.state.session_message_buffers.pop(session_id, None)
        app.state.desynced_sessions.discard(session_id)
        runner_app._session_histories_ref.pop(session_id, None)


@pytest.mark.asyncio
async def test_f4_human_failure_preserves_buffer_without_subagent_notice() -> None:
    """A human turn failure does not silently discard its queued UI message."""
    app = _buffered_app()
    session_id = "conv_human_buffer"
    app.state.active_turns[session_id] = None
    app.state.session_message_buffers[session_id] = [{"type": "message"}]
    runner_app._session_histories_ref.pop(session_id, None)
    try:
        app.state.on_proxy_stream_end(session_id, error={"message": "boom"})
        assert app.state.session_message_buffers[session_id] == [{"type": "message"}]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert runner_app._session_histories_ref[session_id] == [
            {"type": "message", "role": "user", "content": []}
        ]
    finally:
        app.state.active_turns.pop(session_id, None)
        app.state.session_message_buffers.pop(session_id, None)
        runner_app._session_histories_ref.pop(session_id, None)


@pytest.mark.asyncio
async def test_f4_desync_rebind_preserves_and_replays_tracked_subagent_buffer() -> None:
    """Desync teardown keeps a tracked child's continuation for the rebind."""
    app = _buffered_app()
    child = "conv_desync_child"
    parent = "conv_desync_parent"
    runner_app.register_subagent_work(
        parent_session_id=parent, child_session_id=child, agent="worker", title="t"
    )
    app.state.active_turns[child] = None
    app.state.session_message_buffers[child] = [{"type": "message"}]
    app.state.desynced_sessions.add(child)
    app.state.interrupted_sessions.add(child)
    runner_app._session_histories_ref.pop(child, None)
    try:
        app.state.on_proxy_stream_end(child)
        assert app.state.session_message_buffers.get(child) == [{"type": "message"}]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert runner_app._session_histories_ref[child][-1] == {
            "type": "message",
            "role": "user",
            "content": [],
        }
    finally:
        app.state.active_turns.pop(child, None)
        app.state.session_message_buffers.pop(child, None)
        app.state.desynced_sessions.discard(child)
        app.state.interrupted_sessions.discard(child)
        runner_app.unregister_subagent_work(child)
        runner_app._session_histories_ref.pop(child, None)


@pytest.mark.asyncio
async def test_f4_desync_rebind_delivers_real_buffered_child_message() -> None:
    """A real event POST survives desync recovery and enters the next turn."""
    conv = "conv_probe_desync_child"
    parent = "conv_probe_desync_parent"
    harness = _RecoveryScriptedHarnessClient([])
    app = create_runner_app(
        process_manager=_RecoveryFakeProcessManager(harness),  # type: ignore[arg-type]
        server_client=NullServerClient(),  # type: ignore[arg-type]
    )
    runner_app.register_subagent_work(
        parent_session_id=parent, child_session_id=conv, agent="worker", title="t"
    )
    try:
        async with _recovery_runner_client(app) as http:
            live = asyncio.create_task(asyncio.sleep(60))
            app.state.active_turns[conv] = live
            response = await http.post(
                f"/v1/sessions/{conv}/events",
                json={
                    "type": "message",
                    "role": "user",
                    "agent_id": "ag",
                    "model": "x",
                    "content": [{"role": "user", "content": "queued follow up"}],
                },
            )
            assert response.status_code == 202
            await app.state.resync_turn_state(conv, "verdict_delivery_channel_dead")
            for _ in range(20):
                if app.state.session_message_buffers.get(conv) is None:
                    break
                await asyncio.sleep(0)
            assert (
                any(
                    item.get("content") == [{"role": "user", "content": "queued follow up"}]
                    for item in runner_app._session_histories_ref.get(conv, [])
                )
                or conv in app.state.active_turns
            )
    finally:
        live.cancel()
        runner_app.unregister_subagent_work(conv)
        app.state.session_message_buffers.pop(conv, None)
        app.state.active_turns.pop(conv, None)
        runner_app._session_histories_ref.pop(conv, None)


@pytest.mark.asyncio
async def test_f4_deleted_session_terminalizes_outstanding_work() -> None:
    """Deleting a session marks its outstanding child dispatch failed."""
    app = _buffered_app()
    session_id = "conv_deleted_work"
    parent_id = "conv_delete_parent"
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    runner_app._session_inboxes_ref[parent_id] = inbox
    runner_app.register_subagent_work(
        parent_session_id=parent_id,
        child_session_id=session_id,
        agent="worker",
        title="task",
    )
    try:
        async with _runner_client(app) as client:
            response = await client.delete(f"/v1/sessions/{session_id}")
        assert response.status_code == 200
        result = inbox.get_nowait()
        assert result["status"] == "failed"
        assert "deleted" in result["output"]
    finally:
        runner_app.unregister_subagent_work(session_id)
        runner_app._session_inboxes_ref.pop(parent_id, None)


@pytest.mark.asyncio
async def test_drain_session_streams_enqueues_done_sentinel() -> None:
    """Graceful shutdown signals end-of-stream to every open session stream.

    ``app.state.drain_session_streams`` puts the ``None`` sentinel on each
    session event queue so its ``GET /stream`` generator emits ``[DONE]`` and
    the server relay returns cleanly — the mechanism that turns an idle-reaped
    runner's abrupt drop into a quiet end-of-stream (no scary error banner).
    """
    from omnigent.runner.app import _session_event_queues_ref

    app = _scaffold_app()
    q_a: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    q_b: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    _session_event_queues_ref["conv_drain_a"] = q_a
    _session_event_queues_ref["conv_drain_b"] = q_b
    try:
        app.state.drain_session_streams()
        # Each open stream received exactly the end-of-stream sentinel.
        assert q_a.get_nowait() is None
        assert q_b.get_nowait() is None
        assert q_a.empty()
        assert q_b.empty()
    finally:
        _session_event_queues_ref.pop("conv_drain_a", None)
        _session_event_queues_ref.pop("conv_drain_b", None)
