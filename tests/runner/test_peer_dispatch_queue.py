"""Regression tests for peer sends queued by a different caller."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from omnigent.runner import app
from omnigent.runner import tool_dispatch as dispatch


def _clear_queue_test_state(*session_ids: str) -> None:
    app._peer_dispatch_queues.clear()
    app._subagent_work_by_child.clear()
    app._subagent_work_by_parent.clear()
    for session_id in session_ids:
        app._session_inboxes_ref.pop(session_id, None)
    app._threads_by_id.clear()
    app._threads_by_pair.clear()
    app._closed_thread_ids.clear()


def test_second_peer_caller_is_queued_and_keeps_reply_correlation() -> None:
    async def scenario() -> None:
        target = "peer-target"
        caller_a = "caller-a"
        caller_b = "caller-b"
        inbox_a: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        inbox_b: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        app._session_inboxes_ref[caller_a] = inbox_a
        app._session_inboxes_ref[caller_b] = inbox_b
        events: list[dict[str, object]] = []
        started: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET" and request.url.path == f"/v1/sessions/{target}/items":
                return httpx.Response(200, json={"data": []})
            if request.method == "POST" and request.url.path == f"/v1/sessions/{target}/events":
                events.append(json.loads(request.content))
                return httpx.Response(202, json={"delivery": "accepted"})
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        async def target_online(*_args: object, **_kwargs: object) -> bool:
            return True

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://server",
        ) as client:
            old_online = dispatch._runner_online_or_none
            old_start = app._remote_dispatch_start_ref
            dispatch._runner_online_or_none = target_online
            app._remote_dispatch_start_ref = lambda **kwargs: started.append(kwargs["work_id"])
            snap = {
                "status": "running",
                "runner_id": "runner-1",
                "agent_name": "worker",
                "title": "worker:shared",
                "labels": {},
            }
            identity_a = dispatch._SessionTurnIdentity(
                session_id=caller_a,
                actor=None,
                agent_name="agent-a",
                title="a",
                parent_session_id=None,
            )
            identity_b = dispatch._SessionTurnIdentity(
                session_id=caller_b,
                actor=None,
                agent_name="agent-b",
                title="b",
                parent_session_id=None,
            )
            try:
                first = json.loads(
                    await dispatch._send_to_peer_session(
                        target,
                        "first request",
                        server_client=client,
                        conversation_id=caller_a,
                        snap_data=snap,
                        sender_identity=identity_a,
                    )
                )
                active = app.get_subagent_work(target)
                assert active is not None
                active.status = "running"

                second = json.loads(
                    await dispatch._send_to_peer_session(
                        target,
                        "second request",
                        server_client=client,
                        conversation_id=caller_b,
                        snap_data=snap,
                        sender_identity=identity_b,
                        if_busy="queue",
                    )
                )
                assert second["status"] == "queued"
                assert second["work_id"] != first["work_id"]
                assert second["thread_id"] != first["thread_id"]
                assert len(events) == 1

                app.mark_subagent_work_terminal(target, status="completed", output="reply-a")
                await asyncio.sleep(0.01)
                promoted = app.get_subagent_work(target)
                assert promoted is not None
                assert promoted.work_id == second["work_id"]
                assert len(events) == 2
                assert started == [first["work_id"], second["work_id"]]

                app.mark_subagent_work_terminal(target, status="completed", output="reply-b")
                first_payload = inbox_a.get_nowait()
                second_payload = inbox_b.get_nowait()
                assert first_payload["work_id"] == first["work_id"]
                assert first_payload["output"] == "reply-a"
                assert second_payload["work_id"] == second["work_id"]
                assert second_payload["output"] == "reply-b"
            finally:
                dispatch._runner_online_or_none = old_online
                app._remote_dispatch_start_ref = old_start

    try:
        asyncio.run(scenario())
    finally:
        app._peer_dispatch_queues.clear()
        app._subagent_work_by_child.clear()
        app._subagent_work_by_parent.clear()
        app._session_inboxes_ref.pop("caller-a", None)
        app._session_inboxes_ref.pop("caller-b", None)
        app._threads_by_id.clear()
        app._threads_by_pair.clear()
        app._closed_thread_ids.clear()


def test_peer_dispatch_queue_is_bounded() -> None:
    callbacks = [lambda: asyncio.sleep(0) for _ in range(app._SUBAGENT_QUEUED_SEND_CAP)]
    try:
        for callback in callbacks:
            assert app.enqueue_peer_dispatch("target", callback)
        assert app.peer_dispatch_queue_full("target")
        assert not app.enqueue_peer_dispatch("target", lambda: asyncio.sleep(0))
    finally:
        app._peer_dispatch_queues.clear()


def _run_two_caller_queue_path(path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        target = "child-target"
        caller_a = "caller-a"
        caller_b = "caller-b"
        inbox_a: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        inbox_b: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        app._session_inboxes_ref[caller_a] = inbox_a
        app._session_inboxes_ref[caller_b] = inbox_b
        events: list[dict[str, object]] = []
        started: list[str] = []

        snap = {
            "id": target,
            "status": "running",
            "busy": True,
            "runner_id": "runner-1",
            "agent_name": "worker",
            "sub_agent_name": "worker",
            "title": "worker:shared",
            "parent_session_id": caller_a,
            "labels": {},
        }

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET" and request.url.path == f"/v1/sessions/{target}":
                return httpx.Response(200, json=snap)
            if request.method == "GET" and request.url.path == f"/v1/sessions/{caller_b}":
                return httpx.Response(200, json={"id": caller_b, "labels": {}})
            if request.method == "GET" and request.url.path == f"/v1/sessions/{target}/items":
                return httpx.Response(200, json={"data": []})
            if request.method == "POST" and request.url.path == f"/v1/sessions/{target}/events":
                events.append(json.loads(request.content))
                return httpx.Response(202, json={"delivery": "accepted"})
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        async def target_online(*_args: object, **_kwargs: object) -> bool:
            return True

        identity_a = dispatch._SessionTurnIdentity(
            session_id=caller_a,
            actor=None,
            agent_name="agent-a",
            title="a",
            parent_session_id=None,
        )
        identity_b = dispatch._SessionTurnIdentity(
            session_id=caller_b,
            actor=None,
            agent_name="agent-b",
            title="b",
            parent_session_id=None,
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://server",
        ) as client:
            monkeypatch.setattr(dispatch, "_runner_online_or_none", target_online)
            monkeypatch.setattr(
                app,
                "_remote_dispatch_start_ref",
                lambda **kwargs: started.append(kwargs["work_id"]),
            )
            first = json.loads(
                await dispatch._send_to_peer_session(
                    target,
                    "first request",
                    server_client=client,
                    conversation_id=caller_a,
                    snap_data=snap,
                    sender_identity=identity_a,
                )
            )
            active = app.get_subagent_work(target)
            assert active is not None
            active.status = "running"

            if path == "peer":
                second_result = await dispatch._send_to_peer_session(
                    target,
                    "second request",
                    server_client=client,
                    conversation_id=caller_b,
                    snap_data=snap,
                    sender_identity=identity_b,
                    if_busy="queue",
                )
            elif path == "existing":
                monkeypatch.setattr(dispatch, "_session_is_local_to_caller", lambda *_args: True)
                second_result = await dispatch._send_to_existing_session(
                    target,
                    "second request",
                    server_client=client,
                    conversation_id=caller_b,
                    sender_identity=identity_b,
                    if_busy="queue",
                )
            else:

                async def find_existing(**_kwargs: object) -> dict[str, object]:
                    return snap

                monkeypatch.setattr(dispatch, "_has_subagent", lambda *_args: True)
                monkeypatch.setattr(dispatch, "_find_existing_child_session", find_existing)
                monkeypatch.setattr(app, "get_session_agent_id", lambda *_args: "parent-agent")
                second_result = await dispatch._execute_subagent_tool(
                    {
                        "agent": "worker",
                        "title": "shared",
                        "args": "second request",
                        "if_busy": "queue",
                    },
                    server_client=client,
                    conversation_id=caller_b,
                    session_inbox=inbox_b,
                )

            second = json.loads(second_result)
            assert second["status"] == "queued"
            assert second["work_id"] != first["work_id"]
            assert second["thread_id"] != first["thread_id"]
            assert len(events) == 1

            app.mark_subagent_work_terminal(target, status="completed", output="reply-a")
            await asyncio.sleep(0.01)
            promoted = app.get_subagent_work(target)
            assert promoted is not None
            assert promoted.work_id == second["work_id"]
            assert len(events) == 2
            assert started == [first["work_id"], second["work_id"]]

            app.mark_subagent_work_terminal(target, status="completed", output="reply-b")
            first_payload = inbox_a.get_nowait()
            second_payload = inbox_b.get_nowait()
            assert first_payload["work_id"] == first["work_id"]
            assert first_payload["output"] == "reply-a"
            assert second_payload["work_id"] == second["work_id"]
            assert second_payload["output"] == "reply-b"

    try:
        asyncio.run(scenario())
    finally:
        _clear_queue_test_state("caller-a", "caller-b")


@pytest.mark.parametrize("path", ["peer", "existing", "named"])
def test_two_callers_queue_on_all_send_paths(path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _run_two_caller_queue_path(path, monkeypatch)
