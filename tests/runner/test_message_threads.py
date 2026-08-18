"""Focused tests for the Stage 1 in-memory message-thread registry."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from omnigent.runner import app
from omnigent.runner import tool_dispatch as dispatch
from omnigent.runner.tool_dispatch import _agent_message_envelope, _SessionTurnIdentity
from omnigent.tools.builtins.async_inbox import SysReadInboxTool
from omnigent.tools.builtins.spawn import _build_sys_session_send_schema


@pytest.fixture(autouse=True)
def clean_thread_registry():
    saved_id = dict(app._threads_by_id)
    saved_pair = dict(app._threads_by_pair)
    saved_closed = list(app._closed_thread_ids)
    saved_work = dict(app._subagent_work_by_child)
    saved_work_parent = {key: set(value) for key, value in app._subagent_work_by_parent.items()}
    saved_inboxes = dict(app._session_inboxes_ref)
    try:
        app._threads_by_id.clear()
        app._threads_by_pair.clear()
        app._closed_thread_ids.clear()
        app._subagent_work_by_child.clear()
        app._subagent_work_by_parent.clear()
        app._session_inboxes_ref.clear()
        yield
    finally:
        app._threads_by_id.clear()
        app._threads_by_id.update(saved_id)
        app._threads_by_pair.clear()
        app._threads_by_pair.update(saved_pair)
        app._closed_thread_ids.clear()
        app._closed_thread_ids.extend(saved_closed)
        app._subagent_work_by_child.clear()
        app._subagent_work_by_child.update(saved_work)
        app._subagent_work_by_parent.clear()
        app._subagent_work_by_parent.update(saved_work_parent)
        app._session_inboxes_ref.clear()
        app._session_inboxes_ref.update(saved_inboxes)


def test_no_parameters_reuses_caller_owned_outstanding_thread() -> None:
    first = app.resolve_message_thread("parent", "child")
    assert first.error is None
    assert first.thread is not None

    entry = app.register_subagent_work(
        parent_session_id="parent",
        child_session_id="child",
        agent="worker",
        title="one",
        thread_id=first.thread.thread_id,
    )
    resumed = app.resolve_message_thread("parent", "child", outstanding_entry=entry)

    assert resumed.error is None
    assert resumed.thread is first.thread


def test_subject_mints_a_distinct_thread_and_unknown_ids_are_refused() -> None:
    first = app.resolve_message_thread("parent", "child")
    second = app.resolve_message_thread("parent", "child", thread_subject="review")
    unknown = app.resolve_message_thread("parent", "child", thread_id="th_not_minted")

    assert first.thread is not None
    assert second.thread is not None
    assert second.thread.thread_id != first.thread.thread_id
    assert second.thread.subject == "review"
    assert unknown.thread is None
    assert unknown.error == "unknown_thread"
    assert "th_not_minted" not in app._threads_by_id


def test_participant_and_closed_thread_refusals_do_not_disclose_members() -> None:
    created = app.resolve_message_thread("parent", "child")
    assert created.thread is not None
    thread_id = created.thread.thread_id

    outsider = app.resolve_message_thread("outsider", "child", thread_id=thread_id)
    assert outsider.thread is None
    assert outsider.error == "not_a_thread_participant"
    assert outsider.blocking_thread_id is None

    app.close_message_thread(thread_id)
    closed = app.resolve_message_thread("parent", "child", thread_id=thread_id)
    assert closed.thread is None
    assert closed.error == "thread_closed"


def test_closing_thread_prunes_registry_and_pair_index() -> None:
    created = app.resolve_message_thread("parent", "child")
    assert created.thread is not None
    thread_id = created.thread.thread_id

    app.close_message_thread(thread_id)

    assert thread_id not in app._threads_by_id
    assert ("parent", "child") not in app._threads_by_pair
    assert (
        app.resolve_message_thread("parent", "child", thread_id=thread_id).error == "thread_closed"
    )


def test_message_cap_refuses_without_incrementing_past_the_bound() -> None:
    created = app.resolve_message_thread("parent", "child")
    assert created.thread is not None
    thread = created.thread
    thread.message_count = app._THREAD_MESSAGE_CAP

    refused = app.resolve_message_thread("parent", "child", thread_id=thread.thread_id)

    assert refused.thread is None
    assert refused.error == "thread_full"
    assert thread.message_count == app._THREAD_MESSAGE_CAP


def test_envelope_carries_escaped_thread_identity_and_subject() -> None:
    sender = _SessionTurnIdentity(
        session_id="parent",
        actor=None,
        agent_name="worker",
        title="review",
        parent_session_id=None,
    )

    wrapped = _agent_message_envelope(
        "check",
        sender,
        "parent",
        thread_id="th_abc123",
        thread_subject='a "quoted" review',
    )

    assert 'thread="th_abc123"' in wrapped
    assert 'subject="a &quot;quoted&quot; review"' in wrapped
    assert 'This message is on thread th_abc123 ("a &quot;quoted&quot; review")' in wrapped


def test_terminal_delivery_stamps_the_dispatch_thread() -> None:
    import asyncio

    parent_inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    app._session_inboxes_ref["parent"] = parent_inbox
    created = app.resolve_message_thread("parent", "child")
    assert created.thread is not None
    assert app.claim_message_thread(created.thread) is None
    entry = app.register_subagent_work(
        parent_session_id="parent",
        child_session_id="child",
        agent="worker",
        title="one",
        thread_id=created.thread.thread_id,
    )
    assert created.thread.open_work_id == entry.work_id

    ack = app.mark_subagent_work_terminal("child", status="completed", output="done")
    payload = parent_inbox.get_nowait()

    assert ack.delivered
    assert payload["thread_id"] == created.thread.thread_id
    assert created.thread.open_work_id is None


def test_wake_notice_names_all_pending_threads() -> None:
    inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    inbox.put_nowait({"thread_id": "th_a"})
    inbox.put_nowait({"thread_id": "th_b"})
    app._session_inboxes_ref["parent"] = inbox

    thread_ids = app._pending_inbox_thread_ids("parent")
    notice = app._format_subagent_wake_notice(
        agent="worker",
        title="one",
        status="completed",
        pending=2,
        thread_ids=thread_ids,
    )

    assert thread_ids == ["th_a", "th_b"]
    assert "on threads th_a, th_b" in notice


def test_pair_and_opener_caps_refuse_new_threads() -> None:
    for index in range(app._THREADS_PER_PAIR_CAP):
        created = app.resolve_message_thread("parent", "child", thread_subject=f"pair-{index}")
        assert created.thread is not None
    pair_refused = app.resolve_message_thread("parent", "child", thread_subject="pair-overflow")
    assert pair_refused.error == "thread_pair_full"

    app._threads_by_id.clear()
    app._threads_by_pair.clear()
    app._closed_thread_ids.clear()
    for index in range(app._OPEN_THREADS_PER_SESSION_CAP):
        created = app.resolve_message_thread(
            "parent", f"child-{index}", thread_subject=f"session-{index}"
        )
        assert created.thread is not None
    session_refused = app.resolve_message_thread(
        "parent", "child-overflow", thread_subject="session-overflow"
    )
    assert session_refused.error == "session_thread_cap"


def test_send_and_inbox_schemas_advertise_thread_parameters_in_both_modes() -> None:
    for specs in ({}, {"worker": SimpleNamespace(description="worker")}):
        properties = _build_sys_session_send_schema(specs)["function"]["parameters"]["properties"]
        assert "thread_id" in properties
        assert "thread_subject" in properties

    inbox_properties = SysReadInboxTool().get_schema()["function"]["parameters"]["properties"]
    assert "thread_id" in inbox_properties


def test_thread_label_value_keeps_participants_and_id_budget_with_long_subject() -> None:
    created = app.resolve_message_thread("conv_parent", "conv_target", thread_subject="x" * 500)
    assert created.thread is not None

    key = app.message_thread_label_key(created.thread.thread_id)
    value = app.serialize_message_thread_label(created.thread)
    decoded = json.loads(value)

    assert key == f"omnigent.thread.{created.thread.thread_id}"
    assert len(value.encode("utf-8")) <= app.LABEL_VALUE_MAX_LEN - app._THREAD_LABEL_HEADROOM
    assert decoded["o"] == "conv_parent"
    assert decoded["t"] == "conv_target"
    assert decoded["s"]
    assert len(decoded["s"]) < 500


def test_restart_rehydrates_thread_from_target_label_and_preserves_subject() -> None:
    async def scenario() -> None:
        target = "conv_target"
        caller = "conv_parent"
        events: list[dict[str, object]] = []
        patch_paths: list[str] = []
        labels: dict[str, str] = {}
        inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        app._session_inboxes_ref[caller] = inbox

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "PATCH":
                patch_paths.append(request.url.path)
                labels.update(json.loads(request.content).get("labels", {}))
                return httpx.Response(200, json={"id": target, "labels": labels})
            if request.method == "GET" and request.url.path == f"/v1/sessions/{target}/items":
                return httpx.Response(200, json={"data": []})
            if request.method == "POST" and request.url.path == f"/v1/sessions/{target}/events":
                events.append(json.loads(request.content))
                return httpx.Response(202, json={"delivery": "accepted"})
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        async def target_online(*_args: object, **_kwargs: object) -> bool:
            return True

        identity = dispatch._SessionTurnIdentity(
            session_id=caller,
            actor=None,
            agent_name="parent",
            title="parent",
            parent_session_id=None,
        )
        snap = {
            "status": "idle",
            "runner_id": "runner-1",
            "agent_name": "worker",
            "title": "worker:review",
            "labels": labels,
        }
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://server"
        ) as client:
            old_online = dispatch._runner_online_or_none
            old_start = app._remote_dispatch_start_ref
            dispatch._runner_online_or_none = target_online
            app._remote_dispatch_start_ref = lambda **_kwargs: None
            try:
                first = json.loads(
                    await dispatch._send_to_peer_session(
                        target,
                        "first",
                        server_client=client,
                        conversation_id=caller,
                        snap_data=snap,
                        sender_identity=identity,
                        thread_subject="review",
                    )
                )
                thread_id = first["thread_id"]
                subject = first["thread_subject"]
                app.mark_subagent_work_terminal(target, status="completed", output="done")

                app._threads_by_id.clear()
                app._threads_by_pair.clear()
                app._closed_thread_ids.clear()
                app._subagent_work_by_child.clear()
                app._subagent_work_by_parent.clear()

                second = json.loads(
                    await dispatch._send_to_peer_session(
                        target,
                        "after restart",
                        server_client=client,
                        conversation_id=caller,
                        snap_data=snap,
                        sender_identity=identity,
                        thread_id=thread_id,
                    )
                )
                assert second["thread_id"] == thread_id
                assert second["thread_subject"] == subject
                assert len(events) == 2
                assert patch_paths == [
                    f"/v1/sessions/{caller}",
                    f"/v1/sessions/{target}",
                ]
            finally:
                dispatch._runner_online_or_none = old_online
                app._remote_dispatch_start_ref = old_start

    asyncio.run(scenario())
