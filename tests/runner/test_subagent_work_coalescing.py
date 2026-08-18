"""Focused tests for coalesced sub-agent dispatch bookkeeping."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from omnigent.runner import app as runner_app
from omnigent.runner import create_runner_app


@pytest.fixture
def clean_registry():
    saved_child = dict(runner_app._subagent_work_by_child)
    saved_parent = {key: set(value) for key, value in runner_app._subagent_work_by_parent.items()}
    runner_app._subagent_work_by_child.clear()
    runner_app._subagent_work_by_parent.clear()
    try:
        yield
    finally:
        runner_app._subagent_work_by_child.clear()
        runner_app._subagent_work_by_child.update(saved_child)
        runner_app._subagent_work_by_parent.clear()
        runner_app._subagent_work_by_parent.update(saved_parent)


def test_note_subagent_work_send_rejects_stale_work_id(clean_registry) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id="parent",
        child_session_id="child",
        agent="worker",
        title="task",
    )

    assert (
        runner_app.note_subagent_work_send(
            "child", work_id="stale", sent_text="ignored", anchor_item_id="old"
        )
        is None
    )
    assert entry.queued_sends == 0
    assert entry.last_sent_text is None
    assert entry.last_anchor_item_id is None


def test_note_subagent_work_send_reanchors_live_dispatch(clean_registry) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id="parent",
        child_session_id="child",
        agent="worker",
        title="task",
    )

    assert (
        runner_app.note_subagent_work_send(
            "child",
            work_id=entry.work_id,
            sent_text="<message>second</message>",
            anchor_item_id="item_one",
        )
        is entry
    )
    assert entry.queued_sends == 1
    assert entry.last_sent_text == "<message>second</message>"
    assert entry.last_anchor_item_id == "item_one"


def test_note_subagent_work_send_without_anchor_preserves_existing_anchor(clean_registry) -> None:
    """Child-path bookkeeping without an anchor must not erase the poller's anchor."""
    entry = runner_app.register_subagent_work(
        parent_session_id="parent", child_session_id="child", agent="worker", title="task"
    )
    runner_app.note_subagent_work_send(
        "child", work_id=entry.work_id, sent_text="first", anchor_item_id="item_one"
    )
    runner_app.note_subagent_work_send("child", work_id=entry.work_id, sent_text="second")
    assert entry.last_anchor_item_id == "item_one"


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
def test_note_subagent_work_send_rejects_terminal_entry(clean_registry, status: str) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id="parent", child_session_id="child", agent="worker", title="task"
    )
    entry.status = status

    assert runner_app.note_subagent_work_send("child", work_id=entry.work_id) is None
    assert entry.queued_sends == 0


def test_note_subagent_work_send_rejects_dispatch_cap(clean_registry) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id="parent", child_session_id="child", agent="worker", title="task"
    )
    entry.queued_sends = runner_app._SUBAGENT_QUEUED_SEND_CAP

    assert runner_app.note_subagent_work_send("child", work_id=entry.work_id) is None
    assert entry.queued_sends == runner_app._SUBAGENT_QUEUED_SEND_CAP


async def _run_reanchor_poll() -> list[dict[str, str | None]]:
    child = "conv_remote_reanchor"
    parent = "conv_remote_parent"
    inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    queries: list[dict[str, str | None]] = []
    second_poll = asyncio.Event()
    old_text = "<omnigent-agent-message>old</omnigent-agent-message>"
    new_text = "<omnigent-agent-message>new</omnigent-agent-message>"
    entry = runner_app.register_subagent_work(
        parent_session_id=parent, child_session_id=child, agent="worker", title="task"
    )
    runner_app._session_inboxes_ref[parent] = inbox
    runner_app.note_subagent_work_send(
        child, work_id=entry.work_id, sent_text=old_text, anchor_item_id="item_old"
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/items"):
            query = {
                "after": request.url.params.get("after"),
                "order": request.url.params.get("order"),
            }
            queries.append(query)
            if len(queries) == 1:
                runner_app.note_subagent_work_send(
                    child,
                    work_id=entry.work_id,
                    sent_text=new_text,
                    anchor_item_id="item_new_anchor",
                )
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            {
                                "id": "item_old",
                                "role": "user",
                                "content": [{"text": old_text}],
                            },
                            {
                                "id": "item_old_reply",
                                "role": "assistant",
                                "content": [{"text": "reply after first"}],
                            },
                        ]
                    },
                )
            second_poll.set()
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "item_new",
                            "role": "user",
                            "content": [{"text": new_text}],
                        },
                        {
                            "id": "item_reply",
                            "role": "assistant",
                            "content": [{"text": "reply after second"}],
                        },
                    ]
                },
            )
        if request.method == "GET" and request.url.path == f"/v1/sessions/{child}":
            return httpx.Response(200, json={"status": "idle"})
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://server")
    create_runner_app(server_client=client)  # type: ignore[arg-type]
    old_interval = runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S
    runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S = 0.001
    try:
        assert runner_app._remote_dispatch_start_ref is not None
        runner_app._remote_dispatch_start_ref(
            child_session_id=child,
            work_id=entry.work_id,
            anchor_item_id="item_old",
            sent_text=old_text,
        )
        await asyncio.wait_for(second_poll.wait(), timeout=1.0)
        for _ in range(100):
            if not inbox.empty():
                break
            await asyncio.sleep(0.001)
        assert not inbox.empty()
        return queries
    finally:
        runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S = old_interval
        task = runner_app._remote_dispatch_tasks.pop(child, None)
        if task is not None and not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        runner_app._remote_dispatch_start_ref = None
        runner_app.unregister_subagent_work(child)
        runner_app._session_inboxes_ref.pop(parent, None)
        await client.aclose()


@pytest.mark.asyncio
async def test_u2_remote_poller_reanchors_after_coalesced_send(clean_registry) -> None:
    """The poller's next transcript query follows the newest message anchor."""
    queries = await _run_reanchor_poll()
    assert queries[0]["after"] == "item_old"
    assert queries[1]["after"] == "item_new_anchor"


@pytest.mark.asyncio
async def test_f5_remote_poller_does_not_finish_on_turn_one_reply(clean_registry) -> None:
    """A stale first-turn page cannot terminalize a dispatch after re-anchor."""
    queries = await _run_reanchor_poll()
    assert len(queries) >= 2
    assert queries[1]["after"] == "item_new_anchor"


@pytest.mark.asyncio
async def test_remote_poller_reaches_reply_after_more_than_200_items(clean_registry) -> None:
    """A long peer turn is paged until its assistant reply is visible."""
    child = "conv_remote_long_turn"
    parent = "conv_remote_long_parent"
    sent_text = "<omnigent-agent-message>long task</omnigent-agent-message>"
    inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    queries: list[str | None] = []
    entry = runner_app.register_subagent_work(
        parent_session_id=parent, child_session_id=child, agent="worker", title="task"
    )
    runner_app._session_inboxes_ref[parent] = inbox
    runner_app.note_subagent_work_send(
        child, work_id=entry.work_id, sent_text=sent_text, anchor_item_id="item_anchor"
    )

    first_page = [
        {
            "id": "item_sent",
            "role": "user",
            "content": [{"text": sent_text}],
        },
        *[
            {
                "id": f"item_tool_{index}",
                "role": "tool",
                "content": [{"text": f"tool output {index}"}],
            }
            for index in range(199)
        ],
    ]
    second_page = [
        {
            "id": "item_tool_199",
            "role": "tool",
            "content": [{"text": "tool output 199"}],
        },
        {
            "id": "item_reply",
            "role": "assistant",
            "content": [{"text": "long-turn final answer"}],
        },
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/items"):
            after = request.url.params.get("after")
            queries.append(after)
            if after == "item_anchor":
                return httpx.Response(
                    200,
                    json={
                        "data": first_page,
                        "last_id": "item_tool_198",
                        "has_more": True,
                    },
                )
            if after == "item_tool_198":
                return httpx.Response(
                    200,
                    json={"data": second_page, "last_id": "item_reply", "has_more": False},
                )
            return httpx.Response(200, json={"data": [], "has_more": False})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{child}":
            return httpx.Response(200, json={"status": "idle"})
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://server")
    create_runner_app(server_client=client)  # type: ignore[arg-type]
    old_interval = runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S
    runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S = 0.001
    try:
        assert runner_app._remote_dispatch_start_ref is not None
        runner_app._remote_dispatch_start_ref(
            child_session_id=child,
            work_id=entry.work_id,
            anchor_item_id="item_anchor",
            sent_text=sent_text,
        )
        delivered = await asyncio.wait_for(inbox.get(), timeout=1.0)
        assert "long-turn final answer" in repr(delivered)
        assert queries[:2] == ["item_anchor", "item_tool_198"]
    finally:
        runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S = old_interval
        task = runner_app._remote_dispatch_tasks.pop(child, None)
        if task is not None and not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        runner_app._remote_dispatch_start_ref = None
        runner_app.unregister_subagent_work(child)
        runner_app._session_inboxes_ref.pop(parent, None)
        await client.aclose()


@pytest.mark.asyncio
async def test_remote_poller_reports_idle_peer_without_assistant_text(clean_registry) -> None:
    """An ingested message with no assistant text is a prompt terminal failure."""
    child = "conv_remote_no_text"
    parent = "conv_remote_no_text_parent"
    sent_text = "<omnigent-agent-message>write the file</omnigent-agent-message>"
    inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    entry = runner_app.register_subagent_work(
        parent_session_id=parent, child_session_id=child, agent="worker", title="task"
    )
    runner_app._session_inboxes_ref[parent] = inbox
    runner_app.note_subagent_work_send(
        child, work_id=entry.work_id, sent_text=sent_text, anchor_item_id="item_anchor"
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/items"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "item_sent",
                            "role": "user",
                            "content": [{"text": sent_text}],
                        },
                        {
                            "id": "item_tool",
                            "role": "tool",
                            "content": [{"text": "file written"}],
                        },
                    ],
                    "last_id": "item_tool",
                    "has_more": False,
                },
            )
        if request.method == "GET" and request.url.path == f"/v1/sessions/{child}":
            return httpx.Response(200, json={"status": "idle"})
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://server")
    create_runner_app(server_client=client)  # type: ignore[arg-type]
    old_interval = runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S
    runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S = 0.001
    try:
        assert runner_app._remote_dispatch_start_ref is not None
        runner_app._remote_dispatch_start_ref(
            child_session_id=child,
            work_id=entry.work_id,
            anchor_item_id="item_anchor",
            sent_text=sent_text,
        )
        delivered = await asyncio.wait_for(inbox.get(), timeout=1.0)
        assert delivered["status"] == "failed"
        assert "no assistant text" in str(delivered["output"])
    finally:
        runner_app._REMOTE_DISPATCH_POLL_INTERVAL_S = old_interval
        task = runner_app._remote_dispatch_tasks.pop(child, None)
        if task is not None and not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        runner_app._remote_dispatch_start_ref = None
        runner_app.unregister_subagent_work(child)
        runner_app._session_inboxes_ref.pop(parent, None)
        await client.aclose()
