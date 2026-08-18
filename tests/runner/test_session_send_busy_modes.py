"""Behavioral coverage for busy-session send modes."""

from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from omnigent.runner import app as runner_app
from omnigent.runner.tool_dispatch import execute_tool
from omnigent.tools.builtins.spawn import SysSessionSendTool, _build_sys_session_send_schema

PARENT = "conv_parent"
OTHER_PARENT = "conv_other_parent"
CHILD = "conv_child"
PEER = "conv_peer"


@pytest.fixture
def clean_busy_registry() -> Any:
    saved = {
        "child": dict(runner_app._subagent_work_by_child),
        "parent": {key: set(value) for key, value in runner_app._subagent_work_by_parent.items()},
        "inboxes": dict(runner_app._session_inboxes_ref),
        "children": dict(runner_app._child_session_parents),
        "drained": set(runner_app._drained_delivered_subagent_children),
        "remote": runner_app._remote_dispatch_start_ref,
        "peer_queues": {
            key: list(value) for key, value in runner_app._peer_dispatch_queues.items()
        },
    }
    runner_app._subagent_work_by_child.clear()
    runner_app._subagent_work_by_parent.clear()
    runner_app._session_inboxes_ref.clear()
    runner_app._child_session_parents.clear()
    runner_app._drained_delivered_subagent_children.clear()
    runner_app._remote_dispatch_start_ref = None
    runner_app._peer_dispatch_queues.clear()
    try:
        yield
    finally:
        runner_app._subagent_work_by_child.clear()
        runner_app._subagent_work_by_child.update(saved["child"])
        runner_app._subagent_work_by_parent.clear()
        runner_app._subagent_work_by_parent.update(saved["parent"])
        runner_app._session_inboxes_ref.clear()
        runner_app._session_inboxes_ref.update(saved["inboxes"])
        runner_app._child_session_parents.clear()
        runner_app._child_session_parents.update(saved["children"])
        runner_app._drained_delivered_subagent_children.clear()
        runner_app._drained_delivered_subagent_children.update(saved["drained"])
        runner_app._remote_dispatch_start_ref = saved["remote"]
        runner_app._peer_dispatch_queues.clear()
        for key, callbacks in saved["peer_queues"].items():
            runner_app._peer_dispatch_queues[key] = deque(callbacks)


def _snapshot(
    session_id: str,
    *,
    parent: str | None = PARENT,
    status: str = "running",
    busy: bool | None = None,
    wrapper: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": session_id,
        "agent_id": "ag_worker",
        "agent_name": "worker",
        "sub_agent_name": "worker" if parent is not None else None,
        "parent_session_id": parent,
        "title": "worker:task" if parent is not None else "peer task",
        "status": status,
        "runner_id": "runner_remote" if parent is None else None,
    }
    if busy is not None:
        body["busy"] = busy
    if wrapper is not None:
        body["labels"] = {"omnigent.wrapper": wrapper}
    return body


async def _send_child(
    *,
    target: str = CHILD,
    caller: str = PARENT,
    target_body: dict[str, Any] | None = None,
    arguments: dict[str, Any] | None = None,
    post_hook: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    response_json: dict[str, Any] | None = None,
) -> tuple[str, list[dict[str, Any]], asyncio.Queue[dict[str, Any]]]:
    posted: list[dict[str, Any]] = []
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    target_body = target_body or _snapshot(target, parent=caller)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{caller}":
            return httpx.Response(
                200,
                json={
                    "id": caller,
                    "agent_id": "ag_parent",
                    "agent_name": "parent",
                    "title": "parent",
                    "parent_session_id": None,
                },
            )
        if request.method == "GET" and request.url.path == f"/v1/sessions/{target}":
            return httpx.Response(200, json=target_body)
        if request.method == "GET" and request.url.path == "/v1/runners/runner_remote/status":
            return httpx.Response(200, json={"online": True})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{target}/items":
            return httpx.Response(200, json={"data": []})
        if request.method == "POST" and request.url.path == f"/v1/sessions/{target}/events":
            body = json.loads(request.content)
            posted.append(body)
            if post_hook is not None:
                result = post_hook(body)
                if result is not None:
                    await result
            return httpx.Response(202, json=response_json or {"delivery": "buffered"})
        return httpx.Response(404, json={"error": str(request.url)})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as server_client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps({"session_id": target, "args": "message", **(arguments or {})}),
            server_client=server_client,
            conversation_id=caller,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=inbox,
        )
    return output, posted, inbox


def _posted_texts(posted: list[dict[str, Any]]) -> list[str]:
    return [body["data"]["content"][0]["text"] for body in posted]


def test_if_busy_schema_is_present_with_and_without_named_specs() -> None:
    for specs in ({"worker": SimpleNamespace(description="worker")}, {}):
        schema = _build_sys_session_send_schema(specs)
        prop = schema["function"]["parameters"]["properties"]["if_busy"]
        assert prop["enum"] == ["reject", "queue", "interrupt"]
        assert "queue" in schema["function"]["description"]


def test_named_and_session_send_descriptions_disclose_queue_semantics() -> None:
    descriptions = [
        SysSessionSendTool.description(),
        _build_sys_session_send_schema({})["function"]["description"],
    ]
    for description in descriptions:
        assert "runner restart loses them without an error" in description
        assert "supports_midturn_steer" in description
        assert "Two queue sends coalesce into one work_id and one inbox result" in description
        assert "busy refusal is not a mutex signal" in description
        assert "Returns a handle Queued" not in description


@pytest.mark.asyncio
async def test_i1_queue_owned_child_reuses_work_id_and_posts_envelope(
    clean_busy_registry: None,
) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    output, posted, _ = await _send_child(arguments={"if_busy": "queue"})
    handle = json.loads(output)
    assert handle["work_id"] == entry.work_id
    assert handle["status"] == "queued"
    assert len(posted) == 1
    assert "Message:\nmessage\n" in _posted_texts(posted)[0]


@pytest.mark.asyncio
async def test_i2_queue_completion_delivers_one_inbox_result(clean_busy_registry: None) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    output, _, inbox = await _send_child(arguments={"if_busy": "queue"})
    assert json.loads(output)["work_id"] == entry.work_id
    runner_app.mark_subagent_work_terminal(CHILD, status="completed", output="done")
    result = inbox.get_nowait()
    assert result["work_id"] == entry.work_id
    assert result["status"] == "completed"
    assert inbox.empty()


@pytest.mark.asyncio
async def test_i3_queue_transcript_order_uses_two_real_message_bodies(
    clean_busy_registry: None,
) -> None:
    posted: list[dict[str, Any]] = []
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{CHILD}":
            return httpx.Response(200, json=_snapshot(CHILD, parent=PARENT))
        if request.method == "POST" and request.url.path.endswith("/events"):
            posted.append(json.loads(request.content))
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        first = json.loads(
            await execute_tool(
                tool_name="sys_session_send",
                arguments=json.dumps({"session_id": CHILD, "args": "first"}),
                server_client=client,
                conversation_id=PARENT,
                agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
                session_inbox=inbox,
            )
        )
        second = json.loads(
            await execute_tool(
                tool_name="sys_session_send",
                arguments=json.dumps({"session_id": CHILD, "args": "second", "if_busy": "queue"}),
                server_client=client,
                conversation_id=PARENT,
                agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
                session_inbox=inbox,
            )
        )
    assert first["status"] == "launching"
    assert second["work_id"] == first["work_id"]
    message_texts = [
        _posted_texts(posted)[i].split("Message:\n", 1)[1].split("\n", 1)[0] for i in range(2)
    ]
    assert message_texts == [
        "first",
        "second",
    ]


@pytest.mark.asyncio
async def test_i4_queue_defers_other_dispatcher_without_post(clean_busy_registry: None) -> None:
    active = runner_app.register_subagent_work(
        parent_session_id=OTHER_PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    output, posted, _ = await _send_child(arguments={"if_busy": "queue"})
    handle = json.loads(output)
    assert handle["status"] == "queued"
    assert handle["work_id"] != active.work_id
    assert handle["thread_id"]
    assert posted == []
    assert len(runner_app._peer_dispatch_queues[CHILD]) == 1


@pytest.mark.asyncio
async def test_i5_queue_busy_snapshot_without_entry_registers_fresh_work(
    clean_busy_registry: None,
) -> None:
    output, posted, _ = await _send_child(
        target_body=_snapshot(CHILD, parent=PARENT, busy=True),
        arguments={"if_busy": "queue"},
    )
    handle = json.loads(output)
    assert handle["status"] == "launching"
    assert handle["work_id"]
    assert len(posted) == 1
    assert runner_app.get_subagent_work(CHILD).work_id == handle["work_id"]


@pytest.mark.asyncio
async def test_i6_reject_preserves_pre_u7_busy_snapshot_send(
    clean_busy_registry: None,
) -> None:
    """Reject only guards a tracked dispatch; a busy snapshot alone still posts."""
    output, posted, _ = await _send_child(
        target_body=_snapshot(CHILD, parent=PARENT, busy=True),
        arguments={"if_busy": "reject"},
    )
    assert json.loads(output)["status"] == "launching"
    assert [body["type"] for body in posted] == ["message"]


@pytest.mark.asyncio
async def test_b1_queue_full_server_response_is_typed_for_the_tool(
    clean_busy_registry: None,
) -> None:
    """The tool preserves the server's typed queue refusal and shared entry."""
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path in {
            f"/v1/sessions/{PARENT}",
            f"/v1/sessions/{CHILD}",
        }:
            return httpx.Response(
                200,
                json=(
                    {"id": PARENT, "agent_id": "ag_parent"}
                    if request.url.path.endswith(PARENT)
                    else _snapshot(CHILD, parent=PARENT)
                ),
            )
        if request.method == "POST":
            return httpx.Response(
                429,
                json={
                    "error": {
                        "code": "queue_full",
                        "message": "session message buffer is full",
                    }
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps({"session_id": CHILD, "args": "message", "if_busy": "queue"}),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    info = json.loads(output)
    assert info["error"] == "queue_full"
    assert runner_app.get_subagent_work(CHILD) is entry


@pytest.mark.asyncio
async def test_m4_per_dispatch_queue_cap_refuses_without_post(
    clean_busy_registry: None,
) -> None:
    """One dispatch cannot consume every session-buffer slot."""
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    entry.queued_sends = runner_app._SUBAGENT_QUEUED_SEND_CAP
    output, posted, _ = await _send_child(arguments={"if_busy": "queue"})
    assert json.loads(output)["error"] == "queue_full"
    assert posted == []


@pytest.mark.asyncio
async def test_m4_named_per_dispatch_queue_cap_refuses_without_post(
    clean_busy_registry: None,
) -> None:
    runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    ).queued_sends = runner_app._SUBAGENT_QUEUED_SEND_CAP
    posted: list[dict[str, Any]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}/child_sessions":
            return httpx.Response(200, json={"data": [_snapshot(CHILD, parent=PARENT)]})
        if request.method == "POST":
            posted.append(json.loads(request.content))
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps(
                {"agent": "worker", "title": "task", "args": "message", "if_busy": "queue"}
            ),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    assert json.loads(output)["error"] == "queue_full"
    assert posted == []


@pytest.mark.asyncio
async def test_m4_peer_per_dispatch_queue_cap_refuses_without_post(
    clean_busy_registry: None,
) -> None:
    runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=PEER, agent="worker", title="peer"
    ).queued_sends = runner_app._SUBAGENT_QUEUED_SEND_CAP
    runner_app._remote_dispatch_start_ref = lambda **_: None
    output, posted, _ = await _send_child(
        target=PEER,
        target_body=_snapshot(PEER, parent=None),
        arguments={"if_busy": "queue"},
    )
    assert json.loads(output)["error"] == "queue_full"
    assert posted == []


@pytest.mark.asyncio
async def test_b2_named_interrupt_posts_one_cancel_then_message(
    clean_busy_registry: None,
) -> None:
    """A named interrupt does not repeat cancellation from a stale snapshot."""
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    events: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}/child_sessions":
            return httpx.Response(
                200,
                json={"data": [_snapshot(CHILD, parent=PARENT, busy=True)]},
            )
        if request.method == "POST":
            body = json.loads(request.content)
            events.append(body["type"])
            if body["type"] == "interrupt":
                runner_app.mark_subagent_work_terminal(
                    CHILD, status="cancelled", output="cancelled"
                )
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps(
                {
                    "agent": "worker",
                    "title": "task",
                    "args": "message",
                    "if_busy": "interrupt",
                }
            ),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    assert events == ["interrupt", "message"]
    info = json.loads(output)
    assert info["cancelled_work_id"] == old.work_id
    assert info["steered"] is True


@pytest.mark.asyncio
async def test_c4_named_codex_interrupt_handle_is_best_effort(
    clean_busy_registry: None,
) -> None:
    runner_app.register_subagent_work(
        parent_session_id=PARENT,
        child_session_id=CHILD,
        agent="worker",
        title="task",
        wrapper_label="codex-native-ui",
    )
    events: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}/child_sessions":
            return httpx.Response(
                200,
                json={"data": [_snapshot(CHILD, parent=PARENT, wrapper="codex-native-ui")]},
            )
        if request.method == "POST" and request.url.path == f"/v1/sessions/{CHILD}/events":
            events.append(json.loads(request.content)["type"])
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps(
                {"agent": "worker", "title": "task", "args": "message", "if_busy": "interrupt"}
            ),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    info = json.loads(output)
    assert events == ["interrupt", "message"]
    assert info["steered"] is True
    assert info["best_effort"] is True


@pytest.mark.asyncio
async def test_c4_peer_codex_interrupt_handle_is_best_effort(
    clean_busy_registry: None,
) -> None:
    runner_app.register_subagent_work(
        parent_session_id=PARENT,
        child_session_id=PEER,
        agent="worker",
        title="peer",
        wrapper_label="codex-native-ui",
    )
    runner_app._remote_dispatch_start_ref = lambda **_: None
    cancelled = False

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cancelled
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PEER}":
            return httpx.Response(
                200,
                json=_snapshot(
                    PEER,
                    parent=None,
                    status="idle" if cancelled else "running",
                    wrapper="codex-native-ui",
                ),
            )
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PEER}/items":
            return httpx.Response(200, json={"data": []})
        if request.method == "GET" and request.url.path == "/v1/runners/runner_remote/status":
            return httpx.Response(200, json={"online": True})
        if request.method == "POST":
            body = json.loads(request.content)
            if body["type"] == "interrupt":
                cancelled = True
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps({"session_id": PEER, "args": "message", "if_busy": "interrupt"}),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    info = json.loads(output)
    assert info["steered"] is True
    assert info["best_effort"] is True


@pytest.mark.asyncio
async def test_h2_peer_transport_failure_keeps_shared_entry(
    clean_busy_registry: None,
) -> None:
    """A queued peer POST failure cannot destroy the original poller's entry."""
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=PEER, agent="worker", title="peer"
    )
    runner_app._remote_dispatch_start_ref = lambda **_: None

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PEER}":
            return httpx.Response(200, json=_snapshot(PEER, parent=None))
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PEER}/items":
            return httpx.Response(200, json={"data": []})
        if request.method == "GET" and request.url.path == "/v1/runners/runner_remote/status":
            return httpx.Response(200, json={"online": True})
        if request.method == "POST":
            raise httpx.ReadTimeout("peer unavailable")
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps({"session_id": PEER, "args": "message", "if_busy": "queue"}),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    assert "ReadTimeout" in output
    assert runner_app.get_subagent_work(PEER) is entry


@pytest.mark.asyncio
async def test_h3_peer_interrupt_confirms_idle_then_posts_message(
    clean_busy_registry: None,
) -> None:
    """An authorized peer interrupt treats the peer's idle edge as confirmation."""
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=PEER, agent="worker", title="peer"
    )
    runner_app._remote_dispatch_start_ref = lambda **_: None
    events: list[str] = []
    cancelled = False

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cancelled
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PEER}":
            snapshot = _snapshot(PEER, parent=None, status="idle" if cancelled else "running")
            return httpx.Response(200, json=snapshot)
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PEER}/items":
            return httpx.Response(200, json={"data": []})
        if request.method == "GET" and request.url.path == "/v1/runners/runner_remote/status":
            return httpx.Response(200, json={"online": True})
        if request.method == "POST":
            body = json.loads(request.content)
            events.append(body["type"])
            if body["type"] == "interrupt":
                cancelled = True
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps({"session_id": PEER, "args": "message", "if_busy": "interrupt"}),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    info = json.loads(output)
    assert events == ["interrupt", "message"]
    assert info["cancelled_work_id"] == old.work_id
    assert info["steered"] is True


@pytest.mark.asyncio
async def test_i6_accepted_ack_reconciles_a_terminalized_coalesce(
    clean_busy_registry: None,
) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )

    async def terminalize(_: dict[str, Any]) -> None:
        runner_app.mark_subagent_work_terminal(CHILD, status="completed", output="old")

    output, posted, _ = await _send_child(
        arguments={"if_busy": "queue"},
        response_json={"delivery": "accepted"},
        post_hook=terminalize,
    )
    handle = json.loads(output)
    assert handle["work_id"] != old.work_id
    assert len(posted) == 1
    assert runner_app.get_subagent_work(CHILD).work_id == handle["work_id"]


@pytest.mark.asyncio
async def test_c1_named_path_reconciles_accepted_ack_after_terminalization(
    clean_busy_registry: None,
) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}/child_sessions":
            return httpx.Response(200, json={"data": [_snapshot(CHILD, parent=PARENT)]})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{CHILD}/items":
            return httpx.Response(200, json={"data": []})
        if request.method == "POST" and request.url.path == f"/v1/sessions/{CHILD}/events":
            runner_app.mark_subagent_work_terminal(CHILD, status="completed", output="old")
            return httpx.Response(202, json={"delivery": "accepted"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps({"agent": "worker", "title": "task", "args": "message"}),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    handle = json.loads(output)
    assert handle["work_id"] != old.work_id
    assert runner_app.get_subagent_work(CHILD).work_id == handle["work_id"]


@pytest.mark.asyncio
async def test_c1_named_path_reconciles_buffered_ack_after_terminalization(
    clean_busy_registry: None,
) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.method == "GET" and request.url.path == f"/v1/sessions/{PARENT}/child_sessions":
            return httpx.Response(200, json={"data": [_snapshot(CHILD, parent=PARENT)]})
        if request.method == "POST" and request.url.path == f"/v1/sessions/{CHILD}/events":
            runner_app.mark_subagent_work_terminal(CHILD, status="completed", output="old")
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        output = await execute_tool(
            tool_name="sys_session_send",
            arguments=json.dumps({"agent": "worker", "title": "task", "args": "message"}),
            server_client=client,
            conversation_id=PARENT,
            agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
            session_inbox=asyncio.Queue(),
        )
    handle = json.loads(output)
    assert handle["work_id"] != old.work_id
    assert "error" not in handle
    assert runner_app.get_subagent_work(CHILD).work_id == handle["work_id"]


@pytest.mark.asyncio
async def test_c1_peer_path_reconciles_buffered_ack_after_terminalization(
    clean_busy_registry: None,
) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=PEER, agent="worker", title="peer"
    )
    runner_app._remote_dispatch_start_ref = lambda **_: None

    async def terminalize(_: dict[str, Any]) -> None:
        runner_app.mark_subagent_work_terminal(PEER, status="completed", output="old")

    output, _, _ = await _send_child(
        target=PEER,
        target_body=_snapshot(PEER, parent=None),
        arguments={"if_busy": "queue"},
        response_json={"delivery": "buffered"},
        post_hook=terminalize,
    )
    handle = json.loads(output)
    assert handle["work_id"] != old.work_id
    assert "error" not in handle
    assert runner_app.get_subagent_work(PEER).work_id == handle["work_id"]


@pytest.mark.asyncio
async def test_c3_buffered_ack_reconciles_terminalized_coalesce(
    clean_busy_registry: None,
) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )

    async def terminalize(_: dict[str, Any]) -> None:
        runner_app.mark_subagent_work_terminal(CHILD, status="completed", output="old")

    output, _, _ = await _send_child(
        arguments={"if_busy": "queue"},
        response_json={"delivery": "buffered"},
        post_hook=terminalize,
    )
    handle = json.loads(output)
    assert handle["work_id"] != old.work_id
    assert "error" not in handle
    assert runner_app.get_subagent_work(CHILD).work_id == handle["work_id"]


@pytest.mark.asyncio
async def test_i7_ack_absence_is_tolerated_on_coalesced_send(clean_busy_registry: None) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    output, _, _ = await _send_child(
        arguments={"if_busy": "queue"}, response_json={"queued": True}
    )
    assert json.loads(output)["work_id"] == old.work_id


@pytest.mark.asyncio
async def test_i8_denial_preserves_outstanding_coalesced_entry(clean_busy_registry: None) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    output, posted, _ = await _send_child(
        arguments={"if_busy": "queue"},
        response_json={"queued": False, "denied": True, "reason": "policy"},
    )
    info = json.loads(output)
    assert info["error"] == "message_denied"
    assert posted
    assert runner_app.get_subagent_work(CHILD).work_id == old.work_id


@pytest.mark.asyncio
async def test_i9_interrupt_cancels_then_posts_fresh_dispatch(clean_busy_registry: None) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    events: list[str] = []

    async def cancel_then_terminalize(body: dict[str, Any]) -> None:
        event_type = body.get("type")
        events.append(event_type)
        if event_type == "interrupt":
            runner_app.mark_subagent_work_terminal(CHILD, status="cancelled", output="cancelled")

    output, posted, _ = await _send_child(
        arguments={"if_busy": "interrupt"}, post_hook=cancel_then_terminalize
    )
    info = json.loads(output)
    assert events == ["interrupt", "message"]
    assert len(posted) == 2
    assert info["cancelled_work_id"] == old.work_id
    assert info["work_id"] != old.work_id
    assert info["steered"] is True


@pytest.mark.asyncio
async def test_i10_unconfirmed_interrupt_does_not_post_message(
    clean_busy_registry: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    monkeypatch.setattr(
        "omnigent.runner.tool_dispatch._SUBAGENT_INTERRUPT_CONFIRM_TIMEOUT_S", 0.001
    )
    output, posted, _ = await _send_child(arguments={"if_busy": "interrupt"})
    assert json.loads(output)["error"] == "interrupt_unconfirmed"
    assert [body["type"] for body in posted] == ["interrupt"]


@pytest.mark.asyncio
async def test_i11_interrupt_refuses_unauthorized_dispatch(clean_busy_registry: None) -> None:
    runner_app.register_subagent_work(
        parent_session_id=OTHER_PARENT, child_session_id=PEER, agent="worker", title="task"
    )
    runner_app._remote_dispatch_start_ref = lambda **_: None
    output, posted, _ = await _send_child(
        target=PEER,
        target_body=_snapshot(PEER, parent=None, status="running"),
        arguments={"if_busy": "interrupt"},
    )
    assert json.loads(output)["error"] == "cancel_not_authorized"
    assert posted == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wrapper", "expected"),
    [("claude-code-native-ui", "stop_session"), ("codex-native-ui", "interrupt")],
)
async def test_i12_interrupt_routes_native_cancel_event(
    wrapper: str,
    expected: str,
    clean_busy_registry: None,
) -> None:
    runner_app.register_subagent_work(
        parent_session_id=PARENT,
        child_session_id=CHILD,
        agent="worker",
        title="task",
        wrapper_label=wrapper,
    )
    events: list[str] = []
    cancel_bodies: list[dict[str, Any]] = []

    async def cancel(body: dict[str, Any]) -> None:
        events.append(body["type"])
        cancel_bodies.append(body)
        if body["type"] == expected and wrapper == "claude-code-native-ui":
            runner_app.mark_subagent_work_terminal(CHILD, status="cancelled", output="cancelled")

    output, _, _ = await _send_child(
        target_body=_snapshot(CHILD, parent=PARENT, wrapper=wrapper),
        arguments={"if_busy": "interrupt"},
        post_hook=cancel,
    )
    info = json.loads(output)
    assert info["cancelled_work_id"]
    if wrapper == "codex-native-ui":
        assert info["best_effort"] is True
        assert cancel_bodies[0]["data"] == {}
    assert events[0] == expected


@pytest.mark.asyncio
async def test_c1_two_coalesced_sends_share_entry_and_result(clean_busy_registry: None) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    outputs: list[str] = []
    posted: list[dict[str, Any]] = []
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    first_post_started = asyncio.Event()
    second_post_started = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/v1/sessions/{PARENT}":
            return httpx.Response(200, json={"id": PARENT, "agent_id": "ag_parent"})
        if request.url.path == f"/v1/sessions/{CHILD}":
            return httpx.Response(200, json=_snapshot(CHILD, parent=PARENT))
        if request.method == "POST":
            posted.append(json.loads(request.content))
            if len(posted) == 1:
                first_post_started.set()
                await second_post_started.wait()
            elif len(posted) == 2:
                second_post_started.set()
            return httpx.Response(202, json={"delivery": "buffered"})
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        outputs = await asyncio.gather(
            *(
                execute_tool(
                    tool_name="sys_session_send",
                    arguments=json.dumps({"session_id": CHILD, "args": text, "if_busy": "queue"}),
                    server_client=client,
                    conversation_id=PARENT,
                    agent_spec=SimpleNamespace(sub_agents=[SimpleNamespace(name="worker")]),
                    session_inbox=inbox,
                )
                for text in ("one", "two")
            )
        )
    assert all(json.loads(value)["work_id"] == entry.work_id for value in outputs)
    assert len(posted) == 2
    runner_app.mark_subagent_work_terminal(CHILD, status="completed", output="done")
    assert inbox.qsize() == 1


@pytest.mark.asyncio
async def test_c2_terminal_edge_ack_chooses_fresh_entry(clean_busy_registry: None) -> None:
    old = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )

    async def terminalize(_: dict[str, Any]) -> None:
        runner_app.mark_subagent_work_terminal(CHILD, status="completed", output="old")

    output, _, _ = await _send_child(
        arguments={"if_busy": "queue"},
        response_json={"delivery": "accepted"},
        post_hook=terminalize,
    )
    assert json.loads(output)["work_id"] != old.work_id


@pytest.mark.asyncio
async def test_c3_two_parents_only_owner_can_coalesce(clean_busy_registry: None) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    runner_app._remote_dispatch_start_ref = lambda **_: None
    outputs = await asyncio.gather(
        _send_child(arguments={"if_busy": "queue"}),
        _send_child(
            caller=OTHER_PARENT,
            target_body={**_snapshot(CHILD, parent=PARENT), "runner_id": "runner_remote"},
            arguments={"if_busy": "queue"},
        ),
    )
    infos = [json.loads(result[0]) for result in outputs]
    assert any(info.get("work_id") == entry.work_id for info in infos)
    queued = [info for info in infos if info.get("work_id") != entry.work_id]
    assert len(queued) == 1, infos
    assert queued[0]["status"] == "queued"
    assert queued[0]["queued"] is True


@pytest.mark.asyncio
async def test_u5_child_default_is_queue_and_never_implicit_interrupt(
    clean_busy_registry: None,
) -> None:
    entry = runner_app.register_subagent_work(
        parent_session_id=PARENT, child_session_id=CHILD, agent="worker", title="task"
    )
    output, posted, _ = await _send_child()
    assert json.loads(output)["work_id"] == entry.work_id
    assert [body["type"] for body in posted] == ["message"]


@pytest.mark.asyncio
async def test_u5_peer_default_is_queue(clean_busy_registry: None) -> None:
    """A busy peer queues by default so a stale status cannot bounce a send."""
    runner_app._remote_dispatch_start_ref = lambda **_: None
    output, posted, _ = await _send_child(
        target=PEER,
        target_body=_snapshot(PEER, parent=None, status="running"),
    )
    assert "error" not in json.loads(output)
    assert [body["type"] for body in posted] == ["message"]


@pytest.mark.asyncio
async def test_u5_peer_reject_still_available(clean_busy_registry: None) -> None:
    """Explicit reject preserves the old refusal for a caller that wants it."""
    runner_app._remote_dispatch_start_ref = lambda **_: None
    output, posted, _ = await _send_child(
        target=PEER,
        target_body=_snapshot(PEER, parent=None, status="running"),
        arguments={"if_busy": "reject"},
    )
    assert json.loads(output)["error"] == "session_busy"
    assert posted == []
