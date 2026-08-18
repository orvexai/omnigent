"""Focused tests for the Stage 1 thread-scoped inbox drain."""

from __future__ import annotations

import asyncio

import httpx

from omnigent.runner.tool_dispatch import _drain_inbox


def _subagent(thread_id: str | None, handle: str, output: str = "done") -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "sub_agent",
        "work_id": f"work-{handle}",
        "handle_id": handle,
        "task_id": handle,
        "conversation_id": handle,
        "tool_name": "worker",
        "agent": "worker",
        "title": handle,
        "status": "completed",
        "output": output,
    }
    if thread_id is not None:
        payload["thread_id"] = thread_id
    return payload


def test_filtered_drain_consumes_only_one_thread_and_preserves_other_items() -> None:
    async def scenario() -> None:
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"result": "POLICY_ACTION_ALLOW"})
            ),
            base_url="http://test",
        )
        inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        other_first = _subagent("th_other", "other-1")
        wanted = _subagent("th_wanted", "wanted")
        other_second = _subagent("th_other", "other-2")
        for payload in (other_first, wanted, other_second):
            inbox.put_nowait(payload)

        try:
            result = await _drain_inbox(
                inbox,
                server_client=client,
                conversation_id="parent",
                thread_id="th_wanted",
            )

            assert "wanted" in result
            assert inbox.get_nowait() is other_first
            assert inbox.get_nowait() is other_second
        finally:
            await client.aclose()

    asyncio.run(scenario())


def test_filtered_empty_sentinel_distinguishes_other_threads_from_empty() -> None:
    async def scenario() -> None:
        inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        inbox.put_nowait(_subagent("th_other", "other"))

        with_other = await _drain_inbox(inbox, thread_id="th_wanted")
        assert "no messages on thread th_wanted" in with_other
        assert "1 message(s) remain on other threads" in with_other
        assert "th_other" in with_other

        empty: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        assert (
            await _drain_inbox(empty, thread_id="th_wanted")
            == "[System: no messages on thread th_wanted; inbox is empty.]"
        )
        assert await _drain_inbox(asyncio.Queue()) == "Inbox is empty — no completed tasks."

    asyncio.run(scenario())


def test_filter_is_applied_before_policy_evaluation_and_keeps_unthreaded_items() -> None:
    async def scenario() -> None:
        policy_calls: list[object] = []

        def handler(request: httpx.Request) -> httpx.Response:
            policy_calls.append(request.url.path)
            return httpx.Response(200, json={"result": "POLICY_ACTION_ALLOW"})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://test")
        try:
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            skipped = _subagent("th_other", "skipped")
            unthreaded = _subagent(None, "unthreaded")
            wanted = _subagent("th_wanted", "wanted")
            for payload in (skipped, unthreaded, wanted):
                inbox.put_nowait(payload)

            result = await _drain_inbox(
                inbox,
                server_client=client,
                conversation_id="parent",
                thread_id="th_wanted",
            )

            assert "wanted" in result
            assert "[System: sub-agent task unthreaded" not in result
            assert policy_calls == [
                "/v1/sessions/parent/policies/evaluate",
            ]
            assert inbox.get_nowait() is skipped
            assert inbox.get_nowait() is unthreaded
        finally:
            await client.aclose()

    asyncio.run(scenario())


def test_unfiltered_drain_groups_threaded_results_without_changing_legacy_output() -> None:
    async def scenario() -> None:
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"result": "POLICY_ACTION_ALLOW"})
            ),
            base_url="http://test",
        )
        inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        for payload in (
            _subagent("th_a", "a"),
            _subagent("th_b", "b"),
            _subagent("th_a", "a-2"),
        ):
            inbox.put_nowait(payload)
        try:
            result = await _drain_inbox(inbox, server_client=client, conversation_id="parent")
        finally:
            await client.aclose()

        assert result.index("[System: thread th_a]") < result.index("[System: thread th_b]")
        assert result.index("a") < result.index("a-2")

        empty = asyncio.Queue()
        assert await _drain_inbox(empty) == "Inbox is empty — no completed tasks."

    asyncio.run(scenario())


def test_filtered_budget_sentinel_counts_deferred_filtered_and_tail_items() -> None:
    async def scenario() -> None:
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"result": "POLICY_ACTION_ALLOW"})
            ),
            base_url="http://test",
        )
        inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        for payload in (
            _subagent("th_wanted", "wanted-1", "x" * 12000),
            _subagent("th_other", "other-1"),
            _subagent("th_wanted", "wanted-2", "x" * 12000),
            _subagent("th_other", "other-2"),
            _subagent("th_wanted", "wanted-3", "x" * 12000),
            _subagent("th_wanted", "wanted-4", "x" * 12000),
        ):
            inbox.put_nowait(payload)
        try:
            result = await _drain_inbox(
                inbox,
                server_client=client,
                conversation_id="parent",
                thread_id="th_wanted",
            )
        finally:
            await client.aclose()

        assert "3 message(s) remain queued" in result
        assert inbox.qsize() == 3
        assert [inbox.get_nowait()["handle_id"] for _ in range(3)] == [
            "other-1",
            "other-2",
            "wanted-4",
        ]

    asyncio.run(scenario())
