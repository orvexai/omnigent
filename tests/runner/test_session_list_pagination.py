"""The child-list pagination seam, lifted from the production repro."""

from __future__ import annotations

import base64
import json
from typing import cast

import httpx
import pytest

from omnigent.runner.tool_dispatch import _collect_sub_agents


def _handler(own_n: int, sib_n: int, caller_is_child: bool):
    own = [
        {
            "id": f"own-{i:04}",
            "title": f"worker:own-{i:04}",
            "tool": "worker",
            "session_name": f"own-{i:04}",
            "sub_agent_name": "worker",
        }
        for i in range(own_n)
    ]
    siblings = [
        {
            "id": f"sib-{i:04}",
            "title": f"worker:sib-{i:04}",
            "tool": "worker",
            "session_name": f"sib-{i:04}",
            "sub_agent_name": "worker",
        }
        for i in range(sib_n)
    ]

    def page(rows: list[dict[str, str]], after: str | None, limit: int) -> dict[str, object]:
        start = (
            0
            if after is None
            else next((i + 1 for i, row in enumerate(rows) if row["id"] == after), len(rows))
        )
        chunk = rows[start : start + limit]
        end = start + len(chunk)
        return {
            "data": chunk,
            "has_more": end < len(rows),
            "last_id": chunk[-1]["id"] if chunk else None,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        query = request.url.params
        if path == "/v1/sessions/caller":
            return httpx.Response(
                200,
                json={"id": "caller", "parent_session_id": "parent" if caller_is_child else None},
            )
        if path == "/v1/sessions/caller/child_sessions":
            return httpx.Response(200, json=page(own, query.get("after"), int(query["limit"])))
        if path == "/v1/sessions/parent/child_sessions":
            return httpx.Response(
                200, json=page(siblings, query.get("after"), int(query["limit"]))
            )
        raise AssertionError(f"unexpected request: {request.url}")

    return handler


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [5, 100])
@pytest.mark.parametrize("own_case", ["zero", "minus_one", "exact", "plus_one", "double"])
@pytest.mark.parametrize("sib_offset", [0, 1, 2])
@pytest.mark.parametrize("caller_is_child", [True, False])
async def test_child_pages_are_exhaustive_at_the_seam(
    limit: int,
    own_case: str,
    sib_offset: int,
    caller_is_child: bool,
) -> None:
    own_n = {
        "zero": 0,
        "minus_one": limit - 1,
        "exact": limit,
        "plus_one": limit + 1,
        "double": 2 * limit,
    }[own_case]
    sib_n = (0, 1, limit + 1)[sib_offset]
    expected = {*(f"own-{i:04}" for i in range(own_n))}
    if caller_is_child:
        expected.update(f"sib-{i:04}" for i in range(sib_n))
        expected.add("parent")

    seen: list[str] = []
    first_rows: list[dict[str, object]] = []
    after = None
    pages = 0
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(own_n, sib_n, caller_is_child)),
        base_url="http://server",
    ) as client:
        while pages <= ((len(expected) + limit - 1) // limit) + 1:
            rows, page = await _collect_sub_agents("caller", client, limit=limit, after=after)
            pages += 1
            assert len(rows) <= limit
            if not first_rows:
                first_rows = rows
            seen.extend(row["session_id"] for row in rows)
            if not page["has_more"]:
                break
            after = page["next_after"]
        else:
            pytest.fail("child cursor did not terminate")

    assert set(seen) == expected
    assert len(seen) == len(set(seen))
    assert seen.count("parent") == (1 if caller_is_child else 0)
    if own_case == "zero" and caller_is_child:
        assert first_rows[0] == {
            "agent": "main",
            "agent_name": "main",
            "title": None,
            "conversation_id": "parent",
            "session_id": "parent",
            "busy": False,
            "current_task_status": None,
            "status": None,
        }


@pytest.mark.asyncio
async def test_cursor_replays_the_same_page_without_running_ahead() -> None:
    handler = _handler(10, 0, True)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://server",
    ) as client:
        first, first_page = await _collect_sub_agents("caller", client, limit=5)
        replay, replay_page = await _collect_sub_agents("caller", client, limit=5)
        second, _ = await _collect_sub_agents(
            "caller", client, limit=5, after=first_page["next_after"]
        )
    assert replay == first
    assert replay_page == first_page
    assert {row["session_id"] for row in first}.isdisjoint(row["session_id"] for row in second)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cursor",
    [
        "garbage",
        "v2:not-base64",
        "v2:e30",
        123,
        "v2:" + base64.urlsafe_b64encode(b"not-json").decode(),
        "v1:e30",
    ],
)
async def test_malformed_cursor_restarts_safely(cursor: object) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(1, 0, True)),
        base_url="http://server",
    ) as client:
        rows, _ = await _collect_sub_agents(
            "caller", client, limit=5, after=cast(str | None, cursor)
        )
    assert rows[0]["session_id"] == "parent"


def _encoded_cursor(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return "v2:" + base64.urlsafe_b64encode(raw).decode().rstrip("=")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("v", "2"),
        ("phase", 1),
        ("main_pending", "false"),
        ("own_after", 0),
        ("sibling_after", False),
    ],
)
async def test_cursor_with_wrong_member_type_restarts_atomically(
    field: str, value: object
) -> None:
    payload: dict[str, object] = {
        "v": 2,
        "phase": "siblings",
        "own_after": "own-000",
        "sibling_after": "sib-000",
        "main_pending": False,
    }
    payload[field] = value
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(1, 1, True)),
        base_url="http://server",
    ) as client:
        rows, _ = await _collect_sub_agents(
            "caller", client, limit=5, after=_encoded_cursor(payload)
        )
    assert rows[0]["session_id"] == "parent"
