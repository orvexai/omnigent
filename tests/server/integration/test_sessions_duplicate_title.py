"""Sibling titles are unique per parent; a repeat must not 500."""

from __future__ import annotations

import httpx
import pytest

from tests.server.helpers import create_test_agent

pytestmark = pytest.mark.asyncio


async def _create_child(
    client: httpx.AsyncClient,
    *,
    agent_id: str,
    parent_id: str,
    title: str,
) -> httpx.Response:
    return await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "parent_session_id": parent_id, "title": title},
    )


async def test_duplicate_child_title_returns_409_not_500(client: httpx.AsyncClient) -> None:
    """
    A second child with a title a sibling already holds returns 409.

    The store raises a typed name clash, which used to reach FastAPI
    unhandled and surface as an opaque 500 — leaving the caller with no
    way to tell a title collision from a server fault.
    """
    agent = await create_test_agent(client, name="dup-title-agent")
    parent = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert parent.status_code == 201, parent.text
    parent_id = parent.json()["id"]

    first = await _create_child(client, agent_id=agent["id"], parent_id=parent_id, title="worker")
    assert first.status_code == 201, first.text

    second = await _create_child(client, agent_id=agent["id"], parent_id=parent_id, title="worker")
    assert second.status_code == 409, second.text
    body = second.json()
    assert body["error"]["code"] == "already_exists"
    # The message must name the title so the caller can pick another.
    assert "worker" in body["error"]["message"]


async def test_same_title_under_different_parents_is_allowed(client: httpx.AsyncClient) -> None:
    """
    Title uniqueness is scoped per parent, not global.

    Guards the 409 from over-firing: two orchestrators must be able to
    name their own children identically.
    """
    agent = await create_test_agent(client, name="dup-title-scope-agent")
    first_parent = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    second_parent = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert first_parent.status_code == 201, first_parent.text
    assert second_parent.status_code == 201, second_parent.text

    one = await _create_child(
        client, agent_id=agent["id"], parent_id=first_parent.json()["id"], title="worker"
    )
    two = await _create_child(
        client, agent_id=agent["id"], parent_id=second_parent.json()["id"], title="worker"
    )
    assert one.status_code == 201, one.text
    assert two.status_code == 201, two.text
