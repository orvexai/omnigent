"""Delivery acknowledgements remain optional for older runner responses."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from omnigent.errors import OmnigentError
from omnigent.runtime import _globals, set_runner_client
from omnigent.server.routes._sessions.helpers import _SessionEventDispatchResult
from omnigent.server.routes.sessions import create_sessions_router
from tests.server.routes.test_session_resources import _ConversationStore, _FakeRunnerClient


class _AckAgentStore:
    def get(self, agent_id: str) -> None:
        del agent_id
        return


@pytest.fixture
async def ack_client() -> Any:
    app = FastAPI()

    @app.exception_handler(OmnigentError)
    async def _handle_error(request: Request, exc: OmnigentError) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    app.include_router(
        create_sessions_router(_ConversationStore(), _AckAgentStore()), prefix="/v1"
    )
    prior_runner = _globals._runner_client
    set_runner_client(None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://server") as client:
        try:
            yield client
        finally:
            set_runner_client(prior_runner)


def test_session_event_dispatch_result_accepts_optional_delivery() -> None:
    assert _SessionEventDispatchResult(item_id="item", pending_id=None).delivery is None
    assert (
        _SessionEventDispatchResult(item_id="item", pending_id=None, delivery="buffered").delivery
        == "buffered"
    )


@pytest.mark.asyncio
async def test_post_event_propagates_buffered_delivery_ack(ack_client: httpx.AsyncClient) -> None:
    """The public event route exposes a runner ``buffered`` verdict."""
    fake_runner = _FakeRunnerClient(payload={"status": "buffered"})
    set_runner_client(fake_runner)  # type: ignore[arg-type]
    response = await ack_client.post(
        "/v1/sessions/79b22ebd2309e48fdeb450c65611d51b/events",
        json={
            "type": "message",
            "data": {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
        },
    )
    assert response.status_code == 202, response.text
    assert response.json()["delivery"] == "buffered"


@pytest.mark.asyncio
async def test_post_event_propagates_accepted_delivery_ack(ack_client: httpx.AsyncClient) -> None:
    """The public event route exposes a runner ``accepted`` verdict."""
    fake_runner = _FakeRunnerClient(payload={"status": "accepted"})
    set_runner_client(fake_runner)  # type: ignore[arg-type]
    response = await ack_client.post(
        "/v1/sessions/79b22ebd2309e48fdeb450c65611d51b/events",
        json={
            "type": "message",
            "data": {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
        },
    )
    assert response.status_code == 202, response.text
    assert response.json()["delivery"] == "accepted"


@pytest.mark.asyncio
async def test_post_event_is_absence_tolerant_for_old_runner_ack(
    ack_client: httpx.AsyncClient,
) -> None:
    """Older runner responses omit delivery without breaking the route."""
    fake_runner = _FakeRunnerClient(payload={})
    set_runner_client(fake_runner)  # type: ignore[arg-type]
    response = await ack_client.post(
        "/v1/sessions/79b22ebd2309e48fdeb450c65611d51b/events",
        json={
            "type": "message",
            "data": {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
        },
    )
    assert response.status_code == 202, response.text
    assert "delivery" not in response.json()


@pytest.mark.asyncio
async def test_queue_full_from_runner_is_typed_without_failing_session() -> None:
    """A runner buffer refusal reaches the caller without sticky failure state."""
    app = FastAPI()
    store = _ConversationStore()

    @app.exception_handler(OmnigentError)
    async def _handle_error(request: Request, exc: OmnigentError) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    app.include_router(create_sessions_router(store, _AckAgentStore()), prefix="/v1")
    runner_bodies: list[str] = []

    async def runner_handler(request: httpx.Request) -> httpx.Response:
        runner_bodies.append(request.content.decode())
        return httpx.Response(
            429,
            json={"error": "queue_full", "detail": "session message buffer is full"},
        )

    prior_runner = _globals._runner_client
    runner = httpx.AsyncClient(
        transport=httpx.MockTransport(runner_handler), base_url="http://runner"
    )
    set_runner_client(runner)  # type: ignore[arg-type]
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://server"
        ) as client:
            response = await client.post(
                "/v1/sessions/79b22ebd2309e48fdeb450c65611d51b/events",
                json={
                    "type": "message",
                    "data": {
                        "role": "user",
                        "content": [{"type": "input_text", "text": "queued"}],
                    },
                },
            )
        assert response.status_code == 429
        assert response.json()["error"]["code"] == "queue_full"
        assert runner_bodies
        assert '"type":"message"' in runner_bodies[0].replace(" ", "")
        labels = store.get_conversation("79b22ebd2309e48fdeb450c65611d51b").labels
        assert "omnigent.last_task_error_code" not in labels
        assert "omnigent.last_task_error_message" not in labels
    finally:
        set_runner_client(prior_runner)
        await runner.aclose()
