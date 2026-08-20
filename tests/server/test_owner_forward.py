"""Discriminating probes for Stage 3 HTTP owner forwarding."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

import httpx
import pytest

from omnigent.server.owner_forward import OwnerForwardMiddleware, _owner_url
from omnigent.server.routing_stats import RoutingStats

OWNER = "10.20.30.40:8000"
LOCAL = "10.20.30.41:8000"


def test_owner_url_uses_configured_non_default_port_and_rejects_mismatch() -> None:
    scope = {
        "raw_path": b"/v1/sessions/session/events",
        "query_string": b"x=1",
    }

    assert (
        _owner_url("10.20.30.40:51459", scope, 51459)
        == "http://10.20.30.40:51459/v1/sessions/session/events?x=1"
    )
    assert _owner_url("10.20.30.40:8000", scope, 51459) is None


def test_owner_url_brackets_ipv6_literal() -> None:
    scope = {"raw_path": b"/v1/events", "query_string": b""}

    assert _owner_url("[::1]:51459", scope, 51459) == "http://[::1]:51459/v1/events"


@pytest.mark.anyio
async def test_owner_forwarding_startup_log_reports_active_or_inactive(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(200))
    caplog.set_level(logging.INFO, logger="omnigent.server.owner_forward")

    async with httpx.AsyncClient(transport=transport) as client:
        OwnerForwardMiddleware(
            _wrong_replica_app(OWNER),
            pod_addr="10.20.30.41:51459",
            client=client,
            enabled=True,
            server_port=51459,
        )
        OwnerForwardMiddleware(
            _wrong_replica_app(OWNER),
            pod_addr="not-an-ip-address",
            client=client,
            enabled=True,
            server_port=51459,
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any("owner forwarding active" in message for message in messages)
    assert any(
        "owner forwarding inactive" in message and "invalid" in message for message in messages
    )


class _StaticOwnerStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"unexpected-forward"


def _wrong_replica_app(owner: str) -> Callable[..., Awaitable[None]]:
    async def app(scope, receive, send) -> None:
        scope.setdefault("state", {})["omnigent_owner_addr"] = owner
        while True:
            message = await receive()
            if message["type"] != "http.request" or not message.get("more_body", False):
                break
        body = json.dumps({"error": {"code": "wrong_replica", "message": "retry"}}).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})

    return app


async def _request(
    app,
    *,
    client: httpx.AsyncClient,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    middleware = OwnerForwardMiddleware(
        app,
        pod_addr=LOCAL,
        client=client,
        routing_stats=RoutingStats(),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=middleware),
        base_url="http://ingress.test",
    ) as ingress:
        return await ingress.post(
            "/v1/sessions/session/events?x=1",
            content=b"payload",
            headers=headers,
        )


@pytest.mark.anyio
async def test_forwarded_request_is_not_forwarded_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="omnigent.server.owner_forward")
    calls = 0

    async def owner(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=_StaticOwnerStream())

    transport = httpx.MockTransport(owner)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(
            _wrong_replica_app(OWNER),
            client=client,
            headers={"X-Omnigent-Forwarded-By": "10.20.30.1"},
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "wrong_replica"
    assert calls == 0
    assert any(
        record.message.startswith("wrong_replica routing failure:")
        and "reason=already_forwarded" in record.message
        for record in caplog.records
    )


@pytest.mark.anyio
async def test_unreachable_owner_returns_readdressable_wrong_replica(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="omnigent.server.owner_forward")

    async def owner(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("owner unavailable", request=request)

    transport = httpx.MockTransport(owner)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(_wrong_replica_app(OWNER), client=client)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "wrong_replica"
    assert any(
        record.levelno >= logging.WARNING
        and record.message.startswith("wrong_replica routing failure:")
        for record in caplog.records
    )


@pytest.mark.anyio
async def test_owner_path_does_not_take_an_extra_hop() -> None:
    calls = 0

    async def owner(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=_StaticOwnerStream())

    stats = RoutingStats()
    transport = httpx.MockTransport(owner)
    async with httpx.AsyncClient(transport=transport) as client:
        middleware = OwnerForwardMiddleware(
            _wrong_replica_app(LOCAL),
            pod_addr=LOCAL,
            client=client,
            routing_stats=stats,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=middleware),
            base_url="http://ingress.test",
        ) as ingress:
            response = await ingress.post("/v1/sessions/session/events", content=b"payload")

    assert response.status_code == 400
    assert calls == 0
    assert stats.forward_attempted_total == 0


@pytest.mark.anyio
async def test_wrong_replica_without_owner_is_not_forwarded() -> None:
    """A re-addressable error without a durable owner cannot be replayed."""

    async def owner(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected forward to {request.url}")

    body = b'{"error":{"code":"wrong_replica","message":"retry"}}'
    async with httpx.AsyncClient(transport=httpx.MockTransport(owner)) as client:
        middleware = OwnerForwardMiddleware(
            _wrong_replica_app(OWNER),
            pod_addr=LOCAL,
            client=client,
        )
        assert not middleware._can_forward(
            {"type": "http", "state": {}}, body, body_replayable=True
        )


@pytest.mark.anyio
async def test_forward_replays_request_and_pipes_owner_response(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="omnigent.server.owner_forward")
    seen: dict[str, object] = {}

    async def owner(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = await request.aread()
        seen["marker"] = request.headers.get("x-omnigent-forwarded-by")

        class OwnerBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b"owner-"
                yield b"body"

        return httpx.Response(200, stream=OwnerBody())

    transport = httpx.MockTransport(owner)
    async with httpx.AsyncClient(transport=transport) as client:
        response = await _request(_wrong_replica_app(OWNER), client=client)

    assert response.status_code == 200
    assert response.content == b"owner-body"
    assert seen == {
        "url": "http://10.20.30.40:8000/v1/sessions/session/events?x=1",
        "body": b"payload",
        "marker": LOCAL,
    }
    assert not any(
        record.levelno >= logging.WARNING
        and record.message.startswith("wrong_replica routing failure:")
        for record in caplog.records
    )
    assert any("owner forwarding succeeded" in record.message for record in caplog.records)


@pytest.mark.anyio
async def test_client_disconnect_closes_owner_stream() -> None:
    class HangingOwnerStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.closed = False
            self.never = asyncio.Event()

        async def __aiter__(self):
            await self.never.wait()
            yield b"never-reached"

        async def aclose(self) -> None:
            self.closed = True
            self.never.set()

    stream = HangingOwnerStream()

    async def owner(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    owner_client = httpx.AsyncClient(transport=httpx.MockTransport(owner))
    middleware = OwnerForwardMiddleware(
        _wrong_replica_app(OWNER),
        pod_addr=LOCAL,
        client=owner_client,
    )
    messages = [
        {"type": "http.request", "body": b"payload", "more_body": False},
    ]

    async def receive():
        if messages:
            return messages.pop(0)
        return {"type": "http.disconnect"}

    sent: list[dict[str, object]] = []

    async def send(message):
        sent.append(message)

    await middleware(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/events",
            "raw_path": b"/v1/events",
            "query_string": b"",
            "headers": [],
            "state": {},
        },
        receive,
        send,
    )
    await owner_client.aclose()

    assert stream.closed
    assert sent == [{"type": "http.response.start", "status": 200, "headers": []}]
