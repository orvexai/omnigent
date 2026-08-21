"""Forward misrouted HTTP requests to their durable session owner.

The middleware is deliberately exception-driven.  Routes discover ownership
only when their local tunnel lookup misses and put the address in ASGI state;
the middleware then provides the one transport chokepoint for all such routes.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Coroutine

import httpx
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from omnigent.server.routing_stats import RoutingStats

_FORWARDED_BY_HEADER = b"x-omnigent-forwarded-by"
_DEFAULT_BODY_MAX_BYTES = 1 << 20
_DEFAULT_SERVER_PORT = 8000
_logger = logging.getLogger(__name__)


def _env_enabled() -> bool:
    """Return whether owner forwarding is enabled (defaulting to enabled)."""
    return os.environ.get("OMNIGENT_OWNER_FORWARD", "1") != "0"


def _body_max_bytes() -> int:
    """Read the replay body cap, falling back safely for bad configuration."""
    raw = os.environ.get("OMNIGENT_OWNER_FORWARD_MAX_BODY_BYTES")
    if raw is None:
        return _DEFAULT_BODY_MAX_BYTES
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_BODY_MAX_BYTES
    return value if value > 0 else _DEFAULT_BODY_MAX_BYTES


def _header(scope: Scope, name: bytes) -> bytes | None:
    """Return one request header without decoding untrusted bytes."""
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value
    return None


def _parse_host_port(address: str | None) -> tuple[str, int, int] | None:
    """Parse an IP literal plus port, returning host, port, and IP version."""
    if not address:
        return None

    if address.startswith("["):
        close_bracket = address.find("]")
        if close_bracket < 0 or address[close_bracket + 1 : close_bracket + 2] != ":":
            return None
        host = address[1:close_bracket]
        raw_port = address[close_bracket + 2 :]
    else:
        host, separator, raw_port = address.rpartition(":")
        if not separator:
            return None

    if not host or not raw_port:
        return None
    try:
        ip = ipaddress.ip_address(host)
        port = int(raw_port)
    except ValueError:
        return None
    if not 1 <= port <= 65535:
        return None
    return host, port, ip.version


def _format_host_port(host: str, port: int, version: int) -> str:
    """Format an IP literal and port for an HTTP URL or ownership marker."""
    return f"[{host}]:{port}" if version == 6 else f"{host}:{port}"


def _owner_url(owner_addr: str, scope: Scope, server_port: int) -> str | None:
    """Build a pod-local URL only for a validated IP literal and port."""
    parsed = _parse_host_port(owner_addr)
    if parsed is None:
        return None
    host, port, version = parsed
    if port != server_port:
        return None

    raw_path = scope.get("raw_path")
    if not isinstance(raw_path, bytes):
        raw_path = str(scope.get("path", "/")).encode("utf-8")
    query = scope.get("query_string", b"")
    if not isinstance(query, bytes):
        query = str(query).encode("utf-8")
    formatted_addr = _format_host_port(host, port, version)
    url = f"http://{formatted_addr}{raw_path.decode('ascii', 'surrogateescape')}"
    if query:
        url += f"?{query.decode('ascii', 'surrogateescape')}"
    return url


class OwnerForwardMiddleware:
    """Replay retryable wrong-replica HTTP responses at their owner pod."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        pod_addr: str | None,
        routing_stats: RoutingStats | None = None,
        client: httpx.AsyncClient | None = None,
        enabled: bool | None = None,
        server_port: int = _DEFAULT_SERVER_PORT,
        body_max_bytes: int | None = None,
    ) -> None:
        self._app = app
        self._pod_addr = pod_addr
        self._routing_stats = routing_stats
        self._enabled = _env_enabled() if enabled is None else enabled
        self._server_port = server_port
        self._body_max_bytes = body_max_bytes or _body_max_bytes()
        self._client = client
        address_reason = self._pod_addr_reason(pod_addr, server_port)
        if not self._enabled:
            _logger.warning("owner forwarding inactive: disabled by configuration")
        elif address_reason is not None:
            self._enabled = False
            _logger.warning("owner forwarding inactive: %s", address_reason)
        else:
            _logger.info(
                "owner forwarding active: pod_addr=%s server_port=%d",
                pod_addr,
                server_port,
            )
        self._owns_client = client is None and self._enabled and pod_addr is not None
        if self._client is None and self._owns_client:
            self._client = httpx.AsyncClient(
                follow_redirects=False,
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
                timeout=httpx.Timeout(5.0, read=None),
            )

    @staticmethod
    def resolve_server_port(
        pod_addr: str | None,
        server_config: dict[str, Any] | None = None,
    ) -> int:
        """Resolve the port used by the server before middleware construction.

        Container entrypoints resolve ``port`` from server config, then
        ``PORT``. The pod address is the app-level fallback used by the CLI,
        whose resolved Click port is not part of ``create_app``'s arguments.
        """
        candidates: list[Any] = []
        if isinstance(server_config, dict):
            candidates.append(server_config.get("port"))
        candidates.extend(os.environ.get(name) for name in ("PORT", "DATABRICKS_APP_PORT"))
        parsed = _parse_host_port(pod_addr)
        if parsed is not None:
            candidates.append(parsed[1])
        for raw_port in candidates:
            try:
                port = int(raw_port)
            except (TypeError, ValueError):
                continue
            if 1 <= port <= 65535:
                return port
        return _DEFAULT_SERVER_PORT

    @staticmethod
    def _pod_addr_reason(pod_addr: str | None, server_port: int) -> str | None:
        """Return the startup diagnostic for an unusable pod address."""
        if pod_addr is None:
            return "OMNIGENT_POD_ADDR is unset"
        parsed = _parse_host_port(pod_addr)
        if parsed is None:
            return f"OMNIGENT_POD_ADDR={pod_addr!r} is invalid; expected an IP literal and port"
        _host, pod_port, _version = parsed
        if pod_port != server_port:
            return (
                f"OMNIGENT_POD_ADDR port {pod_port} does not match configured server port "
                f"{server_port}"
            )
        return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle HTTP scopes and leave WebSocket/lifespan scopes unchanged."""
        if scope["type"] != "http":
            try:
                await self._app(scope, receive, send)
            finally:
                if scope["type"] == "lifespan" and self._owns_client and self._client:
                    await self._client.aclose()
            return

        body_parts: list[bytes] = []
        body_size = 0
        body_replayable = True

        async def receive_recording() -> Message:
            nonlocal body_size, body_replayable
            message = await receive()
            if message["type"] == "http.request":
                body = message.get("body", b"")
                if body_replayable:
                    if body_size + len(body) <= self._body_max_bytes:
                        body_parts.append(body)
                        body_size += len(body)
                    else:
                        body_replayable = False
                        body_parts.clear()
            return message

        response_start: Message | None = None
        response_body: list[Message] = []
        response_is_candidate = False

        async def send_capture(message: Message) -> None:
            nonlocal response_start, response_is_candidate
            if message["type"] == "http.response.start":
                if response_start is not None:
                    await send(message)
                    return
                if message.get("status") != 400:
                    await send(message)
                    response_is_candidate = False
                    return
                response_start = message
                response_is_candidate = True
                return

            if not response_is_candidate:
                await send(message)
                return

            response_body.append(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                await self._finish_response(
                    scope,
                    body=b"".join(part.get("body", b"") for part in response_body),
                    request_body=b"".join(body_parts),
                    body_replayable=body_replayable,
                    original_start=response_start,
                    original_body=response_body,
                    receive=receive,
                    send=send,
                )

        await self._app(scope, receive_recording, send_capture)

    async def _finish_response(
        self,
        scope: Scope,
        *,
        body: bytes,
        request_body: bytes,
        body_replayable: bool,
        original_start: Message | None,
        original_body: list[Message],
        receive: Receive,
        send: Send,
    ) -> None:
        """Forward a candidate response or emit the captured response unchanged."""
        if original_start is None or not self._can_forward(scope, body, body_replayable):
            await self._emit_original(original_start, original_body, send)
            return

        state = scope.setdefault("state", {})
        state["omnigent_forward_attempted"] = True
        if self._routing_stats is not None:
            self._routing_stats.record_forward_attempted()

        owner_addr = state["omnigent_owner_addr"]
        owner_url = _owner_url(owner_addr, scope, self._server_port)
        _logger.info(
            "owner forwarding attempt: owner_addr=%s owner_url=%s path=%s",
            owner_addr,
            owner_url,
            scope.get("path"),
        )
        if owner_url is None or self._client is None:
            _logger.warning(
                "wrong_replica routing failure: owner forwarding failed before request: "
                "owner_addr=%s owner_url=%s "
                "local_server_port=%s client=%s",
                owner_addr,
                owner_url,
                self._server_port,
                self._client is not None,
            )
            await self._forward_failed(original_start, original_body, send)
            return

        headers = [
            (key, value)
            for key, value in scope.get("headers", [])
            if key.lower() != _FORWARDED_BY_HEADER
        ]
        if self._pod_addr is not None:
            headers.append((_FORWARDED_BY_HEADER, self._pod_addr.encode("ascii")))

        response_started = False
        try:
            async with self._client.stream(
                scope["method"],
                owner_url,
                headers=headers,
                content=request_body,
            ) as response:
                await send(
                    {
                        "type": "http.response.start",
                        "status": response.status_code,
                        "headers": list(response.headers.raw),
                    }
                )
                response_started = True
                await self._pipe_response(response, receive, send)
        except (httpx.HTTPError, OSError, RuntimeError) as exc:
            # The owner may have died after the durable lookup.  Keep the
            # original classified response so the caller can retry through
            # ingress and ownership can be resolved again.
            _logger.warning(
                "wrong_replica routing failure: owner forwarding transport failure: "
                "owner_addr=%s owner_url=%s "
                "response_started=%s error=%s",
                owner_addr,
                owner_url,
                response_started,
                exc,
                exc_info=True,
            )
            if not response_started:
                await self._forward_failed(original_start, original_body, send)
            return

        state["omnigent_forward_succeeded"] = True
        _logger.info(
            "owner forwarding succeeded: owner_addr=%s owner_url=%s path=%s",
            owner_addr,
            owner_url,
            scope.get("path"),
        )
        if self._routing_stats is not None:
            self._routing_stats.record_forward_succeeded()

    def _can_forward(self, scope: Scope, body: bytes, body_replayable: bool) -> bool:
        """Check the complete forwarding conjunction without any network I/O."""
        try:
            payload = json.loads(body)
        except (TypeError, ValueError):
            return False
        if not isinstance(payload, dict):
            return False
        error = payload.get("error")
        if not isinstance(error, dict) or error.get("code") != "wrong_replica":
            return False
        state = scope.get("state")
        owner_addr = state.get("omnigent_owner_addr") if isinstance(state, dict) else None
        if not self._enabled:
            _logger.warning(
                "owner forwarding skipped: reason=disabled owner_addr=%s path=%s",
                owner_addr,
                scope.get("path"),
            )
            return False
        if not self._pod_addr:
            _logger.warning(
                "owner forwarding skipped: reason=missing_local_address owner_addr=%s path=%s",
                owner_addr,
                scope.get("path"),
            )
            return False
        if self._client is None:
            _logger.warning(
                "owner forwarding skipped: reason=missing_client owner_addr=%s path=%s",
                owner_addr,
                scope.get("path"),
            )
            return False
        if not body_replayable:
            _logger.warning(
                "owner forwarding skipped: reason=body_not_replayable owner_addr=%s path=%s",
                owner_addr,
                scope.get("path"),
            )
            return False
        if not isinstance(owner_addr, str):
            _logger.warning(
                "wrong_replica routing failure: owner forwarding skipped: "
                "reason=missing_owner_address owner_addr=%s path=%s",
                owner_addr,
                scope.get("path"),
            )
            return False
        if self._same_address(owner_addr, self._pod_addr):
            _logger.warning(
                "wrong_replica routing failure: owner forwarding skipped: "
                "reason=owner_is_local owner_addr=%s "
                "local_addr=%s path=%s",
                owner_addr,
                self._pod_addr,
                scope.get("path"),
            )
            return False
        # The marker is intentionally untrusted.  It only lets a caller
        # suppress its own replay; it can never select an owner or add trust.
        if _header(scope, _FORWARDED_BY_HEADER) is not None:
            _logger.warning(
                "wrong_replica routing failure: owner forwarding skipped: "
                "reason=already_forwarded owner_addr=%s path=%s",
                owner_addr,
                scope.get("path"),
            )
            return False
        _logger.info(
            "owner forwarding eligible: owner_addr=%s local_addr=%s path=%s",
            owner_addr,
            self._pod_addr,
            scope.get("path"),
        )
        return True

    @staticmethod
    def _same_address(left: str, right: str | None) -> bool:
        """Compare ownership addresses after normalizing IPv4/IPv6 notation."""
        if right is None:
            return False
        left_parsed = _parse_host_port(left)
        right_parsed = _parse_host_port(right)
        if left_parsed is None or right_parsed is None:
            return left == right
        return left_parsed[1:] == right_parsed[1:] and ipaddress.ip_address(
            left_parsed[0]
        ) == ipaddress.ip_address(right_parsed[0])

    async def _forward_failed(
        self,
        original_start: Message,
        original_body: list[Message],
        send: Send,
    ) -> None:
        """Return the retryable original response after a failed forward."""
        if self._routing_stats is not None:
            self._routing_stats.record_forward_failed()
            self._routing_stats.record_forward_returned()
        await self._emit_original(original_start, original_body, send)

    async def _emit_original(
        self,
        original_start: Message | None,
        original_body: list[Message],
        send: Send,
    ) -> None:
        """Emit the buffered downstream response exactly as received."""
        if original_start is not None:
            await send(original_start)
        for message in original_body:
            await send(message)

    async def _pipe_response(self, response: httpx.Response, receive: Receive, send: Send) -> None:
        """Pipe response bytes while closing the upstream on client disconnect."""
        disconnect_task = asyncio.create_task(cast("Coroutine[Any, Any, Message]", receive()))
        response_iterator = response.aiter_raw()
        read_task = asyncio.create_task(
            cast("Coroutine[Any, Any, bytes]", response_iterator.__anext__())
        )
        try:
            while True:
                done, _ = await asyncio.wait(
                    (disconnect_task, read_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if disconnect_task in done:
                    message = disconnect_task.result()
                    if message["type"] == "http.disconnect":
                        read_task.cancel()
                        return
                    disconnect_task = asyncio.create_task(
                        cast("Coroutine[Any, Any, Message]", receive())
                    )
                    continue

                try:
                    chunk = read_task.result()
                except StopAsyncIteration:
                    await send({"type": "http.response.body", "body": b"", "more_body": False})
                    return
                await send({"type": "http.response.body", "body": chunk, "more_body": True})
                read_task = asyncio.create_task(
                    cast("Coroutine[Any, Any, bytes]", response_iterator.__anext__())
                )
        finally:
            for task in (disconnect_task, read_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(disconnect_task, read_task, return_exceptions=True)


__all__ = ["OwnerForwardMiddleware"]
