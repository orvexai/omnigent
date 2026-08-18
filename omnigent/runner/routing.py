"""Conversation-aware runner routing for the Omnigent server.

The tunnel registry is the source of truth for online runners. This
module turns that registry into the one dispatch decision the server
needs: given a conversation and harness kind, read the bound runner and
return an ``httpx`` client that talks to that runner over the WebSocket
tunnel.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

from omnigent.errors import ErrorCode, OmnigentError
from omnigent.harness_aliases import canonicalize_harness
from omnigent.runner.transports.ws_tunnel.transport import WSTunnelTransport
from omnigent.runtime import telemetry
from omnigent.runtime.harnesses import _HARNESS_MODULES
from omnigent.spec import AgentSpec

if TYPE_CHECKING:
    from omnigent.entities import Conversation
    from omnigent.runner.transports.ws_tunnel.registry import RunnerSession, TunnelRegistry
    from omnigent.server.host_registry import HostRegistry
    from omnigent.stores import ConversationStore
    from omnigent.stores.host_store import HostStore
    from omnigent.stores.runner_tunnel_store import RunnerTunnelStore


_EXECUTOR_TYPE_TO_HARNESS: dict[str, str] = {"claude_sdk": "claude-sdk"}


def runner_dispatch_harness(spec: AgentSpec) -> str | None:
    """
    Return the runner-routed harness for an agent spec, if any.

    Mirrors the harness selection in
    :func:`omnigent.runtime.workflow._create_executor`: direct
    executors return ``None`` unless they explicitly name a harness.

    :param spec: Parsed agent spec from the agent cache.
    :returns: Harness key, e.g. ``"codex"``, when the executor is
        runner-routed; otherwise ``None``.
    """
    executor_type = spec.executor.type
    harness = spec.executor.config.get("harness")
    if not harness:
        harness = _EXECUTOR_TYPE_TO_HARNESS.get(executor_type, executor_type)
    canonical = canonicalize_harness(harness) or harness
    return canonical if canonical in _HARNESS_MODULES else None


@dataclass(frozen=True)
class RoutedRunner:
    """
    Runner selected for a conversation dispatch.

    :param runner_id: Runner UUID, e.g.
        ``"runner_0123456789abcdef"``.
    :param client: ``httpx.AsyncClient`` that routes requests to
        ``runner_id`` through the tunnel registry.
    """

    runner_id: str
    client: httpx.AsyncClient


class RunnerRouter:
    """
    Select runners from the live tunnel registry.

    :param registry: In-memory tunnel registry populated by
        ``WS /v1/runners/{runner_id}/tunnel``.
    :param conversation_store: Store used to read
        ``conversations.runner_id`` affinity.
    :param host_registry: Per-replica host-tunnel registry used by the
        legacy fallback classifier when durable runner ownership is inert.
    :param host_store: Cross-replica host liveness used by that same
        backwards-compatible fallback. Once pod identity and the durable
        runner table are wired, the runner row is authoritative instead.
    :param runner_tunnel_store: Durable runner-to-pod ownership records.
    :param pod_addr: This pod's dialable address; ``None`` keeps the new
        classification inert.
    """

    def __init__(
        self,
        *,
        registry: TunnelRegistry,
        conversation_store: ConversationStore,
        host_registry: HostRegistry | None = None,
        host_store: HostStore | None = None,
        runner_tunnel_store: RunnerTunnelStore | None = None,
        pod_addr: str | None = None,
    ) -> None:
        self._registry = registry
        self._conversation_store = conversation_store
        self._host_registry = host_registry
        self._host_store = host_store
        self._runner_tunnel_store = runner_tunnel_store
        self._pod_addr = pod_addr
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._lock = threading.RLock()

    def client_for_conversation(self, *, conversation_id: str, harness: str) -> RoutedRunner:
        """
        Return the runner client for a harness-backed conversation turn.

        Dispatch is a read-only operation for runner affinity. The
        session must already have ``conversations.runner_id`` set by
        ``PATCH /v1/sessions/{id}``; dispatch never picks or persists
        a runner itself.

        :param conversation_id: Conversation id, e.g.
            ``"conv_0123456789abcdef"``.
        :param harness: Harness kind requested by the agent spec,
            e.g. ``"codex"``.
        :returns: Selected runner id and client.
        :raises OmnigentError: If the conversation has no runner
            binding, the bound runner is offline, or the runner
            cannot serve the requested harness.
        """
        conv = self._conversation_store.get_conversation(conversation_id)
        if conv is None:
            raise OmnigentError("conversation not found", code=ErrorCode.NOT_FOUND)
        if conv.runner_id:
            return self._routed_pinned_runner(
                conv.runner_id, harness=harness, host_id=conv.host_id
            )
        raise OmnigentError(
            f"conversation {conversation_id!r} is not bound to a runner; "
            "resume the session to bind a registered runner",
            code=ErrorCode.CONFLICT,
        )

    def client_for_session_resources(
        self,
        conversation_id: str,
        *,
        conversation: Conversation | None = None,
    ) -> RoutedRunner:
        """
        Return a runner client for session resource access.

        Resource APIs use the same session affinity as dispatch. The
        session must already have ``conversations.runner_id`` set by
        ``PATCH /v1/sessions/{id}``; resource access never selects or
        persists a runner itself.

        :param conversation_id: Conversation/session id, e.g.
            ``"conv_0123456789abcdef"``.
        :param conversation: An already-loaded conversation. Callers that
            just authorized the session can pass it to avoid another read.
        :returns: Selected runner id and client.
        :raises OmnigentError: If the conversation is missing, the
            pinned runner is offline, or no online runner is available.
        """
        conv = conversation
        if conv is not None and conv.id != conversation_id:
            raise ValueError(
                f"conversation id mismatch: expected {conversation_id!r}, got {conv.id!r}"
            )
        if conv is None:
            conv = self._conversation_store.get_conversation(conversation_id)
        if conv is None:
            raise OmnigentError("conversation not found", code=ErrorCode.NOT_FOUND)
        if conv.runner_id:
            session = self._registry.get(conv.runner_id)
            if session is None:
                raise OmnigentError(
                    f"runner {conv.runner_id!r} is offline for conversation {conversation_id!r}",
                    code=self._runner_absent_code(conv.host_id, conv.runner_id),
                    owner_addr=self.runner_owner_addr(conv.runner_id),
                )
            return RoutedRunner(
                runner_id=conv.runner_id,
                client=self._client_for_runner(conv.runner_id),
            )

        raise OmnigentError(
            f"conversation {conversation_id!r} is not bound to a runner; "
            "resume the session to bind a registered runner",
            code=ErrorCode.CONFLICT,
        )

    def client_for_existing_conversation(self, conversation_id: str) -> RoutedRunner | None:
        """
        Return the pinned runner client for an already-started conversation.

        Used by server surfaces like terminal listing and interrupt
        forwarding that know the conversation but do not know the
        harness kind. Unpinned or missing conversations return
        ``None`` so callers can fall back to local test/in-process
        behavior.

        :param conversation_id: Conversation id, e.g.
            ``"conv_0123456789abcdef"``.
        :returns: A routed runner when the conversation is pinned;
            ``None`` when it is not pinned or not found.
        :raises OmnigentError: If the pinned runner is offline.
        """
        conv = self._conversation_store.get_conversation(conversation_id)
        if conv is None or not conv.runner_id:
            return None
        session = self._registry.get(conv.runner_id)
        if session is None:
            raise OmnigentError(
                f"runner {conv.runner_id!r} is offline for conversation {conversation_id!r}",
                code=self._runner_absent_code(conv.host_id, conv.runner_id),
                owner_addr=self.runner_owner_addr(conv.runner_id),
            )
        return RoutedRunner(
            runner_id=conv.runner_id,
            client=self._client_for_runner(conv.runner_id),
        )

    def runner_is_online(self, runner_id: str) -> bool:
        """
        Return whether *runner_id* is currently connected.

        :param runner_id: Runner UUID, e.g.
            ``"runner_0123456789abcdef"``.
        :returns: ``True`` when the registry has a live session.
        """
        return self._registry.get(runner_id) is not None

    def runner_owner(self, runner_id: str) -> str | None:
        """
        Return the authenticated owner of *runner_id*, or ``None``.

        Delegates to the tunnel registry. Returns ``None`` when the
        runner is offline or was registered without an owner (single-
        user / no-auth mode).

        :param runner_id: Runner UUID, e.g.
            ``"runner_0123456789abcdef"``.
        :returns: Owner user id, or ``None``.
        """
        return self._registry.runner_owner(runner_id)

    async def aclose(self) -> None:
        """
        Close cached runner clients.

        :returns: None.
        """
        with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for client in clients:
            await client.aclose()

    def _routed_pinned_runner(
        self, runner_id: str, *, harness: str, host_id: str | None = None
    ) -> RoutedRunner:
        """
        Return a routed runner after validating hard affinity.

        :param runner_id: Pinned runner UUID.
        :param harness: Harness kind requested by the agent spec.
        :param host_id: The session's bound host, used to classify an
            offline runner as wrong-replica vs genuinely gone. See
            :meth:`_runner_absent_code`.
        :returns: Selected runner id and client.
        :raises OmnigentError: If the runner is offline or
            lacks the requested harness capability.
        """
        session = self._registry.get(runner_id)
        if session is None:
            raise OmnigentError(
                f"runner {runner_id!r} is offline; resume the session to bind a registered runner",
                code=self._runner_absent_code(host_id, runner_id),
                owner_addr=self.runner_owner_addr(runner_id),
            )
        if not _runner_supports_harness(session, harness):
            raise OmnigentError(
                f"runner {runner_id!r} does not support harness {harness!r}",
                code=ErrorCode.RUNNER_CAPABILITY_MISMATCH,
            )
        return RoutedRunner(runner_id=runner_id, client=self._client_for_runner(runner_id))

    def runner_owner_addr(self, runner_id: str) -> str | None:
        """Return durable runner ownership when Stage 1 is active."""
        if self._runner_tunnel_store is None or self._pod_addr is None:
            return None
        return self._runner_tunnel_store.owner(runner_id)

    def _runner_absent_code(
        self, host_id: str | None, runner_id: str | None = None
    ) -> str:
        """
        Classify a "bound runner, but its tunnel isn't on this replica" miss.

        With durable ownership wired, the runner table tells the two failure
        modes apart:

        - row present with a different owner address → ``WRONG_REPLICA``;
        - row absent, or owned by this pod but missing locally →
          ``RUNNER_UNAVAILABLE``.

        When the durable store is not wired, retain the existing host
        liveness fallback so single-replica and mixed-version deployments
        remain compatible.

        :param host_id: The session's bound host id, or ``None`` (hostless
            local runner — always genuinely offline when its tunnel drops).
        :returns: The error code string to raise.
        """
        if self._runner_tunnel_store is not None and self._pod_addr is not None and runner_id:
            owner_addr = self.runner_owner_addr(runner_id)
            if owner_addr is not None:
                return (
                    ErrorCode.WRONG_REPLICA
                    if owner_addr != self._pod_addr
                    else ErrorCode.RUNNER_UNAVAILABLE
                )
            return ErrorCode.RUNNER_UNAVAILABLE
        if (
            host_id
            and self._host_registry is not None
            and self._host_registry.get(host_id) is None
        ):
            # Absent locally: wrong replica only if the host is live elsewhere.
            # No store wired → no liveness to consult, so keep the registry-only
            # behavior (treat as wrong replica).
            if self._host_store is None or self._host_store.is_online(host_id):
                return ErrorCode.WRONG_REPLICA
        return ErrorCode.RUNNER_UNAVAILABLE

    def _client_for_runner(self, runner_id: str) -> httpx.AsyncClient:
        """
        Return a cached tunnel-backed client for *runner_id*.

        :param runner_id: Runner UUID, e.g.
            ``"runner_0123456789abcdef"``.
        :returns: ``httpx.AsyncClient`` using
            :class:`WSTunnelTransport`.
        """
        with self._lock:
            client = self._clients.get(runner_id)
            if client is None:
                client = httpx.AsyncClient(
                    transport=WSTunnelTransport(self._registry, runner_id),
                    base_url="http://runner",
                    timeout=httpx.Timeout(5.0, read=None),
                )
                # The global httpx instrumentation can't see this client's
                # custom WSTunnelTransport, so instrument the instance
                # directly — otherwise server→runner forwards carry no
                # traceparent and the runner roots a disconnected trace.
                telemetry.instrument_httpx_client(client)
                self._clients[runner_id] = client
            return client


def _runner_supports_harness(session: RunnerSession, harness: str) -> bool:
    """
    Return whether a runner advertised support for *harness*.

    :param session: Live runner session from the tunnel registry.
    :param harness: Harness kind requested by the agent spec,
        e.g. ``"claude-sdk"``.
    :returns: ``True`` when the runner hello frame includes the
        harness kind.
    """
    canonical = canonicalize_harness(harness) or harness
    return canonical in session.hello.harnesses or harness in session.hello.harnesses
