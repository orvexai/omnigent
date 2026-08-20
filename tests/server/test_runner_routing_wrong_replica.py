"""Tests for WRONG_REPLICA classification in RunnerRouter._runner_absent_code."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session

import omnigent.runner.routing as routing
from omnigent.db.db_models import SqlRunnerTunnel
from omnigent.db.utils import get_or_create_engine, now_epoch
from omnigent.errors import ErrorCode, OmnigentError
from omnigent.runner.routing import RunnerRouter
from omnigent.stores.conversation_store import RUNNER_LIVENESS_TTL_S
from omnigent.stores.host_store import HostStore

LOCAL = "10.20.30.41:8000"
REMOTE = "10.20.30.40:8000"


def _set_runner_updated_at(db_uri: str, runner_id: str, value: int) -> None:
    """Force a durable runner lease timestamp for routing classification."""
    engine = get_or_create_engine(db_uri)
    with Session(engine) as session:
        session.execute(
            update(SqlRunnerTunnel)
            .where(SqlRunnerTunnel.runner_id == runner_id)
            .values(updated_at=value)
        )
        session.commit()


class MockHostRegistry:
    """Mock host registry for testing."""

    def __init__(self, hosts=None):
        self.hosts = hosts or {}

    def get(self, host_id):
        return self.hosts.get(host_id)


class MockHostStore:
    """Mock host store for testing."""

    def __init__(self, online_hosts=None):
        self.online_hosts = online_hosts or {}

    def is_online(self, host_id):
        return self.online_hosts.get(host_id, False)


class MockTunnelRegistry:
    """Mock tunnel registry."""

    def __init__(self, sessions=None):
        self.sessions = sessions or {}

    def get(self, runner_id):
        return self.sessions.get(runner_id)


class MockConversationStore:
    """Mock conversation store."""

    def __init__(self, conversation=None):
        self.conversation = conversation

    def get_conversation(self, conversation_id):
        del conversation_id
        return self.conversation


def test_runner_absent_code_no_host_id_returns_runner_unavailable():
    """When no host_id is provided, classify as RUNNER_UNAVAILABLE."""
    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
    )
    code = router._runner_absent_code(None)
    assert code == ErrorCode.RUNNER_UNAVAILABLE


def test_runner_absent_code_no_registries_returns_runner_unavailable():
    """When no registries are wired, classify as RUNNER_UNAVAILABLE."""
    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
        host_registry=None,
        host_store=None,
    )
    code = router._runner_absent_code("host_123")
    assert code == ErrorCode.RUNNER_UNAVAILABLE


def test_runner_absent_code_host_on_this_replica_returns_runner_unavailable():
    """When host is on this replica, it's genuinely unavailable."""
    host_registry = MockHostRegistry({"host_123": "connection_obj"})
    host_store = MockHostStore()

    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
        host_registry=host_registry,
        host_store=host_store,
    )
    code = router._runner_absent_code("host_123")
    assert code == ErrorCode.RUNNER_UNAVAILABLE


def test_runner_absent_code_host_absent_locally_but_online_returns_wrong_replica():
    """When host is absent locally but online elsewhere → WRONG_REPLICA."""
    host_registry = MockHostRegistry({})  # Empty: not on this replica
    host_store = MockHostStore({"host_456": True})  # Online somewhere

    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
        host_registry=host_registry,
        host_store=host_store,
    )
    code = router._runner_absent_code("host_456")
    assert code == ErrorCode.WRONG_REPLICA


def test_runner_absent_code_host_absent_everywhere_returns_runner_unavailable():
    """When host is absent locally AND offline everywhere → RUNNER_UNAVAILABLE."""
    host_registry = MockHostRegistry({})  # Empty: not on this replica
    host_store = MockHostStore({})  # Empty: not online anywhere

    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
        host_registry=host_registry,
        host_store=host_store,
    )
    code = router._runner_absent_code("host_dead")
    assert code == ErrorCode.RUNNER_UNAVAILABLE


def test_runner_absent_code_no_store_registry_only_returns_wrong_replica():
    """When store is absent but registry says host not here → treat as wrong_replica."""
    host_registry = MockHostRegistry({})  # Empty: not on this replica
    # No host_store: should fall back to registry-only check

    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
        host_registry=host_registry,
        host_store=None,
    )
    code = router._runner_absent_code("host_789")
    # Without store, absence locally is treated as wrong replica (could be elsewhere)
    assert code == ErrorCode.WRONG_REPLICA


def test_stale_durable_owner_remains_wrong_replica_for_forwarding(db_uri: str) -> None:
    """A stale remote row still identifies the replica to retry."""
    runner_id = "runner_token_stale_durable_route_b0c8ab2431"
    tunnels = HostStore(db_uri).runner_tunnel_store
    tunnels.claim(runner_id, REMOTE)
    _set_runner_updated_at(db_uri, runner_id, now_epoch() - RUNNER_LIVENESS_TTL_S - 1)
    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(
            SimpleNamespace(runner_id=runner_id, host_id=None)
        ),
        runner_tunnel_store=tunnels,
        pod_addr=LOCAL,
    )

    with pytest.raises(OmnigentError) as exc_info:
        router.client_for_existing_conversation("conv-stale")

    error = exc_info.value
    assert error.code == ErrorCode.WRONG_REPLICA
    assert error.owner_addr == REMOTE
    assert error.http_status == 400


def test_half_open_local_tunnel_does_not_override_durable_owner(db_uri: str) -> None:
    """A local registry entry loses when durable ownership points elsewhere."""
    runner_id = "runner_token_half_open_local_route_b0c8ab2431"
    tunnels = HostStore(db_uri).runner_tunnel_store
    tunnels.claim(runner_id, REMOTE)
    router = RunnerRouter(
        registry=MockTunnelRegistry(
            {
                runner_id: SimpleNamespace(hello=SimpleNamespace(harnesses=["codex"])),
            }
        ),
        conversation_store=MockConversationStore(
            SimpleNamespace(id="conv-half-open", runner_id=runner_id, host_id=None)
        ),
        runner_tunnel_store=tunnels,
        pod_addr=LOCAL,
    )

    with pytest.raises(OmnigentError) as exc_info:
        router.client_for_session_resources("conv-half-open")

    error = exc_info.value
    assert error.code == ErrorCode.WRONG_REPLICA
    assert error.owner_addr == REMOTE


@pytest.mark.asyncio
async def test_runner_tunnel_client_has_bounded_read_timeout() -> None:
    """Tunnel response reads must not wait for the full lease indefinitely."""
    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
    )
    client = router._client_for_runner("runner-timeout")

    assert client.timeout.read == 30.0
    await router.aclose()


@pytest.mark.asyncio
async def test_runner_tunnel_read_timeout_interrupts_a_stalled_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A custom tunnel transport cannot leave its response head pending forever."""

    class _NeverResponds(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(60)
            return httpx.Response(200, request=request)

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(routing, "RUNNER_TUNNEL_READ_TIMEOUT_S", 0.01)
    transport = routing._BoundedWSTunnelTransport("runner-timeout", _NeverResponds())
    request = httpx.Request("GET", "http://runner/health")

    with pytest.raises(httpx.ReadTimeout):
        await transport.handle_async_request(request)
    await transport.aclose()


def test_released_runner_is_unavailable_during_disconnect_grace(db_uri: str) -> None:
    """A released tunnel stays a 503 rather than becoming a 400 re-address."""
    runner_id = "runner_token_released_durable_route_b0c8ab2431"
    tunnels = HostStore(db_uri).runner_tunnel_store
    tunnels.claim(runner_id, REMOTE)
    tunnels.release(runner_id, REMOTE)
    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(
            SimpleNamespace(runner_id=runner_id, host_id=None)
        ),
        runner_tunnel_store=tunnels,
        pod_addr=LOCAL,
    )

    with pytest.raises(OmnigentError) as exc_info:
        router.client_for_existing_conversation("conv-released")

    error = exc_info.value
    assert error.code == ErrorCode.RUNNER_UNAVAILABLE
    assert error.owner_addr is None
    assert error.http_status == 503


def test_fresh_remote_durable_owner_is_wrong_replica(db_uri: str) -> None:
    """A fresh remote row remains eligible for re-addressing."""
    runner_id = "runner_token_fresh_durable_route_b0c8ab2431"
    tunnels = HostStore(db_uri).runner_tunnel_store
    tunnels.claim(runner_id, REMOTE)
    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(
            SimpleNamespace(runner_id=runner_id, host_id=None)
        ),
        runner_tunnel_store=tunnels,
        pod_addr=LOCAL,
    )

    with pytest.raises(OmnigentError) as exc_info:
        router.client_for_existing_conversation("conv-fresh")

    error = exc_info.value
    assert error.code == ErrorCode.WRONG_REPLICA
    assert error.owner_addr == REMOTE


def test_single_replica_skips_durable_owner_read() -> None:
    """Without a pod address, the single-replica path remains inert."""

    class _ExplodingStore:
        def owner(self, runner_id: str) -> str | None:
            raise AssertionError(f"unexpected ownership read for {runner_id}")

    router = RunnerRouter(
        registry=MockTunnelRegistry(),
        conversation_store=MockConversationStore(),
        runner_tunnel_store=_ExplodingStore(),  # type: ignore[arg-type]
        pod_addr=None,
    )

    assert router.runner_owner_addr("runner-single-replica") is None
