"""End-to-end coverage for liveness-gated host forwarding classification."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session

from omnigent.db.db_models import SqlHost
from omnigent.db.utils import get_or_create_engine, now_epoch
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.app import create_app
from omnigent.server.auth import RESERVED_USER_LOCAL
from omnigent.server.owner_forward import OwnerForwardMiddleware
from omnigent.server.routes.host_tunnel import PING_INTERVAL_S, PING_MISS_THRESHOLD
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.comment_store.sqlalchemy_store import SqlAlchemyCommentStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore
from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore
from omnigent.stores.host_store import HOST_LIVENESS_TTL_S, HostStore
from tests.server.helpers import create_test_agent

HOST_ID = "9b2ec6de30f5e014c7056afe505510c3"
OWNER = "10.0.0.7:8000"
LOCAL = "10.0.0.9:8000"


def _seed_host(db_uri: str, *, updated_at: int, owner_addr: str) -> HostStore:
    store = HostStore(db_uri)
    store.upsert_on_connect(
        HOST_ID,
        "owner-classification-host",
        RESERVED_USER_LOCAL,
        owner_addr=owner_addr,
    )
    engine = get_or_create_engine(db_uri)
    with Session(engine) as session:
        session.execute(
            update(SqlHost).where(SqlHost.host_id == HOST_ID).values(updated_at=updated_at)
        )
        session.commit()
    return store


def _app_with_owner_client(
    db_uri: str,
    tmp_path: Path,
    host_store: HostStore,
    monkeypatch: pytest.MonkeyPatch,
    owner_client: httpx.AsyncClient,
):
    monkeypatch.setenv("OMNIGENT_POD_ADDR", LOCAL)
    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    app = create_app(
        agent_store=SqlAlchemyAgentStore(db_uri),
        file_store=SqlAlchemyFileStore(db_uri),
        conversation_store=SqlAlchemyConversationStore(db_uri),
        artifact_store=artifact_store,
        agent_cache=AgentCache(
            artifact_store=artifact_store,
            cache_dir=tmp_path / "cache",
        ),
        comment_store=SqlAlchemyCommentStore(db_uri),
        host_store=host_store,
    )
    for middleware in app.user_middleware:
        if middleware.cls is OwnerForwardMiddleware:
            middleware.kwargs["client"] = owner_client
            app.middleware_stack = None
            return app
    raise AssertionError("create_app did not install OwnerForwardMiddleware")


async def _owner_client(calls: list[httpx.Request]) -> httpx.AsyncClient:
    class SentinelStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"owner-sentinel"

    async def owner(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, stream=SentinelStream())

    return httpx.AsyncClient(transport=httpx.MockTransport(owner))


@pytest.mark.asyncio
async def test_stale_host_miss_is_offline_without_forwarding(
    runtime_init: None,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale owner row returns 409 and never enters owner forwarding."""
    now = now_epoch()
    host_store = _seed_host(
        db_uri,
        updated_at=now - HOST_LIVENESS_TTL_S - 1,
        owner_addr=OWNER,
    )
    calls: list[httpx.Request] = []
    async with await _owner_client(calls) as owner_client:
        app = _app_with_owner_client(db_uri, tmp_path, host_store, monkeypatch, owner_client)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            before = (await client.get("/v1/internal/routing-stats")).json()
            response = await client.get(
                f"/v1/hosts/{HOST_ID}/worktrees",
                params={"path": "/tmp/repo"},
            )
            after = (await client.get("/v1/internal/routing-stats")).json()

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "conflict"
    assert calls == []
    assert after["forward_attempted_total"] == before["forward_attempted_total"]
    assert after["wrong_replica_total"] == before["wrong_replica_total"]


@pytest.mark.asyncio
async def test_live_remote_host_forwards_once(
    runtime_init: None,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live remote owner remains actionable through the assembled app."""
    now = now_epoch()
    host_store = _seed_host(db_uri, updated_at=now, owner_addr=OWNER)
    calls: list[httpx.Request] = []
    async with await _owner_client(calls) as owner_client:
        app = _app_with_owner_client(db_uri, tmp_path, host_store, monkeypatch, owner_client)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            before = (await client.get("/v1/internal/routing-stats")).json()
            response = await client.get(
                f"/v1/hosts/{HOST_ID}/worktrees",
                params={"path": "/tmp/repo"},
            )
            after = (await client.get("/v1/internal/routing-stats")).json()

    assert response.status_code == 200
    assert response.content == b"owner-sentinel"
    assert len(calls) == 1
    assert str(calls[0].url) == f"http://{OWNER}/v1/hosts/{HOST_ID}/worktrees?path=%2Ftmp%2Frepo"
    assert after["forward_attempted_total"] == before["forward_attempted_total"] + 1
    assert after["forward_succeeded_total"] == before["forward_succeeded_total"] + 1


@pytest.mark.asyncio
async def test_host_bound_session_forwards_once(
    runtime_init: None,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A host-bound session create forwards through the session route once."""

    async def _skip_workspace_validation(**_kwargs: object) -> str:
        return "/tmp/repo"

    monkeypatch.setattr(
        "omnigent.server.routes._sessions.orchestration._validate_session_workspace",
        _skip_workspace_validation,
    )
    now = now_epoch()
    host_store = _seed_host(db_uri, updated_at=now, owner_addr=OWNER)
    calls: list[httpx.Request] = []
    async with await _owner_client(calls) as owner_client:
        app = _app_with_owner_client(db_uri, tmp_path, host_store, monkeypatch, owner_client)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            agent = await create_test_agent(client, name="session-forward-agent")
            before = (await client.get("/v1/internal/routing-stats")).json()
            response = await client.post(
                "/v1/sessions",
                json={
                    "agent_id": agent["id"],
                    "host_id": HOST_ID,
                    "workspace": "/tmp/repo",
                },
            )
            after = (await client.get("/v1/internal/routing-stats")).json()

    assert response.status_code == 200
    assert response.content == b"owner-sentinel"
    assert len(calls) == 1
    assert str(calls[0].url) == "http://10.0.0.7:8000/v1/sessions"
    assert after["forward_attempted_total"] == before["forward_attempted_total"] + 1
    assert after["forward_succeeded_total"] == before["forward_succeeded_total"] + 1


def test_host_liveness_ttl_covers_ping_miss_window() -> None:
    assert HOST_LIVENESS_TTL_S >= PING_INTERVAL_S * PING_MISS_THRESHOLD
