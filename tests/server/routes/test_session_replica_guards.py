"""Regression tests for parked session state on non-owner replicas."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import APIRouter
from fastapi.routing import APIRoute
from starlette.requests import Request

from omnigent.entities import Conversation
from omnigent.errors import ErrorCode, OmnigentError
from omnigent.policies.types import PolicyResult
from omnigent.runtime import pending_elicitations
from omnigent.server.routes import sessions as sessions_facade
from omnigent.server.routes._sessions import helpers, orchestration
from omnigent.server.routes.sessions import (
    routes_browser,
    routes_elicitations,
    routes_events,
    routes_hooks,
)
from omnigent.server.routing_stats import RoutingStats
from omnigent.server.schemas import ElicitationResult, SessionEventInput
from omnigent.spec.types import PolicyAction
from omnigent.stores import AgentStore


class _RemoteRouter:
    """Durable ownership says the runner belongs to another pod."""

    _pod_addr = "pod-a:8000"

    def runner_is_online(self, runner_id: str) -> bool:
        del runner_id
        return False

    def runner_owner_addr(self, runner_id: str) -> str:
        del runner_id
        return "pod-b:8000"


class _LocalRouter:
    """The runner is registered on the replica handling the bind."""

    def runner_is_online(self, runner_id: str) -> bool:
        del runner_id
        return True

    def runner_owner_addr(self, runner_id: str) -> str:
        del runner_id
        return "pod-a:8000"


class _UnknownRouter:
    """The runner has no local registration or durable owner."""

    def runner_is_online(self, runner_id: str) -> bool:
        del runner_id
        return False

    def runner_owner_addr(self, runner_id: str) -> None:
        del runner_id


class _ConversationStore:
    def __init__(self, conversation: Conversation) -> None:
        self.conversation = conversation

    def get_conversation(self, conversation_id: str) -> Conversation | None:
        if conversation_id != self.conversation.id:
            return None
        return self.conversation


def _conversation(session_id: str = "conv_replica") -> Conversation:
    return Conversation(
        id=session_id,
        created_at=1,
        updated_at=2,
        root_conversation_id=session_id,
        title="Replica test",
        agent_id="agent_replica",
        runner_id="runner_replica",
    )


def _request(app: Any, path: str, payload: dict[str, Any] | None = None) -> Request:
    body = json.dumps(payload or {}).encode()

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": [(b"content-type", b"application/json")],
        "app": app,
    }
    return Request(scope, receive)


def _app(router: _RemoteRouter) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(
            pod_addr="pod-a:8000",
            routing_stats=RoutingStats(),
            runner_router=router,
        )
    )


def test_runner_bind_remote_owner_is_classified_for_forwarding() -> None:
    """A remote durable owner becomes a forwardable wrong-replica error."""
    with pytest.raises(OmnigentError) as exc_info:
        helpers._registered_runner_id(
            _RemoteRouter(),
            " runner_replica ",
            pod_addr="pod-a:8000",
        )

    assert exc_info.value.code == ErrorCode.WRONG_REPLICA
    assert exc_info.value.owner_addr == "pod-b:8000"


def test_runner_bind_local_owner_preserves_success() -> None:
    """A locally owned live runner still returns its trimmed id."""
    assert (
        helpers._registered_runner_id(
            _LocalRouter(),
            " runner_local ",
            pod_addr="pod-a:8000",
        )
        == "runner_local"
    )


def test_runner_bind_unknown_runner_preserves_invalid_input() -> None:
    """An unknown runner remains the existing 400-class validation error."""
    with pytest.raises(OmnigentError) as exc_info:
        helpers._registered_runner_id(
            _UnknownRouter(),
            "runner_unknown",
            pod_addr="pod-a:8000",
        )

    assert exc_info.value.code == ErrorCode.INVALID_INPUT


def _endpoint(router: APIRouter, suffix: str) -> Any:
    for route in router.routes:
        if isinstance(route, APIRoute) and route.path.endswith(suffix):
            return route.endpoint
    raise AssertionError(f"route not found: {suffix}")


@pytest.mark.asyncio
async def test_native_ask_park_on_remote_replica_is_classified_before_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native ASK on a non-owner pod returns WRONG_REPLICA, not DENY."""
    conversation = _conversation()
    store = _ConversationStore(conversation)
    router = _RemoteRouter()
    app = _app(router)
    hook_router = APIRouter()
    routes_hooks.register_hooks_routes(
        hook_router,
        conversation_store=store,  # type: ignore[arg-type]
        agent_store=cast(
            AgentStore,
            SimpleNamespace(
                get=lambda _agent_id: SimpleNamespace(
                    id="agent_replica", bundle_location=None, session_id=None
                )
            ),
        ),
    )
    endpoint = _endpoint(hook_router, "/policies/evaluate")

    async def access(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(level=2, conversation=conversation)

    class _Cache:
        def load(self, *_args: Any, **_kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(spec=SimpleNamespace())

    class _Caps:
        default_policies: tuple[object, ...] = ()
        policy_llm_connection_factory = None
        llm = None

    class _AskEngine:
        labels: dict[str, str] = {}

        async def evaluate(self, *_args: Any, **_kwargs: Any) -> PolicyResult:
            return PolicyResult(action=PolicyAction.ASK, deciding_policies=["budget"])

    hold_called = False

    async def hold_gate(*_args: Any, **_kwargs: Any) -> bool:
        nonlocal hold_called
        hold_called = True
        return False

    monkeypatch.setattr(routes_hooks, "_require_access_and_level", access)
    monkeypatch.setattr(sessions_facade, "_get_user_id", lambda *_args: "user")
    monkeypatch.setattr(sessions_facade, "get_agent_cache", lambda: _Cache())
    monkeypatch.setattr(sessions_facade, "get_caps", lambda: _Caps())
    monkeypatch.setattr(routes_hooks, "get_policy_store", lambda: None)
    monkeypatch.setattr(routes_hooks, "any_policies_apply", lambda **_kwargs: True)
    monkeypatch.setattr(routes_hooks, "build_policy_engine", lambda **_kwargs: _AskEngine())
    monkeypatch.setattr(routes_hooks, "_build_actor", lambda _actor: {})
    monkeypatch.setattr(
        routes_hooks,
        "_build_evaluation_context",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(routes_hooks, "_hold_native_ask_gate", hold_gate)

    with pytest.raises(OmnigentError) as exc_info:
        await endpoint(
            _request(
                app,
                "/sessions/conv_replica/policies/evaluate",
                {
                    "event": {
                        "type": "PHASE_TOOL_CALL",
                        "data": {"name": "shell", "arguments": {}},
                    }
                },
            ),
            "conv_replica",
        )

    assert exc_info.value.code == ErrorCode.WRONG_REPLICA
    assert exc_info.value.owner_addr == "pod-b:8000"
    assert hold_called is False
    assert app.state.routing_stats.snapshot()["hook_park_off_owner_total"] == 1


@pytest.mark.asyncio
async def test_url_elicitation_resolution_is_classified_before_local_registry_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolve URL on a non-owner pod cannot silently acknowledge a verdict."""
    conversation = _conversation()
    store = _ConversationStore(conversation)
    router = _RemoteRouter()
    app = _app(router)
    elicitation_router = APIRouter()
    routes_elicitations.register_elicitations_routes(
        elicitation_router,
        conversation_store=store,  # type: ignore[arg-type]
        agent_store=cast(AgentStore, SimpleNamespace()),
        runner_router=router,  # type: ignore[arg-type]
    )
    endpoint = _endpoint(elicitation_router, "/resolve")

    async def access(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(conversation=conversation)

    monkeypatch.setattr(routes_elicitations, "_get_user_id", lambda *_args: "user")
    monkeypatch.setattr(routes_elicitations, "_require_access_and_level", access)

    async def immediate_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(routes_elicitations.asyncio, "to_thread", immediate_to_thread)

    with pytest.raises(OmnigentError) as exc_info:
        await endpoint(
            _request(app, "/sessions/conv_replica/elicitations/elicit_replica/resolve"),
            "conv_replica",
            "elicit_replica",
            ElicitationResult(action="accept"),
        )

    assert exc_info.value.code == ErrorCode.WRONG_REPLICA
    assert app.state.routing_stats.snapshot()["elicitation_resolve_off_owner_total"] == 1


@pytest.mark.asyncio
async def test_approval_event_shared_resolver_is_classified_before_tombstone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The approval-event resolver rejects a remote pod before pre-resolving."""
    conversation = _conversation("conv_approval_remote")
    store = _ConversationStore(conversation)
    elicitation_id = "elicit_approval_remote"

    async def immediate_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(orchestration.asyncio, "to_thread", immediate_to_thread)
    pending_elicitations.reset_for_tests()
    try:
        with pytest.raises(OmnigentError) as exc_info:
            await orchestration._resolve_elicitation(
                conversation.id,
                {"elicitation_id": elicitation_id, "action": "accept"},
                _RemoteRouter(),  # type: ignore[arg-type]
                store,  # type: ignore[arg-type]
            )

        assert exc_info.value.code == ErrorCode.WRONG_REPLICA
        assert elicitation_id not in orchestration._harness_pre_resolved_elicitations
    finally:
        pending_elicitations.reset_for_tests()


@pytest.mark.asyncio
async def test_approval_event_posted_to_remote_replica_is_not_silently_acked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The actual approval POST raises so owner-forward can replay it."""
    conversation = _conversation("conv_posted_approval_remote")
    store = _ConversationStore(conversation)
    router = _RemoteRouter()
    event_router = APIRouter()
    routes_events.register_events_routes(
        event_router,
        conversation_store=store,  # type: ignore[arg-type]
        agent_store=cast(AgentStore, SimpleNamespace()),
        runner_router=router,  # type: ignore[arg-type]
    )
    endpoint = _endpoint(event_router, "/events")

    async def access(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(conversation=conversation)

    async def immediate_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(routes_events, "_get_user_id", lambda *_args: "user")
    monkeypatch.setattr(routes_events, "_require_access_and_level", access)
    monkeypatch.setattr(routes_events.asyncio, "to_thread", immediate_to_thread)

    with pytest.raises(OmnigentError) as exc_info:
        await endpoint(
            _request(_app(router), "/sessions/conv_posted_approval_remote/events"),
            "conv_posted_approval_remote",
            SessionEventInput(
                type="approval",
                data={"elicitation_id": "elicit_posted_remote", "action": "accept"},
            ),
        )

    assert exc_info.value.code == ErrorCode.WRONG_REPLICA


@pytest.mark.asyncio
async def test_all_browser_action_routes_classify_remote_parked_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Request, claim, and result never read a remote pod's local registry."""
    conversation = _conversation("conv_browser_remote")
    store = _ConversationStore(conversation)
    router = _RemoteRouter()
    app = _app(router)
    browser_router = APIRouter()
    routes_browser.register_browser_routes(
        browser_router,
        conversation_store=store,  # type: ignore[arg-type]
    )

    async def access(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(conversation=conversation)

    monkeypatch.setattr(routes_browser, "_get_user_id", lambda *_args: "user")
    monkeypatch.setattr(routes_browser, "_require_access_and_level", access)
    monkeypatch.setattr(routes_browser, "get_server_runner_router", lambda: router)

    endpoints = {
        "action_request": _endpoint(browser_router, "/browser/action_request"),
        "action_claim": _endpoint(browser_router, "/browser/action_claim/{action_id}"),
        "action_result": _endpoint(browser_router, "/browser/action_result/{action_id}"),
    }
    calls = (
        (endpoints["action_request"], ("conv_browser_remote", {"action": "click"})),
        (endpoints["action_claim"], ("conv_browser_remote", "baction_remote")),
        (
            endpoints["action_result"],
            ("conv_browser_remote", "baction_remote", {"claim_token": "token"}),
        ),
    )

    for endpoint, args in calls:
        with pytest.raises(OmnigentError) as exc_info:
            if len(args) == 2 and isinstance(args[1], dict):
                await endpoint(
                    _request(
                        app,
                        "/sessions/conv_browser_remote/browser/action_request",
                        args[1],
                    ),
                    *args,
                )
            elif len(args) == 2:
                await endpoint(
                    _request(
                        app,
                        "/sessions/conv_browser_remote/browser/action_claim/baction_remote",
                    ),
                    *args,
                )
            else:
                await endpoint(
                    _request(
                        app,
                        "/sessions/conv_browser_remote/browser/action_result/baction_remote",
                    ),
                    *args,
                )
        assert exc_info.value.code == ErrorCode.WRONG_REPLICA

    assert app.state.routing_stats.snapshot()["hook_park_off_owner_total"] == 3


def test_session_list_reads_durable_pending_count_when_local_index_is_empty() -> None:
    """A runner-bound list row uses the cross-replica pending mirror."""
    conversation = _conversation("conv_list_remote")
    conversation.pending_elicitation_count = 3
    item = orchestration._build_session_list_item(
        conversation,
        agent_names_by_id={"agent_replica": "agent"},
        grants=[],
        user_id=None,
        user_is_admin=False,
        permissions_enabled=False,
        pending_count=0,
        child_session_ids=[],
        comments_fingerprint=None,
    )
    assert item.pending_elicitations_count == 3


@pytest.mark.asyncio
async def test_single_session_snapshot_re_raises_wrong_replica(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A remote runner must not produce a hollow single-session response."""
    conversation = _conversation("conv_snapshot_remote")

    class _SnapshotRouter:
        def client_for_session_resources(self, _session_id: str) -> Any:
            raise OmnigentError(
                "remote runner",
                code=ErrorCode.WRONG_REPLICA,
                owner_addr="pod-b:8000",
            )

    async def immediate_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr("omnigent.runtime.get_runner_router", lambda: _SnapshotRouter())
    monkeypatch.setattr("omnigent.runtime.get_runner_client", lambda: None)
    monkeypatch.setattr(orchestration.asyncio, "to_thread", immediate_to_thread)

    with pytest.raises(OmnigentError) as exc_info:
        await orchestration._get_session_snapshot(
            _ConversationStore(conversation),  # type: ignore[arg-type]
            conversation.id,
            include_items=False,
        )
    assert exc_info.value.code == ErrorCode.WRONG_REPLICA
    assert exc_info.value.owner_addr == "pod-b:8000"


def test_child_summary_reads_durable_busy_and_pending_state() -> None:
    """Child summaries fall back to the durable live-state columns."""
    conversation = _conversation("conv_child_remote")
    conversation.kind = "sub_agent"
    conversation.parent_conversation_id = "conv_parent"
    conversation.title = "worker:remote"
    conversation.live_status = "running"
    conversation.pending_elicitation_count = 2
    helpers._session_status_cache.pop(conversation.id, None)
    pending_elicitations.reset_for_tests()

    summary = helpers._child_session_summary_from_conversation(
        conversation,
        "conv_parent",
        None,
    )

    assert summary.busy is True
    assert summary.current_task_status == "in_progress"
    assert summary.pending_elicitations_count == 2
