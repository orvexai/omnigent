"""Focused tests for Stage 0 routing observability."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from omnigent.errors import ErrorCode, OmnigentError
from omnigent.server import _elicitation_registry
from omnigent.server.routing_stats import (
    RoutingStats,
    record_elicitation_resolution_if_off_owner,
    record_hook_park_if_off_owner,
)


@dataclass
class _Conversation:
    runner_id: str | None


class _Router:
    def __init__(self, online: set[str]) -> None:
        self.online = online

    def runner_is_online(self, runner_id: str) -> bool:
        return runner_id in self.online


class _DurableOwnerRouter(_Router):
    def runner_owner_addr(self, runner_id: str) -> str | None:
        return "pod-b:8000" if runner_id == "runner-remote" else None


def test_routing_stats_counter_only_counts_wrong_replica() -> None:
    stats = RoutingStats()

    stats.record_wrong_replica()
    assert stats.snapshot() == {
        "wrong_replica_total": 1,
        "elicitation_resolve_off_owner_total": 0,
        "hook_park_off_owner_total": 0,
        "forward_attempted_total": 0,
        "forward_succeeded_total": 0,
        "forward_failed_total": 0,
        "wrong_replica_returned_to_client_total": 0,
    }


@pytest.mark.asyncio
async def test_off_owner_elicitation_resolution_is_loud_and_does_not_tombstone() -> None:
    """Stage 2 rejects a remote resolution before local tombstone insertion."""
    stats = RoutingStats()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(routing_stats=stats, pod_addr="pod-a:8000"))
    )
    conversation = _Conversation("runner-remote")
    router = _DurableOwnerRouter(set())
    elicitation_id = "stage2-remote-elicitation"

    _elicitation_registry._harness_pre_resolved_elicitations.pop(elicitation_id, None)
    with pytest.raises(OmnigentError) as exc_info:
        await record_elicitation_resolution_if_off_owner(request, conversation, router)

    assert exc_info.value.code == ErrorCode.WRONG_REPLICA
    assert exc_info.value.owner_addr == "pod-b:8000"
    assert elicitation_id not in _elicitation_registry._harness_pre_resolved_elicitations
    assert stats.snapshot()["elicitation_resolve_off_owner_total"] == 1


@pytest.mark.asyncio
async def test_offline_runner_is_not_counted_as_off_owner() -> None:
    """A runner offline everywhere is not an off-owner hit, and not refused.

    Both sites report what the harness already did. With no remote owner
    to forward to there is nothing a live runner would add, so refusing
    would only lose the report — on a single-replica deployment that is
    the ONLY branch these guards can reach.
    """
    stats = RoutingStats()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(routing_stats=stats, pod_addr="pod-a:8000"))
    )
    offline = _Conversation("runner-offline")
    router = _DurableOwnerRouter(set())

    await record_elicitation_resolution_if_off_owner(request, offline, router)
    await record_hook_park_if_off_owner(
        request,
        session_id="conv-offline",
        conversation_store=SimpleNamespace(),
        runner_router=router,
        conversation=offline,
    )
    assert stats.snapshot()["elicitation_resolve_off_owner_total"] == 0
    assert stats.snapshot()["hook_park_off_owner_total"] == 0


@pytest.mark.asyncio
async def test_offline_hook_is_not_counted_as_off_owner() -> None:
    """A hook park with no remote owner passes through uncounted."""
    stats = RoutingStats()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(routing_stats=stats, pod_addr="pod-a:8000"))
    )
    await record_hook_park_if_off_owner(
        request,
        session_id="conv-offline",
        conversation_store=SimpleNamespace(),
        runner_router=_DurableOwnerRouter(set()),
        conversation=_Conversation("runner-offline"),
    )
    assert stats.snapshot()["hook_park_off_owner_total"] == 0


@pytest.mark.asyncio
async def test_remote_durable_owner_is_counted_for_both_silent_sites() -> None:
    stats = RoutingStats()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(routing_stats=stats, pod_addr="pod-a:8000"))
    )
    remote = _Conversation("runner-remote")
    router = _DurableOwnerRouter(set())

    with pytest.raises(OmnigentError) as elicitation_error:
        await record_elicitation_resolution_if_off_owner(request, remote, router)
    assert elicitation_error.value.code == ErrorCode.WRONG_REPLICA
    with pytest.raises(OmnigentError) as hook_error:
        await record_hook_park_if_off_owner(
            request,
            session_id="conv-remote",
            conversation_store=SimpleNamespace(),
            runner_router=router,
            conversation=remote,
        )
    assert hook_error.value.code == ErrorCode.WRONG_REPLICA
    assert stats.snapshot()["elicitation_resolve_off_owner_total"] == 1
    assert stats.snapshot()["hook_park_off_owner_total"] == 1


@pytest.mark.asyncio
async def test_wrong_replica_handler_counts_and_stats_endpoint_has_no_identifiers(
    app, client, caplog
) -> None:
    handler = app.exception_handlers[OmnigentError]
    request = Request(
        {
            "type": "http",
            "app": app,
            "method": "GET",
            "scheme": "http",
            "path": "/test-routing/conv-secret",
            "query_string": b"",
            "headers": [],
            "path_params": {"session_id": "conv-secret"},
            "state": {},
        }
    )
    response = await handler(
        request,
        OmnigentError("runner is on another replica", code=ErrorCode.WRONG_REPLICA),
    )
    assert response.status_code == 400
    assert app.state.routing_stats.snapshot()["wrong_replica_total"] == 1
    record = next(
        record
        for record in caplog.records
        if record.message.startswith("wrong_replica routing failure:")
    )
    assert "owner_addr=None" in record.message
    assert "owner_resolvable=False" in record.message
    assert "forward_attempted=False" in record.message
    assert record.routing == {
        "path": "/test-routing/conv-secret",
        "session_id": "conv-secret",
        "host_id": None,
        "owner_resolvable": False,
        "forward_attempted": False,
    }

    await handler(
        request,
        OmnigentError("bad input", code=ErrorCode.INVALID_INPUT),
    )
    assert app.state.routing_stats.snapshot()["wrong_replica_total"] == 1

    exposed = await client.get("/v1/internal/routing-stats")
    assert exposed.status_code == 200
    assert exposed.json() == app.state.routing_stats.snapshot()
    assert "conv-secret" not in exposed.text
    assert "session_id" not in exposed.text
