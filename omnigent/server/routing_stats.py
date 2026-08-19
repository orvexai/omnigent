"""In-process counters for session-routing observability.

The server is a single-process, single-event-loop application. These counters
therefore need no locking and deliberately reset when the process restarts.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from omnigent.errors import ErrorCode, OmnigentError


@dataclass
class RoutingStats:
    """Counters exposed by the routing observability endpoint."""

    wrong_replica_total: int = 0
    elicitation_resolve_off_owner_total: int = 0
    hook_park_off_owner_total: int = 0
    forward_attempted_total: int = 0
    forward_succeeded_total: int = 0
    forward_failed_total: int = 0
    wrong_replica_returned_to_client_total: int = 0

    def record_wrong_replica(self) -> None:
        """Count a ``wrong_replica`` response."""
        self.wrong_replica_total += 1

    def record_elicitation_resolve_off_owner(self) -> None:
        """Count an elicitation resolution that missed the local tunnel."""
        self.elicitation_resolve_off_owner_total += 1

    def record_hook_park_off_owner(self) -> None:
        """Count a hook park that missed the local tunnel."""
        self.hook_park_off_owner_total += 1

    def record_forward_attempted(self) -> None:
        """Count a replay attempted by the owner-forwarding middleware."""
        self.forward_attempted_total += 1

    def record_forward_succeeded(self) -> None:
        """Count a replay that received an upstream response."""
        self.forward_succeeded_total += 1

    def record_forward_failed(self) -> None:
        """Count a replay that could not reach or read the owner response."""
        self.forward_failed_total += 1

    def record_forward_returned(self) -> None:
        """Count a classified wrong-replica response returned to the caller."""
        self.wrong_replica_returned_to_client_total += 1

    def snapshot(self) -> dict[str, int]:
        """Return only aggregate counters; never include routing identifiers."""
        return {
            "wrong_replica_total": self.wrong_replica_total,
            "elicitation_resolve_off_owner_total": self.elicitation_resolve_off_owner_total,
            "hook_park_off_owner_total": self.hook_park_off_owner_total,
            "forward_attempted_total": self.forward_attempted_total,
            "forward_succeeded_total": self.forward_succeeded_total,
            "forward_failed_total": self.forward_failed_total,
            "wrong_replica_returned_to_client_total": self.wrong_replica_returned_to_client_total,
        }


def stats_for_request(request: Any) -> RoutingStats | None:
    """Return request app routing stats when this is a fully wired server."""
    app = getattr(request, "app", None)
    return getattr(getattr(app, "state", None), "routing_stats", None)


async def record_elicitation_resolution_if_off_owner(
    request: Any,
    conversation: Any,
    runner_router: Any,
) -> None:
    """Count an elicitation resolution whose durable owner is remote.

    A runner absent from this process is not necessarily on another pod; it
    may be offline everywhere. Only a remote durable owner is an off-owner hit,
    and only that case is refused: the caller is reporting what its harness
    already did, so there is nothing for a live runner to do here.
    """
    stats = stats_for_request(request)
    runner_id = getattr(conversation, "runner_id", None)
    if stats is not None and runner_id and runner_router is not None:
        if not runner_router.runner_is_online(runner_id):
            owner = (
                await asyncio.to_thread(_owner_for_request, request, runner_router, runner_id)
                if _stage2_active(request, runner_router)
                else None
            )
            if owner is not None:
                stats.record_elicitation_resolve_off_owner()
                raise OmnigentError(
                    "session elicitation is on another replica; retry",
                    code=ErrorCode.WRONG_REPLICA,
                    owner_addr=owner,
                )


async def record_hook_park_if_off_owner(
    request: Any,
    *,
    session_id: str,
    conversation_store: Any,
    runner_router: Any,
    conversation: Any = None,
) -> None:
    """Count a hook park missing its local runner tunnel.

    Normal authenticated hook requests pass the conversation already loaded
    by authorization. Auth-disabled and admin requests use the existing store
    as a fallback because their authorization path intentionally skips it.
    """
    stats = stats_for_request(request)
    if stats is None or runner_router is None:
        return
    if conversation is None:
        conversation = await asyncio.to_thread(
            conversation_store.get_conversation,
            session_id,
        )
    runner_id = getattr(conversation, "runner_id", None)
    if runner_id and not runner_router.runner_is_online(runner_id):
        owner = _owner_for_request(request, runner_router, runner_id)
        if owner is not None:
            stats.record_hook_park_off_owner()
            raise OmnigentError(
                "session hook is on another replica; retry",
                code=ErrorCode.WRONG_REPLICA,
                owner_addr=owner,
            )


def _owner_for_request(request: Any, runner_router: Any, runner_id: str) -> str | None:
    """Return a remote durable owner, or ``None`` while Stage 1 is inert."""
    app = getattr(request, "app", None)
    pod_addr = getattr(getattr(app, "state", None), "pod_addr", None)
    owner_lookup = getattr(runner_router, "runner_owner_addr", None)
    if pod_addr is None or owner_lookup is None:
        return None
    owner = owner_lookup(runner_id)
    return owner if owner is not None and owner != pod_addr else None


def _stage2_active(request: Any, runner_router: Any) -> bool:
    """Return whether durable ownership classification is wired."""
    app = getattr(request, "app", None)
    pod_addr = getattr(getattr(app, "state", None), "pod_addr", None)
    return pod_addr is not None and callable(getattr(runner_router, "runner_owner_addr", None))
