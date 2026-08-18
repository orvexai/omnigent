"""In-process counters for session-routing observability.

The server is a single-process, single-event-loop application. These counters
therefore need no locking and deliberately reset when the process restarts.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass
class RoutingStats:
    """Counters exposed by the routing observability endpoint."""

    wrong_replica_total: int = 0
    elicitation_resolve_off_owner_total: int = 0
    hook_park_off_owner_total: int = 0

    def record_wrong_replica(self) -> None:
        """Count a ``wrong_replica`` response."""
        self.wrong_replica_total += 1

    def record_elicitation_resolve_off_owner(self) -> None:
        """Count an elicitation resolution that missed the local tunnel."""
        self.elicitation_resolve_off_owner_total += 1

    def record_hook_park_off_owner(self) -> None:
        """Count a hook park that missed the local tunnel."""
        self.hook_park_off_owner_total += 1

    def snapshot(self) -> dict[str, int]:
        """Return only aggregate counters; never include routing identifiers."""
        return {
            "wrong_replica_total": self.wrong_replica_total,
            "elicitation_resolve_off_owner_total": self.elicitation_resolve_off_owner_total,
            "hook_park_off_owner_total": self.hook_park_off_owner_total,
        }


def stats_for_request(request: Any) -> RoutingStats | None:
    """Return request app routing stats when this is a fully wired server."""
    app = getattr(request, "app", None)
    return getattr(getattr(app, "state", None), "routing_stats", None)


def record_elicitation_resolution_if_off_owner(
    request: Any,
    conversation: Any,
    runner_router: Any,
) -> None:
    """Count an external elicitation resolution missing its local tunnel."""
    stats = stats_for_request(request)
    runner_id = getattr(conversation, "runner_id", None)
    if stats is not None and runner_id and runner_router is not None:
        if not runner_router.runner_is_online(runner_id):
            stats.record_elicitation_resolve_off_owner()


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
        stats.record_hook_park_off_owner()
