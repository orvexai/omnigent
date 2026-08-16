"""
Shared handling for ``POST /v1/sessions/{id}/events`` outcomes.

The route answers a **policy denial** with HTTP **202** and a body of
``{"queued": false, "denied": true, "reason": ...}``. Every caller that
checked only the status code therefore read a refused event as delivered,
and each one turned that into a different silent stall:

- a denied sub-agent message left work registered for a turn that never
  starts, wedging the child as permanently busy;
- a denied *wake notice* left a completed result sitting in a sleeping
  parent's inbox with nothing left to rouse it — the wake IS the delivery
  signal, so nothing else was ever going to fire;
- a denied first message left a freshly created child that never runs,
  reported to the orchestrator as a successful create.

Lives in its own module so both the runner app and the tool dispatcher can
use it without importing each other (``tool_dispatch`` reaches ``app``
lazily inside functions, and a module-level edge either way would risk a
cycle).
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

_DENIAL_FALLBACK_REASON = "denied by policy"


def event_denial_reason(resp: httpx.Response) -> str | None:
    """
    Return the denial reason when an accepted-looking event POST was refused.

    :param resp: The event POST response. A non-2xx is NOT a denial — it is
        an ordinary failure the caller should handle on status.
    :returns: The reason when the body reports a denial, else ``None``.
    """
    try:
        body = resp.json()
    except ValueError:
        return None
    if not isinstance(body, dict) or not body.get("denied"):
        return None
    reason = body.get("reason")
    return reason if isinstance(reason, str) and reason else _DENIAL_FALLBACK_REASON


@dataclass(frozen=True)
class EventPostResult:
    """
    Outcome of posting a session event, with denial distinguished.

    A plain ``bool`` cannot express this: a denial must NOT be retried (the
    policy will refuse again, and for a gate that parks on a human ASK each
    retry re-parks another one, producing duplicate approval cards), while a
    transport failure should be. Collapsing the two is what made the wake
    path report a lie.

    :param delivered: Whether the event was actually accepted.
    :param denial_reason: Why it was refused, when it was. ``None`` on
        success and on transport/status failure alike — check *delivered*
        to tell those apart.
    """

    delivered: bool
    denial_reason: str | None = None

    @property
    def denied(self) -> bool:
        """:returns: ``True`` when the event was refused by policy."""
        return self.denial_reason is not None


__all__ = ["EventPostResult", "event_denial_reason"]
