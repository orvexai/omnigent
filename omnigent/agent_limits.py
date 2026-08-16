"""
Per-host caps on how many agent sessions may run at once.

A host is a finite machine: every agent session on it holds a terminal, a
harness process and its memory. Nothing bounded that, so an orchestrator
fanning out could keep spawning until the box degraded, and the failure
surfaced as unrelated timeouts rather than a refusal anyone could act on.

Two dimensions, because they fail differently. The overall cap protects the
machine. The per-CLI cap protects a single vendor toolchain from being the
thing that saturates it — twenty Codex processes and nothing else is a very
different (and more likely) shape than a balanced mix, and it is the one an
orchestrator drifts into when it has a favourite worker.

Limits resolve runtime override > ``config.yaml`` > built-in default, so a
change made through the MCP tool takes effect on the next create with no
restart, while the file remains the durable record.

Config shape (``~/.omnigent/config.yaml``)::

    agent_limits:
      max_per_host: 50
      max_per_cli_per_host: 20

NOTE: these are enforced by the tool layer an agent calls, not by the
server, so they bound what AGENTS do. A human in the web UI is not subject
to them.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from omnigent.config import global_config_path, load_effective_config, save_global_config

CONFIG_KEY = "agent_limits"
MAX_PER_HOST_KEY = "max_per_host"
MAX_PER_CLI_PER_HOST_KEY = "max_per_cli_per_host"

# Chosen to be a guard rail, not a scheduler: high enough that ordinary
# fan-out never notices, low enough that a runaway loop is stopped while the
# machine is still healthy.
DEFAULT_MAX_PER_HOST = 50
DEFAULT_MAX_PER_CLI_PER_HOST = 20

# Runtime override, set through the MCP tool. Deliberately process-local and
# deliberately NOT seeded from the config: absent means "no override", so
# ``current_limits`` keeps re-reading the file and a config edit is picked up
# without anyone having called the tool.
_override: AgentLimits | None = None


@dataclass(frozen=True)
class AgentLimits:
    """
    Effective per-host agent caps.

    :param max_per_host: Most agent sessions allowed on one host, e.g. ``50``.
    :param max_per_cli_per_host: Most sessions of any ONE CLI/harness on one
        host, e.g. ``20`` — so a single toolchain cannot consume the whole
        host budget.
    """

    max_per_host: int
    max_per_cli_per_host: int


def _coerce_positive_int(value: object, fallback: int) -> int:
    """
    Read a positive int from config, falling back on anything unusable.

    A malformed limit must not disable the cap or crash a create, so a bad
    value degrades to the default rather than propagating.

    :param value: Raw config value.
    :param fallback: Default to use when *value* is not a positive int.
    :returns: The usable limit.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return fallback
    return value if value > 0 else fallback


def configured_limits() -> AgentLimits:
    """
    Return the limits from config, ignoring any runtime override.

    :returns: The file-backed limits, defaults filling anything unset.
    """
    try:
        raw = load_effective_config().get(CONFIG_KEY)
    except Exception:  # noqa: BLE001 — a broken config must not block creates
        raw = None
    section = raw if isinstance(raw, dict) else {}
    return AgentLimits(
        max_per_host=_coerce_positive_int(section.get(MAX_PER_HOST_KEY), DEFAULT_MAX_PER_HOST),
        max_per_cli_per_host=_coerce_positive_int(
            section.get(MAX_PER_CLI_PER_HOST_KEY), DEFAULT_MAX_PER_CLI_PER_HOST
        ),
    )


def current_limits() -> AgentLimits:
    """
    Return the limits in force right now.

    :returns: The runtime override when one has been applied, else the
        config-backed values.
    """
    return _override if _override is not None else configured_limits()


@dataclass(frozen=True)
class LimitUpdate:
    """
    Outcome of an attempt to change the limits.

    :param limits: The values now in force.
    :param persisted_path: Config file written, or ``None`` when the change
        is runtime-only.
    :param persist_error: Why persistence failed, when it did. A non-``None``
        value means the change WILL be lost on restart, and the caller must
        say so rather than reporting a clean success.
    """

    limits: AgentLimits
    persisted_path: Path | None
    persist_error: str | None


def apply_limits(
    *,
    max_per_host: int | None = None,
    max_per_cli_per_host: int | None = None,
    persist: bool = True,
) -> LimitUpdate:
    """
    Change the limits immediately, and record them for the next restart.

    The runtime override is set first and unconditionally, so the new value
    governs the very next create whether or not the file can be written —
    an agent that raised a cap to get unblocked stays unblocked even on a
    read-only filesystem. Persistence is then attempted separately and its
    failure REPORTED rather than raised, because a silent runtime-only
    change would come back as a mysterious regression after the next
    restart.

    :param max_per_host: New overall per-host cap; ``None`` keeps the
        current value.
    :param max_per_cli_per_host: New per-CLI per-host cap; ``None`` keeps
        the current value.
    :param persist: Whether to write the change to ``config.yaml``.
    :returns: The resulting limits and an honest persistence status.
    :raises ValueError: If a supplied limit is not a positive integer —
        a zero or negative cap would silently refuse every create.
    """
    global _override
    base = current_limits()
    for name, value in (
        (MAX_PER_HOST_KEY, max_per_host),
        (MAX_PER_CLI_PER_HOST_KEY, max_per_cli_per_host),
    ):
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer; got {value!r}")
    updated = AgentLimits(
        max_per_host=base.max_per_host if max_per_host is None else max_per_host,
        max_per_cli_per_host=(
            base.max_per_cli_per_host if max_per_cli_per_host is None else max_per_cli_per_host
        ),
    )
    _override = updated
    if not persist:
        return LimitUpdate(limits=updated, persisted_path=None, persist_error=None)
    try:
        from omnigent.config import load_global_config

        config = load_global_config()
        section = config.get(CONFIG_KEY)
        merged = dict(section) if isinstance(section, dict) else {}
        merged[MAX_PER_HOST_KEY] = updated.max_per_host
        merged[MAX_PER_CLI_PER_HOST_KEY] = updated.max_per_cli_per_host
        config[CONFIG_KEY] = merged
        written = save_global_config(config)
    except Exception as exc:  # noqa: BLE001 — reported to the caller, not raised
        return LimitUpdate(
            limits=updated,
            persisted_path=None,
            persist_error=f"{type(exc).__name__}: {exc}",
        )
    return LimitUpdate(limits=updated, persisted_path=written, persist_error=None)


def reset_for_tests() -> None:
    """Drop the runtime override so each test sees config-backed limits."""
    global _override
    _override = None


def config_file_path() -> Path:
    """
    Return the config file limits are persisted to.

    :returns: The user-level ``config.yaml`` path.
    """
    return global_config_path()


__all__ = [
    "DEFAULT_MAX_PER_CLI_PER_HOST",
    "DEFAULT_MAX_PER_HOST",
    "AgentLimits",
    "LimitUpdate",
    "apply_limits",
    "config_file_path",
    "configured_limits",
    "current_limits",
    "reset_for_tests",
]
