"""In-memory registry of live host WebSocket connections.

Each server replica maintains one :class:`HostRegistry` tracking
hosts with active WebSocket tunnels on this replica. The persistent
``hosts`` DB table (queried by ``HostStore``) is the cross-replica
source of truth for which hosts exist; this registry only tracks
which hosts are live *here*.

Simpler than :class:`TunnelRegistry` because the host tunnel
carries only control frames (launch/stop runner), not HTTP
request/response traffic. No per-request reassembly queues needed.

The registry also holds what connected hosts *report* about themselves and
nothing persists — today the per-family gateway-inference map (see
:mod:`omnigent.gateway_inference`). It is delivered on the connect handshake, so
a replica that has never seen a host simply knows nothing about it, and the
readers' unknown-is-backed rule covers that window until the host reconnects and
re-reports.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from cachetools import TTLCache

from omnigent.db.db_models import InvalidUuidError, current_workspace_id, uuid_to_bytes
from omnigent.host.frames import HostHelloFrame, HostStopRunnerFrame, decode_host_frame

if TYPE_CHECKING:
    from omnigent.runner.transports.ws_tunnel.registry import TunnelRegistry

_logger = logging.getLogger(__name__)


def _canonical_host_id(host_id: str) -> str:
    """Reduce a host id to the canonical bare-hex form used as the key.

    Host ids reach the registry in every spelling ``uuid_to_bytes``
    accepts: the bare 32-char hex the tunnel route registers under,
    the legacy ``host_<hex>`` form that pre-migration clients still
    send in REST paths, and the dashed uuid form. The DB layer
    normalizes all of them (``Uuid16``), so the registry must key on
    the same canonical form — otherwise a legacy-form lookup misses a
    live tunnel and runner launches 409 "host is offline" while
    ``GET /v1/hosts`` reports the host online. Ids that aren't
    uuid-shaped at all are keyed verbatim so they simply miss.

    :param host_id: A host id in any accepted spelling, e.g.
        ``"host_a1b2..."``, ``"a1b2..."``, or the dashed uuid.
    :returns: The bare-hex form, or *host_id* unchanged when it is
        not uuid-shaped.
    """
    try:
        return uuid_to_bytes(host_id).hex()
    except InvalidUuidError:
        return host_id


# How long a runner exit report stays answerable, and how many are kept.
# Reports only matter while a client is still waiting for the runner to
# come online (a 60s window today); 10 minutes covers slow retries with
# margin. Runner ids are unique per launch, so entries never need
# invalidation — the TTL is purely a memory bound.
_EXIT_REPORT_TTL_S = 600.0
_EXIT_REPORT_MAX_ENTRIES = 1024
_RUNNER_ATTRIBUTION_MAX_ENTRIES = 4096


@dataclass
class RunnerExitReport:
    """A host daemon's report that a spawned runner died unexpectedly.

    :param error: Human-readable cause composed by the daemon (exit
        code, host-side log path, log tail), e.g.
        ``"runner process exited with code 1 (log on host: ~/...)"``.
    :param owner: User who owns the host tunnel the report arrived on,
        e.g. ``"alice@example.com"``. ``None`` when auth is disabled.
        Gates visibility: only the owner may read the report (the log
        tail can contain agent output).
    """

    error: str
    owner: str | None


class RunnerExitReports:
    """Thread-safe, TTL-bounded store of runner exit reports.

    Written by the host tunnel when a ``host.runner_exited`` frame
    arrives; read by the runner status endpoint so a client polling a
    never-connecting runner learns *why* instead of timing out.
    In-memory and per-replica, same posture as :class:`HostRegistry` —
    the report and the status poll meet on the replica holding the
    host tunnel.
    """

    def __init__(self) -> None:
        """Initialize an empty report store."""
        self._lock = threading.Lock()
        self._reports: TTLCache[str, RunnerExitReport] = TTLCache(
            maxsize=_EXIT_REPORT_MAX_ENTRIES,
            ttl=_EXIT_REPORT_TTL_S,
        )

    def record(self, runner_id: str, error: str, owner: str | None) -> None:
        """Store a runner exit report.

        :param runner_id: The dead runner, e.g. ``"runner_abc123"``.
        :param error: Human-readable cause from the host daemon.
        :param owner: Owner of the reporting host tunnel, or ``None``
            when auth is disabled.
        """
        with self._lock:
            self._reports[runner_id] = RunnerExitReport(error=error, owner=owner)

    def get(self, runner_id: str) -> str | None:
        """Look up a report's error without owner scoping.

        For callers that have already authorized access by another
        means (e.g. the session snapshot, gated on session permission):
        the report pertains to that session's own runner, so no
        separate owner check is needed. The runner status endpoint —
        keyed only by ``runner_id`` with no session-level auth — must
        use :meth:`get_visible` instead.

        :param runner_id: Runner id, e.g. ``"runner_abc123"``.
        :returns: The error message, or ``None`` when no report exists.
        """
        with self._lock:
            report: RunnerExitReport | None = self._reports.get(runner_id)
        return report.error if report is not None else None

    def get_visible(self, runner_id: str, user_id: str | None) -> str | None:
        """Look up a report, scoped to its owner.

        :param runner_id: Runner id, e.g. ``"runner_abc123"``.
        :param user_id: The requesting user, or ``None`` when auth is
            disabled.
        :returns: The error message, or ``None`` when no report exists
            or the caller doesn't own it (W6-2 posture: other users'
            runners reveal nothing).
        """
        with self._lock:
            report: RunnerExitReport | None = self._reports.get(runner_id)
        if report is None:
            return None
        if user_id is not None and report.owner is not None and report.owner != user_id:
            return None
        return report.error


class WebSocketLike(Protocol):
    """Minimal WebSocket protocol for the host tunnel.

    Both Starlette's ``WebSocket`` and test fakes implement this.
    """

    async def send_text(self, data: str) -> None:
        """Send a text frame."""
        ...

    async def receive_text(self) -> str:
        """Receive a text frame."""
        ...


@dataclass
class RunnerAttribution:
    """Host generation that launched or advertised a runner."""

    host_id: str
    host_generation: int


@dataclass
class HostConnection:
    """Per-host state while the tunnel is open.

    :param workspace_id: Tenant partition the tunnel belongs to,
        mirroring the ``hosts`` table's ``(workspace_id, host_id)`` PK.
        Captured at register time so ``send_text``'s replaced-connection
        guard keys on the full ``(workspace_id, host_id)`` without
        reading request context from the long-lived sender loop.
    :param host_id: Stable host identifier, e.g.
        ``"host_a1b2c3d4..."``.
    :param generation: Server-assigned connection generation. It is allocated
        before the hello is read so delayed hellos cannot become current.
    :param ws: The live WebSocket to this host.
    :param hello: The hello frame the host sent on connect.
    :param owner: Authenticated user who established the tunnel,
        e.g. ``"alice@example.com"``. ``None`` when auth is
        disabled (single-user mode).
    :param outbound_queue: Queue consumed by the WebSocket route's
        sender task. Control frames are enqueued here rather than
        calling ``ws.send_text`` directly, since the caller may
        be on a different thread.
    :param connected_at: Unix epoch float of connect time.
    :param last_frame_at: Unix epoch float of the most recent
        frame from this host.
    :param pending_launches: Per-``request_id`` futures for
        in-flight ``host.launch_runner`` requests. Resolved when
        the host sends ``host.launch_runner_result``.
    :param pending_stops: Per-``request_id`` futures for
        in-flight ``host.stop_runner`` requests. Resolved when
        the host sends ``host.stop_runner_result``.
    :param pending_runner_status: Per-``request_id`` futures for
        in-flight ``host.runner_status`` queries. Resolved when the
        host sends ``host.runner_status_result``. Values carry the
        single ``status`` field (``"alive"`` / ``"dead"`` /
        ``"unknown"``).
    :param pending_stats: Per-``request_id`` futures for in-flight
        ``host.stat`` requests. Resolved when the host sends
        ``host.stat_result``. The dict values carry the full
        stat-result fields (``status``, ``exists``, ``type``,
        ``canonical_path``, ``error``); typed as ``Any`` because
        Python ``dict`` parametric types here would force every
        callsite to cast.
    :param pending_list_dirs: Per-``request_id`` futures for
        in-flight ``host.list_dir`` requests. Resolved when the
        host sends ``host.list_dir_result``. Values carry the
        listing fields (``status``, ``entries`` as list of
        dicts, ``has_more``, ``error``). Same ``Any`` typing
        rationale as ``pending_stats``.
    :param pending_create_worktrees: Per-``request_id`` futures for
        in-flight ``host.create_worktree`` requests. Resolved when
        the host sends ``host.create_worktree_result``. Values
        carry the result fields (``status``, ``worktree_path``,
        ``branch``, ``error``). Same ``Any`` typing rationale as
        ``pending_stats``.
    :param pending_remove_worktrees: Per-``request_id`` futures for
        in-flight ``host.remove_worktree`` requests. Resolved when
        the host sends ``host.remove_worktree_result``. Values
        carry ``status`` and ``error``.
    :param pending_create_dirs: Per-``request_id`` futures for
        in-flight ``host.create_dir`` requests. Resolved when the
        host sends ``host.create_dir_result``. Values carry the
        result fields (``status``, ``path``, ``error``). Same
        ``Any`` typing rationale as ``pending_stats``.
    :param pending_installs: Per-``request_id`` futures for in-flight
        ``host.install_harness`` requests. Resolved when the host sends
        ``host.install_harness_result``. Values carry the result fields
        (``status``, ``configured_harnesses``, ``gateway_inference``,
        ``error``). Same ``Any`` typing rationale as ``pending_stats``.
    :param inflight_installs: Install tasks used to coalesce concurrent
        install requests for the same harness family (a double-click, or
        two spellings of one npm package) onto one in-flight install, so
        npm's non-race-safe global writes never run twice at once. Keyed by
        the resolved install key (not ``request_id``) and cleared when the
        install completes.
    :param pending_secret_writes: Per-``request_id`` futures for in-flight
        ``host.store_secret`` requests (a UI-driven harness credential write).
        Resolved when the host sends ``host.store_secret_result``. Values carry
        the result fields (``status``, ``configured_harnesses``,
        ``gateway_inference``, ``error``) — never the secret. Same ``Any``
        typing rationale as ``pending_stats``.
    :param credential_write_lock: Serializes credential writes to this host so
        two overlapping requests (a double-click, or key + gateway in quick
        succession) can't interleave the daemon's non-atomic
        load→merge→save of ``config.yaml`` and clobber a sibling ``providers:``
        entry. Held around the whole store-secret round-trip.
    :param pending_fs_requests: Per-``request_id`` futures for
        in-flight ``host.fs_request`` reads (the workspace file
        panel served from the host while the runner is offline).
        Resolved when the host sends ``host.fs_result``. Values
        carry ``status``, ``payload``, ``error_status``,
        ``error_code``, and ``error``.
    :param pending_model_options: Per-``request_id`` futures for pre-launch
        model catalogs resolved by the selected host.
    """

    workspace_id: int
    host_id: str
    generation: int
    ws: WebSocketLike
    hello: HostHelloFrame
    owner: str | None
    outbound_queue: asyncio.Queue[str | None]
    connected_at: float
    last_frame_at: float
    pending_launches: dict[str, asyncio.Future[dict[str, str | None]]] = field(
        default_factory=dict,
    )
    pending_stops: dict[str, asyncio.Future[dict[str, str | None]]] = field(
        default_factory=dict,
    )
    pending_stop_runner_ids: dict[str, str] = field(default_factory=dict)
    pending_runner_status: dict[str, asyncio.Future[dict[str, str | None]]] = field(
        default_factory=dict,
    )
    pending_stats: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_list_dirs: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_create_worktrees: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_remove_worktrees: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_list_worktrees: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_create_dirs: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_installs: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    inflight_installs: dict[str, asyncio.Task[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_secret_writes: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_credential_detects: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    credential_write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending_fs_requests: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )
    pending_model_options: dict[str, asyncio.Future[dict[str, Any]]] = field(
        default_factory=dict,
    )


class HostRegistry:
    """Thread-safe registry of live host WebSocket connections.

    All public methods acquire ``_lock`` so callers on different
    threads (e.g. REST route handlers vs. WebSocket event loops)
    don't race.

    When host and runner state must be coordinated, the lock order is
    ``HostRegistry._lock`` then ``TunnelRegistry._lock``. The host lock is
    held through inventory reconciliation, so supersession and runner
    retirement form one generation-fenced operation.
    """

    def __init__(self) -> None:
        """Initialize an empty host registry."""
        self._lock = threading.RLock()
        # Keyed by (workspace_id, host_id) to mirror the hosts-table PK:
        # one stable host_id can be live in more than one workspace.
        self._hosts: dict[tuple[int, str], HostConnection] = {}
        # Last gateway-inference map each host reported, keyed by canonical
        # host_id alone: the map describes the machine's local config, so the
        # same machine connected to two workspaces reports the same answer.
        # Kept across a host disconnect (a tunnel flap shouldn't blank a known
        # answer) and lost with the process, which is the point — a restarted
        # server re-learns it from the reconnect handshake.
        self._gateway_inference: dict[str, dict[str, bool]] = {}
        # Retain launch/hello attribution so a runner tunnel can be reconciled
        # after its host reconnects while this server process is warm. Entries
        # are removed by runner lifecycle cleanup or successful reconciliation.
        self._runner_hosts: dict[tuple[int, str], RunnerAttribution] = {}
        self._next_generations: dict[tuple[int, str], int] = {}

    def allocate_generation(self, host_id: str, workspace_id: int | None = None) -> int:
        """Allocate a connection generation before reading a host hello."""
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        key = (ws_id, _canonical_host_id(host_id))
        with self._lock:
            return self._allocate_generation_locked(key)

    def _allocate_generation_locked(self, key: tuple[int, str]) -> int:
        generation = self._next_generations.get(key, 0) + 1
        self._next_generations[key] = generation
        return generation

    def _remember_runner_attribution_locked(
        self,
        key: tuple[int, str],
        attribution: RunnerAttribution,
    ) -> None:
        self._runner_hosts.pop(key, None)
        self._runner_hosts[key] = attribution
        while len(self._runner_hosts) > _RUNNER_ATTRIBUTION_MAX_ENTRIES:
            self._runner_hosts.pop(next(iter(self._runner_hosts)))

    def register(
        self,
        host_id: str,
        ws: WebSocketLike,
        hello: HostHelloFrame,
        owner: str | None,
        workspace_id: int | None = None,
        *,
        generation: int | None = None,
        runner_registry: TunnelRegistry | None = None,
    ) -> HostConnection:
        """Register a host connection (newest wins).

        If ``(workspace_id, host_id)`` is already registered (stale
        connection), the old connection is replaced and its outbound
        queue is poisoned with ``None`` so the sender loop exits.

        Scoping the key by workspace means the same stable ``host_id``
        (a laptop's config id) connecting to two workspaces is tracked
        as two independent connections rather than one evicting the
        other — matching the ``hosts`` table's ``(workspace_id,
        host_id)`` PK.

        :param host_id: Stable host identifier, e.g.
            ``"host_a1b2c3d4..."``.
        :param ws: The live WebSocket.
        :param hello: The hello frame from the host.
        :param owner: Authenticated user ID, or ``None``.
        :param workspace_id: Tenant partition the connection belongs to.
            Defaults to the request-bound :func:`current_workspace_id`
            (``0`` in single-tenant deployments); captured into the
            connection so ``send_text`` need not read request context
            from the sender loop.
        :param generation: Generation allocated at WebSocket acceptance.
            When omitted, tests and in-process callers allocate one here.
        :param runner_registry: Optional runner registry used to atomically
            update a live session when this hello changes its host generation.
        :returns: The new :class:`HostConnection`. Its ``host_id`` is
            the canonical form (see :func:`_canonical_host_id`).
        """
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        host_id = _canonical_host_id(host_id)
        now = time.time()
        with self._lock:
            key = (ws_id, host_id)
            if generation is None:
                generation = self._allocate_generation_locked(key)
            else:
                self._next_generations[key] = max(
                    self._next_generations.get(key, 0),
                    generation,
                )
            conn = HostConnection(
                workspace_id=ws_id,
                host_id=host_id,
                generation=generation,
                ws=ws,
                hello=hello,
                owner=owner,
                outbound_queue=asyncio.Queue(),
                connected_at=now,
                last_frame_at=now,
            )
            old = self._hosts.get(key)
            if old is not None and generation <= old.generation:
                return conn
            if old is not None:
                _logger.info(
                    "replacing stale host connection: ws=%s host=%s",
                    ws_id,
                    host_id,
                )
                old.outbound_queue.put_nowait(None)
            self._hosts[key] = conn
            for runner_id in hello.runners:
                self._remember_runner_attribution_locked(
                    (ws_id, runner_id),
                    RunnerAttribution(
                        host_id=host_id,
                        host_generation=generation,
                    ),
                )
                if runner_registry is not None:
                    runner_registry.update_runner_attribution(
                        runner_id,
                        host_id,
                        generation,
                    )
        return conn

    def record_runner_attribution(
        self,
        host_id: str,
        runner_id: str,
        *,
        workspace_id: int | None = None,
        conn: HostConnection | None = None,
        runner_registry: TunnelRegistry | None = None,
    ) -> bool:
        """Remember that a host launched or advertised ``runner_id``.

        :param host_id: Host identifier, in any accepted spelling.
        :param runner_id: Stable runner identifier, e.g. ``"runner_abc"``.
        :param workspace_id: Tenant partition; defaults to the current one.
        :param conn: Optional host generation guard.
        :returns: ``True`` when the attribution was recorded.
        """
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        canonical_host_id = _canonical_host_id(host_id)
        with self._lock:
            current = self._hosts.get((ws_id, canonical_host_id))
            if current is None or (conn is not None and current is not conn):
                return False
            self._remember_runner_attribution_locked(
                (ws_id, runner_id),
                RunnerAttribution(
                    host_id=canonical_host_id,
                    host_generation=current.generation,
                ),
            )
            if runner_registry is not None:
                runner_registry.update_runner_attribution(
                    runner_id,
                    canonical_host_id,
                    current.generation,
                )
            return True

    def runner_host_id(self, runner_id: str, workspace_id: int | None = None) -> str | None:
        """Return the locally known host attribution for a runner."""
        attribution = self.runner_host_attribution(runner_id, workspace_id)
        return attribution[0] if attribution is not None else None

    def runner_host_attribution(
        self,
        runner_id: str,
        workspace_id: int | None = None,
    ) -> tuple[str, int] | None:
        """Return the host and connection generation for a runner."""
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        with self._lock:
            attribution = self._runner_hosts.get((ws_id, runner_id))
            if attribution is None:
                return None
            return attribution.host_id, attribution.host_generation

    def forget_runner_attribution(
        self,
        runner_id: str,
        *,
        workspace_id: int | None = None,
        host_id: str | None = None,
        host_generation: int | None = None,
        conn: HostConnection | None = None,
    ) -> bool:
        """Remove attribution if it still belongs to the expected owner."""
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        canonical_host_id = _canonical_host_id(host_id) if host_id is not None else None
        with self._lock:
            if conn is not None and self._hosts.get((ws_id, conn.host_id)) is not conn:
                return False
            key = (ws_id, runner_id)
            attribution = self._runner_hosts.get(key)
            if attribution is None:
                return False
            if canonical_host_id is not None and attribution.host_id != canonical_host_id:
                return False
            if host_generation is not None and attribution.host_generation != host_generation:
                return False
            self._runner_hosts.pop(key, None)
            return True

    def reconcile_runner_inventory(
        self,
        conn: HostConnection,
        runner_registry: TunnelRegistry,
        *,
        observed_sequence: int,
    ) -> list[str]:
        """Reconcile this current host generation against its hello frame.

        :param conn: Host connection that received the hello frame.
        :param runner_registry: Live runner-tunnel registry on this replica.
        :param observed_sequence: Runner-registry sequence captured when the
            hello was received.
        :returns: Runner IDs removed from the live registry.
        """
        with self._lock:
            if self._hosts.get((conn.workspace_id, conn.host_id)) is not conn:
                return []
            attributed = {
                runner_id: attribution.host_generation
                for (workspace_id, runner_id), attribution in self._runner_hosts.items()
                if (
                    workspace_id == conn.workspace_id
                    and attribution.host_id == conn.host_id
                    and attribution.host_generation <= conn.generation
                )
            }
            removed = runner_registry.reconcile_host(
                conn.host_id,
                conn.hello.runners,
                observed_sequence=observed_sequence,
                attributed_runner_generations=attributed,
                inventory_generation=conn.generation,
            )
            for runner_id in removed:
                attribution = self._runner_hosts.get((conn.workspace_id, runner_id))
                if (
                    attribution is not None
                    and attribution.host_id == conn.host_id
                    and attribution.host_generation <= conn.generation
                ):
                    self._runner_hosts.pop((conn.workspace_id, runner_id), None)
            advertised = set(conn.hello.runners)
            for key, attribution in list(self._runner_hosts.items()):
                workspace_id, runner_id = key
                if (
                    workspace_id == conn.workspace_id
                    and attribution.host_id == conn.host_id
                    and attribution.host_generation <= conn.generation
                    and runner_id not in advertised
                ):
                    self._runner_hosts.pop(key, None)
            return removed

    def deregister(
        self,
        host_id: str,
        workspace_id: int | None = None,
        conn: HostConnection | None = None,
    ) -> bool:
        """Remove a host connection and end its sender loop.

        No-op if ``(workspace_id, host_id)`` is not registered.

        :param host_id: Host identifier to remove, in any accepted
            spelling (see :func:`_canonical_host_id`).
        :param workspace_id: Tenant partition; defaults to
            :func:`current_workspace_id`.
        :param conn: Optional generation guard, as on
            :meth:`TunnelRegistry.deregister`. When given, the entry is
            removed only if it is still this exact connection.
        :returns: ``True`` when an entry was removed. ``False`` means
            nothing was registered or the guard did not match, so the
            caller is superseded and must not flip the host's durable
            row offline — that row describes the live reconnect.
        """
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        with self._lock:
            key = (ws_id, _canonical_host_id(host_id))
            current = self._hosts.get(key)
            if current is None or (conn is not None and current is not conn):
                return False
            removed = self._hosts.pop(key)
        # Without this the route handler's loops keep running and its ping loop
        # keeps the host row online, even though the host is now unreachable.
        removed.outbound_queue.put_nowait(None)
        return True

    def mark_frame_seen(self, conn: HostConnection) -> bool:
        """Record that a frame arrived for ``conn``.

        :param conn: Connection that received the frame.
        :returns: ``True`` if the connection is still current,
            ``False`` if it has been replaced or deregistered.
        """
        with self._lock:
            if self._hosts.get((conn.workspace_id, conn.host_id)) is not conn:
                return False
            conn.last_frame_at = time.time()
            return True

    def get(self, host_id: str, workspace_id: int | None = None) -> HostConnection | None:
        """Look up a live host connection.

        :param host_id: Host identifier, in any accepted spelling
            (see :func:`_canonical_host_id`).
        :param workspace_id: Tenant partition; defaults to
            :func:`current_workspace_id`.
        :returns: The :class:`HostConnection` if online,
            otherwise ``None``.
        """
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        with self._lock:
            return self._hosts.get((ws_id, _canonical_host_id(host_id)))

    def is_current(self, conn: HostConnection) -> bool:
        """Return whether this connection is the highest accepted generation."""
        with self._lock:
            return self._hosts.get((conn.workspace_id, conn.host_id)) is conn

    def online_host_ids(self, workspace_id: int | None = None) -> list[str]:
        """Return IDs of all hosts connected in one workspace.

        :param workspace_id: Tenant partition; defaults to
            :func:`current_workspace_id`.
        :returns: List of host_id strings live in the workspace.
        """
        ws_id = current_workspace_id() if workspace_id is None else workspace_id
        with self._lock:
            return [hid for (ws, hid) in self._hosts if ws == ws_id]

    def is_host_telemetry_opted_out(self, host_id: str, workspace_id: int | None = None) -> bool:
        """Return whether the host has opted out of telemetry.

        :param host_id: Host identifier, e.g. ``"host_a1b2c3d4..."``.
        :param workspace_id: Tenant partition; defaults to
            :func:`current_workspace_id`.
        :returns: ``True`` when the host sent ``telemetry_opt_out=True``
            in its hello frame.  Defaults to ``False`` when the host is
            offline or unknown.
        """
        conn = self.get(host_id, workspace_id)
        if conn is None:
            return False
        return conn.hello.telemetry_opt_out

    def get_host_installation_id(
        self, host_id: str, workspace_id: int | None = None
    ) -> str | None:
        """Return the installation ID the host advertised in its hello frame.

        :param host_id: Host identifier, e.g. ``"host_a1b2c3d4..."``.
        :param workspace_id: Tenant partition; defaults to
            :func:`current_workspace_id`.
        :returns: The host's installation ID, or ``None`` when offline or
            not set.
        """
        conn = self.get(host_id, workspace_id)
        if conn is None:
            return None
        return conn.hello.installation_id

    def record_gateway_inference(
        self,
        host_id: str,
        gateway_inference: Mapping[str, bool] | None,
    ) -> None:
        """Store the gateway-inference map a host just reported.

        Called for every frame that carries the map — the connect handshake and
        each readiness refresh — so the server's view is delivered rather than
        persisted. ``None`` (a host that cannot evaluate the map at all) clears
        the entry back to unknown instead of recording "nothing is backed".

        :param host_id: Host identifier, in any accepted spelling (see
            :func:`_canonical_host_id`).
        :param gateway_inference: Harness spelling → gateway-backed flag, e.g.
            ``{"claude-native": True, "codex": False}``, or ``None``.
        """
        key = _canonical_host_id(host_id)
        with self._lock:
            if gateway_inference is None:
                self._gateway_inference.pop(key, None)
            else:
                self._gateway_inference[key] = dict(gateway_inference)

    def gateway_inference(self, host_id: str) -> dict[str, bool] | None:
        """Return the gateway-inference map *host_id* last reported here.

        :param host_id: Host identifier, in any accepted spelling.
        :returns: A copy of the reported map, or ``None`` when this replica has
            never had a report from the host — unknown, which readers treat as
            gateway-backed rather than unavailable.
        """
        with self._lock:
            reported = self._gateway_inference.get(_canonical_host_id(host_id))
        return dict(reported) if reported is not None else None

    def send_text(self, conn: HostConnection, data: str) -> None:
        """Enqueue a text frame for sending to the host.

        Must be called on the host WebSocket's owning event loop.
        ``asyncio.Queue`` is coroutine-safe within a single loop, NOT
        thread-safe — ``put_nowait`` mutates the underlying deque
        without a lock. Every current caller (REST handlers, the WS
        receive loop, the ping loop) runs on the uvicorn event loop,
        so the call below is safe. A caller on another thread must use
        ``loop.call_soon_threadsafe(queue.put_nowait, data)`` instead.

        :param conn: The target host connection.
        :param data: JSON-encoded frame text.
        :raises ConnectionError: If the connection has been
            replaced (the outbound queue was poisoned).
        """
        with self._lock:
            current = self._hosts.get((conn.workspace_id, conn.host_id))
            if current is not conn:
                raise ConnectionError(f"host {conn.host_id!r} connection was replaced")
            try:
                outbound_frame = decode_host_frame(data)
            except ValueError:
                outbound_frame = None
            if isinstance(outbound_frame, HostStopRunnerFrame):
                conn.pending_stop_runner_ids[outbound_frame.request_id] = outbound_frame.runner_id

        conn.outbound_queue.put_nowait(data)
