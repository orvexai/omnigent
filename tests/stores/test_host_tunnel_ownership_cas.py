"""CAS coverage for cross-replica host tunnel ownership."""

from __future__ import annotations

from sqlalchemy import update
from sqlalchemy.orm import Session

from omnigent.db.db_models import SqlRunnerTunnel
from omnigent.db.utils import get_or_create_engine, now_epoch
from omnigent.stores.conversation_store import RUNNER_LIVENESS_TTL_S
from omnigent.stores.host_store import HostStore


def _set_runner_updated_at(db_uri: str, runner_id: str, value: int) -> None:
    """Force a runner lease timestamp for deterministic freshness probes."""
    engine = get_or_create_engine(db_uri)
    with Session(engine) as session:
        session.execute(
            update(SqlRunnerTunnel)
            .where(SqlRunnerTunnel.runner_id == runner_id)
            .values(updated_at=value)
        )
        session.commit()


def test_stale_host_disconnect_cannot_clobber_reconnected_owner(db_uri: str) -> None:
    """A stale pod's close and heartbeat cannot overwrite pod B's claim."""
    store = HostStore(db_uri)
    host_id = "a0c8ab2431b35377abb4232febeded94"

    store.upsert_on_connect(host_id, "laptop", "alice@example.com", owner_addr="pod-a:8000")
    store.upsert_on_connect(host_id, "laptop", "alice@example.com", owner_addr="pod-b:8000")

    store.set_offline(host_id, owner_addr="pod-a:8000")
    store.heartbeat(host_id, owner_addr="pod-a:8000")

    current = store.get_host(host_id)
    assert current is not None
    assert current.status == "online"
    assert current.owner_addr == "pod-b:8000"


def test_stale_runner_release_cannot_delete_reconnected_owner(db_uri: str) -> None:
    """Runner tunnel release uses the same owner compare-and-set rule."""
    store = HostStore(db_uri)
    runner_id = "runner_token_b0c8ab2431b35377abb4232febeded94"
    tunnels = store.runner_tunnel_store

    tunnels.claim(runner_id, "pod-a:8000")
    tunnels.claim(runner_id, "pod-b:8000")
    tunnels.release(runner_id, "pod-a:8000")
    tunnels.heartbeat(runner_id, "pod-a:8000")

    assert tunnels.owner(runner_id) == "pod-b:8000"


def test_stale_runner_owner_is_not_honored(db_uri: str) -> None:
    """A dead pod's abandoned lease cannot select a forwarding owner."""
    store = HostStore(db_uri)
    runner_id = "runner_token_older_than_ttl_b0c8ab2431b35377abb4232"
    tunnels = store.runner_tunnel_store

    tunnels.claim(runner_id, "pod-b:8000")
    _set_runner_updated_at(db_uri, runner_id, now_epoch() - RUNNER_LIVENESS_TTL_S - 1)

    assert tunnels.owner(runner_id) is None


def test_fresh_runner_heartbeat_keeps_owner_honored(db_uri: str) -> None:
    """The tunnel ping heartbeat renews a lease before it can expire."""
    store = HostStore(db_uri)
    runner_id = "runner_token_refreshed_within_ttl_b0c8ab2431"
    tunnels = store.runner_tunnel_store

    tunnels.claim(runner_id, "pod-b:8000")
    _set_runner_updated_at(db_uri, runner_id, now_epoch() - RUNNER_LIVENESS_TTL_S - 1)
    tunnels.heartbeat(runner_id, "pod-b:8000")

    assert tunnels.owner(runner_id) == "pod-b:8000"


def test_runner_release_removes_owner_during_disconnect_grace_window(db_uri: str) -> None:
    """A request during teardown gets a local unavailable result, not a dead forward."""
    store = HostStore(db_uri)
    runner_id = "runner_token_release_grace_window_b0c8ab2431"
    tunnels = store.runner_tunnel_store

    tunnels.claim(runner_id, "pod-a:8000")
    tunnels.release(runner_id, "pod-a:8000")

    assert tunnels.owner(runner_id) is None
