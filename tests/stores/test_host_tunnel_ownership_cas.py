"""CAS coverage for cross-replica host tunnel ownership."""

from __future__ import annotations

from omnigent.stores.host_store import HostStore


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
