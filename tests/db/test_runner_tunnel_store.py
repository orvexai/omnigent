"""Concurrency and durable attribution coverage for runner tunnel ownership."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from omnigent.stores.host_store import HostStore


def test_simultaneous_runner_claims_are_atomic_and_keep_a_live_owner(db_uri: str) -> None:
    """Two pods can claim one runner without an integrity or stale-write error."""
    runner_id = "runner_token_concurrent_claim_b0c8ab2431"
    store = HostStore(db_uri).runner_tunnel_store
    barrier = Barrier(2)

    def claim(owner_addr: str) -> None:
        barrier.wait()
        HostStore(db_uri).runner_tunnel_store.claim(runner_id, owner_addr)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(claim, owner) for owner in ("pod-a:8000", "pod-b:8000")]
        for future in futures:
            future.result()

    assert store.owner_addr(runner_id) in {"pod-a:8000", "pod-b:8000"}

    store.claim(runner_id, "pod-live:8000")
    assert store.owner_addr(runner_id) == "pod-live:8000"


def test_runner_claim_persists_host_attribution_across_release(db_uri: str) -> None:
    """Runner host attribution remains available after a tunnel lease ends."""
    runner_id = "runner_token_durable_host_attribution_b0c8ab2431"
    host_id = "host_0123456789abcdef0123456789abcdef"
    store = HostStore(db_uri).runner_tunnel_store

    store.claim(runner_id, "pod-a:8000", host_id=host_id)
    store.release(runner_id, "pod-a:8000")

    assert store.host_id(runner_id) == host_id
