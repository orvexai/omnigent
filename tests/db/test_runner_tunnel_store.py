"""Concurrency and durable attribution coverage for runner tunnel ownership."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event

from sqlalchemy import event, update
from sqlalchemy.orm import Session

from omnigent.db.db_models import SqlRunnerTunnel
from omnigent.db.utils import get_or_create_engine, now_epoch
from omnigent.stores.host_store import HostStore


def test_delayed_older_generation_cannot_overwrite_newer_claim(db_uri: str) -> None:
    """A delayed worker commit cannot steal ownership from a newer connection."""
    runner_id = "runner_token_delayed_claim_fence_b0c8ab2431"
    store = HostStore(db_uri).runner_tunnel_store
    old_generation = store.allocate_generation(runner_id)
    claim_started = Event()
    allow_old_claim = Event()
    engine = get_or_create_engine(db_uri)

    def pause_old_claim(
        _conn,
        _cursor,
        _statement,
        parameters,
        _context,
        _executemany,
    ) -> None:
        if "pod-a:8000" in repr(parameters) and not claim_started.is_set():
            claim_started.set()
            assert allow_old_claim.wait(timeout=10)

    event.listen(engine, "before_cursor_execute", pause_old_claim)
    try:

        def delayed_claim() -> None:
            store.claim(
                runner_id,
                "pod-a:8000",
                generation=old_generation,
            )

        with ThreadPoolExecutor(max_workers=1) as executor:
            old_claim = executor.submit(delayed_claim)
            assert claim_started.wait(timeout=10)

            new_generation = store.allocate_generation(runner_id)
            store.claim(
                runner_id,
                "pod-b:8000",
                generation=new_generation,
            )
            allow_old_claim.set()
            old_claim.result()
    finally:
        event.remove(engine, "before_cursor_execute", pause_old_claim)

    assert store.owner_addr(runner_id) == "pod-b:8000"


def test_older_generation_cannot_heartbeat_or_release_newer_connection(
    db_uri: str,
) -> None:
    """Heartbeat and teardown are fenced even when the pod address is reused."""
    runner_id = "runner_token_generation_guarded_cleanup_b0c8ab2431"
    store = HostStore(db_uri).runner_tunnel_store
    old_generation = store.allocate_generation(runner_id)
    store.claim(runner_id, "pod-a:8000", generation=old_generation)
    new_generation = store.allocate_generation(runner_id)
    store.claim(runner_id, "pod-a:8000", generation=new_generation)

    engine = get_or_create_engine(db_uri)
    with Session(engine) as session:
        session.execute(
            update(SqlRunnerTunnel)
            .where(SqlRunnerTunnel.runner_id == runner_id)
            .values(updated_at=now_epoch() - 1000)
        )
        session.commit()

    store.heartbeat(runner_id, "pod-a:8000", generation=old_generation)
    store.release(runner_id, "pod-a:8000", generation=old_generation)

    assert store.owner(runner_id) is None
    assert store.owner_addr(runner_id) == "pod-a:8000"

    store.heartbeat(runner_id, "pod-a:8000", generation=new_generation)
    assert store.owner(runner_id) == "pod-a:8000"


def test_runner_claim_persists_host_attribution_across_release(db_uri: str) -> None:
    """Runner host attribution remains available after a tunnel lease ends."""
    runner_id = "runner_token_durable_host_attribution_b0c8ab2431"
    host_id = "host_0123456789abcdef0123456789abcdef"
    store = HostStore(db_uri).runner_tunnel_store

    store.claim(runner_id, "pod-a:8000", host_id=host_id)
    store.release(runner_id, "pod-a:8000")

    assert store.host_id(runner_id) == host_id
