"""Deterministic PostgreSQL checks for pool retention and timeout behavior."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import TimeoutError as SqlAlchemyTimeoutError
from sqlalchemy.pool import NullPool

from omnigent.db.utils import _create_engine, make_managed_session_maker


def _require_postgres(db_uri: str) -> None:
    if not db_uri.startswith("postgresql"):
        pytest.skip("requires OMNIGENT_TEST_DB_URI pointing at PostgreSQL")


def _other_backend_count(observer) -> int:
    with observer.connect() as connection:
        return int(
            connection.execute(
                text(
                    "SELECT count(*) "
                    "FROM pg_stat_activity "
                    "WHERE backend_type = 'client backend' "
                    "AND datname = current_database() "
                    "AND pid <> pg_backend_pid()"
                )
            ).scalar_one()
        )


def _wait_for_backend_count(observer, maximum: int, timeout: float = 3) -> int:
    """Allow asynchronous pool return/close work to settle before asserting."""
    deadline = time.monotonic() + timeout
    count = _other_backend_count(observer)
    while count > maximum and time.monotonic() < deadline:
        time.sleep(0.05)
        count = _other_backend_count(observer)
    return count


def test_burst_connections_are_not_retained_above_pool_size(
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 48-checkout burst retains only the configured eight pooled backends."""
    _require_postgres(db_uri)
    monkeypatch.setenv("OMNIGENT_DB_POOL_SIZE", "8")
    monkeypatch.setenv("OMNIGENT_DB_MAX_OVERFLOW", "40")
    monkeypatch.setenv("OMNIGENT_DB_POOL_TIMEOUT_SECONDS", "3")

    engine = _create_engine(db_uri)
    observer = create_engine(db_uri, poolclass=NullPool)
    baseline = _other_backend_count(observer)
    session_maker = make_managed_session_maker(engine)
    release = threading.Event()
    ready = threading.Event()
    ready_count = 0
    ready_lock = threading.Lock()

    def hold_checkout() -> None:
        nonlocal ready_count
        with session_maker() as session:
            session.execute(text("SELECT 1"))
            with ready_lock:
                ready_count += 1
                if ready_count == 48:
                    ready.set()
            release.wait(timeout=10)

    executor = ThreadPoolExecutor(max_workers=48)
    futures = [executor.submit(hold_checkout) for _ in range(48)]
    try:
        assert ready.wait(timeout=10), f"only {ready_count}/48 checkouts became ready"
        peak = _other_backend_count(observer)
        assert 48 <= peak - baseline <= 49
    finally:
        release.set()
        for future in futures:
            future.result(timeout=10)
        executor.shutdown(wait=True)

    retained = _wait_for_backend_count(observer, baseline + 8)
    assert retained - baseline <= 8
    observer.dispose()
    engine.dispose()


def test_pool_timeout_reports_saturation_instead_of_hanging(
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A saturated one-connection pool raises after its configured timeout."""
    _require_postgres(db_uri)
    monkeypatch.setenv("OMNIGENT_DB_POOL_SIZE", "1")
    monkeypatch.setenv("OMNIGENT_DB_MAX_OVERFLOW", "0")
    monkeypatch.setenv("OMNIGENT_DB_POOL_TIMEOUT_SECONDS", "1")

    engine = _create_engine(db_uri)
    holder = engine.connect()
    started = threading.Event()
    result: list[tuple[BaseException, float]] = []

    def attempt_checkout() -> None:
        started.set()
        started_at = time.monotonic()
        try:
            with engine.connect():
                pass
        except BaseException as exc:
            result.append((exc, time.monotonic() - started_at))

    thread = threading.Thread(target=attempt_checkout)
    thread.start()
    try:
        assert started.wait(timeout=1)
        thread.join(timeout=3)
        assert not thread.is_alive(), "pool checkout did not honor pool_timeout"
        assert len(result) == 1
        error, elapsed = result[0]
        assert isinstance(error, SqlAlchemyTimeoutError)
        assert 0.9 <= elapsed <= 3
    finally:
        holder.close()
        thread.join(timeout=1)
        engine.dispose()
