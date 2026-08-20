"""Integration tests for the PostgreSQL migration advisory-lock boundary."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import Engine, create_engine, text

from omnigent.db import utils


@pytest.fixture()
def postgres_engine(db_uri: str) -> Iterator[Engine]:
    """Use the per-worker database only in the real PostgreSQL lane."""
    engine = create_engine(db_uri)
    if engine.dialect.name != "postgresql":
        engine.dispose()
        pytest.skip("migration lock integration tests require PostgreSQL")
    try:
        yield engine
    finally:
        engine.dispose()


def _lock_observation(engine: Engine) -> tuple[int, bool | None]:
    """Observe the migration lock from a backend other than its holder."""
    high = utils._MIGRATION_LOCK_KEY >> 32
    low = utils._MIGRATION_LOCK_KEY & 0xFFFFFFFF
    probe_engine = create_engine(engine.url.render_as_string(hide_password=False))
    try:
        with probe_engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        """
                    SELECT count(*) AS holders,
                           bool_and(a.xact_start IS NULL) AS all_no_txn
                    FROM pg_locks AS l
                    JOIN pg_stat_activity AS a ON a.pid = l.pid
                    WHERE l.locktype = 'advisory'
                      AND l.granted
                      AND l.classid = :high
                      AND l.objid = :low
                      AND l.objsubid = 1
                      AND l.database = (
                          SELECT oid FROM pg_database
                          WHERE datname = current_database()
                      )
                      AND a.pid <> pg_backend_pid()
                    """
                    ),
                    {"high": high, "low": low},
                )
                .mappings()
                .one()
            )
            return int(row["holders"]), row["all_no_txn"]
    finally:
        probe_engine.dispose()


def test_lock_is_held_without_a_snapshot(
    postgres_engine: Engine,
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lock exists, stays around all schema work, and holds no snapshot."""
    from alembic import command

    from omnigent.db.db_models import ConversationBase, OmnigentBase

    observed: dict[str, tuple[int, bool | None]] = {}

    def probe(label: str) -> None:
        observed[label] = _lock_observation(postgres_engine)

    monkeypatch.setattr(command, "upgrade", lambda *_args, **_kwargs: probe("upgrade"))
    monkeypatch.setattr(
        OmnigentBase.metadata,
        "create_all",
        lambda *_args, **_kwargs: probe("create_all:OmnigentBase"),
    )
    monkeypatch.setattr(
        ConversationBase.metadata,
        "create_all",
        lambda *_args, **_kwargs: probe("create_all:ConversationBase"),
    )

    utils._run_migrations(postgres_engine, db_uri)

    assert observed == {
        "upgrade": (1, True),
        "create_all:OmnigentBase": (1, True),
        "create_all:ConversationBase": (1, True),
    }


@pytest.mark.timeout(120)
def test_full_chain_completes_on_a_fresh_postgres_database(
    postgres_engine: Engine, db_uri: str
) -> None:
    """A fresh PostgreSQL database reaches head through the real migration chain."""
    with postgres_engine.begin() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))

    utils._run_migrations(postgres_engine, db_uri)

    assert utils._get_current_db_revision(postgres_engine) == utils._get_head_db_revision(db_uri)


def test_contended_lock_logs_before_blocking(
    postgres_engine: Engine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A contended lock explains its wait, then still acquires after release."""
    blocker = postgres_engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    blocker.execute(
        text("SELECT pg_advisory_lock(:lock_key)"),
        {"lock_key": utils._MIGRATION_LOCK_KEY},
    )
    warning_seen = threading.Event()
    errors: list[BaseException] = []

    class _WarningSignal(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if "Waiting for the migration advisory lock" in record.getMessage():
                warning_seen.set()

    signal = _WarningSignal()
    utils._logger.addHandler(signal)
    acquired = threading.Event()

    def wait_for_lock() -> None:
        try:
            with utils._migration_lock(postgres_engine):
                acquired.set()
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    worker = threading.Thread(target=wait_for_lock)
    try:
        with caplog.at_level(logging.WARNING, logger=utils._logger.name):
            worker.start()
            assert warning_seen.wait(timeout=5)
            blocker.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": utils._MIGRATION_LOCK_KEY},
            )
            worker.join(timeout=5)
    finally:
        utils._logger.removeHandler(signal)
        blocker.close()
        if worker.is_alive():
            worker.join(timeout=5)

    assert not errors
    assert acquired.is_set()
    assert any(
        "Waiting for the migration advisory lock" in record.message for record in caplog.records
    )


def test_contended_lock_wait_is_bounded(
    postgres_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stuck migration holder cannot make boot wait forever."""
    monkeypatch.setenv("OMNIGENT_MIGRATION_LOCK_TIMEOUT_SECONDS", "0.1")
    blocker = postgres_engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    blocker.execute(
        text("SELECT pg_advisory_lock(:lock_key)"),
        {"lock_key": utils._MIGRATION_LOCK_KEY},
    )
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="Timed out after"):
            with utils._migration_lock(postgres_engine):
                pytest.fail("the contended lock should not be acquired")
    finally:
        blocker.execute(
            text("SELECT pg_advisory_unlock(:lock_key)"),
            {"lock_key": utils._MIGRATION_LOCK_KEY},
        )
        blocker.close()
    assert time.monotonic() - started < 2


def test_migration_lock_timeout_is_positive_and_allows_minutes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default gives long migrations room, while zero remains invalid."""
    monkeypatch.delenv("OMNIGENT_MIGRATION_LOCK_TIMEOUT_SECONDS", raising=False)
    assert utils._migration_lock_timeout_seconds() == 600.0

    monkeypatch.setenv("OMNIGENT_MIGRATION_LOCK_TIMEOUT_SECONDS", "0")
    with pytest.raises(RuntimeError, match="OMNIGENT_MIGRATION_LOCK_TIMEOUT_SECONDS"):
        utils._migration_lock_timeout_seconds()


class _ChangingBackendResult:
    def mappings(self) -> _ChangingBackendResult:
        return self

    def one(self) -> dict[str, object]:
        return {"backend_pid": 101, "acquired": True}

    def scalar_one(self) -> int:
        return 202


class _ChangingBackendConnection:
    def __enter__(self) -> _ChangingBackendConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execution_options(self, **_kwargs: object) -> _ChangingBackendConnection:
        return self

    def execute(self, statement: object, _params: object = None) -> _ChangingBackendResult:
        return _ChangingBackendResult()


class _ChangingBackendEngine:
    dialect = SimpleNamespace(name="postgresql")

    def connect(self) -> _ChangingBackendConnection:
        return _ChangingBackendConnection()


class _BackendPidResult:
    def __init__(self, pid: int) -> None:
        self._pid = pid

    def scalar_one(self) -> int:
        return self._pid


class _MigrationFailureChangingBackendConnection:
    def __init__(self) -> None:
        self.backend_checks = 0
        self.unlock_attempts = 0

    def __enter__(self) -> _MigrationFailureChangingBackendConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execution_options(self, **_kwargs: object) -> _MigrationFailureChangingBackendConnection:
        return self

    def execute(self, statement: object, _params: object = None) -> object:
        sql = str(statement)
        if "pg_try_advisory_lock" in sql:
            return _ChangingBackendResult()
        if "pg_backend_pid" in sql:
            self.backend_checks += 1
            return _BackendPidResult(101 if self.backend_checks == 1 else 202)
        if "pg_advisory_unlock" in sql:
            self.unlock_attempts += 1
        return _ChangingBackendResult()


class _MigrationFailureChangingBackendEngine:
    dialect = SimpleNamespace(name="postgresql")

    def __init__(self, connection: _MigrationFailureChangingBackendConnection) -> None:
        self.connection = connection

    def connect(self) -> _MigrationFailureChangingBackendConnection:
        return self.connection


def test_migration_lock_rejects_backend_rebinding() -> None:
    """A backend PID change aborts before schema work can begin."""
    with pytest.raises(RuntimeError, match="not pinned"):
        with utils._migration_lock(_ChangingBackendEngine()):
            pytest.fail("an unpinned migration connection must not yield")


def test_migration_lock_preserves_migration_error_during_cleanup() -> None:
    """Cleanup diagnostics must not replace the migration's real exception."""
    connection = _MigrationFailureChangingBackendConnection()
    engine = _MigrationFailureChangingBackendEngine(connection)

    with pytest.raises(ValueError, match="migration failed"):
        with utils._migration_lock(engine):
            raise ValueError("migration failed")

    assert connection.unlock_attempts == 0


def test_run_migrations_uses_migration_database_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The caller URL wins when only the application URL is in the environment."""
    source_uri = f"sqlite:///{tmp_path / 'explicit.db'}"
    decoy_uri = f"sqlite:///{tmp_path / 'decoy.db'}"
    monkeypatch.delenv("MIGRATION_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", decoy_uri)
    source_engine = create_engine(source_uri)
    decoy_engine = create_engine(decoy_uri)
    try:
        utils._run_migrations(source_engine, source_uri)
        assert utils._get_current_db_revision(source_engine) == utils._get_head_db_revision(
            source_uri
        )
        assert utils._get_current_db_revision(decoy_engine) is None
    finally:
        source_engine.dispose()
        decoy_engine.dispose()


def test_run_migrations_honors_explicit_migration_database_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct migration URL still redirects callers that opt into it."""
    source_uri = f"sqlite:///{tmp_path / 'caller.db'}"
    target_uri = f"sqlite:///{tmp_path / 'migration.db'}"
    monkeypatch.setenv("MIGRATION_DATABASE_URL", target_uri)
    source_engine = create_engine(source_uri)
    try:
        utils._run_migrations(source_engine, source_uri)
        target_engine = create_engine(target_uri)
        try:
            assert utils._get_current_db_revision(target_engine) == utils._get_head_db_revision(
                target_uri
            )
            assert utils._get_current_db_revision(source_engine) is None
        finally:
            target_engine.dispose()
    finally:
        source_engine.dispose()


def test_non_postgres_lock_is_a_no_op(tmp_path: Path) -> None:
    """SQLite keeps the existing no-op migration-lock behaviour."""
    engine = create_engine(f"sqlite:///{tmp_path / 'migration-lock.db'}")
    try:
        with utils._migration_lock(engine):
            with engine.connect() as connection:
                assert connection.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()


def test_ahead_of_code_revision_reports_rollback(tmp_path: Path) -> None:
    """An unknown revision explains rollback-across-a-migration recovery."""
    db_path = tmp_path / "ahead.db"
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        with engine.begin() as connection:
            connection.execute(
                text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
            )
            connection.execute(text("INSERT INTO alembic_version VALUES ('zzzz_future_rev')"))

        with pytest.raises(RuntimeError, match="rollback-across-a-migration") as exc_info:
            utils._initialize_or_verify_schema(engine, f"sqlite:///{db_path}")
        message = str(exc_info.value)
        assert "ahead of this code" in message
        assert "Roll forward" in message
        assert "restore" in message
        assert "out of date" not in message
    finally:
        engine.dispose()
