"""Tests for database engine pool configuration (omnigent/db/utils.py)."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from alembic import command
from sqlalchemy import create_engine

from omnigent.db.utils import (
    _DISABLE_PREPARED_STATEMENTS_ENV,
    _LAKEBASE_POOL_RECYCLE_SECONDS,
    _SERVER_POOL_RECYCLE_SECONDS,
    _build_alembic_config,
    _connect_dbapi_with_retry,
    _create_engine,
    _get_current_db_revision,
    _get_head_db_revision,
    _initialize_or_verify_schema,
    _install_connection_retry,
    _install_lakebase_token_refresh,
    _is_transient_connection_error,
    _resolve_lakebase_token_provider,
    _resolve_migration_database_url,
    build_search_snippet,
    builtin_agent_id,
    clear_engine_cache,
    extract_search_text,
    generate_agent_id,
    generate_item_id,
    get_or_create_engine,
    set_lakebase_token_provider,
    strip_nul_bytes,
)
from omnigent.entities.conversation import (
    ErrorData,
    NewConversationItem,
    ResourceEventData,
    SlashCommandData,
)


class _FakeDbapiError(Exception):
    """Minimal DBAPI error with the psycopg SQLSTATE surface."""

    def __init__(self, message: str, *, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


@pytest.fixture(autouse=True)
def _clean_engine_cache() -> None:
    """
    Clear the module-level engine cache before each test
    so that each test creates a fresh engine.
    """
    clear_engine_cache()


def test_non_sqlite_engine_has_pool_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Non-SQLite engines must be created with pool_pre_ping=True and
    pool_recycle=1800 to prevent stale/dead connections.
    """
    captured_kwargs: dict[str, Any] = {}
    mock_engine = MagicMock()

    def _capturing_create_engine(uri: str, **kwargs: Any) -> MagicMock:
        captured_kwargs.update(kwargs)
        return mock_engine

    monkeypatch.setattr(
        "omnigent.db.utils.create_engine",
        _capturing_create_engine,
    )
    monkeypatch.setattr("omnigent.db.utils.event.listen", lambda *args, **kwargs: None)
    # Skip migrations -- we only care about engine creation kwargs.
    monkeypatch.setattr(
        "omnigent.db.utils._run_migrations",
        lambda engine, db_uri: None,
    )

    get_or_create_engine("postgresql://user:pass@localhost/testdb")

    # pool_pre_ping=True prevents "server has gone away" errors
    # after idle periods. Failure means dead connections won't be
    # detected before checkout, causing intermittent query failures.
    assert captured_kwargs.get("pool_pre_ping") is True

    # pool_recycle=1800 (30 min) prevents stale connections when
    # the database server restarts or closes idle connections.
    # Failure means connections could persist indefinitely and break.
    assert captured_kwargs.get("pool_recycle") == 1800

    assert captured_kwargs["pool_size"] == 32
    assert captured_kwargs["max_overflow"] == 32
    assert captured_kwargs["pool_timeout"] == 10
    assert captured_kwargs["connect_args"] == {"connect_timeout": 3}
    assert captured_kwargs["pool_reset_on_return"] == "rollback"


def test_prepare_threshold_is_disabled_only_under_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, Any]] = []
    mock_engine = MagicMock()

    def _capturing_create_engine(uri: str, **kwargs: Any) -> MagicMock:
        captured.append(kwargs)
        return mock_engine

    monkeypatch.setattr("omnigent.db.utils.create_engine", _capturing_create_engine)
    monkeypatch.setattr("omnigent.db.utils.event.listen", lambda *args, **kwargs: None)
    monkeypatch.delenv(_DISABLE_PREPARED_STATEMENTS_ENV, raising=False)
    _create_engine("postgresql://user:pass@localhost/testdb")
    monkeypatch.setenv(_DISABLE_PREPARED_STATEMENTS_ENV, "1")
    _create_engine("postgresql://user:pass@localhost/testdb")

    assert captured[0]["connect_args"] == {"connect_timeout": 3}
    assert captured[1]["connect_args"] == {
        "connect_timeout": 3,
        "prepare_threshold": None,
    }


def test_non_sqlite_pool_settings_are_environment_configurable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}
    mock_engine = MagicMock()

    def _capturing_create_engine(uri: str, **kwargs: Any) -> MagicMock:
        captured_kwargs.update(kwargs)
        return mock_engine

    monkeypatch.setattr("omnigent.db.utils.create_engine", _capturing_create_engine)
    monkeypatch.setattr("omnigent.db.utils.event.listen", lambda *args, **kwargs: None)
    monkeypatch.setattr("omnigent.db.utils._run_migrations", lambda engine, db_uri: None)
    monkeypatch.setenv("OMNIGENT_DB_POOL_SIZE", "7")
    monkeypatch.setenv("OMNIGENT_DB_MAX_OVERFLOW", "3")
    monkeypatch.setenv("OMNIGENT_DB_POOL_TIMEOUT_SECONDS", "2.5")

    get_or_create_engine("postgresql://user:pass@localhost/testdb")

    assert captured_kwargs["pool_size"] == 7
    assert captured_kwargs["max_overflow"] == 3
    assert captured_kwargs["pool_timeout"] == 2.5


@pytest.mark.parametrize(
    ("env_name", "value"),
    [
        ("OMNIGENT_DB_POOL_SIZE", "abc"),
        ("OMNIGENT_DB_POOL_SIZE", "0"),
        ("OMNIGENT_DB_POOL_SIZE", "-1"),
        ("OMNIGENT_DB_MAX_OVERFLOW", "true"),
        ("OMNIGENT_DB_POOL_TIMEOUT_SECONDS", "0"),
    ],
)
def test_invalid_pool_setting_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    value: str,
) -> None:
    monkeypatch.setenv(env_name, value)

    with pytest.raises(RuntimeError, match=env_name):
        _create_engine("postgresql://user:pass@localhost/testdb")


def test_transient_connection_failure_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The SQLAlchemy new-connection hook retries a refusal and then succeeds."""
    clock = 0.0
    sleeps: list[float] = []
    attempts = 0

    def monotonic() -> float:
        return clock

    def sleep(seconds: float) -> None:
        nonlocal clock
        sleeps.append(seconds)
        clock += seconds

    monkeypatch.setattr("omnigent.db.utils.time.monotonic", monotonic)
    monkeypatch.setattr("omnigent.db.utils.time.sleep", sleep)
    monkeypatch.setattr("omnigent.db.utils.random.uniform", lambda lower, upper: 1.0)
    monkeypatch.setenv("OMNIGENT_DB_CONNECT_RETRY_MAX_SECONDS", "1")
    monkeypatch.setenv("OMNIGENT_DB_CONNECT_RETRY_INITIAL_DELAY_SECONDS", "0.1")
    monkeypatch.setenv("OMNIGENT_DB_CONNECT_RETRY_MAX_DELAY_SECONDS", "0.2")

    def connect(*args: object, **kwargs: object) -> str:
        nonlocal attempts
        del args, kwargs
        attempts += 1
        if attempts <= 2:
            raise _FakeDbapiError(
                'connection failed: connection to server at "db" failed: Connection refused'
            )
        return "connected"

    listeners: list[Any] = []
    monkeypatch.setattr(
        "omnigent.db.utils.event.listen",
        lambda _engine, _identifier, listener: listeners.append(listener),
    )
    listener = _install_connection_retry(MagicMock())
    dbapi = SimpleNamespace(Error=_FakeDbapiError, connect=connect)

    assert listener(SimpleNamespace(dbapi=dbapi), None, [], {}) == "connected"
    assert listeners == [listener]
    assert attempts == 3
    assert sleeps == [0.1, 0.2]


@pytest.mark.parametrize(
    ("message", "sqlstate"),
    [
        ("connection reset by peer", None),
        ("could not connect now", "57P03"),
        ("server closed the connection unexpectedly", None),
        ("cannot execute INSERT in a read-only transaction", "25006"),
    ],
)
def test_known_promotion_connection_failures_are_transient(
    message: str,
    sqlstate: str | None,
) -> None:
    error = _FakeDbapiError(message, sqlstate=sqlstate)
    assert _is_transient_connection_error(error, dbapi_error_type=_FakeDbapiError)


@pytest.mark.parametrize(
    ("message", "sqlstate"),
    [
        ("password authentication failed for user", "28P01"),
        ('database "missing" does not exist', "3D000"),
        ("permission denied for database", "42501"),
        ("syntax error at or near SELECT", "42601"),
        ("duplicate key value violates unique constraint", "23505"),
    ],
)
def test_permanent_connection_errors_are_not_retried(
    message: str,
    sqlstate: str,
) -> None:
    """Authentication, catalog, privilege, syntax, and constraint errors fail fast."""
    error = _FakeDbapiError(message, sqlstate=sqlstate)
    attempts = 0

    def connect(*args: object, **kwargs: object) -> str:
        nonlocal attempts
        del args, kwargs
        attempts += 1
        raise error

    with pytest.raises(_FakeDbapiError) as exc_info:
        _connect_dbapi_with_retry(
            connect,
            [],
            {},
            dbapi_error_type=_FakeDbapiError,
            max_seconds=30.0,
            initial_delay_seconds=0.1,
            max_delay_seconds=1.0,
            jitter=0.2,
        )

    assert exc_info.value is error
    assert attempts == 1


def test_transient_connection_retry_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A continuously refused endpoint gets a clear bounded failure."""
    clock = 0.0
    attempts = 0

    def monotonic() -> float:
        return clock

    def sleep(seconds: float) -> None:
        nonlocal clock
        clock += seconds

    monkeypatch.setattr("omnigent.db.utils.time.monotonic", monotonic)
    monkeypatch.setattr("omnigent.db.utils.time.sleep", sleep)
    monkeypatch.setattr("omnigent.db.utils.random.uniform", lambda lower, upper: 1.0)

    def connect(*args: object, **kwargs: object) -> str:
        nonlocal attempts
        del args, kwargs
        attempts += 1
        raise _FakeDbapiError("connection refused")

    with pytest.raises(
        ConnectionError,
        match=re.escape("remained unavailable for 0.3s after 3 attempts"),
    ):
        _connect_dbapi_with_retry(
            connect,
            [],
            {},
            dbapi_error_type=_FakeDbapiError,
            max_seconds=0.3,
            initial_delay_seconds=0.1,
            max_delay_seconds=0.2,
            jitter=0.0,
        )

    assert attempts == 3
    assert clock == 0.3


def test_sqlite_pool_settings_are_not_read_or_forwarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}
    mock_engine = MagicMock()

    def _capturing_create_engine(uri: str, **kwargs: Any) -> MagicMock:
        captured_kwargs.update(kwargs)
        return mock_engine

    monkeypatch.setattr("omnigent.db.utils.create_engine", _capturing_create_engine)
    monkeypatch.setattr(
        "omnigent.db.utils.event.listens_for",
        lambda *args, **kwargs: lambda function: function,
    )
    monkeypatch.setenv("OMNIGENT_DB_POOL_SIZE", "7")
    monkeypatch.setenv("OMNIGENT_DB_MAX_OVERFLOW", "3")
    monkeypatch.setenv("OMNIGENT_DB_POOL_TIMEOUT_SECONDS", "2.5")

    _create_engine("sqlite:///test.db")

    assert "pool_size" not in captured_kwargs
    assert "max_overflow" not in captured_kwargs
    assert "pool_timeout" not in captured_kwargs


def test_engine_configuration_log_is_secret_safe(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_engine = MagicMock()
    mock_engine.dialect.name = "postgresql"
    monkeypatch.setattr(
        "omnigent.db.utils.create_engine",
        lambda uri, **kwargs: mock_engine,
    )
    monkeypatch.setattr("omnigent.db.utils.event.listen", lambda *args, **kwargs: None)
    monkeypatch.setattr("omnigent.db.utils._run_migrations", lambda engine, db_uri: None)

    with caplog.at_level("INFO", logger="omnigent.db.utils"):
        get_or_create_engine("postgresql://user:secret@localhost/testdb")

    records = [
        record for record in caplog.records if "database engine configured" in record.message
    ]
    assert len(records) == 1
    assert "dialect=postgresql" in records[0].message
    assert "pool_size=32" in records[0].message
    assert "max_overflow=32" in records[0].message
    assert "pool_timeout=10" in records[0].message
    assert "pool_recycle=1800" in records[0].message
    assert "@" not in records[0].message
    assert "secret" not in records[0].message


def test_sqlite_engine_skips_server_pool_settings_and_enables_wal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    SQLite engines must NOT receive server-DB pool settings
    (``pool_pre_ping`` / ``pool_recycle``) — those are meaningful
    only for multi-connection server databases. They must, however,
    enable WAL journal mode and a 20s ``busy_timeout`` on every
    connection so multi-process workloads (REPL + Omnigent server +
    runner subprocess + DBOS scheduler all hitting the same
    ``chat.db``) don't surface as ``disk I/O error`` /
    ``database is locked`` under default ``journal_mode=DELETE``.

    Uses a real SQLite engine on a tempfile (rather than a
    ``MagicMock``) because the connect-listener that applies the
    PRAGMAs cannot be attached to a mock target — and a real
    connection is the only way to verify the PRAGMAs actually
    took effect on a fresh DBAPI connection.
    """
    monkeypatch.setattr(
        "omnigent.db.utils._run_migrations",
        lambda engine, db_uri: None,
    )

    db_path = tmp_path / "test.db"
    engine = get_or_create_engine(f"sqlite:///{db_path}")

    # Server-DB pool settings are not relevant to a single-file
    # SQLite engine. Failure here means SQLite engines started
    # carrying options meant for postgres/mysql.
    assert engine.url.get_backend_name() == "sqlite"

    with engine.connect() as conn:
        # WAL is the entire point of this fix: it allows readers
        # and a single writer to coexist, where DELETE serializes
        # everything and produces ``database is locked`` /
        # ``disk I/O error`` under contention.
        assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
        # 20s lets brief contention windows (DBOS write-bursts on
        # spawn, conversation-append) wait rather than fail.
        assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 20000
        # foreign_keys on so cascades + ondelete=CASCADE actually
        # fire (mirrors :func:`make_managed_session_maker`'s
        # per-session PRAGMA).
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        # synchronous=NORMAL is the WAL-recommended mode — durable
        # at commit, much faster than FULL.
        assert conn.exec_driver_sql("PRAGMA synchronous").scalar() == 1


# ── Lakebase token-aware engine ─────────────────────────


@pytest.fixture(autouse=True)
def _clear_lakebase_override() -> Any:
    """Ensure the process-wide token provider override never leaks across
    tests (it is module-global state). Clears before and after each test."""
    from omnigent.db.utils import set_lakebase_token_provider as _set

    _set(None)
    yield
    _set(None)


@pytest.mark.databricks
def test_static_postgres_uri_path_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    (a) Backward compatibility: with no Lakebase config, a Postgres engine is
    created with the standard 30-minute recycle window and no token-refresh
    listener. The always-on connection retry listener is separate from the
    opt-in token path.
    """
    from omnigent.db import utils

    # No override installed (autouse fixture) and no env var → no token path.
    monkeypatch.delenv("OMNIGENT_LAKEBASE_INSTANCE", raising=False)
    assert _resolve_lakebase_token_provider() is None

    engine = utils._create_engine("postgresql+psycopg://user:pass@host:5432/db")
    try:
        # Standard (non-Lakebase) recycle window, unchanged from before.
        assert engine.pool._recycle == _SERVER_POOL_RECYCLE_SECONDS == 1800

        # Exactly one listener is expected: connection retry. The token-refresh
        # path is opt-in and must not be attached to a static-password engine.
        registered = list(engine.dialect.dispatch.do_connect)
        assert len(registered) == 1, (
            "static-password Postgres engine must carry only the connection "
            f"retry listener, found: {registered!r}"
        )

        # Cross-check with the real token install helper: it adds a second
        # listener only when explicitly requested.
        from sqlalchemy import event

        installed = _install_lakebase_token_refresh(engine, lambda: "tok")
        assert event.contains(engine, "do_connect", installed)
        assert len(list(engine.dialect.dispatch.do_connect)) == 2
    finally:
        engine.dispose()


def test_resolve_token_provider_env_and_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    The provider resolves from ``OMNIGENT_LAKEBASE_INSTANCE`` when set, and an
    explicit override installed via :func:`set_lakebase_token_provider` takes
    precedence over the env var.
    """
    # Env var unset → no provider.
    monkeypatch.delenv("OMNIGENT_LAKEBASE_INSTANCE", raising=False)
    assert _resolve_lakebase_token_provider() is None

    # Env var set → a provider resolves (the SDK-backed lambda).
    monkeypatch.setenv("OMNIGENT_LAKEBASE_INSTANCE", "omnigent-db")
    assert callable(_resolve_lakebase_token_provider())

    # Explicit override wins over the env var.
    sentinel: LakebaseSentinel = LakebaseSentinel()
    set_lakebase_token_provider(sentinel)
    assert _resolve_lakebase_token_provider() is sentinel


class LakebaseSentinel:
    """A trivial provider used to assert override identity/precedence."""

    def __call__(self) -> str:
        return "sentinel-token"


@pytest.mark.databricks
def test_token_callback_invoked_per_connection() -> None:
    """
    (b) The ``do_connect`` listener calls the token provider once per new
    connection and overwrites the password connection parameter with the fresh
    token. ``do_connect`` fires once per *new* DBAPI connection, so calling the
    registered listener N times models N new connections — each must re-mint.
    """
    calls: list[int] = []

    def _provider() -> str:
        calls.append(1)
        return f"token-{len(calls)}"

    engine = create_engine("postgresql+psycopg://user@host:5432/db")
    try:
        listener = _install_lakebase_token_refresh(engine, _provider)

        # The listener is actually wired onto the engine's do_connect event.
        from sqlalchemy import event

        assert event.contains(engine, "do_connect", listener)

        # Simulate two new connections: each re-mints a fresh token.
        first: dict[str, object] = {}
        second: dict[str, object] = {}
        listener(None, None, [], first)
        listener(None, None, [], second)

        assert len(calls) == 2, "token must be re-minted per new connection"
        assert first["password"] == "token-1"
        assert second["password"] == "token-2"
    finally:
        engine.dispose()


@pytest.mark.databricks
def test_create_engine_wires_token_refresh_and_short_recycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    (b)+(c) With a token provider active, ``_create_engine`` lowers
    ``pool_recycle`` to the Lakebase window and installs the token-refresh
    listener (verified by spying on the install helper to confirm it receives
    the resolved provider).
    """
    from omnigent.db import utils

    def _override() -> str:
        return "live-token"

    set_lakebase_token_provider(_override)

    installed: dict[str, object] = {}
    real_install = utils._install_lakebase_token_refresh

    def _spy_install(engine: object, provider: object) -> object:
        installed["engine"] = engine
        installed["provider"] = provider
        return real_install(engine, provider)  # type: ignore[arg-type]

    monkeypatch.setattr(utils, "_install_lakebase_token_refresh", _spy_install)

    engine = utils._create_engine("postgresql+psycopg://user@host:5432/db")
    try:
        # Shorter recycle so connections (and their tokens) refresh ahead of
        # the ~1h OAuth expiry.
        assert engine.pool._recycle == _LAKEBASE_POOL_RECYCLE_SECONDS == 600
        # The refresh listener was installed with the resolved provider.
        assert installed["provider"] is _override
        assert installed["engine"] is engine
    finally:
        engine.dispose()


# ── _initialize_or_verify_schema ────────────────────────


def _make_db_at_revision(db_path: Path, revision: str) -> str:
    """
    Build a SQLite database whose Alembic version is *revision*.

    Used to manufacture the "out-of-date DB" scenario without having
    to keep around an old binary fixture file.

    :param db_path: Filesystem path the SQLite file should live at.
    :param revision: Alembic revision hash to upgrade to (e.g.
        ``"8a4f1e9c2b07"`` to land below head).
    :returns: The SQLAlchemy URI for the created database.
    """
    uri = f"sqlite:///{db_path}"
    engine = create_engine(uri)
    config = _build_alembic_config(uri)
    try:
        with engine.begin() as conn:
            config.attributes["connection"] = conn
            command.upgrade(config, revision)
    finally:
        engine.dispose()
    return uri


def test_initialize_or_verify_schema_initializes_fresh_db(
    tmp_path: Path,
) -> None:
    """
    A brand-new SQLite file (no ``alembic_version`` table) is
    initialized to head on first boot. This is the "fresh install"
    path — without it, every new install would error with the
    upgrade-required hint.
    """
    db_path = tmp_path / "fresh.db"
    uri = f"sqlite:///{db_path}"
    engine = create_engine(uri)
    try:
        # Sanity: no alembic_version yet.
        # A non-None reading here means the test setup is wrong —
        # the file should be empty before _initialize_or_verify_schema.
        assert _get_current_db_revision(engine) is None

        _initialize_or_verify_schema(engine, uri)

        # After initialization, the DB is at head. If this is None,
        # the fresh-DB branch didn't actually run migrations; if it's
        # some other revision, head detection is broken.
        head = _get_head_db_revision(uri)
        assert _get_current_db_revision(engine) == head
    finally:
        engine.dispose()


def test_initialize_or_verify_schema_no_op_when_at_head(
    tmp_path: Path,
) -> None:
    """
    A database already at head is a no-op — does not raise, does
    not re-run migrations. This is the steady-state hot path on
    every server boot.
    """
    db_path = tmp_path / "at_head.db"
    head = _get_head_db_revision(f"sqlite:///{db_path}")
    uri = _make_db_at_revision(db_path, head)

    engine = create_engine(uri)
    try:
        # Must not raise. If it does, the head-equality check is
        # wrong (e.g. comparing wrong type, off-by-one revision).
        _initialize_or_verify_schema(engine, uri)
        # Still at head after the call. If the revision changed,
        # something inside the no-op branch wrote to the DB.
        assert _get_current_db_revision(engine) == head
    finally:
        engine.dispose()


def test_initialize_or_verify_schema_auto_migrates_when_stale(
    tmp_path: Path,
) -> None:
    """
    A database behind head is automatically upgraded during startup.

    Regression guard for the original bug report — booting against
    an existing DB that was missing ``conversations.runner_id`` used
    to terminate with an upgrade hint. The server should now attempt
    the migration itself and only fail if Alembic cannot upgrade.
    """
    db_path = tmp_path / "stale.db"
    # 8a4f1e9c2b07 is the previous head, before c9d3a1f2e4b5 added
    # the runner_id column. If the migration chain changes such that
    # this revision ID no longer exists, this test will fail loudly
    # at _make_db_at_revision and needs updating to a current
    # below-head revision.
    stale_revision = "8a4f1e9c2b07"
    uri = _make_db_at_revision(db_path, stale_revision)
    head = _get_head_db_revision(uri)
    # Sanity: the stale revision must actually be behind head, else
    # the test is structurally incapable of failing.
    assert stale_revision != head, (
        f"Test fixture revision {stale_revision!r} is now at head; "
        f"pick an older revision so the stale-DB path is exercised."
    )

    engine = create_engine(uri)
    try:
        _initialize_or_verify_schema(engine, uri)
        assert _get_current_db_revision(engine) == head
    finally:
        engine.dispose()


def test_initialize_or_verify_schema_flag_off_refuses_stale_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disabling boot upgrades still refuses to serve an old schema."""
    db_path = tmp_path / "stale_no_boot_migration.db"
    stale_revision = "8a4f1e9c2b07"
    uri = _make_db_at_revision(db_path, stale_revision)
    head = _get_head_db_revision(uri)
    assert stale_revision != head
    monkeypatch.setenv("OMNIGENT_RUN_MIGRATIONS_ON_BOOT", "0")

    migration_calls: list[tuple[object, object]] = []

    def _unexpected_migration(*args: object, **kwargs: object) -> None:
        migration_calls.append((args, kwargs))

    monkeypatch.setattr("omnigent.db.utils._run_migrations", _unexpected_migration)
    engine = create_engine(uri)
    try:
        with pytest.raises(RuntimeError, match="OMNIGENT_RUN_MIGRATIONS_ON_BOOT=0"):
            _initialize_or_verify_schema(engine, uri)
    finally:
        engine.dispose()

    assert migration_calls == []


def test_migration_database_url_defaults_to_caller_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MIGRATION_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@decoy.example/db")

    assert _resolve_migration_database_url("sqlite:///caller.db") == "sqlite:///caller.db"


def test_initialize_or_verify_schema_reports_manual_retry_when_auto_migration_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    If automatic migration fails, startup still terminates, but with
    an actionable message that includes the stale revision, expected
    head, DB URL, and manual ``omnigent debug db-upgrade`` retry command.
    """
    db_path = tmp_path / "stale_failure.db"
    stale_revision = "8a4f1e9c2b07"
    uri = _make_db_at_revision(db_path, stale_revision)
    head = _get_head_db_revision(uri)
    assert stale_revision != head

    def _fail_migration(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr("omnigent.db.utils._run_migrations", _fail_migration)

    engine = create_engine(uri)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            _initialize_or_verify_schema(engine, uri)
    finally:
        engine.dispose()

    msg = str(exc_info.value)
    assert stale_revision in msg, (
        f"Error message must include the stale revision so the "
        f"operator can confirm the diagnosis. Got: {msg!r}"
    )
    assert head in msg, (
        f"Error message must include the expected head so the operator knows the gap. Got: {msg!r}"
    )
    assert "omnigent debug db-upgrade" in msg, (
        f"Error message must include the literal upgrade command "
        f"the operator can run manually. Got: {msg!r}"
    )
    assert uri in msg, (
        f"Error message must include the database URL so the "
        f"command is copy-pastable. Got: {msg!r}"
    )


# ── slash_command persistence path ────────────────────


def test_generate_item_id_supports_slash_command() -> None:
    """Append path raises ``ValueError`` here if the prefix is missing."""
    item_id = generate_item_id("slash_command")
    assert re.fullmatch(r"[0-9a-f]{32}", item_id)


def test_generate_item_id_supports_error_item() -> None:
    """``generate_item_id`` raises ``ValueError`` here if ``error`` is unknown."""
    item_id = generate_item_id("error")
    assert re.fullmatch(r"[0-9a-f]{32}", item_id)


def test_generate_item_id_supports_resource_event() -> None:
    """Regression: ``resource_event`` (terminal launch/close lifecycle) was
    registered in the read-path map (``ITEM_TYPE_TO_DATA_CLS``) but missing
    from ``_ITEM_TYPES``, so every such item failed ``generate_item_id``
    with 'unknown item type' and never persisted (relay-persist traceback flood
    on every terminal launch/close)."""
    item_id = generate_item_id("resource_event")
    assert re.fullmatch(r"[0-9a-f]{32}", item_id)


def test_item_type_id_and_data_registries_cover_the_same_types() -> None:
    """The write/id registry (``_ITEM_TYPE_PREFIX``) and the read/data registry
    (``ITEM_TYPE_TO_DATA_CLS``) must list the SAME item types.

    A type in only one is silently half-wired: in the read map but not the id
    map cannot be persisted (``generate_item_id`` raises); the reverse persists
    but cannot be parsed back. This guard turns the next such omission into a
    loud unit-test failure instead of a per-item production traceback — exactly
    how ``resource_event`` slipped through (added to the data map, forgotten in
    the id map)."""
    from omnigent.db.utils import _ITEM_TYPES
    from omnigent.entities.conversation import ITEM_TYPE_TO_DATA_CLS

    assert set(_ITEM_TYPES) == set(ITEM_TYPE_TO_DATA_CLS), (
        "item-type registries diverged — "
        f"only in id/write path: {set(_ITEM_TYPES) - set(ITEM_TYPE_TO_DATA_CLS)}; "
        f"only in data/read path: {set(ITEM_TYPE_TO_DATA_CLS) - set(_ITEM_TYPES)}"
    )


def test_builtin_agent_id_is_deterministic_and_name_specific() -> None:
    """Same name → same id (survives a store rebuild); different name → different id."""
    assert builtin_agent_id("nessie") == builtin_agent_id("nessie")
    assert builtin_agent_id("nessie") != builtin_agent_id("claude-native-ui")


def test_builtin_agent_id_matches_generated_id_shape_and_length() -> None:
    """Pins both to a bare 32-char hex id so a built-in id stays
    indistinguishable from a generated one and the two can't diverge in length."""
    built_in = builtin_agent_id("nessie")
    assert re.fullmatch(r"[0-9a-f]{32}", built_in)
    assert len(built_in) == len(generate_agent_id()) == 32


def test_extract_search_text_for_slash_command_with_output() -> None:
    """FTS covers name + args + stdout so historical Skills are searchable."""
    item = NewConversationItem(
        type="slash_command",
        response_id="resp_1",
        data=SlashCommandData(
            agent="claude-native-ui",
            name="oncall",
            arguments="file-bug",
            output="oncall: file-bug subcommand started",
        ),
    )
    text = extract_search_text(item)
    assert "oncall" in text
    assert "file-bug" in text
    assert "subcommand started" in text


def test_extract_search_text_for_slash_command_without_output() -> None:
    """Absent ``output`` + empty args index cleanly (no stray whitespace)."""
    item = NewConversationItem(
        type="slash_command",
        response_id="resp_1",
        data=SlashCommandData(
            agent="claude-native-ui",
            name="dev-productivity:simplify",
            arguments="",
        ),
    )
    assert extract_search_text(item) == "dev-productivity:simplify"


def test_extract_search_text_for_error_item() -> None:
    """FTS covers source, code, and message for durable error banners."""
    item = NewConversationItem(
        type="error",
        response_id="resp_1",
        data=ErrorData(
            source="execution",
            code="native_terminal_start_failed",
            message="Native Codex requires the 'codex' CLI on PATH.",
        ),
    )
    text = extract_search_text(item)
    assert "execution" in text
    assert "native_terminal_start_failed" in text
    assert "Codex" in text


def test_extract_search_text_for_resource_event_item() -> None:
    """Runner resource replay persists cleanly and indexes stable ids."""
    item = NewConversationItem(
        type="resource_event",
        response_id="8e32600337d08f59ad381caf96a90659",
        data=ResourceEventData(
            event_type="session.resource.created",
            resource_id="resource_codex_conv_1",
            resource_type="terminal",
            resource={"metadata": {"opaque": "not indexed"}},
        ),
    )

    text = extract_search_text(item)

    assert "session.resource.created" in text
    assert "resource_codex_conv_1" in text
    assert "terminal" in text
    assert "opaque" not in text
    assert "not indexed" not in text


@pytest.mark.parametrize(
    "value,expected",
    [
        # Single NUL embedded in otherwise-printable text — the exact
        # shape that aborts a Postgres INSERT.
        ("before\x00after", "beforeafter"),
        # Multiple/contiguous NULs (e.g. a chunk of a binary file).
        ("a\x00\x00\x00b", "ab"),
        # Leading/trailing NULs.
        ("\x00x\x00", "x"),
        # No NUL — must be returned byte-for-byte unchanged.
        ("clean text", "clean text"),
        # Empty string is a no-op.
        ("", ""),
        # A literal backslash-u escape (6 chars) is NOT a NUL byte and
        # must survive untouched — this is how json.dumps already
        # encodes NUL, so stripping must not disturb it.
        ("esc\\u0000seq", "esc\\u0000seq"),
    ],
)
def test_strip_nul_bytes(value: str, expected: str) -> None:
    """
    ``strip_nul_bytes`` removes raw NUL (0x00) bytes and nothing else.

    A failure here means either a NUL byte survived (the input would
    still abort a Postgres text-column INSERT) or non-NUL content was
    altered (lossy sanitization corrupting stored output).
    """
    assert strip_nul_bytes(value) == expected
    # The result must never contain a raw NUL, regardless of input.
    assert "\x00" not in strip_nul_bytes(value)


def test_extract_search_text_routing_decision() -> None:
    """routing_decision items must index (model + rationale) — an
    unregistered type raises in the store's append path, which silently
    dropped every verdict chip on persistence (the relay swallows it)."""
    item = NewConversationItem.model_validate(
        {
            "type": "routing_decision",
            "response_id": "resp_x",
            "data": {
                "model": "databricks-claude-opus-4-8",
                "applied": True,
                "rationale": "Deep design work.",
            },
        }
    )
    assert extract_search_text(item) == "databricks-claude-opus-4-8 Deep design work."


def test_build_search_snippet_short_text_returned_whole() -> None:
    """A match within a short line yields the whole (collapsed) line, no ellipsis."""
    assert (
        build_search_snippet("Hello what model are you using?", "what")
        == "Hello what model are you using?"
    )


def test_build_search_snippet_is_case_insensitive() -> None:
    """Matching mirrors the store's case-insensitive LIKE filter."""
    assert build_search_snippet("Deploy the SERVICE now", "service") is not None


def test_build_search_snippet_windows_long_text_with_ellipses() -> None:
    """A match buried in long text is windowed with … on both elided ends."""
    text = "a" * 200 + " deploy error " + "b" * 200
    snippet = build_search_snippet(text, "deploy error")
    assert snippet is not None
    assert "deploy error" in snippet
    assert snippet.startswith("…") and snippet.endswith("…")
    # Kept short enough for a single UI row.
    assert len(snippet) <= 160 + 2


def test_build_search_snippet_collapses_whitespace() -> None:
    """Multi-line / repeated whitespace collapses so the snippet is one clean line."""
    snippet = build_search_snippet("line one\n\n   line two matches here", "matches")
    assert snippet == "line one line two matches here"


def test_build_search_snippet_never_clamps_out_the_match() -> None:
    """A query term longer than max_len still appears in the snippet.

    The length cap must not truncate the window before the matched span
    ends — otherwise the UI would have nothing to highlight.
    """
    term = "x" * 300
    snippet = build_search_snippet(f"prefix {term} suffix", term)
    assert snippet is not None
    assert term in snippet


def test_build_search_snippet_no_match_returns_none() -> None:
    """No occurrence (or empty query) yields None so the caller shows no preview."""
    assert build_search_snippet("no match here", "xyz") is None
    assert build_search_snippet("anything", "") is None
