"""Database utilities — engine caching, session management, helpers.

``MIGRATION_DATABASE_URL`` selects the direct database endpoint used for
Alembic work and defaults to the caller's database URL. ``OMNIGENT_RUN_MIGRATIONS_ON_BOOT=0``
disables automatic upgrades while retaining the head-revision check.
``OMNIGENT_MIGRATION_LOCK_TIMEOUT_SECONDS`` bounds lock waits at 10 minutes by
default, and ``OMNIGENT_DB_DISABLE_PREPARED_STATEMENTS=1`` disables psycopg3
named preparation for transaction-pooling deployments.
New PostgreSQL connections retry transient endpoint failures for up to 30
seconds by default; ``OMNIGENT_DB_CONNECT_RETRY_MAX_SECONDS``,
``OMNIGENT_DB_CONNECT_RETRY_INITIAL_DELAY_SECONDS``,
``OMNIGENT_DB_CONNECT_RETRY_MAX_DELAY_SECONDS``, and
``OMNIGENT_DB_CONNECT_RETRY_JITTER`` configure that bounded wait.
"""

from __future__ import annotations

import hashlib
import logging
import math
import os
import random
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from sqlalchemy import Connection, Engine, create_engine, event, inspect, text

if TYPE_CHECKING:
    from alembic.config import Config
from sqlalchemy.orm import Session, sessionmaker

from omnigent.db.query_context import query_name_scope
from omnigent.entities import NewConversationItem

_logger = logging.getLogger(__name__)

# A callable that returns a context manager yielding a Session.
ManagedSessionMaker = Callable[[], AbstractContextManager[Session]]

# A callable that requires a semantic query-name suffix for each transaction.
NamedManagedSessionMaker = Callable[[str], AbstractContextManager[Session]]

# A zero-argument callable returning a fresh database password (e.g. a
# short-lived Lakebase OAuth token). Invoked once per *new* DBAPI connection.
LakebaseTokenProvider = Callable[[], str]


# ── Lakebase token-aware connections ───────────────────
#
# Databricks Lakebase (managed Postgres) authenticates with a short-lived
# OAuth token (~1h TTL, rotated) used as the Postgres *password* — there is no
# static password to bake into the URL. To stay connected we must mint a fresh
# token for every new physical connection instead of pinning one at engine
# construction. This is OPT-IN: it activates only when a token provider is
# resolvable (``OMNIGENT_LAKEBASE_INSTANCE`` is set, or a provider was injected
# via :func:`set_lakebase_token_provider`). When it is not active, engine
# creation is byte-for-byte the legacy static-URI path (SQLite or
# static-password Postgres) — see :func:`_create_engine`.

# Env var naming the Lakebase database *instance* whose OAuth token should be
# minted per connection. Its presence is what flips a Postgres engine into
# token-refresh mode.
_LAKEBASE_INSTANCE_ENV = "OMNIGENT_LAKEBASE_INSTANCE"

# Recycle (close + reopen) pooled connections older than this many seconds.
# Static deployments use 30 min (stale-connection hygiene). Lakebase lowers it
# to 10 min so a connection is rebuilt — and its OAuth token re-minted via the
# ``do_connect`` hook — comfortably before the ~1h token lifetime lapses, even
# for connections that sit idle in the pool across a rotation.
_SERVER_POOL_RECYCLE_SECONDS = 1800
_LAKEBASE_POOL_RECYCLE_SECONDS = 600

_POOL_SIZE_ENV = "OMNIGENT_DB_POOL_SIZE"
_MAX_OVERFLOW_ENV = "OMNIGENT_DB_MAX_OVERFLOW"
_POOL_TIMEOUT_ENV = "OMNIGENT_DB_POOL_TIMEOUT_SECONDS"
_DB_CONNECT_RETRY_MAX_SECONDS_ENV = "OMNIGENT_DB_CONNECT_RETRY_MAX_SECONDS"
_DB_CONNECT_RETRY_INITIAL_DELAY_SECONDS_ENV = "OMNIGENT_DB_CONNECT_RETRY_INITIAL_DELAY_SECONDS"
_DB_CONNECT_RETRY_MAX_DELAY_SECONDS_ENV = "OMNIGENT_DB_CONNECT_RETRY_MAX_DELAY_SECONDS"
_DB_CONNECT_RETRY_JITTER_ENV = "OMNIGENT_DB_CONNECT_RETRY_JITTER"
_MIGRATION_DATABASE_URL_ENV = "MIGRATION_DATABASE_URL"
_RUN_MIGRATIONS_ON_BOOT_ENV = "OMNIGENT_RUN_MIGRATIONS_ON_BOOT"
_MIGRATION_LOCK_TIMEOUT_ENV = "OMNIGENT_MIGRATION_LOCK_TIMEOUT_SECONDS"
_DISABLE_PREPARED_STATEMENTS_ENV = "OMNIGENT_DB_DISABLE_PREPARED_STATEMENTS"
_DEFAULT_POOL_SIZE = 32
_DEFAULT_MAX_OVERFLOW = 32
_DEFAULT_POOL_TIMEOUT_SECONDS = 10
_DEFAULT_DB_CONNECT_RETRY_MAX_SECONDS = 30.0
_DEFAULT_DB_CONNECT_RETRY_INITIAL_DELAY_SECONDS = 0.25
_DEFAULT_DB_CONNECT_RETRY_MAX_DELAY_SECONDS = 5.0
_DEFAULT_DB_CONNECT_RETRY_JITTER = 0.2
_DEFAULT_MIGRATION_LOCK_TIMEOUT_SECONDS = 600.0

_TRANSIENT_CONNECTION_SQLSTATES = frozenset(
    {
        "08001",  # SQL-client unable to establish a connection
        "08004",  # server rejected the connection
        "08006",  # connection failure
        "25006",  # read-only during promotion/recovery
        "57P01",  # admin shutdown
        "57P02",  # crash shutdown
        "57P03",  # cannot connect now / starting up
    }
)
_PERMANENT_CONNECTION_SQLSTATE_PREFIXES = ("23", "28")  # constraint/auth errors
_PERMANENT_CONNECTION_SQLSTATES = frozenset(
    {
        "3D000",  # invalid catalog name / database does not exist
        "42501",  # insufficient privilege
        "42601",  # syntax error
    }
)
_TRANSIENT_CONNECTION_MESSAGES = (
    "connection refused",
    "connection reset",
    "could not connect now",
    "cannot connect now",
    "server closed the connection unexpectedly",
    "database system is starting up",
    "database system is shutting down",
    "not yet accepting connections",
    "database system is in recovery",
    "during recovery",
    "read-only transaction",
)
_PERMANENT_CONNECTION_MESSAGES = (
    "authentication failed",
    "password authentication failed",
    "database does not exist",
    "permission denied",
    "syntax error",
    "constraint violation",
    "violates ",
)


class _DatabaseConnectionRetryError(ConnectionError):
    """Raised when transient connection failures outlive the retry window."""


class _DbapiWithConnect(Protocol):
    """Small DBAPI typing surface used by SQLAlchemy's connect hook."""

    connect: Callable[..., object]
    Error: type[BaseException]


class _DialectWithDbapi(Protocol):
    """Small typing surface exposed by SQLAlchemy's ``do_connect`` hook."""

    dbapi: _DbapiWithConnect


# Process-wide override, primarily for tests and for callers that want to plug
# in their own token source (e.g. a non-default Databricks auth flow) without
# the env-var path. ``None`` means "not overridden".
_lakebase_token_provider_override: LakebaseTokenProvider | None = None


def _pool_setting(
    env_name: str,
    default: int | float,
    *,
    integer: bool,
    minimum: float,
    inclusive: bool = True,
) -> int | float:
    """Resolve and validate one non-SQLite pool setting from the environment."""
    raw_value = os.environ.get(env_name)
    if raw_value is None:
        return default

    try:
        value: int | float = int(raw_value) if integer else float(raw_value)
    except ValueError as exc:
        kind = "an integer" if integer else "a number"
        raise RuntimeError(f"{env_name} must be {kind}, got {raw_value!r}") from exc

    if not math.isfinite(value) or value < minimum or (not inclusive and value == minimum):
        comparison = f">= {int(minimum)}" if inclusive else f"> {minimum:g}"
        raise RuntimeError(f"{env_name} must be {comparison}, got {raw_value!r}")
    return value


def _env_flag(env_name: str, default: bool) -> bool:
    """Resolve a boolean environment setting with an explicit default."""
    raw_value = os.environ.get(env_name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() not in {"0", "false", "no", "off"}


def _connection_retry_jitter() -> float:
    """Resolve the fractional jitter applied to connection retry delays."""
    raw_value = os.environ.get(_DB_CONNECT_RETRY_JITTER_ENV)
    if raw_value is None:
        return _DEFAULT_DB_CONNECT_RETRY_JITTER
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            f"{_DB_CONNECT_RETRY_JITTER_ENV} must be a number between 0 and 1, got {raw_value!r}"
        ) from exc
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise RuntimeError(
            f"{_DB_CONNECT_RETRY_JITTER_ENV} must be between 0 and 1, got {raw_value!r}"
        )
    return value


def _connection_retry_settings() -> tuple[float, float, float, float]:
    """Resolve and validate the bounded new-connection retry settings."""
    return (
        float(
            _pool_setting(
                _DB_CONNECT_RETRY_MAX_SECONDS_ENV,
                _DEFAULT_DB_CONNECT_RETRY_MAX_SECONDS,
                integer=False,
                minimum=0.0,
            )
        ),
        float(
            _pool_setting(
                _DB_CONNECT_RETRY_INITIAL_DELAY_SECONDS_ENV,
                _DEFAULT_DB_CONNECT_RETRY_INITIAL_DELAY_SECONDS,
                integer=False,
                minimum=0.0,
                inclusive=False,
            )
        ),
        float(
            _pool_setting(
                _DB_CONNECT_RETRY_MAX_DELAY_SECONDS_ENV,
                _DEFAULT_DB_CONNECT_RETRY_MAX_DELAY_SECONDS,
                integer=False,
                minimum=0.0,
                inclusive=False,
            )
        ),
        _connection_retry_jitter(),
    )


def _run_migrations_on_boot() -> bool:
    """Return whether boot may upgrade a database that is behind head."""
    return _env_flag(_RUN_MIGRATIONS_ON_BOOT_ENV, True)


def set_lakebase_token_provider(provider: LakebaseTokenProvider | None) -> None:
    """
    Install (or clear) a process-wide Lakebase token provider.

    When set, every Postgres engine subsequently created by
    :func:`get_or_create_engine` mints its connection password by calling
    *provider* once per new DBAPI connection, and uses the shorter
    Lakebase pool-recycle window. Pass ``None`` to clear the override and
    fall back to the ``OMNIGENT_LAKEBASE_INSTANCE`` env-var path.

    This is the documented seam for swapping the token source: the default
    env-var path mints tokens via the Databricks SDK
    (:func:`_databricks_lakebase_token_provider`), but a deployment with a
    bespoke credential flow can inject its own zero-arg ``() -> str`` here.

    :param provider: A zero-arg callable returning a fresh password string,
        or ``None`` to clear a previously installed override.
    """
    global _lakebase_token_provider_override
    _lakebase_token_provider_override = provider


def _databricks_lakebase_token_provider(instance_name: str) -> str:
    """
    Mint a fresh short-lived Lakebase OAuth token via the Databricks SDK.

    Uses ambient Databricks authentication (the workspace's app identity /
    service principal when running inside a Databricks App, or a configured
    profile / env credentials elsewhere). The returned token is used as the
    Postgres password for a single connection; it expires in roughly an hour,
    which is why it is re-minted per connection rather than cached.

    :param instance_name: The Lakebase database instance name, e.g.
        ``"omnigent-db"`` (the value of ``OMNIGENT_LAKEBASE_INSTANCE``).
    :returns: A short-lived OAuth token string to use as the DB password.
    :raises ImportError: If the ``databricks-sdk`` (the ``databricks`` extra)
        is not installed.
    """
    from databricks.sdk import WorkspaceClient

    workspace_client = WorkspaceClient()
    credential = workspace_client.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    if not credential.token:
        raise RuntimeError(
            f"Databricks returned no Lakebase credential token for instance "
            f"{instance_name!r}. Verify the instance name and that this identity "
            f"has access to it."
        )
    return credential.token


def _resolve_lakebase_token_provider() -> LakebaseTokenProvider | None:
    """
    Return the active Lakebase token provider, or ``None`` if not configured.

    Resolution order:

    1. A provider installed via :func:`set_lakebase_token_provider` (override).
    2. The Databricks SDK provider, bound to the instance named by
       ``OMNIGENT_LAKEBASE_INSTANCE``.
    3. ``None`` — no token path; engines use the static-URI behavior.

    :returns: A zero-arg ``() -> str`` token provider, or ``None``.
    """
    if _lakebase_token_provider_override is not None:
        return _lakebase_token_provider_override
    instance_name = os.environ.get(_LAKEBASE_INSTANCE_ENV)
    if instance_name:
        return lambda: _databricks_lakebase_token_provider(instance_name)
    return None


def _install_lakebase_token_refresh(
    engine: Engine,
    token_provider: LakebaseTokenProvider,
) -> Callable[[object, object, list[object], dict[str, object]], None]:
    """
    Wire *engine* to refresh its connection password on every new connection.

    Registers a SQLAlchemy ``do_connect`` listener that overwrites the
    ``password`` connection parameter with a freshly minted token immediately
    before each physical DBAPI connection is opened. ``do_connect`` fires once
    per *new* connection (not per pool checkout), so pooled connections reuse
    their token until recycled — which is why :func:`_create_engine` pairs this
    with the shorter ``_LAKEBASE_POOL_RECYCLE_SECONDS`` window.

    :param engine: The SQLAlchemy engine to attach the listener to.
    :param token_provider: Zero-arg callable returning a fresh password.
    :returns: The registered listener (returned so callers/tests can assert it
        is wired and exercise it directly).
    """

    def _provide_fresh_token(
        _dialect: object,
        _conn_rec: object,
        _cargs: list[object],
        cparams: dict[str, object],
    ) -> None:
        # do_connect lets us mutate the connection params psycopg receives.
        # Overwriting ``password`` here means the token is read fresh for each
        # new connection — never baked into the cached engine's URL.
        cparams["password"] = token_provider()

    event.listen(engine, "do_connect", _provide_fresh_token)
    return _provide_fresh_token


# ── URL normalization ──────────────────────────────────


def normalize_database_url(url: str) -> str:
    """Rewrite a PaaS ``postgres://`` / ``postgresql://`` URL to the
    ``postgresql+psycopg://`` form SQLAlchemy needs; other URLs pass through.

    :param url: A SQLAlchemy-compatible database URL.
    :returns: The URL with the psycopg3 dialect specifier applied when needed.
    """
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix) :]
    return url


def _resolve_migration_database_url(database_url: str) -> str:
    """Resolve and normalize the URL reserved for schema migrations."""
    configured_url = os.environ.get(_MIGRATION_DATABASE_URL_ENV)
    if configured_url is None:
        return normalize_database_url(database_url)
    return normalize_database_url(configured_url)


# ── Engine caching ─────────────────────────────────────

_engine_cache: dict[str, Engine] = {}
_engine_lock = threading.Lock()


def _exception_sqlstate(exc: BaseException) -> str | None:
    """Return a DBAPI exception's SQLSTATE, including psycopg diagnostics."""
    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(sqlstate, str):
        return sqlstate.upper()
    diagnostic = getattr(exc, "diag", None)
    diagnostic_sqlstate = getattr(diagnostic, "sqlstate", None)
    if isinstance(diagnostic_sqlstate, str):
        return diagnostic_sqlstate.upper()
    return None


def _is_transient_connection_error(
    exc: BaseException,
    *,
    dbapi_error_type: type[BaseException],
) -> bool:
    """Classify only known transient DBAPI connection failures as retryable.

    This runs around ``dbapi.connect`` only, so message matching cannot turn a
    failed SQL statement into a retried operation. Unknown SQLSTATEs fail fast.
    """
    if not isinstance(exc, dbapi_error_type):
        return False

    sqlstate = _exception_sqlstate(exc)
    if sqlstate is not None:
        if sqlstate in _PERMANENT_CONNECTION_SQLSTATES or sqlstate.startswith(
            _PERMANENT_CONNECTION_SQLSTATE_PREFIXES
        ):
            return False
        return sqlstate in _TRANSIENT_CONNECTION_SQLSTATES

    message = str(exc).lower()
    if any(marker in message for marker in _PERMANENT_CONNECTION_MESSAGES):
        return False
    return any(marker in message for marker in _TRANSIENT_CONNECTION_MESSAGES)


def _connect_dbapi_with_retry(
    connect: Callable[..., object],
    cargs: Sequence[object],
    cparams: dict[str, object],
    *,
    dbapi_error_type: type[BaseException],
    max_seconds: float,
    initial_delay_seconds: float,
    max_delay_seconds: float,
    jitter: float,
) -> object:
    """Open one physical connection, retrying only classified outages."""
    started = time.monotonic()
    deadline = started + max_seconds
    attempt = 0
    delay_seconds = initial_delay_seconds

    while True:
        attempt += 1
        try:
            return connect(*cargs, **cparams)
        except Exception as exc:
            if not _is_transient_connection_error(exc, dbapi_error_type=dbapi_error_type):
                raise

            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                elapsed_seconds = max(0.0, time.monotonic() - started)
                message = (
                    "database connection remained unavailable for "
                    f"{elapsed_seconds:.1f}s after {attempt} attempts; "
                    f"last transient error: {exc}"
                )
                _logger.warning("%s", message)
                raise _DatabaseConnectionRetryError(message) from exc

            jittered_delay = delay_seconds * random.uniform(1 - jitter, 1 + jitter)
            sleep_seconds = min(jittered_delay, max_delay_seconds, remaining_seconds)
            _logger.debug(
                "Transient database connection failure on attempt %d; retrying in %.3fs",
                attempt,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
            delay_seconds = min(delay_seconds * 2, max_delay_seconds)


def _install_connection_retry(
    engine: Engine,
) -> Callable[[object, object, list[object], dict[str, object]], object]:
    """Retry transient failures from SQLAlchemy's new-connection hook.

    ``do_connect`` fires before a new DBAPI connection is opened, not when a
    pooled connection is checked out. It therefore complements ``pool_pre_ping``
    without retrying statements or migration work.
    """
    (
        max_seconds,
        initial_delay_seconds,
        max_delay_seconds,
        jitter,
    ) = _connection_retry_settings()

    def _connect_with_retry(
        dialect: object,
        _conn_rec: object,
        cargs: list[object],
        cparams: dict[str, object],
    ) -> object:
        dbapi = cast(_DialectWithDbapi, dialect).dbapi
        return _connect_dbapi_with_retry(
            dbapi.connect,
            cargs,
            cparams,
            dbapi_error_type=dbapi.Error,
            max_seconds=max_seconds,
            initial_delay_seconds=initial_delay_seconds,
            max_delay_seconds=max_delay_seconds,
            jitter=jitter,
        )

    event.listen(engine, "do_connect", _connect_with_retry)
    return _connect_with_retry


def _create_engine(db_uri: str) -> Engine:
    """
    Create a SQLAlchemy engine with connection pool configuration.

    SQLite engines enable WAL journal mode and a 20s
    ``busy_timeout`` on every connection (not just sessions
    created via :func:`make_managed_session_maker`). Without WAL,
    multi-process workloads — REPL + Omnigent server + runner subprocess
    all hitting the same ``chat.db`` — surface as spurious
    ``disk I/O error`` and ``database is locked`` failures because
    the default ``journal_mode=DELETE`` only permits one writer at
    a time and synchronous-write contention propagates immediately.
    WAL also lets readers proceed concurrently with a writer.

    Non-SQLite databases use connection pooling with ``pool_pre_ping`` to
    verify pooled connections before use and a bounded retry around new
    physical connections. When a Lakebase token provider is active (see
    :func:`_resolve_lakebase_token_provider`), the engine additionally
    re-mints its OAuth token per new connection and uses a shorter
    ``pool_recycle`` window; otherwise the static URI (and its baked-in
    password, if any) is used unchanged.

    :param db_uri: SQLAlchemy database connection string, e.g.
        ``"sqlite:///mydb.db"`` or
        ``"postgresql://<user>:<password>@host/dbname"``.
    :returns: A configured :class:`~sqlalchemy.engine.Engine`.
    """
    is_sqlite = db_uri.startswith("sqlite")
    if is_sqlite:
        # ``check_same_thread=False`` lets SQLAlchemy's pool hand a
        # connection to whichever worker thread asks for it (FastAPI,
        # asyncio.to_thread). The library still serializes access via
        # the pool, so this isn't a footgun — it just removes the
        # legacy single-thread restriction.
        engine = create_engine(
            db_uri,
            connect_args={"check_same_thread": False, "timeout": 20.0},
        )

        # Apply WAL + busy_timeout on every fresh DBAPI connection
        # so AsyncSession instances and any other consumer all
        # benefit — the per-session PRAGMA in
        # :func:`make_managed_session_maker` only fires for code
        # paths that go through that helper.
        import sqlite3

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn: sqlite3.Connection, _conn_record: object) -> None:
            cur = dbapi_conn.cursor()
            try:
                cur.execute("PRAGMA journal_mode=WAL")
                cur.execute("PRAGMA busy_timeout=20000")  # 20s
                cur.execute("PRAGMA synchronous=NORMAL")  # WAL-safe + fast
                cur.execute("PRAGMA foreign_keys=ON")
            finally:
                cur.close()

        return engine
    # Lakebase (managed Postgres) authenticates with a short-lived OAuth token
    # re-minted per connection; everything else uses the static URI as-is. The
    # token path is OPT-IN — ``_resolve_lakebase_token_provider`` returns
    # ``None`` unless ``OMNIGENT_LAKEBASE_INSTANCE`` is set or a provider was
    # injected — so a static-password Postgres URI is byte-for-byte unchanged.
    token_provider = _resolve_lakebase_token_provider()
    pool_recycle = (
        _LAKEBASE_POOL_RECYCLE_SECONDS if token_provider else _SERVER_POOL_RECYCLE_SECONDS
    )
    pool_size = _pool_setting(
        _POOL_SIZE_ENV,
        _DEFAULT_POOL_SIZE,
        integer=True,
        minimum=1.0,
    )
    max_overflow = _pool_setting(
        _MAX_OVERFLOW_ENV,
        _DEFAULT_MAX_OVERFLOW,
        integer=True,
        minimum=0.0,
    )
    pool_timeout = _pool_setting(
        _POOL_TIMEOUT_ENV,
        _DEFAULT_POOL_TIMEOUT_SECONDS,
        integer=False,
        minimum=0.0,
        inclusive=False,
    )
    connect_args: dict[str, object] = (
        {"connect_timeout": 3} if db_uri.startswith("postgresql") else {}
    )
    if db_uri.startswith("postgresql") and _env_flag(_DISABLE_PREPARED_STATEMENTS_ENV, False):
        connect_args["prepare_threshold"] = None
    engine = create_engine(
        db_uri,
        connect_args=connect_args,
        # Verify connections are alive before checking them out
        # from the pool. Prevents "server has gone away" errors
        # after idle periods; new connections use the retry hook below.
        pool_pre_ping=True,
        # Recycle connections older than this window. Prevents stale
        # connections when the database server restarts or closes idle
        # connections; in Lakebase token mode the shorter window also keeps
        # each connection's OAuth token refreshed ahead of its ~1h expiry.
        pool_recycle=pool_recycle,
        # Hold the thread ceiling in the pool; borrow overflow connections for
        # bursts so transient demand does not become retained idle capacity.
        pool_size=pool_size,
        max_overflow=max_overflow,
        # Bound the wait when the pool is exhausted instead of
        # blocking indefinitely; surfaces real saturation as an
        # error rather than a hang.
        pool_timeout=pool_timeout,
        # Return sessions with no open transaction or session-local state.
        pool_reset_on_return="rollback",
    )
    _logger.info(
        "database engine configured dialect=%s pool_size=%s max_overflow=%s "
        "pool_timeout=%s pool_recycle=%s",
        engine.dialect.name,
        pool_size,
        max_overflow,
        pool_timeout,
        pool_recycle,
    )
    if token_provider:
        _install_lakebase_token_refresh(engine, token_provider)
    _install_connection_retry(engine)
    return engine


def get_or_create_engine(db_uri: str) -> Engine:
    """
    Return a cached engine for the given URI, creating one if needed.

    On first creation, initializes or upgrades the database schema
    by running migrations to head. See
    :func:`_initialize_or_verify_schema`.

    :param db_uri: SQLAlchemy database connection string, e.g.
        ``"sqlite:///mydb.db"`` or
        ``"postgresql://<user>:<password>@host/dbname"``.
    :returns: A :class:`~sqlalchemy.engine.Engine` for the given URI.
    :raises RuntimeError: If automatic schema migration fails.
    """
    if db_uri not in _engine_cache:
        with _engine_lock:
            if db_uri not in _engine_cache:
                engine = _create_engine(db_uri)
                _initialize_or_verify_schema(engine, db_uri)
                from omnigent.runtime.telemetry import instrument_sqlalchemy_engine

                instrument_sqlalchemy_engine(engine)
                _engine_cache[db_uri] = engine
    return _engine_cache[db_uri]


def get_or_create_conversation_engine(conv_uri: str) -> Engine:
    """
    Return a cached engine for the Agent Platform DB URI.

    Unlike :func:`get_or_create_engine`, this does NOT run Alembic
    migrations — the AP DB is expected to be a fresh database that
    gets its tables created via ``ConversationBase.metadata.create_all()``.
    For the common case where AP DB == Omnigent DB, callers should
    use :func:`get_or_create_engine` directly and share the engine.

    :param conv_uri: SQLAlchemy database URI for the AP DB.
    :returns: A :class:`~sqlalchemy.engine.Engine` for the given URI.
    """
    if conv_uri not in _engine_cache:
        with _engine_lock:
            if conv_uri not in _engine_cache:
                engine = _create_engine(conv_uri)
                _ensure_conversation_tables(engine)
                from omnigent.runtime.telemetry import instrument_sqlalchemy_engine

                instrument_sqlalchemy_engine(engine)
                _engine_cache[conv_uri] = engine
    return _engine_cache[conv_uri]


def _ensure_conversation_tables(engine: Engine) -> None:
    """Create AP tables (conversations, conversation_items, conversation_labels) if absent."""
    from omnigent.db.db_models import ConversationBase

    with query_name_scope("omnigent.database.ensure_conversation_schema"):
        ConversationBase.metadata.create_all(bind=engine, checkfirst=True)
        ensure_fts_table(engine)


def _build_alembic_config(db_uri: str) -> Config:
    """
    Build an Alembic ``Config`` pointed at our migrations directory.

    Centralized so :func:`_run_migrations` and the
    :func:`omnigent debug db-upgrade` CLI command share the same
    config (URL, script location). The script_location in
    ``alembic.ini`` is relative — resolve it against the ini
    file's parent so the config works from any working directory.

    :param db_uri: SQLAlchemy database URL, e.g.
        ``"sqlite:///mydb.db"`` or ``"postgresql://..."``.
    :returns: A populated ``alembic.config.Config`` ready to hand
        to ``alembic.command.upgrade``.
    """
    from alembic.config import Config

    alembic_ini = Path(__file__).parent / "alembic.ini"
    config = Config(str(alembic_ini))
    config.set_main_option("sqlalchemy.url", db_uri)
    config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    return config


_MIGRATION_LOCK_KEY = 734_891_203


def _migration_lock_timeout_seconds() -> float:
    """Resolve the bounded wait for the PostgreSQL migration lock."""
    return float(
        _pool_setting(
            _MIGRATION_LOCK_TIMEOUT_ENV,
            _DEFAULT_MIGRATION_LOCK_TIMEOUT_SECONDS,
            integer=False,
            minimum=0.0,
            inclusive=False,
        )
    )


def _assert_migration_lock_backend(
    connection: Connection,
    expected_pid: int,
    *,
    phase: str,
) -> None:
    """Abort if a transaction pooler moved the session to another backend."""
    observed_pid = int(connection.execute(text("SELECT pg_backend_pid()")).scalar_one())
    if observed_pid != expected_pid:
        raise RuntimeError(
            "Migration advisory lock connection is not pinned: PostgreSQL backend PID "
            f"changed from {expected_pid} to {observed_pid} {phase}. "
            "Set MIGRATION_DATABASE_URL to a direct PostgreSQL endpoint, not a "
            "PgBouncer transaction-pooling endpoint."
        )


@contextmanager
def _migration_lock(engine: Engine) -> Iterator[None]:
    """Serialize schema changes on PostgreSQL using a dedicated connection.

    SQLite and MySQL have no equivalent lock requirement here and use a
    no-op context.

    :param engine: Engine for the database being migrated.
    """
    if engine.dialect.name != "postgresql":
        yield
        return

    timeout_seconds = _migration_lock_timeout_seconds()
    deadline = time.monotonic() + timeout_seconds
    warned = False
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as lock_connection:
        while True:
            row = (
                lock_connection.execute(
                    text(
                        "SELECT pg_backend_pid() AS backend_pid, "
                        "pg_try_advisory_lock(:lock_key) AS acquired"
                    ),
                    {"lock_key": _MIGRATION_LOCK_KEY},
                )
                .mappings()
                .one()
            )
            if row["acquired"]:
                backend_pid = int(row["backend_pid"])
                break

            if not warned:
                _logger.warning(
                    "Waiting for the migration advisory lock, held by another process "
                    "(Job or another server pod); this deploy will proceed once it releases."
                )
                warned = True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "Timed out after "
                    f"{timeout_seconds:g} seconds waiting for the migration advisory lock. "
                    "Check for a stuck migration holder before retrying."
                )
            time.sleep(min(0.1, remaining))

        _assert_migration_lock_backend(
            lock_connection,
            backend_pid,
            phase="before schema work",
        )
        migration_error: BaseException | None = None
        try:
            yield
        except BaseException as exc:
            migration_error = exc
            raise
        finally:
            try:
                _assert_migration_lock_backend(
                    lock_connection,
                    backend_pid,
                    phase="before unlock",
                )
                lock_connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": _MIGRATION_LOCK_KEY},
                )
            except BaseException:
                if migration_error is None:
                    raise
                _logger.warning(
                    "Could not verify or release the migration advisory lock after "
                    "the migration failed; preserving the original migration error.",
                    exc_info=True,
                )


def _run_migrations(engine: Engine, db_uri: str) -> None:
    """
    Bring the database schema up to head.

    Always invokes ``alembic.command.upgrade("head")`` regardless
    of whether application tables already exist. Alembic is
    idempotent — when the database is already at head this is a
    fast no-op (one ``SELECT`` on ``alembic_version``) — so the
    extra call is cheap, and it's the only way for column-level
    follow-up migrations (e.g. an ``ALTER TABLE ... ADD COLUMN``)
    to land on databases that were initialized at an earlier
    revision. The previous ``if expected_tables.issubset(...): return``
    short-circuit silently skipped those migrations, leaving
    existing DBs missing columns that the runtime expects.

    :param engine: The SQLAlchemy engine bound to the target
        database.
    :param db_uri: Database connection string forwarded to
        Alembic's ``sqlalchemy.url`` config option, e.g.
        ``"sqlite:///mydb.db"``.
    """
    from alembic import command

    from omnigent.db.db_models import ConversationBase, OmnigentBase

    migration_url = _resolve_migration_database_url(db_uri)
    migration_engine = engine
    owns_migration_engine = False
    if migration_url != db_uri:
        migration_engine = _create_engine(migration_url)
        owns_migration_engine = True

    try:
        _logger.info("Running database migrations...")
        config = _build_alembic_config(migration_url)
        # Pass a shared connection so Alembic operates within the same
        # engine (required for SQLite in-memory databases, and avoids
        # creating a second connection pool). The connection is handed over
        # outside any transaction so Alembic owns transaction demarcation:
        # a migration with an autocommit_block (CREATE INDEX CONCURRENTLY)
        # cannot suspend an externally-begun transaction.
        with (
            query_name_scope("omnigent.database.run_migrations"),
            _migration_lock(migration_engine),
        ):
            with migration_engine.connect() as connection:
                config.attributes["connection"] = connection
                command.upgrade(config, "head")
            # Keep metadata creation under the same lock: checkfirst is an
            # inspect-then-CREATE sequence and is not safe across callers.
            for base in (OmnigentBase, ConversationBase):
                base.metadata.create_all(bind=migration_engine, checkfirst=True)
    finally:
        if owns_migration_engine:
            migration_engine.dispose()


def _get_current_db_revision(engine: Engine) -> str | None:
    """
    Return the database's current Alembic revision, or ``None``.

    ``None`` means the database has no ``alembic_version`` table at
    all — i.e. nothing has ever been migrated against this database.
    A database that exists at some revision (even if not head) returns
    that revision string.

    :param engine: SQLAlchemy engine bound to the target database.
    :returns: The current revision hash (e.g. ``"c9d3a1f2e4b5"``) or
        ``None`` if the ``alembic_version`` table is absent.
    """
    from alembic.runtime.migration import MigrationContext

    with query_name_scope("omnigent.database.select_current_revision"):
        inspector = inspect(engine)
        if "alembic_version" not in inspector.get_table_names():
            return None
        with engine.connect() as connection:
            ctx = MigrationContext.configure(connection)
            return ctx.get_current_revision()


def _get_head_db_revision(db_uri: str) -> str:
    """
    Return the head Alembic revision for our migrations directory.

    Reads the migration scripts on disk (not the database). Raises
    if the migrations directory is empty or otherwise has no head —
    that would indicate a packaging bug.

    :param db_uri: Database URL — only used to build an Alembic
        ``Config`` pointing at our scripts directory; the database
        itself is not contacted.
    :returns: The head revision hash, e.g. ``"c9d3a1f2e4b5"``.
    :raises RuntimeError: If no head revision is defined.
    """
    from alembic.script import ScriptDirectory

    config = _build_alembic_config(db_uri)
    script = ScriptDirectory.from_config(config)
    head = script.get_current_head()
    if head is None:
        raise RuntimeError(
            "No Alembic head revision found — the migrations directory appears to be empty."
        )
    return head


def _initialize_or_verify_schema(engine: Engine, db_uri: str) -> None:
    """
    Bring a fresh or stale database to head before the server starts.

    Three cases:

    - **Fresh DB** (no ``alembic_version`` table) — run migrations to
      head. This covers brand-new SQLite files and freshly created
      Postgres schemas.
    - **At head** — no-op.
    - **Behind head** — log a warning, attempt an automatic Alembic
      upgrade to head, then verify that the database reached head.
      If the migration fails, re-raise with context so the server
      still terminates with an actionable error instead of continuing
      against an incompatible schema.

    :param engine: SQLAlchemy engine bound to the target database.
    :param db_uri: Database URL, used both for Alembic config and in
        any migration-failure error message.
    :raises RuntimeError: If automatic schema migration fails or does
        not bring the database to head.
    """
    head = _get_head_db_revision(db_uri)
    current = _get_current_db_revision(engine)

    if current is None:
        if not _run_migrations_on_boot():
            raise RuntimeError(
                "Omnigent database schema is not at head "
                f"(found revision <empty>, expected {head!r}); "
                f"{_RUN_MIGRATIONS_ON_BOOT_ENV}=0 disables automatic migration. "
                "Run the migration job or enable boot migrations before starting the server."
            )
        _run_migrations(engine, db_uri)
        return

    from alembic.script import ScriptDirectory
    from alembic.util.exc import CommandError

    script = ScriptDirectory.from_config(_build_alembic_config(db_uri))
    try:
        script.get_revision(current)
    except CommandError as exc:
        raise RuntimeError(
            f"Database revision {current!r} is ahead of this code: "
            "rollback-across-a-migration is not supported. "
            "Roll forward to a version that includes this revision, or restore "
            "the database from a backup."
        ) from exc

    if current != head:
        if not _run_migrations_on_boot():
            raise RuntimeError(
                "Omnigent database schema is out of date "
                f"(found revision {current!r}, expected {head!r}); "
                f"{_RUN_MIGRATIONS_ON_BOOT_ENV}=0 disables automatic migration. "
                "Run the migration job or enable boot migrations before starting the server."
            )
        _logger.warning(
            "Omnigent database schema is out of date "
            "(found revision %r, expected %r); attempting automatic migration.",
            current,
            head,
        )
        try:
            _run_migrations(engine, db_uri)
        except Exception as exc:
            raise RuntimeError(
                f"Omnigent database schema is out of date "
                f"(found revision {current!r}, expected {head!r}) "
                f"and automatic migration failed. Take a backup of your database, then run\n"
                f"\n"
                f"    omnigent debug db-upgrade {db_uri!r}\n"
                f"\n"
                f"to inspect or retry the migration manually."
            ) from exc

        migrated = _get_current_db_revision(engine)
        if migrated != head:
            raise RuntimeError(
                f"Omnigent automatic database migration did not reach head "
                f"(started at {current!r}, now at {migrated!r}, expected {head!r}). "
                f"Take a backup of your database, then run\n"
                f"\n"
                f"    omnigent debug db-upgrade {db_uri!r}\n"
                f"\n"
                f"to inspect or retry the migration manually."
            )


def clear_engine_cache() -> None:
    """
    Dispose of all cached engines and clear the engine cache.

    Intended for test teardown to ensure a fresh database state
    between test runs.
    """
    with _engine_lock:
        for engine in _engine_cache.values():
            engine.dispose()
        _engine_cache.clear()


# ── Managed session ────────────────────────────────────


# Ambient per-engine sessions for a read-only "share one checkout" scope. When
# active (see :func:`shared_read_scope`), ``managed_session()`` reuses the
# scope's session for its engine instead of opening a fresh pool checkout,
# collapsing several back-to-back reads (e.g. the access-control check's
# permission + conversation lookups) into a single connection round-trip.
# Keyed by ``id(engine)`` so distinct engines (split-DB) still get independent
# checkouts. Unset outside a scope, so it is a strict no-op for every ordinary
# caller.
_shared_read_sessions: ContextVar[dict[int, Session] | None] = ContextVar(
    "omnigent_shared_read_sessions", default=None
)


@contextmanager
def shared_read_scope() -> Iterator[None]:
    """Collapse back-to-back reads into one pool checkout per engine.

    Within this scope, ``managed_session()`` reuses a single session per
    engine rather than checking out a fresh pooled connection (plus a
    ``pool_pre_ping`` round-trip) on every store call. Intended for a short,
    strictly READ-ONLY burst — an access-control check, a snapshot assembly —
    where the per-call checkout dominates the actual query time.

    Nesting reuses the outer scope. Write makers (``immediate=True``) never
    participate, so they keep their own ``BEGIN IMMEDIATE`` isolation even
    when nested here. Never hold this open across network I/O: it pins a
    pooled connection for the scope's whole duration.
    """
    if _shared_read_sessions.get() is not None:
        # Already inside a scope — the outer one owns the sessions.
        yield
        return
    sessions: dict[int, Session] = {}
    token = _shared_read_sessions.set(sessions)
    try:
        yield
        for session in sessions.values():
            session.commit()
    except BaseException:
        for session in sessions.values():
            session.rollback()
        raise
    finally:
        for session in sessions.values():
            session.close()
        _shared_read_sessions.reset(token)


def make_managed_session_maker(
    engine: Engine,
    *,
    immediate: bool = False,
) -> ManagedSessionMaker:
    """
    Create a context-manager factory for database sessions.

    Sessions auto-commit on success and auto-rollback on failure.
    When the underlying dialect is SQLite, each session additionally
    enables ``PRAGMA foreign_keys`` and sets a 20-second
    ``busy_timeout``.

    :param engine: The SQLAlchemy engine to bind sessions to.
    :param immediate: When ``True`` and the dialect is SQLite, starts
        the transaction with ``BEGIN IMMEDIATE`` to acquire the write
        lock before any read, preventing check-then-insert races.
        No-op on PostgreSQL (``SELECT ... FOR UPDATE`` is used there).
    :returns: A callable that, when invoked, returns a context
        manager yielding a :class:`~sqlalchemy.orm.Session`.
    """
    # expire_on_commit=False keeps column attributes accessible on ORM
    # instances after the session commits and closes. Without it, SQLAlchemy
    # expires all attributes on commit, and any access outside the session
    # context (e.g. after the ``with session:`` block exits) raises
    # DetachedInstanceError. This is safe here because each managed session
    # is short-lived and single-writer, so there is no cross-session stale
    # data concern.
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    is_sqlite = engine.dialect.name == "sqlite"

    @contextmanager
    def managed_session() -> Iterator[Session]:
        """
        Yield a managed :class:`~sqlalchemy.orm.Session`.

        Commits on clean exit, rolls back on exception. For SQLite
        backends, enables foreign key enforcement and sets a
        busy timeout before yielding.

        Inside a :func:`shared_read_scope` (and only for read makers), the
        scope's per-engine session is reused instead of a fresh checkout;
        the scope — not this block — owns its commit/close.
        """
        if not immediate:
            shared = _shared_read_sessions.get()
            if shared is not None:
                key = id(engine)
                session = shared.get(key)
                if session is None:
                    session = factory()
                    # Register before the PRAGMAs: those executes force the pool
                    # checkout, so if one raises the scope must already track the
                    # session to close it (otherwise the connection would leak).
                    shared[key] = session
                    if is_sqlite:
                        session.execute(text("PRAGMA foreign_keys = ON"))
                        session.execute(text("PRAGMA busy_timeout = 20000"))  # 20s
                yield session
                return
        with factory() as session:
            try:
                if is_sqlite:
                    # PRAGMAs must run before BEGIN IMMEDIATE: foreign_keys is
                    # a no-op inside a transaction, and busy_timeout must be
                    # set before lock acquisition or it doesn't apply.
                    session.execute(text("PRAGMA foreign_keys = ON"))
                    session.execute(text("PRAGMA busy_timeout = 20000"))  # 20s
                    if immediate:
                        # Acquire write lock before any read to prevent
                        # concurrent check-then-insert races on SQLite.
                        session.execute(text("BEGIN IMMEDIATE"))
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    return managed_session


def make_named_managed_session_maker(
    engine: Engine,
    *,
    query_name_prefix: str,
    immediate: bool = False,
) -> NamedManagedSessionMaker:
    """Create managed sessions whose database work always has a semantic name.

    The supplied suffix is joined to ``query_name_prefix`` and remains active
    through the session's implicit flush and commit. A nested
    :func:`query_name_scope` can provide a more specific name for one statement.

    :param engine: The SQLAlchemy engine to bind sessions to.
    :param query_name_prefix: Stable namespace shared by the store's queries,
        e.g. ``"omnigent.file_store"``.
    :param immediate: Forwarded to :func:`make_managed_session_maker`.
    :returns: A callable accepting one semantic query-name suffix per session.
    """
    prefix = query_name_prefix.rstrip(".")
    if not prefix.strip():
        raise ValueError("query_name_prefix must not be empty")

    managed_session = make_managed_session_maker(engine, immediate=immediate)

    @contextmanager
    def named_managed_session(query_name: str) -> Iterator[Session]:
        if not query_name.strip():
            raise ValueError("query_name must not be empty")
        with query_name_scope(f"{prefix}.{query_name}"), managed_session() as session:
            yield session

    return named_managed_session


# ── ID generation ──────────────────────────────────────

# Recognised conversation-item types, validated at id generation. The item's
# type lives in the ``conversation_items.type`` column, not in its id. Kept in
# parity with ``ITEM_TYPE_TO_DATA_CLS`` (see the db util tests).
_ITEM_TYPES: frozenset[str] = frozenset(
    {
        "message",
        "function_call",
        "function_call_output",
        "error",
        "reasoning",
        "compaction",
        "native_tool",
        "resource_event",
        "slash_command",
        "terminal_command",
        "routing_decision",
    }
)


def generate_agent_id() -> str:
    """
    Generate a unique agent identifier.

    :returns: A bare 32-char hex uuid,
        e.g. ``"0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c"``.
    """
    return uuid.uuid4().hex


def builtin_agent_id(name: str) -> str:
    """
    Deterministic agent id for a built-in agent, derived from its name.

    Same shape and length as :func:`generate_agent_id` (bare 32-char hex), but
    stable across processes: a multi-tenant deployment reseeds the built-ins into
    an ephemeral per-pod store, where a random id would change each boot and
    dangle a persisted ``conversation.agent_id``. Do NOT revert built-in seeding
    to :func:`generate_agent_id` (guarded by the ``builtin_agent_id`` tests).

    :param name: The built-in agent's unique name, e.g. ``"polly"``.
    :returns: A deterministic bare 32-char hex id.
    """
    digest = hashlib.sha256(f"builtin:{name}".encode()).hexdigest()
    return digest[:32]


def generate_file_id() -> str:
    """
    Generate a unique file identifier.

    :returns: A bare 32-char hex uuid,
        e.g. ``"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"``.
    """
    return uuid.uuid4().hex


def generate_conversation_id() -> str:
    """
    Generate a unique conversation identifier.

    :returns: A bare 32-char hex uuid,
        e.g. ``"e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9"``.
    """
    return uuid.uuid4().hex


def generate_task_id() -> str:
    """
    Generate a unique task (response) identifier.

    :returns: A string of the form ``"resp_<32-char hex>"``,
        e.g. ``"resp_d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3"``.
    """
    return f"resp_{uuid.uuid4().hex}"


def generate_item_id(item_type: str) -> str:
    """
    Generate a unique conversation-item identifier.

    *item_type* is validated against :data:`_ITEM_TYPES` but no longer encoded
    into the id — the type lives in the ``conversation_items.type`` column.

    :param item_type: One of the members of :data:`_ITEM_TYPES`.
    :returns: A bare 32-char hex uuid, e.g. ``"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"``.
    :raises ValueError: If *item_type* is not a recognised type.
    """
    if item_type not in _ITEM_TYPES:
        raise ValueError(f"unknown item type: {item_type!r}")
    return uuid.uuid4().hex


# ── FTS (SQLite FTS5) ─────────────────────────────────

_FTS_TABLE = "conversation_items_fts"

_CREATE_FTS = text(
    f"CREATE VIRTUAL TABLE IF NOT EXISTS {_FTS_TABLE} USING fts5("
    "item_id UNINDEXED, conversation_id UNINDEXED, search_text)"
)

# Dialects that support SQLite's FTS5 extension. Cloudflare D1 is SQLite
# served over HTTP, so it gets full-text search too — gate FTS on the dialect
# *family*, not the literal name "sqlite". (The engine-level WAL/PRAGMA path in
# ``_create_engine`` stays sqlite-only: those are local-file concerns that D1
# neither needs nor supports over the wire.)
_FTS5_DIALECTS = frozenset({"sqlite", "cloudflare_d1"})


def _supports_fts5(dialect_name: str) -> bool:
    """
    Whether *dialect_name* is a SQLite-family dialect that supports FTS5.

    :param dialect_name: A SQLAlchemy ``dialect.name``, e.g. ``"sqlite"``,
        ``"cloudflare_d1"``, or ``"postgresql"``.
    :returns: ``True`` for SQLite and SQLite-over-the-wire dialects (D1),
        ``False`` otherwise.
    """
    return dialect_name in _FTS5_DIALECTS


def ensure_fts_table(engine: Engine) -> None:
    """
    Create the FTS5 virtual table on SQLite-family dialects. Idempotent.

    On dialects without FTS5 (e.g. PostgreSQL) this is a no-op.

    :param engine: The SQLAlchemy engine whose dialect is inspected.
        On a SQLite-family dialect (SQLite or Cloudflare D1) the
        ``conversation_items_fts`` virtual table is created if absent.
    """
    if _supports_fts5(engine.dialect.name):
        with query_name_scope("omnigent.database.ensure_fts_table"), engine.connect() as conn:
            conn.execute(_CREATE_FTS)
            conn.commit()


def insert_fts(
    session: Session,
    item_id: str,
    conversation_id: str,
    search_text: str,
) -> None:
    """
    Dual-write a row into the FTS5 table (SQLite-family dialects only).

    On dialects without FTS5 this is a no-op.

    :param session: An active SQLAlchemy session. Its bound engine's
        dialect is checked to decide whether to write.
    :param item_id: The conversation-item ID to index, e.g.
        ``"msg_a1b2c3d4..."``.
    :param conversation_id: The parent conversation ID, e.g.
        ``"conv_e4f5a6b7..."``.
    :param search_text: Plain-text content to store in the FTS
        index for this item.
    """
    if session.bind and _supports_fts5(session.bind.dialect.name):
        session.execute(
            text(
                f"INSERT INTO {_FTS_TABLE}"
                "(item_id, conversation_id, search_text) "
                "VALUES (:item_id, :cid, :st)"
            ),
            {"item_id": item_id, "cid": conversation_id, "st": search_text},
        )


def insert_fts_bulk(
    session: Session,
    rows: list[tuple[str, str, str]],
) -> None:
    """
    Dual-write multiple rows into the FTS5 table in a single INSERT.

    On dialects without FTS5 this is a no-op. An empty ``rows`` list is also
    a no-op.

    :param session: An active SQLAlchemy session.
    :param rows: Each tuple is ``(item_id, conversation_id, search_text)``.
    """
    if not rows:
        return
    if not (session.bind and _supports_fts5(session.bind.dialect.name)):
        return
    # 3 params per row; keep total < 999 (SQLite's safe SQLITE_MAX_VARIABLE_NUMBER
    # on pre-3.32 builds). Newer SQLite raised the limit to 32766, but chunking at
    # 300 is safe on all versions.
    _CHUNK_SIZE = 300
    for chunk_start in range(0, len(rows), _CHUNK_SIZE):
        chunk = rows[chunk_start : chunk_start + _CHUNK_SIZE]
        placeholders = ", ".join(f"(:item_id_{i}, :cid_{i}, :st_{i})" for i in range(len(chunk)))
        params: dict[str, str] = {}
        for i, (item_id, conversation_id, search_text) in enumerate(chunk):
            params[f"item_id_{i}"] = item_id
            params[f"cid_{i}"] = conversation_id
            params[f"st_{i}"] = search_text
        session.execute(
            text(
                f"INSERT INTO {_FTS_TABLE}"
                f"(item_id, conversation_id, search_text) "
                f"VALUES {placeholders}"
            ),
            params,
        )


def delete_fts_by_conversation(session: Session, conversation_id: str) -> None:
    """
    Remove all FTS rows for a conversation (SQLite-family dialects only).

    On dialects without FTS5 this is a no-op.

    :param session: An active SQLAlchemy session. Its bound engine's
        dialect is checked to decide whether to delete.
    :param conversation_id: The conversation whose FTS rows should be
        removed, e.g. ``"conv_e4f5a6b7..."``.
    """
    if session.bind and _supports_fts5(session.bind.dialect.name):
        session.execute(
            text(f"DELETE FROM {_FTS_TABLE} WHERE conversation_id = :cid"),
            {"cid": conversation_id},
        )


def delete_fts_by_conversation_ids(session: Session, conv_ids: list[str]) -> None:
    """
    Remove all FTS rows for a list of conversations in a single query.

    No-op when ``conv_ids`` is empty or the dialect lacks FTS5.

    :param session: An active SQLAlchemy session.
    :param conv_ids: Conversation IDs whose FTS rows should be removed.
    """
    if not conv_ids:
        return
    if session.bind and _supports_fts5(session.bind.dialect.name):
        placeholders = ", ".join(f":cid{i}" for i in range(len(conv_ids)))
        params = {f"cid{i}": cid for i, cid in enumerate(conv_ids)}
        session.execute(
            text(f"DELETE FROM {_FTS_TABLE} WHERE conversation_id IN ({placeholders})"),
            params,
        )


# ── Search text extraction ─────────────────────────────


def extract_search_text(item: NewConversationItem) -> str:
    """
    Extract plain text for FTS from an item's data, per DBSPEC.

    The item has already been Pydantic-validated, so required fields
    (content, name, arguments, output, summary) are guaranteed
    present. We use direct dict access to fail loud if that
    assumption is ever violated.

    Content/summary blocks are heterogeneous (text, image, etc.)
    so we filter to only text-bearing blocks via ``.get("text")``.

    :param item: A Pydantic-validated conversation item whose
        ``type`` is one of ``"message"``, ``"function_call"``,
        ``"function_call_output"``, ``"reasoning"``,
        ``"compaction"``, ``"native_tool"``, ``"resource_event"``,
        ``"slash_command"``, ``"terminal_command"``, or
        ``"routing_decision"``.
    :returns: A single plain-text string suitable for FTS indexing.
    :raises ValueError: If *item.type* is not a recognised type.
    """
    from omnigent.entities.conversation import CompactionData

    data = item.data.model_dump()
    if item.type == "message":
        return " ".join(
            block["text"]
            for block in data["content"]
            if isinstance(block, dict) and block.get("text")
        )
    if item.type == "function_call":
        return f"{data['name']} {data['arguments']}"
    if item.type == "function_call_output":
        return str(data["output"])
    if item.type == "error":
        return " ".join(part for part in (data["source"], data["code"], data["message"]) if part)
    if item.type == "reasoning":
        return " ".join(
            block["text"]
            for block in data["summary"]
            if isinstance(block, dict) and block.get("text")
        )
    if item.type == "compaction":
        assert isinstance(item.data, CompactionData)
        return item.data.summary
    if item.type == "native_tool":
        # Native tool items are opaque provider dicts — no
        # meaningful text to index for search.
        return ""
    if item.type == "resource_event":
        # Resource lifecycle records are metadata. Index only the stable
        # identifiers so persistence succeeds and basic resource lookup can
        # find the event, without dumping opaque metadata into FTS.
        return " ".join(
            part
            for part in (data["event_type"], data["resource_id"], data["resource_type"])
            if part
        )
    if item.type == "slash_command":
        # Index command name + args + stdout so FTS can find a
        # historical Skill invocation by what the operator typed
        # or what the command echoed. ``output`` may be absent
        # (skills with no inline stdout); coerce to "" for join.
        return " ".join(
            part for part in (data["name"], data["arguments"], data.get("output") or "") if part
        )
    if item.type == "terminal_command":
        # Index the command input + stdout so FTS can find historical
        # !cmd executions by what was typed or what was printed.
        return " ".join(
            part for part in (data.get("input") or "", data.get("stdout") or "") if part
        )
    if item.type == "routing_decision":
        # Index model + rationale so FTS can find a router verdict by
        # the model it picked or its one-line explanation.
        return " ".join(part for part in (data.get("model"), data.get("rationale")) if part)
    raise ValueError(f"unknown item type: {item.type!r}")


def strip_nul_bytes(value: str) -> str:
    """
    Remove NUL (``0x00``) bytes from a string bound for a text column.

    PostgreSQL ``text``/``varchar`` columns reject NUL bytes outright
    (``psycopg.DataError: PostgreSQL text fields cannot contain NUL
    (0x00) bytes``), so any tool output, message, or search text that
    embeds a NUL — e.g. a tool that returns the contents of a binary
    file — would otherwise abort the whole ``INSERT``. SQLite tolerates
    NUL, so stripping uniformly here also keeps the two backends
    behaving identically. NUL carries no textual or full-text-search
    meaning, so removing it is lossless for our purposes.

    :param value: The string about to be persisted to a text column,
        e.g. a JSON-serialized item payload or an FTS search string.
    :returns: The same string with every ``"\\x00"`` removed; returned
        unchanged when no NUL bytes are present.
    """
    return value.replace("\x00", "")


def build_search_snippet(
    text: str,
    query: str,
    *,
    context: int = 60,
    max_len: int = 160,
) -> str | None:
    """
    Build a short excerpt of ``text`` centered on the first ``query`` match.

    Powers the session-search preview: the sidebar/palette matches on chat
    content, so a hit is often invisible in the session title. This returns
    the matching span plus a little surrounding context, with ``…`` marking
    elided ends, so the UI can show *where* a session matched.

    Matching is case-insensitive substring (mirrors the ``LIKE`` filter that
    selected the row). Whitespace in ``text`` is collapsed first so a match
    inside a multi-line tool output renders as one clean line.

    :param text: The item's plain search text to excerpt from.
    :param query: The user's search string, e.g. ``"deploy error"``.
    :param context: Characters of context to keep on each side of the match.
    :param max_len: Hard cap on the returned snippet length (excluding the
        ``…`` markers) so a giant match term can't blow up the row.
    :returns: The excerpt, or ``None`` when ``query`` is empty or does not
        occur in ``text`` (caller then falls back to no preview).
    """
    if not query:
        return None
    collapsed = " ".join(text.split())
    idx = collapsed.lower().find(query.lower())
    if idx == -1:
        return None
    match_end = idx + len(query)
    start = max(0, idx - context)
    end = min(len(collapsed), match_end + context)
    # Keep the total under max_len, but never clamp the matched term itself out
    # of the window — otherwise the UI would highlight nothing. A pathologically
    # long match term overflows max_len rather than being cut mid-term.
    if end - start > max_len:
        end = max(start + max_len, match_end)
    snippet = collapsed[start:end]
    if start > 0:
        snippet = f"…{snippet}"
    if end < len(collapsed):
        snippet = f"{snippet}…"
    return snippet


# ── Timestamp ──────────────────────────────────────────


def now_epoch() -> int:
    """
    Return the current time as Unix epoch seconds (integer).

    :returns: Seconds since 1970-01-01 00:00:00 UTC, truncated to
        an integer.
    """
    return int(time.time())


def now_epoch_us() -> int:
    """
    Return the current time as Unix epoch microseconds (integer).

    Used for change-detection timestamps (``comments.updated_at``)
    where consecutive writes inside the same second must still produce
    distinct, ordered values — second-granularity ``now_epoch`` would
    make back-to-back mutations indistinguishable to diff-based
    consumers like the ``WS /v1/sessions/updates`` fingerprint.
    Microseconds rather than nanoseconds because epoch-µs stays below
    JavaScript's ``Number.MAX_SAFE_INTEGER`` (until ~2255), so web
    clients read the JSON value exactly.

    :returns: Microseconds since 1970-01-01 00:00:00 UTC.
    """
    return time.time_ns() // 1_000


def utc_day(epoch_seconds: int) -> str:
    """
    Return the UTC calendar day for a Unix epoch timestamp.

    The day key used to bucket per-user daily cost: a session spanning
    midnight UTC splits its spend across both days. Always UTC so the
    bucket is unambiguous across deployments.

    :param epoch_seconds: Unix epoch seconds, e.g. ``1749081600``.
    :returns: The UTC date as ``"YYYY-MM-DD"``, e.g. ``"2026-06-05"``.
    """
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).date().isoformat()
