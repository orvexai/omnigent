"""OSS Docker entrypoint for the Omnigent server.

Mirrors ``deploy/databricks/src/app.py`` (the Databricks Apps entrypoint) but
configured for a plain Postgres database and a local-filesystem
artifact store. Intended to run inside the image built by
``deploy/docker/Dockerfile``.

Execution mode: external runners only. The server accepts runner
WebSocket connections at ``/v1/runner/tunnel`` and never spawns
harness subprocesses on its own. Users run ``omnigent run … --server
<url>`` on their own machine; that runner dials in.

Importing this module has **no side effects**: configuration loading,
DB migrations, store construction, and app building all live inside
``build_app()`` / ``run_migrations()`` / ``main()``. Nothing connects
to a database, reads config, or builds the app until ``main()`` runs —
which the ``if __name__ == "__main__":`` block (i.e. the Docker
``CMD ["python", "/app/entrypoint.py"]``) invokes. This keeps the
module importable for testing / tooling without a live database.

Configuration is via environment variables:

  DATABASE_URL          Required. SQLAlchemy URL. Both PaaS-style URLs
                        (``postgresql://<user>:<password>@host:5432/db``,
                        ``postgres://...``) and the explicit psycopg3
                        form (``postgresql+psycopg://...``) are accepted;
                        the prefix is normalized automatically.
  MIGRATION_DATABASE_URL Optional direct PostgreSQL URL for Alembic; defaults
                        to the resolved application database URL.
  OMNIGENT_RUN_MIGRATIONS_ON_BOOT
                        Defaults to ``1``. Set to ``0`` to verify the schema
                        without upgrading it.
  OMNIGENT_DB_DISABLE_PREPARED_STATEMENTS
                        Defaults to ``0``. Set to ``1`` for transaction pools.
  OMNIGENT_MIGRATION_LOCK_TIMEOUT_SECONDS
                        Defaults to ``600`` seconds for the advisory-lock wait.
  OMNIGENT_SERVER_SHUTDOWN_TIMEOUT_S
                        Defaults to ``20`` seconds for the bounded drain.
  ARTIFACT_DIR          Directory for the local artifact store.
                        Defaults to ``/data/artifacts`` (the volume
                        mount point used by docker-compose).
  HOST, PORT            Bind address. Default ``0.0.0.0:8000``.
"""

from __future__ import annotations

import logging
import math
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI

    from omnigent.stores.artifact_store import ArtifactStore

logging.basicConfig(level=logging.INFO, stream=sys.stderr, force=True)
logger = logging.getLogger("omnigent-docker")

# Defaults live as module-level constants — the Dockerfile and
# docker-compose.yaml both also set these, so the values here just
# document the contract for anyone running entrypoint.py outside the
# image. Source: deploy/docker/Dockerfile (ENV block) and
# deploy/docker/docker-compose.yaml.
_DEFAULT_HOST = "0.0.0.0"
# Pinned to 8000 by design (container/platform convention) — deliberately
# decoupled from the CLI's local-server default (6767 in host/local_server.py).
_DEFAULT_PORT = "8000"
_DEFAULT_ARTIFACT_DIR = "/data/artifacts"
_DEFAULT_SERVER_SHUTDOWN_TIMEOUT_S = 20
_SERVER_SHUTDOWN_TIMEOUT_ENV = "OMNIGENT_SERVER_SHUTDOWN_TIMEOUT_S"


@dataclass(frozen=True)
class _ResolvedConfig:
    """Configuration resolved before migrations and app construction."""

    cfg: dict[str, Any]
    database_url: str
    artifact_dir: Path
    # When set (an ``s3://bucket[/prefix]`` URI), the artifact store is remote
    # (S3/R2) and ``artifact_dir`` is only a local scratch dir (cookie secret,
    # on-disk cache). ``None`` -> the local filesystem store at ``artifact_dir``.
    artifact_store_uri: str | None
    host: str
    port: int


@dataclass(frozen=True)
class _BuiltApp:
    """The FastAPI app plus resolved bind settings.

    ``_resolve_config`` handles config loading, database URL normalization,
    artifact directory setup, auth defaults, and HOST/PORT resolution before
    migrations run. ``main()`` then runs migrations explicitly and calls
    ``build_app`` to construct the app from that resolved config. Returning
    HOST/PORT with the app keeps the handoff to ``uvicorn.run`` explicit
    without requiring a second config-resolution pass.
    """

    app: FastAPI
    host: str
    port: int


def run_migrations(database_url: str) -> None:
    """Run the Alembic upgrade against ``database_url``.

    The SQLAlchemy stores refuse to start on a stale schema, so this
    runs before any store boots. Creates a throwaway engine, upgrades,
    and disposes it.
    """
    from omnigent.db.utils import (
        _create_engine,
        _resolve_migration_database_url,
    )
    from omnigent.db.utils import (
        _run_migrations as _run_alembic_upgrade,
    )

    migration_database_url = _resolve_migration_database_url(database_url)
    migration_engine = _create_engine(migration_database_url)
    try:
        _run_alembic_upgrade(migration_engine, migration_database_url)
    finally:
        migration_engine.dispose()


def _run_migrations_on_boot() -> bool:
    """Return whether the Docker server should upgrade its schema at boot."""
    raw_value = os.environ.get("OMNIGENT_RUN_MIGRATIONS_ON_BOOT")
    return raw_value is None or raw_value.strip().lower() not in {"0", "false", "no", "off"}


def _server_shutdown_timeout() -> float:
    """Resolve the positive Uvicorn shutdown timeout for the container."""
    from omnigent.cli import _parse_shutdown_timeout

    raw_value = os.environ.get(
        _SERVER_SHUTDOWN_TIMEOUT_ENV,
        str(_DEFAULT_SERVER_SHUTDOWN_TIMEOUT_S),
    )
    try:
        return _parse_shutdown_timeout(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            f"{_SERVER_SHUTDOWN_TIMEOUT_ENV} must be a positive number, got {raw_value!r}"
        ) from exc


def _resolve_database_url(cfg: dict[str, Any]) -> str:
    """Resolve and normalize the database URL without app startup side effects.

    :param cfg: Parsed server configuration mapping.
    :returns: A SQLAlchemy database URL using the psycopg3 scheme when needed.
    :raises RuntimeError: If no database URL is configured.
    """
    from omnigent.db.utils import normalize_database_url

    database_url = os.environ.get("DATABASE_URL") or cfg.get("database_uri")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is required (env), or set `database_uri:` in the server config. "
            "Accepted forms: "
            "'postgresql+psycopg://user:pw@host:5432/omnigent' (explicit psycopg3), "
            "or the 'postgres://' / 'postgresql://' URLs emitted by Railway, Render, etc."
        )
    return normalize_database_url(database_url)


def _resolve_config() -> _ResolvedConfig:
    """Load config and resolve startup settings before migrations run."""

    from omnigent.server.paas_env import detect_base_url, resolve_bind_host
    from omnigent.server.server_config import load_server_config

    # ── Configuration ────────────────────────────────────────
    # Non-secret settings come from a YAML config file (default
    # <data_dir>/config.yaml, e.g. /data/config.yaml on the volume, or
    # OMNIGENT_CONFIG) — the same experience a laptop gets from
    # `omnigent server -c`. Secrets stay in the environment:
    # DATABASE_URL (carries the password) and the cookie / OIDC secrets.
    cfg = load_server_config()

    database_url = _resolve_database_url(cfg)

    # App settings are config-first, env fallback, then the built-in default.
    artifact_dir = Path(
        cfg.get("artifact_location") or os.environ.get("ARTIFACT_DIR") or _DEFAULT_ARTIFACT_DIR
    )
    # resolve_bind_host strips the bracketed IPv6 form some platforms inject
    # ("[::]") and coerces Railway's IPv6 wildcard to IPv4 (its edge is v4-only).
    host = resolve_bind_host(
        cfg.get("host") or os.environ.get("HOST"), os.environ, default=_DEFAULT_HOST
    )
    port = int(cfg.get("port") or os.environ.get("PORT") or _DEFAULT_PORT)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    # Optional remote artifact store (S3 / Cloudflare R2 / MinIO / …). When set,
    # the artifact STORE is remote and durable; artifact_dir stays local for the
    # cookie secret and on-disk cache. Mirrors how DATABASE_URL selects the DB.
    artifact_store_uri = cfg.get("artifact_store_uri") or os.environ.get("OMNIGENT_ARTIFACT_URI")
    if artifact_store_uri and not artifact_store_uri.startswith("s3://"):
        raise RuntimeError(
            "OMNIGENT_ARTIFACT_URI (or `artifact_store_uri:` in config) must be an "
            f"'s3://bucket[/prefix]' URI, got: {artifact_store_uri!r}"
        )

    logger.info(
        "Config: HOST=%s PORT=%d DB=%s ARTIFACTS=%s",
        host,
        port,
        database_url.split("@", 1)[-1] if "@" in database_url else database_url,
        artifact_store_uri or artifact_dir,
    )

    # Containerized / remote deploys default to authenticated auth.
    # The framework-wide default (a bare local `omnigent server`) is
    # single-user header mode with no login, but a Docker / HF / PaaS
    # instance is typically network-exposed, so we opt it into the
    # multi-user login flow here — accounts by default, or OIDC if the
    # operator supplied OMNIGENT_OIDC_* config. An operator can still
    # force header/oidc/accounts via OMNIGENT_AUTH_PROVIDER, or turn
    # auth off with OMNIGENT_AUTH_ENABLED=0.
    os.environ.setdefault("OMNIGENT_AUTH_ENABLED", "1")

    # Kill-switch ergonomics: OMNIGENT_AUTH_ENABLED=0 means "no login,
    # single-user local container" (the documented local-dev posture).
    # Header mode now fails closed on a missing X-Forwarded-Email,
    # so without this marker a no-auth container would
    # 401 every request — nothing injects the header. Only the implicit
    # kill-switch path gets the marker: an EXPLICIT
    # OMNIGENT_AUTH_PROVIDER=header deploy declared a header-injecting
    # proxy and must stay strict.
    from omnigent.server.auth import (
        env_var_is_truthy,
        resolve_auth_source,
        warn_if_single_user_exposed,
    )

    # Compose passes OMNIGENT_AUTH_PROVIDER as "" when unset
    # ("${VAR:-}"): empty and missing both mean "not explicitly pinned".
    _raw_auth_provider = os.environ.get("OMNIGENT_AUTH_PROVIDER")
    _auth_provider_explicit = bool(_raw_auth_provider and _raw_auth_provider.strip())
    if not _auth_provider_explicit and not env_var_is_truthy("OMNIGENT_AUTH_ENABLED"):
        os.environ.setdefault("OMNIGENT_LOCAL_SINGLE_USER", "1")

    # Accounts mode ergonomics: when the operator hasn't set them, supply the
    # two required vars (cookie secret + base URL) so a 1-click / `docker
    # compose up` deploy works with zero config. Gate on the *resolved*
    # selection so an explicit header/oidc deploy (or AUTH_ENABLED=0)
    # doesn't mint accounts secrets it never reads.
    if resolve_auth_source() == "accounts":
        from omnigent.server.accounts_secret import load_or_generate_cookie_secret

        # Empty-check, not setdefault: compose passes these as empty strings
        # ("${VAR:-}"), which setdefault would leave in place — defeating the default.
        if not os.environ.get("OMNIGENT_ACCOUNTS_COOKIE_SECRET"):
            os.environ["OMNIGENT_ACCOUNTS_COOKIE_SECRET"] = load_or_generate_cookie_secret(
                artifact_dir
            )
        if not os.environ.get("OMNIGENT_ACCOUNTS_BASE_URL"):
            # Auto-detect the public URL from the PaaS env (Render / Railway /
            # Fly / HF Spaces) so a 1-click deploy needs zero manual config;
            # falls back to the bind address for local / Docker / EC2.
            os.environ["OMNIGENT_ACCOUNTS_BASE_URL"] = detect_base_url(
                os.environ, host=host, port=port
            )

    # Logged, not printed: container stderr is buried in a platform log viewer.
    _exposure = warn_if_single_user_exposed(host)
    if _exposure:
        logger.warning("%s", _exposure)

    return _ResolvedConfig(
        cfg=cfg,
        database_url=database_url,
        artifact_dir=artifact_dir,
        artifact_store_uri=artifact_store_uri,
        host=host,
        port=port,
    )


def _select_artifact_store(resolved_config: _ResolvedConfig) -> ArtifactStore:
    """
    Pick the artifact store implementation from the resolved config.

    An ``s3://bucket[/prefix]`` ``artifact_store_uri`` selects the remote,
    durable :class:`~omnigent.stores.artifact_store.s3.S3ArtifactStore` (AWS S3,
    Cloudflare R2, MinIO, …), which survives an ephemeral or multi-replica
    deploy. Otherwise the local-filesystem store at ``artifact_dir`` is used.
    Mirrors how ``DATABASE_URL`` selects the database backend.

    :param resolved_config: The resolved startup configuration.
    :returns: The selected
        :class:`~omnigent.stores.artifact_store.ArtifactStore`.
    """
    from omnigent.stores.artifact_store.local import LocalArtifactStore

    if resolved_config.artifact_store_uri:
        from omnigent.stores.artifact_store.s3 import S3ArtifactStore

        return S3ArtifactStore(resolved_config.artifact_store_uri)
    return LocalArtifactStore(str(resolved_config.artifact_dir))


def _build_local_llm_routing_client(
    server_llm: Any,  # type: ignore[explicit-any]  # LLMConfig | None
) -> Any | None:  # type: ignore[explicit-any]  # LLMRoutingClient | None
    if server_llm is None:
        return None
    from omnigent.runtime.policies.builder import (
        _build_policy_llm_client,
        _resolve_server_llm_connection,
    )

    conn = _resolve_server_llm_connection(server_llm)
    policy_client = _build_policy_llm_client(server_llm, conn)
    if policy_client is None:
        return None
    from omnigent.server.smart_routing import LLMRoutingClient

    return LLMRoutingClient(policy_client)


def _build_routing(
    cfg: dict[str, Any],
    server_llm: Any,  # type: ignore[explicit-any]  # LLMConfig | None
) -> tuple[Any, Any]:  # type: ignore[explicit-any]  # (RoutingClient | None, RoutingSettings)
    """Build the routing client and settings from the ``routing:`` block.

    Reuses the CLI's parser and builder so a Docker deployment honours the
    same ``routing.*`` keys (router name, selection model, model prefixes) a
    local server does.

    :param cfg: The parsed server config mapping.
    :param server_llm: The parsed server-level ``LLMConfig``, used for the
        built-in judge when no external router is configured.
    :returns: ``(routing_client, routing_settings)`` for ``RuntimeCaps``.
    """
    from omnigent.cli import _build_external_routing_client, parse_routing_settings

    routing_cfg = cfg.get("routing")
    settings = parse_routing_settings(routing_cfg)
    if isinstance(routing_cfg, dict) and routing_cfg.get("provider") == "external":
        return _build_external_routing_client(routing_cfg, settings), settings
    return _build_local_llm_routing_client(server_llm), settings


def _resolve_execution_timeout(cfg: dict[str, Any]) -> int:
    """Return the configured execution limit or the RuntimeCaps default."""
    return int(cfg.get("execution_timeout") or 7200)


def build_app(resolved_config: _ResolvedConfig | None = None) -> _BuiltApp:
    """Resolve config if needed, wire the stores, and build the app.

    This function intentionally does not run migrations; ``main()`` runs
    them explicitly after config resolution and before store construction.
    """
    from omnigent.server.app import create_app
    from omnigent.server.server_config import config_str_list

    if resolved_config is None:
        resolved_config = _resolve_config()

    cfg = resolved_config.cfg
    database_url = resolved_config.database_url
    artifact_dir = resolved_config.artifact_dir

    # ── Stores ───────────────────────────────────────────────

    from omnigent.runtime import init as init_runtime
    from omnigent.runtime import telemetry
    from omnigent.runtime.agent_cache import AgentCache
    from omnigent.runtime.caps import RuntimeCaps
    from omnigent.server.managed_hosts import parse_sandbox_config
    from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
    from omnigent.stores.comment_store.sqlalchemy_store import (
        SqlAlchemyCommentStore,
    )
    from omnigent.stores.conversation_store.sqlalchemy_store import (
        SqlAlchemyConversationStore,
    )
    from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore
    from omnigent.stores.host_store import HostStore
    from omnigent.stores.permission_store.sqlalchemy_store import (
        SqlAlchemyPermissionStore,
    )
    from omnigent.stores.policy_store.sqlalchemy_store import SqlAlchemyPolicyStore
    from omnigent.stores.project_store.sqlalchemy_store import SqlAlchemyProjectStore
    from omnigent.stores.scheduled_task_store.sqlalchemy_store import (
        SqlAlchemyScheduledTaskStore,
    )

    telemetry.init()

    agent_store = SqlAlchemyAgentStore(database_url)
    file_store = SqlAlchemyFileStore(database_url)
    conversation_store = SqlAlchemyConversationStore(database_url)
    comment_store = SqlAlchemyCommentStore(database_url)
    permission_store = SqlAlchemyPermissionStore(database_url)
    host_store = HostStore(database_url)
    policy_store = SqlAlchemyPolicyStore(database_url)
    scheduled_task_store = SqlAlchemyScheduledTaskStore(database_url)
    project_store = SqlAlchemyProjectStore(database_url)
    # Fail startup loud on a malformed `sandbox:` section (an operator
    # typo should not surface as a runtime 502 on the first managed
    # session); the startup catch-all below logs it.
    sandbox_config = parse_sandbox_config(cfg.get("sandbox"))
    artifact_store = _select_artifact_store(resolved_config)

    agent_cache = AgentCache(
        artifact_store=artifact_store,
        cache_dir=artifact_dir / ".cache",
    )

    from omnigent.spec import parse_default_policies, parse_server_llm

    server_llm = parse_server_llm(cfg.get("llm"))

    routing_client, routing_settings = _build_routing(cfg, server_llm)

    caps = RuntimeCaps(
        execution_timeout=_resolve_execution_timeout(cfg),
        default_policies=parse_default_policies(cfg.get("policies")),
        llm=server_llm,
        routing_client=routing_client,
        routing_settings=routing_settings,
    )

    init_runtime(
        agent_cache=agent_cache,
        caps=caps,
        agent_store=agent_store,
        file_store=file_store,
        conversation_store=conversation_store,
        artifact_store=artifact_store,
        comment_store=comment_store,
        policy_store=policy_store,
    )

    # Build the auth provider from the live env (header/oidc/accounts).
    # Accounts mode also needs an AccountStore explicitly wired — the
    # entrypoint constructs it here rather than letting create_app do
    # so internally, so this same code path can opt out by passing
    # None for non-accounts deploys (matching the structural
    # contract used on the hosted product).
    from omnigent.server.auth import UnifiedAuthProvider as _UAP
    from omnigent.server.auth import create_auth_provider

    auth_provider = create_auth_provider()
    account_store = None
    if isinstance(auth_provider, _UAP) and auth_provider._source == "accounts":
        from omnigent.server.accounts_store import SqlAlchemyAccountStore

        account_store = SqlAlchemyAccountStore(database_url)

    app = create_app(
        agent_store=agent_store,
        file_store=file_store,
        conversation_store=conversation_store,
        artifact_store=artifact_store,
        agent_cache=agent_cache,
        comment_store=comment_store,
        permission_store=permission_store,
        policy_store=policy_store,
        host_store=host_store,
        scheduled_task_store=scheduled_task_store,
        project_store=project_store,
        auth_provider=auth_provider,
        account_store=account_store,
        # Non-secret auth settings from the config file (admins are the
        # canonical, declarative roster; allowed_domains gates OIDC). Both
        # union with their runtime-editable files under <data_dir>.
        admins=config_str_list(cfg.get("admins")),
        allowed_domains=config_str_list(cfg.get("allowed_domains")),
        sandbox_config=sandbox_config,
    )

    return _BuiltApp(app=app, host=resolved_config.host, port=resolved_config.port)


def main() -> None:
    """Boot the server: build the app and hand it to uvicorn.

    Wraps the whole boot in the startup catch-all so any failure
    (config, migrations, store wiring) lands in the container logs and
    the process holds open briefly for log capture before exiting
    non-zero — the orchestrator then restarts us.
    """
    try:
        resolved_config = _resolve_config()

        # ── Migrations ───────────────────────────────────────────
        # Alembic upgrade runs before the stores boot — the SQLAlchemy
        # stores refuse to start on a stale schema.
        if _run_migrations_on_boot():
            run_migrations(resolved_config.database_url)
        else:
            logger.info(
                "Skipping database migrations at boot; schema verification remains enabled."
            )

        resolved = build_app(resolved_config)

        import uvicorn

        from omnigent.runner.transports.ws_tunnel.limits import (
            RUNNER_TUNNEL_MAX_MESSAGE_BYTES,
            TUNNEL_KEEPALIVE_PING_INTERVAL_S,
            TUNNEL_KEEPALIVE_PING_TIMEOUT_S,
        )

        logger.info("Starting omnigent server on %s:%d", resolved.host, resolved.port)
        # Let Uvicorn close SSE as a transport drop so clients reconnect; do
        # not emit session_stream's _DONE sentinel, which means deliberate close.
        uvicorn.run(
            resolved.app,
            host=resolved.host,
            port=resolved.port,
            ws_max_size=RUNNER_TUNNEL_MAX_MESSAGE_BYTES,
            ws_ping_interval=TUNNEL_KEEPALIVE_PING_INTERVAL_S,
            ws_ping_timeout=TUNNEL_KEEPALIVE_PING_TIMEOUT_S,
            timeout_graceful_shutdown=math.ceil(_server_shutdown_timeout()),
        )
    except TimeoutError:
        logger.error("FATAL: migration advisory lock timed out:\n%s", traceback.format_exc())
        sys.exit(1)
    except Exception:  # noqa: BLE001 — startup catch-all so failures land in logs
        logger.error("FATAL: omnigent server failed to start:\n%s", traceback.format_exc())
        # Keep the process alive briefly so the container log capture has time
        # to flush before the orchestrator restarts us.
        import time  # deferred — keeps module inert

        time.sleep(30)
        sys.exit(1)


def migrate_only() -> int:
    """Run migrations for the short-lived migration Job.

    :returns: Zero on success, or one when configuration, connection, or
        migration fails.
    """
    migration_engine = None
    try:
        from omnigent.db.utils import (
            _get_current_db_revision,
            _get_head_db_revision,
            _run_migrations,
        )
        from omnigent.server.server_config import load_server_config

        database_url = _resolve_database_url(load_server_config())
        from omnigent.db.utils import _create_engine, _resolve_migration_database_url

        migration_database_url = _resolve_migration_database_url(database_url)
        migration_engine = _create_engine(migration_database_url)
        current = _get_current_db_revision(migration_engine)
        head = _get_head_db_revision(migration_database_url)
        if current == head:
            logger.info("Database schema already at head (%s)", head)
        else:
            logger.info("Migrating database schema: %s → %s", current or "<empty>", head)
        _run_migrations(migration_engine, migration_database_url)
        return 0
    except Exception:  # noqa: BLE001 — a Job must fail promptly without retry sleep
        logger.error("Database migration failed:\n%s", traceback.format_exc())
        return 1
    finally:
        if migration_engine is not None:
            migration_engine.dispose()


def _cli() -> int:
    """Dispatch the server or migrate-only command line mode."""
    import argparse

    parser = argparse.ArgumentParser(description="Run the Omnigent Docker entrypoint")
    parser.add_argument(
        "--migrate-only",
        action="store_true",
        help="run database migrations and exit without starting the server",
    )
    args = parser.parse_args()
    if args.migrate_only:
        return migrate_only()
    main()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
