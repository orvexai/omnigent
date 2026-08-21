"""Guard: importing the OSS Docker entrypoint has no side effects.

The Docker image runs ``python /app/entrypoint.py`` (see
``deploy/docker/Dockerfile``), so all of the boot work — config load,
Alembic migrations, store construction, ``create_app`` — lives behind
``main()`` and must not fire at import time. This test enforces that:
the module must import cleanly with ``DATABASE_URL`` unset and without
ever touching the database (``sqlalchemy.create_engine`` is wired to
blow up if called during import).
"""

from __future__ import annotations

import importlib
import inspect
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn

import pytest

from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.artifact_store.s3 import S3ArtifactStore

_ENTRYPOINT_MODULE = "deploy.docker.entrypoint"
_BOOT_MODULES = (
    "fastapi",
    "omnigent.db.utils",
    "omnigent.runtime",
    "omnigent.server.app",
    "omnigent.server.server_config",
    "omnigent.stores.agent_store.sqlalchemy_store",
    "omnigent.stores.artifact_store.local",
    "uvicorn",
)


@pytest.fixture
def _fresh_entrypoint_import(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Force a from-scratch import of the entrypoint, DB-unset.

    Drops any cached copy of the module, clears ``DATABASE_URL`` so the
    import can't lean on an ambient one, and trip-wires
    ``sqlalchemy.create_engine`` so any import-time DB access fails the
    test loudly rather than silently connecting.
    """
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delitem(sys.modules, _ENTRYPOINT_MODULE, raising=False)
    for module_name in _BOOT_MODULES:
        monkeypatch.delitem(sys.modules, module_name, raising=False)

    import sqlalchemy

    create_engine_calls: list[str] = []

    def _no_engine_at_import(*args: object, **kwargs: object) -> NoReturn:
        create_engine_calls.append(repr((args, kwargs)))
        raise AssertionError(
            "sqlalchemy.create_engine() must not be called while importing "
            f"{_ENTRYPOINT_MODULE} — DB work belongs in main()/build_app()."
        )

    monkeypatch.setattr(sqlalchemy, "create_engine", _no_engine_at_import)
    return create_engine_calls


def test_entrypoint_imports_without_side_effects(
    _fresh_entrypoint_import: list[str],
) -> None:
    # Importing must not raise (the old module-level code raised
    # RuntimeError here because DATABASE_URL was unset) and must not
    # have created an engine (the monkeypatched create_engine would
    # have raised AssertionError).
    module = importlib.import_module(_ENTRYPOINT_MODULE)

    # The boot entry points exist and the module is inert until called.
    assert callable(module.main)
    assert callable(module.build_app)
    assert callable(module.run_migrations)
    # No app was built at import time.
    assert not hasattr(module, "app")
    assert _fresh_entrypoint_import == []
    # Config, migrations, runtime/store wiring, and create_app all stay behind
    # build_app()/main() rather than being imported or executed at module import.
    for module_name in _BOOT_MODULES:
        assert module_name not in sys.modules


# ── artifact-store resolution + selection ────────────────────────────────
# OMNIGENT_ARTIFACT_URI=s3://… selects the remote S3ArtifactStore (durable on an
# ephemeral/multi-replica deploy); anything else falls back to local. The URI is
# validated up front (must be s3://), mirroring how DATABASE_URL picks the DB.


@pytest.fixture
def _entrypoint_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Minimal env for ``_resolve_config``: a DB URL and a tmp artifact dir,
    auth disabled so it doesn't mint accounts secrets, and no ambient
    artifact-store URI (each test sets it as needed)."""
    # Point config at an empty file so the resolver doesn't read the developer's
    # ambient ~/.omnigent/config.yaml (keeps the test hermetic; CI has none).
    config_file = tmp_path / "config.yaml"
    config_file.write_text("{}\n")
    monkeypatch.setenv("OMNIGENT_CONFIG", str(config_file))
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/omnigent")
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setenv("OMNIGENT_AUTH_ENABLED", "0")
    monkeypatch.delenv("OMNIGENT_ARTIFACT_URI", raising=False)


def test_resolve_config_captures_s3_artifact_uri(
    _entrypoint_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deploy.docker.entrypoint import _resolve_config

    monkeypatch.setenv("OMNIGENT_ARTIFACT_URI", "s3://my-bucket/artifacts")
    assert _resolve_config().artifact_store_uri == "s3://my-bucket/artifacts"


def test_resolve_config_defaults_to_no_remote_store(_entrypoint_env: None) -> None:
    from deploy.docker.entrypoint import _resolve_config

    assert _resolve_config().artifact_store_uri is None


def test_resolve_config_rejects_non_s3_artifact_uri(
    _entrypoint_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deploy.docker.entrypoint import _resolve_config

    monkeypatch.setenv("OMNIGENT_ARTIFACT_URI", "gs://my-bucket")
    with pytest.raises(RuntimeError, match="s3://"):
        _resolve_config()


@pytest.mark.parametrize(
    ("database_url", "expected"),
    [
        (
            "postgres://u:p@localhost:5432/omnigent",
            "postgresql+psycopg://u:p@localhost:5432/omnigent",
        ),
        (
            "postgresql://u:p@localhost:5432/omnigent",
            "postgresql+psycopg://u:p@localhost:5432/omnigent",
        ),
        (
            "postgresql+psycopg://u:p@localhost:5432/omnigent",
            "postgresql+psycopg://u:p@localhost:5432/omnigent",
        ),
    ],
)
def test_migration_database_url_matches_full_config_resolution(
    _entrypoint_env: None,
    monkeypatch: pytest.MonkeyPatch,
    database_url: str,
    expected: str,
) -> None:
    from deploy.docker.entrypoint import _resolve_config, _resolve_database_url
    from omnigent.server.server_config import load_server_config

    monkeypatch.setenv("DATABASE_URL", database_url)
    cfg = load_server_config()
    assert _resolve_database_url(cfg) == expected
    assert _resolve_config().database_url == expected


def test_migration_database_url_can_come_from_config_file(
    _entrypoint_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from deploy.docker.entrypoint import _resolve_config, _resolve_database_url
    from omnigent.server.server_config import load_server_config

    monkeypatch.delenv("DATABASE_URL")
    config_path = Path(os.environ["OMNIGENT_CONFIG"])
    config_path.write_text("database_uri: postgres://u:p@localhost/omnigent\n")
    cfg = load_server_config()
    expected = "postgresql+psycopg://u:p@localhost/omnigent"
    assert _resolve_database_url(cfg) == expected
    assert _resolve_config().database_url == expected


def test_migrate_only_cli_bypasses_startup_catch_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deploy.docker.entrypoint as entrypoint

    called: list[str] = []
    monkeypatch.setattr(sys, "argv", ["entrypoint.py", "--migrate-only"])
    monkeypatch.setattr(entrypoint, "main", lambda: called.append("main"))
    monkeypatch.setattr(entrypoint, "migrate_only", lambda: 0)

    assert entrypoint._cli() == 0
    assert called == []


def test_migrate_only_cli_rejects_bad_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    import deploy.docker.entrypoint as entrypoint

    monkeypatch.setattr(sys, "argv", ["entrypoint.py", "--unknown"])
    with pytest.raises(SystemExit) as exc_info:
        entrypoint._cli()
    assert exc_info.value.code == 2


def test_migrate_only_failure_returns_promptly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deploy.docker.entrypoint as entrypoint

    def _fail(_cfg: dict[str, object]) -> str:
        raise RuntimeError("migration failed")

    monkeypatch.setattr(entrypoint, "_resolve_database_url", _fail)
    started = time.monotonic()
    assert entrypoint.migrate_only() == 1
    assert time.monotonic() - started < 2


def test_main_exits_nonzero_when_migration_lock_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deploy.docker.entrypoint as entrypoint

    monkeypatch.setattr(
        entrypoint,
        "_resolve_config",
        lambda: SimpleNamespace(database_url="postgresql+psycopg://user:pass@db/omnigent"),
    )
    monkeypatch.setattr(
        entrypoint,
        "run_migrations",
        lambda _database_url: (_ for _ in ()).throw(TimeoutError("lock timeout")),
    )

    with pytest.raises(SystemExit) as exc_info:
        entrypoint.main()

    assert exc_info.value.code == 1


@pytest.mark.parametrize(
    ("boot_flag", "migration_calls"),
    [(None, 1), ("0", 0)],
)
def test_main_configures_bounded_reconnect_drain(
    monkeypatch: pytest.MonkeyPatch,
    boot_flag: str | None,
    migration_calls: int,
) -> None:
    """Docker boot bounds shutdown and leaves SSE as a reconnecting drop."""
    import uvicorn

    import deploy.docker.entrypoint as entrypoint

    if boot_flag is None:
        monkeypatch.delenv("OMNIGENT_RUN_MIGRATIONS_ON_BOOT", raising=False)
    else:
        monkeypatch.setenv("OMNIGENT_RUN_MIGRATIONS_ON_BOOT", boot_flag)
    monkeypatch.delenv("OMNIGENT_SERVER_SHUTDOWN_TIMEOUT_S", raising=False)
    config = SimpleNamespace(database_url="postgresql+psycopg://user:pass@db/omnigent")
    migration_seen: list[object] = []
    uvicorn_kwargs: dict[str, object] = {}

    monkeypatch.setattr(entrypoint, "_resolve_config", lambda: config)
    monkeypatch.setattr(
        entrypoint,
        "run_migrations",
        lambda database_url: migration_seen.append(database_url),
    )
    monkeypatch.setattr(
        entrypoint,
        "build_app",
        lambda _config: entrypoint._BuiltApp(app=object(), host="127.0.0.1", port=8000),
    )
    monkeypatch.setattr(
        uvicorn,
        "run",
        lambda _app, **kwargs: uvicorn_kwargs.update(kwargs),
    )

    entrypoint.main()

    assert len(migration_seen) == migration_calls
    source = inspect.getsource(entrypoint)
    assert "shutdown_all" not in source
    assert "[DONE]" not in source
    assert uvicorn_kwargs["timeout_graceful_shutdown"] == 20
    assert uvicorn_kwargs["ws_ping_interval"] == 30.0
    assert uvicorn_kwargs["ws_ping_timeout"] == 90.0


def test_main_allows_shutdown_timeout_override(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    import deploy.docker.entrypoint as entrypoint

    monkeypatch.setenv("OMNIGENT_RUN_MIGRATIONS_ON_BOOT", "0")
    monkeypatch.setenv("OMNIGENT_SERVER_SHUTDOWN_TIMEOUT_S", "7.5")
    monkeypatch.setattr(
        entrypoint,
        "_resolve_config",
        lambda: SimpleNamespace(database_url="postgresql+psycopg://user:pass@db/omnigent"),
    )
    monkeypatch.setattr(
        entrypoint,
        "build_app",
        lambda _config: entrypoint._BuiltApp(app=object(), host="127.0.0.1", port=8000),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(uvicorn, "run", lambda _app, **kwargs: captured.update(kwargs))

    entrypoint.main()

    assert captured["timeout_graceful_shutdown"] == 8


def test_fractional_shutdown_timeout_allows_cli_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env = os.environ.copy()
    env["OMNIGENT_SERVER_SHUTDOWN_TIMEOUT_S"] = "7.5"
    result = subprocess.run(
        [sys.executable, "-c", "import omnigent.cli"],
        cwd=Path(__file__).parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_migrate_only_uses_direct_migration_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deploy.docker.entrypoint as entrypoint
    from omnigent.db import utils

    class _Engine:
        def dispose(self) -> None:
            pass

    created: list[str] = []
    migrated: list[tuple[object, str]] = []
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@pooler/omnigent")
    monkeypatch.setenv("MIGRATION_DATABASE_URL", "postgres://user:pass@direct/omnigent")
    monkeypatch.setattr(entrypoint, "_resolve_database_url", lambda _cfg: "pooler")
    monkeypatch.setattr(
        "omnigent.server.server_config.load_server_config",
        dict,
    )
    monkeypatch.setattr(utils, "_create_engine", lambda url: created.append(url) or _Engine())
    monkeypatch.setattr(utils, "_get_current_db_revision", lambda _engine: None)
    monkeypatch.setattr(utils, "_get_head_db_revision", lambda _url: "head")
    monkeypatch.setattr(
        utils,
        "_run_migrations",
        lambda engine, url: migrated.append((engine, url)),
    )

    assert entrypoint.migrate_only() == 0
    expected = "postgresql+psycopg://user:pass@direct/omnigent"
    assert created == [expected]
    assert migrated == [(migrated[0][0], expected)]


@pytest.mark.parametrize(
    ("artifact_store_uri", "expected_type"),
    [
        ("s3://my-bucket/artifacts", S3ArtifactStore),
        (None, LocalArtifactStore),
    ],
)
def test_select_artifact_store(
    tmp_path: Path, artifact_store_uri: str | None, expected_type: type
) -> None:
    from deploy.docker.entrypoint import _ResolvedConfig, _select_artifact_store

    resolved = _ResolvedConfig(
        cfg={},
        database_url="postgresql://u:p@localhost/omnigent",
        artifact_dir=tmp_path,
        artifact_store_uri=artifact_store_uri,
        host="0.0.0.0",
        port=8000,
    )
    assert isinstance(_select_artifact_store(resolved), expected_type)


# ── routing wiring ────────────────────────────────────────────────────────
# A Docker deploy must honour its own `routing:` block rather than running on
# all-default knobs, so the settings that reach RuntimeCaps are the parsed ones.


def test_build_routing_carries_the_configured_settings() -> None:
    from deploy.docker.entrypoint import _build_routing

    cfg = {
        "routing": {
            "provider": "external",
            "base_url": "https://host/ai-gateway/routing/v1",
            "router_name": "task_v1",
            "model_prefix": ["databricks-", "system.ai."],
        }
    }
    client, settings = _build_routing(cfg, None)

    assert settings.model_prefixes == ("databricks-", "system.ai.")
    assert client is not None
    assert client._model_prefixes == ["databricks-", "system.ai."]


def test_build_routing_defaults_without_a_routing_block() -> None:
    from deploy.docker.entrypoint import _build_routing
    from omnigent.server.smart_routing import RoutingSettings

    client, settings = _build_routing({}, None)
    assert client is None
    assert settings == RoutingSettings()


@pytest.mark.parametrize(
    ("cfg", "expected_timeout"),
    [
        ({"execution_timeout": 86_400}, 86_400),
        ({}, 7_200),
    ],
)
def test_resolve_execution_timeout(cfg: dict[str, int], expected_timeout: int) -> None:
    from deploy.docker.entrypoint import _resolve_execution_timeout

    assert _resolve_execution_timeout(cfg) == expected_timeout
