"""Durable server-wide settings with legacy file read-through.

The sharing-mode override, public-sharing flag, and OIDC allowed-domain
additions are stored in the shared application database so every replica sees
the same value. Existing files under :func:`resolve_data_dir` are imported on
first read or write, preserving settings from older single-node deployments.

The database table is declared here because these settings are optional to
embedded applications and must not require a new store dependency. Its
metadata is shared with the other Omnigent server tables, while the table
itself is created only by its Alembic migration.
"""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
import time
from pathlib import Path
from threading import Lock
from typing import Any

from sqlalchemy import Column, String, Table, Text, insert, select, update
from sqlalchemy.exc import IntegrityError

from omnigent.db.db_models import OmnigentBase
from omnigent.db.utils import get_or_create_engine, make_named_managed_session_maker
from omnigent.server.admin_list import resolve_data_dir
from omnigent.server.auth import SharingMode

logger = logging.getLogger(__name__)

_SHARING_MODE_FILE = "sharing_mode"
_PUBLIC_SHARING_FILE = "public_sharing"
_PUBLIC_FALSY = ("0", "false", "no", "off")

_SETTINGS_TABLE = Table(
    "omnigent_server_settings",
    OmnigentBase.metadata,
    Column("key", String(128), primary_key=True),
    Column("value", Text, nullable=False),
)
_session_makers: dict[str, Any] = {}
_database_lock = Lock()
_OVERRIDE_CACHE_TTL_S = 1.0
_override_cache_lock = Lock()
_override_cache: dict[tuple[str, str, str | None], tuple[float, str | None]] = {}


def resolve_sharing_mode_path() -> Path:
    """Path of the legacy admin sharing-mode override file."""
    return resolve_data_dir() / _SHARING_MODE_FILE


def resolve_public_sharing_path() -> Path:
    """Path of the legacy admin public-sharing override file."""
    return resolve_data_dir() / _PUBLIC_SHARING_FILE


def _read_override_text(path: Path) -> str | None:
    """Read a legacy override file without caching its contents."""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _write_override_text(path: Path, value: str) -> None:
    """Persist a legacy override atomically when no database is configured."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(value + "\n")
        os.replace(tmp, path)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _database_uri() -> str | None:
    """Return the URI used by the initialized application stores, if any."""
    try:
        from omnigent.runtime import get_agent_store

        storage_location = get_agent_store().storage_location
    except (RuntimeError, AttributeError):
        return None
    return storage_location if isinstance(storage_location, str) and storage_location else None


def _session_maker() -> Any | None:
    """Return the named session maker for the shared application database."""
    database_uri = _database_uri()
    if database_uri is None:
        return None
    with _database_lock:
        session_maker = _session_makers.get(database_uri)
        if session_maker is None:
            engine = get_or_create_engine(database_uri)
            session_maker = make_named_managed_session_maker(
                engine,
                query_name_prefix="omnigent.server_settings",
            )
            _session_makers[database_uri] = session_maker
    return session_maker


def _read_database_setting(
    key: str,
    legacy_path: Path,
    session_maker: Any,
) -> str | None:
    """Read a DB value, importing the legacy file when the key is absent."""
    try:
        with session_maker("read_setting") as session:
            value = session.execute(
                select(_SETTINGS_TABLE.c.value).where(_SETTINGS_TABLE.c.key == key)
            ).scalar_one_or_none()
            if value is not None:
                return str(value)

            legacy_value = _read_override_text(legacy_path)
            if legacy_value is None:
                return None
            session.execute(insert(_SETTINGS_TABLE).values(key=key, value=legacy_value))
            return legacy_value
    except IntegrityError:
        # Another replica may win the first-read migration race.
        with session_maker("read_setting_after_migration_race") as session:
            value = session.execute(
                select(_SETTINGS_TABLE.c.value).where(_SETTINGS_TABLE.c.key == key)
            ).scalar_one_or_none()
            return None if value is None else str(value)


def _write_database_setting(
    key: str,
    value: str,
    legacy_path: Path,
    session_maker: Any,
) -> None:
    """Import a legacy value if needed, then update the shared DB value."""
    _read_database_setting(key, legacy_path, session_maker)
    try:
        with session_maker("write_setting") as session:
            updated = session.execute(
                update(_SETTINGS_TABLE).where(_SETTINGS_TABLE.c.key == key).values(value=value)
            )
            if updated.rowcount == 0:
                session.execute(insert(_SETTINGS_TABLE).values(key=key, value=value))
    except IntegrityError:
        with session_maker("write_setting_after_race") as session:
            session.execute(
                update(_SETTINGS_TABLE).where(_SETTINGS_TABLE.c.key == key).values(value=value)
            )


def read_runtime_setting(key: str, legacy_path: Path) -> str | None:
    """Read a shared setting, falling back to its legacy file when needed."""
    session_maker = _session_maker()
    if session_maker is None:
        return _read_override_text(legacy_path)
    return _read_database_setting(key, legacy_path, session_maker)


def write_runtime_setting(key: str, value: str, legacy_path: Path) -> None:
    """Write a shared setting, or the legacy file for embedded file-only apps."""
    session_maker = _session_maker()
    if session_maker is None:
        _write_override_text(legacy_path, value)
    else:
        _write_database_setting(key, value, legacy_path, session_maker)
    _invalidate_runtime_setting_cache(key, legacy_path)


def _cached_runtime_setting(key: str, legacy_path: Path) -> str | None:
    """Read a runtime setting with a short TTL for request hot paths."""
    cache_key = (key, str(legacy_path), _database_uri())
    now = time.monotonic()
    with _override_cache_lock:
        cached = _override_cache.get(cache_key)
        if cached is not None and cached[0] > now:
            return cached[1]
    value = read_runtime_setting(key, legacy_path)
    with _override_cache_lock:
        _override_cache[cache_key] = (now + _OVERRIDE_CACHE_TTL_S, value)
    return value


def _invalidate_runtime_setting_cache(key: str, legacy_path: Path) -> None:
    """Drop all cached sources for a setting after a successful write."""
    path = str(legacy_path)
    with _override_cache_lock:
        for cache_key in tuple(_override_cache):
            if cache_key[:2] == (key, path):
                _override_cache.pop(cache_key, None)


def read_sharing_mode_override() -> SharingMode | None:
    """Return the admin-set sharing-mode override, or ``None`` when unset."""
    path = resolve_sharing_mode_path()
    raw = _cached_runtime_setting("sharing_mode", path)
    if not raw:
        return None
    try:
        return SharingMode(raw.lower())
    except ValueError:
        logger.warning("Ignoring unrecognized sharing_mode override %r", raw)
        return None


def write_sharing_mode_override(mode: SharingMode) -> None:
    """Persist the admin sharing-mode override."""
    path = resolve_sharing_mode_path()
    write_runtime_setting("sharing_mode", mode.value, path)
    _invalidate_runtime_setting_cache("sharing_mode", path)


def public_sharing_env_default() -> bool:
    """Boot default for public sharing from ``OMNIGENT_PUBLIC_SHARING``."""
    raw = os.environ.get("OMNIGENT_PUBLIC_SHARING")
    if not raw or not raw.strip():
        return True
    return raw.strip().lower() not in _PUBLIC_FALSY


def read_public_sharing_override() -> bool | None:
    """Return the admin-set public-sharing override, or ``None`` when unset."""
    path = resolve_public_sharing_path()
    raw = _cached_runtime_setting("public_sharing", path)
    if raw is None or raw == "":
        return None
    return raw.lower() not in _PUBLIC_FALSY


def write_public_sharing_override(enabled: bool) -> None:
    """Persist the admin public-sharing override."""
    path = resolve_public_sharing_path()
    write_runtime_setting("public_sharing", "on" if enabled else "off", path)
    _invalidate_runtime_setting_cache("public_sharing", path)
