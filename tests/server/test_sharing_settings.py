"""Tests for database-backed server settings and legacy-file migration."""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent.runtime import init as init_runtime
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.admin_list import AdminList
from omnigent.server.auth import SharingMode
from omnigent.server.oidc_access import OidcAdmissionPolicy
from omnigent.server.sharing_settings import (
    read_public_sharing_override,
    read_sharing_mode_override,
    write_public_sharing_override,
    write_runtime_setting,
    write_sharing_mode_override,
)
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.conversation_store.sqlalchemy_store import (
    SqlAlchemyConversationStore,
)


def _reinitialize_runtime(db_uri: str, tmp_path: Path) -> None:
    """Install fresh store instances, like a second server process."""
    artifact_store = LocalArtifactStore(str(tmp_path / "restarted-artifacts"))
    init_runtime(
        conversation_store=SqlAlchemyConversationStore(db_uri),
        agent_store=SqlAlchemyAgentStore(db_uri),
        agent_cache=AgentCache(
            artifact_store=artifact_store,
            cache_dir=tmp_path / "restarted-cache",
        ),
    )


def _clear_settings_process_state() -> None:
    """Drop only process-local session/table setup, never setting values."""
    from omnigent.server import sharing_settings

    sharing_settings._session_makers.clear()


def test_sharing_settings_survive_restart_and_second_instance(
    runtime_init: None,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh settings accessor sees values written by an earlier instance."""
    monkeypatch.setenv("OMNIGENT_ADMIN_CREDENTIALS_PATH", str(tmp_path / "data" / "admins"))
    write_sharing_mode_override(SharingMode.READ_ONLY)
    write_public_sharing_override(False)

    _clear_settings_process_state()
    _reinitialize_runtime(db_uri, tmp_path)

    assert read_sharing_mode_override() == SharingMode.READ_ONLY
    assert read_public_sharing_override() is False


def test_preexisting_on_disk_settings_are_migrated(
    runtime_init: None,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy files are imported and remain effective after those files vanish."""
    data_dir = tmp_path / "data"
    monkeypatch.setenv("OMNIGENT_ADMIN_CREDENTIALS_PATH", str(data_dir / "admins"))
    data_dir.mkdir()
    (data_dir / "sharing_mode").write_text("restricted_read_only\n")
    (data_dir / "public_sharing").write_text("off\n")

    assert read_sharing_mode_override() == SharingMode.RESTRICTED_READ_ONLY
    assert read_public_sharing_override() is False
    (data_dir / "sharing_mode").unlink()
    (data_dir / "public_sharing").unlink()
    _clear_settings_process_state()

    assert read_sharing_mode_override() == SharingMode.RESTRICTED_READ_ONLY
    assert read_public_sharing_override() is False


def test_default_oidc_domain_additions_are_shared_and_migrated(
    runtime_init: None,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default-path OIDC additions use the shared DB after file migration."""
    data_dir = tmp_path / "data"
    monkeypatch.setenv("OMNIGENT_ADMIN_CREDENTIALS_PATH", str(data_dir / "admins"))
    data_dir.mkdir()
    (data_dir / "allowed_domains").write_text("example.com\n")
    admins = data_dir / "admins"
    admins.write_text("")
    policy = OidcAdmissionPolicy(
        env_allowed_domains=None,
        domains_file_path=data_dir / "allowed_domains",
        admin_list=AdminList(admins),
    )

    assert policy.effective_domains() == frozenset({"example.com"})
    (data_dir / "allowed_domains").unlink()
    _clear_settings_process_state()
    _reinitialize_runtime(db_uri, tmp_path)

    restarted_policy = OidcAdmissionPolicy(
        env_allowed_domains=None,
        domains_file_path=data_dir / "allowed_domains",
        admin_list=AdminList(admins),
    )
    assert restarted_policy.effective_domains() == frozenset({"example.com"})


def test_explicit_oidc_domains_path_stays_file_backed(
    runtime_init: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit OIDC path bypasses the shared default-domain setting."""
    data_dir = tmp_path / "data"
    admins = data_dir / "admins"
    explicit_path = tmp_path / "explicit_allowed_domains"
    monkeypatch.setenv("OMNIGENT_ADMIN_CREDENTIALS_PATH", str(admins))
    monkeypatch.setenv("OMNIGENT_OIDC_ALLOWED_DOMAINS_PATH", str(explicit_path))
    admins.parent.mkdir()
    admins.write_text("")
    explicit_path.write_text("explicit.example\n")
    write_runtime_setting("oidc_allowed_domains", "shared.example\n", data_dir / "allowed_domains")

    policy = OidcAdmissionPolicy(
        env_allowed_domains=None,
        domains_file_path=explicit_path,
        admin_list=AdminList(admins),
    )

    assert policy.effective_domains() == frozenset({"explicit.example"})
