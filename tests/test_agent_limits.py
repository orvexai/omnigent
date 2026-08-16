"""Per-host agent caps: resolution, runtime override, and persistence."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

from omnigent import agent_limits
from omnigent.agent_limits import (
    DEFAULT_MAX_PER_CLI_PER_HOST,
    DEFAULT_MAX_PER_HOST,
    apply_limits,
    configured_limits,
    current_limits,
)


@pytest.fixture(autouse=True)
def _isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """
    Point config resolution at a temp dir and clear the runtime override.

    Without the redirect these tests would read — and ``apply_limits`` would
    WRITE — the developer's real ``~/.omnigent/config.yaml``. The override is
    process-global, so it must be cleared around every test too.

    :param tmp_path: Pytest temp dir.
    :param monkeypatch: Pytest monkeypatch fixture.
    :returns: The temp config home.
    """
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(tmp_path))
    # load_local_config reads ./.omnigent/config.yaml relative to cwd; run
    # from the temp dir so a real project config cannot leak in.
    monkeypatch.chdir(tmp_path)
    agent_limits.reset_for_tests()
    yield tmp_path
    agent_limits.reset_for_tests()


def _write_config(home: Path, payload: dict[str, object]) -> Path:
    """Write a config.yaml into *home* and return its path."""
    path = home / "config.yaml"
    path.write_text(yaml.safe_dump(payload))
    return path


def test_limits_default_when_unconfigured() -> None:
    """
    With no config, the built-in defaults apply.

    A missing config must not mean "no cap" — an absent file is the common
    case, and an unbounded default would leave the machine unprotected for
    every user who never configured anything.
    """
    limits = current_limits()

    assert limits.max_per_host == DEFAULT_MAX_PER_HOST
    assert limits.max_per_cli_per_host == DEFAULT_MAX_PER_CLI_PER_HOST


def test_limits_read_from_config(_isolated_config: Path) -> None:
    """Configured values win over the defaults."""
    _write_config(
        _isolated_config,
        {"agent_limits": {"max_per_host": 7, "max_per_cli_per_host": 3}},
    )

    limits = current_limits()

    assert limits.max_per_host == 7
    assert limits.max_per_cli_per_host == 3


@pytest.mark.parametrize(
    "bad_value",
    [0, -1, "twelve", None, True],
    ids=["zero", "negative", "string", "null", "bool"],
)
def test_unusable_configured_limit_falls_back_to_the_default(
    _isolated_config: Path,
    bad_value: object,
) -> None:
    """
    A malformed limit degrades to the default rather than disabling the cap.

    Zero or negative is the dangerous case: read literally it refuses EVERY
    create, turning a typo in a config file into a total outage. ``True`` is
    included because ``bool`` is an ``int`` subclass in Python and would
    otherwise slip through as a cap of 1.
    """
    _write_config(_isolated_config, {"agent_limits": {"max_per_host": bad_value}})

    assert current_limits().max_per_host == DEFAULT_MAX_PER_HOST


def test_apply_limits_takes_effect_immediately_and_persists(_isolated_config: Path) -> None:
    """
    A change applies now AND is written to config.yaml.

    Both halves matter: "no reload" is the point of the tool, and a change
    that vanished on the next restart would come back as a mysterious
    regression nobody connects to this call.
    """
    update = apply_limits(max_per_host=99)

    assert update.limits.max_per_host == 99
    assert update.persist_error is None
    assert update.persisted_path is not None
    # In force for the very next read, with no reload.
    assert current_limits().max_per_host == 99
    # And durable: the file now carries it.
    on_disk = yaml.safe_load(update.persisted_path.read_text())
    assert on_disk["agent_limits"]["max_per_host"] == 99
    # The untouched dimension keeps its value rather than being reset.
    assert on_disk["agent_limits"]["max_per_cli_per_host"] == DEFAULT_MAX_PER_CLI_PER_HOST


def test_apply_limits_preserves_unrelated_config_keys(_isolated_config: Path) -> None:
    """
    Writing a limit must not drop the rest of the user's config.

    The file is rewritten whole, so a naive dump would silently destroy
    every other setting the user has.
    """
    _write_config(_isolated_config, {"harness": {"default": "codex"}, "telemetry": False})

    update = apply_limits(max_per_cli_per_host=5)

    on_disk = yaml.safe_load(Path(str(update.persisted_path)).read_text())
    assert on_disk["harness"] == {"default": "codex"}
    assert on_disk["telemetry"] is False
    assert on_disk["agent_limits"]["max_per_cli_per_host"] == 5


def test_apply_limits_reports_a_failed_write_instead_of_hiding_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    An unwritable config yields a runtime-only change that SAYS it is one.

    The agent raised the cap to get unblocked, so the new value must still
    take effect — but reporting a clean success would promise durability
    that does not exist.
    """

    def _boom(*_args: object, **_kwargs: object) -> Path:
        raise OSError("read-only file system")

    monkeypatch.setattr(agent_limits, "save_global_config", _boom)

    update = apply_limits(max_per_host=11)

    # Applied regardless — the agent is unblocked.
    assert current_limits().max_per_host == 11
    # But honestly flagged as ephemeral.
    assert update.persisted_path is None
    assert "read-only file system" in str(update.persist_error)


@pytest.mark.parametrize("bad", [0, -3])
def test_apply_limits_rejects_a_non_positive_cap(bad: int) -> None:
    """
    A cap below 1 is refused rather than stored.

    Zero would refuse every subsequent create, and the agent that set it
    would have no working tool left to undo it with.
    """
    with pytest.raises(ValueError, match="positive integer"):
        apply_limits(max_per_host=bad)


def test_runtime_override_does_not_mask_a_later_config_edit() -> None:
    """
    Before any override, config edits are picked up without a restart.

    The override is deliberately not seeded from the file, so an operator
    editing config.yaml sees it take effect on the next create.
    """
    assert configured_limits().max_per_host == DEFAULT_MAX_PER_HOST
    assert current_limits() == configured_limits()
