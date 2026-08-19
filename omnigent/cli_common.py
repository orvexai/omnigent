"""Shared constants and guards for the CLI, importable without ``omnigent.cli``.

The native coding-agent subcommands live in :mod:`omnigent.cli_native`, which
``omnigent.cli`` imports at module load to register them on the ``cli`` group.
Click evaluates command decorators at import time, so any module-level name a
decorator references (``flag_value=``, help-string interpolation) must resolve
before the command object is built. Keeping those names here — in a leaf module
that imports nothing from ``omnigent.cli`` — lets both ``cli`` and ``cli_native``
import them without an import cycle.
"""

from __future__ import annotations

import click

from omnigent._platform import IS_WINDOWS, resolve_cli_binary

# Click ``flag_value`` for bare ``--resume`` (no arg). Must exist before any
# command's decorator evaluates.
RESUME_PICKER_SENTINEL = "__resume_picker__"

# Env var that force-enables native Claude startup timing marks. Referenced in a
# command's ``--profile-startup`` help string, so it is decorator-time state.
CLAUDE_STARTUP_PROFILE_ENV_VAR = "OMNIGENT_CLAUDE_STARTUP_PROFILE"


def reject_native_on_windows(harness: str) -> None:
    """Fail a native (tmux/PTY) harness command when Windows has no tmux.

    The ``omnigent claude`` / ``codex`` / ``cursor`` native wrappers drive a
    private tmux server and PTY. Windows has no native PTY, but a POSIX layer
    (e.g. msys2/Git Bash/WSL's tmux on PATH) provides a real tmux server that
    ``subprocess``-spawns and drives fine, so only reject when no ``tmux`` is
    resolvable at all — point users at the SDK harnesses / web UI instead of
    letting them hit a tmux crash.

    :param harness: The native command name, e.g. ``"claude"``.
    :raises click.ClickException: On Windows, when ``tmux`` isn't on PATH.
    """
    if IS_WINDOWS and resolve_cli_binary("tmux", env_var="OMNIGENT_TMUX_PATH") is None:
        raise click.ClickException(
            f"`omnigent {harness}` (native tmux/PTY terminal) needs `tmux` on "
            "PATH on Windows (install msys2, then `pacman -S tmux`). Otherwise use an "
            "SDK-based harness via `omnigent run <agent.yaml>` or the web UI."
        )
