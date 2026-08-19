"""Central, dependency-light platform flags and OS-portability helpers.

omnigent grew up on Linux/macOS and bakes a number of POSIX assumptions into
process management, shells, and user identity. This module is the single place
that answers "which OS are we on?" and provides the small portable primitives
that the rest of the package uses instead of branching on :data:`os.name`
ad hoc.

Keep this module import-cheap and free of heavy/optional dependencies: it is
imported very early (and on Windows it must import before any POSIX-only module
would otherwise crash), so it must never pull in ``fcntl``/``termios``/``pty``
or anything platform-specific at module top level.
"""

from __future__ import annotations

import getpass
import hashlib
import logging
import os
import shutil
import sys
from collections.abc import Callable, Mapping
from contextlib import suppress
from pathlib import Path

_logger = logging.getLogger(__name__)


# Common global install dirs for npm/homebrew CLIs, probed when a binary isn't
# on ``PATH``. The host daemon snapshots ``PATH`` at spawn and never refreshes
# it, so a CLI installed into an nvm/npm/homebrew bin dir that only interactive
# shell init puts on ``PATH`` is invisible to ``shutil.which``.
def _cli_fallback_dirs() -> tuple[Path, ...]:
    """Return the global install dirs to probe when a CLI isn't on ``PATH``.

    Includes nvm's version-specific bin dirs (``~/.nvm/versions/node/*/bin``),
    where npm global installs land under nvm — the common driver of a CLI that
    a foreground shell sees but the daemon's frozen ``PATH`` doesn't.
    """
    home = Path.home()
    dirs = [
        home / ".local" / "bin",
        Path("/usr/local/bin"),
        Path("/opt/homebrew/bin"),
        home / ".npm-global" / "bin",
    ]
    if os.name == "nt":
        # msys2's default install root ships tmux; a native Windows Python
        # daemon has no POSIX PATH entries of its own, so probe it directly
        # rather than requiring the user to add it to the system PATH.
        dirs.append(Path("C:/msys64/usr/bin"))
    # nvm keeps global bins per Node version; newest first so a current install
    # wins over a stale one. Sort by parsed numeric version (so v10 > v9, not
    # the lexicographic order in which "v10" < "v9").
    nvm_versions = home / ".nvm" / "versions" / "node"
    with suppress(OSError):
        version_dirs = [p for p in nvm_versions.iterdir() if p.is_dir()]
        version_dirs.sort(key=lambda p: _parse_node_version(p.name), reverse=True)
        dirs.extend(p / "bin" for p in version_dirs)
    return tuple(dirs)


def _parse_node_version(name: str) -> tuple[int, ...]:
    """Parse an nvm version dir name (e.g. ``"v20.5.0"``) into a sortable tuple.

    Non-numeric or malformed names sort lowest (empty tuple) so real versions
    win over anything unparseable.
    """
    parts = name.lstrip("v").split(".")
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        return ()


def resolve_cli_binary(
    name: str,
    *,
    env_var: str | None = None,
    which: Callable[[str], str | None] | None = None,
) -> str | None:
    """Resolve a CLI binary that may live off the process ``PATH``.

    Checks an optional ``env_var`` override first (an explicit path or a name
    on ``PATH``), then ``PATH`` via :func:`shutil.which`, then a ladder of
    common global install dirs (:func:`_cli_fallback_dirs`). This survives the
    host daemon's frozen ``PATH``, which omits nvm/npm/homebrew bin dirs that
    only interactive shell init adds. Returns ``None`` when none resolve; the
    caller decides whether that's fatal.

    :param name: The binary name, e.g. ``"codex"`` or ``"claude"``.
    :param env_var: Optional env var holding an override path/name, e.g.
        ``"OMNIGENT_CODEX_PATH"``.
    :param which: PATH-lookup hook, defaulting to :func:`shutil.which`. The
        native harness resolvers thread their own test seam through here; the
        fallback ladder always uses the real filesystem.
    :returns: An absolute path to the executable, or ``None``.
    """
    which = shutil.which if which is None else which
    if env_var:
        override = os.environ.get(env_var, "").strip()
        if override:
            resolved = which(override)
            if resolved:
                return resolved
            if os.access(override, os.X_OK) and os.path.isfile(override):
                return override
            # A set-but-unresolvable override (typo, moved/non-executable
            # binary) silently falling through to PATH would launch a
            # *different* binary than intended — warn so the misconfig surfaces.
            _logger.warning(
                "%s=%r does not resolve to an executable file; falling back to PATH for %r.",
                env_var,
                override,
                name,
            )
    on_path = which(name)
    if on_path is not None:
        return on_path
    for directory in _cli_fallback_dirs():
        # ``shutil.which`` rather than probing ``directory / name`` directly: it
        # applies PATHEXT, without which a Windows install is invisible here —
        # msys2 ships ``tmux.exe``, so ``directory / "tmux"`` never exists and
        # the whole ladder silently returns nothing. On POSIX this is equivalent
        # to the previous is-file-and-executable check.
        found = shutil.which(name, path=str(directory))
        if found is not None:
            return found
    return None


#: True on native Windows (cmd/PowerShell), i.e. ``os.name == "nt"``. This is
#: *not* true under WSL, where Python reports a Linux platform.
IS_WINDOWS = os.name == "nt"
#: True on any POSIX host (Linux, macOS, BSD, WSL).
IS_POSIX = os.name == "posix"
#: True on Linux specifically (the only platform with bwrap + seccomp).
IS_LINUX = sys.platform.startswith("linux")
#: True on macOS specifically (the seatbelt sandbox platform).
IS_DARWIN = sys.platform == "darwin"

#: Non-sensitive Windows environment variables that a spawned omnigent
#: subprocess needs to function, for env-passthrough allowlists that otherwise
#: assume POSIX names. Python uppercases env keys on Windows, so these match
#: ``os.environ`` as stored; they are absent on POSIX, so including them in an
#: allowlist is a no-op there (only present vars pass through).
#:
#: - ``SYSTEMROOT`` is MANDATORY: Winsock loads its providers from
#:   ``%SystemRoot%\system32\mswsock.dll``, so a child without it dies at
#:   ``import asyncio`` with ``WinError 10106`` (WSAEPROVIDERFAILEDINIT).
#: - ``USERPROFILE`` / ``HOMEDRIVE`` / ``HOMEPATH`` let ``Path.home()`` /
#:   ``expanduser("~")`` resolve (the Windows analog of POSIX ``HOME``).
#: - ``APPDATA`` / ``LOCALAPPDATA`` are where Windows apps (keyring, pip, npm,
#:   …) keep per-user config and cache.
#: - The rest let a Windows process and shell resolve binaries normally.
#:
#: All are path/identity constants, not credentials — consistent with POSIX
#: ``HOME``/``PATH`` already being allowed.
WINDOWS_ENV_PASSTHROUGH: tuple[str, ...] = (
    "SYSTEMROOT",
    "SYSTEMDRIVE",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_IDENTIFIER",
    "PROCESSOR_LEVEL",
    "PROCESSOR_REVISION",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "APPDATA",
    "LOCALAPPDATA",
)


def to_posix_path(path: str | Path) -> str:
    """
    Render a Windows path in the POSIX form an msys2/Cygwin program expects.

    ``C:\\Users\\me\\x`` becomes ``/c/Users/me/x``. A path that is already
    POSIX comes back with separators normalized only, so this is safe to
    apply unconditionally.

    Needed wherever a path is handed to *tmux itself* on Windows: tmux is an
    msys2 binary that does not recognize ``C:\\...`` as absolute, and silently
    resolves it against its own cwd instead — e.g. ``load-buffer`` reported
    ``No such file or directory: /home/me/repo/C:\\Users\\...\\paste.bin``.
    Arguments consumed by the *pane's* program (a native Windows ``claude.exe``
    or ``codex.exe``) must keep their Windows form.

    :param path: The path to convert, e.g. ``Path("C:/Temp/paste.bin")``.
    :returns: A POSIX-style path string.
    """
    drive, rest = os.path.splitdrive(str(path))
    if len(drive) == 2 and drive[1] == ":":
        return f"/{drive[0].lower()}{rest.replace(os.sep, '/')}"
    return str(path).replace(os.sep, "/")


def tmux_spawn_env(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """
    Build the environment for spawning msys2 ``tmux``, with globbing disabled.

    Cygwin reconstructs a child's argv from the Windows command line and
    brace-expands unquoted arguments, so a tmux format specifier such as
    ``#{pane_dead}`` reaches tmux as the literal ``#pane_dead``. tmux then
    echoes that string back instead of the value, and every liveness probe
    silently reads "alive" — a dead pane is never detected. ``MSYS=noglob``
    turns that expansion off. (A format containing a space happens to survive
    unmangled, which is why only some probes were affected.)

    Appended to any existing ``MSYS`` value rather than replacing it: that
    variable also carries unrelated settings such as ``winsymlinks``. No-op
    off Windows, so callers can apply it unconditionally.

    :param env: Base environment; defaults to :data:`os.environ`.
    :returns: A copy of *env* safe for spawning tmux.
    """
    base = dict(os.environ if env is None else env)
    if not IS_WINDOWS:
        return base
    existing = base.get("MSYS", "").strip()
    if "noglob" not in existing.split():
        base["MSYS"] = f"{existing} noglob".strip()
    return base


def default_shell_argv(command: str) -> list[str]:
    """
    Build the argv to run ``command`` through the host's default shell.

    On POSIX this mirrors the long-standing behavior: prefer ``bash`` with
    ``--noprofile --norc`` (skip user rc files for a predictable environment),
    falling back to ``sh -c``. On Windows there is no ``/bin/sh``; route through
    ``cmd.exe`` (``%COMSPEC%``) with ``/c``.

    :param command: The shell command string to execute.
    :returns: An argv list suitable for :func:`subprocess.Popen` (no
        ``shell=True`` needed).
    """
    if IS_WINDOWS:
        comspec = os.environ.get("COMSPEC", "cmd.exe")
        return [comspec, "/c", command]
    import shutil

    bash = shutil.which("bash")
    if bash:
        return [bash, "--noprofile", "--norc", "-c", command]
    sh = shutil.which("sh") or "/bin/sh"
    return [sh, "-c", command]


#: Interactive shells we honor from ``$SHELL`` for a user terminal. Anything
#: outside this set (or a ``$SHELL`` that doesn't resolve on PATH) falls back to
#: bash for a predictable pane.
_KNOWN_INTERACTIVE_SHELLS = frozenset({"bash", "zsh", "fish", "sh", "dash", "ksh", "tcsh"})

#: Mainstream interactive shells we proactively offer as launch choices (the
#: "New shell" picker), in display order. The user's ``$SHELL`` is always
#: offered first regardless (see :func:`installed_interactive_shells`); this is
#: the set of well-known alternatives we surface beyond it.
_OFFERED_INTERACTIVE_SHELLS = ("bash", "zsh", "fish")


def default_interactive_shell() -> str:
    """
    Basename of the user's login shell for an interactive terminal.

    Reads ``$SHELL`` and keeps its basename when it names a known shell that
    resolves on PATH; otherwise falls back to ``"bash"``. Returns a basename
    (not the absolute ``$SHELL`` path) so it stays PATH-resolvable when the
    terminal launches under a runner on a different host than the one that read
    the env.

    :returns: A shell basename such as ``"zsh"``, ``"fish"``, or ``"bash"``.
    """
    if IS_WINDOWS:
        # Native tmux/PTY terminals are unsupported on Windows anyway.
        return "bash"
    import shutil

    name = os.path.basename(os.environ.get("SHELL", "")).strip()
    if name in _KNOWN_INTERACTIVE_SHELLS and shutil.which(name):
        return name
    return "bash"


def installed_interactive_shells() -> list[str]:
    """
    Ordered, deduped shell basenames to offer for a new interactive terminal.

    The user's login shell (:func:`default_interactive_shell`) comes first — so
    the "New shell" affordance can treat entry ``[0]`` as the click default —
    followed by any mainstream alternatives (bash/zsh/fish) that resolve on
    PATH. Always non-empty (the default is always present, and bash is the
    ultimate fallback).

    :returns: Basenames such as ``["zsh", "bash", "fish"]`` — the default first.
    """
    ordered = [default_interactive_shell()]
    if IS_WINDOWS:
        # Native tmux/PTY terminals are unsupported on Windows anyway; the lone
        # bash default from above is all we can meaningfully offer.
        return ordered
    import shutil

    for name in _OFFERED_INTERACTIVE_SHELLS:
        if name not in ordered and shutil.which(name):
            ordered.append(name)
    return ordered


def stable_user_id() -> str:
    """
    A stable, filesystem-safe token identifying the current OS user.

    Used to namespace per-user scratch directories (e.g.
    ``omnigent-<id>`` / ``claude-<id>`` under the temp dir). On POSIX this is
    the numeric uid, matching historical behavior. Windows has no ``getuid``;
    derive a short hex digest from the login name so the value is stable across
    runs and safe to embed in a path.

    The digest is for path namespacing only — not security — so ``getuser``'s
    value never needs to be recoverable or collision-proof against an
    adversary; it just needs to be stable and filesystem-safe. SHA-256 with
    ``usedforsecurity=False`` documents that intent (and avoids flagging SHA-1).

    :returns: A short string with no path separators or shell-special chars.
    """
    if IS_POSIX and hasattr(os, "getuid"):
        return str(os.getuid())
    try:
        name = getpass.getuser()
    except (OSError, KeyError, ModuleNotFoundError):
        name = os.environ.get("USERNAME") or os.environ.get("USER") or "user"
    return hashlib.sha256(name.encode("utf-8"), usedforsecurity=False).hexdigest()[:12]


def resolve_repo_symlink(path: Path) -> Path:
    """
    Follow a Git symlink that a no-symlink Windows checkout left as a text file.

    On Windows with ``core.symlinks=false`` (the default when Developer Mode is
    off and Git was not run elevated), Git materializes a repository symlink as a
    *regular file* whose entire content is the link target — e.g. the checked-out
    ``omnigent/resources/examples/polly`` is a 23-byte file containing
    ``../../../examples/polly`` rather than a link to that directory. Code that
    expects to open the linked directory then reads this stub instead (the
    symptom: ``expected YAML mapping at top level, got str``).

    Detect that exact shape — a small, single-line regular file whose content,
    resolved relative to the stub's parent, names an existing path — and return
    the real target. Everything else (real directories, real symlinks, genuine
    single-file specs, multi-line or unresolvable content) is returned
    unchanged. No-op off Windows, where the symlink is followed natively.

    :param path: The path as resolved from packaged resources.
    :returns: The dereferenced target on Windows when *path* is a Git-symlink
        stub, otherwise *path* unchanged.
    """
    if not IS_WINDOWS:
        return path
    try:
        if not path.is_file() or path.stat().st_size > 4096:
            return path
        body = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return path
    target = body.strip()
    if not target or "\n" in target:
        return path
    candidate = path.parent / target
    if candidate.exists():
        return candidate.resolve()
    return path
