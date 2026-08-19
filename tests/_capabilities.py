"""Small, behavior-based probes for host capabilities needed by tests."""

from __future__ import annotations

import errno
import subprocess
from dataclasses import dataclass
from functools import cache
from pathlib import Path


@dataclass(frozen=True)
class BwrapCapability:
    """Whether bubblewrap can create the namespace shape a test needs."""

    available: bool
    reason: str


@cache
def probe_bwrap(*, network: bool) -> BwrapCapability:
    """Probe bwrap with the exact namespaces used by the production path."""
    args = [
        "bwrap",
        "--die-with-parent",
        "--unshare-pid",
        "--unshare-uts",
        "--unshare-ipc",
    ]
    if network:
        args.append("--unshare-net")
    args += ["--new-session", "--ro-bind", "/", "/", "--", "true"]
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except FileNotFoundError:
        return BwrapCapability(False, "bwrap is not installed")
    except subprocess.TimeoutExpired:
        return BwrapCapability(False, "bwrap capability probe timed out")
    except OSError as exc:
        return BwrapCapability(False, f"bwrap capability probe failed: {exc}")
    if result.returncode == 0:
        return BwrapCapability(True, "available")
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    return BwrapCapability(False, detail)


@cache
def probe_bwrap_mounts() -> BwrapCapability:
    """Probe only the mount operations used by mount-only regression tests."""
    args = ["bwrap", "--ro-bind", "/usr", "/usr"]
    for path in ("/bin", "/lib", "/lib64"):
        if Path(path).exists():
            args += ["--ro-bind-try", path, path]
    args += ["--dev", "/dev", "--tmpfs", "/tmp", "--", "/usr/bin/true"]
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except FileNotFoundError:
        return BwrapCapability(False, "bwrap is not installed")
    except subprocess.TimeoutExpired:
        return BwrapCapability(False, "bwrap mount probe timed out")
    except OSError as exc:
        return BwrapCapability(False, f"bwrap mount probe failed: {exc}")
    if result.returncode == 0:
        return BwrapCapability(True, "available")
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    return BwrapCapability(False, detail)


def _is_permission_denial(exc: OSError) -> bool:
    """Return whether *exc* is specifically a DAC denial."""
    return isinstance(exc, PermissionError) or exc.errno in {errno.EACCES, errno.EPERM}


def probe_restricted_write(parent: Path) -> bool:
    """Return whether mode bits actually prevent creating a child."""
    locked = parent / ".permission-write-probe"
    locked.mkdir()
    locked.chmod(0o500)
    created = locked / "created"
    try:
        try:
            created.write_text("probe", encoding="utf-8")
        except OSError as exc:
            return _is_permission_denial(exc)
        return False
    finally:
        created.unlink(missing_ok=True)
        locked.chmod(0o700)
        locked.rmdir()


def probe_restricted_read(parent: Path) -> bool:
    """Return whether mode bits actually prevent reading a file."""
    unreadable = parent / ".permission-read-probe"
    unreadable.write_text("probe", encoding="utf-8")
    unreadable.chmod(0o000)
    try:
        try:
            unreadable.read_text(encoding="utf-8")
        except OSError as exc:
            return _is_permission_denial(exc)
        return False
    finally:
        unreadable.chmod(0o600)
        unreadable.unlink(missing_ok=True)
