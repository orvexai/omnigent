"""
Frozen-binary entry point for the standalone `omnigent` executable.

PyInstaller needs a real script to analyze, and pointing it at the
installed package's ``__main__.py`` would make it ambiguous whether the
repo tree or site-packages is the source of truth. This file is
deliberately trivial and imports nothing at module scope beyond the CLI,
so the analyzer's entry graph starts at exactly one place.

Built by ``.github/workflows/release-binaries.yml``.
"""

from __future__ import annotations

import multiprocessing


def _run() -> None:
    """Invoke the same console-script entry point the wheel installs."""
    from omnigent.cli import main

    main()


if __name__ == "__main__":
    # A --onefile binary re-executes itself for every child process, so
    # without this a spawned worker re-runs the CLI instead of the worker
    # body — the classic fork-bomb-on-Windows failure. Must precede any
    # other work.
    multiprocessing.freeze_support()
    _run()
