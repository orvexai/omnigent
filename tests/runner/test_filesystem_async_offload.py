"""Regression tests for blocking Git work in runner filesystem routes."""

from __future__ import annotations

import asyncio
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from omnigent.entities import DEFAULT_ENVIRONMENT_ID
from omnigent.inner.datamodel import OSEnvSandboxSpec, OSEnvSpec
from omnigent.inner.os_env import create_os_environment
from omnigent.runner import create_runner_app
from omnigent.runner.resource_registry import SessionResourceRegistry
from omnigent.runtime import filesystem_registry as filesystem_registry_module
from omnigent.runtime.filesystem_registry import GitFilesystemRegistry
from tests.runner.helpers import NullServerClient


class _SessionResponse:
    """Minimal server response containing a session workspace."""

    status_code = 200

    def __init__(self, workspace: Path) -> None:
        self._body = {
            "id": "conv_test",
            "agent_id": "agent_test",
            "created_at": 1,
            "workspace": str(workspace),
        }

    def json(self) -> dict[str, object]:
        """Return the fake session body."""
        return self._body


class _SessionServerClient(NullServerClient):
    """Server client returning a configured per-session workspace."""

    def __init__(self, workspace: Path) -> None:
        self._workspace = workspace

    async def get(self, url: str, **kwargs: Any) -> _SessionResponse | NullServerClient._Response:
        """Return the session body for the configured session."""
        del kwargs
        if url.endswith("/v1/sessions/conv_test"):
            return _SessionResponse(self._workspace)
        return await super().get(url)


class _SlowGitSubprocess:
    """Block selected Git calls until the test has observed loop progress."""

    def __init__(self, real_run: Any) -> None:
        self._real_run = real_run
        self.started = threading.Event()
        self.release = threading.Event()
        self.started_at: list[float] = []
        self.finished_count = 0

    def __call__(self, args: Any, *positional: Any, **kwargs: Any) -> Any:
        """Pause the Git operation while the caller checks event-loop ticks."""
        command = [str(part) for part in args]
        is_git_probe = command[:2] == ["git", "-C"] and "rev-parse" in command
        is_git_status = command[:2] == ["git", "status"]
        is_git_show = command[:2] == ["git", "show"]
        if is_git_probe or is_git_status or is_git_show:
            self.started_at.append(time.monotonic())
            self.started.set()
            self.release.wait(timeout=5)
        result = self._real_run(args, *positional, **kwargs)
        if is_git_probe or is_git_status or is_git_show:
            self.finished_count += 1
        return result


def _git_env() -> dict[str, str]:
    """Return Git identity settings for temporary test repositories."""
    return {
        **os.environ,
        "GIT_AUTHOR_NAME": "Test",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "Test",
        "GIT_COMMITTER_EMAIL": "test@example.com",
    }


def _make_git_workspace(root: Path, *, committed_file: bool = False) -> None:
    """Create a real Git worktree, optionally with a committed file."""
    root.mkdir()
    env = _git_env()
    subprocess.run(["git", "init", str(root)], check=True, capture_output=True, env=env)
    if committed_file:
        (root / "tracked.txt").write_text("before\n")
        subprocess.run(["git", "add", "tracked.txt"], cwd=root, check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "initial"],
            cwd=root,
            check=True,
            capture_output=True,
            env=env,
        )


def _make_app(
    workspace: Path,
    *,
    server_client: NullServerClient | _SessionServerClient | None = None,
) -> Any:
    """Create a runner app with an OS environment rooted at *workspace*."""
    os_env = create_os_environment(
        OSEnvSpec(
            type="caller_process",
            cwd=str(workspace),
            sandbox=OSEnvSandboxSpec(type="none"),
        )
    )
    assert os_env is not None
    resource_registry = SessionResourceRegistry()
    resource_registry._primary_envs["conv_test"] = os_env
    return create_runner_app(
        resource_registry=resource_registry,
        runner_workspace=workspace,
        server_client=server_client or NullServerClient(),  # type: ignore[arg-type]
    )


async def _assert_request_keeps_loop_responsive(
    app: Any,
    path: str,
    *,
    expected_slow_calls: int,
) -> None:
    """Assert a slow Git subprocess does not stop the request's event loop."""
    slow_git = _SlowGitSubprocess(subprocess.run)
    original_run = filesystem_registry_module.subprocess.run
    filesystem_registry_module.subprocess.run = slow_git  # type: ignore[assignment]
    ticks: list[float] = []
    release_at: list[float] = []
    monitor_errors: list[BaseException] = []

    def _release_after_delay() -> None:
        try:
            for call_number in range(expected_slow_calls):
                deadline = time.monotonic() + 2
                while len(slow_git.started_at) <= call_number and time.monotonic() < deadline:
                    time.sleep(0.001)
                if len(slow_git.started_at) <= call_number:
                    raise AssertionError("the request did not reach the expected slow Git call")
                time.sleep(0.1)
                release_at.append(time.monotonic())
                slow_git.release.set()
                while slow_git.finished_count <= call_number and time.monotonic() < deadline + 2:
                    time.sleep(0.001)
                slow_git.release.clear()
        except BaseException as exc:  # pragma: no cover - surfaced below
            monitor_errors.append(exc)
            slow_git.release.set()

    async def _tick() -> None:
        while True:
            ticks.append(time.monotonic())
            await asyncio.sleep(0.01)

    release_thread = threading.Thread(target=_release_after_delay)
    tick_task = asyncio.create_task(_tick())
    try:
        transport = httpx.ASGITransport(app=app)
        release_thread.start()
        async with httpx.AsyncClient(transport=transport, base_url="http://runner") as client:
            response = await asyncio.wait_for(client.get(path), timeout=4)
    finally:
        slow_git.release.set()
        release_thread.join(timeout=3)
        tick_task.cancel()
        await asyncio.gather(tick_task, return_exceptions=True)
        filesystem_registry_module.subprocess.run = original_run  # type: ignore[assignment]

    assert not monitor_errors, f"Git monitor failed: {monitor_errors[0]}"
    assert len(release_at) == expected_slow_calls
    assert len(slow_git.started_at) >= expected_slow_calls
    for start, release in zip(slow_git.started_at, release_at, strict=True):
        assert any(start <= tick < release for tick in ticks), (
            "the request event loop stopped while Git was blocked"
        )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_changed_files_and_diff_git_calls_are_offloaded(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """Changed-file, changed-file lookup, and baseline Git calls share the offload guard."""
    monkeypatch.setattr(GitFilesystemRegistry, "_enable_untracked_cache", lambda self: None)
    workspace = tmp_path / "repo"
    _make_git_workspace(workspace, committed_file=True)
    (workspace / "tracked.txt").write_text("after\n")
    (workspace / "untracked.txt").write_text("new\n")
    app = _make_app(workspace)

    changes_path = (
        f"/v1/sessions/conv_test/resources/environments/{DEFAULT_ENVIRONMENT_ID}/changes"
    )
    diff_path = (
        f"/v1/sessions/conv_test/resources/environments/{DEFAULT_ENVIRONMENT_ID}/diff/tracked.txt"
    )
    await _assert_request_keeps_loop_responsive(app, changes_path, expected_slow_calls=1)
    await _assert_request_keeps_loop_responsive(app, diff_path, expected_slow_calls=2)


@pytest.mark.asyncio
async def test_session_registry_creation_is_offloaded(tmp_path: Path, monkeypatch: Any) -> None:
    """A per-session Git registry probe does not block the runner event loop."""
    monkeypatch.setattr(GitFilesystemRegistry, "_enable_untracked_cache", lambda self: None)
    runner_workspace = tmp_path / "runner"
    session_workspace = tmp_path / "session"
    _make_git_workspace(runner_workspace)
    _make_git_workspace(session_workspace)
    app = _make_app(
        runner_workspace,
        server_client=_SessionServerClient(session_workspace),
    )
    changes_path = (
        f"/v1/sessions/conv_test/resources/environments/{DEFAULT_ENVIRONMENT_ID}/changes"
    )
    await _assert_request_keeps_loop_responsive(app, changes_path, expected_slow_calls=2)


@pytest.mark.asyncio
async def test_plain_workspace_changes_fall_back_without_500(tmp_path: Path) -> None:
    """A non-git workspace uses edit tracking and serves an empty change list."""
    app = _make_app(tmp_path)
    path = f"/v1/sessions/conv_test/resources/environments/{DEFAULT_ENVIRONMENT_ID}/changes"

    async def _request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://runner") as client:
            return await client.get(path)

    response = await _request()
    assert response.status_code == 200, response.text
    assert response.json()["data"] == []
