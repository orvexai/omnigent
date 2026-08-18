"""Shared fixtures for tools tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent.spec.types import SkillSpec
from omnigent.tools.base import ToolContext


@pytest.fixture()
def tool_ctx() -> ToolContext:
    """
    Dummy :class:`ToolContext` for tool tests that don't
    depend on specific task/agent identity.

    :returns: A :class:`ToolContext` with placeholder IDs.
    """
    return ToolContext(task_id="task_test", agent_id="agent_test")


@pytest.fixture(autouse=True)
def _isolate_host_skill_discovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Keep tool tests independent of host and checkout skill trees."""
    from omnigent.spec import parser

    isolated_root = tmp_path / "host-workspace"
    isolated_root.mkdir()
    isolated_home = tmp_path / "home"
    isolated_home.mkdir()
    monkeypatch.setenv("HOME", str(isolated_home))

    discover_host_skills = parser.discover_host_skills

    def discover_from_isolated_roots(
        _agent_root: Path,
        skills_filter: str | list[str],
    ) -> list[SkillSpec]:
        return discover_host_skills(isolated_root, skills_filter)

    monkeypatch.setattr(parser, "discover_host_skills", discover_from_isolated_roots)
