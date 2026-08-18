"""Regression tests for fail-loud tool argument validation."""

from __future__ import annotations

import json

from omnigent.spec.types import AgentSpec
from omnigent.tools import ToolManager
from omnigent.tools.base import ToolContext
from omnigent.tools.schema_validation import validate_tool_arguments


def test_unknown_parameter_names_the_key_and_obvious_sibling() -> None:
    """The sys_session_create args/message mix-up must be actionable."""
    schema = {
        "type": "object",
        "properties": {"message": {"type": "string"}},
        "additionalProperties": False,
    }

    error = validate_tool_arguments("sys_session_create", {"args": "brief"}, schema)

    assert error == (
        "tool 'sys_session_create' has unknown parameter 'args'; did you mean 'message'?"
    )


def test_required_parameter_is_enforced() -> None:
    """A declared required field cannot be silently omitted."""
    error = validate_tool_arguments(
        "example",
        {},
        {"type": "object", "properties": {"query": {}}, "required": ["query"]},
    )

    assert error == "tool 'example' is missing required parameter 'query'"


def test_in_process_sys_session_create_rejects_args_before_invoke() -> None:
    """The original bad create call is refused by the in-process surface."""
    manager = ToolManager(AgentSpec(spec_version=1, spawn=True))

    result = manager.call_tool(
        "sys_session_create",
        json.dumps({"agent_id": "ag_worker", "args": "brief"}),
        ToolContext(task_id="task_test", agent_id="ag_parent"),
    )

    assert "unknown parameter 'args'" in result
    assert "did you mean 'message'" in result


def test_in_process_required_parameter_is_rejected_before_invoke() -> None:
    """A missing declared field is rejected on the same boundary."""
    manager = ToolManager(AgentSpec(spec_version=1))

    result = manager.call_tool(
        "sys_agent_get",
        json.dumps({}),
        ToolContext(task_id="task_test", agent_id="ag_parent"),
    )

    assert "missing required parameter 'session_id'" in result
