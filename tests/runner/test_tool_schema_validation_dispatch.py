"""Regression tests for runner-side schema enforcement."""

from __future__ import annotations

from typing import Any

import pytest
from mcp.types import Tool as McpToolDef

from omnigent.runner import mcp_manager as _mcp_manager_module
from omnigent.runner import tool_dispatch as _tool_dispatch
from omnigent.runner.mcp_manager import RunnerMcpManager
from omnigent.spec.types import AgentSpec, MCPServerConfig


@pytest.mark.asyncio
async def test_sys_session_create_args_is_rejected_before_proxy_dispatch() -> None:
    """The original args/message typo cannot create an idle child via the proxy."""

    class _NeverCalledProxy:
        async def call_tool(
            self,
            _spec: AgentSpec,
            _tool_name: str,
            _arguments: dict[str, Any],
        ) -> str:
            raise AssertionError("invalid sys_session_create reached the proxy")

    output = await _tool_dispatch.execute_tool(
        tool_name="sys_session_create",
        arguments='{"agent_id":"ag_worker","args":"brief"}',
        agent_spec=AgentSpec(spec_version=1, spawn=True),
        conversation_id="conv_parent",
        mcp_manager=_NeverCalledProxy(),
    )

    assert "unknown parameter 'args'" in output
    assert "did you mean 'message'" in output


@pytest.mark.asyncio
async def test_dispatch_schema_manager_is_cached_per_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Schema validation must not rebuild the full manager for each call."""
    construction_count = 0
    real_manager = _tool_dispatch.ToolManager

    class _CountingToolManager(real_manager):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            nonlocal construction_count
            construction_count += 1
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(_tool_dispatch, "ToolManager", _CountingToolManager)
    _tool_dispatch._tool_schema_cache.clear()
    spec = AgentSpec(spec_version=1, spawn=True)

    for _ in range(2):
        output = await _tool_dispatch.execute_tool(
            tool_name="sys_session_create",
            arguments='{"agent_id":"ag_worker","args":"brief"}',
            agent_spec=spec,
        )
        assert "unknown parameter 'args'" in output

    assert construction_count == 1


@pytest.mark.asyncio
async def test_direct_mcp_manager_rejects_unknown_parameter_before_server_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct MCP dispatch applies the discovered inputSchema before calling."""
    calls: list[tuple[str, dict[str, Any]]] = []

    class _Connection:
        def __init__(self, **_: Any) -> None:
            pass

        async def connect(self) -> list[McpToolDef]:
            return [
                McpToolDef(
                    name="create",
                    description="create",
                    inputSchema={
                        "type": "object",
                        "properties": {"message": {"type": "string"}},
                        "additionalProperties": False,
                    },
                )
            ]

        async def close(self) -> None:
            pass

        async def call_tool(self, name: str, arguments: dict[str, Any], **_: Any) -> str:
            calls.append((name, arguments))
            return "created"

    monkeypatch.setattr(_mcp_manager_module, "McpServerConnection", _Connection)
    spec = AgentSpec(
        spec_version=1,
        name="test-agent",
        mcp_servers=[MCPServerConfig(name="worker", transport="http", url="http://mcp/worker")],
    )
    manager = RunnerMcpManager()
    try:
        with pytest.raises(ValueError, match=r"unknown parameter 'args'.*message"):
            await manager.call_tool(spec, "worker__create", {"args": "brief"})
    finally:
        await manager.shutdown()

    assert calls == []
