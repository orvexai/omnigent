"""LLM-facing tool for reading and changing the per-host agent caps."""

from __future__ import annotations

from typing import Any

from omnigent.tools.base import Tool, ToolContext


class SysAgentLimitsTool(Tool):
    """
    Read, or change, how many agent sessions may run on one host.

    Called with no arguments it reports the caps in force. Supplying either
    limit changes it for the very next create — no restart — and also writes
    it to ``config.yaml`` so it survives one.

    The two dimensions exist because they fail differently: the overall cap
    protects the machine, while the per-CLI cap stops one toolchain (all
    Codex, say) consuming the whole budget by itself.
    """

    @classmethod
    def name(cls) -> str:
        """:returns: ``"sys_agent_limits"``."""
        return "sys_agent_limits"

    @classmethod
    def description(cls) -> str:
        """:returns: Human-readable description of the tool."""
        return (
            "Read or change the per-host caps on concurrent agent "
            "sessions. Call with no arguments to see the current limits "
            "and how they were set. Pass max_per_host and/or "
            "max_per_cli_per_host to change them: the new value applies "
            "to the very next sys_session_create (no restart needed) and "
            "is also written to config.yaml so it survives a restart. The "
            "response states explicitly whether it was persisted — if it "
            "was not, the change is runtime-only and WILL be lost when "
            "the host restarts. Use this when a create is refused with "
            "host_agent_limit_reached or cli_agent_limit_reached and the "
            "cap is genuinely too low; prefer closing finished sessions "
            "with sys_session_close first."
        )

    def get_schema(self) -> dict[str, Any]:
        """
        Return the OpenAI-format tool schema.

        :returns: Dict with ``"type": "function"`` and a ``"function"``
            sub-dict.
        """
        return {
            "type": "function",
            "function": {
                "name": SysAgentLimitsTool.name(),
                "description": SysAgentLimitsTool.description(),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "max_per_host": {
                            "type": "integer",
                            "minimum": 1,
                            "description": (
                                "New cap on total agent sessions per host, "
                                "e.g. 50. Omit to leave unchanged."
                            ),
                        },
                        "max_per_cli_per_host": {
                            "type": "integer",
                            "minimum": 1,
                            "description": (
                                "New cap on sessions of any ONE CLI "
                                "(codex, claude, ...) per host, e.g. 20. "
                                "Omit to leave unchanged."
                            ),
                        },
                    },
                    "required": [],
                    "additionalProperties": False,
                },
            },
        }

    def invoke(self, arguments: str, ctx: ToolContext) -> str:
        """
        Report or update the caps.

        :param arguments: JSON-encoded arguments string.
        :param ctx: Server-side execution context (unused).
        :returns: JSON describing the limits in force and, on a change,
            whether it was persisted.
        """
        from omnigent.runner.tool_dispatch import execute_agent_limits

        return execute_agent_limits(arguments)
