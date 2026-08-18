"""Small, fail-loud checks for the executable part of tool schemas."""

from __future__ import annotations

from difflib import get_close_matches
from typing import Any


def validate_tool_arguments(
    tool_name: str,
    arguments: dict[str, Any],
    schema: dict[str, Any],
) -> str | None:
    """Return an actionable schema error, or ``None`` when arguments pass.

    Tool schemas are provider-facing JSON Schema, so this deliberately enforces
    only the contract bits Omnigent relies on at dispatch time: ``required``
    and ``additionalProperties: false``. Other JSON Schema keywords remain
    advisory until the dispatchers can support them consistently.
    """
    return _validate_object(tool_name, arguments, schema, path="")


def _validate_object(
    tool_name: str,
    value: dict[str, Any],
    schema: dict[str, Any],
    *,
    path: str,
) -> str | None:
    properties = schema.get("properties")
    properties = properties if isinstance(properties, dict) else {}

    required = schema.get("required")
    if isinstance(required, list):
        for name in required:
            if isinstance(name, str) and name not in value:
                return f"tool {tool_name!r} is missing required parameter {name!r}"

    if schema.get("additionalProperties") is False:
        for name in value:
            if name not in properties:
                display_name = f"{path}.{name}" if path else name
                suggestion = _suggestion(name, properties)
                if suggestion is not None:
                    return (
                        f"tool {tool_name!r} has unknown parameter {display_name!r}; "
                        f"did you mean {suggestion!r}?"
                    )
                return f"tool {tool_name!r} has unknown parameter {display_name!r}"

    for name, child_schema in properties.items():
        child_value = value.get(name)
        if not isinstance(child_value, dict) or not isinstance(child_schema, dict):
            continue
        error = _validate_object(
            tool_name,
            child_value,
            child_schema,
            path=f"{path}.{name}" if path else name,
        )
        if error is not None:
            return error
    return None


def _suggestion(name: str, properties: dict[str, Any]) -> str | None:
    """Suggest one nearby property, including the common args/message mix-up."""
    names = [key for key in properties if isinstance(key, str)]
    if name == "args" and "message" in properties:
        return "message"
    if name == "message" and "args" in properties:
        return "args"
    matches = get_close_matches(name, names, n=1, cutoff=0.6)
    return matches[0] if matches else None
