"""Tests for the embedded-browser (``browser_*``) tool surface.

Covers the runner-side half of the feature:

- ``_execute_browser_tool``: the blocking ``server_client.post`` to the
  server ``/browser/action_request`` route — correct URL / ``action`` /
  ``args`` payload, verbatim JSON passthrough, and the clean timeout
  error on ``httpx.ReadTimeout``.
- Registration in ``omnigent.tools.builtins``: the five ``browser_*``
  names are always registered.
- Native-relay exposure: ``build_native_relay_tool_schemas`` surfaces the
  five ``browser_*`` schemas when the spec declares them — native
  harnesses see the relay as their only tool surface, so a miss here
  means the feature is dead on the desktop app.
"""

from __future__ import annotations

import json

import httpx
import pytest

import omnigent.tools.builtins as builtins_mod
from omnigent.runner.tool_dispatch import (
    _BROWSER_TOOLS,
    _NATIVE_RELAY_BUILTIN_TOOLS,
    _execute_browser_tool,
    build_native_relay_tool_schemas,
)
from omnigent.spec.types import AgentSpec

# ── Helpers ──────────────────────────────────────────────────────


class _RecordingResponse:
    """Minimal httpx response stub with a scripted body."""

    def __init__(self, *, status_code: int = 200, body: dict[str, object] | None = None) -> None:
        self.status_code = status_code
        self._body = body if body is not None else {}

    @property
    def text(self) -> str:
        """Return the JSON body as text (what the tool returns verbatim)."""
        return json.dumps(self._body)


class _RecordingClient:
    """httpx.AsyncClient stub that records the POST and returns a script."""

    def __init__(self, response: _RecordingResponse | None = None) -> None:
        self.calls: list[tuple[str, dict[str, object], object]] = []
        self._response = response or _RecordingResponse(body={"final_url": "https://x"})

    async def post(
        self,
        url: str,
        *,
        json: dict[str, object] | None = None,
        timeout: object = None,
    ) -> _RecordingResponse:
        """Record the call and return the scripted response."""
        self.calls.append((url, json or {}, timeout))
        return self._response


class _TimeoutClient:
    """httpx.AsyncClient stub whose POST raises ReadTimeout."""

    async def post(self, url: str, **_: object) -> _RecordingResponse:
        """Raise the read timeout the tool must translate to clean JSON."""
        raise httpx.ReadTimeout("read timed out")


class _ErrorClient:
    """httpx.AsyncClient stub whose POST raises a generic HTTPError."""

    async def post(self, url: str, **_: object) -> _RecordingResponse:
        """Raise a connect error the tool must surface as an error string."""
        raise httpx.ConnectError("connection refused")


# ── _execute_browser_tool ────────────────────────────────────────


@pytest.mark.asyncio
async def test_browser_tool_posts_action_request_with_stripped_prefix() -> None:
    """
    The tool POSTs to the action_request route with ``action`` = tool
    name minus ``browser_`` and forwards ``args`` verbatim.
    """
    client = _RecordingClient(_RecordingResponse(body={"final_url": "https://example.com"}))
    out = await _execute_browser_tool(
        "browser_navigate",
        {"url": "https://example.com"},
        server_client=client,
        conversation_id="conv_abc",
    )

    assert len(client.calls) == 1
    url, body, timeout = client.calls[0]
    assert url == "/v1/sessions/conv_abc/browser/action_request"
    assert body == {"action": "navigate", "args": {"url": "https://example.com"}}
    # read budget MUST exceed the server await (30s) so the runner never
    # severs the still-open POST first.
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.read == 60.0
    # Result is the server JSON verbatim.
    assert json.loads(out) == {"final_url": "https://example.com"}


@pytest.mark.asyncio
async def test_browser_tool_strips_prefix_for_every_action() -> None:
    """Each of the five tools maps to the correct wire ``action``."""
    expected = {
        "browser_navigate": "navigate",
        "browser_snapshot": "snapshot",
        "browser_click": "click",
        "browser_type": "type",
        "browser_screenshot": "screenshot",
    }
    for tool_name, action in expected.items():
        client = _RecordingClient()
        await _execute_browser_tool(tool_name, {}, server_client=client, conversation_id="conv_x")
        assert client.calls[0][1]["action"] == action


@pytest.mark.asyncio
async def test_browser_tool_read_timeout_returns_clean_json() -> None:
    """
    A runner-side ``httpx.ReadTimeout`` becomes the clean timeout-error
    JSON, not an exception — so the LLM sees an actionable tool error.
    """
    out = await _execute_browser_tool(
        "browser_snapshot",
        {},
        server_client=_TimeoutClient(),
        conversation_id="conv_abc",
    )
    parsed = json.loads(out)
    assert "timed out" in parsed["error"]
    assert "Omnigent desktop app" in parsed["error"]


@pytest.mark.asyncio
async def test_browser_tool_http_error_returns_error_json() -> None:
    """A generic HTTP error is surfaced as an error JSON, not raised."""
    out = await _execute_browser_tool(
        "browser_click",
        {"ref": 3},
        server_client=_ErrorClient(),
        conversation_id="conv_abc",
    )
    parsed = json.loads(out)
    assert "browser_click failed" in parsed["error"]


@pytest.mark.asyncio
async def test_browser_tool_4xx_returns_error_json() -> None:
    """A >=400 response body is reported as an error string, not raised."""
    client = _RecordingClient(_RecordingResponse(status_code=403, body={"detail": "nope"}))
    out = await _execute_browser_tool(
        "browser_type",
        {"text": "hi"},
        server_client=client,
        conversation_id="conv_abc",
    )
    parsed = json.loads(out)
    assert "browser_type returned 403" in parsed["error"]


@pytest.mark.asyncio
async def test_browser_tool_requires_server_and_session() -> None:
    """Missing server_client or conversation_id fails loud with JSON."""
    out_no_client = await _execute_browser_tool(
        "browser_navigate", {"url": "u"}, server_client=None, conversation_id="conv"
    )
    assert "requires server access" in json.loads(out_no_client)["error"]

    out_no_conv = await _execute_browser_tool(
        "browser_navigate",
        {"url": "u"},
        server_client=_RecordingClient(),
        conversation_id=None,
    )
    assert "requires a session id" in json.loads(out_no_conv)["error"]


# ── Framework-owned registration (always on, no spec opt-in) ─────

_EXPECTED_BROWSER_NAMES = {
    "browser_navigate",
    "browser_snapshot",
    "browser_click",
    "browser_type",
    "browser_screenshot",
    # Synthesised runner-side from repeated ``snapshot`` actions rather than
    # a renderer verb, but framework-owned and reserved like the rest.
    "browser_wait_for",
}


def test_browser_names_reserved_framework_owned() -> None:
    """
    The ``browser_*`` names are reserved in the builtin registry so
    user specs can't shadow them, but they are FRAMEWORK-OWNED — like
    ``list_comments`` / ``update_comment``, they are NOT instantiable via
    ``get_builtin_tool`` (registration is ToolManager's job). This pins
    the single source of truth: ToolManager, not the registry factory.
    """
    browser = {n for n in builtins_mod.BUILTIN_NAMES if n.startswith("browser_")}
    assert browser == _EXPECTED_BROWSER_NAMES
    # Framework-owned → reserved but not user-instantiable.
    assert not (_EXPECTED_BROWSER_NAMES & set(builtins_mod.INSTANTIABLE_BUILTINS))
    for name in sorted(browser):
        assert builtins_mod.get_builtin_tool(name) is None


def test_toolmanager_always_registers_browser_tools() -> None:
    """
    EVERY session — even a spec with NO ``tools.builtins`` declared — has
    all five ``browser_*`` tools registered on its ToolManager. This is
    the invariant the earlier per-spec registration missed (agents fell
    back to WebFetch because no shipped spec declared browser_*).
    """
    from omnigent.tools.manager import ToolManager

    mgr = ToolManager(AgentSpec(spec_version=1))  # empty tools.builtins
    for name in sorted(_EXPECTED_BROWSER_NAMES):
        tool = mgr.get_tool(name)
        assert tool is not None, f"{name} not registered on a bare spec"
        schema = tool.get_schema()
        assert schema["function"]["name"] == name
        assert schema["function"]["description"]


# ── Native-relay exposure ────────────────────────────────────────


def test_browser_tools_in_native_relay_union() -> None:
    """The relay builtin union must include every browser tool name."""
    assert _BROWSER_TOOLS <= _NATIVE_RELAY_BUILTIN_TOOLS


def test_native_relay_includes_browser_for_bare_spec() -> None:
    """
    A spec with NO ``tools.builtins`` still surfaces all five
    ``browser_*`` schemas on the native relay — because ToolManager
    always registers them, the relay (which filters ToolManager's
    schemas by the union) always emits them. The desktop app runs native
    sessions that see only the relay, so this is the load-bearing path.
    """
    schemas = build_native_relay_tool_schemas(AgentSpec(spec_version=1))
    names = {s["name"] for s in schemas if s["name"].startswith("browser_")}
    assert names == _EXPECTED_BROWSER_NAMES
    # Each relay entry is the flat {name, description, parameters} shape.
    for schema in schemas:
        if schema["name"].startswith("browser_"):
            assert schema["description"]
            assert schema["parameters"]["type"] == "object"


def test_snapshot_truncation_bounds_a_large_tree_and_says_so() -> None:
    """
    A big page is capped, and the result declares the truncation.

    The renderer returns the whole accessibility tree with no way to scope
    it — one wiki front page ran to ~490 elements, a large slice of an
    agent's context for a single look. Truncating silently would be worse
    than not truncating: the agent would conclude an element does not exist
    when it was merely cut off, so the cut is reported explicitly.
    """
    from omnigent.runner.tool_dispatch import _truncate_browser_snapshot

    tree = "\n".join(f'- link "item {i}" [ref={i}]' for i in range(50))
    raw = json.dumps({"ok": True, "data": {"snapshot_id": "s1", "url": "u", "tree": tree}})

    out = json.loads(_truncate_browser_snapshot(raw, 10))

    assert len(out["data"]["tree"].splitlines()) == 10
    assert out["data"]["truncated"] is True
    assert out["data"]["dropped_elements"] == 40
    # The agent must be told an absent element may simply be below the cut.
    assert "below the cut" in out["data"]["truncation_note"]


def test_snapshot_truncation_leaves_a_small_tree_untouched() -> None:
    """A tree within the cap is returned byte-identical — no note, no flag."""
    from omnigent.runner.tool_dispatch import _truncate_browser_snapshot

    raw = json.dumps({"ok": True, "data": {"tree": '- heading "Hi" [ref=1]'}})

    assert _truncate_browser_snapshot(raw, 10) == raw


def test_snapshot_truncation_passes_through_a_failed_action() -> None:
    """
    An error payload is never rewritten.

    Truncation must not mangle a failure into something that looks like a
    successful-but-empty snapshot.
    """
    from omnigent.runner.tool_dispatch import _truncate_browser_snapshot

    raw = json.dumps({"ok": False, "error": "No browser view"})

    assert _truncate_browser_snapshot(raw, 1) == raw


def test_unknown_viz_error_becomes_actionable_guidance() -> None:
    """
    Chromium's raw compositor error is replaced with what to actually do.

    `UnknownVizError` means the pane is hidden (setVisible(false)), which the
    desktop app does whenever a dialog or the Workspace panel is up. Only
    screenshot needs a compositor surface, so every other verb keeps working
    and the raw error reads like a broken tool. An agent given the raw string
    has no way to know a UI state is the cause.
    """
    from omnigent.runner.tool_dispatch import _browser_action_guidance

    out = json.loads(_browser_action_guidance('{"ok": false, "error": "UnknownVizError"}'))

    assert out["error"] == "browser_pane_not_visible"
    assert "Workspace panel" in out["message"]
    # The original is preserved so the cause is still diagnosable.
    assert "UnknownVizError" in out["detail"]


def test_unrecognised_browser_errors_pass_through_unchanged() -> None:
    """A failure we have no guidance for is never dressed up as one we do."""
    from omnigent.runner.tool_dispatch import _browser_action_guidance

    raw = '{"ok": false, "error": "No browser view"}'

    assert _browser_action_guidance(raw) == raw
