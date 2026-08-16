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

import base64
import io
import json
import random

import httpx
import pytest
from PIL import Image

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


def _png_data_url(width: int, height: int, *, detailed: bool = False) -> str:
    """Create a real PNG data URL for screenshot dispatch tests."""
    if detailed:
        rng = random.Random(0)
        tile = Image.frombytes(
            "RGB",
            (120, 120),
            bytes(rng.randrange(256) for _ in range(120 * 120 * 3)),
        )
        image = tile.resize((width, height), Image.Resampling.NEAREST)
    else:
        image = Image.new("RGB", (width, height), "white")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


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
async def test_screenshot_bounds_the_realistic_large_image() -> None:
    """A 1380x1744 screenshot is resized/re-encoded under the default budget."""
    data_url = _png_data_url(1380, 1744, detailed=True)
    client = _RecordingClient(_RecordingResponse(body={"ok": True, "data_url": data_url}))

    out = json.loads(
        await _execute_browser_tool(
            "browser_screenshot",
            {},
            server_client=client,
            conversation_id="conv_x",
        )
    )

    returned_payload = out["data_url"].split(",", 1)[1]
    assert len(returned_payload) <= 48_000
    assert out["original_width"] == 1380
    assert out["original_height"] == 1744
    assert out["returned_width"] <= 900
    assert out["returned_height"] <= 900
    assert out["downscaled"] is True
    assert out["truncated"] is True
    assert out["encoding"] in {"png", "jpeg"}
    assert client.calls[0][1]["args"] == {}


@pytest.mark.asyncio
async def test_small_screenshot_keeps_data_url_unchanged() -> None:
    """An already-fitting image is not silently re-encoded."""
    data_url = _png_data_url(20, 10)
    client = _RecordingClient(_RecordingResponse(body={"ok": True, "data_url": data_url}))

    out = json.loads(
        await _execute_browser_tool(
            "browser_screenshot",
            {},
            server_client=client,
            conversation_id="conv_x",
        )
    )

    assert out["data_url"] == data_url
    assert out["original_width"] == out["returned_width"] == 20
    assert out["original_height"] == out["returned_height"] == 10
    assert out["encoding"] == "png"
    assert out["downscaled"] is False
    assert out["truncated"] is False
    assert "returned unchanged" in out["truncation_note"]


@pytest.mark.asyncio
async def test_screenshot_parameters_override_default_bounds() -> None:
    """Screenshot-specific edge and payload limits are honored by the runner."""
    data_url = _png_data_url(40, 20)
    client = _RecordingClient(_RecordingResponse(body={"ok": True, "data_url": data_url}))

    out = json.loads(
        await _execute_browser_tool(
            "browser_screenshot",
            {"max_edge": 5, "max_chars": 1_000},
            server_client=client,
            conversation_id="conv_x",
        )
    )

    assert out["returned_width"] <= 5
    assert out["returned_height"] <= 5
    assert len(out["data_url"].split(",", 1)[1]) <= 1_000
    assert client.calls[0][1]["args"] == {}


@pytest.mark.asyncio
async def test_screenshot_without_pillow_returns_original_payload(monkeypatch) -> None:
    """Without Pillow, an oversized image remains available with an explicit note."""
    import omnigent.runner.tool_dispatch as tool_dispatch

    data_url = _png_data_url(1380, 1744, detailed=True)
    client = _RecordingClient(_RecordingResponse(body={"ok": True, "data_url": data_url}))
    monkeypatch.setattr(tool_dispatch, "_PILImage", None)

    out = json.loads(
        await _execute_browser_tool(
            "browser_screenshot",
            {"max_chars": 48_000},
            server_client=client,
            conversation_id="conv_x",
        )
    )

    assert out["ok"] is True
    assert out["data_url"] == data_url
    assert out["truncated"] is False
    assert out["downscaled"] is False
    assert out["returned_base64_chars"] > 48_000
    assert "Pillow is unavailable" in out["truncation_note"]
    assert "data_url" in out


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
        if name == "browser_screenshot":
            assert set(schema["function"]["parameters"]["properties"]) == {
                "max_edge",
                "max_chars",
            }


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


@pytest.mark.asyncio
async def test_superseded_snapshot_action_gets_a_warning() -> None:
    """A successful action with an older known snapshot id is annotated."""
    import omnigent.runner.tool_dispatch as tool_dispatch

    tool_dispatch._browser_snapshot_ids.clear()
    snapshot_body = {
        "ok": True,
        "data": {"snapshot_id": "snapshot-1", "tree": '- link "one" [ref=1]'},
    }
    await _execute_browser_tool(
        "browser_snapshot",
        {},
        server_client=_RecordingClient(_RecordingResponse(body=snapshot_body)),
        conversation_id="conv_snapshots",
    )
    await _execute_browser_tool(
        "browser_snapshot",
        {},
        server_client=_RecordingClient(
            _RecordingResponse(
                body={
                    "ok": True,
                    "data": {"snapshot_id": "snapshot-2", "tree": '- link "two" [ref=1]'},
                }
            )
        ),
        conversation_id="conv_snapshots",
    )

    out = json.loads(
        await _execute_browser_tool(
            "browser_click",
            {"ref": 1, "snapshot_id": "snapshot-1"},
            server_client=_RecordingClient(_RecordingResponse(body={"ok": True})),
            conversation_id="conv_snapshots",
        )
    )

    assert "superseded" in out["warning"]
    assert "detached element" in out["warning"]
    assert "fresh browser_snapshot" in out["warning"]


@pytest.mark.asyncio
async def test_current_or_unrecorded_snapshot_action_is_unmodified() -> None:
    """Current and unknown snapshot ids do not add a warning."""
    import omnigent.runner.tool_dispatch as tool_dispatch

    tool_dispatch._browser_snapshot_ids.clear()
    snapshot_response = _RecordingResponse(
        body={
            "ok": True,
            "data": {"snapshot_id": "snapshot-current", "tree": '- link "one" [ref=1]'},
        }
    )
    await _execute_browser_tool(
        "browser_snapshot",
        {},
        server_client=_RecordingClient(snapshot_response),
        conversation_id="conv_current",
    )
    current_body = {"ok": True, "clicked": True}
    current_client = _RecordingClient(_RecordingResponse(body=current_body))
    current_out = await _execute_browser_tool(
        "browser_click",
        {"ref": 1, "snapshot_id": "snapshot-current"},
        server_client=current_client,
        conversation_id="conv_current",
    )
    assert current_out == json.dumps(current_body)

    unknown_body = {"ok": True, "clicked": True}
    unknown_out = await _execute_browser_tool(
        "browser_click",
        {"ref": 1, "snapshot_id": "snapshot-unknown"},
        server_client=_RecordingClient(_RecordingResponse(body=unknown_body)),
        conversation_id="conv_without_snapshot_record",
    )
    assert unknown_out == json.dumps(unknown_body)


def test_snapshot_records_are_bounded() -> None:
    """Snapshot ids are evicted when the session record reaches its cap."""
    import omnigent.runner.tool_dispatch as tool_dispatch
    from omnigent.runner.tool_dispatch import (
        _BROWSER_SNAPSHOT_MAX_TRACKED,
        _truncate_browser_snapshot,
    )

    tool_dispatch._browser_snapshot_ids.clear()
    for index in range(_BROWSER_SNAPSHOT_MAX_TRACKED + 1):
        raw = json.dumps(
            {
                "ok": True,
                "data": {"snapshot_id": f"snapshot-{index}", "tree": ""},
            }
        )
        _truncate_browser_snapshot(raw, 1, conversation_id=f"conv-{index}")

    assert len(tool_dispatch._browser_snapshot_ids) == _BROWSER_SNAPSHOT_MAX_TRACKED


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


def test_ref_renderer_script_failure_becomes_fresh_snapshot_guidance() -> None:
    """A generic renderer failure points agents at the likely stale-ref recovery."""
    from omnigent.runner.tool_dispatch import _browser_action_guidance

    raw = (
        '{"ok": false, "error": "Script failed to execute; check the renderer console."}'
        + " x" * 120
    )

    out = json.loads(_browser_action_guidance(raw, addressing="ref"))

    assert out["error"] == "browser_action_failed_in_renderer"
    assert "most likely cause" in out["message"]
    assert "fresh browser_snapshot" in out["message"]
    assert "ref from that new snapshot" in out["message"]
    assert out["detail"] == raw[:200]


@pytest.mark.asyncio
async def test_current_ref_renderer_script_failure_suggests_interactable_element() -> None:
    """A ref from the latest snapshot gets interaction-specific guidance."""
    import omnigent.runner.tool_dispatch as tool_dispatch
    from omnigent.runner.tool_dispatch import _truncate_browser_snapshot

    tool_dispatch._browser_snapshot_ids.clear()
    conversation_id = "conv_current_ref_guidance"
    _truncate_browser_snapshot(
        json.dumps(
            {
                "ok": True,
                "data": {"snapshot_id": "snapshot-current", "tree": ""},
            }
        ),
        1,
        conversation_id=conversation_id,
    )
    raw = {"ok": False, "error": "Script failed to execute; check the renderer console."}

    out = json.loads(
        await _execute_browser_tool(
            "browser_click",
            {"ref": 1, "snapshot_id": "snapshot-current"},
            server_client=_RecordingClient(_RecordingResponse(body=raw)),
            conversation_id=conversation_id,
        )
    )

    assert out["error"] == "browser_action_failed_in_renderer"
    assert "superseded" not in out["message"].casefold()
    assert "stale" not in out["message"].casefold()
    assert "current browser snapshot" in out["message"]
    assert "not be interactable" in out["message"]
    assert "selector" in out["message"]
    assert out["detail"] == json.dumps(raw)[:200]


@pytest.mark.asyncio
async def test_superseded_ref_renderer_script_failure_keeps_stale_guidance() -> None:
    """A ref from an older recorded snapshot keeps stale-ref guidance."""
    import omnigent.runner.tool_dispatch as tool_dispatch
    from omnigent.runner.tool_dispatch import _truncate_browser_snapshot

    tool_dispatch._browser_snapshot_ids.clear()
    conversation_id = "conv_superseded_ref_guidance"
    for snapshot_id in ("snapshot-old", "snapshot-current"):
        _truncate_browser_snapshot(
            json.dumps({"ok": True, "data": {"snapshot_id": snapshot_id, "tree": ""}}),
            1,
            conversation_id=conversation_id,
        )
    raw = {"ok": False, "error": "Script failed to execute; check the renderer console."}

    out = json.loads(
        await _execute_browser_tool(
            "browser_click",
            {"ref": 1, "snapshot_id": "snapshot-old"},
            server_client=_RecordingClient(_RecordingResponse(body=raw)),
            conversation_id=conversation_id,
        )
    )

    assert "most likely cause" in out["message"]
    assert "superseded browser snapshot" in out["message"]
    assert "fresh browser_snapshot" in out["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "record_snapshot"),
    [
        ({"ref": 1}, True),
        ({"ref": 1, "snapshot_id": "snapshot-unknown"}, False),
    ],
)
async def test_ref_without_known_snapshot_keeps_hedged_guidance(
    args: dict[str, object], record_snapshot: bool
) -> None:
    """Refs without enough snapshot knowledge remain hedged."""
    import omnigent.runner.tool_dispatch as tool_dispatch
    from omnigent.runner.tool_dispatch import _truncate_browser_snapshot

    tool_dispatch._browser_snapshot_ids.clear()
    conversation_id = (
        "conv_ref_without_snapshot_id" if record_snapshot else "conv_ref_without_record"
    )
    if record_snapshot:
        _truncate_browser_snapshot(
            json.dumps(
                {
                    "ok": True,
                    "data": {"snapshot_id": "snapshot-current", "tree": ""},
                }
            ),
            1,
            conversation_id=conversation_id,
        )
    raw = {"ok": False, "error": "Script failed to execute; check the renderer console."}

    out = json.loads(
        await _execute_browser_tool(
            "browser_click",
            args,
            server_client=_RecordingClient(_RecordingResponse(body=raw)),
            conversation_id=conversation_id,
        )
    )

    assert "most likely cause" in out["message"]
    assert "superseded browser snapshot" in out["message"]
    assert "fresh browser_snapshot" in out["message"]


def test_selector_renderer_script_failure_names_missing_element() -> None:
    """Selector failures advise checking the selector, not stale refs."""
    from omnigent.runner.tool_dispatch import _browser_action_guidance

    raw = '{"ok": false, "error": "Script failed to execute; check the renderer console."}'
    out = json.loads(_browser_action_guidance(raw, addressing="selector"))

    assert "selector matched no element on the current page" in out["message"]
    assert "stale" not in out["message"].casefold()
    assert "ref" not in out["message"].casefold()
    assert "fresh browser_snapshot" in out["message"]
    assert out["detail"] == raw[:200]


def test_unknown_renderer_script_failure_names_both_possibilities() -> None:
    """Unknown addressing keeps both possible causes explicitly conditional."""
    from omnigent.runner.tool_dispatch import _browser_action_guidance

    raw = '{"ok": false, "error": "Script failed to execute; check the renderer console."}'
    out = json.loads(_browser_action_guidance(raw))

    assert "may have used a ref" in out["message"]
    assert "selector that matched no element" in out["message"]
    assert "most likely" not in out["message"]
    assert out["detail"] == raw[:200]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "expected", "forbidden"),
    [
        (
            {"selector": "#missing"},
            "selector matched no element on the current page",
            "stale",
        ),
        ({"ref": 1}, "most likely cause", "selector matched no element"),
        ({}, "may have used a ref", "most likely cause"),
    ],
)
async def test_browser_action_guidance_uses_element_addressing(
    args: dict[str, object], expected: str, forbidden: str
) -> None:
    """Dispatch passes ref, selector, and unknown addressing to guidance."""
    raw = {"ok": False, "error": "Script failed to execute; check the renderer console."}
    out = json.loads(
        await _execute_browser_tool(
            "browser_click",
            args,
            server_client=_RecordingClient(_RecordingResponse(body=raw)),
            conversation_id="conv_guidance",
        )
    )

    assert expected in out["message"]
    assert forbidden not in out["message"]
    assert out["detail"] == json.dumps(raw)[:200]


def test_browser_action_timeout_becomes_action_specific_guidance() -> None:
    """The server timeout identifies an unanswered action, not a dead session."""
    from omnigent.runner.tool_dispatch import _browser_action_guidance

    raw = (
        '{"error": "browser action timed out — is the session open in the '
        'Omnigent desktop app?", "request_id": "abc"}'
    )

    out = json.loads(_browser_action_guidance(raw))

    assert out["error"] == "browser_action_timed_out"
    assert "this particular browser action" in out["message"]
    assert "30-second window" in out["message"]
    assert "browser_snapshot" in out["message"]
    assert "browser_screenshot" in out["message"]
    assert "visible or composited" in out["message"]
    assert "closed" not in out["message"].casefold()
    assert "not open" not in out["message"].casefold()
    assert out["detail"] == raw[:200]


def test_unrecognised_browser_errors_pass_through_unchanged() -> None:
    """A failure we have no guidance for is never dressed up as one we do."""
    from omnigent.runner.tool_dispatch import _browser_action_guidance

    raw = '{"ok": false, "error": "No browser view"}'

    assert _browser_action_guidance(raw) == raw
