"""Acceptance tests for the measured session-list response bound."""

from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from omnigent.runner.tool_dispatch import (
    _fit_session_row,
    _render_session_list,
    _session_list_payload,
    _session_list_via_rest,
)
from omnigent.tools.builtins.spawn import (
    _SESSION_LIST_RESPONSE_BUDGET_CHARS,
    _SESSION_LIST_ROW_MAX_CHARS,
)

_FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures/session_list_production.json").read_text()
)

_H = "0" * 32


def _dense_sessions_row() -> dict[str, Any]:
    return {
        "id": _H,
        "agent_name": "a" * 64,
        "title": "t" * 200,
        "status": "running",
        "current_task_status": "in_progress",
        "updated_at": 1_787_111_220,
        "runner_id": _H,
        "host_id": _H,
        "parent_session_id": _H,
        "workspace": "w" * 512,
        "git_branch": "b" * 128,
    }


def _dense_sub_agent_row(
    *, task_summary: object = "q" * 600, message: str = "m" * 200
) -> dict[str, Any]:
    code = "runner_disconnected_while_task_in_progress_retry_exhausted"
    return {
        "session_id": _H,
        "conversation_id": _H,
        "agent": "a" * 32,
        "agent_name": "a" * 32,
        "title": "t" * 48,
        "label": "l" * 96,
        "busy": False,
        "current_task_status": "failed",
        "status": "failed",
        "updated_at": 1_787_111_220,
        "pending_elicitations_count": 3,
        "error_code": code,
        "label_source": "l" * 20,
        "task_summary": task_summary,
        "last_task_error": {
            "code": code,
            "message": message,
            "title": "T" * 40,
            "cause": "c" * 120,
            "remediation": "r" * 120,
        },
    }


def _scaled_rows(rows: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    scaled: list[dict[str, Any]] = []
    for index in range(count):
        row = copy.deepcopy(rows[index % len(rows)])
        identifier = f"{index:032x}"
        for key in ("id", "session_id", "conversation_id"):
            if key in row:
                row[key] = identifier
        if "id" not in row:
            row["id"] = identifier
        scaled.append(row)
    return scaled


def _raw_child_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replay the captured child view through the raw child-session shape."""
    raw_rows: list[dict[str, Any]] = []
    for row in rows:
        raw = copy.deepcopy(row)
        identifier = raw.get("conversation_id") or raw.get("session_id")
        agent = raw.get("agent") or raw.get("agent_name") or "worker"
        title = raw.get("title") or "untitled"
        raw.update(
            {
                "id": identifier,
                "title": f"{agent}:{title}",
                "tool": agent,
                "session_name": title,
                "sub_agent_name": agent,
                "current_task_status": raw.get("current_task_status") or raw.get("status"),
            }
        )
        for key in (
            "agent",
            "agent_name",
            "conversation_id",
            "session_id",
            "label",
            "label_source",
            "status",
            "busy",
            "runner_online",
        ):
            raw.pop(key, None)
        raw_rows.append(raw)
    return raw_rows


@pytest.fixture()
def session_list_client() -> httpx.AsyncClient:
    sub_agents = _scaled_rows(_raw_child_rows(_FIXTURE["sub_agents"]), 200)
    sessions = _scaled_rows(_FIXTURE["sessions"], 200)

    def page(rows: list[dict[str, Any]], after: str | None, limit: int) -> dict[str, Any]:
        start = (
            0
            if after is None
            else next(
                (index + 1 for index, row in enumerate(rows) if row.get("id") == after),
                len(rows),
            )
        )
        chunk = rows[start : start + limit]
        end = start + len(chunk)
        return {
            "data": chunk,
            "has_more": end < len(rows),
            "last_id": chunk[-1].get("id") if chunk else None,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        query = request.url.params
        if path == "/v1/sessions/caller":
            return httpx.Response(200, json={"id": "caller", "parent_session_id": None})
        if path == "/v1/sessions/caller/child_sessions":
            return httpx.Response(
                200, json=page(sub_agents, query.get("after"), int(query["limit"]))
            )
        if path == "/v1/sessions":
            return httpx.Response(
                200, json=page(sessions, query.get("after"), int(query["limit"]))
            )
        if path.startswith("/v1/runners/"):
            return httpx.Response(200, json={"online": True})
        raise AssertionError(f"unexpected request: {request.url}")

    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://server",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ["both", "sub_agents", "sessions"])
@pytest.mark.parametrize("detail", ["summary", "full"])
@pytest.mark.parametrize("sub_agents_limit", [1, 100])
@pytest.mark.parametrize("sessions_limit", [1, 100])
async def test_session_list_bound_is_measured_on_dispatcher_result(
    session_list_client: httpx.AsyncClient,
    view: str,
    detail: str,
    sub_agents_limit: int,
    sessions_limit: int,
) -> None:
    async with session_list_client as client:
        result = await _session_list_via_rest(
            "caller",
            client,
            sub_agents_limit=sub_agents_limit,
            sessions_limit=sessions_limit,
            view=view,
            detail=detail,
        )
    assert len(result) <= 16_000
    payload = json.loads(result)
    assert all(len(json.dumps(row)) <= 1_280 for row in payload["sub_agents"])
    assert all(len(json.dumps(row)) <= 1_280 for row in payload["sessions"])


@pytest.mark.asyncio
async def test_row_fitting_preserves_atomic_metadata_or_drops_it_whole() -> None:
    row = {
        "session_id": "s" * 32,
        "conversation_id": "c" * 32,
        "agent": "researcher",
        "agent_name": "researcher",
        "title": "t" * 100,
        "label": "label",
        "busy": True,
        "current_task_status": "running",
        "status": "running",
        "updated_at": "2026-08-19T12:34:56.123456+00:00",
        "runner_online": True,
        "parent_session_id": "c0ffee1234567890abcdef0987654321",
        "host_id": "9b1e77aa42cd4f0eb3d5661c8a2f0d34",
        "workspace": "/home/daniel/repos/omnigent/.worktrees/listing",
        "git_branch": "feat/session-listing-usable",
    }
    fitted = _fit_session_row(row, view="sessions", detail="full")
    for key in (
        "session_id",
        "conversation_id",
        "parent_session_id",
        "host_id",
        "updated_at",
        "workspace",
        "git_branch",
        "status",
    ):
        assert fitted[key] == row[key]
    assert fitted["title"] == row["title"]
    assert len(json.dumps(fitted)) <= 1_280

    exempt_only = {
        "session_id": "s" * 300,
        "conversation_id": "c" * 300,
        "updated_at": "u" * 300,
        "status": "running",
    }
    fitted_exempt = _fit_session_row(exempt_only, view="sub_agents", detail="summary")
    for key, value in exempt_only.items():
        assert fitted_exempt[key] == value
    rendered = await _render_session_list(
        [],
        {"has_more": False, "next_after": None},
        [
            {
                **exempt_only,
                "parent_session_id": "p" * 300,
                "host_id": "h" * 300,
                "workspace": "w" * 300,
                "git_branch": "g" * 300,
            }
        ],
        {"has_more": False, "next_after": None},
        view="sessions",
        server_client=None,
    )
    assert len(rendered) <= 16_000
    assert json.loads(rendered)["error"] == "session_list_response_budget_exceeded"


def test_dense_sessions_row_keeps_atomic_fields_and_readable_title() -> None:
    row = _dense_sessions_row()
    row["session_id"] = _H
    row["conversation_id"] = _H
    row["parent_session_id"] = _H
    fit_input = {
        **row,
        "session_id": row["id"],
        "conversation_id": row["id"],
        "agent": row["agent_name"],
        "busy": True,
        "runner_online": True,
    }
    fit_input.pop("id")
    assert len(json.dumps(fit_input)) == 1_427
    fitted = _fit_session_row(
        fit_input,
        view="sessions",
        detail="full",
    )
    public = {key: value for key, value in fitted.items() if not key.startswith("_")}
    assert set(public) == {
        "session_id",
        "conversation_id",
        "agent",
        "agent_name",
        "title",
        "busy",
        "current_task_status",
        "status",
        "updated_at",
        "runner_online",
        "host_id",
        "workspace",
        "git_branch",
        "runner_id",
    }
    assert public["workspace"] == row["workspace"]
    assert public["git_branch"] == row["git_branch"]
    assert public["host_id"] == row["host_id"]
    assert public["agent"] == public["agent_name"] == row["agent_name"]
    assert len(public["title"]) >= 24
    assert public["title"].endswith("...")
    assert len(json.dumps(public)) <= _SESSION_LIST_ROW_MAX_CHARS
    assert fitted["_altered"]["dropped"] == ["parent_session_id"]


def test_row_fit_is_a_noop_without_pressure() -> None:
    row = {
        "session_id": "s" * 32,
        "conversation_id": "c" * 32,
        "agent": "worker",
        "agent_name": "worker",
        "title": "t" * 79,
        "busy": False,
        "current_task_status": None,
        "status": "idle",
    }
    fitted = _fit_session_row(row, view="sub_agents", detail="summary")
    assert fitted == row
    assert "_altered" not in fitted


def test_row_fit_retains_key_when_shortenable_value_collapses_to_marker() -> None:
    row = {
        "session_id": _H,
        "conversation_id": _H,
        "agent": "a" * 32,
        "agent_name": "a" * 32,
        "title": "t" * 300,
        "busy": True,
        "current_task_status": "in_progress",
        "status": "running",
        "updated_at": 1_787_111_220,
        "runner_online": True,
        "host_id": "h" * 815,
        "workspace": "w" * 300,
        "git_branch": "b" * 300,
        "runner_id": _H,
        "parent_session_id": _H,
    }
    fitted = _fit_session_row(row, view="sessions", detail="full")
    assert fitted["title"] == "..."
    assert "title" in fitted
    public = {key: value for key, value in fitted.items() if not key.startswith("_")}
    assert len(json.dumps(public)) <= 1_280


def test_row_fit_does_not_grow_short_token_values() -> None:
    row = {
        "session_id": _H,
        "conversation_id": _H,
        "agent": "a",
        "agent_name": "a",
        "title": "t" * 48,
        "busy": True,
        "current_task_status": "in_progress",
        "status": "running",
        "updated_at": 1_787_111_220,
        "host_id": "h" * 2_000,
        "workspace": "w" * 300,
        "git_branch": "b" * 300,
    }
    fitted = _fit_session_row(row, view="sessions", detail="summary")
    assert fitted["agent"] == fitted["agent_name"] == "a"
    assert "agent" not in fitted.get("_altered", {}).get("shortened", [])
    assert "agent_name" not in fitted.get("_altered", {}).get("shortened", [])


def test_row_fit_shortens_tokens_before_crossing_prose_floor() -> None:
    row = {
        "session_id": _H,
        "conversation_id": _H,
        "agent": "a" * 50,
        "agent_name": "a" * 50,
        "title": "t" * 24,
        "busy": True,
        "current_task_status": "in_progress",
        "status": "running",
        "updated_at": 1_787_111_220,
        "runner_online": True,
        "host_id": "h" * 836,
        "workspace": "",
        "git_branch": "",
    }
    assert len(json.dumps(row)) == 1_281
    fitted = _fit_session_row(row, view="sessions", detail="summary")
    assert fitted["title"] == row["title"]
    assert fitted["agent"] == fitted["agent_name"] == "a" * 46 + "..."
    assert (
        len(json.dumps({key: value for key, value in fitted.items() if not key.startswith("_")}))
        == 1_279
    )


def test_row_fit_collapses_pre_marked_tokens_to_bare_marker() -> None:
    row = {
        "session_id": _H,
        "conversation_id": _H,
        "agent": "a...",
        "agent_name": "a...",
        "title": "t" * 24,
        "busy": True,
        "current_task_status": "in_progress",
        "status": "running",
        "updated_at": 1_787_111_220,
        "runner_online": True,
        "host_id": "h" * 949,
        "workspace": "",
        "git_branch": "",
    }
    assert len(json.dumps(row)) == 1_302
    fitted = _fit_session_row(row, view="sessions", detail="summary")
    assert fitted["agent"] == fitted["agent_name"] == "..."
    assert (
        len(json.dumps({key: value for key, value in fitted.items() if not key.startswith("_")}))
        == 1_279
    )


@pytest.mark.parametrize(
    ("host_length", "raw_size", "fitted_size"),
    [(951, 1_280, 1_280), (952, 1_281, 1_279), (960, 1_289, 1_287)],
)
def test_row_fit_pre_marked_token_host_sweep(
    host_length: int, raw_size: int, fitted_size: int
) -> None:
    row = {
        "session_id": _H,
        "conversation_id": _H,
        "agent": "a...",
        "agent_name": "a...",
        "title": "",
        "busy": True,
        "current_task_status": "in_progress",
        "status": "running",
        "updated_at": 1_787_111_220,
        "runner_online": True,
        "host_id": "h" * host_length,
        "workspace": "",
        "git_branch": "",
    }
    assert len(json.dumps(row)) == raw_size
    fitted = _fit_session_row(row, view="sessions", detail="summary")
    public = {key: value for key, value in fitted.items() if not key.startswith("_")}
    assert len(json.dumps(public)) == fitted_size
    expected_agent = "a..." if raw_size == 1_280 else "..."
    assert public["agent"] == public["agent_name"] == expected_agent


def test_row_fit_keeps_settled_aliases_and_shortens_agent_pair() -> None:
    row = {
        "session_id": _H,
        "conversation_id": _H,
        "agent": "a" * 10_000,
        "agent_name": "a" * 10_000,
        "title": "t" * 10_000,
        "busy": True,
        "current_task_status": "running",
        "status": "running",
        "workspace": "w" * 10_000,
        "git_branch": "b" * 10_000,
        "error_code": "e" * 10_000,
    }
    fitted = _fit_session_row(row, view="sub_agents", detail="summary")
    assert {
        "session_id",
        "conversation_id",
        "agent",
        "agent_name",
        "busy",
        "current_task_status",
        "status",
    } <= set(fitted)
    assert fitted["agent"] == fitted["agent_name"]
    assert fitted["agent"].endswith("...")
    assert len(fitted["agent"]) >= 8
    assert fitted["session_id"] == row["session_id"]
    assert len(json.dumps({k: v for k, v in fitted.items() if not k.startswith("_")})) <= 1_280


def test_nested_error_code_is_atomic_and_shape_is_normalized() -> None:
    fitted = _fit_session_row(
        _dense_sub_agent_row(task_summary="q" * 600, message="m" * 200),
        view="sub_agents",
        detail="full",
    )
    assert fitted["last_task_error"]["code"] == fitted["error_code"]
    assert set(fitted["last_task_error"]) == {"code", "message"}
    assert "task_summary" not in fitted
    assert "label_source" not in fitted

    dropped = _fit_session_row(
        _dense_sub_agent_row(task_summary="q" * 900, message="m" * 900),
        view="sub_agents",
        detail="full",
    )
    assert "last_task_error" not in dropped
    assert dropped["error_code"] == _dense_sub_agent_row()["error_code"]

    undeclared = _fit_session_row(
        {**_dense_sub_agent_row(), "task_summary": {"secret": "value"}},
        view="sub_agents",
        detail="full",
    )
    assert "task_summary" not in undeclared
    assert "secret" not in json.dumps(undeclared)


@pytest.mark.asyncio
async def test_dense_sessions_row_reports_alteration() -> None:
    row = _dense_sessions_row()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"data": [row], "has_more": False})
        raise AssertionError(request.url)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        result = await _session_list_via_rest(
            "caller", client, view="sessions", detail="summary", sessions_limit=1
        )
    payload = json.loads(result)
    altered = payload["truncated"]["rows_altered"]["sessions"]
    assert payload["truncated"]["reason"] == "row_cap"
    assert payload["truncated"]["row_max_chars"] == _SESSION_LIST_ROW_MAX_CHARS
    assert altered["count"] == 1
    assert "title" in altered["shortened"]
    assert "parent_session_id" in altered["dropped"]
    assert payload["sessions_has_more"] is False
    assert payload["sessions_next_after"] is None
    assert all(not key.startswith("_") for key in payload["sessions"][0])
    assert payload["truncated"]["next_step"] == (
        "Call sys_session_get_info(session_id=...) for one session's full detail."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("detail", ["summary", "full"])
async def test_dense_sessions_projection_has_exact_end_to_end_shape(detail: str) -> None:
    row = _dense_sessions_row()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"data": [row], "has_more": False})
        raise AssertionError(request.url)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        payload = json.loads(
            await _session_list_via_rest(
                "caller", client, view="sessions", detail=detail, sessions_limit=1
            )
        )
    expected = {
        "session_id",
        "conversation_id",
        "agent",
        "agent_name",
        "title",
        "busy",
        "current_task_status",
        "status",
        "updated_at",
        "runner_online",
        "host_id",
        "workspace",
        "git_branch",
    }
    if detail == "full":
        expected.add("runner_id")
    assert set(payload["sessions"][0]) == expected
    assert payload["sessions"][0]["workspace"] == row["workspace"]
    assert payload["sessions"][0]["host_id"] == row["host_id"]
    assert payload["sessions"][0]["git_branch"] == row["git_branch"]


@pytest.mark.asyncio
async def test_fitter_is_idempotent_for_dense_and_fixture_rows() -> None:
    dense_sessions = _dense_sessions_row()
    dense_sessions_fit_input = {
        **dense_sessions,
        "session_id": dense_sessions["id"],
        "conversation_id": dense_sessions["id"],
        "agent": dense_sessions["agent_name"],
        "busy": True,
        "runner_online": True,
    }
    dense_sessions_fit_input.pop("id")
    rows = [
        (dense_sessions_fit_input, "sessions", "full"),
        (_dense_sub_agent_row(), "sub_agents", "full"),
        (_FIXTURE["sessions"][0], "sessions", "summary"),
    ]
    for index, (row, view, detail) in enumerate(rows):
        first = _fit_session_row(row, view=view, detail=detail)
        if index < 2:
            assert first != row
            assert first.get("_altered", {}).get("shortened") or first.get("_altered", {}).get(
                "dropped"
            )
        second = _fit_session_row(first, view=view, detail=detail)
        third = _fit_session_row(second, view=view, detail=detail)
        assert second == first
        assert third == first


def test_session_list_envelope_floor_is_satisfiable() -> None:
    page = {
        "has_more": True,
        "next_after": "x" * 197,
        "server_count": 100,
        "error": "e" * 197,
    }
    altered = {
        "sub_agents": [
            {
                "shortened": ["agent", "agent_name", "last_task_error.message", "title"],
                "dropped": [
                    "error_code",
                    "label_source",
                    "last_task_error",
                    "pending_elicitations_count",
                    "task_summary",
                ],
            }
        ],
        "sessions": [
            {"shortened": ["git_branch", "title", "workspace"], "dropped": ["parent_session_id"]}
        ],
    }
    envelope = _session_list_payload(
        [],
        page,
        [],
        page,
        sub_count=0,
        sessions_count=0,
        sub_cut=True,
        sessions_cut=True,
        rows_altered=altered,
    )
    assert (
        len(json.dumps(envelope)) + 2 * _SESSION_LIST_ROW_MAX_CHARS
        <= _SESSION_LIST_RESPONSE_BUDGET_CHARS
    )


def test_drops_precede_shortening() -> None:
    row = {
        "session_id": _H,
        "conversation_id": _H,
        "parent_session_id": _H,
        "host_id": _H,
        "runner_id": _H,
        "agent": "a" * 20,
        "agent_name": "a" * 20,
        "title": "t" * 64,
        "busy": True,
        "current_task_status": "in_progress",
        "status": "running",
        "updated_at": 1_787_111_220,
        "runner_online": True,
        "workspace": "w" * 640,
        "git_branch": "b" * 96,
    }
    assert len(json.dumps(row)) == 1_299
    fitted = _fit_session_row(row, view="sessions", detail="full")
    assert "parent_session_id" not in fitted
    assert fitted["title"] == row["title"]
    assert fitted["_altered"] == {
        "dropped": ["parent_session_id"],
        "shortened": [],
    }


@pytest.mark.asyncio
async def test_row_floor_keeps_one_row_per_nonempty_view() -> None:
    row = {
        "session_id": "child",
        "conversation_id": "child",
        "agent": "worker",
        "agent_name": "worker",
        "title": "child",
        "busy": False,
        "current_task_status": None,
        "status": None,
    }
    result = await _render_session_list(
        [row],
        {"has_more": False, "next_after": None},
        [row],
        {"has_more": False, "next_after": None},
        view="both",
        server_client=None,
    )
    payload = json.loads(result)
    assert len(payload["sub_agents"]) == 1
    assert len(payload["sessions"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit", "expected"),
    [(-1, 1), (0, 1), (True, 100), (1.5, 100), ("10**9", 100), (10**9, 100)],
)
async def test_malformed_sub_agent_limits_are_clamped(limit: object, expected: int) -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions/caller":
            return httpx.Response(200, json={"parent_session_id": None})
        if request.url.path == "/v1/sessions/caller/child_sessions":
            requested.append(request.url.params["limit"])
            return httpx.Response(200, json={"data": [], "has_more": False})
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        await _session_list_via_rest("caller", client, view="sub_agents", sub_agents_limit=limit)
    assert requested == [str(expected)]


@pytest.mark.asyncio
async def test_session_list_truncation_is_actionable(
    session_list_client: httpx.AsyncClient,
) -> None:
    async with session_list_client as client:
        result = await _session_list_via_rest("caller", client, detail="full")
    payload = json.loads(result)
    assert payload["truncated"]["reason"] in {"response_budget", "both"}
    assert payload["sub_agents_has_more"] is True
    assert isinstance(payload["sub_agents_next_after"], str)


@pytest.mark.asyncio
async def test_session_list_omits_truncation_when_all_rows_fit() -> None:
    rows = _raw_child_rows(_FIXTURE["sub_agents"][:3])
    sessions = copy.deepcopy(_FIXTURE["sessions"][:2])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions/caller":
            return httpx.Response(200, json={"parent_session_id": None})
        if request.url.path == "/v1/sessions/caller/child_sessions":
            return httpx.Response(200, json={"data": rows, "has_more": False, "last_id": "child"})
        if request.url.path == "/v1/sessions":
            return httpx.Response(
                200,
                json={
                    "data": sessions,
                    "has_more": False,
                    "last_id": sessions[-1]["session_id"],
                },
            )
        raise AssertionError(request.url)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        result = await _session_list_via_rest(
            "caller", client, sub_agents_limit=3, sessions_limit=2
        )
    payload = json.loads(result)
    assert payload["sub_agents"]
    assert payload["sessions"]
    assert "truncated" not in payload
    assert payload["sub_agents_has_more"] is False
    assert payload["sessions_has_more"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", ["x" * 7_851, "😀" * 3_000])
async def test_pathological_identifier_returns_bounded_failure_envelope(identifier: str) -> None:
    """A single uncappable identifier must not escape as an assertion error."""

    row = {
        "id": identifier,
        "title": "worker:pathological",
        "tool": "worker",
        "session_name": "pathological",
        "sub_agent_name": "worker",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions/caller":
            return httpx.Response(200, json={"parent_session_id": None})
        if request.url.path == "/v1/sessions/caller/child_sessions":
            return httpx.Response(200, json={"data": [row], "has_more": False})
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"data": [], "has_more": False})
        raise AssertionError(request.url)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        result = await _session_list_via_rest("caller", client, view="sub_agents")

    assert len(result) <= 16_000
    assert json.loads(result) == {
        "error": "session_list_response_budget_exceeded",
        "budget_chars": 16_000,
    }


@pytest.mark.asyncio
async def test_full_detail_uses_exact_private_runner_id_for_connectivity() -> None:
    """Fitting public fields must not truncate the connectivity lookup key."""

    runner_id = "runner_token_" + "a" * 60
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "session-1",
                            "agent_name": "worker",
                            "title": "job",
                            "status": "running",
                            "runner_id": runner_id,
                        }
                    ],
                    "has_more": False,
                },
            )
        prefix = "/v1/runners/"
        if request.url.path.startswith(prefix) and request.url.path.endswith("/status"):
            requested.append(request.url.path[len(prefix) : -len("/status")])
            return httpx.Response(
                200,
                json={"runner_id": requested[-1], "online": requested[-1] == runner_id},
            )
        raise AssertionError(request.url)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        result = await _session_list_via_rest(
            "caller", client, view="sessions", detail="full", sessions_limit=1
        )

    payload = json.loads(result)
    assert requested == [runner_id]
    assert payload["sessions"][0]["runner_online"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ["sub_agents", "sessions"])
async def test_budget_cursor_advances_past_predicate_consumed_rows(view: str) -> None:
    """A budget cut must not replay a closed row consumed after the last open row."""

    rows: list[dict[str, Any]] = []
    for index in range(100):
        closed = index % 2 == 1
        rows.append(
            {
                "id": f"{('closed' if closed else 'open')}-{index:03}",
                "title": f"worker:{'closed' if closed else 'open'}-{index:03}",
                "tool": "worker",
                "session_name": f"{'closed' if closed else 'open'}-{index:03}",
                "sub_agent_name": "worker",
                "labels": {"omnigent.closed": "true"} if closed else {},
                "task_summary": "verbose " * 100,
                "last_task_error": {"code": "failure", "message": "verbose " * 100},
            }
        )
    seen: list[str] = []
    after: str | None = None

    def page(after_value: str | None, limit: int) -> dict[str, Any]:
        start = (
            0
            if after_value is None
            else next((i + 1 for i, row in enumerate(rows) if row["id"] == after_value), len(rows))
        )
        chunk = rows[start : start + limit]
        return {
            "data": chunk,
            "has_more": start + len(chunk) < len(rows),
            "last_id": chunk[-1]["id"] if chunk else after_value,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions/caller":
            return httpx.Response(200, json={"parent_session_id": None})
        if view == "sub_agents" and request.url.path == "/v1/sessions/caller/child_sessions":
            return httpx.Response(200, json=page(request.url.params.get("after"), 100))
        if view == "sessions" and request.url.path == "/v1/sessions":
            return httpx.Response(200, json=page(request.url.params.get("after"), 100))
        raise AssertionError(request.url)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        while True:
            payload = json.loads(
                await _session_list_via_rest(
                    "caller",
                    client,
                    view=view,
                    detail="full",
                    sub_agents_limit=100,
                    sessions_limit=100,
                    sub_agents_after=after if view == "sub_agents" else None,
                    sessions_after=after if view == "sessions" else None,
                )
            )
            key = "sub_agents" if view == "sub_agents" else "sessions"
            seen.extend(row["session_id"] for row in payload[key])
            if not payload[f"{key}_has_more"]:
                break
            after = payload[f"{key}_next_after"]
            assert isinstance(after, str)
            # The continuation is allowed to pass closed rows, but never the
            # first open row withheld by the response budget.
            if view == "sub_agents":
                encoded = after.removeprefix("v2:")
                state = json.loads(base64.urlsafe_b64decode(encoded + "==="))
                assert state["own_after"].startswith("closed-")
            else:
                assert after.startswith(("open-", "closed-"))

    expected = [row["id"] for row in rows if row["id"].startswith("open-")]
    assert seen == expected


def _deep_error(depth: int) -> dict[str, Any]:
    value: Any = "leaf"
    for _ in range(depth):
        value = {"nested": value}
    return value


@pytest.mark.asyncio
async def test_rest_nested_error_payload_returns_bounded_listing() -> None:
    """An undeclared nested diagnostic is dropped while the row remains usable."""

    nested = '{"detail":"boom"}'
    body = (
        '{"data":[{"id":"child","title":"worker:child",'
        '"tool":"worker","session_name":"child",'
        '"sub_agent_name":"worker","last_task_error":' + nested + '}],"has_more":false}'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions/caller":
            return httpx.Response(200, json={"parent_session_id": None})
        if request.url.path == "/v1/sessions/caller/child_sessions":
            return httpx.Response(
                200,
                content=body.encode(),
                headers={"content-type": "application/json"},
            )
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"data": [], "has_more": False})
        raise AssertionError(request.url)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://server"
    ) as client:
        result = await _session_list_via_rest("caller", client, view="sub_agents", detail="full")

    assert len(result) <= 16_000
    payload = json.loads(result)
    assert len(payload["sub_agents"]) == 1
    assert "last_task_error" not in payload["sub_agents"][0]
    assert "sub_agents_error" not in payload


@pytest.mark.asyncio
async def test_renderer_nested_error_payload_returns_bounded_envelope() -> None:
    """Already-decoded deeply nested diagnostics are dropped at normalization."""

    row = {
        "session_id": "child",
        "conversation_id": "child",
        "agent": "worker",
        "agent_name": "worker",
        "title": "child",
        "busy": False,
        "current_task_status": None,
        "status": None,
        "last_task_error": _deep_error(1_500),
    }
    result = await _render_session_list(
        [row],
        {"has_more": False, "next_after": None},
        [],
        {"has_more": False, "next_after": None},
        view="sub_agents",
        server_client=None,
    )

    assert len(result) <= 16_000
    payload = json.loads(result)
    assert len(payload["sub_agents"]) == 1
    assert "last_task_error" not in payload["sub_agents"][0]
    assert "sub_agents_error" not in payload

    deep_title_row = {**row, "title": _deep_error(1_500), "last_task_error": None}
    failure = await _render_session_list(
        [deep_title_row],
        {"has_more": False, "next_after": None},
        [],
        {"has_more": False, "next_after": None},
        view="sub_agents",
        server_client=None,
    )
    assert json.loads(failure) == {
        "error": "session_list_response_budget_exceeded",
        "budget_chars": 16_000,
    }
