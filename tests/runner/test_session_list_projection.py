"""Focused contract tests for the runner's sub-agent list projection."""

from __future__ import annotations

import json
from types import SimpleNamespace

from omnigent.runner.tool_dispatch import _child_rows_to_entries
from omnigent.session_lifecycle import CLOSED_LABEL_KEY, CLOSED_LABEL_VALUE


def _row(
    conversation_id: str,
    title: str,
    *,
    tool: str | None = None,
    session_name: str | None = None,
    sub_agent_name: str | None = None,
    description: str | None = None,
    task_summary: str | None = None,
    labels: dict[str, str] | None = None,
    closed: bool = False,
) -> dict[str, object]:
    row: dict[str, object] = {
        "id": conversation_id,
        "title": title,
        "tool": tool,
        "session_name": session_name,
        "sub_agent_name": sub_agent_name,
        "task_summary": task_summary,
        "busy": True,
        "current_task_status": "in_progress",
        "updated_at": 1234,
        "last_task_error": None,
        "pending_elicitations_count": 2,
        "labels": {
            **(labels or {}),
            **({"omnigent.claude_native.description": description} if description else {}),
        },
    }
    if closed:
        row["labels"] = {**row["labels"], CLOSED_LABEL_KEY: CLOSED_LABEL_VALUE}
    return row


def _entries(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return _child_rows_to_entries(
        rows,
        # Deliberately do not declare the synthetic named child. The durable
        # server field, rather than the parent's AgentSpec, must bind it.
        SimpleNamespace(sub_agents=[]),
    )


def test_projection_preserves_legacy_fields_and_projects_child_state() -> None:
    [entry] = _entries(
        [
            _row(
                "conv_research",
                "researcher:auth",
                tool="researcher",
                session_name="auth",
                sub_agent_name="researcher",
            )
        ]
    )

    assert entry["agent"] == "researcher"
    assert entry["title"] == "auth"
    assert entry["conversation_id"] == "conv_research"
    assert entry["label"] == "auth"
    assert entry["label_source"] == "session_name"
    assert entry["busy"] is True
    assert entry["current_task_status"] == "in_progress"
    assert entry["updated_at"] == 1234
    assert entry["last_task_error"] is None
    assert entry["pending_elicitations_count"] == 2
    assert entry["task_summary"] is None


def test_claude_native_description_is_the_human_label_without_hex_suffix() -> None:
    [entry] = _entries(
        [
            _row(
                "conv_plan",
                "Plan:ae59deadbeef",
                tool="Plan",
                session_name="ae59deadbeef",
                sub_agent_name="Plan",
                description="Investigate authentication flow",
            )
        ]
    )

    assert entry["agent"] == "Plan"
    assert entry["title"] == "ae59deadbeef"
    assert entry["label"] == "Investigate authentication flow"
    assert entry["label_source"] == "omnigent.claude_native.description"
    assert "ae59deadbeef" not in entry["label"]


def test_free_form_colon_title_does_not_invent_agent() -> None:
    [entry] = _entries([_row("conv_bug", "bug: login 500")])

    assert entry["agent"] is None
    assert entry["title"] == "bug: login 500"
    assert entry["label"] == "bug: login 500"
    assert entry["label_source"] == "title"


def test_prose_colon_title_label_falls_back_to_whole_title() -> None:
    """A prose colon is not an agent/session delimiter for the label."""
    [entry] = _entries(
        [
            _row(
                "conv_u8",
                "U8-THREADS implement: stage 1",
                tool="U8-THREADS implement",
                session_name=" stage 1",
                sub_agent_name="worker",
            )
        ]
    )

    assert entry["label"] == "U8-THREADS implement: stage 1"
    assert entry["label"] != " stage 1"
    assert entry["label_source"] == "title"
    assert entry["agent"] == "worker"


def test_child_projection_is_compact_and_drops_raw_labels() -> None:
    """The list result keeps projections, not the bulky source label map."""
    rows = [
        _row(
            f"conv_{index}",
            f"worker:task-{index}",
            tool="worker",
            session_name=f"task-{index}",
            sub_agent_name="worker",
            labels={
                "omnigent.last_context_tokens": "196529",
                "omnigent.last_task_error_cause": "",
                "omnigent.last_task_error_remediation": "",
                "omnigent.ui": "terminal",
                "omnigent.wrapper": "codex-native-ui",
                "omnigent.last_task_error_code": "",
                "omnigent.last_task_error_message": "",
                "omnigent.last_task_error_title": "",
            },
        )
        for index in range(75)
    ]

    entries = _entries(rows)
    rendered = json.dumps({"sub_agents": entries}, separators=(",", ":"))

    assert len(rendered) < 40_000
    assert all("labels" not in entry for entry in entries)


def test_ui_title_keeps_explicit_agent_and_label() -> None:
    [entry] = _entries(
        [_row("conv_ui", "ui:researcher:my-label", tool="researcher", session_name="my-label")]
    )

    assert entry["agent"] == "researcher"
    assert entry["title"] == "my-label"
    assert entry["label"] == "my-label"
    assert entry["label_source"] == "session_name"


def test_siblings_with_same_agent_have_distinct_description_labels() -> None:
    entries = _entries(
        [
            _row(
                "conv_one",
                "Plan:one",
                tool="Plan",
                session_name="one",
                sub_agent_name="Plan",
                description="Audit login",
            ),
            _row(
                "conv_two",
                "Plan:two",
                tool="Plan",
                session_name="two",
                sub_agent_name="Plan",
                description="Audit logout",
            ),
        ]
    )

    assert [entry["agent"] for entry in entries] == ["Plan", "Plan"]
    assert [entry["label"] for entry in entries] == ["Audit login", "Audit logout"]


def test_label_fallback_order_reports_winning_source() -> None:
    rows = [
        _row(
            "conv_description",
            "worker:instance",
            tool="worker",
            session_name="instance",
            sub_agent_name="worker",
            description="Description",
            task_summary="Task summary",
        ),
        _row(
            "conv_task",
            "worker:instance",
            tool="worker",
            session_name="instance",
            sub_agent_name="worker",
            task_summary="Task summary",
        ),
        _row(
            "conv_session",
            "worker:instance",
            tool="worker",
            session_name="instance",
            sub_agent_name="worker",
        ),
        _row("conv_title", "free-form title"),
    ]

    entries = {entry["conversation_id"]: entry for entry in _entries(rows)}
    assert (entries["conv_description"]["label"], entries["conv_description"]["label_source"]) == (
        "Description",
        "omnigent.claude_native.description",
    )
    assert (entries["conv_task"]["label"], entries["conv_task"]["label_source"]) == (
        "Task summary",
        "task_summary",
    )
    assert (entries["conv_session"]["label"], entries["conv_session"]["label_source"]) == (
        "instance",
        "session_name",
    )
    assert (entries["conv_title"]["label"], entries["conv_title"]["label_source"]) == (
        "free-form title",
        "title",
    )


def test_closed_and_titleless_rows_are_omitted() -> None:
    entries = _entries(
        [
            _row("conv_closed", "worker:closed", closed=True),
            {"id": "conv_titleless", "title": None},
        ]
    )

    assert entries == []
