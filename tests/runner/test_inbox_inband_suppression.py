"""Runner-owned suppression of results already delivered by a native harness."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest

from omnigent._wrapper_labels import (
    ANTIGRAVITY_NATIVE_SUBAGENT_WRAPPER_VALUE,
    CLAUDE_NATIVE_SUBAGENT_WRAPPER_VALUE,
    CODEX_NATIVE_SUBAGENT_WRAPPER_VALUE,
)
from omnigent.runner import app as runner_app
from omnigent.server.routes._sessions import helpers as session_helpers


@pytest.fixture
def _clean_subagent_state() -> Iterator[None]:
    saved = (
        dict(runner_app._subagent_work_by_child),
        {key: set(value) for key, value in runner_app._subagent_work_by_parent.items()},
        dict(runner_app._session_inboxes_ref),
        set(runner_app._drained_delivered_subagent_children),
    )
    runner_app._subagent_work_by_child.clear()
    runner_app._subagent_work_by_parent.clear()
    runner_app._session_inboxes_ref.clear()
    runner_app._drained_delivered_subagent_children.clear()
    try:
        yield
    finally:
        runner_app._subagent_work_by_child.clear()
        runner_app._subagent_work_by_child.update(saved[0])
        runner_app._subagent_work_by_parent.clear()
        runner_app._subagent_work_by_parent.update(saved[1])
        runner_app._session_inboxes_ref.clear()
        runner_app._session_inboxes_ref.update(saved[2])
        runner_app._drained_delivered_subagent_children.clear()
        runner_app._drained_delivered_subagent_children.update(saved[3])


@pytest.mark.parametrize(
    ("wrapper", "id_label"),
    [
        (CLAUDE_NATIVE_SUBAGENT_WRAPPER_VALUE, "claude-id"),
        (CODEX_NATIVE_SUBAGENT_WRAPPER_VALUE, "codex-id"),
    ],
)
def test_native_in_band_labels_suppress_terminal_delivery(
    _clean_subagent_state: None,
    wrapper: str,
    id_label: str,
) -> None:
    runner_app._session_inboxes_ref["parent"] = asyncio.Queue()
    entry = runner_app.register_subagent_work(
        parent_session_id="parent",
        child_session_id="child",
        agent="worker",
        title="worker",
        wrapper_label=wrapper,
        subagent_id_label=id_label,
    )

    ack = runner_app.mark_subagent_work_terminal(
        "child", status="completed", output="complete result"
    )

    assert ack.delivered is True
    assert ack.delivered_now is False
    assert ack.reason == "suppressed_native_inband"
    assert entry.status == "completed"
    assert entry.output == "complete result"
    assert runner_app.get_subagent_work("child") is None
    assert runner_app._session_inboxes_ref["parent"].empty()


def test_explicit_dispatch_with_native_label_is_delivered(_clean_subagent_state: None) -> None:
    runner_app._session_inboxes_ref["parent"] = asyncio.Queue()
    runner_app.register_subagent_work(
        parent_session_id="parent",
        child_session_id="child",
        agent="worker",
        title="worker",
        wrapper_label=CLAUDE_NATIVE_SUBAGENT_WRAPPER_VALUE,
        subagent_id_label="claude-id",
        dispatched_explicitly=True,
    )

    ack = runner_app.mark_subagent_work_terminal(
        "child", status="completed", output="explicit result"
    )

    assert ack.delivered is True
    assert ack.delivered_now is True
    assert runner_app._session_inboxes_ref["parent"].get_nowait()["output"] == "explicit result"


def test_unknown_or_missing_in_band_metadata_delivers(_clean_subagent_state: None) -> None:
    runner_app._session_inboxes_ref["parent"] = asyncio.Queue()
    for child, wrapper, id_label in (
        ("unknown", "cursor-native-ui-subagent", "id"),
        ("missing-id", CLAUDE_NATIVE_SUBAGENT_WRAPPER_VALUE, None),
    ):
        runner_app.register_subagent_work(
            parent_session_id="parent",
            child_session_id=child,
            agent="worker",
            title=child,
            wrapper_label=wrapper,
            subagent_id_label=id_label,
        )
        ack = runner_app.mark_subagent_work_terminal(child, status="completed", output=child)
        assert ack.delivered_now is True

    assert runner_app._session_inboxes_ref["parent"].qsize() == 2


def test_suppression_labels_are_reserved_by_server(_clean_subagent_state: None) -> None:
    """Every delivery-suppressing label is rejected on client label writes."""
    del _clean_subagent_state
    assert runner_app._INBAND_SUBAGENT_WRAPPER_LABELS <= (
        session_helpers._SERVER_RESERVED_NATIVE_SUBAGENT_WRAPPER_LABEL_VALUES
    )


def test_antigravity_native_label_is_delivered(_clean_subagent_state: None) -> None:
    """Antigravity child text remains inbox-delivered until its relay is proven."""
    runner_app._session_inboxes_ref["parent"] = asyncio.Queue()
    runner_app.register_subagent_work(
        parent_session_id="parent",
        child_session_id="child",
        agent="worker",
        title="worker",
        wrapper_label=ANTIGRAVITY_NATIVE_SUBAGENT_WRAPPER_VALUE,
        subagent_id_label="antigravity-id",
    )

    ack = runner_app.mark_subagent_work_terminal("child", status="completed", output="agy result")

    assert ack.delivered_now is True
    assert runner_app._session_inboxes_ref["parent"].get_nowait()["output"] == "agy result"
