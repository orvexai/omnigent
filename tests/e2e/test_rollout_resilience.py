"""Opt-in acceptance gate for zero-downtime rolling replacement."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.posix_only]


def test_local_stack_urls_model_replica_ips_on_one_service_port(tmp_path: Path) -> None:
    """Local replicas keep the forwarding port invariant while avoiding bind conflicts."""
    from dev.loadtest.rollout_resilience import LocalStack

    stack = LocalStack(tmp_path, replicas=3)
    stack.server_hosts = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]
    stack.ports = [43123, 43123, 43123]

    assert stack.urls == (
        "http://127.0.0.1:43123",
        "http://127.0.0.2:43123",
        "http://127.0.0.3:43123",
    )


def test_runner_observer_reads_server_and_runner_close_code_formats(tmp_path: Path) -> None:
    """Server and runner evidence both populate the per-runner close code."""
    from dev.loadtest.rollout_resilience import RunnerObserver

    server_log = tmp_path / "server.log"
    server_log.write_text("Runner runner-a websocket disconnected (code=1012)\n")
    runner_log_dir = tmp_path / "runner-logs"
    runner_log_dir.mkdir()
    (runner_log_dir / "runner-b.log").write_text(
        "runner tunnel disconnected: server recycled the tunnel (close 1012); "
        "reconnecting promptly\n"
    )
    observer = RunnerObserver(
        (),
        ("runner-a", "runner-b"),
        (server_log, runner_log_dir),
        expected_disconnects=2,
    )
    observer.disconnects = [
        {"runner_id": "runner-a", "close_code": None},
        {"runner_id": "runner-b", "close_code": None},
    ]

    observer._fill_close_codes()

    assert [item["close_code"] for item in observer.disconnects] == [1012, 1012]


@pytest.mark.asyncio
async def test_turn_driver_counts_idle_after_busy_without_approval(tmp_path: Path) -> None:
    """Completion evidence is independent from the separately measured approval path."""
    from dev.loadtest.rollout_resilience import TurnDriver

    class Response:
        status_code = 200

        def __init__(self, body: dict[str, object]) -> None:
            self.body = body

        def json(self) -> dict[str, object]:
            return self.body

        def raise_for_status(self) -> None:
            return None

    class Client:
        def __init__(self) -> None:
            self.polls = 0

        async def post(self, _url: str, **_kwargs: object) -> Response:
            return Response({})

        async def get(self, _url: str, **_kwargs: object) -> Response:
            self.polls += 1
            if self.polls == 1:
                return Response({"status": "running"})
            return Response({"status": "idle"})

    driver = TurnDriver(
        ("http://server",),
        {"session-1": "http://server"},
        tmp_path,
        turns_per_session=1,
        require_approvals=True,
    )
    await driver._run_session(Client(), "session-1", asyncio.Event())

    assert driver.completed_turns == 1
    assert driver.lost_turns == 0
    assert driver.metrics()["observed_approvals"] == 0


def test_close_code_assertion_rejects_a_genuine_non_1012(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The close-code gate still fails when its evidence reports another code."""
    from dev.loadtest.rollout_resilience import HarnessReport, PodMetrics, _add_assertions

    monkeypatch.setattr(
        "dev.loadtest.rollout_resilience._read_runner_tunnels",
        lambda _uri, _addresses: ({"runner-a"}, set(), None),
    )
    report = HarnessReport("local", "graceful", "now", 0.0)
    _add_assertions(
        report,
        http_metrics={
            "unhandled_5xx": 0,
            "retry_failures": 0,
            "retryable_503": 0,
            "rollout_p99_limit_ms": 100.0,
            "rollout_p99_ms": 10.0,
        },
        sse_metrics={"unrecovered_streams": 0, "done_streams": 0, "max_gap_s": 0.0},
        turn_metrics={
            "lost_turns": 0,
            "completed_turns": 1,
            "expected_turns": 1,
            "duplicate_turns": 0,
            "approval_errors": 0,
            "observed_approvals": 0,
            "expected_approvals": 0,
            "approval_check_exercised": False,
        },
        runner_metrics={
            "disconnects": [{"runner_id": "runner-a", "close_code": 1001}],
            "reconnect_latencies_s": [0.1],
            "max_reconnect_s": 0.1,
            "all_unregistered_polls": 0,
            "online_at_stop": ["runner-a"],
        },
        expected_disconnects=1,
        pod_metrics=PodMetrics(),
        database_uri="sqlite:///:memory:",
        live_runner_ids={"runner-a"},
        valid_owner_addresses={"127.0.0.1:8000"},
    )

    close_assertion = next(
        item for item in report.assertions if item.name == "runner_disconnects_are_1012"
    )
    assert not close_assertion.passed
    assert "1001" in close_assertion.detail
    assert next(item for item in report.assertions if item.name == "turns_complete").passed
    assert next(
        item for item in report.assertions if item.name == "sessions_not_lost_or_duplicated"
    ).passed
    approval_assertion = next(
        item for item in report.assertions if item.name == "approval_verdicts_not_dropped"
    )
    assert approval_assertion.passed
    assert "not exercised" in approval_assertion.detail


@pytest.mark.timeout(900)
def test_rollout_resilience_acceptance_gate(tmp_path: Path) -> None:
    """Run the local proof only when explicitly enabled by a deploy gate."""
    if os.environ.get("RUN_ROLLOUT_RESILIENCE") != "1":
        pytest.skip("set RUN_ROLLOUT_RESILIENCE=1 to run the opt-in rollout gate")
    output_dir = tmp_path / "rollout-resilience"
    result = subprocess.run(
        [
            sys.executable,
            "dev/loadtest/run.py",
            "--rollout-resilience",
            "--out-dir",
            str(output_dir),
        ],
        cwd=Path(__file__).resolve().parents[2],
        check=False,
        text=True,
        capture_output=True,
    )
    print(result.stdout)
    print(result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
