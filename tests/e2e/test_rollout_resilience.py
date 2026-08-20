"""Opt-in acceptance gate for zero-downtime rolling replacement."""

from __future__ import annotations

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
