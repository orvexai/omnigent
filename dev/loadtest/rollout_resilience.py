"""Zero-downtime rollout proof harness.

The drivers in this module are transport-agnostic: they only need a list of
server URLs and session identifiers. ``LocalStack`` supplies three local
server processes; ``KubernetesTarget`` supplies the same measurements while
replacing pods through ``kubectl``.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import math
import os
import re
import signal
import socket
import subprocess
import sys
import tarfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import httpx
import yaml

from dev.loadtest.run import _mock_reply

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MOCK_SERVER = _REPO_ROOT / "tests" / "server" / "integration" / "mock_llm_server.py"
_HEARTBEAT_S = 15.0
_SSE_GAP_LIMIT_S = 20.0
_REREGISTRATION_LIMIT_S = 3.0
_POLICY_LLM_KEY = "_policy_llm_"
_MODEL = "rollout-resilience-model"
_HOST_COUNT = 2
_DEFAULT_REPLICAS = 3
_DEFAULT_ROLLOUT_SETTLE_S = 30.0
_TURN_COMPLETION_MARGIN_S = 30.0
_INTERNAL_ORIGIN = "omnigent://internal"
_CLOSE_CODE_RE = re.compile(
    r"(?:\(\s*code\s*=\s*|\bclose(?:\s+code)?\s*(?:=|:)?\s*)(?P<code>\d{3,4})\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _RolloutTiming:
    """Keep rollout configuration and the dependent turn wait in one value."""

    pod_count: int
    settle_s: float
    margin_s: float = _TURN_COMPLETION_MARGIN_S

    @property
    def rollout_s(self) -> float:
        return self.pod_count * self.settle_s

    @property
    def llm_gate_hold_s(self) -> float:
        # The local proof keeps the gate closed until the configured rollout ends.
        return self.rollout_s

    @property
    def turn_completion_budget_s(self) -> float:
        return self.rollout_s + self.llm_gate_hold_s + self.margin_s

    def as_json(self) -> dict[str, float | int]:
        return {
            "pod_count": self.pod_count,
            "settle_s": self.settle_s,
            "rollout_s": self.rollout_s,
            "llm_gate_hold_s": self.llm_gate_hold_s,
            "margin_s": self.margin_s,
            "turn_completion_budget_s": self.turn_completion_budget_s,
        }

    def log(self) -> None:
        print(
            "turn completion wait budget: "
            f"{self.turn_completion_budget_s:.1f}s = "
            f"({self.pod_count} pods x {self.settle_s:.1f}s settle "
            f"= {self.rollout_s:.1f}s rollout) + "
            f"{self.llm_gate_hold_s:.1f}s LLM gate hold + "
            f"{self.margin_s:.1f}s margin"
        )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _p99(values: list[float]) -> float | None:
    if not values:
        return None
    return sorted(values)[min(len(values) - 1, math.ceil(len(values) * 0.99) - 1)]


def _json_contains(value: object, needle: str) -> int:
    if isinstance(value, dict):
        return sum(_json_contains(k, needle) + _json_contains(v, needle) for k, v in value.items())
    if isinstance(value, list):
        return sum(_json_contains(item, needle) for item in value)
    return int(isinstance(value, str) and needle in value)


@dataclass
class AssertionResult:
    name: str
    passed: bool
    detail: str
    metrics: dict[str, object] = field(default_factory=dict)

    def as_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
            "metrics": self.metrics,
        }


@dataclass
class HarnessReport:
    mode: str
    failure_mode: str
    generated_at: str
    duration_s: float
    assertions: list[AssertionResult] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)
    artifacts: dict[str, str] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return bool(self.assertions) and all(item.passed for item in self.assertions)

    def add(self, name: str, passed: bool, detail: str, **metrics: object) -> None:
        self.assertions.append(AssertionResult(name, passed, detail, metrics))

    def as_json(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "failure_mode": self.failure_mode,
            "generated_at": self.generated_at,
            "duration_s": self.duration_s,
            "passed": self.passed,
            "assertions": [item.as_json() for item in self.assertions],
            "metadata": self.metadata,
            "artifacts": self.artifacts,
        }

    def write(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_path = out_dir / "rollout_resilience.json"
        summary_path.write_text(json.dumps(self.as_json(), indent=2, sort_keys=True) + "\n")
        self.artifacts["json"] = str(summary_path)
        lines = [
            "# Rollout resilience proof",
            "",
            f"- Mode: `{self.mode}`",
            f"- Failure mode: `{self.failure_mode}`",
            f"- Duration: `{self.duration_s:.1f}s`",
            f"- Outcome: `{'PASS' if self.passed else 'FAIL'}`",
            "",
            "| Assertion | Result | Detail |",
            "|---|---|---|",
        ]
        for item in self.assertions:
            result = "PASS" if item.passed else "FAIL"
            detail = item.detail.replace("|", "\\|")
            lines.append(f"| `{item.name}` | **{result}** | {detail} |")
        lines.extend(["", f"Machine-readable result: `{summary_path}`", ""])
        summary_md = out_dir / "rollout_resilience.md"
        summary_md.write_text("\n".join(lines))
        self.artifacts["markdown"] = str(summary_md)
        summary_path.write_text(json.dumps(self.as_json(), indent=2, sort_keys=True) + "\n")


@dataclass
class HttpSample:
    phase: str
    method: str
    url: str
    status: int | None
    latency_ms: float
    retry: bool = False
    retry_after: str | None = None
    error: str | None = None

    def as_json(self) -> dict[str, object]:
        return self.__dict__.copy()


class HttpProber:
    """Generate session reads and writes at five requests per second."""

    def __init__(self, urls: tuple[str, ...], session_ids: tuple[str, ...], out_dir: Path) -> None:
        self.urls = urls
        self.session_ids = session_ids
        self.out_dir = out_dir
        self.samples: list[HttpSample] = []
        self.unhandled_5xx = 0
        self.retryable_503 = 0
        self.retry_failures = 0

    async def run(self, stop: asyncio.Event, phase: dict[str, str]) -> None:
        index = 0
        pending: set[asyncio.Task[None]] = set()
        next_tick = time.monotonic()
        async with httpx.AsyncClient(timeout=5.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            while not stop.is_set():
                session_id = self.session_ids[index % len(self.session_ids)]
                url = self.urls[index % len(self.urls)].rstrip("/")
                index += 1
                body: dict[str, Any] | None
                if index % 2:
                    method = "GET"
                    path = f"/v1/sessions/{session_id}"
                    body = None
                else:
                    method = "POST"
                    path = f"/v1/sessions/{session_id}/events"
                    body = {
                        "type": "external_conversation_item",
                        "data": {
                            "item_type": "message",
                            "item_data": {
                                "role": "user",
                                "content": [{"type": "input_text", "text": f"probe-{index}"}],
                            },
                        },
                    }
                task = asyncio.create_task(
                    self._request(client, phase["value"], method, f"{url}{path}", body)
                )
                pending.add(task)
                task.add_done_callback(pending.discard)
                next_tick += 0.2
                await asyncio.sleep(max(0.0, next_tick - time.monotonic()))
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        (self.out_dir / "http_samples.jsonl").write_text(
            "".join(json.dumps(sample.as_json()) + "\n" for sample in self.samples)
        )

    async def _request(
        self,
        client: httpx.AsyncClient,
        phase: str,
        method: str,
        request_url: str,
        body: dict[str, object] | None,
    ) -> None:
        response: httpx.Response | None = None
        retry = False
        retry_after: str | None = None
        started = time.monotonic()
        try:
            attempt_started = time.monotonic()
            response = await client.request(method, request_url, json=body)
            self.samples.append(
                HttpSample(
                    phase=phase,
                    method=method,
                    url=request_url,
                    status=response.status_code,
                    latency_ms=(time.monotonic() - attempt_started) * 1000,
                )
            )
            if response.status_code == 503:
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    self.retryable_503 += 1
                    retry = True
                    try:
                        delay = min(1.0, max(0.0, float(retry_after)))
                    except ValueError:
                        self.retry_failures += 1
                        delay = 0.0
                    await asyncio.sleep(delay)
                    attempt_started = time.monotonic()
                    response = await client.request(method, request_url, json=body)
                    self.samples.append(
                        HttpSample(
                            phase=phase,
                            method=method,
                            url=request_url,
                            status=response.status_code,
                            latency_ms=(time.monotonic() - attempt_started) * 1000,
                            retry=True,
                            retry_after=retry_after,
                        )
                    )
                    if response.status_code >= 500:
                        self.retry_failures += 1
                else:
                    self.retry_failures += 1
            if response.status_code >= 500 and not (retry and response.status_code < 500):
                self.unhandled_5xx += 1
        except (httpx.HTTPError, ValueError) as exc:
            self.samples.append(
                HttpSample(
                    phase=phase,
                    method=method,
                    url=request_url,
                    status=response.status_code if response is not None else None,
                    latency_ms=(time.monotonic() - started) * 1000,
                    retry=retry,
                    retry_after=retry_after,
                    error=repr(exc),
                )
            )

    def metrics(self) -> dict[str, object]:
        baseline = [s.latency_ms for s in self.samples if s.phase == "steady"]
        action = [s.latency_ms for s in self.samples if s.phase == "rollout"]
        baseline_p99 = _p99(baseline)
        action_p99 = _p99(action)
        limit = baseline_p99 * 2 if baseline_p99 is not None else None
        return {
            "requests": len(self.samples),
            "status_counts": {
                str(code): sum(sample.status == code for sample in self.samples)
                for code in sorted(
                    {sample.status for sample in self.samples if sample.status is not None}
                )
            },
            "unhandled_5xx": self.unhandled_5xx,
            "retryable_503": self.retryable_503,
            "retry_failures": self.retry_failures,
            "steady_p99_ms": baseline_p99,
            "rollout_p99_ms": action_p99,
            "rollout_p99_limit_ms": limit,
        }


class SseSubscriber:
    """Keep one reconnecting SSE subscription per live session."""

    def __init__(self, urls: tuple[str, ...], session_ids: tuple[str, ...], out_dir: Path) -> None:
        self.urls = urls
        self.session_ids = session_ids
        self.out_dir = out_dir
        self.gaps_s: list[float] = []
        self.done_streams = 0
        self.stream_failures = 0
        self.recovered_streams = 0
        self.connects = 0

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._run_one(session_id, i, stop))
            for i, session_id in enumerate(self.session_ids)
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        (self.out_dir / "sse_gaps.json").write_text(json.dumps(self.gaps_s, indent=2) + "\n")

    async def _run_one(self, session_id: str, start_index: int, stop: asyncio.Event) -> None:
        url_index = start_index
        had_failure = False
        last_signal = time.monotonic()
        while not stop.is_set():
            base_url = self.urls[url_index % len(self.urls)].rstrip("/")
            url_index += 1
            try:
                saw_done = False
                async with (
                    httpx.AsyncClient(
                        timeout=None,
                        headers={"Origin": _INTERNAL_ORIGIN, "Accept": "text/event-stream"},
                    ) as client,
                    client.stream(
                        "GET", f"{base_url}/v1/sessions/{session_id}/stream"
                    ) as response,
                ):
                    if response.status_code != 200:
                        raise RuntimeError(f"SSE status {response.status_code}")
                    self.connects += 1
                    if had_failure:
                        self.recovered_streams += 1
                        had_failure = False
                    async for line in response.aiter_lines():
                        now = time.monotonic()
                        self.gaps_s.append(now - last_signal)
                        last_signal = now
                        if line.startswith("data:") and line[5:].strip() == "[DONE]":
                            self.done_streams += 1
                            saw_done = True
                        if stop.is_set():
                            break
                    if not stop.is_set() and not saw_done:
                        self.stream_failures += 1
                        had_failure = True
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, RuntimeError):
                if stop.is_set():
                    break
                self.stream_failures += 1
                had_failure = True
                await asyncio.sleep(0.05)

    def metrics(self) -> dict[str, object]:
        return {
            "streams": len(self.session_ids),
            "connects": self.connects,
            "stream_failures": self.stream_failures,
            "recovered_streams": self.recovered_streams,
            "unrecovered_streams": max(0, self.stream_failures - self.recovered_streams),
            "done_streams": self.done_streams,
            "max_gap_s": max(self.gaps_s, default=0.0),
            "heartbeat_s": _HEARTBEAT_S,
            "gap_limit_s": _SSE_GAP_LIMIT_S,
        }


class TurnDriver:
    """Drive real host-bound turns and resolve the generated approvals."""

    def __init__(
        self,
        urls: tuple[str, ...],
        session_urls: dict[str, str],
        out_dir: Path,
        turns_per_session: int = 3,
        require_approvals: bool = True,
        rollout_timing: _RolloutTiming | None = None,
    ) -> None:
        self.urls = urls
        self.session_urls = session_urls
        self.out_dir = out_dir
        self.turns_per_session = turns_per_session
        self.require_approvals = require_approvals
        self.rollout_timing = rollout_timing or _RolloutTiming(
            _DEFAULT_REPLICAS, _DEFAULT_ROLLOUT_SETTLE_S
        )
        self.expected_turns = len(session_urls) * turns_per_session
        self.completed_turns = 0
        self.lost_turns = 0
        self.duplicate_turns = 0
        self.expected_approvals = self.expected_turns if require_approvals else 0
        self.approval_ids: set[str] = set()
        self.approval_errors = 0
        self.markers: dict[str, list[str]] = {session_id: [] for session_id in session_urls}
        self.started = asyncio.Event()

    async def run(self, stop: asyncio.Event) -> None:
        async with httpx.AsyncClient(timeout=5.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            tasks = [
                asyncio.create_task(self._run_session(client, session_id, stop))
                for session_id in self.session_urls
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._check_markers()
        (self.out_dir / "turns.json").write_text(
            json.dumps(self.metrics(), indent=2, sort_keys=True) + "\n"
        )

    async def _run_session(
        self, client: httpx.AsyncClient, session_id: str, stop: asyncio.Event
    ) -> None:
        for turn_index in range(self.turns_per_session):
            if stop.is_set() and turn_index > 0:
                return
            marker = f"rollout-turn-{session_id[:6]}-{turn_index}-{uuid.uuid4().hex[:8]}"
            self.markers[session_id].append(marker)
            base_url = self.session_urls[session_id].rstrip("/")
            try:
                response = await client.post(
                    f"{base_url}/v1/sessions/{session_id}/events",
                    json={
                        "type": "message",
                        "data": {
                            "role": "user",
                            "content": [{"type": "input_text", "text": marker}],
                        },
                    },
                )
                response.raise_for_status()
                self.started.set()
            except httpx.HTTPError:
                self.lost_turns += 1
                continue
            if not await self._wait_for_turn(client, session_id, marker):
                self.lost_turns += 1
                continue
            self.completed_turns += 1

    async def _wait_for_turn(
        self, client: httpx.AsyncClient, session_id: str, marker: str
    ) -> bool:
        deadline = time.monotonic() + self.rollout_timing.turn_completion_budget_s
        seen_busy = False
        url_index = 0
        while time.monotonic() < deadline:
            base_url = self.urls[url_index % len(self.urls)].rstrip("/")
            url_index += 1
            try:
                response = await client.get(f"{base_url}/v1/sessions/{session_id}")
                if response.status_code != 200:
                    await asyncio.sleep(0.1)
                    continue
                body = response.json()
                status = body.get("status")
                if status in {"running", "waiting"}:
                    seen_busy = True
                pending = body.get("pending_elicitations") or []
                for elicitation in pending:
                    if not isinstance(elicitation, dict):
                        continue
                    elicitation_id = elicitation.get("elicitation_id")
                    if not isinstance(elicitation_id, str) or elicitation_id in self.approval_ids:
                        continue
                    if await self._approve(client, session_id, elicitation_id):
                        self.approval_ids.add(elicitation_id)
                    else:
                        self.approval_errors += 1
                marker_seen = _json_contains(body, marker) > 0
                if status == "failed":
                    return False
                if status == "idle" and (seen_busy or marker_seen) and not pending:
                    return True
            except (httpx.HTTPError, ValueError):
                pass
            await asyncio.sleep(0.1)
        return False

    async def _approve(
        self, client: httpx.AsyncClient, session_id: str, elicitation_id: str
    ) -> bool:
        for url in self.urls:
            try:
                response = await client.post(
                    f"{url.rstrip('/')}/v1/sessions/{session_id}/events",
                    json={
                        "type": "approval",
                        "data": {"elicitation_id": elicitation_id, "action": "accept"},
                    },
                )
                if response.status_code in {200, 202}:
                    return True
            except httpx.HTTPError:
                continue
        return False

    async def _check_markers(self) -> None:
        async with httpx.AsyncClient(timeout=5.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            for session_id, markers in self.markers.items():
                for url in self.urls:
                    try:
                        response = await client.get(
                            f"{url.rstrip('/')}/v1/sessions/{session_id}/items",
                            params={"limit": 1000},
                        )
                        if response.status_code == 200:
                            text = json.dumps(response.json())
                            for marker in markers:
                                count = text.count(marker)
                                if count > 1:
                                    self.duplicate_turns += count - 1
                            break
                    except (httpx.HTTPError, ValueError):
                        continue

    def metrics(self) -> dict[str, object]:
        return {
            "expected_turns": self.expected_turns,
            "completed_turns": self.completed_turns,
            "lost_turns": self.lost_turns,
            "duplicate_turns": self.duplicate_turns,
            "expected_approvals": self.expected_approvals,
            "observed_approvals": len(self.approval_ids),
            "approval_errors": self.approval_errors,
            "approval_check_exercised": self.require_approvals,
            "turn_completion_budget_s": self.rollout_timing.turn_completion_budget_s,
            "session_ids": sorted(self.session_urls),
        }


class RunnerObserver:
    """Observe online transitions through every replica's status endpoint."""

    def __init__(
        self,
        urls: tuple[str, ...],
        runner_ids: tuple[str, ...],
        log_paths: tuple[Path, ...],
        expected_disconnects: int,
    ) -> None:
        self.urls = urls
        self.runner_ids = runner_ids
        self.log_paths = log_paths
        self.expected_disconnects = expected_disconnects
        self.disconnects: list[dict[str, object]] = []
        self.reconnect_latencies: list[float] = []
        self.all_unregistered_polls = 0
        self.online_now: set[str] = set()

    async def run(self, stop: asyncio.Event) -> None:
        previous: dict[str, bool] = dict.fromkeys(self.runner_ids, True)
        disconnected_at: dict[str, float] = {}
        async with httpx.AsyncClient(timeout=2.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            while not stop.is_set():
                online: set[str] = set()
                for runner_id in self.runner_ids:
                    is_online = False
                    for url in self.urls:
                        try:
                            response = await client.get(
                                f"{url.rstrip('/')}/v1/runners/{runner_id}/status"
                            )
                            if (
                                response.status_code == 200
                                and response.json().get("online") is True
                            ):
                                is_online = True
                                break
                        except (httpx.HTTPError, ValueError):
                            continue
                    if is_online:
                        online.add(runner_id)
                    if previous[runner_id] and not is_online:
                        disconnected_at[runner_id] = time.monotonic()
                        self.disconnects.append(
                            {"runner_id": runner_id, "at": time.time(), "close_code": None}
                        )
                    elif not previous[runner_id] and is_online and runner_id in disconnected_at:
                        self.reconnect_latencies.append(
                            time.monotonic() - disconnected_at.pop(runner_id)
                        )
                    previous[runner_id] = is_online
                self.online_now = online
                if not online:
                    self.all_unregistered_polls += 1
                await asyncio.sleep(0.1)
        self._fill_close_codes()

    def _fill_close_codes(self) -> None:
        codes_by_runner: dict[str, list[int]] = {runner_id: [] for runner_id in self.runner_ids}
        unscoped_codes: list[int] = []
        seen_paths: set[Path] = set()
        for path in self.log_paths:
            paths = sorted(path.glob("*.log")) if path.is_dir() else [path]
            for log_path in paths:
                if log_path in seen_paths or not log_path.exists():
                    continue
                seen_paths.add(log_path)
                try:
                    lines = log_path.read_text(errors="replace").splitlines()
                except OSError:
                    continue
                for line in lines:
                    codes = [int(match.group("code")) for match in _CLOSE_CODE_RE.finditer(line)]
                    if not codes:
                        continue
                    owners = [runner_id for runner_id in self.runner_ids if runner_id in line]
                    if owners:
                        for runner_id in owners:
                            codes_by_runner[runner_id].extend(codes)
                    else:
                        unscoped_codes.extend(codes)

        fallback_index = 0
        for item in self.disconnects:
            runner_id = str(item["runner_id"])
            codes = codes_by_runner.get(runner_id, [])
            if codes:
                # Preserve a non-1012 observation if sources disagree so the
                # close-code assertion cannot turn conflicting evidence into a pass.
                item["close_code"] = next((code for code in codes if code != 1012), codes[0])
            elif fallback_index < len(unscoped_codes):
                item["close_code"] = unscoped_codes[fallback_index]
                fallback_index += 1

    def metrics(self) -> dict[str, object]:
        return {
            "runner_ids": list(self.runner_ids),
            "expected_disconnects": self.expected_disconnects,
            "disconnects": self.disconnects,
            "reconnect_latencies_s": self.reconnect_latencies,
            "max_reconnect_s": max(self.reconnect_latencies, default=None),
            "all_unregistered_polls": self.all_unregistered_polls,
            "online_at_stop": sorted(self.online_now),
        }


@dataclass
class PodMetrics:
    max_terminating: int = 0
    max_total: int = 0


@dataclass
class HostHandle:
    host_id: str
    workspace: Path
    process: subprocess.Popen[bytes]
    log_path: Path
    server_url: str
    runner_log_dir: Path


class LocalStack:
    """Three local server processes sharing one database and mock LLM."""

    def __init__(
        self,
        out_dir: Path,
        failure_mode: str = "graceful",
        replicas: int = _DEFAULT_REPLICAS,
        require_approvals: bool = False,
    ) -> None:
        self.out_dir = out_dir
        self.failure_mode = failure_mode
        self.replicas = replicas
        self.require_approvals = require_approvals
        self.tmp = out_dir / "local-stack"
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.db_path = self.tmp / "rollout.db"
        self.db_uri = f"sqlite:///{self.db_path}"
        self.artifact_dir = self.tmp / "artifacts"
        self.artifact_dir.mkdir(exist_ok=True)
        self.mock_url = ""
        self.mock_process: subprocess.Popen[bytes] | None = None
        self.servers: list[subprocess.Popen[bytes] | None] = []
        self.server_logs: list[Path] = []
        self.server_hosts: list[str] = []
        self.ports: list[int] = []
        self.server_tokens: list[str] = []
        self.hosts: list[HostHandle] = []
        self.session_urls: dict[str, str] = {}
        self.runner_ids: list[str] = []
        self.agent_id = ""
        self.pod_metrics = PodMetrics()
        self._mock_log = self.tmp / "mock.log"
        self._agent_name = "rollout-resilience-agent"

    @property
    def urls(self) -> tuple[str, ...]:
        return tuple(
            f"http://{host}:{port}"
            for host, port in zip(self.server_hosts, self.ports, strict=True)
        )

    @property
    def runner_log_dirs(self) -> tuple[Path, ...]:
        return tuple(host.runner_log_dir for host in self.hosts)

    async def start(self) -> None:
        mock_port = _free_port()
        self.mock_url = f"http://127.0.0.1:{mock_port}"
        mock_log = self._mock_log.open("wb")
        self.mock_process = subprocess.Popen(
            [sys.executable, str(_MOCK_SERVER), str(mock_port)],
            cwd=str(_REPO_ROOT),
            env=self._base_env(),
            stdout=mock_log,
            stderr=subprocess.STDOUT,
        )
        await self._wait_for_status(f"{self.mock_url}/stats")
        server_port = _free_port()
        for index in range(self.replicas):
            self.server_hosts.append(f"127.0.0.{index + 1}")
            port = server_port
            self.ports.append(port)
            self.server_tokens.append(uuid.uuid4().hex)
            self._spawn_server(index, port)
            await self._wait_for_status(f"http://127.0.0.1:{port}/health", timeout=90.0)
        await self._configure_mock()
        self.agent_id = await self._register_agent()
        for index in range(_HOST_COUNT):
            await self._spawn_host(index)
        for host in self.hosts:
            session_id = await self._create_hosted_session(host)
            self.session_urls[session_id] = host.server_url
            runner_id = await self._wait_for_runner(session_id)
            self.runner_ids.append(runner_id)
        self.pod_metrics.max_total = len(self.servers)

    def _base_env(self) -> dict[str, str]:
        pythonpath = str(_REPO_ROOT)
        if os.environ.get("PYTHONPATH"):
            pythonpath += os.pathsep + os.environ["PYTHONPATH"]
        return {
            **os.environ,
            "PYTHONPATH": pythonpath,
            "OMNIGENT_AUTH_PROVIDER": "header",
            "OMNIGENT_AUTH_ENABLED": "0",
            "OMNIGENT_LOCAL_SINGLE_USER": "1",
            "OMNIGENT_SERVER_SHUTDOWN_TIMEOUT_S": "20",
            "OPENAI_API_KEY": "mock-key",
            "OPENAI_BASE_URL": f"{self.mock_url}/v1",
        }

    def _server_config(self) -> Path:
        config_path = self.tmp / "server.yaml"
        config_path.write_text(
            yaml.safe_dump(
                {
                    "llm": {
                        "model": _POLICY_LLM_KEY,
                        "connection": {
                            "base_url": f"{self.mock_url}/v1",
                            "api_key": "mock-key",
                        },
                    }
                }
            )
        )
        return config_path

    def _spawn_server(self, index: int, port: int) -> None:
        log_path = self.tmp / f"server-{index}.log"
        if index >= len(self.server_logs):
            self.server_logs.append(log_path)
        env = {
            **self._base_env(),
            "OMNIGENT_POD_ADDR": f"{self.server_hosts[index]}:{port}",
            "OMNIGENT_RUNNER_TUNNEL_TOKEN": self.server_tokens[index],
        }
        args = [
            sys.executable,
            "-m",
            "omnigent.cli",
            "server",
            "--host",
            self.server_hosts[index],
            "--port",
            str(port),
            "--database-uri",
            self.db_uri,
            "--artifact-location",
            str(self.artifact_dir),
            "--config",
            str(self._server_config()),
        ]
        handle = log_path.open("ab")
        process = subprocess.Popen(
            args,
            cwd=str(_REPO_ROOT),
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        if index == len(self.servers):
            self.servers.append(process)
        else:
            self.servers[index] = process

    async def _wait_for_status(self, url: str, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        async with httpx.AsyncClient(timeout=2.0) as client:
            while time.monotonic() < deadline:
                try:
                    response = await client.get(url)
                    if response.status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.2)
        raise RuntimeError(f"did not become ready: {url}")

    async def _configure_mock(self) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for payload in (
                {"key": _POLICY_LLM_KEY, "text": '{"action":"allow","reason":""}'},
                {"key": "default", "text": _mock_reply(40), "stream": True},
            ):
                response = await client.post(f"{self.mock_url}/mock/set_fallback", json=payload)
                response.raise_for_status()
            response = await client.post(
                f"{self.mock_url}/mock/configure",
                json={
                    "key": _MODEL,
                    "responses": [
                        {"text": _mock_reply(40), "stream": True, "block": True},
                        {"text": _mock_reply(40), "stream": True, "block": True},
                    ],
                },
            )
            response.raise_for_status()

    def _agent_bundle(self) -> bytes:
        config: dict[str, object] = {
            "spec_version": 1,
            "name": self._agent_name,
            "prompt": "You are a deterministic assistant used by the rollout proof harness.",
            "executor": {
                "type": "omnigent",
                "model": _MODEL,
                "config": {"harness": "openai-agents"},
                "auth": {
                    "type": "api_key",
                    "api_key": "mock-key",
                    "base_url": f"{self.mock_url}/v1",
                },
                "connection": {"base_url": f"{self.mock_url}/v1", "api_key": "mock-key"},
            },
            "os_env": {"type": "caller_process", "cwd": ".", "sandbox": {"type": "none"}},
        }
        if self.require_approvals:
            config["policies"] = {
                "always_ask_on_input": {
                    "type": "function",
                    "on": ["request"],
                    "function": {
                        "path": "omnigent.policies.function.make_fixed_action_callable",
                        "arguments": {
                            "action": "ask",
                            "reason": "Confirm this rollout-proof turn.",
                            "on_phases": ["request"],
                        },
                    },
                }
            }

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as archive:
            payload = yaml.safe_dump(config).encode()
            info = tarfile.TarInfo("config.yaml")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
        return buf.getvalue()

    async def _register_agent(self) -> str:
        async with httpx.AsyncClient(timeout=20.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            response = await client.post(
                f"{self.urls[0]}/v1/sessions",
                data={"metadata": "{}"},
                files={"bundle": ("agent.tar.gz", self._agent_bundle(), "application/gzip")},
            )
            if response.status_code not in {200, 201, 409}:
                raise RuntimeError(
                    f"agent registration failed: {response.status_code} {response.text[:400]}"
                )
            listing = await client.get(
                f"{self.urls[0]}/v1/sessions", params={"agent_name": self._agent_name, "limit": 1}
            )
            listing.raise_for_status()
            return str(listing.json()["data"][0]["agent_id"])

    async def _spawn_host(self, index: int) -> None:
        host_id = uuid.uuid4().hex
        workspace = self.tmp / f"host-{index}"
        home = workspace / "home"
        workspace.mkdir(parents=True, exist_ok=True)
        home.mkdir(exist_ok=True)
        data_dir = workspace / "data"
        data_dir.mkdir(exist_ok=True)
        log_path = workspace / "host.log"
        env = {
            **self._base_env(),
            "HOME": str(home),
            "OMNIGENT_DATA_DIR": str(data_dir),
            "OMNIGENT_HOST_ID": host_id,
            "OMNIGENT_HOST_NAME": f"rollout-host-{index}",
        }
        server_url = self.urls[index % len(self.urls)]
        handle = log_path.open("wb")
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "omnigent",
                "host",
                "--server",
                server_url,
                "--non-interactive",
            ],
            cwd=str(workspace),
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        host = HostHandle(
            host_id,
            workspace,
            process,
            log_path,
            server_url,
            data_dir / "logs" / "runner",
        )
        self.hosts.append(host)
        await self._wait_for_host(host)

    async def _wait_for_host(self, host: HostHandle) -> None:
        deadline = time.monotonic() + 60.0
        async with httpx.AsyncClient(timeout=2.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            while time.monotonic() < deadline:
                if host.process.poll() is not None:
                    tail = host.log_path.read_text(errors="replace")[-1000:]
                    raise RuntimeError(f"host {host.host_id} exited: {tail}")
                try:
                    response = await client.get(f"{host.server_url}/v1/hosts")
                    if response.status_code == 200 and any(
                        item.get("host_id") == host.host_id and item.get("status") == "online"
                        for item in response.json().get("hosts", [])
                    ):
                        return
                except (httpx.HTTPError, ValueError):
                    pass
                await asyncio.sleep(0.2)
        raise RuntimeError(f"host {host.host_id} did not register")

    async def _create_hosted_session(self, host: HostHandle) -> str:
        async with httpx.AsyncClient(timeout=20.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            response = await client.post(
                f"{host.server_url}/v1/sessions",
                json={
                    "agent_id": self.agent_id,
                    "host_id": host.host_id,
                    "host_type": "external",
                    "workspace": str(host.workspace),
                    "title": f"rollout-resilience-{host.host_id[:8]}",
                },
            )
            response.raise_for_status()
            return str(response.json()["id"])

    async def _wait_for_runner(self, session_id: str) -> str:
        deadline = time.monotonic() + 90.0
        async with httpx.AsyncClient(timeout=3.0, headers={"Origin": _INTERNAL_ORIGIN}) as client:
            while time.monotonic() < deadline:
                for url in self.urls:
                    try:
                        response = await client.get(f"{url}/v1/sessions/{session_id}")
                        if response.status_code != 200:
                            continue
                        runner_id = response.json().get("runner_id")
                        if not isinstance(runner_id, str) or not runner_id:
                            continue
                        status = await client.get(f"{url}/v1/runners/{runner_id}/status")
                        if status.status_code == 200 and status.json().get("online") is True:
                            return runner_id
                    except (httpx.HTTPError, ValueError):
                        continue
                await asyncio.sleep(0.2)
        raise RuntimeError(f"runner for {session_id} did not become online")

    async def pending_mock_requests(self) -> int:
        async with httpx.AsyncClient(timeout=2.0) as client:
            gate_response = await client.get(f"{self.mock_url}/gate/pending")
            if gate_response.status_code != 200 or not gate_response.json().get("pending"):
                return 0
            # The mock exposes a Boolean gate flag, so count captured requests
            # for the queue whose first configured responses are blocking.
            response = await client.get(
                f"{self.mock_url}/mock/requests",
                params={"key": _MODEL},
            )
            if response.status_code != 200:
                return 0
            requests = response.json().get("requests", [])
            return len(requests) if isinstance(requests, list) else 0

    async def wait_for_gates(self, count: int) -> None:
        deadline = time.monotonic() + 45.0
        while time.monotonic() < deadline:
            if await self.pending_mock_requests() >= count:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"mock LLM did not reach {count} blocked requests")

    async def release_gates(self) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            while await self.pending_mock_requests():
                response = await client.post(f"{self.mock_url}/gate/release")
                response.raise_for_status()

    async def roll_all(self, settle_s: float) -> None:
        for index in range(len(self.servers)):
            await self._replace(index)
            if index + 1 < len(self.servers):
                await asyncio.sleep(settle_s)

    async def _replace(self, index: int) -> None:
        old = self.servers[index]
        if old is not None and old.poll() is None:
            old.send_signal(signal.SIGKILL if self.failure_mode == "hard-kill" else signal.SIGTERM)
        self.pod_metrics.max_terminating = max(self.pod_metrics.max_terminating, 1)
        if old is not None:
            try:
                await asyncio.to_thread(old.wait, 30)
            except subprocess.TimeoutExpired:
                old.kill()
                await asyncio.to_thread(old.wait, 5)
        self.servers[index] = None
        self._spawn_server(index, self.ports[index])
        self.pod_metrics.max_total = max(self.pod_metrics.max_total, len(self.servers))
        await self._wait_for_status(f"{self.urls[index]}/health", timeout=90.0)

    async def stop(self) -> None:
        for host in reversed(self.hosts):
            if host.process.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(os.getpgid(host.process.pid), signal.SIGTERM)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    await asyncio.to_thread(host.process.wait, 10)
        for process in self.servers:
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGTERM)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    await asyncio.to_thread(process.wait, 25)
        if self.mock_process is not None and self.mock_process.poll() is None:
            self.mock_process.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                await asyncio.to_thread(self.mock_process.wait, 5)


class KubernetesTarget:
    """Replace one ready pod at a time while the common drivers run.

    An operator supplies a service URL, live session and runner IDs, the
    database URL, namespace, and pod label selector. The drivers and
    assertions are identical to local mode; only this adapter uses kubectl.
    """

    def __init__(
        self,
        base_url: str,
        namespace: str,
        selector: str,
        graceful_timeout_s: float = 20.0,
    ) -> None:
        self.urls = (base_url.rstrip("/"),)
        self.namespace = namespace
        self.selector = selector
        self.graceful_timeout_s = graceful_timeout_s
        self.pod_metrics = PodMetrics()

    async def _kubectl(self, *args: str) -> str:
        command = ["kubectl", "-n", self.namespace, *args]
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(stderr.decode(errors="replace"))
        return stdout.decode(errors="replace")

    async def _pods(self) -> list[dict[str, Any]]:
        raw = await self._kubectl("get", "pods", "-l", self.selector, "-o", "json")
        return list(json.loads(raw).get("items", []))

    async def roll_all(self, delay_s: float = 0.5) -> None:
        initial = [str(item["metadata"]["name"]) for item in await self._pods()]
        for pod_name in initial:
            await self._kubectl(
                "delete",
                "pod",
                pod_name,
                "--wait=false",
                f"--grace-period={math.ceil(self.graceful_timeout_s)}",
            )
            await self._wait_for_replacement(pod_name, desired_replicas=len(initial))
            await asyncio.sleep(delay_s)

    async def _wait_for_replacement(self, old_name: str, desired_replicas: int) -> None:
        deadline = time.monotonic() + 180.0
        while time.monotonic() < deadline:
            pods = await self._pods()
            terminating = sum(1 for pod in pods if pod["metadata"].get("deletionTimestamp"))
            self.pod_metrics.max_terminating = max(self.pod_metrics.max_terminating, terminating)
            self.pod_metrics.max_total = max(self.pod_metrics.max_total, len(pods))
            ready = sum(
                1
                for pod in pods
                if pod["status"].get("phase") == "Running"
                and any(
                    condition.get("status") == "True"
                    for condition in pod["status"].get("conditions", [])
                    if condition.get("type") == "Ready"
                )
            )
            if (
                old_name not in {str(pod["metadata"]["name"]) for pod in pods}
                and ready >= desired_replicas
                and terminating == 0
            ):
                return
            await asyncio.sleep(0.5)
        raise RuntimeError(f"replacement for pod {old_name} did not become ready")


def _read_runner_tunnels(
    database_uri: str, valid_owner_addresses: set[str]
) -> tuple[set[str], set[str], str | None]:
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(database_uri)
        with engine.connect() as connection:
            rows = connection.execute(
                text("SELECT runner_id, owner_addr FROM runner_tunnels")
            ).all()
        engine.dispose()
        row_ids = {str(row[0]) for row in rows}
        bad_addresses = {str(row[1]) for row in rows if str(row[1]) not in valid_owner_addresses}
        return row_ids, bad_addresses, None
    except Exception as exc:  # noqa: BLE001 — report database evidence as an assertion
        return set(), set(), repr(exc)


def _add_assertions(
    report: HarnessReport,
    *,
    http_metrics: dict[str, object],
    sse_metrics: dict[str, object],
    turn_metrics: dict[str, object],
    runner_metrics: dict[str, object],
    expected_disconnects: int,
    pod_metrics: PodMetrics,
    database_uri: str,
    live_runner_ids: set[str],
    valid_owner_addresses: set[str],
) -> None:
    """Apply the acceptance gate to measurements from any rollout target."""
    report.metadata.update(
        {
            "http": http_metrics,
            "sse": sse_metrics,
            "turns": turn_metrics,
            "runners": runner_metrics,
            "pods": pod_metrics.__dict__,
        }
    )
    report.add(
        "prober_zero_5xx",
        http_metrics["unhandled_5xx"] == 0,
        f"{http_metrics['unhandled_5xx']} unhandled 5xx responses",
        **http_metrics,
    )
    report.add(
        "prober_503_retry",
        http_metrics["retry_failures"] == 0,
        f"{http_metrics['retryable_503']} 503 responses retried; "
        f"{http_metrics['retry_failures']} retry failures",
        **http_metrics,
    )
    p99_limit = cast(float | None, http_metrics["rollout_p99_limit_ms"])
    rollout_p99 = cast(float | None, http_metrics["rollout_p99_ms"])
    report.add(
        "prober_p99_within_2x_steady_state",
        p99_limit is not None and rollout_p99 is not None and rollout_p99 <= p99_limit,
        f"rollout p99={rollout_p99}ms, limit={p99_limit}ms",
        **http_metrics,
    )
    report.add(
        "sse_streams_recover",
        sse_metrics["unrecovered_streams"] == 0,
        f"{sse_metrics['unrecovered_streams']} SSE streams did not recover",
        **sse_metrics,
    )
    report.add(
        "sse_no_done_sentinel",
        sse_metrics["done_streams"] == 0,
        f"{sse_metrics['done_streams']} streams received [DONE]",
        **sse_metrics,
    )
    max_gap = cast(float, sse_metrics["max_gap_s"])
    report.add(
        "sse_gap_under_20s",
        max_gap <= _SSE_GAP_LIMIT_S,
        f"max event gap={max_gap:.3f}s against {_HEARTBEAT_S:.0f}s heartbeat",
        **sse_metrics,
    )
    report.add(
        "turns_complete",
        turn_metrics["lost_turns"] == 0
        and turn_metrics["completed_turns"] == turn_metrics["expected_turns"],
        f"{turn_metrics['completed_turns']}/{turn_metrics['expected_turns']} turns completed",
        **turn_metrics,
    )
    report.add(
        "sessions_not_lost_or_duplicated",
        turn_metrics["lost_turns"] == 0 and turn_metrics["duplicate_turns"] == 0,
        f"lost={turn_metrics['lost_turns']}, duplicated={turn_metrics['duplicate_turns']}",
        **turn_metrics,
    )
    approval_check_exercised = bool(
        turn_metrics.get("approval_check_exercised", turn_metrics["expected_approvals"] > 0)
    )
    if approval_check_exercised:
        report.add(
            "approval_verdicts_not_dropped",
            turn_metrics["approval_errors"] == 0
            and turn_metrics["observed_approvals"] == turn_metrics["expected_approvals"],
            f"{turn_metrics['observed_approvals']}/{turn_metrics['expected_approvals']} "
            "approvals accepted",
            **turn_metrics,
        )
    else:
        report.add(
            "approval_verdicts_not_dropped",
            True,
            "not exercised: this acceptance run does not configure an approval policy",
            **turn_metrics,
        )
    disconnects = cast(list[dict[str, object]], runner_metrics["disconnects"])
    reconnect_latencies = cast(list[float], runner_metrics["reconnect_latencies_s"])
    disconnect_codes = [item.get("close_code") for item in disconnects]
    report.add(
        "runner_disconnects_are_1012",
        len(disconnects) >= expected_disconnects
        and all(code == 1012 for code in disconnect_codes),
        f"disconnects={len(disconnect_codes)}, close codes={disconnect_codes}",
        **runner_metrics,
    )
    report.add(
        "runner_reregisters_under_3s",
        len(reconnect_latencies) >= expected_disconnects
        and max(reconnect_latencies, default=float("inf")) < _REREGISTRATION_LIMIT_S,
        f"max re-registration latency={runner_metrics['max_reconnect_s']}s",
        **runner_metrics,
    )
    report.add(
        "runners_never_all_unregistered",
        runner_metrics["all_unregistered_polls"] == 0,
        f"all runners absent in {runner_metrics['all_unregistered_polls']} samples",
        **runner_metrics,
    )
    report.add(
        "pods_one_terminating",
        pod_metrics.max_terminating <= 1,
        f"max terminating processes={pod_metrics.max_terminating}",
        **pod_metrics.__dict__,
    )
    report.add(
        "pods_total_under_4",
        pod_metrics.max_total <= 4,
        f"max total processes={pod_metrics.max_total}",
        **pod_metrics.__dict__,
    )
    row_ids, bad_addresses, db_error = _read_runner_tunnels(database_uri, valid_owner_addresses)
    report.add(
        "runner_tunnels_equal_live_runners",
        db_error is None and row_ids == live_runner_ids,
        f"rows={sorted(row_ids)}, live={sorted(live_runner_ids)}"
        if db_error is None
        else db_error,
        row_ids=sorted(row_ids),
        live_runner_ids=sorted(live_runner_ids),
        db_error=db_error,
    )
    report.add(
        "runner_tunnels_have_existing_pod_addresses",
        db_error is None and not bad_addresses,
        f"invalid owner addresses={sorted(bad_addresses)}" if db_error is None else db_error,
        invalid_owner_addresses=sorted(bad_addresses),
        valid_owner_addresses=sorted(valid_owner_addresses),
        db_error=db_error,
    )


async def _run_local(args: argparse.Namespace, out_dir: Path) -> HarnessReport:
    started = time.monotonic()
    rollout_timing = _RolloutTiming(args.replicas, args.rollout_settle_s)
    rollout_timing.log()
    stack = LocalStack(
        out_dir,
        failure_mode=args.failure_mode,
        replicas=rollout_timing.pod_count,
        require_approvals=args.expect_approvals,
    )
    report = HarnessReport(
        mode="local",
        failure_mode=args.failure_mode,
        generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        duration_s=0.0,
    )
    try:
        await stack.start()
        urls = stack.urls
        session_ids = tuple(stack.session_urls)
        prober = HttpProber(urls, session_ids, out_dir)
        sse = SseSubscriber(urls, session_ids, out_dir)
        turns = TurnDriver(
            urls,
            stack.session_urls,
            out_dir,
            require_approvals=args.expect_approvals,
            rollout_timing=rollout_timing,
        )
        observer = RunnerObserver(
            urls,
            tuple(stack.runner_ids),
            (
                *stack.server_logs,
                *(host.log_path for host in stack.hosts),
                *stack.runner_log_dirs,
            ),
            expected_disconnects=len(stack.runner_ids),
        )
        stop = asyncio.Event()
        phase = {"value": "steady"}
        tasks = [
            asyncio.create_task(prober.run(stop, phase)),
            asyncio.create_task(sse.run(stop)),
            asyncio.create_task(turns.run(stop)),
            asyncio.create_task(observer.run(stop)),
        ]
        await stack.wait_for_gates(_HOST_COUNT)
        await asyncio.sleep(args.baseline_s)
        phase["value"] = "rollout"
        rollout_started = time.monotonic()
        await stack.roll_all(rollout_timing.settle_s)
        await stack.release_gates()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(tasks[2]), timeout=60.0)
        remaining = args.duration_s - (time.monotonic() - rollout_started)
        if remaining > 0:
            await asyncio.sleep(remaining)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        http_metrics = prober.metrics()
        sse_metrics = sse.metrics()
        turn_metrics = turns.metrics()
        runner_metrics = observer.metrics()
        report.metadata.update(
            {
                "replicas": len(stack.servers),
                "server_urls": list(urls),
                "session_ids": list(session_ids),
                "runner_ids": list(stack.runner_ids),
                "database_uri": stack.db_uri,
                "approval_check_exercised": args.expect_approvals,
                "rollout_settle_s": args.rollout_settle_s,
                **rollout_timing.as_json(),
            }
        )
        _add_assertions(
            report,
            http_metrics=http_metrics,
            sse_metrics=sse_metrics,
            turn_metrics=turn_metrics,
            runner_metrics=runner_metrics,
            expected_disconnects=len(stack.runner_ids),
            pod_metrics=stack.pod_metrics,
            database_uri=stack.db_uri,
            live_runner_ids=set(stack.runner_ids),
            valid_owner_addresses={
                f"{host}:{port}"
                for host, port in zip(stack.server_hosts, stack.ports, strict=True)
            },
        )
    except Exception as exc:  # noqa: BLE001 — startup and teardown failures are gate evidence
        report.add("harness_started_and_completed", False, repr(exc))
        report.metadata["exception"] = repr(exc)
    finally:
        await stack.stop()
        report.duration_s = time.monotonic() - started
        report.write(out_dir)
    return report


async def _run_kubernetes(args: argparse.Namespace, out_dir: Path) -> HarnessReport:
    started = time.monotonic()
    report = HarnessReport(
        mode="kubernetes",
        failure_mode=args.failure_mode,
        generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        duration_s=0.0,
    )
    target = KubernetesTarget(
        args.base_url,
        namespace=args.namespace,
        selector=args.selector,
        graceful_timeout_s=args.graceful_timeout_s,
    )
    session_ids = tuple(args.session_id)
    runner_ids = tuple(args.runner_id)
    try:
        if (
            not args.base_url
            or not session_ids
            or not runner_ids
            or not args.database_uri
            or not args.pod_address
        ):
            raise ValueError(
                "kubernetes mode requires --base-url, --session-id, --runner-id, "
                "--database-uri, and --pod-address"
            )
        session_urls = dict.fromkeys(session_ids, target.urls[0])
        prober = HttpProber(target.urls, session_ids, out_dir)
        sse = SseSubscriber(target.urls, session_ids, out_dir)
        turns = TurnDriver(
            target.urls,
            session_urls,
            out_dir,
            require_approvals=args.expect_approvals,
        )
        observer = RunnerObserver(
            target.urls,
            runner_ids,
            tuple(Path(path) for path in args.runner_log),
            expected_disconnects=len(runner_ids),
        )
        stop = asyncio.Event()
        phase = {"value": "steady"}
        tasks = [
            asyncio.create_task(prober.run(stop, phase)),
            asyncio.create_task(sse.run(stop)),
            asyncio.create_task(turns.run(stop)),
            asyncio.create_task(observer.run(stop)),
        ]
        await asyncio.sleep(args.baseline_s)
        phase["value"] = "rollout"
        rollout_started = time.monotonic()
        await target.roll_all(args.restart_delay_s)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(tasks[2]), timeout=60.0)
        remaining = args.duration_s - (time.monotonic() - rollout_started)
        if remaining > 0:
            await asyncio.sleep(remaining)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        report.metadata.update(
            {
                "server_urls": list(target.urls),
                "session_ids": list(session_ids),
                "runner_ids": list(runner_ids),
                "database_uri": args.database_uri,
            }
        )
        _add_assertions(
            report,
            http_metrics=prober.metrics(),
            sse_metrics=sse.metrics(),
            turn_metrics=turns.metrics(),
            runner_metrics=observer.metrics(),
            expected_disconnects=len(runner_ids),
            pod_metrics=target.pod_metrics,
            database_uri=args.database_uri,
            live_runner_ids=set(runner_ids),
            valid_owner_addresses=set(args.pod_address),
        )
    except Exception as exc:  # noqa: BLE001 — cluster failures are gate evidence
        report.add("harness_started_and_completed", False, repr(exc))
        report.metadata["exception"] = repr(exc)
    finally:
        report.duration_s = time.monotonic() - started
        report.write(out_dir)
    return report


def _print_report(report: HarnessReport) -> None:
    print(f"ROLL_OUT_RESILIENCE {'PASS' if report.passed else 'FAIL'}")
    for item in report.assertions:
        print(f"  {'PASS' if item.passed else 'FAIL'} {item.name}: {item.detail}")
    print(f"  JSON: {report.artifacts.get('json', '<not written>')}")


async def run_resilience(args: argparse.Namespace, out_dir: Path) -> HarnessReport:
    if args.mode == "local":
        report = await _run_local(args, out_dir)
    else:
        report = await _run_kubernetes(args, out_dir)
    _print_report(report)
    return report


def build_resilience_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rollout-resilience",
        action="store_true",
        help="Run the zero-downtime rollout proof instead of the ordinary Locust load.",
    )
    parser.add_argument("--mode", choices=("local", "kubernetes"), default="local")
    parser.add_argument("--replicas", type=int, default=_DEFAULT_REPLICAS)
    parser.add_argument("--failure-mode", choices=("graceful", "hard-kill"), default="graceful")
    parser.add_argument("--baseline-s", type=float, default=2.0)
    parser.add_argument("--duration-s", type=float, default=12.0)
    parser.add_argument(
        "--rollout-settle-s",
        type=float,
        default=_DEFAULT_ROLLOUT_SETTLE_S,
        help="Local seconds between replacements; models minReadySeconds: 30.",
    )
    parser.add_argument("--restart-delay-s", type=float, default=0.5)
    parser.add_argument("--base-url", default="")
    parser.add_argument("--session-id", action="append", default=[])
    parser.add_argument("--runner-id", action="append", default=[])
    parser.add_argument("--database-uri", default="")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--selector", default="app=omnigent")
    parser.add_argument("--graceful-timeout-s", type=float, default=20.0)
    parser.add_argument("--pod-address", action="append", default=[])
    parser.add_argument("--runner-log", action="append", default=[])
    parser.add_argument("--expect-approvals", action="store_true")


def resilience_main(args: argparse.Namespace, out_dir: Path) -> int:
    report = asyncio.run(run_resilience(args, out_dir))
    return 0 if report.passed else 1
