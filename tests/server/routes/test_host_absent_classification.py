"""Unit coverage for host-registry-miss classification."""

from __future__ import annotations

import pytest

from omnigent.errors import ErrorCode, OmnigentError
from omnigent.server.routes._host_launch import classify_host_absent, resolve_host_launch
from omnigent.stores.host_store import HOST_LIVENESS_TTL_S, Host, host_is_live, now_epoch

POD = "10.0.0.9:8000"
OWNER = "10.0.0.7:8000"


def _host(
    *,
    now: int,
    status: str = "online",
    age: int = 0,
    owner_addr: str | None = OWNER,
) -> Host:
    return Host(
        host_id="host_1",
        name="test-host",
        user_id="alice",
        status=status,
        created_at=now - 100,
        updated_at=now - age,
        owner_addr=owner_addr,
    )


CLASSIFIER_CASES = [
    ("online", 0, None, None, ErrorCode.WRONG_REPLICA, None),
    ("online", 0, None, POD, ErrorCode.WRONG_REPLICA, None),
    ("online", 0, OWNER, POD, ErrorCode.WRONG_REPLICA, OWNER),
    ("online", 0, POD, POD, ErrorCode.WRONG_REPLICA, None),
    ("online", 0, OWNER, None, ErrorCode.WRONG_REPLICA, None),
    ("online", HOST_LIVENESS_TTL_S + 1, OWNER, POD, ErrorCode.CONFLICT, None),
    # T12: stale owner must be offline even when forwarding is unwired.
    ("online", HOST_LIVENESS_TTL_S + 1, OWNER, None, ErrorCode.CONFLICT, None),
    ("online", HOST_LIVENESS_TTL_S + 1, None, POD, ErrorCode.CONFLICT, None),
    ("offline", 0, OWNER, POD, ErrorCode.CONFLICT, None),
    # T13: a fresh offline owner must not emit a forwarding instruction.
    ("offline", 0, OWNER, None, ErrorCode.CONFLICT, None),
    ("offline", HOST_LIVENESS_TTL_S + 1, OWNER, POD, ErrorCode.CONFLICT, None),
    ("online", HOST_LIVENESS_TTL_S, OWNER, POD, ErrorCode.WRONG_REPLICA, OWNER),
]

IDENTITY_CASES = [
    ("online", 0, None, ErrorCode.WRONG_REPLICA, None),
    ("online", HOST_LIVENESS_TTL_S, None, ErrorCode.WRONG_REPLICA, None),
    ("online", HOST_LIVENESS_TTL_S + 1, None, ErrorCode.CONFLICT, None),
    ("offline", 0, None, ErrorCode.CONFLICT, None),
    ("offline", HOST_LIVENESS_TTL_S, None, ErrorCode.CONFLICT, None),
    ("offline", HOST_LIVENESS_TTL_S + 1, None, ErrorCode.CONFLICT, None),
    ("online", 0, POD, ErrorCode.WRONG_REPLICA, None),
    ("online", HOST_LIVENESS_TTL_S, POD, ErrorCode.WRONG_REPLICA, None),
    ("online", HOST_LIVENESS_TTL_S + 1, POD, ErrorCode.CONFLICT, None),
    ("offline", 0, POD, ErrorCode.CONFLICT, None),
    ("offline", HOST_LIVENESS_TTL_S, POD, ErrorCode.CONFLICT, None),
    ("offline", HOST_LIVENESS_TTL_S + 1, POD, ErrorCode.CONFLICT, None),
]

PARITY_CASES = CLASSIFIER_CASES + [
    (status, age, None, pod_addr, expected_code, expected_owner)
    for status, age, pod_addr, expected_code, expected_owner in IDENTITY_CASES
]


@pytest.mark.parametrize(
    ("status", "age", "owner_addr", "pod_addr", "code", "expected_owner"),
    CLASSIFIER_CASES,
)
def test_host_absent_matrix(
    status: str,
    age: int,
    owner_addr: str | None,
    pod_addr: str | None,
    code: ErrorCode,
    expected_owner: str | None,
) -> None:
    now = now_epoch()
    err = classify_host_absent(
        _host(now=now, status=status, age=age, owner_addr=owner_addr),
        pod_addr=pod_addr,
        now=now,
    )
    assert (err.code, err.owner_addr) == (code, expected_owner)


@pytest.mark.parametrize(
    ("status", "age", "pod_addr", "expected_code", "expected_owner"),
    IDENTITY_CASES,
)
def test_owner_null_identity_matrix(
    status: str,
    age: int,
    pod_addr: str | None,
    expected_code: ErrorCode,
    expected_owner: str | None,
) -> None:
    now = now_epoch()
    err = classify_host_absent(
        _host(now=now, status=status, age=age, owner_addr=None),
        pod_addr=pod_addr,
        now=now,
    )
    assert (err.code, err.owner_addr) == (expected_code, expected_owner)


def test_launch_miss_matches_pinned_matrix() -> None:
    class Store:
        def __init__(self, row: Host) -> None:
            self.row = row

        def get_host(self, _host_id: str) -> Host:
            return self.row

    class Registry:
        def get(self, _host_id: str) -> None:
            return None

    for status, age, owner_addr, pod_addr, code, expected_owner in PARITY_CASES:
        now = now_epoch()
        host = _host(now=now, status=status, age=age, owner_addr=owner_addr)
        with pytest.raises(OmnigentError) as exc_info:
            resolve_host_launch(
                user_id="alice",
                host_id=host.host_id,
                session_id="session_1",
                host_store=Store(host),
                host_registry=Registry(),
                conversation_store=object(),  # type: ignore[arg-type]
                permission_store=None,
                pod_addr=pod_addr,
                now=now,
            )
        assert (exc_info.value.code, exc_info.value.owner_addr) == (code, expected_owner)


def test_ttl_edge_is_inclusive() -> None:
    now = now_epoch()
    host = _host(now=now, age=HOST_LIVENESS_TTL_S)
    assert host_is_live(host, now=now)
    assert classify_host_absent(host, pod_addr=POD, now=now).owner_addr == OWNER
