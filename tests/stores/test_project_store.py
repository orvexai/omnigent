"""Tests for :class:`SqlAlchemyProjectStore`.

Exercises ``create``, ``get``, ``list``, ``update`` and ``delete`` against a
real SQLite database, covering owner scoping and per-owner name uniqueness.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.exc import IntegrityError

from omnigent.errors import ErrorCode, OmnigentError
from omnigent.stores.project_store.sqlalchemy_store import SqlAlchemyProjectStore


# projects.id is a Uuid16 column (16 raw bytes) read back as bare 32-char hex.
# ``_uid`` maps a readable seed to a deterministic bare-hex UUID so tests stay
# legible while the store still round-trips real UUIDs.
def _uid(seed: str) -> str:
    """Deterministic bare 32-char hex UUID string from a short readable seed."""
    return uuid.uuid5(uuid.NAMESPACE_DNS, seed).hex


@pytest.fixture()
def store(db_uri: str) -> SqlAlchemyProjectStore:
    """A fresh :class:`SqlAlchemyProjectStore` backed by the test SQLite DB.

    :param db_uri: Per-test SQLite URI from the root conftest fixture.
    :returns: A ready-to-use :class:`SqlAlchemyProjectStore` instance.
    """
    return SqlAlchemyProjectStore(db_uri)


# ── create / get ──────────────────────────────────────────────────────────


def test_create_returns_project(store: SqlAlchemyProjectStore) -> None:
    """``create`` echoes the fields back and stamps ``created_at``."""
    project = store.create(_uid("p1"), "My Project", "alice@example.com")
    assert project.id == _uid("p1")
    assert project.name == "My Project"
    assert project.user_id == "alice@example.com"
    assert project.created_at > 0
    assert project.updated_at is None


def test_get_returns_created_project(store: SqlAlchemyProjectStore) -> None:
    """``get`` reads back a created project for its owner."""
    store.create(_uid("p1"), "My Project", "alice@example.com")
    got = store.get(_uid("p1"), user_id="alice@example.com")
    assert got is not None
    assert got.name == "My Project"


def test_get_missing_returns_none(store: SqlAlchemyProjectStore) -> None:
    """``get`` returns ``None`` for an unknown id."""
    assert store.get(_uid("nope"), user_id="alice@example.com") is None


def test_get_scoped_to_owner(store: SqlAlchemyProjectStore) -> None:
    """A project owned by someone else reads back as not found."""
    store.create(_uid("p1"), "Alice Project", "alice@example.com")
    assert store.get(_uid("p1"), user_id="bob@example.com") is None


# ── list ──────────────────────────────────────────────────────────────────


def test_list_orders_by_created_at_then_id(store: SqlAlchemyProjectStore) -> None:
    """``list`` orders by ``created_at ASC, id ASC``.

    Both rows are created in the same second here, so the ``id`` tiebreaker
    decides the order — assert against that rather than insertion order.
    """
    store.create(_uid("p1"), "First", "alice@example.com")
    store.create(_uid("p2"), "Second", "alice@example.com")
    listed = store.list(user_id="alice@example.com")
    assert {p.name for p in listed} == {"First", "Second"}
    # Whatever the tie order, it is ascending by (created_at, id).
    keys = [(p.created_at, p.id) for p in listed]
    assert keys == sorted(keys)


def test_list_scoped_to_owner(store: SqlAlchemyProjectStore) -> None:
    """``list`` only returns the requesting owner's projects."""
    store.create(_uid("p1"), "Alice Project", "alice@example.com")
    store.create(_uid("p2"), "Bob Project", "bob@example.com")
    alice = store.list(user_id="alice@example.com")
    assert [p.name for p in alice] == ["Alice Project"]


def test_list_empty(store: SqlAlchemyProjectStore) -> None:
    """``list`` returns an empty list when the owner has no projects."""
    assert store.list(user_id="alice@example.com") == []


# ── single-user (None owner) vs multi-user isolation ────────────────────────


def test_none_owner_and_named_owner_are_isolated(store: SqlAlchemyProjectStore) -> None:
    """The single-user ``None`` owner is a distinct scope from any named user.

    A project created in single-user mode (``user_id=None``) must not be
    visible to a named multi-user identity, and vice versa — the same DB can
    hold both without cross-leaking.
    """
    store.create(_uid("solo"), "Solo Project", None)
    store.create(_uid("alice"), "Alice Project", "alice@example.com")

    # Each scope lists only its own.
    assert [p.name for p in store.list(user_id=None)] == ["Solo Project"]
    assert [p.name for p in store.list(user_id="alice@example.com")] == ["Alice Project"]


def test_named_owner_cannot_get_none_owner_project(store: SqlAlchemyProjectStore) -> None:
    """A ``None``-owner project is not found for a named user (and vice versa)."""
    store.create(_uid("solo"), "Solo", None)
    assert store.get(_uid("solo"), user_id="alice@example.com") is None
    assert store.get(_uid("solo"), user_id=None) is not None


def test_named_owner_cannot_mutate_none_owner_project(store: SqlAlchemyProjectStore) -> None:
    """update / delete on a ``None``-owner project are no-ops for a named user."""
    store.create(_uid("solo"), "Solo", None)
    updated = store.update(_uid("solo"), user_id="alice@example.com", name="Hacked")
    assert updated is None
    deleted = store.delete(_uid("solo"), user_id="alice@example.com")
    assert deleted is False
    # Untouched for the real (None) owner.
    assert store.get(_uid("solo"), user_id=None).name == "Solo"


# ── name uniqueness ────────────────────────────────────────────────────────


def test_create_rejects_duplicate_name_per_owner(store: SqlAlchemyProjectStore) -> None:
    """Two projects with the same name for one owner are rejected."""
    store.create(_uid("p1"), "Dup", "alice@example.com")
    with pytest.raises(OmnigentError) as exc:
        store.create(_uid("p2"), "Dup", "alice@example.com")
    assert exc.value.code == ErrorCode.ALREADY_EXISTS


def test_same_name_allowed_across_owners(store: SqlAlchemyProjectStore) -> None:
    """Two different owners may each have a project with the same name."""
    a = store.create(_uid("p1"), "Shared Name", "alice@example.com")
    b = store.create(_uid("p2"), "Shared Name", "bob@example.com")
    assert a.name == b.name == "Shared Name"


def test_duplicate_name_rejected_for_null_owner(store: SqlAlchemyProjectStore) -> None:
    """Single-user mode (NULL owner) enforces name uniqueness in the store.

    ``_name_taken`` is the sole guard for every owner, NULL included — no unique
    index backs it.
    """
    store.create(_uid("p1"), "Solo", None)
    with pytest.raises(OmnigentError) as exc:
        store.create(_uid("p2"), "Solo", None)
    assert exc.value.code == ErrorCode.ALREADY_EXISTS


def test_duplicate_name_lands_when_precheck_is_bypassed(
    store: SqlAlchemyProjectStore,
) -> None:
    """A duplicate name is accepted once ``_name_taken`` is bypassed.

    No unique index covers (workspace_id, user_id, name), so the pre-check is
    the only guard and a concurrent create racing past it lands. Pins that
    accepted cost: both rows exist, each addressable by its own id.

    Monkeypatching ``_name_taken`` to always-miss simulates two concurrent
    creates both passing the check.
    """
    store.create(_uid("p1"), "Dup", "alice@example.com")
    store._name_taken = lambda *a, **k: False  # type: ignore[method-assign]
    store.create(_uid("p2"), "Dup", "alice@example.com")

    assert [p.name for p in store.list(user_id="alice@example.com")] == ["Dup", "Dup"]
    assert store.get(_uid("p1"), user_id="alice@example.com") is not None
    assert store.get(_uid("p2"), user_id="alice@example.com") is not None


def test_primary_key_collision_is_not_masked(store: SqlAlchemyProjectStore) -> None:
    """Reusing an id surfaces as ``IntegrityError``, not ``ALREADY_EXISTS``.

    The PK is the only remaining constraint on this table, and the store must
    not dress its violation up as a name collision.
    """
    store.create(_uid("p1"), "Original", "alice@example.com")
    store._name_taken = lambda *a, **k: False  # type: ignore[method-assign]
    with pytest.raises(IntegrityError):
        store.create(_uid("p1"), "Different name", "alice@example.com")


# ── config (default session settings) ──────────────────────────────────────


def test_create_defaults_to_empty_config(store: SqlAlchemyProjectStore) -> None:
    """A project created without config reads back an empty dict (SQL NULL)."""
    project = store.create(_uid("p1"), "No Config", "alice@example.com")
    assert project.config == {}
    assert store.get(_uid("p1"), user_id="alice@example.com").config == {}


def test_create_persists_config(store: SqlAlchemyProjectStore) -> None:
    """A config passed to ``create`` round-trips through ``get``/``list``."""
    cfg = {"host_id": "host_abc", "workspace": "/work/repo", "model": "claude-opus-4-8"}
    store.create(_uid("p1"), "Configured", "alice@example.com", cfg)
    assert store.get(_uid("p1"), user_id="alice@example.com").config == cfg
    assert store.list(user_id="alice@example.com")[0].config == cfg


def test_update_replaces_config_and_stamps_updated_at(store: SqlAlchemyProjectStore) -> None:
    """Passing a new ``config`` replaces the stored one and stamps updated_at."""
    store.create(_uid("p1"), "P", "alice@example.com", {"host_id": "old"})
    updated = store.update(
        _uid("p1"), user_id="alice@example.com", config={"host_id": "new", "model": "m"}
    )
    assert updated is not None
    assert updated.config == {"host_id": "new", "model": "m"}
    assert updated.updated_at is not None


def test_update_config_none_leaves_it_unchanged(store: SqlAlchemyProjectStore) -> None:
    """``config=None`` (the default) leaves the stored config untouched."""
    store.create(_uid("p1"), "P", "alice@example.com", {"host_id": "keep"})
    # Rename only — config omitted — must not wipe the stored defaults.
    updated = store.update(_uid("p1"), user_id="alice@example.com", name="Renamed")
    assert updated is not None
    assert updated.config == {"host_id": "keep"}


def test_update_empty_config_clears_defaults(store: SqlAlchemyProjectStore) -> None:
    """An explicit ``config={}`` clears the stored defaults (distinct from None)."""
    store.create(_uid("p1"), "P", "alice@example.com", {"host_id": "drop"})
    updated = store.update(_uid("p1"), user_id="alice@example.com", config={})
    assert updated is not None
    assert updated.config == {}
    assert updated.updated_at is not None


def test_update_same_config_is_noop(store: SqlAlchemyProjectStore) -> None:
    """Re-setting the identical config changes nothing, leaving updated_at None."""
    store.create(_uid("p1"), "P", "alice@example.com", {"host_id": "x"})
    updated = store.update(_uid("p1"), user_id="alice@example.com", config={"host_id": "x"})
    assert updated is not None
    assert updated.updated_at is None


def test_create_rejects_oversized_config(store: SqlAlchemyProjectStore) -> None:
    """A config whose serialized form exceeds the cap is rejected on create.

    The value is persisted verbatim and reflected back on every read, so an
    unbounded blob is capped as defense-in-depth (INVALID_INPUT → HTTP 400).
    """
    huge = {"blob": "x" * (64 * 1024 + 1)}
    with pytest.raises(OmnigentError) as exc:
        store.create(_uid("p1"), "Big", "alice@example.com", huge)
    assert exc.value.code == ErrorCode.INVALID_INPUT


def test_update_rejects_oversized_config(store: SqlAlchemyProjectStore) -> None:
    """An oversized config is rejected on update, leaving the row unchanged."""
    store.create(_uid("p1"), "P", "alice@example.com", {"host_id": "keep"})
    huge = {"blob": "x" * (64 * 1024 + 1)}
    with pytest.raises(OmnigentError) as exc:
        store.update(_uid("p1"), user_id="alice@example.com", config=huge)
    assert exc.value.code == ErrorCode.INVALID_INPUT
    # The prior config is untouched (the encode guard fires before any write).
    assert store.get(_uid("p1"), user_id="alice@example.com").config == {"host_id": "keep"}


def test_decode_coerces_non_object_blob_to_empty(store: SqlAlchemyProjectStore) -> None:
    """A stored non-object blob (manual edit / future writer) reads back as {}.

    The encode path only ever writes JSON objects, but ``_decode_config`` must
    stay defensive so callers can always treat config as a mapping.
    """
    from sqlalchemy import text as sa_text

    store.create(_uid("p1"), "P", "alice@example.com")
    # Bypass the store to plant a scalar JSON blob directly.
    with store._engine.begin() as conn:
        conn.execute(
            sa_text("UPDATE projects SET config = :c WHERE id = :i"),
            {"c": '"just a string"', "i": _uid("p1")},
        )
    got = store.get(_uid("p1"), user_id="alice@example.com")
    assert got is not None
    assert got.config == {}


# ── update ─────────────────────────────────────────────────────────────────


def test_update_renames_and_stamps_updated_at(store: SqlAlchemyProjectStore) -> None:
    """Renaming changes ``name`` and sets ``updated_at``."""
    store.create(_uid("p1"), "Old", "alice@example.com")
    updated = store.update(_uid("p1"), user_id="alice@example.com", name="New")
    assert updated is not None
    assert updated.name == "New"
    assert updated.updated_at is not None


def test_update_noop_leaves_updated_at_none(store: SqlAlchemyProjectStore) -> None:
    """An update that changes nothing leaves ``updated_at`` untouched."""
    store.create(_uid("p1"), "Same", "alice@example.com")
    updated = store.update(_uid("p1"), user_id="alice@example.com", name="Same")
    assert updated is not None
    assert updated.updated_at is None


def test_update_missing_returns_none(store: SqlAlchemyProjectStore) -> None:
    """Updating an unknown project returns ``None``."""
    updated = store.update(_uid("nope"), user_id="alice@example.com", name="X")
    assert updated is None


def test_update_scoped_to_owner(store: SqlAlchemyProjectStore) -> None:
    """A non-owner cannot rename another user's project."""
    store.create(_uid("p1"), "Alice Project", "alice@example.com")
    updated = store.update(_uid("p1"), user_id="bob@example.com", name="Hacked")
    assert updated is None
    # Unchanged for the real owner.
    assert store.get(_uid("p1"), user_id="alice@example.com").name == "Alice Project"


def test_update_rejects_duplicate_name(store: SqlAlchemyProjectStore) -> None:
    """Renaming onto another of the owner's project names is rejected."""
    store.create(_uid("p1"), "First", "alice@example.com")
    store.create(_uid("p2"), "Second", "alice@example.com")
    with pytest.raises(OmnigentError) as exc:
        store.update(_uid("p2"), user_id="alice@example.com", name="First")
    assert exc.value.code == ErrorCode.ALREADY_EXISTS


# ── delete ─────────────────────────────────────────────────────────────────


def test_delete_removes_project(store: SqlAlchemyProjectStore) -> None:
    """``delete`` removes the project and is idempotent."""
    store.create(_uid("p1"), "Doomed", "alice@example.com")
    deleted = store.delete(_uid("p1"), user_id="alice@example.com")
    assert deleted is True
    assert store.get(_uid("p1"), user_id="alice@example.com") is None
    deleted_again = store.delete(_uid("p1"), user_id="alice@example.com")
    assert deleted_again is False


def test_delete_scoped_to_owner(store: SqlAlchemyProjectStore) -> None:
    """A non-owner cannot delete another user's project."""
    store.create(_uid("p1"), "Alice Project", "alice@example.com")
    deleted = store.delete(_uid("p1"), user_id="bob@example.com")
    assert deleted is False
    assert store.get(_uid("p1"), user_id="alice@example.com") is not None


# ── Orvex: the ``shared`` flag ─────────────────────────────────────────────
#
# Store-level half of the story. The route-level half — and the AC5
# "ordinary, non-admin user" test — lives in
# ``tests/server/routes/test_projects_sharing.py``.

ALICE = "alice@example.com"
BOB = "bob@example.com"


def test_create_defaults_to_private(store: SqlAlchemyProjectStore) -> None:
    """A project created without naming ``shared`` is private.

    The default is the entire safety property of this change: every caller
    that predates the flag keeps creating owner-private projects.
    """
    project = store.create(_uid("p1"), "Private", ALICE)
    assert project.shared is False
    assert store.get(_uid("p1"), user_id=ALICE) is not None
    assert store.get(_uid("p1"), user_id=ALICE).shared is False


def test_create_can_mark_shared(store: SqlAlchemyProjectStore) -> None:
    """``shared=True`` persists and reads back."""
    project = store.create(_uid("p1"), "Fleet", ALICE, None, True)
    assert project.shared is True
    read_back = store.get(_uid("p1"), user_id=ALICE)
    assert read_back is not None and read_back.shared is True


def test_get_resolves_shared_project_for_non_owner(store: SqlAlchemyProjectStore) -> None:
    """A non-owner can ``get`` a shared project — the change that unblocks
    filing a session into it and keeping a fork of one filed."""
    store.create(_uid("p1"), "Fleet", ALICE, None, True)
    got = store.get(_uid("p1"), user_id=BOB)
    assert got is not None
    assert got.name == "Fleet"
    assert got.user_id == ALICE, "the entity still reports its real owner"


def test_list_returns_shared_projects_to_a_non_owner(store: SqlAlchemyProjectStore) -> None:
    """``list`` is owned-OR-shared, not owner-only.

    This is the store-level form of the trap: with an owner-scoped ``list``
    the shared project below simply vanishes for Bob, and every surface built
    on ``list`` (sidebar, project list, ``?project=`` resolution) goes with it.
    """
    store.create(_uid("p1"), "Fleet", ALICE, None, True)
    store.create(_uid("p2"), "Alice Private", ALICE)
    store.create(_uid("p3"), "Bob Own", BOB)

    names = {p.name for p in store.list(user_id=BOB)}
    assert names == {"Fleet", "Bob Own"}, (
        f"Bob should see his own project and the shared one only; got {names}"
    )


def test_update_is_owner_only_even_when_shared(store: SqlAlchemyProjectStore) -> None:
    """Reads open, writes stay closed: a non-owner cannot rename a shared project."""
    store.create(_uid("p1"), "Fleet", ALICE, None, True)
    assert store.update(_uid("p1"), user_id=BOB, name="Hijacked") is None
    unchanged = store.get(_uid("p1"), user_id=ALICE)
    assert unchanged is not None and unchanged.name == "Fleet"


def test_delete_is_owner_only_even_when_shared(store: SqlAlchemyProjectStore) -> None:
    """A non-owner cannot delete a shared project."""
    store.create(_uid("p1"), "Fleet", ALICE, None, True)
    assert store.delete(_uid("p1"), user_id=BOB) is False
    assert store.get(_uid("p1"), user_id=ALICE) is not None


def test_non_owner_cannot_flip_the_share_flag(store: SqlAlchemyProjectStore) -> None:
    """Sharing and un-sharing are writes, so they are owner-only too."""
    store.create(_uid("p1"), "Fleet", ALICE, None, True)
    assert store.update(_uid("p1"), user_id=BOB, shared=False) is None
    still_shared = store.get(_uid("p1"), user_id=ALICE)
    assert still_shared is not None and still_shared.shared is True

    store.create(_uid("p2"), "Alice Private", ALICE)
    assert store.update(_uid("p2"), user_id=BOB, shared=True) is None
    assert store.get(_uid("p2"), user_id=BOB) is None, "the private project stays invisible"


def test_owner_can_share_and_unshare(store: SqlAlchemyProjectStore) -> None:
    """The owner toggles the flag, and ``updated_at`` is stamped for it."""
    store.create(_uid("p1"), "Fleet", ALICE)
    shared = store.update(_uid("p1"), user_id=ALICE, shared=True)
    assert shared is not None and shared.shared is True
    assert shared.updated_at is not None, "flipping the flag is a change, so it stamps"
    assert store.get(_uid("p1"), user_id=BOB) is not None

    unshared = store.update(_uid("p1"), user_id=ALICE, shared=False)
    assert unshared is not None and unshared.shared is False
    assert store.get(_uid("p1"), user_id=BOB) is None, "un-sharing takes the access back"


def test_update_leaves_shared_untouched_when_not_named(store: SqlAlchemyProjectStore) -> None:
    """A rename must not silently un-share the project."""
    store.create(_uid("p1"), "Fleet", ALICE, None, True)
    renamed = store.update(_uid("p1"), user_id=ALICE, name="Fleet v2")
    assert renamed is not None
    assert renamed.name == "Fleet v2"
    assert renamed.shared is True


# ── Orvex: a private project is upstream, exactly (SEC-8 / AC6) ────────────
#
# Written for this case specifically, not as a by-product of the sharing tests
# above. A bug that leaked the flag onto unshared rows would expose people's
# personal sessions, so the whole private surface is asserted in one place.


def test_private_project_is_untouched_by_the_shared_flag(store: SqlAlchemyProjectStore) -> None:
    """Every store method behaves for a private project exactly as upstream.

    Alice owns one private project. Bob is an unrelated user who owns a shared
    project of his own — present deliberately, so the test also proves the
    existence of *some* shared row in the table does not widen an unrelated
    private one.
    """
    store.create(_uid("p1"), "Alice Private", ALICE)
    store.create(_uid("p2"), "Bob Shared", BOB, None, True)

    # get: not found for a non-owner.
    assert store.get(_uid("p1"), user_id=BOB) is None
    # list: absent for a non-owner (and Bob still sees only his own row).
    assert [p.name for p in store.list(user_id=BOB)] == ["Bob Shared"]
    # update: refused, and nothing changes.
    assert store.update(_uid("p1"), user_id=BOB, name="Hijacked") is None
    # delete: refused.
    assert store.delete(_uid("p1"), user_id=BOB) is False

    # The owner's own view of her private project is entirely unaffected.
    owned = store.get(_uid("p1"), user_id=ALICE)
    assert owned is not None
    assert owned.name == "Alice Private"
    assert owned.shared is False
    alice_view = {p.name: p.shared for p in store.list(user_id=ALICE)}
    assert alice_view["Alice Private"] is False
    # Alice sees Bob's shared project too — sharing is symmetric, and that is
    # the point. What must never happen is her own row acquiring the flag.
    assert alice_view["Bob Shared"] is True
