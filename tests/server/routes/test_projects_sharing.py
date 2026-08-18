"""Orvex — a shared project reaches the people who hold access to it.

Upstream, a project is owner-private on every surface: the sidebar folder list,
the first-class project list, ``GET /v1/projects/{id}``, the per-project session
query, filing, renaming and deleting. This fork adds one boolean to the row.
When it is set the *read* surfaces open to non-owners; the *write* surfaces do
not. When it is unset — the default, and the value every project on the server
already carries — nothing whatsoever changes.

These tests drive the real routes over file-backed SQLite with header auth, so
each request carries a real, distinct identity.

Two failure modes are pinned here on purpose, because both ship green:

* **The ``project_store.list()`` trap.** Leaving ``list()`` owner-scoped breaks
  sharing for every identity *except* one that owns the projects — which is the
  identity a smoke test is most likely to use, so the bug hides inside the check
  meant to catch it. ``CAROL`` below is an ordinary header-auth user: not an
  admin, not the owner of anything, holding no session grant at all.
  ``test_reverting_list_to_owner_scoped_breaks_sharing`` then re-runs the same
  assertion against a deliberately reverted ``list()`` and requires it to fail.
* **Leaking the flag onto private projects.** Asserted in its own section
  below, written for that case rather than inferred from the sharing tests: a
  private project that started behaving like a shared one would expose people's
  personal sessions, which is the single security consequence of this change.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from omnigent.entities import Project
from omnigent.errors import OmnigentError
from omnigent.server.auth import LEVEL_OWNER, LEVEL_READ, UnifiedAuthProvider
from omnigent.server.routes.projects import create_projects_router
from omnigent.server.routes.sessions import create_sessions_router
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.conversation_store.sqlalchemy_store import (
    SqlAlchemyConversationStore,
)
from omnigent.stores.permission_store.sqlalchemy_store import (
    SqlAlchemyPermissionStore,
)
from omnigent.stores.project_store.sqlalchemy_store import SqlAlchemyProjectStore

ALICE = "alice@example.com"
BOB = "bob@example.com"
# The AC5 identity: an ordinary user. There is no service-principal concept in
# this server's auth layer at all — the closest thing is "the identity that
# happens to own the projects", which is exactly what CAROL is not. She owns
# nothing, is never promoted to admin, and holds no session grant.
CAROL = "carol@example.com"
AGENT_ID = "087b7cb7ac30abf4debfaa578d052ec6"


def _hdr(user: str) -> dict[str, str]:
    """Header identifying the requesting user under header auth."""
    return {"X-Forwarded-Email": user}


def _ensure_agent(db_uri: str) -> None:
    """Sessions need an agent binding to show up in ``GET /v1/sessions``."""
    agent_store = SqlAlchemyAgentStore(db_uri)
    if agent_store.get(AGENT_ID) is None:
        agent_store.create(
            agent_id=AGENT_ID,
            name="test-agent",
            bundle_location=f"{AGENT_ID}/bundle",
        )


def _app(db_uri: str) -> FastAPI:
    """Header-auth app mounting the sessions and projects routers at ``/v1``."""
    app = FastAPI()

    @app.exception_handler(OmnigentError)
    async def _handle(request: Request, exc: OmnigentError) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    auth = UnifiedAuthProvider(source="header")
    project_store = SqlAlchemyProjectStore(db_uri)
    app.include_router(
        create_sessions_router(
            conversation_store=SqlAlchemyConversationStore(db_uri),
            agent_store=SqlAlchemyAgentStore(db_uri),
            auth_provider=auth,
            permission_store=SqlAlchemyPermissionStore(db_uri),
            project_store=project_store,
        ),
        prefix="/v1",
    )
    app.include_router(
        create_projects_router(project_store=project_store, auth_provider=auth),
        prefix="/v1",
    )
    return app


def _seed_session(db_uri: str, owner: str, title: str, *, readers: tuple[str, ...] = ()) -> str:
    """Create a session owned by ``owner``, read-granted to ``readers``."""
    conv = SqlAlchemyConversationStore(db_uri).create_conversation(title=title, agent_id=AGENT_ID)
    perms = SqlAlchemyPermissionStore(db_uri)
    perms.ensure_user(owner)
    perms.grant(owner, conv.id, LEVEL_OWNER)
    for reader in readers:
        perms.ensure_user(reader)
        perms.grant(reader, conv.id, LEVEL_READ)
    return conv.id


def _create_project(client: TestClient, user: str, name: str, *, shared: bool) -> dict[str, Any]:
    """Create a project as ``user`` and return the response body."""
    resp = client.post("/v1/projects", json={"name": name, "shared": shared}, headers=_hdr(user))
    assert resp.status_code == 200, resp.text
    return resp.json()


def _file(client: TestClient, user: str, session_id: str, project_id: str) -> Any:
    """File ``session_id`` into ``project_id`` as ``user``."""
    return client.patch(
        f"/v1/sessions/{session_id}",
        json={"project_id": project_id},
        headers=_hdr(user),
    )


# ── AC1: the field is on the API ───────────────────────────────────────────


def test_create_defaults_to_private_and_echoes_the_field(db_uri: str) -> None:
    """``POST /v1/projects`` exposes ``shared`` and defaults it to false."""
    client = TestClient(_app(db_uri))
    default = client.post("/v1/projects", json={"name": "Default"}, headers=_hdr(ALICE)).json()
    assert default["shared"] is False, "a project created without the field must be private"

    explicit = _create_project(client, ALICE, "Fleet", shared=True)
    assert explicit["shared"] is True


def test_patch_can_share_and_unshare(db_uri: str) -> None:
    """``PATCH /v1/projects/{id}`` exposes the field, and the owner may flip it."""
    client = TestClient(_app(db_uri))
    project = client.post("/v1/projects", json={"name": "Fleet"}, headers=_hdr(ALICE)).json()

    shared = client.patch(
        f"/v1/projects/{project['id']}", json={"shared": True}, headers=_hdr(ALICE)
    )
    assert shared.status_code == 200
    assert shared.json()["shared"] is True
    assert client.get(f"/v1/projects/{project['id']}", headers=_hdr(BOB)).status_code == 200

    unshared = client.patch(
        f"/v1/projects/{project['id']}", json={"shared": False}, headers=_hdr(ALICE)
    )
    assert unshared.json()["shared"] is False
    assert client.get(f"/v1/projects/{project['id']}", headers=_hdr(BOB)).status_code == 404


# ── AC2 + AC5: the ordinary-user visibility test ───────────────────────────


def test_shared_project_is_visible_to_an_ordinary_non_admin_user(db_uri: str) -> None:
    """Carol — no ownership, no admin flag, no session grant — sees the folder.

    This is the AC5 test. It covers both surfaces AC2 names: the sidebar folder
    list (``GET /v1/sessions/projects``) and the first-class project list
    (``GET /v1/projects``), which are two different code paths over the same
    ``project_store.list()`` call. Alice's *private* project is in the same
    table throughout, so the test also shows the widening is per-row.
    """
    _ensure_agent(db_uri)
    client = TestClient(_app(db_uri))

    # Pin the "ordinary" claim rather than assuming it: header auth never
    # promotes anyone, but an admin would trivially pass this test for the
    # wrong reason.
    perms = SqlAlchemyPermissionStore(db_uri)
    perms.ensure_user(CAROL)
    assert perms.is_admin(CAROL) is False, "AC5 requires a non-admin identity"

    fleet = _create_project(client, ALICE, "Fleet", shared=True)
    _create_project(client, ALICE, "Alice Private", shared=False)

    sidebar = client.get("/v1/sessions/projects", headers=_hdr(CAROL))
    assert sidebar.status_code == 200
    assert sidebar.json() == [{"id": fleet["id"], "name": "Fleet"}], (
        "the shared project — and only it — must reach an ordinary user's sidebar"
    )

    listing = client.get("/v1/projects", headers=_hdr(CAROL))
    assert listing.status_code == 200
    assert [(p["id"], p["name"], p["shared"]) for p in listing.json()["data"]] == [
        (fleet["id"], "Fleet", True)
    ]

    # And the single-project read resolves for her too.
    single = client.get(f"/v1/projects/{fleet['id']}", headers=_hdr(CAROL))
    assert single.status_code == 200
    assert single.json()["name"] == "Fleet"


def test_reverting_list_to_owner_scoped_breaks_sharing(
    db_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The AC5 guard, made executable.

    AC5 does not just ask for a non-owner test; it asks that the test *fail* if
    ``project_store.list()`` is reverted to owner-scoped. Rather than trusting
    that claim, this reverts ``list()`` to upstream's implementation and asserts
    the sharing surfaces collapse. If someone later re-narrows ``list()``,
    ``test_shared_project_is_visible_to_an_ordinary_non_admin_user`` above is
    the test that fails — and this one documents exactly why.

    Note what does NOT collapse: ``get`` still resolves, which is why omitting
    ``list()`` looks like a working implementation from an API smoke test that
    happens to hold the project id.
    """
    _ensure_agent(db_uri)
    app = _app(db_uri)
    client = TestClient(app)
    fleet = _create_project(client, ALICE, "Fleet", shared=True)
    assert client.get("/v1/sessions/projects", headers=_hdr(CAROL)).json() != []

    def _owner_scoped_list(self: SqlAlchemyProjectStore, *, user_id: str | None) -> list[Project]:
        """Upstream's ``list``: owner rows only."""
        from sqlalchemy import asc, select

        from omnigent.db.db_models import SqlProject, current_workspace_id
        from omnigent.stores.project_store.sqlalchemy_store import _to_entity

        with self._session("list_projects") as session:
            stmt = (
                select(SqlProject)
                .where(SqlProject.workspace_id == current_workspace_id())
                .where(SqlProject.user_id == user_id)
                .order_by(asc(SqlProject.created_at), asc(SqlProject.id))
            )
            return [_to_entity(r) for r in session.execute(stmt).scalars().all()]

    monkeypatch.setattr(SqlAlchemyProjectStore, "list", _owner_scoped_list)

    assert client.get("/v1/sessions/projects", headers=_hdr(CAROL)).json() == [], (
        "with an owner-scoped list() the shared folder must disappear — "
        "if it does not, the ordinary-user test above is not load-bearing"
    )
    assert client.get("/v1/projects", headers=_hdr(CAROL)).json()["data"] == []
    # The owner is unaffected, which is precisely why the bug is invisible to
    # whoever created the projects.
    assert client.get("/v1/sessions/projects", headers=_hdr(ALICE)).json() == [
        {"id": fleet["id"], "name": "Fleet"}
    ]


# ── AC3: the sessions inside a shared project ──────────────────────────────


def test_non_owner_sees_every_accessible_session_in_a_shared_project(db_uri: str) -> None:
    """The per-project query scopes by ACCESS, not ownership, when shared.

    Alice owns the folder and a session in it that she has read-shared with
    Bob; Bob has filed a session of his own into the same folder. Asking for
    the folder, Bob gets both — which upstream's ``owned_by`` scoping would
    have reduced to just his own.
    """
    _ensure_agent(db_uri)
    client = TestClient(_app(db_uri))
    fleet = _create_project(client, ALICE, "Fleet", shared=True)

    alice_session = _seed_session(db_uri, ALICE, "alice work", readers=(BOB,))
    bob_session = _seed_session(db_uri, BOB, "bob work")
    assert _file(client, ALICE, alice_session, fleet["id"]).status_code == 200
    assert _file(client, BOB, bob_session, fleet["id"]).status_code == 200

    listed = client.get("/v1/sessions?project=Fleet", headers=_hdr(BOB))
    assert listed.status_code == 200
    assert {s["id"] for s in listed.json()["data"]} == {alice_session, bob_session}


def test_non_owner_can_file_a_session_into_a_shared_project(db_uri: str) -> None:
    """Filing succeeds for a non-owner — unblocked by the ``get`` change."""
    _ensure_agent(db_uri)
    client = TestClient(_app(db_uri))
    fleet = _create_project(client, ALICE, "Fleet", shared=True)
    bob_session = _seed_session(db_uri, BOB, "bob work")

    filed = _file(client, BOB, bob_session, fleet["id"])
    assert filed.status_code == 200
    assert filed.json()["project_id"] == fleet["id"]


def test_sharing_a_project_does_not_share_the_sessions_in_it(db_uri: str) -> None:
    """Project visibility and session grants stay orthogonal.

    Carol can see the folder, but a session inside it that nobody granted her
    stays invisible. Sharing widens which project rows a user may see — never
    which sessions.
    """
    _ensure_agent(db_uri)
    client = TestClient(_app(db_uri))
    fleet = _create_project(client, ALICE, "Fleet", shared=True)
    alice_session = _seed_session(db_uri, ALICE, "alice private work")
    assert _file(client, ALICE, alice_session, fleet["id"]).status_code == 200

    assert client.get("/v1/sessions/projects", headers=_hdr(CAROL)).json() == [
        {"id": fleet["id"], "name": "Fleet"}
    ]
    listed = client.get("/v1/sessions?project=Fleet", headers=_hdr(CAROL))
    assert listed.status_code == 200
    assert listed.json()["data"] == [], (
        "a shared folder must not hand out sessions the caller holds no grant on"
    )


def test_the_callers_own_folder_wins_a_name_collision(db_uri: str) -> None:
    """A shared project cannot shadow a like-named folder the caller owns.

    Otherwise anyone could re-point one of your sidebar folders at their own
    sessions by naming a shared project after it.
    """
    _ensure_agent(db_uri)
    client = TestClient(_app(db_uri))
    alice_fleet = _create_project(client, ALICE, "Fleet", shared=True)
    bob_fleet = _create_project(client, BOB, "Fleet", shared=False)

    alice_session = _seed_session(db_uri, ALICE, "alice work", readers=(BOB,))
    bob_session = _seed_session(db_uri, BOB, "bob work")
    assert _file(client, ALICE, alice_session, alice_fleet["id"]).status_code == 200
    assert _file(client, BOB, bob_session, bob_fleet["id"]).status_code == 200

    # One folder in Bob's sidebar, and it is his.
    assert client.get("/v1/sessions/projects", headers=_hdr(BOB)).json() == [
        {"id": bob_fleet["id"], "name": "Fleet"}
    ]
    listed = client.get("/v1/sessions?project=Fleet", headers=_hdr(BOB))
    assert [s["id"] for s in listed.json()["data"]] == [bob_session], (
        "Bob's own folder must resolve to his own project, not Alice's shared one"
    )


# ── AC4: writes stay owner-only ────────────────────────────────────────────


def test_non_owner_cannot_rename_or_delete_a_shared_project(db_uri: str) -> None:
    """PATCH and DELETE refuse a non-owner exactly as upstream refuses them."""
    client = TestClient(_app(db_uri))
    fleet = _create_project(client, ALICE, "Fleet", shared=True)

    renamed = client.patch(
        f"/v1/projects/{fleet['id']}", json={"name": "Hijacked"}, headers=_hdr(BOB)
    )
    assert renamed.status_code == 404
    deleted = client.delete(f"/v1/projects/{fleet['id']}", headers=_hdr(BOB))
    assert deleted.status_code == 404

    # Still there, still named what its owner named it.
    survivor = client.get(f"/v1/projects/{fleet['id']}", headers=_hdr(ALICE))
    assert survivor.status_code == 200
    assert survivor.json()["name"] == "Fleet"


def test_non_owner_cannot_unshare_or_reshare(db_uri: str) -> None:
    """The flag is itself a write: only the owner sets it."""
    client = TestClient(_app(db_uri))
    fleet = _create_project(client, ALICE, "Fleet", shared=True)
    private = _create_project(client, ALICE, "Alice Private", shared=False)

    assert (
        client.patch(
            f"/v1/projects/{fleet['id']}", json={"shared": False}, headers=_hdr(BOB)
        ).status_code
        == 404
    )
    assert client.get(f"/v1/projects/{fleet['id']}", headers=_hdr(ALICE)).json()["shared"] is True

    assert (
        client.patch(
            f"/v1/projects/{private['id']}", json={"shared": True}, headers=_hdr(BOB)
        ).status_code
        == 404
    )
    assert client.get(f"/v1/projects/{private['id']}", headers=_hdr(BOB)).status_code == 404


# ── AC6 / SEC-8: a private project is upstream, exactly ────────────────────
#
# Written for this case specifically. The tests above would keep passing if the
# flag leaked onto private rows; these are the ones that would not.


def test_private_project_is_invisible_and_immutable_to_a_non_owner(db_uri: str) -> None:
    """Every surface, swept in one place, for a project with ``shared = false``.

    Sidebar, project list, single read, per-project session query, filing,
    rename and delete — all as unforked upstream behaves. A shared project
    belonging to a third user sits in the same table throughout, so the sweep
    also proves the flag does not widen a private row merely by existing.
    """
    _ensure_agent(db_uri)
    client = TestClient(_app(db_uri))

    private = _create_project(client, ALICE, "Alice Private", shared=False)
    decoy = _create_project(client, CAROL, "Carol Shared", shared=True)

    # A session Bob can even *read* — shared with him at session level — is
    # still not reachable through Alice's private folder.
    alice_session = _seed_session(db_uri, ALICE, "alice work", readers=(BOB,))
    assert _file(client, ALICE, alice_session, private["id"]).status_code == 200

    # Sidebar: only the decoy, never Alice's private folder.
    assert client.get("/v1/sessions/projects", headers=_hdr(BOB)).json() == [
        {"id": decoy["id"], "name": "Carol Shared"}
    ]
    # Project list: same.
    assert [p["name"] for p in client.get("/v1/projects", headers=_hdr(BOB)).json()["data"]] == [
        "Carol Shared"
    ]
    # Single read: 404, not 403 — upstream does not leak existence.
    assert client.get(f"/v1/projects/{private['id']}", headers=_hdr(BOB)).status_code == 404
    # Per-project session query: the name resolves to nothing of Bob's, so the
    # folder is empty even though he can read the session inside it.
    empty = client.get("/v1/sessions?project=Alice%20Private", headers=_hdr(BOB))
    assert empty.status_code == 200
    assert empty.json()["data"] == []
    # Filing into it: 404.
    bob_session = _seed_session(db_uri, BOB, "bob work")
    assert _file(client, BOB, bob_session, private["id"]).status_code == 404
    # Writes: 404.
    assert (
        client.patch(
            f"/v1/projects/{private['id']}", json={"name": "Hijacked"}, headers=_hdr(BOB)
        ).status_code
        == 404
    )
    assert client.delete(f"/v1/projects/{private['id']}", headers=_hdr(BOB)).status_code == 404

    # The session is still reachable where upstream puts it: the flat list,
    # which the UI splits into "Shared with me".
    flat = client.get("/v1/sessions", headers=_hdr(BOB))
    assert alice_session in {s["id"] for s in flat.json()["data"]}


def test_the_shared_flag_never_lands_on_a_project_that_did_not_ask_for_it(
    db_uri: str,
) -> None:
    """Creating shared projects alongside private ones leaves the latter false.

    Guards the leak directly at the row level rather than through a behaviour:
    reads go through the store, so a default that flipped — or a ``shared``
    column that decayed to NULL and got truthy-coerced — would show up here
    first.
    """
    client = TestClient(_app(db_uri))
    _create_project(client, ALICE, "Fleet", shared=True)
    client.post("/v1/projects", json={"name": "Omitted"}, headers=_hdr(ALICE))
    _create_project(client, ALICE, "Explicit Private", shared=False)

    store = SqlAlchemyProjectStore(db_uri)
    by_name = {p.name: p.shared for p in store.list(user_id=ALICE)}
    assert by_name == {"Fleet": True, "Omitted": False, "Explicit Private": False}
    assert all(value is not None for value in by_name.values()), "the flag is never NULL"
