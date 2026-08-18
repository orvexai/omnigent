# Orvex fork log

Every place the `orvex` branch diverges from `omnigent-ai/omnigent` main in a way that has to be
re-merged by hand on each upstream sync. Divergence is budgeted in **sites**, not lines, because a
site is what someone re-reads and re-reconciles when upstream moves; a hundred-line site costs the
same attention as a two-line one.

Each entry is marked:

- **upstreamable** — the change is a plausible upstream contribution. If it lands upstream, the site
  leaves this log and stops costing anything.
- **permanent Orvex divergence** — the change encodes a decision upstream has taken the other way,
  and will have to be reapplied indefinitely.

Two areas are budgeted in total. This file covers both, so the count stays in one place.

---

## Area 1 — Shared projects (Story 1.1, FR-48…FR-52, SEC-8, NFR-5)

**What it does.** Adds a `shared` boolean to the `projects` row. When it is true, a user who does not
own the project can see it and open the sessions in it that they already hold a grant on. When it is
false — the default, and the value every pre-existing row carries — behaviour is upstream's exactly.

**Why it cannot be done through the API.** Omnigent projects carry no ACL and every project surface
is owner-scoped, so no amount of orchestration reaches the outcome. The alternative shape — dropping
the owner scope globally — is two lines and changes behaviour for every private project on the
server. This shape is larger and confined to rows that opt in.

**Shape of the whole area:** 11 code sites + 1 migration + 1 generated artifact.

### The behavioural sites

| # | Site | Change | Status |
|---|------|--------|--------|
| 1 | `omnigent/db/db_models.py` — `SqlProject` | New `shared` column (NOT NULL, server default false); docstring no longer claims projects "are never shared". | **upstreamable** |
| 2 | `omnigent/stores/project_store/sqlalchemy_store.py` — `list()` | `WHERE user_id = :me` → `WHERE user_id = :me OR shared`. | **upstreamable** |
| 3 | `omnigent/stores/project_store/sqlalchemy_store.py` — `get()` | Post-fetch owner check relaxed to owner-or-shared. Unblocks filing (`routes_core` PATCH) and fork inheritance without either becoming a site of its own. | **upstreamable** |
| 4 | `omnigent/stores/project_store/sqlalchemy_store.py` — `update()` | **Deliberately unchanged owner scoping**, plus a new `shared` parameter so the owner can set the flag. Logged because "this stays owner-only" is a decision that must survive an upstream merge, not an absence. | **upstreamable** |
| 5 | `omnigent/stores/project_store/sqlalchemy_store.py` — `delete()` | **Deliberately unchanged owner scoping.** Comment only. | **upstreamable** |
| 6 | `omnigent/server/routes/sessions/routes_core.py` — sidebar folder list (`GET /v1/sessions/projects`) | Shared projects reach the union via site 2; added explicit precedence so a shared project can never shadow a like-named folder the caller owns. | **upstreamable** |
| 7 | `omnigent/server/routes/sessions/routes_core.py` — per-project session query (`GET /v1/sessions?project=`) | When the named folder resolves to someone else's shared project, drop `owned_by` (scope by access) and pass `project_owner`. Both halves are required; either alone yields an empty folder. New module-level helper `_shared_project_owner_for`. | **upstreamable** |
| 8 | `omnigent/stores/conversation_store/sqlalchemy_store.py` — `list_conversations` project name→id resolution | New `project_owner` parameter, defaulting to `owned_by`. Upstream conflates "whose sessions" with "whose project", which is correct only while projects are owner-private. **Not in the story's site list** — see "Corrections" below. | **upstreamable** |
| 9 | `omnigent/stores/project_store/__init__.py` (abstract) | `create(shared=...)`, `update(shared=...)`, contract docstrings. | **upstreamable** |
| 10 | `omnigent/entities/project.py` | `Project.shared` field. | **upstreamable** |
| 11 | `omnigent/server/schemas.py` + `omnigent/server/routes/projects.py` | `shared` on `CreateProjectRequest`, `UpdateProjectRequest` and `ProjectObject`; passed through create/patch and echoed in the response. Both request models are `extra="forbid"`, so exposing the field means editing them. | **upstreamable** |

### Supporting artifacts

| Artifact | Note |
|----------|------|
| `omnigent/db/migrations/versions/orvex1a2b3c4_add_shared_to_projects.py` | Forward migration. Revision id sits outside upstream's `<letter>1a2b3c4d5e6` sequence so an upstream head cannot collide with it; rebasing means editing `down_revision` only. |
| `openapi.json` | Regenerated (`python scripts/dump_openapi.py`) — drift is test-enforced. Regenerates cleanly from the sources above; not a hand-maintained site. |
| `tests/server/routes/test_projects_sharing.py`, `tests/stores/test_project_store.py` (appended section), `tests/db/test_migration_projects_shared.py` | Tests. Additive; no upstream test was modified or deleted. |

### Sites deliberately NOT touched

Recorded so a future reader does not "fix" them:

- **Host filesystem-browse endpoints** (`omnigent/server/routes/hosts.py:991, 1029, 1159, 1507`) —
  owner-only, unchanged. They expose the whole host filesystem outside any session scope, which is a
  different risk class from launching a runner in a known workspace (FR-51). They check against
  `hosts.user_id` and never consult `project_store`, so the flag cannot reach them.
- **Filing a session into a project** (`routes_core.py`, `PATCH /v1/sessions/{id}`) — the ownership
  probe is upstream's, unchanged. It admits a non-owner only because site 3 changed underneath it.
- **Fork project inheritance** (`routes_core.py`, `POST /v1/sessions/{id}/fork`) — same: upstream's
  probe, unchanged, behaviour follows site 3. A fork of a session in someone else's **private**
  project still lands unfiled, exactly as upstream.
- **Host sharing** — out of scope by decision. Roughly twelve further permanent sites for the
  convenience of picking a host from Omnigent's own launcher; session brokering already delivers the
  capability.

### Corrections to the story's site list

The story specifies **six** sites. The real behavioural surface is **eleven**, and one of the extras
is load-bearing rather than cosmetic:

- **Site 8 (`list_conversations`'s project name→id resolution) is a genuine seventh behavioural
  site that nobody listed.** The API filters by project *name*, and upstream resolves that name
  against `SqlProject.user_id == owned_by`. Relaxing `owned_by` alone — the one-line change the
  story implies at `routes_core.py:901` — resolves the name against `user_id IS NULL` and returns an
  empty folder (and could match an unrelated single-user project of the same name). Both halves are
  needed; each was mutation-tested independently.
- Sites 9, 10 and 11 are the entity/abstract-contract/schema plumbing the flag needs to exist at
  all. The story counts them under "plus the schema changes" rather than as sites; they are listed
  here because they are re-merge surface like any other.
- The story lists **filing** (`routes_core.py:1952`) and (via the coordinator) **fork inheritance**
  as risks. Both turned out to require no code change: they validate through `project_store.get`,
  so site 3 carries them. Their comments were corrected, which is not a behavioural site.

**If the divergence budget is stated as "six sites", it is understating this area by five.** The
number is worth restating rather than quietly exceeding.

---

## Area 2 — Scoped service-principal credential (Story 7.1, FR-54)

Not yet implemented. Entries land here when it ships, in the same shape.

---

## Pre-existing orvex divergence (before this log existed)

Carried by commit `971ab99f`, and listed for completeness rather than re-litigated:

- `.github/workflows/orvex-ci.yml`, `sync-upstream.yml`, `actionlint.yaml` — org-owned CI files
  only; no upstream workflow is modified. **permanent Orvex divergence** (org infrastructure).
- `tekton/` — build pipeline, kustomization, trigger and rollout RBAC. **permanent Orvex
  divergence** (org infrastructure).
- `omnigent/server/oidc.py`, `omnigent/server/routes/auth.py` — JWKS fetched over httpx (upstream's
  urllib fetch is rejected by Cloudflare bot protection); group-based admin promotion via
  `OMNIGENT_OIDC_ADMIN_GROUPS`. **upstreamable** (the JWKS transport override especially).
