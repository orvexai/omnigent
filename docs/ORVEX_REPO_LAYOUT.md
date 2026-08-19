# Repository layout — orvexai/omnigent

This fork has exactly five branches. `mcp` is stacked on top of `web`: it
contains every `web` commit plus the binary and agent-surface work. Anything
outside this layout is a mistake to be cleaned up, not a workflow to be
extended.

## Branches

| branch | purpose | rules |
|---|---|---|
| `main` | exact fast-forward mirror of `upstream/main` | **never commit here.** No org change of any kind. |
| `web` | server-side changes, rebased on `main` | the lower layer; linear, one commit per change |
| `mcp` | `web` **plus** binary and agent-surface changes | rebased on `web`, never merged; releases are cut from here (`orvex-vX.Y.Z`) |
| `mcp-old` | frozen backup of the pre-replay `mcp` | read-only, never built on |
| `orvex-old` | frozen backup of the pre-replay `orvex` | read-only, never built on |

**No feature branches.** Work lands directly on `web` or `mcp`. A change that
needs isolation gets a commit, not a branch.

**Worktrees are working state, not structure.** Use them freely for gates,
control runs at a base commit, and parallel agents — then remove them. A
worktree that outlives the task that created it is litter: it pins a branch,
holds a checkout of the whole tree, and keeps an agent's language servers and
MCP bridges resident. Check for live processes before removing one; a worktree
with work still running in it must be drained, not deleted underneath.

Every worktree lives under `.worktrees/` or outside the repository entirely,
and none of them is a place work lands. Commits go to `web` or `mcp`.

## Where a change lands

**Every server-side change goes on `web`. Nothing that is not server-side ever
goes on `web`.** Both halves of that rule are absolute — `web` is the branch the
server image is built from, so a server-side change that lands anywhere else
never reaches production, and a non-server change that lands on `web` ships
runner code into the server image and pollutes the branch we would send
upstream.

- **`web`** — `omnigent/server/`, `omnigent/stores/`, `omnigent/db/` and its
  migrations, `omnigent/api/`, `deploy/`, `tekton/`, auth, the web client, and
  any shared module the server process imports and depends on behaviourally
  (`omnigent/errors.py`, `omnigent/_wrapper_labels.py`,
  `omnigent/harness_plugins.py`, ...).
- **`mcp`** — the runner, harness adapters, MCP tools, `omnigent/tools/`, CLI,
  native wrappers, packaging, release CI. Never a server-side line.

A feature that needs a server route *and* a runner change is **two commits**:
the server half on `web`, the runner half on `mcp`. It is never one commit on
`mcp`. Because `mcp` is rebased on `web`, the pair is still exercised together
on `mcp` — but only the server half ships in the server image, which is exactly
what production needs.

The test that decides it: *would this line run inside the server pod?* If yes,
it belongs on `web`, whatever else the change touches.

Path is not the test — behaviour is. `tests/server/integration/` holds one file
that imports `omnigent.runner.tool_dispatch`; it exercises the runner through
the server API, so it lives on `mcp` and fails on `web` alone. Before moving a
test down to `web`, run it there.

`web` and `mcp` never merge into each other in either direction.

## Restacking

`web` is `mcp`'s base, so every `web` commit invalidates `mcp` and `mcp` must be
replayed. The ritual, in order:

```sh
git tag backup/mcp-<what-changed> mcp     # before any rewrite
git rebase web mcp                        # replay mcp's commits onto the new web
<run the gate>                            # scoped to the changed-file list
git push --force-with-lease origin mcp
```

Never commit to `web` while an `mcp` restack is in flight — the second commit
forces a second rewrite of work that was already replayed.

Upstream syncs go through the stack in strict order, never straight to `mcp`:

```sh
git checkout main && git merge --ff-only upstream/main
git rebase main web  && git push --force-with-lease origin web
git rebase web  mcp  && git push --force-with-lease origin mcp
```

Turn on `git rerere` so a conflict resolved once during a restack replays on the
next one.

## History rules

Both working branches are built by replaying onto the current upstream, so:

- one commit per change, carrying only that change's final state;
- no merge commits, no revert-of-a-revert, no "restore what the re-land dropped";
- rebase onto upstream rather than merging upstream in;
- imperative subject describing the user-visible effect, not the mechanism.

If history needs repair, rewrite and force-push with a `backup/*` tag first —
do not add a commit that undoes another.

## Deploy and release

- The server image is built by Tekton on push to the branch named in
  `tekton/trigger.yaml`, and tagged with that branch name.
- Standalone binaries are cut from `mcp` on an `orvex-vX.Y.Z` tag.

The deploy branch decides which half of the stack reaches production. Building
from `web` ships only the lower layer, so any server-side change that an MCP
feature depends on would never reach the running server; building from `mcp`
ships both, at the cost of putting in-flight runner work on the deploy path.
Whichever is chosen, the gate has to run before the push that triggers it.

## Local tooling is not tracked

`.claude/` and `_bmad/` are local agent tooling and are ignored. The only
`.claude/` files in the tree are upstream's own. Committing local tooling was
the single largest source of phantom lint failures on this fork: 927 files that
made `ruff check` report ~190 errors on every branch, on every run, unrelated to
any change under review.

## CI

Every GitHub Actions job must run on `public-runners`. GitHub-hosted runners are
disabled org-wide, so `ubuntu-latest`, `macos-*` and `windows-latest` never get a
runner — jobs queue until they expire at 24 hours. This is invisible from the
workflow file, which looks normal.
