# Repository layout — orvexai/omnigent

This fork has exactly five branches and no worktrees. Anything else is a mistake
to be cleaned up, not a workflow to be extended.

## Branches

| branch | purpose | rules |
|---|---|---|
| `main` | exact fast-forward mirror of `upstream/main` | **never commit here.** No org change of any kind. |
| `web` | server-side changes | rebased on latest upstream, linear, one commit per change |
| `mcp` | binary and agent-surface changes | same; releases are cut from here (`orvex-vX.Y.Z`) |
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

## Choosing between `web` and `mcp`

- Server-side — `omnigent/server/`, routes, stores, migrations, deploy, auth,
  the web client: **`web`**.
- Binary and agent surface — the runner, harness adapters, MCP tools,
  `omnigent/tools/`, CLI, packaging: **`mcp`**.

A change that genuinely spans both is two commits, one per branch, not a merge
between them. `web` and `mcp` never merge into each other.

## History rules

Both working branches are built by replaying onto the current upstream, so:

- one commit per change, carrying only that change's final state;
- no merge commits, no revert-of-a-revert, no "restore what the re-land dropped";
- rebase onto upstream rather than merging upstream in;
- imperative subject describing the user-visible effect, not the mechanism.

If history needs repair, rewrite and force-push with a `backup/*` tag first —
do not add a commit that undoes another.

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
