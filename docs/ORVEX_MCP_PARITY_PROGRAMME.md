# MCP↔UI parity — consolidated programme spec

Consolidated 2026-08-20 from `WISHLIST-STATE.md`, `MCP-PARITY-CONTRACT.md`,
`MCP-DRIFT-INVENTORY.md`, `U1-TRUTH-CONTRACT.md`, `INBOX-CONTRACT.md`, the
friction logs, and a full audit of `~/repos/codex-plugin-cc` (Codex app-server
protocol) for requirements worth adopting.

This file is the single source of truth for the programme. It is re-read on every
Ralph iteration. If it disagrees with an older document, this file wins.

---

## 0. The requirement

> **P (Parity).** For every session action a human can take in the web UI against
> a session, an agent holding equivalent authority can take the same action,
> against the same session, through MCP, with the same observable effect — or
> receives an explicit, machine-readable refusal naming which of {permission,
> harness capability, liveness} denied it.

Two clauses are load-bearing:

- **"the same session"** — including sub-agents, sub-agents of trees the caller
  did not spawn, and sessions on other hosts. The UI navigates the whole tree;
  MCP must too.
- **"or an explicit refusal"** — parity is satisfied by an honest *no*. It is
  **not** satisfied by silence. A message that queues indefinitely is a worse
  contract than a refusal, because the caller cannot tell slow from ignored from
  declined.

**Restriction burden of proof.** Any narrowing on the MCP surface relative to what
the server permits must carry a stated reason that survives "but I can do it in
the UI". Absent that reason, the narrowing is a defect.

**Never conclude a capability is impossible from a declaration**, a code comment,
or a tool description. Establish capability by execution, or state that you could
not. `harness_plugins.py` declares `steering=False` with a comment claiming live
verification; the owner steers via the UI daily.

---

## 1. Status

### 1.1 Landed on `mcp` (do not redo)

| what | commit |
|---|---|
| queue/steer into a busy session (`if_busy: reject\|queue\|interrupt`) | `bb5a6423`, `512efb21` |
| agent-to-agent message threads, durable across runner restart | `24473f0e`, `cc9b2384` |
| sub-agent rows carry status + readable name | `665b276f` |
| sub-agent listing made usable | `1b236c4a` |
| large inbox backlog delivered instead of one oversized result | `a11ab012` |
| agent provenance + session failures exposed | `fdb71d0d` |
| native sub-agent result no longer delivered twice | `d1b7cffd` |
| closing dead top-level sessions authorized | `9e368829` |
| `sys_session_list` bounded + pagination seam fixed (**Unit A**) | `ed5cce0d` ← current tip |

Unblockers already landed: `owner_forward`, `mcp dispatch`, `close-auth`, `host-owner`.

### 1.2 Canonical numbering — collision resolved

Two numbering schemes existed and they collide. **The parity-contract numbering
(U1–U7 below) is canonical.** The older messaging numbering (its "U7 queue/steer",
"U8 threads") is retired; that work is listed as landed in §1.1. `U9 Inbox` keeps
its own name because its contract is written against it.

---

## 2. Source documents

| doc | path |
|---|---|
| parity contract (master, 100 KB) | `/home/daniel/scratch-omnigent/MCP-PARITY-CONTRACT.md` |
| drift inventory (companion to §5) | `/home/daniel/scratch-omnigent/MCP-DRIFT-INVENTORY.md` |
| U1 contract (accepted, 1404 lines) | `/home/daniel/scratch-omnigent/U1-TRUTH-CONTRACT.md` |
| U9 inbox contract | `/home/daniel/scratch-omnigent/INBOX-CONTRACT.md` (recovered 2026-08-20 from transcript after `/tmp` was wiped) |
| parity evidence (live probes) | `/home/daniel/scratch-omnigent/PARITY-EVIDENCE.md` |
| programme tracker | `/home/daniel/scratch-omnigent/WISHLIST-STATE.md` |
| stack/branch status | `/home/daniel/scratch-omnigent/STACK-STATUS.md` (stale — see §5) |

Scratch lives on the main disk, never `/tmp`. `/tmp` is tmpfs and has already
destroyed one friction log and one contract.

---

## 3. Work units

Size of the remaining problem (parity contract §4.8): **22 missing tools, 18
missing parameters, 6 authority gaps, ~60 declaration-drift rows, 2 deliberate
server-side exclusions, and 0 genuine harness limitations.** Nothing is blocked by
a vendor limit. Every row is omission, narrowing, or drift.

### U1 — Truth (registry + declaration conformance) — **DO THIS FIRST, ALONE**

Contract accepted: 22 criteria (14 detectors / 8 guards), 10 files, 6 stages.

- **Implementation base `B` = `ed5cce0d`.** §1.6 defines it as "the mcp tip after
  the listing unit merges"; that is now determinate. Record it in the PR
  description at start.
- **Evidence pin `E` = `0d5ca204` is orphaned.** `mcp` was rebased onto `web`
  three times on 2026-08-20, so `0d5ca204` is no longer an ancestor of `mcp`. It
  survives only via tags `backup/mcp-preclean` and `backup/unita-0f2965fb` —
  **do not delete those tags.** `git show 0d5ca204:<path>` still resolves.
  **Re-locate every symbol by name; never trust a line number.**
- **Red-first gate (mandatory).** Run every §6 detector against `B` first. Any
  detector already green is struck from the contract with its reason recorded —
  never quietly satisfied.

Why first: it needs no design decision, it relieves the loudest symptom (agents
cannot find sessions / believe restrictions that do not exist), and it lands the
registry every later unit builds on.

### U2 — Steer semantics + queue identity (depends: U1)
### U2b — Threads coherence (depends: U2 disposition shape; only unit touching `runner/app.py`)
### U3 — Control verbs (depends: U1; parallel with U2/U4)
### U4 — Settings parity (depends: U1; parallel with U2/U3)
### U5 — Discovery (depends: Unit A — now landed)
### U6 — Approvals + authority (depends: U1, U2)
### U7 — Durable unified queue (`web` + `mcp`; separate contract)

**The parallelism unlock:** once U1 lands the registry, U2, U2b, U3 and U4 touch
different dispatch functions and different tools and can run concurrently. That is
the point where this stops being a queue.

### U9 — Inbox (contract written; partially landed)

Its header uses the **retired** numbering ("composes with U7 queue/steer, U8
threads") — read that as the landed work in §1.1, not as units in §3.

Solves F-13, F-17, F-19 fully and F-9 by narrowing. Four stages: funnel gate →
spill → reparenting → `work_id` drain filter.

- The contract is pinned to `.worktrees/mcp` @ `25c8a395`, written **before** the
  three 2026-08-20 rebases. Treat every `file:line` in it as pre-rebase and
  **re-locate every symbol by name**, exactly as U1 requires.
- Stage 1 partially landed as `d1b7cffd`.
- **Still live:** `_truncate_inbox_output` at `omnigent/runner/tool_dispatch.py:10121`
  caps inbox payloads and counts characters *dropped*, so a ~16,250-char result
  silently loses ~4,250 characters. Stage 2 (spill) is not done.

---

## 4. Requirements adopted from the Codex app-server audit

Audited `~/repos/codex-plugin-cc` (~6 kLOC: app-server JSON-RPC client, broker,
job store, hooks). Each row below is a design already proven in a shipping product
that solves a problem this programme has open. Adopt the idea, not the code.

| # | Adopted requirement | Evidence in codex-plugin-cc | Lands in |
|---|---|---|---|
| A1 | **Busy gates the data plane only; the control plane stays open.** A busy session must still admit `interrupt`/`cancel` from a *different* authorized caller while a turn is in flight. | `app-server-broker.mjs` refuses every request from a non-owning socket with `-32001` **except** `turn/interrupt` (`allowInterruptDuringActiveStream`) | U2, U3 |
| A2 | **A busy refusal is a numbered, machine-readable code** the caller can branch on, not prose. | `BROKER_BUSY_RPC_CODE = -32001`, consumed by `withAppServer` to fall back automatically | U2 |
| A3 | **Every refusal carries a `next_action`.** | "Job X is still running. Check /codex:status and try again once it finishes."; "Use a longer job id." | U1 (descriptions), all units (errors) |
| A4 | **Typed item taxonomy on the event stream** — `commandExecution`, `fileChange`, `mcpToolCall`, `dynamicToolCall`, `collabAgentToolCall`, `webSearch`, `enteredReviewMode`/`exitedReviewMode`, `agentMessage`, `reasoning` — each with `started`/`completed` lifecycle. | `describeStartedItem` / `recordItem` in `lib/codex.mjs` | U5, U9 |
| A5 | **`phase` as a first-class progress field**, written only when it changes (`starting`/`investigating`/`editing`/`running`/`verifying`/`finalizing`/`done`/`failed`). Answers "working or stalled?" — which `last_activity_at` provably cannot. | `createJobProgressUpdater` diffs phase before writing | C2, U5 |
| A6 | **An explicit done marker.** `agentMessage` with `phase: "final_answer"` plus drained sub-agent turns; inference is the *fallback*, is time-boxed, and is **labelled as inferred** in the output. | `finalAnswerSeen`, `scheduleInferredCompletion`, `completeTurn(..., {inferred: true})` | U9, C1 |
| A7 | **Sub-agent correlation is structural, not guessed.** A collaboration tool call carries `receiverThreadIds`; the parent registers those threads and tracks a turn ID per thread. | `registerThread`, `state.threadTurnIds`, `belongsToTurn` | C1, U5 |
| A8 | **Notifications are addressed to the caller that owns the turn**, not broadcast into a global drain. Ownership is released on `turn/completed` for an owned thread. | `routeNotification` in the broker | U9 (the strongest argument for the `work_id` filter) |
| A9 | **Buffer notifications that arrive before the correlation ID is known**, then replay them through the filter. | `state.bufferedNotifications` + replay in `captureTurn` | U9 |
| A10 | **Verbosity is negotiated by the consumer, not truncated by the producer.** The client declares which notification classes it does not want. | `optOutNotificationMethods: ["item/agentMessage/delta", "item/reasoning/*Delta", ...]` | U5, U9 (replaces lossy `_truncate_inbox_output`) |
| A11 | **Two-tier records**: a bounded index (capped, pruned, orphan files cleaned) + a full per-record file by ID + a separate append-only log. | `state.json` `jobs[]` (MAX_JOBS 50) + `jobs/<id>.json` + `jobs/<id>.log` | U5 |
| A12 | **Purpose-shaped projections, not uniform row dumps**: `running` / `latestFinished` / `recent` (bounded, `--all` to override), with a 4-line `progressPreview` only for active or failed rows. | `buildStatusSnapshot` | U5 |
| A13 | **`elapsed` and `duration` are distinct fields** (in-flight vs terminal). | `enrichJob` | U5 |
| A14 | **Liveness is checkable, not inferred**: record `pid` while running, null it on terminal. | `runTrackedJob` | host/session liveness, U5 |
| A15 | **Structured output contract on dispatch.** A send may carry an `output_schema`; the result is validated and a parse failure is reported as `parse_error`, never as prose. | `outputSchema` on `turn/start`; `schemas/review-output.schema.json`; `parseStructuredOutput` | U6, reviews |
| A16 | **A real query surface for discovery**: `cwd`, `limit`, `sortKey`, `sourceKinds`, `searchTerm`. | `thread/list` params in `findLatestTaskThread` | U5 |
| A17 | **Prefix-matching IDs with an explicit ambiguity refusal** — never a silent best guess. | `matchJobReference`: "Job reference X is ambiguous. Use a longer job id." | U1, U5 |
| A18 | **Ephemeral sessions**: a session explicitly created as disposable and not persisted into the tree. Throwaway test sessions currently pollute the tree forever. | `ephemeral: true` on `thread/start` | U4, lifecycle |
| A19 | **Human-readable durable session identity**, derived from the request and findable later by name. Today sub-agent titles leak handles like `Plan:ae59583209bc82629`. | `buildTaskThreadName`, `thread/name/set`, prefix search | U4, U5 |
| A20 | **Cancel is two-layer and reports both layers**: graceful `interrupt` over a second connection first, then forceful terminate, returning `{attempted, delivered}` separately. `close` is not `stop`. | `handleCancel`; `terminateProcessTree` returns `{attempted, delivered, method}` | U3, close semantics (evidence §2.7) |
| A21 | **Provenance on projected fields**: `source` (where the fact came from), `verified: true\|false\|null` (proved / configured-but-unproved / unknown), `explicit: true\|false` (caller-chosen vs inferred default). | `buildAuthStatus`, `resolveReviewTarget` | U1, U5 |
| A22 | **Capability probing distinguishes "method unsupported" from "call failed"**, and maps unsupported to a remediation. Only `unknown variant`/`unknown method` is swallowed; everything else rethrows. | `startThread`'s `thread/name/set` guard; `-32601` → upgrade instruction | U1 |
| A23 | **Idempotent creation keyed by content hash**, so a repeat never becomes a 500. Directly targets the duplicate-child-title → HTTP 500 defect. | `external_agent_session_imports.json` ledger keyed by `source_path` + `content_sha256` | session create |
| A24 | **Scope is one documented default with an explicit opt-out** — not two code paths that silently differ. Default session-scoped; an explicit ID searches globally. | `filterJobsForCurrentSession` vs unfiltered `matchJobReference` | D1, U1 |
| A25 | **Cross-harness session import as a first-class primitive**, with a completion notification and a recorded imported-thread ID. | `externalAgentConfig/import`, `EXTERNAL_AGENT_IMPORT_COMPLETED` | future (cross-harness handoff) |

Rows A1, A8, A10 and A15 are the highest value: they are the four places where the
omnigent design is currently the *inverse* of a working one.

---

## 5. Constraints — verify these before trusting anything

- **Branch split is absolute.** `mcp` is stacked on `web` (`web ⊂ mcp`). Every
  server-side change goes on `web`; nothing that is not server-side ever does.
  U1–U6 are runner-and-tools only (`omnigent/tools/`, `omnigent/runner/`) —
  that is `mcp`. **No unit in this spec edits `omnigent/server/`.** If a unit
  turns out to need a server half, STOP and surface it; it is a separate commit
  on `web`. See `docs/ORVEX_REPO_LAYOUT.md`.
- **`STACK-STATUS.md` is stale.** It claims the stack is unpushed at `b1c75bd9`;
  `mcp` and `origin/mcp` are both `ed5cce0d`.
- **Ignore everything bmad.** ~50 untracked `.claude/skills/bmad-*/` dirs are
  present, are NOT used on this project, and `bmad-build` falsely declares itself
  mandatory for "any change request". That tree is **proved to change test
  outcomes** through on-disk skill discovery — identical source and tests give
  46 passed in a clean scratch copy and 5 failed from the worktree. Any
  "pre-existing failure" claim must be reproduced outside the contaminated tree.
- **`uv run` prunes extras.** Always `uv run --group test --extra all`; a bare
  `uv run` breaks pre-commit on files nobody touched.
- **Test scope comes from the changed-file list**, not from what the change is
  about. Deriving scope from the topic has failed three times.
- **Candidate IDs must be reproducible from git.** An ID nobody can recompute is
  a label, not a fact.
- **Never remove a worktree without checking** whether a reviewer is running in it.
- Sign off every commit (`git commit -s`). Never commit to `main`. The fork has
  issues disabled — deferrals go in commit bodies, not an issue tracker.

---

## 6. Definition of done, per unit

A unit is done when **all** of these hold:

1. an accepted Opus engineering contract exists for it;
2. every acceptance criterion in that contract is satisfied;
3. the red-first gate ran (where the contract requires one) and struck detectors
   are recorded with reasons;
4. all deterministic gates are green, run from a clean tree, with raw output —
   not an implementer summary — as the evidence;
5. an independent Codex/Sol technical review and an independent fresh-Opus
   requirements review both returned `approve` against the **same** candidate ID;
6. no unresolved BLOCKER or HIGH finding at confidence ≥ 80% remains;
7. the changed-file set is exactly the unit's declared write set.

---

## 7. Out of scope

Cross-host session creation (needs its own contract, §7.4 runner-principal
invariant) · making agents obey a steer (the goal is that a receiver can
*distinguish* an authorized steer, and a sender can *observe* the outcome) ·
the replica status-cache staleness recorded in `STATUS.md` · redesigning CI lanes ·
filing anything upstream to omnigent-ai/omnigent.

---

## 8. Upstream items for `codex-plugin-cc` (NOT omnigent work)

Found during the audit. Record only; do not act on them in this programme.

1. `review` / `adversarial-review` parse `--background` and `--wait` and then
   ignore them — `handleReviewCommand` always calls `runForegroundCommand`.
   Detaching only happens because Claude Code runs the Bash call with
   `run_in_background: true` (documented at `commands/review.md:38`). `task`, by
   contrast, genuinely detaches via `enqueueBackgroundTask`.
2. No `--resume-thread <id>`. `--resume-last` resolves the newest job with
   `jobClass === "task"` for the session, so any unrelated `task` call silently
   steals the resume target of a multi-role workflow.
3. The `SessionEnd` hook terminates every job whose `sessionId` matches the
   ending session and purges the records — which makes a long background job
   unusable from any harness whose session ends between steps.
