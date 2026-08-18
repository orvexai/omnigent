## omnigent

Omnigent is an open-source meta-harness for running and governing AI agents. The Python 3.12 application lives in `omnigent/`; React/Vite, desktop, mobile, and editor clients live under `web/` and `editors/`. Contributor guidance lives in `CONTRIBUTING.md`, deeper system material in `docs/`, and BMad planning output in `_bmad-output/planning-artifacts/`.

## Policy

- Never commit org-specific changes to `main`; it is an exact fast-forward mirror of upstream. Keep org customizations on `orvex`.
- Sign off every commit with `git commit -s`.
- Before committing, run the pre-commit hook and fix everything it reports.
- Every PR must reference an issue and preserve every section and checkbox in `.github/pull_request_template.md`; UI changes must include images or video in Demo.
- Add or update focused tests for observable behavior changes. New user-facing features require an e2e happy-path test; web behavior changes require a colocated Vitest test, and user-facing UI changes also require `tests/e2e_ui/` coverage.
- Name the planned removal version in code and in the PR or commit description whenever deprecating a feature.

## Where things are

- Full contributor and review workflow: `CONTRIBUTING.md` and `.github/copilot-instructions.md`
- Web streaming-model parity rules: `web/AGENTS.md`
- Real-LLM journeys: `tests/e2e/AGENTS.md`
- Per-harness integration journeys: `tests/integration/AGENTS.md`
- `.github/triage_v2/` has its own scoped `AGENTS.md`.
- `omnigent/onboarding/agent/AGENTS.md`, `dev/repro-agent/AGENTS.md`, and `dev/resolve-agent/AGENTS.md` are product-agent prompts; edit them as runtime behavior, not as generic repository guidance.

## Running and verifying

- Use `omnidev` for worktree-safe full-stack testing. Open the exact UI URL it displays, and run checkout-bound CLI commands through `omnidev omnigent`; ports and state are isolated per worktree.
- Use WSL2 with the checkout in its Linux filesystem for Windows development; native Windows and Git Bash cannot run the full pytest/pre-commit toolchain.
- Follow the scoped test guides for integration and e2e prerequisites, credentials, background execution, and cleanup.
- End every task with concrete human verification steps, including inputs and expected behavior.

## Conventions that differ from defaults

- Keep code comments to one or two scenario-focused lines; do not narrate change history or cite issue or PR numbers.
- Application stores use `make_named_managed_session_maker` with stable, intent-based operation names. Use nested `query_name_scope` only for important distinct subqueries; never add `flush()` solely for observability.
- Keep framework-owned lifecycle and metadata instructions in the owning framework module and compose them in `omnigent/runtime/prompt.py`; harness adapters only transport the result and must not add lifecycle metadata to `AgentSpec`.
