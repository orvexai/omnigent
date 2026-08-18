<!-- bmad:context -->
<!-- Verified 2026-08-14 against f76fb96c1df05a168521c38c646d2e645b275751. Managed by bmad-project-context; edits inside this block are replaced on refresh. Keep anything you want preserved outside the markers. -->

## web

The React/TypeScript/Vite client renders Omnigent sessions across browser and native shells. Parts of its streaming model intentionally mirror the Python client SDK. The authoritative mapping and web-only divergences are documented in `web/README.md`.

## Where things are

- Reducer, block, event, type, and SSE mirror mappings: `web/README.md`, under “Reducer parity”
- Python source counterparts: `sdks/python-client/omnigent_client/`
- TypeScript counterparts and tests: `web/src/lib/`

## Running and verifying

- Run both the relevant Python SDK tests and web Vitest tests when changing mirrored streaming behavior; no cross-language CI gate detects drift.

## Conventions that differ from defaults

- When changing a mirrored Python reducer, block, event, type, or SSE implementation, update its TypeScript counterpart and the corresponding colocated test.
- Preserve the intentional web-only divergences documented in `web/README.md`; do not restore parity by copying Python behavior blindly.

<!-- /bmad:context -->
