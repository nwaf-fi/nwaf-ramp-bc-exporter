# Changelog

All notable changes for the repository are recorded in this file.

Unreleased
---------

PR1 — Scaffold runtime packages
- Created `app/` and `lib/` packages and copied runtime modules to those locations.

PR2 — Compatibility shims
- Added thin shims at original paths that re-export from `app.*` and `lib.*` to preserve all import paths.

PR3 — Archive legacy scripts
- Moved legacy, one-off scripts into `archive/legacy_scripts/` and replaced originals with non-breaking shims.

PR4 — Tests & CI
- Added smoke import tests and a deterministic transform checksum test in `tests/`.
- Added GitHub Actions CI workflow at `.github/workflows/ci.yml`.

PR5 — Documentation & final cleanup
- Added this `CHANGELOG.md`, `ARCHIVE_MOVE_MAP.md`, and `PR_SUMMARY.md` describing the PRs and branch/commit plan.
