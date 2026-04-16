# PR Summary and branch plan

This repository refactor is split into five focused PRs to keep changes small and non-breaking.

PR1 - scaffold/runtime
- Branch: `pr1-scaffold-app-lib`
- Changes: add `app/` and `lib/` packages with copies of runtime modules.
- Commit message: "PR1: scaffold app/ and lib/ packages and copy runtime modules"

PR2 - shims
- Branch: `pr2-compat-shims`
- Changes: add thin compatibility shims at original top-level paths that re-export from the new packages.
- Commit message: "PR2: add thin compatibility shims to preserve import paths"

PR3 - archive legacy scripts
- Branch: `pr3-archive-legacy`
- Changes: move legacy scripts to `archive/legacy_scripts/`, add archive package, replace originals with non-breaking shims.
- Commit message: "PR3: move legacy scripts to archive/ and add non-breaking shims"

PR4 - tests and CI
- Branch: `pr4-tests-ci`
- Changes: add pytest smoke tests, deterministic transform checksum test, and GitHub Actions CI workflow.
- Commit message: "PR4: add smoke tests, transform checksum test, and CI workflow"

PR5 - docs & cleanup
- Branch: `pr5-docs-cleanup`
- Changes: changelog, archive move map, PR summary, small README migration notes, final linting and formatting as needed.
- Commit message: "PR5: docs, CHANGELOG, and final cleanup"

Notes for merging
- Merge PRs in order (PR1 → PR5). Each PR is non-breaking and includes tests; PR2/PR3 include DeprecationWarning shims to allow staged rollouts.
