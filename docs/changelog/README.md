# Changelog development artifacts

This directory stores the artifacts the project uses to record and track
changes between and across releases. It exists for development, review, and
release tooling — it is not user documentation. Node operators looking for
what changed in a release should read the root
[`CHANGELOG.md`](../../CHANGELOG.md).

Contents:

- [`guidelines.md`](guidelines.md) — how and when to update the changelogs
  in this repository (policy).
- [`params.md`](params.md) — a compact ledger of parameter re-tunings
  (constants, defaults, timeouts, limits).
- [`unreleased/`](unreleased/README.md) — one pending changelog fragment per
  pull request, consumed into the root `CHANGELOG.md` at release assembly.
