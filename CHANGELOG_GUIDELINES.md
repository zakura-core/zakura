# Changelog Guidelines

How and when to update the changelogs in this repository.

## Which file records what

| File | Records | Audience |
| --- | --- | --- |
| `CHANGELOG.md` (root) | User-visible `zakurad` changes | Node operators |
| `<crate>/CHANGELOG.md` | Consumer-visible library API changes | Rust developers using the crate |
| `CHANGELOG_PARAMS.md` | Parameter re-tunings (constants, defaults, timeouts, limits) | Reviewers and operators |

`CHANGELOG_PARAMS.md` is a compact ledger that complements the prose
changelogs; it stays maintained at all times, including through the pre-1.0.0
freeze described below.

## The v1.0.0 baseline ("Initial release")

Zakura v1.0.0 is the fork point and the project's first release, so there are
no earlier Zakura releases to describe deltas against:

- Until v1.0.0 ships, changelogs are **frozen**: do not add change entries to
  the root or crate changelogs, even for user-visible changes. This
  intentionally overrides the general rule below.
- At v1.0.0, the root changelog and **every crate changelog** carry a single
  "Initial release" entry plus a note that the codebase is a fork of
  [Zebra](https://github.com/ZcashFoundation/zebra) at v5.0.0, pointing at
  upstream's changelog for pre-fork history. The delta against upstream Zebra
  belongs in release notes and the README, not in versioned changelog
  sections.
- The `v1.0.0-rc*` release candidates are pre-releases _of_ v1.0.0 and never
  get their own entries.

After v1.0.0, normal changelog maintenance (everything below) resumes.

## Root `CHANGELOG.md`

- Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/); versions
  follow [Semantic Versioning](https://semver.org).
- Add an entry under `## [Unreleased]` in the same PR as any **user-visible**
  change: behavior, RPCs, command-line, configuration, performance, supported
  platforms.
- Skip internal-only changes (refactors, CI, tests, docs). Use the
  `C-exclude-from-changelog` label for PRs that need no entry.
- Categories: `### Added`, `### Changed`, `### Deprecated`, `### Removed`,
  `### Fixed`, `### Security`. Prefer `Fixed` if you're not sure.
- Write for node operators: describe the observable effect, not the
  implementation. Start each item with a verb and link the PR, for example
  `- Fixed X so that Y ([#123](https://github.com/zakura-core/zakura/pull/123))`.
- Label PRs accurately (`C-feature`, `C-bug`, `C-security`, …) — the labels
  drive the Release Drafter draft that the release checklist folds into
  `CHANGELOG.md` at release time (see
  `.github/PULL_REQUEST_TEMPLATE/release-checklist.md`).
- A change to a tunable parameter gets a row in `CHANGELOG_PARAMS.md` _in
  addition to_ a changelog entry when it is user-visible.

## Crate changelogs

Workspace crate versions evolve independently: a crate is bumped and published
only when it has changes to publish, and its changelog moves on the same
cadence.

- Record **consumer-visible** changes only: public API additions, changes, and
  removals; MSRV bumps; feature-flag changes; semver-relevant behavior
  changes.
- A crate gains a version section when it is published at that version, in its
  bump PR. Use `cargo semver-checks` and `cargo public-api diff` to enumerate
  the API delta (the release checklist walks through this).
- Crates without changes get no entries — and are not bumped or published just
  to align version numbers.

## Security entries

Use the `### Security` category, and coordinate timing with the disclosure
process: an entry must not describe an undisclosed or unfixed vulnerability.
