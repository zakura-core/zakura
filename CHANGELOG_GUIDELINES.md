# Changelog Guidelines

How and when to update the changelogs in this repository.

## Which file records what

| File | Records | Audience |
| --- | --- | --- |
| `changelog-unreleased/<PR>.md` | Unreleased root entries owned by one PR | Reviewers and release tooling |
| `CHANGELOG.md` (root) | User-visible `zakurad` changes | Node operators |
| `CHANGELOG_PARAMS.md` | Parameter re-tunings (constants, defaults, timeouts, limits) | Reviewers and operators |

`CHANGELOG_PARAMS.md` is a compact ledger that complements the prose
changelogs; it stays maintained at all times, including through the pre-1.0.0
freeze described below.

## The v1.0.0 baseline ("Initial release")

Zakura v1.0.0 is the fork point and the project's first release, so there are
no earlier Zakura releases to describe deltas against:

- Until v1.0.0 ships, the changelog is **frozen**: do not add change entries to
  the root changelog, even for user-visible changes. This
  intentionally overrides the general rule below.
- At v1.0.0, the root changelog carries a single "Initial release" entry plus a
  note that the codebase is a fork of
  [Zebra](https://github.com/ZcashFoundation/zebra) at v5.0.0, pointing at
  upstream's changelog for pre-fork history. The delta against upstream Zebra
  belongs in release notes and the README, not in versioned changelog
  sections.
- The `v1.0.0-rc*` release candidates are pre-releases _of_ v1.0.0 and never
  get their own entries.

After v1.0.0, normal changelog maintenance (everything below) resumes.

## One fragment per pull request

Ordinary PRs do not edit the shared root changelog. After opening a draft PR,
add exactly one `changelog-unreleased/<PR-number>.md` file. Keeping each PR in
its own file avoids merge conflicts while preserving the link between the
change, its review, and its release note.

A user-visible fragment contains one or more Keep a Changelog categories:

```markdown
## Fixed

- Fixed an operator-visible problem
  ([#123](https://github.com/zakura-core/zakura/pull/123)).
```

Internal-only PRs still own a fragment, but use an explicit exclusion with a
reason:

```markdown
<!-- changelog: none -->

This PR only changes tests and has no operator-visible effect.
```

Dependabot and release PRs are the only automated exceptions to the one-file
check. The `C-exclude-from-changelog` label remains useful release metadata,
but does not replace the explicit fragment for ordinary PRs.

Run `./scripts/changelog.py check` locally. CI validates the syntax and checks
that the fragment filename matches the PR number. The concise format reference
is in
[`changelog-unreleased/README.md`](changelog-unreleased/README.md).

## Root `CHANGELOG.md`

- Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/); versions
  follow [Semantic Versioning](https://semver.org).
- Add a fragment entry in the same PR as any **user-visible** change: behavior,
  RPCs, command-line, configuration, performance, or supported platforms.
- Use the explicit no-changelog fragment for internal-only changes such as
  refactors, CI, tests, and docs.
- Categories: `### Added`, `### Changed`, `### Deprecated`, `### Removed`,
  `### Fixed`, `### Security`. Prefer `Fixed` if you're not sure.
- Write for node operators: describe the observable effect, not the
  implementation. Start each item with a verb and link the PR, for example
  `- Fixed X so that Y ([#123](https://github.com/zakura-core/zakura/pull/123))`.
- Label PRs accurately (`C-feature`, `C-bug`, `C-security`, …) so repository
  triage agrees with the fragment category.
- A change to a tunable parameter gets a row in `CHANGELOG_PARAMS.md` _in
  addition to_ a changelog entry when it is user-visible.

## Library crates

The repository does not maintain per-crate changelogs. Release preparation
uses `cargo semver-checks`, `cargo public-api diff`, and the code diff to choose
version bumps for changed publishable crates. Those checks are release inputs,
not permanent changelog entries.

## Release assembly

After the `zakura` package version is bumped on the release branch, run:

```sh
make prepare-release-changelog RELEASE_TAG=vX.Y.Z
```

The command validates and consumes all pending fragments, merges their entries
by category, and creates the new root changelog version section. Review and
commit the generated changelog and fragment deletions. Then run the full
release gate:

```sh
make pre-release RELEASE_TAG=vX.Y.Z BASE_TAG=v<previous>
```

The gate runs the assembler in check mode and fails if fragments remain or the
generated changelog was not committed. Release PRs must contain no pending
fragment files.

## Security entries

Use the `### Security` category, and coordinate timing with the disclosure
process: an entry must not describe an undisclosed or unfixed vulnerability.
