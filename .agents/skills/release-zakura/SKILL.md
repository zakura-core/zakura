---
name: release-zakura
description: >-
  Prepare, review, and publish Zakura releases and release candidates. Use when
  bumping Zakura versions, preparing a release PR, validating the crates.io
  package graph, reviewing release readiness, or running the protected Create
  release workflow.
---

# Release Zakura

Use the repository's
`.github/PULL_REQUEST_TEMPLATE/release-checklist.md` as the canonical checklist.
Release policy (tag protection, promotion, retention) is canonical in
`docs/release-tag-protection.md`; this skill points at policy rather than
defining it, and adds the Zakura-specific checks that are easy to miss.

## Safety

- Preparing or reviewing a release does not authorize publishing it.
- Get explicit confirmation immediately before dispatching `create-release.yml`
  or publishing crates to crates.io.
- Dispatch releases only from merged `main`.
- Never create a `v*` tag manually. `create-release.yml` is the only supported
  tag creation path.
- Do not promote a release candidate from pre-release to Latest. Published
  release candidates stay up as pre-releases; removing one is an owner-level
  decision, never part of a release flow.

## Gather release context

Determine:

- target tag, including the `v` prefix, such as `v1.0.0-rc5`
- previous GitHub tag
- latest published version of each affected crate on crates.io
- whether this release publishes crates, GitHub assets, Docker images, or all
  three
- changed crates since each crate's last published version
- the release tracking issue and checkpoint plan

Do not infer the crate publish set only from the previous GitHub tag. A binary
release may have skipped crates.io publishing.

Useful commands:

```bash
gh release list --repo zakura-core/zakura --limit 10
cargo search zakura --limit 5
git diff --stat <previous-tag>
```

## Prepare the release branch

### Package versions

Always update the `zakura` package version in `zakurad/Cargo.toml`; release
binaries self-report `CARGO_PKG_VERSION`.

Stable releases always publish to crates.io. Release candidates decide
per-release: a crates.io-publishing release candidate bumps all changed
crates like a stable release; a GitHub-only release candidate bumps only
`zakura`.

For crates.io publishing:

1. Identify changed publishable crates.
2. Bump those crates before their dependents.
3. Update every direct dependency requirement that must select the new crate.
4. Refresh `Cargo.lock`.
5. Confirm unchanged published versions still satisfy the resulting graph.

Partial version graphs are allowed, but all tooling must handle them. Do not
assume every publishable crate has the `zakura` package version.

### Release metadata

Update:

- the README `cargo install --git ... --tag` example
- `zakurad/tests/common/configs/<version>.toml`
- `ESTIMATED_RELEASE_HEIGHT` from the current chain tip and expected tag date
- pending `changelog-unreleased/<PR-number>.md` fragments according to project
  policy
- the root changelog by running the fragment assembler after the `zakura`
  package version bump is final
- examples in release documentation only when they are intended to track the
  current release

Generate the stored config from the release branch; do not copy it blindly when
config defaults or fields changed.

After the `zakura` package version bump is final, run:

```bash
make prepare-release-changelog RELEASE_TAG=<tag>
```

Review and commit the generated root changelog and fragment deletions.
For a stable release, confirm the generated section combines and replaces all
matching release-candidate sections; no `X.Y.Z-rc*` section for that stable
version should remain in the root changelog.

## Verify before opening or approving the PR

Run:

```bash
cargo metadata --no-deps --format-version 1 --locked
make pre-release RELEASE_TAG=<tag> BASE_TAG=<previous-tag>
./scripts/check-crate-packaging.sh --verify
```

Also:

- run Markdown lint on every changed Markdown file
- check IDE diagnostics on changed files
- run `cargo semver-checks` and `cargo public-api diff` for changed library
  crates when publishing them
- verify the packaging script resolves each archive using that crate's actual
  version when the workspace contains mixed versions
- confirm the package preflight rebuilds packaged archives, not just workspace
  sources
- confirm checkpoints are current or record an explicit rapid-RC waiver

Follow the repository's risk policy for additional Rust checks. Report skipped
checks and why.

## Release PR requirements

- Use a conventional title such as
  `chore(release): prepare v1.0.0-rc5`.
- Add the `A-release` label before treating CI as complete.
- Confirm `Check no git dependencies` and Docker configuration/build checks ran;
  a skipped result usually means the label is missing.
- Include motivation, solution, test evidence, issue/reference links, risk
  classification, follow-up work, and AI disclosure.
- Confirm all `changelog-unreleased/<PR-number>.md` files were consumed and the
  generated root changelog was committed.
- Verify the release graph independently; a green ordinary PR build does not
  prove crates.io packaging.
- Post-1.0.0 releases only: confirm the assembled root changelog contains
  concrete release notes for every user-visible change since the previous
  release. Through 1.0.0 the changelog is frozen — 1.0.0 ships "Initial
  release" only in the root changelog.
- Audit the checklist against `docs/release-tag-protection.md`.
- Wait for required human approval and all enabled CI checks.

## Publish

After the release PR is merged and explicit confirmation is given:

```bash
gh workflow run create-release.yml \
  --repo zakura-core/zakura \
  --ref main \
  -f release_tag=<tag>
```

The workflow must:

1. validate the tag against the `zakura` package version
2. build and verify assets before tag creation
3. wait for approval of the `release` environment
4. create the immutable tag and GitHub pre-release
5. publish the release assets; the tag push then triggers
   `release-binaries.yml`, which publishes the Docker images

Do not retry from an unmerged branch. A version mismatch failure means `main`
still has the previous package version.

## Post-release verification

- Verify both `zakurad-<tag>` archives, the manifest, and `SHA256SUMS.txt` are
  present.
- Verify release checksums.
- Verify the standard Docker image has amd64 and arm64; verify the
  zcashd-compat image has amd64.
- Publish only changed crates, preserving dependency order.
- Install the exact version from crates.io and run `zakurad --version`.
- Replace the boilerplate GitHub release body with concrete notes from the final
  changelog or approved release-note draft.
- Keep release candidates marked as pre-releases.

## Review output

When reviewing readiness, report:

1. blockers
2. missing gates or skipped checks
3. verified items
4. exact remaining publish steps

Distinguish repository changes required before merge from post-release
automation and operator follow-up.
