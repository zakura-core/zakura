---
name: release-zakura
description: >-
  Prepare, review, and publish Zakura releases and release candidates. Use when
  bumping Zakura versions, preparing a release PR, validating the crates.io
  package graph, updating installer metadata, reviewing release readiness, or
  running the protected Create release workflow.
---

# Release Zakura

Use the repository's
`.github/PULL_REQUEST_TEMPLATE/release-checklist.md` as the canonical checklist.
This skill adds the Zakura-specific checks that are easy to miss.

## Safety

- Preparing or reviewing a release does not authorize publishing it.
- Get explicit confirmation immediately before dispatching `create-release.yml`
  or publishing crates to crates.io.
- Dispatch releases only from merged `main`.
- Never create a `v*` tag manually. `create-release.yml` is the only supported
  tag creation path.
- Do not promote a release candidate from pre-release to Latest.

## Gather release context

Determine:

- target tag, including the `v` prefix, such as `v1.0.0-rc5`
- previous GitHub tag
- latest published version of each affected crate on crates.io
- whether this release publishes crates, GitHub assets, Docker images, or all
  three
- changed crates since each crate's last published version
- the release tracking issue and checkpoint plan
- the latest successful main-branch sync-confidence run after relevant state or
  checkpoint changes

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
- affected root and crate changelogs according to project policy
- examples in release documentation only when they are intended to track the
  current release

Generate the stored config from the release branch; do not copy it blindly when
config defaults or fields changed.

### Installer metadata

Keep these four assignments in `scripts/install-zakura.sh` coherent:

```text
ZAKURA_RELEASE_TAG
ZAKURA_ARCHIVE_SHA256
ZAKURA_DOCKER_IMAGE
ZAKURA_COMPAT_DOCKER_IMAGE
```

Normally leave the complete metadata for the latest published release intact
until the next release exists. The raw-main installer is a documented install
route, and an all-zero or mismatched checksum breaks it.

The release workflow does not need source placeholders: it rewrites all four
values in the published installer and opens a follow-up PR with the real
metadata. Review and merge that PR after the release. If a release process
explicitly stages source metadata beforehand, update all four assignments
together and call out that raw-main installation remains unavailable until the
new artifacts exist.

## Verify before opening or approving the PR

Run:

```bash
cargo metadata --no-deps --format-version 1 --locked
./scripts/check-release-version.sh <tag>
bash -n scripts/install-zakura.sh
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
- require successful main-branch sync-confidence evidence after the latest
  relevant state or checkpoint change

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
- Verify the release graph independently; a green ordinary PR build does not
  prove crates.io packaging.
- Confirm the root changelog or a separate draft contains concrete release notes
  for every user-visible change since the previous release.
- Audit the checklist against `docs/release-tag-protection.md`; ignore or fix any
  stale instruction to promote a hyphenated RC tag to Latest.
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
5. publish release assets and Docker images

Do not retry from an unmerged branch. A version mismatch failure means `main`
still has the previous package version.

## Post-release verification

- Verify both `zakurad-<tag>` archives, the manifest, installer, and
  `SHA256SUMS.txt` are present.
- Verify checksums and installer pins.
- Verify the standard Docker image has amd64 and arm64; verify the
  zcashd-compat image has amd64.
- Review and merge the installer metadata update PR.
- Publish only changed crates, preserving dependency order.
- Install the exact version from crates.io and run `zakurad --version`.
- Replace the boilerplate GitHub release body with concrete notes from the final
  changelog or approved release-note draft.
- Keep release candidates marked as pre-releases.
- Run sync-confidence validation when the release scope or operator plan
  requires it.

## Review output

When reviewing readiness, report:

1. blockers
2. missing gates or skipped checks
3. verified items
4. exact remaining publish steps

Distinguish repository changes required before merge from post-release
automation and operator follow-up.
