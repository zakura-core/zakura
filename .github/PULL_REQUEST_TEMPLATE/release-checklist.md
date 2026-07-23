---
name: "Release Checklist Template"
about: "Checklist to create and publish a Zakura release"
title: "Release Zakura (version)"
labels: "A-release, C-exclude-from-changelog, P-Critical :ambulance:"
assignees: ""
---

# Prepare for the Release

# Mainnet Release State

For performance and security, every release should carry a current Mainnet checkpoint list and its matching VCT frontier.

- [ ] Run the [Update Mainnet release state workflow](https://github.com/zakura-core/zakura/actions/workflows/update-release-state.yml) from `main`. It imports the newest publisher bundle and opens or updates a draft PR (it exits green with no PR when the committed state is already current).
- [ ] Review and merge that draft PR: the diff is append-only over the committed checkpoint list; spot-check a few new heights and the terminal hash against an independent node or explorer.
- [ ] `make pre-release` verifies the committed pairing and rejects pre-pipeline `legacy-bootstrap` state; for an emergency release with a broken publisher, export `ZAKURA_ALLOW_BOOTSTRAP_RELEASE_STATE=1` locally, check the `allow_bootstrap_release_state` input when dispatching the Create release workflow, and note it in the release PR.
- [ ] Testnet checkpoints are still updated manually when needed, per [the zakura-checkpoints README](https://github.com/zakura-core/zakura/blob/main/zakura-utils/README.md#zakura-checkpoints).

# Missed Dependency Updates

Sometimes `dependabot` misses some dependency updates, or we accidentally turned them off.

This step can be skipped if there is a large pending dependency upgrade. (For example, shared ECC crates.)

Here's how we make sure we got everything:

- [ ] Run `cargo update` on the latest `main` branch, and keep the output
- [ ] Until we bump the workspace MSRV to 1.88 or higher, `home` must be downgraded manually: `cargo update home@0.5.12 --precise 0.5.11`
- [ ] If needed, add duplicate dependency exceptions to `deny.toml`.
- [ ] If needed, remove resolved duplicate dependencies from `deny.toml`
- [ ] Open a separate PR with the changes
- [ ] Add the output of `cargo update` to that PR as a comment

# Summarise Release Changes

These steps can be done a few days before the release, in the same PR:

## Change Log

Changelog policy lives in
[`CHANGELOG_GUIDELINES.md`](https://github.com/zakura-core/zakura/blob/main/CHANGELOG_GUIDELINES.md) —
follow it if it and these steps ever disagree. In particular, `v1.0.0` and its
release candidates take a single "Initial release" entry (already in place)
and **skip** the fragment-assembly steps below; they apply to releases after
`v1.0.0`.

Unreleased notes live in one `changelog-unreleased/<PR-number>.md` fragment per
PR. To prepare them for assembly:

- [ ] Run `./scripts/changelog.py check`.
- [ ] Review every pending fragment for concrete operator-visible effects.
- [ ] Remove trivial entries by changing their fragment to an explicit
      no-changelog fragment with a reason.
- [ ] Combine duplicate descriptions by editing the relevant fragments.
- [ ] Check each category. Prefer `Fixed` if you're not sure.
- [ ] Confirm every item contains its PR link.

Do not copy GitHub draft release notes or edit the shared `[Unreleased]`
section. The version-aware assembly runs after all package version bumps below.
Release Drafter remains responsible for the separate GitHub release-note draft.

## README

README updates can be skipped for urgent releases.

Update the README to:

- [ ] Remove any "Known Issues" that have been fixed since the last release.
- [ ] Update the "Build and Run Instructions" with any new dependencies.
      Check for changes in the `Dockerfile` since the last tag: `git diff <previous-release-tag> docker/Dockerfile`.
- [ ] If Zakura has started using newer Rust language features or standard library APIs, update the known working Rust version in the README, book, and `Cargo.toml`s

You can use a command like:

```sh
fastmod --fixed-strings '1.58' '1.65'
```

## Create the Release PR

- [ ] If a release-capable maintainer has asked you to hold releases, stop
      here until they lift the hold — a security hotfix may be in flight for
      the same version, invisible to you under embargo.
- [ ] Push the reviewed fragments and README updates into a new branch named
      `release/v<version>`, for example `release/v1.0.0` (CI triggers match
      `release/**`; any name different from the tag works, but never
      `hotfix/v*` — that namespace is reserved for the
      [hotfix release process](https://github.com/zakura-core/zakura/blob/main/docs/security-hotfix-release.md)).
- [ ] Create a release PR by adding `&template=release-checklist.md` to the comparing url ([Example](https://github.com/zakura-core/zakura/compare/release/v1.0.0?expand=1&template=release-checklist.md)).
- [ ] Freeze the [`batched` queue](https://dashboard.mergify.com/github/valargroup/repo/zakura/queues) using Mergify.
- [ ] Mark all the release PRs as `Critical` priority, so they go in the `urgent` Mergify queue.
- [ ] Mark all non-release PRs with `do-not-merge`, because Mergify checks approved PRs against every commit, even when a queue is frozen.
- [ ] Add the `A-release` tag to the release pull request in order for the `check-no-git-dependencies` to run.

## Zakura git sources dependencies

- [ ] Ensure the `check-no-git-dependencies` check passes.

This check runs automatically on pull requests with the `A-release` label. It must pass for crates to be published to crates.io. If the check fails, you should either halt the release process or proceed with the understanding that the crates will not be published on crates.io.

# Update Versions and End of Support

## Update Zakura Version

Zakura follows [semantic versioning](https://semver.org). Semantic versions look like: MAJOR.MINOR.PATCH[-TAG.PRE-RELEASE]

Choose a release level for `zakurad`. Release levels are based on user-visible changes from the changelog:

- Mainnet Network Upgrades are `major` releases
- significant new features or behaviour changes; changes to RPCs, command-line, or configs; and deprecations or removals are `minor` releases
- otherwise, it is a `patch` release

**This step is mandatory for every release, including release candidates.**
Release binaries are built without `.git`, so `zakurad --version` reports the
`zakura` package version, not the tag — v1.0.0-rc1 was tagged without this bump
and its binaries self-report `1.0.0-rc0`. The `release-binaries.yml` workflow
refuses to build or publish assets for a tag that does not match the package
version.

- [ ] Bump the `zakura` package version to the release version:

```sh
cargo release version --verbose --execute --allow-branch '*' -p zakura patch # [ major | minor ]
```

- [ ] Generate and commit the stored config for the new version — the
      `last_config_is_stored` acceptance test derives the expected filename
      from the package version and fails without it:

```sh
cargo build --bin zakurad &&
./target/debug/zakurad generate |
sed "s#${XDG_CACHE_HOME:-$HOME/.cache}/zakura#cache_dir#g" |
sed "s#$HOME/.zakura#identity_dir#g" \
  > zakurad/tests/common/configs/v<version>.toml
```

The replacements are global path-string substitutions, mirroring
`last_config_is_stored` — the default cache path also appears in fields other
than `cache_dir` (for example `cookie_dir`), so per-field rewrites produce a
snapshot the test rejects.

## Update Crate Versions

If you're publishing crates for the first time, [log in to crates.io](https://github.com/zakura-core/zakura/dev/crate-owners.html#logging-in-to-cratesio),
and make sure you're a member of owners group.

The `Semver checks` CI job enforces bump-with-change against stable
crates.io baselines on every Rust PR, so most bumps already exist by release
time; the steps below review and complete them (for example, for non-API
changes that still warrant publishing).

Check that the release will work:

- [ ] Review the changed-crate version advisory from `make pre-release`. It
      warns when publishable crates changed since the latest release tag without
      a package version bump. This warning is local-only and advisory; unchanged
      crates are not bumped or published.
- [ ] Update (or install) `semver-checks`: `cargo +stable install cargo-semver-checks --locked`
- [ ] Update (or install) `public-api`: `cargo +stable install cargo-public-api --locked`
- [ ] For each crate that requires a release:
  - [ ] Determine which type of release to make. Run `semver-checks` to list API
        changes: `cargo semver-checks -p <crate> --default-features`. If there are
        breaking API changes, do a major release, or try to revert the API change
        if it was accidental. Otherwise do a minor or patch release depending on
        whether a new API was added. Note that `semver-checks` won't work
        if the previous realase was yanked; you will have to determine the
        type of release manually.
  - [ ] Review `cargo public-api diff latest -p <crate> -sss` alongside
        `cargo semver-checks` when choosing the bump. Per-crate changelogs are
        not maintained.
  - [ ] Update crate versions:

```sh
cargo release version --verbose --execute --allow-branch '*' -p <crate> patch # [ major | minor ]
# zakura only
cargo release replace --verbose --execute --allow-branch '*' -p zakura
```

- [ ] Commit and push the above version changes to the release branch.

## Assemble and Verify the Change Log

- [ ] Assemble the fragments after the `zakura` package version bump is final:

```sh
make prepare-release-changelog RELEASE_TAG=v<version>
```

- [ ] Confirm the `make/release.mk` target consumed every numbered
      `changelog-unreleased/<PR-number>.md` file, including no-changelog
      fragments. Keep `changelog-unreleased/README.md`; it documents the
      fragment format.
- [ ] Review the generated root changelog.
- [ ] For a stable release, confirm the generated section combines and replaces
      every matching `v<version>-rc*` changelog section.
- [ ] Commit the generated changelog and fragment deletions.
- [ ] On that release commit, run the complete pre-release gate with the
      previous release tag as the base:
      `make pre-release RELEASE_TAG=v<version> BASE_TAG=v<previous-release-tag>`.
      For example:
      `make pre-release RELEASE_TAG=v1.0.3 BASE_TAG=v1.0.2`.

## Update End of Support

The end of support height is calculated from the current blockchain height:

- [ ] Find where the Zcash blockchain tip is now by using a [Zcash Block Explorer](https://mainnet.zcashexplorer.app/) or other tool.
- [ ] Replace `ESTIMATED_RELEASE_HEIGHT` in [`end_of_support.rs`](https://github.com/zakura-core/zakura/blob/main/zakurad/src/components/sync/end_of_support.rs) with the height you estimate the release will be tagged. (The release-state PR floors this value near its bundle height, but this manual estimate is authoritative — with an 18-day support window, days matter.)

<details>

<summary>Optional: calculate the release tagging height</summary>

- Find where the Zcash blockchain tip is now by using a [Zcash Block Explorer](https://mainnet.zcashexplorer.app/) or other tool.
- Add `1152` blocks for each day until the release
- For example, if the release is in 3 days, add `1152 * 3` to the current Mainnet block height

</details>

## Update the Release PR

- [ ] Push the version increments and the release constants to the release branch.

# Publish the Zakura Release

## Create the GitHub Pre-Release

- [ ] Wait for all the release PRs to be merged
- [ ] Run the [Create release workflow](https://github.com/zakura-core/zakura/actions/workflows/create-release.yml)
      from `main`, entering the exact version tag, for example `v1.0.0-rc2`.
      The workflow verifies that the tag matches the `zakura` package version,
      then builds and verifies the assets without creating a tag.
- [ ] Wait for the build and no-push Docker checks to pass, then approve the
      `release` environment deployment. The workflow publishes a complete
      pre-release and creates the protected tag as its final step.
- [ ] Review and update the new release description against the final changelog
      you created, starting just _after_ the title `## [Zakura ...` of the
      current version and ending just _before_ the title of the previous
      release.

## Test the Pre-Release

- [ ] Wait until the release assets and Docker images have been built:
  - [ ] [release-binaries.yml](https://github.com/zakura-core/zakura/actions/workflows/release-binaries.yml?query=event%3Apush)

## Promote Release (stable releases only)

Pre-releases are **never** promoted — see
[Promotion and the "Latest" Release](https://github.com/zakura-core/zakura/blob/main/docs/release-tag-protection.md#promotion-and-the-latest-release).
For a release candidate, skip this section: the release stays a pre-release
from publication until deletion.

- [ ] For a stable release, after `make sign-release` has run against the tag:
      [edit the release](https://github.com/zakura-core/zakura/releases) to
      disable 'pre-release' **and** check "Set as the latest release"
      (`make_latest: true`) — both steps are explicit; nothing does this
      automatically.

## Publish Crates

- [ ] [Run `cargo login`](https://github.com/zakura-core/zakura/dev/crate-owners.html#logging-in-to-cratesio)
- [ ] It is recommended that the following step be run from a fresh checkout of
      the repo, to avoid accidentally publishing files like e.g. logs that might
      be lingering around
- [ ] Publish the crates to crates.io; edit the list to only include the crates that
      have been changed, but keep their overall order:

```
for c in zakura-test zakura-tower-fallback zakura-jsonl-trace zakura-chain zakura-tower-batch-control zakura-node-services zakura-script zakura-state zakura-consensus zakura-network zakura-rpc zakura-utils zakura; do cargo release publish --verbose --execute -p $c; done
```

- [ ] Check that Zakura can be installed from `crates.io`:
      `cargo install --locked --force --version <version> zakura && ~/.cargo/bin/zakurad`
      and put the output in a comment on the PR.

## Publish Docker Images

- [ ] Confirm the pinned zcashd compat manifest is ready before publishing:
  - [ ] Update [`zakurad/zcashd-compat-manifest.json`](https://github.com/zakura-core/zakura/blob/main/zakurad/zcashd-compat-manifest.json) to the intended `zcashd` compat release (it is the single source of truth: zakurad embeds it at compile time and CI/Docker builds read it directly).
  - [ ] Confirm the manifest contains only the `x86_64-pc-linux-gnu` artifact before publishing zcashd-compat Docker images.
  - [ ] Confirm the workflow logs show the expected `/usr/local/bin/zcashd --version` for the zcashd-compat linux/amd64 image variant.
- [ ] Wait for the [the Docker images to be published successfully](https://github.com/zakura-core/zakura/actions/workflows/release-binaries.yml?query=event%3Apush).
- [ ] Confirm `release-binaries.yml` published `zakurad-<tag>-linux-x86_64.tar.gz`, `zakurad-<tag>-linux-aarch64.tar.gz`, `zakurad-manifest-<tag>.json`, and `SHA256SUMS.txt` to the GitHub release.
- [ ] Wait for the new tag in the [Docker Hub zakura space](https://hub.docker.com/r/zakuracore/zakura/tags)
- [ ] Confirm `zakuracore/zakura:<version>` includes `linux/amd64` and `linux/arm64`, and `zakuracore/zakura:zcashd-compat-<version>` includes only `linux/amd64`.
- [ ] Un-freeze the [`batched` queue](https://dashboard.mergify.com/github/valargroup/repo/zakura/queues) using Mergify.
- [ ] Remove `do-not-merge` from the PRs you added it to

## Release Failures

If the pre-tag build or packaging stage fails, fix the failure on `main` and
dispatch the workflow again with the same version. No tag has been created, so
the version remains usable.

If testing fails after the pre-release has been published and tagged:

<details>

<summary>Tag a new release, following these instructions...</summary>

1. Fix the bug that caused the failure
2. Start a new `patch` release
3. Skip the **Release Preparation**, and start at the **Release Changes** step
4. Update `CHANGELOG.md` with details about the fix
5. Follow the release checklist for the new Zakura version

</details>
