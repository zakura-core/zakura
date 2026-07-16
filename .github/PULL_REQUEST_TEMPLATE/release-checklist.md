---
name: "Release Checklist Template"
about: "Checklist to create and publish a Zakura release"
title: "Release Zakura (version)"
labels: "A-release, C-exclude-from-changelog, P-Critical :ambulance:"
assignees: ""
---

# Prepare for the Release

# Checkpoints

For performance and security, we want to update the Zakura checkpoints in every release.

- [ ] You can copy the latest checkpoints from CI by following [the zakura-checkpoints README](https://github.com/zakura-core/zakura/blob/main/zakura-utils/README.md#zakura-checkpoints).

# Missed Dependency Updates

Sometimes `dependabot` misses some dependency updates, or we accidentally turned them off.

This step can be skipped if there is a large pending dependency upgrade. (For example, shared ECC crates.)

Here's how we make sure we got everything:

- [ ] Run `cargo update` on the latest `main` branch, and keep the output
- [ ] Until we bump the workspace MSRV to 1.88 or higher, `home` must be downgraded manually: `cargo update home@0.5.12 --precise 0.5.11`
- [ ] If needed, [add duplicate dependency exceptions to deny.toml](https://github.com/zakura-core/zakura/blob/main/book/src/dev/continuous-integration.md#fixing-duplicate-dependencies-in-check-denytoml-bans)
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
and **skip** the draft-changelog steps below; they apply to releases after
`v1.0.0`.

**Important**: Any merge into `main` deletes any edits to the draft changelog.
Once you are ready to tag a release, copy the draft changelog into `CHANGELOG.md`.

We use [the Release Drafter workflow](https://github.com/marketplace/actions/release-drafter) to automatically create a [draft changelog](https://github.com/zakura-core/zakura/releases). We follow the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.

To create the final change log:

- [ ] Copy the [**latest** draft
      changelog](https://github.com/zakura-core/zakura/releases) into
      `CHANGELOG.md` (there can be multiple draft releases)
- [ ] Delete any trivial changes
  - [ ] Put the list of deleted changelog entries in a PR comment to make reviewing easier
- [ ] Combine duplicate changes
- [ ] Edit change descriptions so they will make sense to Zakura users
- [ ] Check the category for each change
  - Prefer the "Fix" category if you're not sure

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

- [ ] Push the updated changelog and README into a new branch
      for example: `bump-v1.0.0` - this needs to be different to the tag name
- [ ] Create a release PR by adding `&template=release-checklist.md` to the comparing url ([Example](https://github.com/zakura-core/zakura/compare/bump-v1.0.0?expand=1&template=release-checklist.md)).
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
      `last_config_is_stored` — the default cache path also appears in
      fields other than `cache_dir` (for example `cookie_dir`), so
      per-field rewrites produce a snapshot the test rejects.

- [ ] On the release commit, run the pre-release checks for the tag you are
      about to create, using the previous release tag as the base:
      `make pre-release RELEASE_TAG=v<version> BASE_TAG=v<previous-release-tag>`
      For example: `make pre-release RELEASE_TAG=v1.0.0 BASE_TAG=v1.0.0-rc5`

## Update Crate Versions and Crate Change Logs

If you're publishing crates for the first time, [log in to crates.io](https://github.com/zakura-core/zakura/dev/crate-owners.html#logging-in-to-cratesio),
and make sure you're a member of owners group.

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
  - [ ] Update the crate `CHANGELOG.md` listing the API changes or other
        relevant information for a crate consumer, per
        [`CHANGELOG_GUIDELINES.md`](https://github.com/zakura-core/zakura/blob/main/CHANGELOG_GUIDELINES.md)
        (crate changelogs move on the crate's own release cadence; for `v1.0.0`
        they are a single "Initial release" entry). Use `public-api` to list all
        API changes: `cargo public-api diff latest -p <crate> -sss`. You can use
        e.g. copilot to turn it into a human-readable list, e.g. (write the output
        to `api.txt` beforehand):
        <!-- markdownlint-disable MD038 -->
        ```
        copilot -p "Transform @api.txt which is a API diff into a human-readable description of the API changes. Be terse. Write output api-readable.txt. Use backtick quotes for identifiers. Use '### Breaking Changes' header for changes and removals, and '### Added' for additions. Make each item start with a verb e.g, Added, Changed" --allow-tool write
        ```
        <!-- markdownlint-enable MD038 -->
        It might also make sense to copy entries from the `zakurad` changelog.
  - [ ] Update crate versions:

```sh
cargo release version --verbose --execute --allow-branch '*' -p <crate> patch # [ major | minor ]
# zakura only
cargo release replace --verbose --execute --allow-branch '*' -p zakura
```

- [ ] Commit and push the above version changes to the release branch.

## Update End of Support

The end of support height is calculated from the current blockchain height:

- [ ] Find where the Zcash blockchain tip is now by using a [Zcash Block Explorer](https://mainnet.zcashexplorer.app/) or other tool.
- [ ] Replace `ESTIMATED_RELEASE_HEIGHT` in [`end_of_support.rs`](https://github.com/zakura-core/zakura/blob/main/zakurad/src/components/sync/end_of_support.rs) with the height you estimate the release will be tagged.

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
- [ ] Review and merge the installer metadata update PR opened by the release
      workflow.

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
- [ ] Confirm `release-binaries.yml` published `zakurad-<tag>-linux-x86_64.tar.gz`, `zakurad-<tag>-linux-aarch64.tar.gz`, `zakurad-manifest-<tag>.json`, `install-zakura.sh`, and `SHA256SUMS.txt` to the GitHub release.
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
