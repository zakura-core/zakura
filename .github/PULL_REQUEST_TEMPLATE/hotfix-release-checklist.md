---
name: "Hotfix Release Checklist Template"
about: "Checklist to create and publish a hotfix Zakura release"
title: "Release Zakura (version)"
labels: "A-release, C-exclude-from-changelog, P-Critical :ambulance:"
assignees: ""
---

A hotfix release should only be created when a bug or critical issue is
discovered in an existing release, and waiting for the next scheduled release
is impractical or unacceptable. It ships the fix on top of the release
operators are already running, instead of everything unreleased on `main`.

For an **embargoed security fix**, follow
[`docs/security-hotfix-release.md`](https://github.com/zakura-core/zakura/blob/main/docs/security-hotfix-release.md):
all preparation below happens in the private staging repo, this PR is opened
at forward-merge time, and the extra steps there (advisory, dress rehearsal,
canary soak, announcement) apply on top of this checklist.

## Create the Hotfix Branch

- [ ] Cut `hotfix/v<version>` from the tag of the release being fixed (not
      from `main`). The branch **must** be named for the exact tag it will
      release — the `Create release` workflow refuses to tag `v<version>`
      from any other branch. The `hotfix/v*` ruleset blocks deletion and
      force-pushes.
- [ ] Embargoed main-base hotfix: the T-0 public PR branch is **also** named
      `hotfix/v<version>` (based on `main`, pushed at T-0). Whatever the
      base, the hotfix process never pushes `release/v*` or `bump-v*` —
      those names belong to the regular release process, and disjoint
      namespaces are what prevent an embargo-blind collision with it (see
      the process doc's branch namespace rule).
- [ ] Make the required changes, minimal and with tests.
- [ ] For a public (non-embargoed) hotfix: open this PR from the branch with
      `&template=hotfix-release-checklist.md` in the compare URL, and add the
      `do-not-merge` label — the release is dispatched from the branch itself,
      and the PR is merged (as a merge commit) only after the release.
- [ ] Add the `A-release` and `C-exclude-from-changelog` labels **when
      opening the PR**: the changelog fragment check only accepts a
      fragment-consuming release branch in release-PR mode, and relabeling
      after the fact leaves a confusing trail of cancelled check runs.
      `A-release` also makes `check-no-git-dependencies` run; ensure it
      passes.

## Update Versions and Prepare the Release

Hotfixes are `patch` releases of the `zakura` package, plus semver bumps for
each changed crate — follow the "Update Versions" sections of the
[regular checklist](https://github.com/zakura-core/zakura/blob/main/.github/PULL_REQUEST_TEMPLATE/release-checklist.md)
for the commands, using the branch checkout (release.toml allows
`hotfix/v*`).

- [ ] Bump the `zakura` package version and changed-crate versions.
- [ ] De-rc'ing an rc line to its stable version? Re-run `cargo
      semver-checks` against the published **stable** baselines — post-rc
      changes can raise the required bump level (v1.0.3's planned patch
      de-rcs became major bumps) — and normalize internal dependency
      requirements: `dependent-version = "fix"` leaves stale `^X.Y.Z-rcN`
      requirements in place when the stable version still matches them.
- [ ] Generate and commit the stored config snapshot
      (`zakurad/tests/common/configs/v<version>.toml`) — `last_config_is_stored`
      fails without it.
- [ ] Assemble the changelog section for `v<version>`:
  - public hotfix with fragments on the branch: run
    `make prepare-release-changelog RELEASE_TAG=v<version>` and commit the
    result;
  - embargoed hotfix (no public PR yet): write the release section by hand,
    exactly as assembly would produce it — for a stable tag that includes
    absorbing any `v<version>-rc*` sections — and verify with
    `./scripts/changelog.py release v<version> --check`.
- [ ] Do **not** bump `ESTIMATED_RELEASE_HEIGHT`: a hotfix inherits the base
      release's end-of-support schedule (see the process doc for why). Check
      the remaining runway and state it in the release notes if it is short.
- [ ] Run `make pre-release RELEASE_TAG=v<version> BASE_TAG=v<base-version>`
      and get it passing on the final commit.
- [ ] Verify crate packaging: `./scripts/check-crate-packaging.sh --verify`.

## Publish the Release

- [ ] Confirm every release-capable maintainer has been told to hold
      releases (for an embargoed hotfix, the day-before heads-up in the
      process doc), and check for an in-flight regular release before
      pushing anything: open PRs labeled `A-release`, `release/v*` or
      `bump-v*` branches, and running `Create release` dispatches. Two
      trains must never claim the same version — tags are immutable and
      never reused.
- [ ] Push the final branch to `zakura-core/zakura`.
- [ ] Dispatch the
      [Create release workflow](https://github.com/zakura-core/zakura/actions/workflows/create-release.yml)
      **from the `hotfix/v<version>` branch** with the exact tag. The
      workflow validates, builds and verifies the assets, then waits at the
      `release` environment.
- [ ] Approve the `release` environment deployment (right commit? right
      tag?). The workflow publishes a complete pre-release and creates the
      protected tag; the tag push starts Docker publishing.
- [ ] Update the release description from the changelog section.
- [ ] Sign the release: `make sign-release TAG=v<version>`.
- [ ] Stable hotfix only: promote the release — disable 'pre-release' **and**
      "Set as the latest release". Release candidates are never promoted.

## Publish Crates

- [ ] From a fresh checkout of the new tag: `cargo login`, then publish the
      changed crates in dependency order per the regular checklist
      (`cargo release publish --verbose --execute -p <crate>`), editing the
      list to only the crates that changed.
- [ ] Check `cargo install --locked --force --version <version> zakura` works
      and put the output in a comment on this PR.

## Publish Docker Images

- [ ] Wait for the tag-push
      [release-binaries.yml run](https://github.com/zakura-core/zakura/actions/workflows/release-binaries.yml?query=event%3Apush)
      to publish images, and confirm the new tags in the
      [Docker Hub zakura space](https://hub.docker.com/r/zakuracore/zakura/tags).
      Hyphenated tags (release candidates) never move `latest`.

## Merge the Hotfix into Main

This section applies to hotfix-branch releases only: a main-base hotfix
entered `main` at T-0 (the tag points at its squash commit) and has nothing
to forward-merge.

- [ ] Forward-merge `hotfix/v<version>` into `main` **immediately** via this
      PR. Solve conflicts in the branch without force-pushing — the released
      commit must become an ancestor of `main`.
- [ ] Merge with a **merge commit** (do not squash), so the tagged commit is
      preserved in `main`'s history. **The merge button defaults to squash —
      change the dropdown before clicking.** (A squash copies the content
      but orphans the tagged commit from `main`'s history, breaking later
      base-tag ancestry checks — this happened to both rc-drill
      forward-merges, #350 and #354.) The `main` ruleset only offers squash
      and rebase, so this depends on the merge-method standing precondition
      in the process doc: if `merge` is not permanently allowed there,
      temporarily add it to the ruleset's allowed merge methods, merge, then
      revert the edit.
- [ ] Delete the hotfix branch after the merge; the tag is permanent.

## Release Failures

If the pre-tag build or validation fails, no tag exists: fix the branch and
re-dispatch `Create release` with the same version. If a failure is found
after the tag exists, start a new `patch` hotfix from the top of this
document — tags are immutable and are never reused.
