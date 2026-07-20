---
name: "🚀 Zakura Release"
about: "Zakura team use only"
title: "Publish next Zakura release: (version)"
labels: "A-release, C-exclude-from-changelog, P-Medium :zap:"
assignees: ""
---

# Prepare for the Release

These release steps can be done a week before the release, in separate PRs.
They can be skipped for urgent releases.

## Checkpoints

For performance and security, we want to update the Zakura checkpoints in every release.

- [ ] You can copy the latest checkpoints from CI by following [the zakura-checkpoints README](https://github.com/zakura-core/zakura/blob/main/zakura-utils/README.md#zakura-checkpoints).

## Missed Dependency Updates

Sometimes `dependabot` misses some dependency updates, or we accidentally turned them off.

This step can be skipped if there is a large pending dependency upgrade. (For example, shared ECC crates.)

Here's how we make sure we got everything:

- [ ] Run `cargo update` on the latest `main` branch, and keep the output
- [ ] Until we bump the workspace MSRV to 1.88 or higher, `home` must be downgraded manually: `cargo update home@0.5.12 --precise 0.5.11`
- [ ] If needed, [add duplicate dependency exceptions to deny.toml](https://github.com/zakura-core/zakura/blob/main/book/src/dev/continuous-integration.md#fixing-duplicate-dependencies-in-check-denytoml-bans)
- [ ] If needed, remove resolved duplicate dependencies from `deny.toml`
- [ ] Open a separate PR with the changes
- [ ] Add the output of `cargo update` to that PR as a comment

# Prepare and Publish the Release

Follow the steps in the [release checklist](https://github.com/zakura-core/zakura/blob/main/.github/PULL_REQUEST_TEMPLATE/release-checklist.md) to prepare the release:

Release PR:

- [ ] Review and assemble root changelog fragments
- [ ] Update README
- [ ] Update Zakura Versions
- [ ] Update Crate Versions
- [ ] Update End of Support Height

Publish Release:

- [ ] Create & Test GitHub Pre-Release
- [ ] Publish GitHub Release
- [ ] Publish Rust Crates
- [ ] Publish Docker Images
