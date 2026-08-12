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

- [ ] You can copy the latest checkpoints from CI by following [the zakura-checkpoints README](https://github.com/zakura-core/zakura/blob/main/crates/zakura-utils/README.md#zakura-checkpoints).

## Curated Dependency Updates

Routine Cargo version updates are intentionally disabled. Do not run a blanket
`cargo update` during release preparation: it can introduce a large cargo-vet
evidence backlog without a release-specific justification.

- [ ] Review open Dependabot security alerts and focused dependency updates
      already planned for this release.
- [ ] If an update is needed, open a separate focused PR from the latest `main`.
- [ ] Restrict the update to the required crate or dependency family, and include
      the update command and output in the PR.
- [ ] Confirm cargo-vet evidence covers the update and run targeted runtime tests.
- [ ] Update duplicate dependency exceptions in `deny.toml` only as required by
      the focused resolution change.

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
