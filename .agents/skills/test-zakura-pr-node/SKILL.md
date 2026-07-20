---
name: test-zakura-pr-node
description: Dispatch and validate Zakura real node tests with the repository's Golden DigitalOcean image and state fixtures. Use for fresh server or Droplet tests, real node tests of a Zakura PR or ref, managed zcashd wrapper or zcashd-compat tests, reproducing node or sync issues, and requests to use a Golden image or Golden assets instead of downloading chain state.
---

# Test a Zakura PR Node

Use `.github/workflows/zakura-pr-node.yml` for DigitalOcean tests. Let the
workflow create the Droplet and clone the maintained Golden assets. Never
provision an ad hoc test Droplet or manually download or unpack chain state.
Route provisioning and asset refreshes through `gh workflow run`; do not call
`doctl` or the DigitalOcean API directly.

Read `docs/pr-node-do-setup.md` before changing workflow inputs or refreshing
assets.

## Choose a profile

- Use `zakura` for normal Zakura sync, validation, performance, or regression
  tests. Choose `tip`, `sandblast`, or `genesis` according to the test.
- Use `zcashd-compat` when testing the managed zcashd wrapper, sidecar lifecycle,
  block serving, or the pruned state compatibility path. This profile requires
  `network=mainnet` and `snapshot_mode=tip`.
- Use `genesis` only when the user explicitly wants a checkpoint sync from
  genesis. Do not use it as a fallback for a missing Golden fixture.

## Dispatch

Resolve exactly one target as a PR number or a branch, tag, or SHA. Dispatch the
workflow and record the resulting run ID.

```bash
# Ordinary Zakura test
gh workflow run zakura-pr-node.yml \
  -f pr_number=123 \
  -f test_profile=zakura

# Managed zcashd compatibility test
gh workflow run zakura-pr-node.yml \
  -f pr_number=123 \
  -f test_profile=zcashd-compat \
  -f network=mainnet \
  -f snapshot_mode=tip
```

Set `teardown_after_run=true` when no SSH inspection is needed. Otherwise leave
the Droplet and its cloned volumes for the reaper. Do not remove or alter the
workflow's resource tags or bypass its cleanup path.

## Validate the run

Watch the dispatched run through completion:

```bash
gh run list --workflow=zakura-pr-node.yml --limit=5
gh run watch RUN_ID --exit-status
gh run view RUN_ID --log-failed
```

Check the job summary and artifact for the requested target SHA, selected
profile, selected Golden image and state snapshots, node health, and increasing
heights. For `zcashd-compat`, also require a successful cold wrapper
installation, exactly one direct zcashd child, both Zakura and zcashd heights
advancing, a clean managed child shutdown, and a warm restart with the same
cached binary hash and modification time. Require continued height progress and
no pruned block sidecar failure. Report the Droplet lifetime and whether
immediate teardown or the scheduled reaper will remove it.

## Handle unavailable assets

- If an image or state snapshot is missing or stale, run
  `gh workflow run zakura-pr-node-bake.yml`, wait for it to finish, and retry.
- If the state database format is incompatible, rebake after the needed format
  lands on `main`. If the target PR needs an unsupported newer format, stop and
  report it. Do not rename or manually patch an incompatible fixture.
- If the first zcashd fixture does not exist, pass the bake workflow's documented
  seed inputs to `gh workflow run` with an existing cleanly stopped zcashd
  datadir. Do not create an untracked fixture or fetch a chain archive on a run
  Droplet.
- If the bake cannot produce a valid asset, stop and report the failing guard or
  missing prerequisite. Do not silently fall back to a slow manual setup.
