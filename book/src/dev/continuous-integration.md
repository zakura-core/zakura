# Zakura Continuous Integration

## Overview

Zakura has extensive continuous integration tests for node syncing and `lightwalletd` integration.

## Workflow Reference

For a comprehensive overview of all CI/CD workflows including architecture diagrams,
see the [CI/CD Architecture documentation](https://github.com/zakura-core/zakura/blob/main/.github/workflows/README.md).

## Integration Tests

On relevant PR changes, Zakura runs [its end-to-end test workflow](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-e2e.yml), including:

- a Zakura regtest end-to-end gate
- a multi-node testkit integration test
- block-sync fuzz scenarios after merges to `main`
- longer multi-node modes on a schedule or on demand

Full-sync coverage against the real network comes from the
[continuous genesis sync fleet](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-continuous-sync.yml),
which permanently re-syncs a small set of nodes from genesis and audits their progress on a schedule.
A PR can also be tested on a real node before merge with the
[PR node workflow](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-pr-node.yml),
which boots an ephemeral droplet from a pre-baked chain-state snapshot; see
[Continuous Delivery](continuous-delivery.md#ephemeral-pr-nodes).

Some of our builds and tests are repeated on the `main` branch, due to:

- GitHub's cache sharing rules, or
- generating base coverage for PR coverage reports.

Zakura also does [a smaller set of tests](https://github.com/zakura-core/zakura/blob/main/.github/workflows/tests-unit.yml) on tier 2 platforms using GitHub actions runners.

## Automated Merges

PRs land through GitHub's native merge queue.
To merge, a PR has to pass all required `main` branch protection checks, and be approved by a Zakura developer.

The merge queue revalidates the merged result against the latest `main`: `lint.yml`, `tests-unit.yml`,
and `zakura-e2e.yml` run on every queued entry via their `merge_group` triggers.
`merge_group` ignores path filters, so these checks run unconditionally in the queue.

Merging with failing CI is usually disabled by our branch protection rules.
See the `Admin: Manually Merging PRs` section below for manual merge instructions.

We use workflow conditions to skip some checks on PRs or the `main` branch.
For example, some workflow changes skip Rust code checks. When a workflow can skip a check, we need to create [a patch workflow](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/collaborating-on-repositories-with-code-quality-features/troubleshooting-required-status-checks#handling-skipped-but-required-checks)
with an empty job with the same name. This is a [known Actions issue](https://github.com/orgs/community/discussions/13690#discussioncomment-6653382).
This lets the branch protection rules pass when the job is skipped. In Zakura, we name these workflows with the extension `.patch.yml`.

### Branch Protection Rules

Branch protection rules should be added for every failure that should stop a PR merging, break a release, or cause problems for Zakura users.
We also add branch protection rules for developer or devops features that we need to keep working, like coverage.

But the following jobs don't need branch protection rules:

- Testnet jobs: testnet is unreliable.
- Optional linting jobs: some lint jobs are required, but some jobs like spelling and actions are optional.
- Jobs that rarely run: for example, scheduled-only or manually dispatched jobs.
- Setup jobs that will fail another later job which always runs.

When a new job is added in a PR, use the `#devops` Slack channel to ask a GitHub admin to add a branch protection rule after it merges.
Adding a new Zakura crate automatically adds a new job to build that crate by itself in [test-crates.yml](https://github.com/zakura-core/zakura/blob/main/.github/workflows/test-crates.yml),
so new crate PRs also need to add a branch protection rule.

#### Admin: Changing Branch Protection Rules

Zakura repository admins and organisation owners
can add or delete branch protection rules in the Zakura repository.

To change branch protection rules:

Any developer:

0. Run a PR containing the new rule, so its name is available to autocomplete.
1. If the job doesn't run on all PRs, add a patch job with the name of the job.
   If the job calls a reusable workflow, the name is `Caller job / Reusable step`.
   (The name of the job inside the reusable workflow is ignored.)

Admin:

1. Go to the [branch protection rule settings](https://github.com/zakura-core/zakura/settings/branches)
2. Click on `Edit` for the `main` branch
3. Scroll down to the `Require status checks to pass before merging` section.
   (This section must always be enabled. If it is disabled, all the rules get deleted.)

To add jobs:

1. Start typing the name of the job or step in the search box
2. Select the name of the job or step to add it

To remove jobs:

1. Go to `Status checks that are required.`
2. Find the job name, and click the cross on the right to remove it

And finally:

1. Click `Save changes`, using your security key if needed

If you accidentally delete a lot of rules, and you can't remember what they were, ask an
organisation owner to send you a copy of the rules from the [audit log](https://github.com/organizations/zakura-core/settings/audit-log).

Organisation owners can also monitor rule changes and other security settings using this log.

#### Admin: Manually Merging PRs

Admins can allow merges with failing CI, to fix CI when multiple issues are causing failures.

Admin:

1. Follow steps 2 and 3 above to open the `main` branch protection rule settings
2. Scroll down to `Do not allow bypassing the above settings`
3. Uncheck it
4. Click `Save changes`
5. Do the manual merge, and put an explanation on the PR
6. Re-open the branch protection rule settings, and re-enable `Do not allow bypassing the above settings`

### Pull Requests from Forked Repositories

GitHub doesn't give PRs from forked repositories access to our repository secrets and variables, even after we approve their CI.
This means that workflows needing secrets (for example, the fleet-deploy and PR-node workflows) can't run on these PRs.

When an external PR needs the full CI suite, we can merge it by:

1. Reviewing the code to make sure it won't give our secret keys to anyone
2. Pushing a copy of the branch to the Zakura repository
3. Opening a PR using that branch
4. Closing the original PR with a note that it will be merged
5. Asking another Zakura developer to approve the new PR

## Troubleshooting

Some CI jobs are stateful, or depend on external state:

- the Zakura e2e suite drives a host-networked docker-compose stack of multiple nodes
- PR-node runs depend on the weekly-baked droplet image and chain-state volume snapshots
- sync tests depend on the current height and user-submitted transactions on the blockchain, which change every minute

### Finding Errors

0. Check if the same failure is happening on the `main` branch or multiple PRs.
   If it is, open a ticket and tell the Zakura team lead.

1. Look for the earliest job that failed, and find the earliest failure.

   Later jobs often fail with confusing consequence errors (a missing artifact, an invalid
   template, a skipped dependency) when the real problem is a compile or test failure a few
   steps or jobs earlier.

2. The earliest failure can also be in another job:
   - check the whole workflow run (use the "Summary" button on the top left of the job details, and zoom in)

3. If that doesn't help, try looking for the latest failure. In Rust tests, the "failure:" notice contains the failed test names.

### Fixing CI Sync Timeouts

CI sync jobs near the tip will take different amounts of time as:

- the blockchain grows, and
- Zakura's checkpoints are updated.

To fix a CI sync timeout, follow these steps until the timeouts are fixed:

1. Check for recent PRs that could have caused a performance decrease
2. [Update Zakura's checkpoints](https://github.com/zakura-core/zakura/blob/main/zakura-utils/README.md#zakura-checkpoints)
3. If a Rust test fails with "command did not log any matches for the given regex, within the ... timeout":

   a. If it's the full sync test, use [Zebra PR #5129](https://github.com/ZcashFoundation/zebra/pull/5129/files) as an example of increasing the timeout.

   b. If it's an update sync test, [increase the update sync timeouts](https://github.com/zakura-core/zakura/commit/9fb87425b76ba3747985ea2f22043ff0276a03bd#diff-92f93c26e696014d82c3dc1dbf385c669aa61aa292f44848f52167ab747cb6f6R51)

### Fixing Duplicate Dependencies in `Check deny.toml bans`

Zakura's CI checks for duplicate crate dependencies: multiple dependencies on different versions of the same crate.
If a developer or dependabot adds a duplicate dependency, the `Check deny.toml bans` CI job will fail.

You can view Zakura's entire dependency tree using `cargo tree`. It can also show the active features on each dependency.

To fix duplicate dependencies, follow these steps until the duplicate dependencies are fixed:

1. Check for updates to the crates mentioned in the `Check deny.toml bans` logs, and try doing them in the same PR.
   For an example, see [Zebra PR #5009](https://github.com/ZcashFoundation/zebra/pull/5009#issuecomment-1232488943).

   a. Check for open dependabot PRs, and

   b. Manually check for updates to those crates on <https://crates.io>.

2. If there are still duplicate dependencies, try removing those dependencies by disabling crate features:

   a. Check for features that Zakura activates in its `Cargo.toml` files, and try turning them off, then

   b. Try adding `default-features = false` to Zakura's dependencies (see [Zebra PR #4082](https://github.com/ZcashFoundation/zebra/pull/4082/files)).

3. If there are still duplicate dependencies, add or update `skip-tree` in [`deny.toml`](https://github.com/zakura-core/zakura/blob/main/deny.toml):

   a. Prefer exceptions for dependencies that are closer to Zakura in the dependency tree (sometimes this resolves other duplicates as well),

   b. Add or update exceptions for the earlier version of duplicate dependencies, not the later version, and

   c. Add a comment about why the dependency exception is needed: what was the direct Zakura dependency that caused it?

   d. For an example, see [Zebra PR #4890](https://github.com/ZcashFoundation/zebra/pull/4890/files).

4. Repeat step 3 until the dependency warnings are fixed. Adding a single `skip-tree` exception can resolve multiple warnings.

#### Fixing "unmatched skip root" warnings in `Check deny.toml bans`

1. Run `cargo deny --all-features check bans`, or look at the output of the latest "Check deny.toml bans --all-features" job on the `main` branch

2. If there are any "skip tree root was not found in the dependency graph" warnings, delete those versions from `deny.toml`

### Fixing Disk Full Errors

If a PR test node's state volume is full, increase the volume size in the
[PR-node bake workflow](https://github.com/zakura-core/zakura/blob/main/.github/workflows/zakura-pr-node-bake.yml)
and create a new snapshot.

If the GitHub Actions disks are full, follow these steps until the errors are fixed:

0. Check if error is also happening on the `main` branch. If it is, skip the next step.
1. Update your branch to the latest `main` branch, this builds with all the latest dependencies in the `main` branch cache.
2. Clear the GitHub Actions code cache for the failing branch. Code caches are named after the compiler version.
3. Clear the GitHub Actions code caches for all the branches and the `main` branch.

These errors often happen after a new compiler version is released, because the caches can end up with files from both compiler versions.

You can find a list of caches using:

```sh
gh api -H "Accept: application/vnd.github+json" repos/zakura-core/zakura/actions/caches
```

And delete a cache by `id` using:

```sh
gh api --method DELETE -H "Accept: application/vnd.github+json" /repos/zakura-core/zakura/actions/caches/<id>
```

These commands are from the [GitHub Actions Cache API reference](https://docs.github.com/en/rest/actions/cache).

### Retrying After Temporary Errors

Some errors happen due to network connection issues, high load, or other rare situations.

If it looks like a failure might be temporary, try re-running all the jobs on the PR using one of these methods:

1. `@dependabot recreate` (for dependabot PRs only)
2. click on the failed job, and select "re-run all jobs". If the workflow hasn't finished, you might need to cancel it, and wait for it to finish.

Here are some of the rare and temporary errors that should be retried:

- Docker: "buildx failed with ... cannot reuse body, request must be retried"
- Failure in `local_listener_fixed_port_localhost_addr_v4` Rust test; the inherited [Zebra issue #4999](https://github.com/ZcashFoundation/zebra/issues/4999) has additional context.
- any network connection or download failures

We track some rare errors using tickets, so we know if they are becoming more common and we need to fix them.
