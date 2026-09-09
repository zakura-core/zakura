# Manual genesis comparisons

The `candidate-start`, `candidate-collect`, and `candidate-cleanup` actions in
`zakura-continuous-sync.yml` operate on an isolated temporary pair. They skip the
fleet job and use a separate concurrency group, so the automatic fleet keeps
running. Select a candidate branch containing this harness when dispatching.

## Candidates

| Branch | Change being measured |
| --- | --- |
| `adam/genesis-sync-prefix` | Admit a checkpoint-sized header prefix when the body pipeline is running low, so it can continue while more headers arrive. |
| `adam/genesis-sync-prefix-context` | The prefix change plus reuse of already validated checkpoint headers in retained state. |

Both start from main commit `20fab8ce679ff2b32ad05f0c5a0f585d83b276e1` and
share this harness. Request and memory budgets stay unchanged. The default
baseline resolves the latest `main` at launch; set `baseline_ref` to the common
base SHA above to isolate just the candidate changes if main has since advanced.

The prefix change reduced elapsed time by 40.9% in the controlled fixture with
deliberately delayed header pages. The saved time was accounted for by reduced
header waiting. With prompt pages, its mean time was 0.35% slower, within the
overlapping run ranges. These are narrow fixture results, not predictions for
genesis sync. The additional state change has correctness coverage but no
controlled end-to-end payoff result yet.

## Launch

One launch creates a baseline and candidate in the same networking mode. Run
dual and Zakura comparisons separately. The helper caps outstanding comparison
and profiling-fixture droplets at four; each launch needs two free slots.

```bash
gh workflow run zakura-continuous-sync.yml --repo zakura-core/zakura \
  --ref adam/genesis-sync-prefix \
  -f action=candidate-start -f candidate_mode=dual
```

Use `candidate_mode=zakura` for Zakura-only. Select
`--ref adam/genesis-sync-prefix-context` for the combined candidate. Optional
`-f candidate_ref=<branch-or-SHA>` overrides the candidate node source while
keeping the selected workflow branch as the harness. Both source refs and the
harness revision are pinned to full commit SHAs in the launch artifact.

Find the launch run ID in the Actions URL or the `launch_run_id` field in its
artifact. The launch job finishes after provisioning; its green status means
the hosts were launched, not that either sync completed.

Each host gets a fresh Ubuntu 24.04 image, `g-8vcpu-32gb` hardware in `sfo2`,
and its own blank 200 GiB volume. The pair uses the same concrete image ID and
Rust 1.98.1. CPU, kernel, package versions, source SHA, binary digest and rendered
configuration are retained with results. Build directories and genesis state
are fresh. Automatic package maintenance is stopped before the measurement.

The hosts run the existing controller once, using its normal release build,
pruned Mainnet state, VCT enabled, checkpoint sync, bootstrap peers, logging and
readiness rules. Six ready samples 30 seconds apart confirm completion.

## Collect and compare

```bash
gh workflow run zakura-continuous-sync.yml --repo zakura-core/zakura \
  --ref adam/genesis-sync-prefix \
  -f action=candidate-collect -f candidate_run_id=<launch-run-id>
```

Use the same harness branch as the launch. Collection is safe while syncing;
it reports the current phase without stopping the hosts. A completed or failed
run also uploads its logs, traces, configuration and bootstrap/service journal.
Download the collection workflow's `genesis-candidate-<collection-run-id>`
artifact before cleanup. A missing host, failed bootstrap, stalled sync or
unconfirmed height has no BPS result.

The Actions summary reports each completed run's confirmed height, integer
duration and blocks per second. It uses the digest's formula:
`(end_height + 1) / duration_seconds`. Build and state preparation are excluded;
startup, stable-readiness confirmation, stopping and log archival are included.
The `+1` counts genesis. Retain the unrounded numbers for comparisons.

For example, if a candidate has higher BPS than today's digest but its paired
baseline improves by the same amount, that pair does not establish a benefit
from the change. Compare the pair first, inspect height/time traces and host
metadata for mismatches, then repeat. Public peers, chain growth and host or
storage performance can still vary; a single pair is not causal proof. The
existing digest remains a useful observational reference.

## Cleanup and limits

The node stops after its one cycle or on failure. Sync has a 20-hour limit;
the host job has a 22-hour total limit including setup/build. Stopped droplets
and volumes still cost money until deleted. Collect promptly: the existing
hourly reaper deletes tagged droplets older than 24 hours and detached
`zakura-pr-*` volumes older than two hours, including their local evidence.

After downloading results, delete this pair:

```bash
gh workflow run zakura-continuous-sync.yml --repo zakura-core/zakura \
  --ref adam/genesis-sync-prefix \
  -f action=candidate-cleanup -f candidate_run_id=<launch-run-id>
```

Cleanup is also how to abort a pair. If provisioning fails, use its launch run
ID to remove partial resources. Cleanup verifies exact experiment names, tags
and volume attachments before deletion; it accepts no fleet node or arbitrary
host address. No Slack or GitHub comments are sent by these actions.
