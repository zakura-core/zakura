# Zakura Continuous Genesis Sync Fleet

This directory codifies the three permanent mainnet sync nodes that repeatedly
test a fresh genesis-to-tip sync from the latest `origin/main` build:

| Node | Address | Mode |
| --- | --- | --- |
| `temp-zakura-sync-test-1` | `root@138.68.43.212` | dual-stack |
| `temp-zakura-sync-test-2` | `root@138.197.218.91` | Zakura/v2-only |
| `temp-zakura-sync-test-5` | `root@142.93.27.189` | Zebra/legacy-only |

Each node runs a local systemd controller. GitHub Actions installs and audits the
controller, but it does not hold an SSH session open during the long sync.

The dual-stack and Zakura/v2-only nodes exercise the experimental Zakura P2P
v2 stack.

For temporary baseline/candidate pairs that leave this fleet running, see
[Manual genesis comparisons](CANDIDATES.md).

## Lifecycle

`zakura-continuous-sync.service` runs
`/usr/local/sbin/zakura-continuous-sync.py` on each host:

1. Stop the node, prune old artifacts, check disk space, then fetch `origin/main` in `/root/zakura` and pin the full commit SHA.
2. Build `zakurad` from a detached worktree and cache the binary by SHA.
3. Atomically install the binary at `/usr/local/bin/zakurad`.
4. Stop `zakura.service`.
5. Verify `/var/lib/zakura/.continuous-sync-wipe-ok` exists.
6. Delete only the configured disposable state entries:
   `/var/lib/zakura/state` and `/var/lib/zakura/non_finalized_state`.
7. Preserve `/var/lib/zakura/network` and controller state.
8. Render `/etc/zakura/zebrad.toml` with the node's assigned `p2p_stack`.
   Detailed JSONL traces are enabled for every sync.
9. Start `zakura.service` with `Restart=no`.
10. Poll metrics and `/ready` until the node is stably near tip.
11. Stop the node, record the completion for the daily audit digest, and start
    the next cycle after a short cooldown.

The same commit may be tested repeatedly. That is intentional: the fleet is a
continuous sync canary, not a once-per-SHA CI job.

## Failure Semantics

Build, install, cleanup, startup, sync, stall, timeout, metrics, and readiness
failures halt the affected node. Disk pressure follows the automatic recovery
policy below. Other failures behave as follows:

- `zakura-continuous-sync.service` exits non-zero.
- `/var/lib/zakura-continuous-sync/state.json` records `failed = true`.
- the current run's `run.json` records the phase and failure reason.
- a Slack alert is posted with the node mode, SHA, height, SSH target, log path,
  trace path, and monitor log path.

For failures other than disk pressure, resume is an explicit operator action:

```bash
python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-2 resume
```

or run the **Zakura continuous genesis sync fleet** workflow with
`action=resume` and the failed node name.

## Files and Services

Tracked repository files:

- `nodes.toml` is the source-of-truth fleet inventory and policy.
- Each node's `public_ip` controls its advertised legacy and Zakura addresses.
  The legacy address uses port 8233. Zakura combines the same public IP with
  its port 8234 listener when it creates the signed discovery record.
- `health_min_connected_peers` controls the health endpoint's legacy peer
  threshold. The v2-only node sets this threshold to zero because the health
  endpoint does not count Zakura connections. Its readiness check still
  requires a recent chain tip within the configured block distance.
- `continuous-sync.py` is the host-local controller.
- `alert-monitor.py` is the cluster Slack alerter.
- `alert-status.py` emits one node's local status as JSON for peer queries.
- `deploy.py` installs, checks status, resumes, and audits nodes.
- `templates/` contains the rendered `zakurad` config template, systemd units,
  logrotate policy, and tmpfiles policy.

Host files:

- controller config: `/etc/zakura-continuous-sync/controller.toml`
- `zakurad` config: `/etc/zakura/zebrad.toml`
- node service: `zakura.service`
- controller service: `zakura-continuous-sync.service`
- alert service and timer: `zakura-monitor.service` / `zakura-monitor.timer`
- controller state: `/var/lib/zakura-continuous-sync/state.json`
- alert state: `/var/lib/zakura-monitor/cluster-state.json`
- run artifacts: `/var/log/zakura/runs/<timestamp>-<sha>/`
- node log: `/var/log/zakura/zebrad.log`
- trace symlink: `/var/log/zakura/traces`
- legacy sync trace: `/var/log/zakura/traces/legacy_sync.jsonl`
- monitor log: `/var/log/zakura/monitor.log`

## Deployment

Manual deploy from a machine with SSH access:

```bash
python3 deploy/continuous-sync/deploy.py deploy
python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-1 deploy
python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-1 deploy --no-start
```

GitHub Actions:

1. Open **Zakura continuous genesis sync fleet**.
2. Choose `action=deploy`.
3. Leave `node` empty for all nodes, or set one node name for a staged rollout.
4. Use `no_start=true` to install files without handing the node to the
   controller.

The workflow uses the repository's `DO_SSH_PRIVATE_KEY` secret and requires its
matching `zebra-ci` public key in each host's `/root/.ssh/authorized_keys`. It
validates the key and every host connection before running an operation, so SSH
configuration failures stop the workflow instead of being reported as node
health failures. Slack audit alerts use the existing `SLACK_WEB_HOOK` secret.
Per-host completion/failure alerts use the root-only
`/etc/zakura-alerts.env` file already used by the temporary monitor.

## Status and Audit

Fetch status:

```bash
python3 deploy/continuous-sync/deploy.py status
python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-5 status
```

The workflow requests `audit` twice per hour, but GitHub scheduling can delay or
skip runs for hours. It alerts when a host is
unreachable, the controller is halted, the node service is inactive while a run
claims to be syncing, metrics are unavailable during sync, disk free space is
below the configured 10 GiB floor, or the node has not completed a run in four
days. A run is capped at 48 hours, so that last check cannot be tripped by a
legitimately long sync. These external audit checks intentionally remain
independent of the per-host minute monitor.

New problems and changed failures still alert immediately when the audit observes
them. A confirmed controller delivery receipt suppresses the first duplicate audit
page only when the run, failure timestamp, reason, and Slack destination match.
Missing, failed, mismatched, or future-dated receipts do not suppress the audit.
A new run with the same failure reason remains a separate incident. Older
controllers without receipts keep their existing audit fallback.

Cached incident delivery is also tied to the Slack destination. After a webhook
change, the next audit alerts the new destination unless a matching controller
receipt confirms delivery there. It does not send recoveries for incidents known
only to the old destination. Cache records without a destination cannot suppress
an alert without a matching receipt.

Unchanged failures remain in the audit's daily reminder. New failures and
recoveries do not wait for that reminder. Routine completions are delivered by
the separate daily sender described below; normal audits never emit them.

### Daily summary

`daily_summary.py` runs on the single sender named in `[summary]` in `nodes.toml`.
`zakura-sync-summary.timer` checks once per minute, with a deadline of **5:00 p.m.
America/Denver**, following daylight saving changes. The first check at or after
the deadline sends the summary. A failed delivery retries on the next check.
For example, a post delayed until 5:10 p.m. does not move tomorrow's 5:00 p.m.
deadline. After downtime, one catch-up post includes all still-unreported runs.

The sender uses the existing monitor's read-only peer SSH access and stores the
last delivered run number and ID for each node in
`/var/lib/zakura-sync-summary/state.json`, independently of the Actions cache and
disposable chain state. Only a confirmed Slack response advances these cursors.
The state file is atomically replaced and flushed to disk, and a file lock prevents
concurrent senders on that host. Missing or corrupt state fails visibly instead
of restarting a 24-hour wait or replaying old completions. The configured Slack
destination must match the saved delivery history.

Each post includes runs completed since that node was last successfully reported.
Runs still in progress wait for the next day. An unavailable node is identified in
the message and retains its cursor, so its unreported runs are included when it
becomes reachable. A reset or inconsistent completion counter is treated as
unavailable until the operator restores the correct history. New nodes require an
explicit cursor; use number zero and an empty run ID only if none of their runs
have ever been reported. Before retiring a node, deliver its pending results.

The summary names each networking mode (dual, Zakura only, or legacy only),
keeps the host ID for troubleshooting, and includes hosts with zero completions.
It shows one row per completed run, with duration and average blocks
per second (BPS), oldest first, plus the currently observed controller phase.
Sync duration excludes the build and state cleanup; it includes startup, readiness
confirmation, shutdown, and log archiving. Failures still alert immediately.

Each cycle starts with empty chain state. The BPS calculation uses the confirmed height
from the final readiness sample plus one for genesis, using the committed-block
height gauge. The block count is retained for calculation but omitted from Slack. Average BPS
divides that count by the full, unrounded duration in
seconds. This is overall sync throughput, not instantaneous verifier speed;
blocks differ in cost and the node may process more blocks during shutdown.
An estimated tip or an earlier progress sample cannot supply the count. Missing
heights and zero or missing durations produce an unavailable rate.

For example, a digest can show these illustrative per-run results:

```text
Dual networking · 1 completed
• 6h 40m · 145 blocks/sec

Zakura networking only · 3 completed
• 7h 10m · 134 blocks/sec
• 7h 00m · 138 blocks/sec
• 7h 20m · 131 blocks/sec

Legacy networking only · 1 completed
• 8h 00m · 120 blocks/sec
```

The Slack summary also retains host IDs and current status for troubleshooting.

Controllers retain the latest 256 completion durations and ending heights in their
state, independently of run-log cleanup. The sender reads these on each delivery
attempt. Counts remain available when older timings are no longer retained.
Missing records, including those beyond retention, are explicitly marked unavailable.
Malformed optional controller history is discarded without failing a successful
sync; completion counters remain authoritative. Per-run logs and artifacts remain
available on each host.

Failure-alert state is still carried between workflow runs in the Actions cache;
cache loss can repeat audit alerts but cannot reset the daily summary. A failed Slack
post does not advance notification state; the next audit retries it. Targeted
audits preserve other nodes' incidents and do not send the fleet digest. The audit
job exits non-zero when an inspected node has a problem or Slack delivery fails.
The workflow also checks that the daily sender's timer is active and its latest
deadline is no more than 15 minutes overdue. Host-local failures appear in
`systemctl status zakura-sync-summary.service` and its journal immediately; the
external check remains subject to GitHub scheduling delays.

### Sender rollout and recovery

Install from the reviewed revision, without starting the sender or restarting any
sync controller:

```bash
python3 deploy/continuous-sync/deploy.py deploy-summary --no-start
```

On the sender, prepare a root-only seed JSON file with `last_posted_at` (UTC Unix
seconds of the last confirmed summary) and `cursors`. Each cursor is keyed by the
configured node name and contains `number` and `run_id` from that node's last
reported completion. Verify them against the Slack post and controller history;
do not use the current counters, which would silently skip pending runs, or a
reset Actions cache, which may include previously reported runs. Initialization
requires every configured node and refuses to overwrite existing state.

After the GitHub audit revision that disables routine summaries is active, and
any older audit has finished, initialize and preview on the sender:

```bash
systemd-run --wait --collect -p EnvironmentFile=/etc/zakura-alerts.env \
  /usr/bin/python3 /opt/zakura-sync-summary/daily_summary.py \
  initialize --from-file /root/sync-summary-seed.json
systemd-run --wait --collect -p EnvironmentFile=/etc/zakura-alerts.env \
  /usr/bin/python3 /opt/zakura-sync-summary/daily_summary.py run --dry-run
```

The preview respects the deadline and never advances state. Enable the timer only
after reviewing the migration and pending message:

```bash
systemctl enable --now zakura-sync-summary.timer
python3 /opt/zakura-sync-summary/daily_summary.py status
journalctl -u zakura-sync-summary.service --since today
```

If today's deadline has passed, enabling sends one catch-up summary on the next
minute. Check delivery and the saved cursors before declaring rollout complete.
Keep a backup of the delivery state. For a host replacement, stop the old sender
first and restore that state on the newly configured owner. Missing state must be
reconstructed from confirmed posts; never silently initialize from the present.
Changing the destination or schedule also requires reviewing the saved history.

Slack incoming webhooks do not make delivery and the local state update one atomic
operation. If Slack accepts a post but its response is lost, or the host crashes
before saving confirmation, a retry can duplicate that post. Known failed
deliveries retain all runs; the sender never claims exactly-once delivery.

For rollback, stop the timer first. `audit --legacy-digest` retains the old reporting
path, but its cache is not updated by the new sender; reconstruct its completion
baseline from the last confirmed post before using it. Never enable both senders.

### VCT canary notifications

`canary-notify.py` keeps delivery state per GitHub workflow run. Repeating the
notification job for the same canary execution and complete diagnostics does not
send another failure message within 24 hours. A new workflow run, a re-execution
of the canary, changed diagnostics, or missing diagnostics still alerts. Failures
from separate executions are not assumed to have the same root cause. A successful
retry posts one recovery for a previously delivered failure in that workflow run;
cancelled and skipped results do not clear it. Delivery failure retains the prior
state, and missing cache state causes another alert rather than suppressing one.

A completely unreachable host cannot run its local minute monitor. Its fallback
alert comes from the audit. The requested cadence is 30 minutes, but observed
GitHub scheduling gaps mean this is not a maximum detection delay.

On a host:

```bash
systemctl status zakura-continuous-sync.service
systemctl status zakura.service
journalctl -u zakura-continuous-sync.service -f
/usr/local/sbin/zakura-continuous-sync.py status
/usr/local/sbin/zakura-monitor-status.py
/usr/local/sbin/zakura-monitor.py --dry-run
```

## Slack Alert Monitor

`zakura-monitor.timer` runs once per minute on every node. The monitor loads
`/etc/zakura-continuous-sync/alert-monitor.toml`, queries each node's
`zakura-monitor-status.py` helper locally or over SSH, and persists alert state
in `/var/lib/zakura-monitor/cluster-state.json`.

Alerts are posted through `/etc/zakura-alerts.env`, which should define one of
`SLACK_WEB_HOOK`, `SLACK_WEBHOOK_URL`, or `SLACK_WEBHOOK`. The file is a
root-only host secret and is not managed by the repository.

Alert ownership is local-only:

- each node emits alerts only for its own `zakura.service` and sync progress;
- peer queries are used only as evidence that the local node is stalled;
- no minute monitor emits down or stall alerts for another host;
- repeated alerts are throttled by `alert_throttle_seconds`;
- recovery messages are posted when a condition clears.

The status helper reports service state, metrics reachability, current block
height, controller state, and the diagnostic paths included in Slack alerts.

Node-down alerts require two consecutive samples where `zakura.service` is
explicitly inactive and the controller phase is `syncing` or unknown. Build,
install, cleanup, cooldown, complete, and failed controller phases can
intentionally leave the service inactive and do not produce node-down alerts.
Local status-query failures and metrics scrape failures also do not page as
down, and a failed local query restarts the confirmation streak so that the two
inactive samples are genuinely consecutive rather than separated by an
arbitrarily long gap of unknown service state. Metrics degradation is logged
because `/metrics` can grow large during long genesis syncs when historical
per-peer series accumulate.

The controller owns lifecycle alerts. It posts an immediate failure with a
bounded reason, posts one recovery after an explicit `resume` successfully
starts `zakura-continuous-sync.service`, and continues to own sync-completion
messages. The twice-hourly external audit still reports controller, service,
metrics, disk, unreachable-host, and stale-run problems by design.

## Completion Criteria

The controller requires several consecutive `/ready` successes before declaring
a cycle complete. `/ready` checks that the node has live peers, is near the
estimated network tip, and has a fresh tip. The controller also records
Prometheus samples in `samples.jsonl` so a completed or failed run has evidence
for height movement, readiness, legacy pipeline depth, and each active download
or verification phase. Node logs are written directly into the run directory,
so a failure keeps both the current log and its preceding rotated segment.

The relevant loopback endpoints are only bound locally:

- metrics: `http://127.0.0.1:9999/metrics`
- readiness: `http://127.0.0.1:8080/ready`
- liveness: `http://127.0.0.1:8080/healthy`

## Retention

Detailed traces stay enabled so a failure can be investigated without reproducing
it. The controller keeps up to 10 runs within a 20 GiB retention target, deleting
successful runs first, oldest first. The current run and the most recent failed
run are protected, including their traces, metadata, samples, and log tail.
Protected runs may exceed the target; cleanup never discards them to meet it.

During sync, the controller checks trace files with logrotate every polling
interval (normally 30 seconds). Each stream rotates at 128 MiB and keeps two older
segments beside the current file. Files can exceed that size between checks.
This preserves recent detailed history, not necessarily the entire sync.
`copytruncate` keeps the existing append-only writer working without a restart;
a small number of records can be lost at the copy/truncate boundary. Only the
controller rotates traces, so retention cannot race a separate trace cleaner.

For example, read `block_sync.jsonl.2`, then `.1`, then `block_sync.jsonl` for
chronological history. To use tools that expect one file, concatenate those
segments into a separate analysis directory. The stopped failure's files remain
unchanged until a newer failure replaces its protected status.

The controller also retains two cached binaries and removes interrupted
controller build worktrees and temporary binary copies. The cache is reserved
for controller builds. Unknown names and symlinked child directories are left
alone; the runs directory itself may point to another volume.

The controller also rotates each run's node log at 64 MiB, keeping the current
file and one prior segment. `/var/log/zakura/zebrad.log` points to the current
run's log. The minute monitor retains seven rotations of its own log and handles
legacy node logs until they are replaced by this symlink.

Cleanup runs before the initial disk check. During sync the controller checks
the state, run, and build-cache filesystems. Below 10 GiB free it stops the node,
records and alerts on the failed attempt, and prunes unprotected history. Recovery
resets the stopped, disposable chain database before checking for 15 GiB free on
all three filesystems. It then starts a fresh sync and sends a recovery alert
identifying the failed attempt once the new node service is active. Failed
delivery remains pending across controller restarts and retries during sync
polling; a new sync failure supersedes that pending recovery.
Until then it stays failed and rechecks once a minute. If protected evidence or
unrelated files occupy the remaining space, it waits rather than deleting them.
Other sync failures remain halted for investigation.

Artifact cleanup preserves chain state. Disk recovery and fresh attempts use the
existing sentinel-protected reset of disposable chain state, preserving network
identity and the failed run's diagnostics. Stopping the
controller also stops automatic recovery. Deployment removes the earlier
standalone storage timer; `--no-start` does not start the controller.

## Replacement Node Bootstrap

For a fresh Ubuntu x86_64 host:

1. Ensure Roman's SSH key can log in as root.
2. Add the DigitalOcean `zebra-ci` public key (fingerprint
   `12:4f:db:a1:b1:25:47:c0:92:73:08:76:4d:30:b4:30`) to
   `/root/.ssh/authorized_keys`.
3. Clone this repository to `/root/zakura`.
4. Install build prerequisites:

   ```bash
   apt-get update
   apt-get install -y \
     build-essential clang cmake git libclang-dev pkg-config \
     protobuf-compiler python3 logrotate
   ```

5. Install the Rust toolchain specified by `rust-toolchain.toml`.
6. Copy or recreate `/etc/zakura-alerts.env` with the Slack webhook value.
7. Update `nodes.toml` with the new host address if it changed.
8. Deploy only that node:

   ```bash
   python3 deploy/continuous-sync/deploy.py \
     --node temp-zakura-sync-test-1 deploy --no-start
   python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-1 status
   python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-1 deploy
   ```

## Safety Invariants

- Only paths under `/var/lib/zakura` are eligible for destructive cleanup.
- The wipe sentinel must exist before any state deletion.
- Only configured `wipe_entries` are deleted.
- `preserve_entries` are never deleted by the controller.
- A failed cleanup halts the controller before `zakurad` starts.
- `zakura.service` uses `Restart=no`; crashes are failures, not hidden
  restarts.
- Secrets are read from host env files or GitHub secrets and are never written to
  repository-managed templates.
