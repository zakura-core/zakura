# Zakura Continuous Genesis Sync Fleet

This directory codifies the six permanent mainnet sync nodes that repeatedly
test a fresh genesis-to-tip sync from the latest `origin/main` build:

| Node | Address | Mode |
| --- | --- | --- |
| `temp-zakura-sync-test-1` | `root@138.68.43.212` | dual-stack |
| `temp-zakura-sync-test-2` | `root@138.197.218.91` | Zakura/v2-only |
| `temp-zakura-sync-test-3` | `root@134.209.49.92` | Zebra/legacy-only |
| `temp-zakura-sync-test-4` | `root@134.209.57.146` | Zebra/legacy-only |
| `temp-zakura-sync-test-5` | `root@142.93.27.189` | Zebra/legacy-only |
| `temp-zakura-sync-test-6` | `root@138.68.249.46` | Zebra/legacy-only |

Each node runs a local systemd controller. GitHub Actions installs and audits the
controller, but it does not hold an SSH session open during the long sync.

## Lifecycle

`zakura-continuous-sync.service` runs
`/usr/local/sbin/zakura-continuous-sync.py` on each host:

1. Fetch `origin/main` in `/root/zakura` and pin the full commit SHA.
2. Build `zakurad` from a detached worktree and cache the binary by SHA.
3. Atomically install the binary at `/usr/local/bin/zakurad`.
4. Stop `zakura.service`.
5. Verify `/var/lib/zakura/.continuous-sync-wipe-ok` exists.
6. Delete only the configured disposable state entries:
   `/var/lib/zakura/state` and `/var/lib/zakura/non_finalized_state`.
7. Preserve `/var/lib/zakura/network`, controller state, logs, traces, and build
   cache.
8. Render `/etc/zakura/zebrad.toml` with the node's assigned `p2p_stack` and a
   run-specific trace directory.
9. Start `zakura.service` with `Restart=no`.
10. Poll metrics and `/ready` until the node is stably near tip.
11. Stop the node, post a completion alert to `#zakura-alerts`, and start the
    next cycle after a short cooldown.

The same commit may be tested repeatedly. That is intentional: the fleet is a
continuous sync canary, not a once-per-SHA CI job.

## Failure Semantics

Any build, install, cleanup, startup, sync, stall, timeout, disk, metrics, or
readiness failure halts the affected node:

- `zakura-continuous-sync.service` exits non-zero.
- `/var/lib/zakura-continuous-sync/state.json` records `failed = true`.
- the current run's `run.json` records the phase and failure reason.
- a Slack alert is posted with the node mode, SHA, height, SSH target, log path,
  trace path, and monitor log path.

The controller does not automatically retry after failure. Resume is an explicit
operator action:

```bash
python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-2 resume
```

or run the **Zakura continuous genesis sync fleet** workflow with
`action=resume` and the failed node name.

## Files and Services

Tracked repository files:

- `nodes.toml` is the source-of-truth fleet inventory and policy.
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
python3 deploy/continuous-sync/deploy.py --node temp-zakura-sync-test-3 status
```

The scheduled workflow runs `audit` twice per hour. It alerts when a host is
unreachable, the controller is halted, the node service is inactive while a run
claims to be syncing, metrics are unavailable during sync, or disk free space is
below the configured 10 GiB floor.

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

Alert ownership matches the original temporary-node script:

- a node emits its own local down alert;
- the elected healthy leader emits peer down and peer-evidence stall alerts;
- repeated alerts are throttled by `alert_throttle_seconds`;
- recovery messages are posted when a condition clears.

The status helper reports service state, metrics reachability, current block
height, controller state, and the diagnostic paths included in Slack alerts.

## Completion Criteria

The controller requires several consecutive `/ready` successes before declaring
a cycle complete. `/ready` checks that the node has live peers, is near the
estimated network tip, and has a fresh tip. The controller also records
Prometheus samples in `samples.jsonl` so a completed or failed run has evidence
for height movement, readiness, legacy pipeline depth, and each active download
or verification phase. The controller copies the current node log into the run
directory on both completion and failure.

The relevant loopback endpoints are only bound locally:

- metrics: `http://127.0.0.1:9999/metrics`
- readiness: `http://127.0.0.1:8080/ready`
- liveness: `http://127.0.0.1:8080/healthy`

## Retention

Detailed run artifacts live under `/var/log/zakura/runs/`. The controller keeps
the active run and the two newest prior runs (`retention_runs = 3`), deleting
older completed or failed run directories when each cycle starts.

`templates/logrotate` also rotates `/var/log/zakura/zebrad.log` and
`/var/log/zakura/monitor.log` daily, keeping five compressed rotations.

The controller checks disk free space before and during sync. If free space falls
below `min_free_bytes`, it halts and alerts instead of filling the host.

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
     protobuf-compiler python3
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

## Relationship to Other Sync Workflows

This fleet is permanent and destructive on its own dedicated hosts. It differs
from `.github/workflows/do-sync-test.yml`, which creates an ephemeral DigitalOcean
droplet, restores a cached state, runs a bounded sync-confidence window, and
destroys the droplet.
