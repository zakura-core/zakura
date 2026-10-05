# Zakura Runner Services

This directory contains helper services and scripts that run on Zakura deploy or
benchmark hosts. The deploy workflows copy the tracked service files here onto
the self-hosted runners.

The public `sendrawtransaction` broadcast gateway lives under
[`deploy/gateway/`](../gateway/README.md).

## Cluster Status and Public Ironwood API

`zakura-cluster-status.py` polls authenticated node RPC endpoints over SSH and
serves both the existing fleet dashboard and a narrow public API:

- `GET /ironwood-status.json` returns a fresh, verified website response.
- `OPTIONS /ironwood-status.json` handles the allow-listed CORS preflight.
- `GET /healthz` reports service liveness.
- `GET /data` retains the existing fleet dashboard and watchdog response.
- `GET /node/<name>` serves the per-node detail page.
- `GET /data/node/<name>` returns that node's detail payload.

The public Ironwood response is unavailable with HTTP `503` if the service
cannot verify a matching network, Ironwood pool, tip, or source client, or if
the most recent complete observation is more than 120 seconds old. The endpoint
is rate-limited and permits cross-origin reads from `https://zakura.com`.
Testnet additionally permits the two development origins on port `1111`.

### Per-Node Detail

Node names in the fleet table link to `/node/<name>`. Both routes serve the same
HTML; the page branches on `location.pathname`, so the CSS and formatters are
shared and there is no second template to keep in sync.

The detail page leads with host vitals — free disk on the state directory,
available memory, process RSS, load, uptime, systemd restart count and kernel OOM
kills — then chain position, the sync pipeline, and peers. None of the vitals
need a metrics endpoint, so they populate on every node including `zcashd-compat`.

The sync and peer panels read the node's Prometheus exporter. The probe scrapes
`http://<metrics_endpoint>/metrics` from inside the node and filters it against a
small allowlist before returning, so a few KB crosses the ssh pipe rather than the
full ~350-name surface. Set `metrics_endpoint` and `health_listen_addr` in the
deployer config to populate these; where they are unset the panels say so instead
of rendering blanks.

Sparklines come from an in-memory per-node ring buffer sized by
`--history-window` (default 3h). This history is deliberately not persisted:
`--state-file` carries the durable orphan-pair and stall timers, and losing
sparklines across a dashboard restart is acceptable.

The probe collects a redacted tail of the node's `ERROR`/`WARN` log lines, but
the page does not serve it unless the dashboard runs with `--expose-logs`. These
dashboards are public and unauthenticated, so log text stays off by default even
though peer addresses are already redacted on the node.

### Tip Agreement and Orphan Pairs

Each poll groups the fleet by `(height, tip hash)`. A single group means the
nodes agree; several groups at the leading height mean a split, and the
dashboard labels each node `majority`, `fork`, `ahead`, or `behind`. Fork depth
between two tips at the same height is estimated from best-chain ancestor
hashes sampled at 1, 2, 5, 10, and 32 blocks back, so an unresolved fork reports
`> 32 or unknown` rather than a wrong number.

A node whose height drops or whose tip hash changes at the same height records
an orphan pair: the discarded hash, the new canonical hash, and the depth. Pass
`--state-file` to persist that history across restarts; the deploy workflows
point it at `/var/lib/zakura-<network>-dashboard/orphan-pairs.json`. Without the
flag the history is in-memory only and is lost on every restart.

## Fleet Slack Watchdog

`zakura-cluster-watchdog.py` is a small stdlib-only Python service that polls the
mainnet and testnet cluster status dashboards and posts Slack transition alerts
when a fleet node remains unhealthy.

It is installed by `.github/workflows/zakura-mainnet-deploy.yml` on `us-east-0`:

- systemd service: `zakura-fleet-watchdog.service`
- install dir: `/opt/zakura-fleet-watchdog`
- config: `/opt/zakura-fleet-watchdog/fleets.toml`
- state: `/var/lib/zakura-fleet-watchdog/state.json`
- Slack env: `/etc/zakura-fleet-watchdog/env`
- deploy suppression marker: `/run/zakura-fleet-watchdog/deploy-suppressed-until`

The default config in `fleet-watchdog.toml` watches:

- mainnet: `http://127.0.0.1:8090/data`
- testnet: `http://167.99.103.111:8090/data`

Alerts fire only after a sustained condition:

- `health` is `down` or `rpc_error` for at least 10 minutes
- one node's height has not advanced for at least 10 minutes
- a strict majority of observable nodes shares the highest observed height and
  block hash for at least 30 minutes, with at least two participating nodes
- a dashboard endpoint is unreachable, malformed, or serves a stale collector
  snapshot for at least 10 minutes

Stall alerts include a fixed set of sync-pipeline and VCT repair metrics. The
fleet `/data` response copies only those numeric fields into each row. The
watchdog therefore uses the same collector snapshot for the stall condition and
its diagnostics. The diagnostic object contains no per-node history, logs, or
addresses.

The watchdog coalesces that majority tip into one fleet alert. Nodes at lower
heights keep their individual stall timers, so a lagging or resyncing node does
not cause a separate alert for each node at the majority tip. A higher observed
tip, conflicting hashes at the highest height, a missing height/hash/timer on
any observable node, or the absence of a strict majority keeps individual
incident tracking. Down and starting nodes do not participate in the tip
comparison. Down alerts take precedence over stalled alerts and retain their
own timers.

When a shared tip starts advancing, former tip followers get up to two minutes
for propagation before their individual stall alerts can fire. Starting this
grace requires a shared observation within the preceding two minutes, no
existing individual alert for the node, complete tip observations, and reported
ancestry linking all higher tips to the former shared block. A shared fleet
warning does not disqualify its followers.

The watchdog persists each reference's last positively linked tip. Later
snapshots can link through that tip when the collector does not sample the
exact distance to the original shared block. If neither distance is sampled,
an already proven reference retains only the remainder of the original grace;
its unproven newer tip is not saved as an ancestry witness. Missing node
height/hash/timer, conflicting hashes or ancestry, a reference rolling back,
or an unproven new reference cancels the grace. This adds no collector RPCs.

The grace timer starts when the newer block is first observed and survives
restarts; more blocks do not extend it. A node that remains stuck then follows
the existing stall threshold. A new watchdog with no prior shared observation,
or an initial extension whose ancestry cannot be established, uses the batched
fallback.

This comparison is within the monitored fleet, not an independent verification
of network health. A correlated sync failure can also produce agreement, so the
30-minute shared warning remains enabled. The existing 10-minute individual
stall, RPC/down, and dashboard thresholds remain in place. Only the narrow
propagation grace can defer an otherwise due node stall. Testnet uses the same
policy.

The watchdog posts only on transitions. Persistent failures do not post every
poll. A transition to another unhealthy state is labeled as a condition change,
not a green recovery. Consolidating alerts or changing shared-tip ownership is
informational; clearing a shared stall after height progress is a recovery.

### Batched delivery

Each fleet sends at most one Slack payload per poll. Simultaneous transitions
share a message containing the essential lines for each incident and dashboard
links. Single incidents retain their detailed diagnostics. Eleven simultaneous
stall alerts and eleven simultaneous recoveries therefore produce two messages,
even when tip grouping is unavailable. The messages are grouped at the sender;
they are not Slack thread replies.

Larger batches split at the message size limit without dropping incident
summaries. Remaining chunks stay in `pending_delivery` in the state file and
send one per poll, in order. Each payload includes its observation time (except
a legacy single message that already fills the size limit), so delayed delivery
is distinguishable from a fresh observation. Polling and incident tracking
continue while delivery is pending, including recoveries and new failures.
Deployment suppression defers queued payloads until the suppression expires.

The watchdog checkpoints queued messages before sending and checkpoints each
successful acknowledgement. Confirmed alert state changes are committed when
the fleet's pending messages have all been accepted. Failed payloads retry after
restart; acknowledged chunks are not retried after their checkpoint. Delivery
is at least once: a timeout after Slack accepts a message, or a crash before the
acknowledgement checkpoint, can still repeat that payload. Keep the state file
when upgrading and allow pending delivery to drain before reverting to a
watchdog version that does not understand the queue. A prolonged Slack outage
can grow the queue; the watchdog retains those transitions instead of dropping
them.

### Decision evidence

The `decisions` state bucket retains the last 16 grouping/tip/grace changes per
fleet, with at most 64 rows per entry. It records observation time, the grouping
reason, participant names, heights, hashes, and progress ages. It excludes RPC
credentials, webhook URLs, and arbitrary diagnostics. Row counts identify a
truncated fleet snapshot. Pending delivery retains its prospective decision
history until acknowledged.

Slack delivery is **webhook-only**. Set:

- `SLACK_WEB_HOOK`

Do not commit real Slack credentials. Install them on the runner in
`/etc/zakura-fleet-watchdog/env` with mode `600`, or provide the
`SLACK_WEB_HOOK` GitHub Actions environment secret so the deploy workflow
writes the env file.

Manual checks on `us-east-0`:

```bash
systemctl status zakura-fleet-watchdog
journalctl -u zakura-fleet-watchdog -f
```

One-shot dry run:

```bash
python3 /opt/zakura-fleet-watchdog/zakura-cluster-watchdog.py \
  --config /opt/zakura-fleet-watchdog/fleets.toml \
  --state-file /tmp/zakura-fleet-watchdog-state.json \
  --once \
  --dry-run
```

During restart deploys, the workflows write a Unix timestamp 20 minutes in the
future to `/run/zakura-fleet-watchdog/deploy-suppressed-until`. While that marker
is active, new failure alerts are logged locally but not posted to Slack.

## Compatibility Monitoring

The fleet watchdog also monitors the zcashd-compat pair on `zakura-compat`
(`root@159.203.113.196`). It replaces the Rust `zakura-watchdog` sidecar and its
Sentry reporting; failures and recoveries go to `#zakura-alerts` with the other
fleet alerts.

The stdlib-only package `zakura_monitoring/` is shared by both hosts:

| Module | Role |
| --- | --- |
| `compat.py` | Local checker: process, peer-pinning and height-drift predicates |
| `monitor.py` | Fleet lane: bounded probe worker and untrusted-outcome validation |
| `remote.py` | Bounded subprocess and SSH execution |
| `slack.py` | Slack sanitization and webhook transport |
| `state.py`, `delivery.py` | Durable state and batched, retried delivery |
| `suppression.py` | Fleet and compatibility deployment markers |
| `install.py` | Versioned releases, cutover and rollback operations |

### Checker

`zakura-compat-check` (and its thin wrapper `deploy/zcashd-compat/sync-check.sh`)
checks, in order: a `zakurad .*--zcashd-compat` process, a `zcashd .*-connect`
process, zcashd `getconnectioncount == 1`, and absolute zakurad/zcashd
`getblockcount` drift `<= HEIGHT_MAX_DRIFT` (default 10, as in the deployed Rust
watchdog; `HEIGHT_MAX_DRIFT=30` gives the same ~12 minutes at NU7's 25-second
spacing). It reads the variables of the former shell check:
`ZAKURA_RPC_URL`, `ZAKURA_COOKIE_FILE`, `ZAKURA_RPC_CONF`, `ZAKURA_RPC_USER`,
`ZAKURA_RPC_PASSWORD`, the `ZCASHD_*` equivalents, the process patterns,
`HEIGHT_MAX_DRIFT`, `SYNC_CHECK_TIMEOUT` (600), `SYNC_CHECK_INTERVAL` (15) and
`WATCHDOG_RPC_TIMEOUT` (30). A cookie file wins over a config file and
user/password; an explicitly empty cookie path disables cookie auth.
Command-line flags override the environment, which overrides `--env-file`.

- `zakura-compat-check probe` runs one cycle and always prints one JSON outcome
  (schema `zakura-compat-outcome/1`): status, predicate, numeric details,
  observation time and suppression metadata. Health failures exit 0; invalid
  configuration exits 2.
- `zakura-compat-check check` retries every 15 seconds within a 600-second
  deadline that also bounds each RPC. It exits 0 on pass, 1 on failure or
  deadline, 2 on invalid configuration, and never reads Slack or suppression.

Outcomes and errors contain fixed predicates, error categories and integers
only: never cookies, passwords, URLs, response bodies or logs.

### Fleet Lane

With `ZAKURA_COMPAT_MONITORING=1` (written by cutover as the drop-in
`zakura-fleet-watchdog.service.d/80-compat-monitoring.conf`) the watchdog probes
the single `[[compatibility]]` target in `fleet-watchdog.toml` every 60 seconds:
one worker thread, at most one probe in flight, and a 120-second hard timeout
covering SSH, RPC, output collection and process cleanup. Pipe reads are
non-blocking, so a detached descendant holding stdout open cannot strand the
worker. It uses root's SSH identity with `BatchMode=yes` and the
host key pinned in `/etc/zakura-fleet-watchdog/known_hosts`. Fleet polls never
wait for a probe; the main thread alone applies results and owns state.
The lane passes `height_max_drift = 10` from that target explicitly, so a host
env file cannot loosen monitoring unnoticed. Validation runs parity with the
same explicit limit.

- The first completed failure alerts immediately, including after a restart
  with no open incident. A persistent failure, whatever its predicate, is one
  incident. A complete, valid pass after a reported incident sends one recovery.
- An SSH timeout or failure, a missing checker, or malformed, stale, oversized
  or mismatched output is a monitoring failure. It can open or continue an
  incident but never recovers one.
- Messages name the host, check, predicate, peer count, heights, drift and
  observation time.
- Incidents live in `compatibility`, probe telemetry in `compatibility_probes`
  and undelivered messages in `compatibility_pending_delivery`, separate from
  the fleet namespaces. Delivery reuses the fleet lane's batching, checkpointing
  and retry, so a Slack outage delays but never drops an alert or recovery.
- Only the compatibility marker
  `/run/zakura-watchdog/deployment-suppressed-until` on `zakura-compat`, written
  by deploys that restart `zakurad-compat`, mutes this lane. A marker more than
  1200 seconds ahead, or not a whole Unix timestamp, is ignored. Probes keep
  running; queued alerts and recoveries are retained without delivery or
  acknowledgement until suppression expires or a valid probe clears it. The
  last bounded window survives restart and missing or unavailable probes.
  A failure that outlives the marker alerts on the next probe. The fleet
  marker on `us-east-0` does not mute this lane.

### Deployment

Regular mainnet deploys install the fleet watchdog as a versioned release under
`/opt/zakura-fleet-watchdog/releases/<sha>` with `current`,
`zakura-cluster-watchdog.py` and `fleets.toml` symlinks. They keep the live unit
and its drop-ins (including the Mac comparison settings), state and every
non-Slack env setting, and replace the webhook (env mode 600) only when the
secret is provided. Once the lane is enabled they also refresh the checker on
`zakura-compat`. Neither path retires the Rust watchdog.

`zakura-mainnet-deploy.yml` with `operation=monitoring`, `node=zakura-compat`
and `ref=<full tested SHA>` runs one explicit stage of
`zakura-monitoring-deploy.py` at a time. No stage builds, installs, stops or
restarts `zakurad` or `zcashd`, or touches the dashboard or gateway.

Before dispatch, provision the repository-level GitHub Actions secret
`ZAKURA_COMPAT_SSH_KNOWN_HOSTS` with a valid `known_hosts` entry for
`159.203.113.196`, verified through an independent trusted host console or
existing operator trust store. Keep the verified entry in Infisical and mirror
it to the repository secret; no environment-specific host-key setting is needed.
This is a public SSH host key, not a private credential. The workflow fails
before contacting the host if this secret is
missing, malformed or names another host. It never trusts a fresh network scan.
The same entry authenticates deployment SSH and is installed for fleet probes;
rotate it only after independently verifying a host key change.

| Stage | Effect |
| --- | --- |
| `status` | Read-only summary of both hosts |
| `install` | Checker release on `zakura-compat` (`/opt/zakura-monitoring`, activated, inert), env seeded from the Rust watchdog's checker settings, fleet release staged, host key pinned |
| `validate` | Service-context probe, Rust/Python parity (`SENTRY_DSN` unset) and the shipped synthetic tests on both hosts |
| `cutover` | Back up fleet state, activate the fleet release, enable the lane, restart only `zakura-fleet-watchdog`, wait for a passing live probe |
| `soak` | 30-minute read-only record of probes, height advancement and service health |
| `slack-test` | Opt-in: one labeled failure and recovery to `#zakura-alerts` from temporary state |
| `finalize` | Requires both active releases to match the requested SHA and a fresh passing live probe; stops `zakura-watchdog` and moves its unit, binary and env to `/var/backups/zakura-monitoring/` |
| `rollback` | Disables the lane, restores the previous fleet and checker releases, restores the Rust watchdog if finalized; nodes are untouched |

Retirement aborts before moving artifacts if stopping the Rust service fails.
Service reload or restart failures fail the workflow. Rollback retains its
original release target across retries, including partial rollback failures.
Rust restoration also resumes partially moved files after verifying their
hashes; it records completion only after the service starts successfully.
Operator edits to restored files are preserved and cause a failed retry.

Each run uploads `monitoring-evidence.json` with the commit, package and
manifest digests, run URL and per-step results. Record any Slack message links
from the channel by hand; webhooks do not return them. To restore the
pre-cutover fleet state too, run on `us-east-0`:

```bash
python3 deploy/runner/zakura-monitoring-deploy.py rollback --sha <sha> \
  --known-hosts <pinned known_hosts> \
  --restore-state /var/lib/zakura-fleet-watchdog/backups/state-<stamp>.json
```

The acceptance tool also runs directly from an installed release, for example:

```bash
python3 /opt/zakura-fleet-watchdog/current/zakura-monitoring-acceptance.py synthetic
python3 /opt/zakura-fleet-watchdog/current/zakura-monitoring-acceptance.py soak --duration 1800
```
