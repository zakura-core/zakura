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
- `seconds_since_advanced` is at least 600 seconds for at least 10 minutes
- a dashboard endpoint is unreachable for at least 10 minutes

Down alerts take precedence over stalled alerts, so each node has at most one
active alert. The watchdog posts only on transitions: first failure after the
threshold, then recovery. Persistent failures do not post every poll.

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
