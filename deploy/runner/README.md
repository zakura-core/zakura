# Zakura Runner Services

This directory contains helper services and scripts that run on Zakura deploy or
benchmark hosts. The deploy workflows copy the tracked service files here onto
the self-hosted runners.

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
