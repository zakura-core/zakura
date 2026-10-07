# Zakura live dashboard

A separate, read-only web app for observing one mainnet Zakura node. It has no
dependency on the block processing profiler. The initial deployment runs on
`us-east-0` at <https://status-mainnet.valargroup.dev/live/>. Existing fleet status
and broadcast endpoints retain their routes.

## Data

The collector polls local RPC every 5 seconds, selected Prometheus metrics every
15 seconds, and RPC peers and existing fleet host observations every 30 seconds.
Browsers read cached aggregates. They cannot issue RPC requests, scrape metrics,
choose upstream URLs, or read node logs. Peer addresses, configuration, and host
paths are dropped. SQLite belongs to this dashboard and is stored on the host
root filesystem. The collector never opens the node database.

Charts retain 24 hours of real 15-second samples. Initial collection and missing
series remain unavailable rather than becoming zero. Failed sources immediately
become stale. Chain observations also expire after 25 seconds, other observations
after 120 seconds. Counter resets and gaps over 120 seconds do not produce rates.
History gaps over 45 seconds are not joined by chart lines. Changing the time
range changes activity charts only. Exporter quantiles retain their own rolling
window and are never summed across labels.
The exporter emits zeroes for empty rolling summaries. If its maximum duration
is zero, the dashboard reports unavailable latency rather than a zero-cost
operation. Support blocks remaining are derived from the supported height and
the current verified height because the node's remaining-block gauge updates
on a slower loop.

The block stream walks the observed best tip's ancestry. Backfill is limited to
four RPC reads per cycle and stops starting reads after two seconds. Each read
has a five-second socket timeout and a 16 MiB response limit. Up to 100 blocks
are cached and the newest 30 are published. An alternate block at an observed
height is marked off-chain. Older unlinked blocks have unknown membership.
Returning to a previously seen fork recalculates membership. Miner timestamps
and polling observations are not block processing traces.

The finalized height means Zakura's finalized storage boundary, not absolute
proof-of-work finality. Header height does not establish block validity. Peer
gauges may overlap. Legacy transport traffic excludes native transport. Host
load is not CPU percent. Proof batch wall times are not CPU time. Pool totals
are public aggregates and do not reveal shielded balances or owners.

## Run and check

Python 3.12 or later is sufficient. There are no third-party dependencies.

```sh
python3 -m unittest discover -s deploy/live-dashboard/tests -v
node --check deploy/live-dashboard/static/app.js
python3 deploy/live-dashboard/dashboard.py --history /tmp/zakura-dashboard.sqlite3
```

By default, RPC uses `127.0.0.1:8232`, metrics use `127.0.0.1:9999/metrics`, and
host observations come from the existing public fleet service. Override
`--rpc`, `--metrics`, `--fleet`, and `--node` for a compatible mainnet node.
Open <http://127.0.0.1:8095/>. Without upstreams, the UI explicitly shows unavailable
data. Do not expose the Python listener directly to the Internet.

## Deploy

Commit and push the branch first. Run from a checkout with the mainnet gateway
base fetched. The script requires root SSH access to the existing gateway.

```sh
bash deploy/live-dashboard/deploy.sh us-east-0
```

The script archives the exact commit into an immutable release directory, checks
that the live Caddyfile matches the base or candidate, validates the candidate,
and runs backend tests on the host. It starts a separate unprivileged systemd
service with a 256 MiB memory cap and 25% CPU quota. Only after readiness does it
reload Caddy. Existing fleet and broadcast health routes are checked afterward.
Failures restore the previous dashboard release and Caddy configuration. Zakura
is never restarted. Rollback copies are retained under
`/opt/zakura-live-dashboard/rollback.*`.

The source of truth for ingress remains `deploy/gateway/mainnet/Caddyfile`.
The normal mainnet deployment workflow installs that file from its selected
revision. **While these changes remain on a branch, a deployment from main can
remove the dashboard route.** Rerun this branch's deployment after reconciling
any gateway drift. The collector and saved history continue running if the route
is removed. A future merge should retain this route in the gateway source.

Read-only operational checks:

```sh
systemctl status zakura-live-dashboard
journalctl -u zakura-live-dashboard -n 30 --no-pager
curl --fail http://127.0.0.1:8095/healthz
```

The public surface consists of static files, `/api/overview`,
`/api/history?window=15m|1h|6h|24h`, and `/healthz`, all under `/live/` through
Caddy. Requests are bounded to 24 concurrent server threads. History retention,
block cache size, and upstream response limits bound memory and disk growth.
Readiness requires fresh chain data. Other sources degrade independently.
