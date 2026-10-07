# Zakura live dashboard

A separate, read-only web app for observing one mainnet Zakura node. It has no
dependency on the block processing profiler. It runs on the dedicated
`codex-vast-tide-7164` host in San Francisco, separate from the production fleet.
The intended public hostname is <https://gui.valargroup.dev/>. Direct access
during DNS setup is <http://146.190.146.239/>.
The original fleet status page stays at <https://status-mainnet.valargroup.dev/>.
This dashboard does not replace it or install routes on the production gateway.

## Data

The collector polls local RPC every 5 seconds, selected Prometheus metrics every
15 seconds, and RPC peers and Linux host observations every 30 seconds.
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
Verified transaction rates count completed verification before mempool admission.
Policy rejection rates cover the oversized-transaction counter, not all failures.
Neither rate is chain TPS.

## Dedicated node

`standalone/zakurad.toml` and `standalone/zakura-dashboard-node.service` configure
an independent mainnet node with both legacy TCP and native QUIC networking.
Pruned storage keeps the default 10,000-block transaction retention window.
Its state and native identity live under `/var/lib/zakura-dashboard-node` and
survive restarts. Install the configuration under `/etc/zakura-dashboard-node`
and point `/opt/zakura-dashboard-node/current/zakurad` at the selected binary.
The node runs as the dedicated `zakura-dashboard-node` user.

RPC, metrics, and health listeners bind to loopback. Public P2P uses TCP 8233 and
UDP 8234. Heavy JSONL tracing is disabled in this baseline configuration. Enable
it only for bounded captures with sufficient disk space. This node is not a
fleet deployment target or a fleet alert source.

The dashboard service has no systemd dependency on the node service, so these
commands leave the website available and preserve chain state:

```sh
systemctl stop zakura-dashboard-node
systemctl start zakura-dashboard-node
systemctl restart zakura-dashboard-node
```

Host data comes from `/proc`, filesystem capacity counters, and a bounded
read-only systemd query. Automatic restart counts refer to systemd's current
unit counter. OOM history and tip-switch history are unavailable in local mode.
The web service uses the persistent `zakura-dashboard-web` identity so D-Bus
can authenticate its read-only systemd queries, with systemd sandboxing retained.
A missing process reports unavailable RSS while host observations remain fresh.
The dashboard starts its own history for this node. Do not import observations
from a different node.

## Run and check

Python 3.12 or later is sufficient. There are no third-party dependencies.

```sh
python3 -m unittest discover -s deploy/live-dashboard/tests -v
node --check deploy/live-dashboard/static/app.js
python3 deploy/live-dashboard/dashboard.py --history /tmp/zakura-dashboard.sqlite3
```

By default, RPC uses `127.0.0.1:8232`, metrics use `127.0.0.1:9999/metrics`, and
host observations describe the local Linux host and `zakura-dashboard-node`
service. Override `--rpc`, `--metrics`, `--node`, `--node-service`, and `--node-disk`
for a compatible mainnet node. The optional `--fleet` argument retains the old
fleet-host adapter during migration.
Open <http://127.0.0.1:8095/>. Without upstreams, the UI explicitly shows unavailable
data. Do not expose the Python listener directly to the Internet.

## Deploy

Commit and push the branch first. The script requires root SSH access to the
**dedicated** dashboard host. It refuses a running fleet `zakurad` service.

```sh
bash deploy/live-dashboard/deploy.sh codex-vast-tide-7164
```

The script archives the exact commit into an immutable release directory,
checks ingress drift against the previous release, validates Caddy configuration,
and runs backend tests on the host. It starts a separate unprivileged systemd
service with a 256 MiB memory cap and 25% CPU quota. Readiness checks verify that
the expected dashboard build answers cached API reads. Deployment can succeed
while the experimental node is stopped. `/healthz` separately reports fresh
chain data. Failures restore the previous dashboard release and Caddy
configuration. Zakura is never restarted by this script.

Ingress is owned by `standalone/Caddyfile`, not the fleet gateway deployment.
The new host is DigitalOcean droplet `607024683`, region `sfo3`, size
`s-8vcpu-16gb` (8 vCPU, 16 GB RAM, 320 GB disk). Its public IPv4 is
`146.190.146.239`. DNS is a dedicated DNS-only A record for `gui.valargroup.dev`
with automatic TTL. This record is outside the voting infrastructure Terraform
resources. Keep this document and the Caddyfile in sync with any hostname or
address change. Do not repoint either production status hostname.

The initial node binary is the signed `v1.6.0` release, commit `ebda15e23ed0`.
Its release checksum signature is verified with the maintainer key documented
in `docs/verify.md`. The node is seeded from the publisher's `mainnet-pruned`
snapshot, verified against its manifest checksum before extraction. No production
node database is read or copied. The snapshot manifest is retained under
`/opt/zakura-dashboard-node/downloads/snapshot.json`.

The host has Rust 1.99 and a checkout at `/root/workspace/zakura`. For node
experiments, select a commit in that checkout and build with:

```sh
. /root/.cargo/env
cd /root/workspace/zakura
CARGO_BUILD_JOBS=4 cargo build --release --locked -p zakurad
```

Stop the node while compiling if an experiment needs more memory. Before
installing the build, stop `zakura-dashboard-node`. Install the binary in a new immutable
`/opt/zakura-dashboard-node/releases/<commit>/bin/` directory, and change the
`current` symlink before starting the service. Each release directory needs a
`zakurad` symlink to `bin/zakurad`. Retain the previous release for rollback and
verify `/ready` on port 8080 plus RPC chain progress after a change. Database
format changes may prevent binary rollback. Preserve or recreate this disposable
node's state as appropriate for that experiment. The website and its history
remain separate throughout.

Read-only operational checks:

```sh
systemctl status zakura-live-dashboard
journalctl -u zakura-live-dashboard -n 30 --no-pager
curl --fail http://127.0.0.1:8095/healthz
```

The public surface consists of static files, `/api/overview`,
`/api/history?window=15m|1h|6h|24h`, and `/healthz`, served at the site root through
Caddy. Requests are bounded to 24 concurrent server threads. History retention,
block cache size, and upstream response limits bound memory and disk growth.
Readiness requires fresh chain data. Other sources degrade independently.
