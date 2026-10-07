# Zakura live dashboard

A separate, read-only web app for observing one mainnet Zakura node. It has no
dependency on the block processing profiler. It runs on the dedicated
`codex-vast-tide-7164` host in San Francisco, separate from the production fleet.
The intended public hostname is <https://gui.valargroup.dev/>. Direct access
during DNS setup is <http://146.190.146.239/>.
The original fleet status page stays at <https://status-mainnet.valargroup.dev/>.
This dashboard does not replace it or install routes on the production gateway.

## Console layout

All telemetry is visible on one scrolling page. The sticky toolbar provides
section jump links and one period control for history charts, event totals, and
the Recent blocks panel. The live block rail keeps the newest 30 blocks.

- **Activity** shows chain TPS, recent block transaction counts, mempool stage
  counts, cumulative proof checks, block queues, and peer RTT.
- **Pipeline** separates current work from saved processing timings. It shows
  outstanding and applying work, the latest observed p50/p95 stage and verifier
  summaries, a history of real timing samples, and memory pressure.
- **Network** separates legacy TCP wire traffic from host interface traffic.
  It includes native QUIC sessions and discovery outcomes, protocol message
  counts, stream opens, cumulative first-body arrivals, and anonymized RPC peers.
- **System** shows CPU utilization, I/O wait, node RSS, storage and compactions,
  RPC latency, chain support limits, and public value pools.

Chain TPS uses at most 30 consecutive intervals on the observed best chain.
The transaction count excludes the oldest boundary block and is divided by the
miner timestamp difference between the two boundary heights. Excluding coinbase
subtracts one transaction per counted block. Fewer than two linked intervals or
a nonpositive timestamp difference is unavailable. This window is independent
of the chart time-range selector. Reorganizations recompute it from the current
ancestry. TPS measures chain activity, not hardware capacity.

The transaction flow is the useful counterpart to Firedancer's TPU view for a
Zcash node. Its stages are independent event totals in the selected period,
not a conserved funnel or a trace of the same transaction. Verification completes
before mempool admission, and advertisements can repeat. Missing counters remain
unavailable. Failed tasks include download errors and timeouts. Their raw error
labels are not published. Oversize policy rejections are a separate counter.

The native queue gauge records the most recent enqueue depth for a stream kind.
It is not a live sum across connections. Peer RTT percentiles use the measured
RPC peers and omit missing or negative values. Host network counters include all
non-loopback interfaces and all applications. They must not be labeled QUIC
traffic. CPU busy and I/O wait percentages use Linux tick deltas across all CPUs.
Counter resets, interface changes, and long sampling gaps leave rates unavailable.

### Further node instrumentation

Still needed for a complete processing waterfall: bounded events joining block
receive, verification, contextual checks, and commit by block hash. Aggregate
latency summaries overlap and cannot be stacked into an end-to-end duration.
For transaction flow, add explicit mempool admission, mined, expiration, eviction,
and bounded rejection reason counters plus correlated queue/verification timings.
For native networking, add wire bytes, per-peer RTT and loss/retransmission
statistics, and queue gauges that aggregate every active connection and update on
dequeue. Keep these experiments on the dedicated node before proposing upstream
changes. The console omits Solana stake, voting, leader schedule, slots, shreds,
and program-cache panels because they do not describe a Zcash full node.

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
range changes charts, event totals, and recent blocks. Exporter quantiles retain their own rolling
window and are never summed across labels.

Event totals use exact counter increases over successful sampling intervals of
at most 45 seconds. Only whole intervals inside the requested period count.
Repeated intervals are deduplicated. Missing counters, counter decreases, and
long gaps produce no count. Cumulative charts retain these gaps and sum only
recorded events. Coverage is reported separately, per counter in the API. Old
rate-only samples are not converted into estimated event counts.

Metrics collection reads the local node's systemd activation ID before and after
the scrape. Count intervals require the same activation across both scrapes.
This also detects a restart whose new counters exceed the previous values.
Host error counts require the same boot and interface set. These identities
remain private. Event counts require local mode and systemd identity access.
The optional fleet mode continues to provide rates but no node event counts.
Dashboard restarts preserve recorded history but start a new counting baseline.
The history endpoint accepts a bounded period and an optional end timestamp so
changing periods while paused does not pull in newer events.
The exporter emits zeroes for empty rolling summaries. If its maximum duration
is zero, the dashboard reports unavailable latency rather than a zero-cost
operation. The pipeline retains each stage and verifier's last non-empty summary
for up to 24 hours in the dashboard's SQLite database. Every saved reading has
its own observation time and survives dashboard restarts. Readings remain visible
when collection fails, with live source freshness reported separately. Observation
time means when the dashboard sampled the summary, not when an individual block
finished. Rows may describe different windows or mempool work. They are not a
single block's waterfall. Live gauges, rates, and chart history never use these
saved readings as replacements. Crypto charts sample live rolling p50/p95 summaries every 15 seconds, separately
for each verifier. These are not individual batch events or percentiles recomputed
for the selected period. Crypto history begins at deployment and is not backfilled
from retained readings. The processing history uses individual dots so
isolated timing samples remain visible without joining gaps.

Support blocks remaining are derived from the supported height and
the current verified height because the node's remaining-block gauge updates
on a slower loop.

The block stream walks the observed best tip's ancestry. Backfill is limited to
four RPC reads per cycle and stops starting reads after two seconds. Each read
has a five-second socket timeout and a 16 MiB response limit. Up to 4,096 blocks
are cached and the newest 30 populate the live rail. Backfill does not request
bodies below the reported prune height. Recent blocks shows available canonical
blocks whose miner timestamps are inside the selected period, including the
period's mean serialized size. Long periods scroll horizontally. Older blocks
may be unavailable until recorded or backfilled. An alternate block at an observed
height is marked off-chain. Older unlinked blocks have unknown membership.
Returning to a previously seen fork recalculates membership. Miner timestamps
and polling observations are not block processing traces.

The finalized height means Zakura's finalized storage boundary, not absolute
proof-of-work finality. Header height does not establish block validity. Peer
gauges may overlap. Legacy transport traffic excludes native transport. Host
load is not CPU percent. Proof batch wall times are not CPU time. Pool totals
are public aggregates and do not reveal shielded balances or owners.
Verified transaction totals count completed verification before mempool admission.
Policy rejection totals cover the oversized-transaction counter, not all failures.
These event counts are distinct from chain TPS.

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

Python 3.12 or later is sufficient for the server, which uses only the standard
library. Charts use a pinned, locally served uPlot 1.6.32 browser bundle. See
`static/vendor/uplot/README.md` for its source, integrity, and license. No package
installation or CDN is needed on the host.

uPlot handles axes, rendering, the vertical cursor, and colored sample markers.
The cursor and compact tooltip snap to the same timestamp. Missing readings
have no marker, and gaps remain unconnected. Existing chart instances receive
live updates so hovering is not reset by every dashboard poll.

```sh
python3 -m unittest discover -s deploy/live-dashboard/tests -v
node --check deploy/live-dashboard/static/app.js
node --check deploy/live-dashboard/static/charts.js
node --test deploy/live-dashboard/tests/test_charts.cjs
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

Sync buffers show current values and the maximum available observation within the
selected period. These are temporary sync reservations and attributed buffers,
not total process memory. Observed maxima are not true high-water marks because
the 15-second sampler can miss short bursts. Missing history remains unavailable.

Processing timings now show per-stage p50/p95 history with independent duration
scales and the shared selected time range. Like crypto history, these are sampled
rolling summaries, not per-block spans. Both percentiles are collected from this
deployment onward without backfilling retained readings into history.

The header and tab icon use the official Zakura flower from
https://zakura.com/zakura-flower-v1.svg, served locally as `static/favicon.svg`.

Timing history requires an increase in the matching histogram observation count
across scrapes within 45 seconds and the same node activation. Unchanged counts,
resets, unknown identity, missing counters, and repeated saves leave gaps. Equal
durations with increasing counts remain distinct observations. Each point is still
a rolling summary and can include multiple new events. Historical timing samples
without observation-count gating are excluded on read, without deleting other data.
