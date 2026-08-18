# Block Propagation Tracing

Opt-in JSONL tracing that measures how a block moves through first
announcement, full-body receipt, and durable commit across a group of nodes. It
covers both legacy TCP gossip and native Zakura sync.

The block hash is the correlation key. The report supports two origins:

- `broadcast` starts `t0` at `mined_block_broadcast_started` on a controlled
  mining node, after `submitblock` has validated and committed the block. It
  does not measure the interval from the physical miner finding a solution to
  S-NOMP submitting it.
- `first-discovery` starts `t0` at the earliest announcement, body receipt, or
  commit across the supplied observer traces. Use this on Mainnet when the
  mining node is not observable.

Enable it only for a bounded measurement window. Trace files append across
restarts, and wall-clock timestamps are only comparable when node clocks are
synchronized.

## What it records

Every JSONL row includes a Unix wall-clock timestamp (`wall_ts_unix_us`) and a
stable node identity (`ZAKURA_NODE_ID`, then `ZEBRA_NODE_ID`, then hostname).
Do not treat cross-node differences below the known clock uncertainty as
network latency. The report defaults to ±100 ms.

| Phase | Legacy TCP | Native Zakura |
| --- | --- | --- |
| Broadcast origin | `mined_block_broadcast_started` / `_finished` | same local events |
| Announcement | `block_announced` | `header_status_received` |
| Body received | `legacy_block_downloaded` | `block_body_received` |
| Commit | `legacy_block_finished` | `commit_finish` |

An observer may receive the same block through both transports. Duplicate
events are retained so the report can show which path arrived first.

Legacy peer addresses stay redacted unless `network.expose_peer_addresses`
is enabled for that run. Native peer labels stay pseudonymous (`peer:<hex>`).

## Enable tracing

Tracing is off by default. There are two mutually exclusive settings:

```toml
[network.zakura]
block_propagation_trace_dir = "/mnt/data/traces/block-propagation"
```

The dedicated setting writes only `block_propagation.jsonl`. The existing
general `trace_dir` remains available for full Zakura diagnostics and also
contains propagation data for backward compatibility. Zakura rejects startup
if both settings are configured.

### Testnet observers

Deploy with propagation tracing enabled on the selected fleet:

```bash
gh workflow run zakura-testnet-deploy.yml \
  --ref REF \
  -f ref=REF \
  -f p2p_stack=auto \
  -f block_propagation_trace=true
```

The workflow writes each node's traces to:

```text
/mnt/data/traces/block-propagation
```

`block_propagation_trace` can be enabled for the entire selected fleet. The
older `header_sync_trace` input remains restricted to one explicit node.

### Mainnet observers

The Mainnet deploy is binary-only, so the workflow enables narrow tracing with
an owned systemd drop-in rather than rewriting the hand-managed node config or
base unit:

```bash
gh workflow run zakura-mainnet-deploy.yml \
  --ref REF \
  -f ref=REF \
  -f block_propagation_trace=true
```

Leave `node` blank to select the whole fleet, or pass `-f node=NAME` for one
observer. The drop-in sets the dedicated trace directory and a stable
`ZAKURA_NODE_ID`, and writes traces under:

```text
/mnt/data/traces/block-propagation
```

The managed observers are `asia-0`, `us-0`, `us-east-0`, `us-west-0`,
`canada-0`, `europe-west-0`, `europe-central-0`, `asia-south-0`, and
`asia-pacific-0`. Their current host addresses are listed in the
[deployer documentation](../deploy/deployer/README.md#github-actions-mainnet-fleet-deploy).

Confirm the observers are synchronized and their clocks are synchronized
before the measurement. After a new block is committed, obtain its hash from
`getbestblockhash`, the Zakura logs, or a Mainnet explorer. Copy each
observer's trace directory into a separate local directory, then run:

```bash
python3 scripts/zakura-block-propagation-report.py \
  --origin first-discovery \
  --hash BLOCK_HASH \
  --trace-dir us-east-0="$run_dir/us-east-0" \
  --trace-dir europe-west-0="$run_dir/europe-west-0" \
  --trace-dir asia-pacific-0="$run_dir/asia-pacific-0" \
  --clock-uncertainty-ms 100 \
  --json-out "$run_dir/report.json" \
  --markdown-out "$run_dir/report.md"
```

Here, `t0` is the first time any supplied observer recorded an announcement,
body, or commit. The offsets measure spread within the observed fleet, not time
from the miner. They are a lower bound on true network propagation delay
because the miner and unobserved peers are outside the measurement.

### Testnet mining node

On the mining host, use the opt-in Compose override:

```bash
cd docker/mining
docker compose -f docker-compose.yml -f docker-compose.trace.yml up -d
```

The mining node remains legacy TCP-only. Its trace directory is stored in the
`block-propagation-traces` Docker volume and its default trace label is
`zakura-testnet-miner`.

Verify the trace file appears after startup:

```bash
docker compose -f docker-compose.yml -f docker-compose.trace.yml \
  exec zakura sh -c 'ls -l /var/lib/zakura-traces'
```

## Capture a block

Before mining:

1. Confirm the mining node and observers are synchronized at the same tip.
2. Confirm each Linux host reports synchronized time:

   ```bash
   timedatectl show -p NTPSynchronized --value
   ```

3. Record the exact Zakura ref deployed to the mining node and observers.
4. Confirm sufficient free space under `/mnt/data` on observers and in Docker's
   storage directory on the mining host.

Then mine one block:

1. Start the smallest practical Mining Rig Rentals worker.
2. Confirm accepted shares and low reject/stale rates.
3. Stop or redirect the rental after one accepted block.
4. Record the accepted block hash from S-NOMP, the Zakura log, or a Testnet
   explorer.
5. Wait until all observers report the block at their committed tip.

To force a live native-path measurement when natural propagation only
exercises legacy TCP, redeploy one explicit observer with `p2p_stack=zakura`
and `block_propagation_trace=true`, capture one block, then restore that node
with `p2p_stack=auto`.

## Collect traces

Create one local directory per node:

```bash
run_dir="propagation-artifacts/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p \
  "$run_dir/zakura-testnet-miner" \
  "$run_dir/zakura-testnet-1" \
  "$run_dir/zakura-testnet-eu" \
  "$run_dir/zakura-testnet-as"
```

Export the mining trace:

```bash
docker compose -f docker-compose.yml -f docker-compose.trace.yml \
  cp zakura:/var/lib/zakura-traces/. \
  "$run_dir/zakura-testnet-miner/"
```

Copy observer traces from their managed hosts:

```bash
scp -r root@167.99.103.111:/mnt/data/traces/block-propagation/. \
  "$run_dir/zakura-testnet-1/"
scp -r root@164.92.209.78:/mnt/data/traces/block-propagation/. \
  "$run_dir/zakura-testnet-eu/"
scp -r root@206.189.148.0:/mnt/data/traces/block-propagation/. \
  "$run_dir/zakura-testnet-as/"
```

The report partitions warnings by `process_trace_id` and filters events by
block hash, so prior runs can remain in the directory.

## Generate the report

Run the stdlib-only report tool from the repository root:

```bash
python3 scripts/zakura-block-propagation-report.py \
  --hash BLOCK_HASH \
  --trace-dir zakura-testnet-miner="$run_dir/zakura-testnet-miner" \
  --trace-dir zakura-testnet-1="$run_dir/zakura-testnet-1" \
  --trace-dir zakura-testnet-eu="$run_dir/zakura-testnet-eu" \
  --trace-dir zakura-testnet-as="$run_dir/zakura-testnet-as" \
  --native-node zakura-testnet-1=57ad39fad4f0bca46cf1ea831772a99d5027b372fef2be5a0ea68e1b5bb4da49 \
  --native-node zakura-testnet-eu=2bbb907b5d90598ef49f2e637066586b311a64587479be6ed43e8388587fcd2a \
  --native-node zakura-testnet-as=50999835f48f4a048c0e9042e5332844c9673943d7fab1f7e993bae698c27ea3 \
  --clock-uncertainty-ms 100 \
  --json-out "$run_dir/report.json" \
  --markdown-out "$run_dir/report.md"
```

`--legacy-node NAME=IP` resolves legacy edges only for a run that already has
`network.expose_peer_addresses = true`. Do not enable address exposure for
ordinary retained traces. Native peer labels can be resolved with the public
node IDs shown above.

The report contains:

- a globally ordered event timeline in JSON
- first announcement, body receipt, and commit offset per node
- per-node discovery-to-body and discovery-to-commit duration
- first-to-last observer spread for each phase
- inferred managed-node edges when peer labels can be resolved
- duplicate transport paths, missing phases, process restarts, and clock
  warnings

Before using a controlled-miner report for optimization, confirm:

- the mining trace contains `mined_block_broadcast_started` for the hash
- every observer has at least a body-receive and commit event for that hash
- native `block_body_received` and legacy `legacy_block_downloaded` rows
  include the same hash
- no observer reports an unexpected rejection, reorg, or sustained tip
  divergence
- missing or negative offsets are explained by a known path or clock issue

## Disable tracing

On Testnet, redeploy observers with `block_propagation_trace=false`. On the
mining host, restart with only the base Compose file:

```bash
docker compose -f docker-compose.yml -f docker-compose.trace.yml down
docker compose up -d
```

On Mainnet, rerun the deploy workflow with
`block_propagation_trace=false`. This removes only the workflow-owned drop-in
and leaves the hand-managed configuration and base unit untouched.

Keep the exported run directory with the report, exact ref, node
configuration, and clock preflight results. Remove remote or Docker-volume
traces only after confirming the export is complete.
