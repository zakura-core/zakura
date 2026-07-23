# Mempool load harness

An isolated local-genesis testnet that blasts funded Orchard transactions at a
small fleet of zakurad nodes and grades the result: throughput, propagation
latency, backpressure, and tip convergence.

It exists to answer "did this mempool change regress anything?" for work like
zakura-core/zakura#342, #341, and #64 — load- and backpressure-dependent paths
that unit tests do not exercise.

The chain is synthetic: local genesis, a fresh network magic per run, premine
funds, and no public peers. No Mainnet state and no real value are involved.

## Pieces

| File | Role |
| --- | --- |
| `mempool-load-lab.py` | Generates the chain, rewrites per-node configs, seeds/starts/stops nodes, runs the blast, collects artifacts |
| `mempool-load-monitor.py` | Samples every node, derives the numbers, writes `summary.json` / `summary.md`, sets the verdict |
| `mempool-load-compare.py` | Renders a baseline-vs-target regression table |
| `mempool-load-run.sh` | Droplet entrypoint used by `zakura-mempool-load.yml` |
| `test_mempool_load.py` | Unit tests (`python3 test_mempool_load.py`) |

All Python is stdlib-only, matching `deploy/deployer/deploy.py`.

CI runs the same `mempool-load-lab.py` code path as a local rehearsal, so a
green local run exercises what CI will.

## Local rehearsal

Build both binaries. The features are not optional: `internal-miner` produces
the blocks that let the workload drain, and `prometheus` exposes the mempool
backpressure counters, which exist nowhere else.

```bash
cargo build --release --features internal-miner,prometheus --package zakura --bin zakurad

# Kresko builds against the zakura crates directly; no patch needed.
git clone https://github.com/valargroup/kresko /tmp/kresko
cd /tmp/kresko
cargo build --release
```

Then drive the lab:

```bash
LAB=/tmp/mempool-lab
S=.github/workflows/scripts
BINS="--zakurad-binary target/release/zakurad --kresko-binary /tmp/kresko/target/release/kresko"

python3 $S/mempool-load-lab.py --lab-dir $LAB $BINS --node-count 3 \
  genesis --orchard-lanes-per-miner 24
python3 $S/mempool-load-lab.py --lab-dir $LAB $BINS --node-count 3 up
python3 $S/mempool-load-lab.py --lab-dir $LAB $BINS --node-count 3 \
  blast --tx-rate 5 --duration-secs 900 &
python3 $S/mempool-load-monitor.py --lab-dir $LAB --node-count 3 \
  --duration-secs 180 --out /tmp/mempool-out
python3 $S/mempool-load-lab.py --lab-dir $LAB collect --out /tmp/mempool-out
python3 $S/mempool-load-lab.py --lab-dir $LAB down
```

A healthy run reaches `[shielded] steady state ready_lanes=N`, then
`submitted=N, errors=0`, with a non-zero mempool on every node.

## Design notes

Each of these encodes a failure a real run actually produced.

**Nodes bind distinct loopback addresses, not distinct ports.** Kresko fixes one
p2p/RPC port per host (18233/18232) because it assumes one node per machine.
Giving each node its own `127.0.0.x` lets Kresko's generated configs and peer
lists be used almost as-is. Addresses start at `127.0.0.101`: a dev box or
droplet very often already has something on `127.0.0.1`.

**`up` refuses to start if any port is taken.** Without that check, a node that
fails to bind leaves the lab talking to whatever else owns the port — which in
practice meant submitting blocks to an unrelated node and getting a confusing
`rejected`.

**`up` gates on peering before returning.** A network whose nodes never find
each other still runs, still mines, and still accepts transactions — it just
measures nothing. The gate is on *total* connections (`>= n-1`), not per node:
`getpeerinfo` reports a connection from the dialling side, so a healthy 3-node
lab routinely reads `2 / 0 / 1`.

**`up` is idempotent.** It skips seeding a node that already holds the chain.
Resubmitting genesis to a node whose tip has moved past it returns a bare
`rejected`, which reads like a chain failure rather than "this already ran".

**`prepare_node_dirs` owns the peer list.** Kresko bakes peer addresses at
genesis time. Regenerating them at start time keeps `up` correct if the node
count or address base changes; a stale list silently produces a 0-peer network.

**Config rewrites are addressed by `section.key`.** The same key name recurs
across sections — `cache_dir` is both the peer cache and the state DB,
`listen_addr` appears three times. A bare-key rewrite hits only the first, which
left every node sharing one RocksDB.

**Teardown escalates and reports.** `down` walks SIGINT → SIGTERM → SIGKILL and
keeps any PID it could not kill in `pids.json`, exiting non-zero. The blaster is
signalled as a process group: signalling only the Python wrapper orphaned the
`kresko` child, which then submitted into the next A/B leg.

## Version coupling with Kresko

Kresko renders the node config from `ZakuradConfig::default()` of **the zakura
version Kresko itself links** (pinned in its `Cargo.toml`), and zakura's config
is `#[serde(deny_unknown_fields)]`.

So the ref under test must be new enough to understand that schema. A node
older than Kresko's pin dies at startup with, for example:

```
Configuration error: unknown field `expose_peer_addresses`
```

This matters most for `baseline_ref`: an old baseline can fail here while the
target ref is fine. `mempool-load-lab.py` recognises this failure and says so
rather than leaving you with the raw field name.

Fix by testing a newer ref, or by repinning Kresko's zakura dependency and
updating `kresko_ref`.

## Artifact safety

Premine spending keys never leave the box, even though they are worthless off
the throwaway chain. Three independent controls:

1. `COLLECTED_PATHS` is an explicit allowlist — never the lab directory wholesale.
2. Collection sanitizes `config.json` and refuses any file whose *content*
   carries key material. This matters: `kresko genesis` writes every funded
   key's `secret_key_hex`, and the bootstrap treasury key, into `config.json` —
   a filename no name-based check would ever flag.
3. The workflow re-scans collected output and fails before uploading anything.

`test_mempool_load.py` asserts all three.

## Interpreting a run

`summary.md` carries the verdict; `summary.json` is the machine-readable form
and the input to `mempool-load-compare.py`.

- **Throughput** is bounded by Orchard proving, not by the mempool. Effective
  tx/s runs well under `--tx-rate` on a small box (~0.2 tx/s for 3 nodes here);
  compare legs against each other, never against the requested rate.
- **Propagation spread** (p50/p95 across nodes) is the figure that moves for
  gossip changes like #341 and #64. Read it against `resolution_secs`, the
  median sampling gap: spreads below one round are unresolvable. Measured p50
  held at ~8s across both 2s and 1s sampling, so it reflects real gossip rather
  than the sampling cadence — but it varies run to run (4-8s observed), which is
  why `baseline_ref` compares two legs on one droplet rather than against a
  stored number.
- **Node rejects vs workload failures.** `unknown_orchard_anchor` means the
  blaster built against an anchor a new block superseded; Kresko rebuilds and
  retries. Those are counted as `workload_failures` and excluded from the graded
  reject rate — one of them in a 22-submission run is 4.35%, near enough the 5%
  threshold to fail a healthy PR.
- **Reject rate is only graded above `--min-graded-submissions`** (default 50),
  for the same reason.
- A run is `failed` on panics, zero submissions, missing traces, graded rejects
  over threshold, tip divergence, a chain that never advanced, a node that
  stopped answering RPC, no transaction reaching a second node, or no Prometheus
  metrics at all. `degraded` means clean but too thin to conclude from.
- `mempool_rejected_transaction_ids` is the size of the rejected-ID cache, not a
  count of rejected workload transactions. Read rejects from the txblast traces.
