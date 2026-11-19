# Mempool load harness

An isolated local-genesis testnet that blasts funded shielded transactions at a
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
| `mempool-load-lab.py` | Generates the chain, starts/stops nodes, runs the blast, collects artifacts (per-node config localization and chain seeding are delegated to `kresko localize-fleet` / `kresko seed-local`) |
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
python3 $S/mempool-load-lab.py --lab-dir $LAB $BINS --node-count 3 up --profile v2
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
Giving each node its own `127.0.0.x` lets `kresko localize-fleet` re-point each
generated config at that node's own address and directories without a port
remap. Addresses start at `127.0.0.101`: a dev box or droplet very often already
has something on `127.0.0.1`.

**Per-node config localization and seeding live in Kresko.** The lab used to
rewrite each generated config in Python (`prepare_node_dirs`) and submit the
genesis/premine blocks itself. That work is now `kresko localize-fleet` (binds,
per-node directories, metrics endpoint, internal-miner flag, cleared public
peers, regenerated loopback peer list) and `kresko seed-local` (the
`submitblock` loop), keeping the rewrite colocated with the config generation it
depends on and covered by Kresko's own unit tests. The lab still owns node
process lifecycle (`spawn`/`up`/`down`, port preflight, peering and convergence
gates).

**`up` refuses to start if any port is taken.** Without that check, a node that
fails to bind leaves the lab talking to whatever else owns the port — which in
practice meant submitting blocks to an unrelated node and getting a confusing
`rejected`.

**Every run uses the same chain layout.** Seeding every node with every other
one gave each run a random 3- or 4-link layout (a star in one leg, a 3-hop chain
in the next), so "reached every node" compared layouts, not pools. After
`localize-fleet`, the lab seeds node k with node k-1 only, so the nodes form the
chain 0-1-...-(n-1) and the blaster's node 0 sits at one end. Propagation is
then measured over 1 to n-1 hops every run.

**`up` gates on peering before returning.** A network whose nodes never find
each other still runs, still mines, and still accepts transactions — it just
measures nothing. The gate checks each node's `getpeerinfo` count against the
chain (`1 / 2 / ... / 2 / 1`), which lists inbound and outbound connections, so a
missing or extra link fails `up` instead of skewing the run.

**Kresko's configs are rewritten to main's schema.** Kresko renders configs for
the zakura experiment branch it pins, with custom parameters under
`[network.testnet_parameters]` next to `network = "Testnet"`. Main accepts custom
parameters only as the explicit `[network.network]` table, so `localize-fleet` is
followed by that rewrite, which also drops `post_blossom_pow_target_spacing`.

**The chain runs NU7, like Mainnet.** Main takes each block's target spacing
from the upgrade active at its height, 75 s from Blossom and 25 s from NU7, and
has no key to override it. So `--latest-upgrade` defaults to `nu7`, and
`--block-time-secs` defaults to 25 to match, since Kresko spaces the seeded
blocks by it. NU6.3 closes Orchard to new value, so Kresko blasts Ironwood on
this chain.

**The chain starts at a calibrated difficulty.** NU7 averages difficulty over
102 blocks, so a run of a few dozen blocks stays near its starting difficulty,
and blocks come as fast as the internal miner finds them there. Kresko's
`generate` mode seeds at the easiest difficulty, which gave blocks about every
7 s. The lab uses Kresko's `pow` mode instead, which seeds the chain at a
difficulty sized for `--miner-sol-per-sec` at the 25 s target. The default of
4.0 should give blocks about every 23 s on a c-16 under the default blast. One
miner thread there fits about 5.4 solutions a second plus ~4 s of fixed delay
per block, which Kresko's formula leaves out. If a run's blocks drift, multiply
the rate by (wanted interval - 4 s) / (observed interval - 4 s). Main has no
`pow_start_height`, so the rewrite keeps `disable_pow = true`. Nodes then skip
proof-of-work checks, but the internal miner still solves to the template's
difficulty.

**Each leg runs one node setup.** `up --profile` starts every run node with
`ZAKURA_*` environment overrides for the P2P stack, the mempool implementation
and tx-relay. zakurad applies them on top of the config file, so a collected
`zakura.toml` still shows Kresko's values, and the lab's `started` line lists
what each node got. `legacy` is what Mainnet runs today, legacy P2P and the
classic pool. `v2` is P2P v2 (`dual`, which upgrades every lab peer), tx-relay
and the pool core, which tx-relay needs. By default (`setups: legacy-vs-v2`)
the workflow runs a legacy baseline leg and a v2 target leg on the same commit.

P2P v2 can't announce blocks faster than about one per 15 s for long. Its
block-sync status allows a burst of 22 messages and earns one more every 15 s,
and a new block goes out early only while more than 11 are left. At 7 s blocks
the spare ones ran out after about 20 blocks. From then on a v2 node announced
new blocks once per 15 s, two to five at a time, and each batch reset the next
node's mempool. Legacy nodes announce every block at once. The calibrated
difficulty keeps blocks slower than that, but if a run's blocks come faster
than one per 15 s, a v2 deficit in reach or confirm delay comes from block
relay first.

**Nodes write Zakura traces.** The node logs run at INFO and say nothing about
block-sync requests, so the same overrides also set each run node's
`[network.zakura] trace_dir` to `nodes/<name>/zakura-traces`. `collect` keeps
the `block_sync`, `header_sync`, `chain_tip`, `mempool` and `ratelimit` tables,
which show when each block was requested, committed and applied to the mempool,
including the mempool resets that follow a two-block jump.

**`up` is idempotent.** It skips seeding a node that already holds the chain.
Resubmitting genesis to a node whose tip has moved past it returns a bare
`rejected`, which reads like a chain failure rather than "this already ran".

**The lab sets the peer list at `up` time.** Kresko bakes peer addresses at
genesis time, and a stale list silently produces a 0-peer network. The chain
seeds are derived from the node count and address base on every `up`, after
`localize-fleet` regenerates its own list.

**Config value rewrites are addressed by `section.key`.** The same key name
recurs across sections — `cache_dir` is both the peer cache and the state DB,
`listen_addr` appears three times. A bare-key rewrite hits only the first, which
left every node sharing one RocksDB. `localize-fleet` addresses each by its full
dotted path; the array clears (public seed peers) match by bare key across every
section, exactly as before. See Kresko's `zebra_config::set_toml_values` /
`set_toml_arrays` and their unit tests.

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

The NU7 chain also needs a Kresko that knows the node's `unknown Ironwood anchor`
rejection. An older one neither rescans after it nor classifies it, so the run
counts those races as node rejects.

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

- **Throughput follows blocks mined.** The blaster keeps `ORCHARD_LANES`
  transactions in flight (32 by default), and each waits for a block before its
  lane frees up. So effective tx/s tracks how many blocks land in the window,
  which swings with mining luck. In one A/B with the same miner, the legacy leg
  mined 14 blocks and the v2 leg 7, so v2 "submitted" 44% less. The comparison
  shows a blocks-mined row and reports throughput without grading it. Never
  compare it with `--tx-rate`.
- **Propagation spread** (p50/p95 across nodes) is the figure that moves for
  gossip changes like #341 and #64. Read it against `resolution_secs`, the
  median sampling gap: spreads below one round are unresolvable. Measured p50
  held at ~8s across both 2s and 1s sampling, so it reflects real gossip rather
  than the sampling cadence — but it varies run to run (4-8s observed), which is
  why `baseline_ref` compares two legs on one droplet rather than against a
  stored number.
- **Node rejects vs workload failures.** `unknown_orchard_anchor`, or
  `unknown_ironwood_anchor` from NU6.3, means the blaster built against an anchor
  a new block superseded; Kresko rebuilds and retries. Those are counted as
  `workload_failures` and excluded from the graded reject rate — one of them in
  a 22-submission run is 4.35%, near enough the 5% threshold to fail a healthy
  PR.
- **Failed verifications come with a reason.** The backpressure table shows a
  `mempool_failed_verify_tasks_total{reason="..."}` row per reason, summed over
  nodes. `samples.jsonl` keeps them per node.
- **Reject rate is only graded above `--min-graded-submissions`** (default 50),
  for the same reason.
- A run is `failed` on panics, zero submissions, missing traces, graded rejects
  over threshold, tip divergence, a chain that never advanced, a node that
  stopped answering RPC, no transaction reaching a second node, or no Prometheus
  metrics at all. `degraded` means clean but too thin to conclude from.
- `mempool_rejected_transaction_ids` is the size of the rejected-ID cache, not a
  count of rejected workload transactions. Read rejects from the txblast traces.
