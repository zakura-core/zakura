# Announcement-claim lab (not for merge)

This lab measures how the equal-work tie-break behaves when two blocks at the same height race through
a network. It is how #2605 (announcement claims), #2618 (claims from legacy `inv`s) and #2630
(claims from the start of a `block` message) were tested. Everything here is lab-only: the knobs and
log lines it needs are marked `LAB ONLY` in the source.

## What it does

Each node is a real `zakurad` in its own Docker container on a private Regtest network. The
controller (`lab.py`) shapes every container's egress with `tc netem` (RTT and bandwidth), blackholes
every pair of nodes that is not an edge of the chosen topology, and drives the nodes over RPC.

A trial:

1. Brings every node to the same tip.
2. Prepares two competing blocks at the next height with `labctl`: one from miner `a`, one from
   miner `b`, of a chosen size.
3. Submits `a`'s block, then `b`'s block a chosen offset later, each to its own miner.
4. Waits for the network to settle and records which block every relay ended on.
5. Reads each relay's `LAB claim_drawn` and `LAB claim_resolved` log lines to record how each
   block's claim was drawn (`header`, `inv` or `block`) and resolved (`used`, `expired`, `voided`),
   with its age when the body arrived.
6. Checks the trial is valid (links shaped as asked, no stuck node, host not overloaded) and
   reconciles the network back to one tip.

A cell repeats until it has `--min-valid` valid trials. Every row of the output CSV carries `valid`
and `invalid_reason`.

## Lab-only changes to the node

- `crates/zakura-consensus/src/block/receipt/claims.rs`
  - `lab_knob(name)` reads a number from `$CLAIM_LAB_DIR/<name>` on every call, so the controller
    can change a running node.
  - `window_ms` overrides the 2 s claim deadline. Setting it to 0 on the relays turns the rule
    off, which is the "claims off" arm of every comparison.
  - `LAB claim_drawn` is logged when a claim is drawn: a header claim, the first `inv`, or a
    reserved `block` message.
- `crates/zakura-consensus/src/block/receipt/claims/resolution.rs`
  - `LAB claim_resolved` is logged when a body resolves its claim.
- `crates/zakurad/src/components/inbound.rs` and
  `crates/zakurad/src/commands/start/zakura/block_range_source.rs`
  - `body_delay_ms` makes a node sit on a body before serving it, for the withholding
    experiments.
- `crates/zakurad/examples/labctl.rs`
  - Builds the competing Regtest blocks: a block template from a node, padded to a target size
    with zero-fee transactions. `--invalid` overpays the coinbase by one zatoshi, which gives a
    block whose header passes but which fails verification.

## Running it

The host needs Docker and `tc`. Build the node and `labctl` in release mode, and the runtime
image:

```sh
cargo build --release --locked -p zakura --bin zakurad --example labctl
docker build -t zakura-claim-lab:v4 docker/zakura-announcement-lab
export LAB_TARGET=$PWD/target/release LAB_IMAGE=zakura-claim-lab:v4
cd docker/zakura-announcement-lab
```

Bring up a cluster, run a sweep, and tear it down:

```sh
python3 lab.py up 6 --topology line:6 --stack legacy
sleep 200   # let every node finish its first sync
python3 lab.py multihop 6 --label "line,all-legacy" --sizes 2000000 --offsets 450 \
  --rtt 100 --rate 20 --rule-off ";r1+r2+r3+r4" --min-valid 10 --max-trials 14 --pace 40 \
  --out results/pushed-claims/line.csv
python3 lab.py down 6
```

`--rule-off ";r1+r2+r3+r4"` runs two arms: an empty set (claims on everywhere) and the four relays
with the deadline set to 0 (claims off). `--pace` spaces trials so header and peer status refreshes
do not overlap a race.

Other commands (`exp1`, `grid`, `exp2`, `exp3`, `withhold`, `edge`) are described at the top of
`lab.py`. `analyze.py` summarises a results CSV, and `stall_check.py` and `stuck_check.py` check a
run's node logs for stalls.

### Host limits

The lab is heavy. On a shared host:

- Run at most 6 containers. `lab.py up` takes the cluster size as its instance name.
- Keep two cores free. Build with `nice` and `taskset`, and do not build while trials run.
- Blocks over 2 MB need the block and message size limits raised, and 8 MB blocks need
  `LAB_MEMORY=3g`. Those changes are not in this branch.

## How #2605, #2618 and #2630 were tested

Topology `line:6`: `a - r1 - r2 - r3 - r4 - b`, every node legacy-only, 100 ms RTT, a 2 MB block
from `a` and a small block from `b`. Each cell has 10 valid trials, claims on and off.

The `evidence/` folder has the raw CSVs:

- `evidence/legacy-inv-2618.csv`: #2618 alone. It found the regression: with claims on, `a`'s direct
  neighbour `r1` ended on `b` in 0 of 10 trials at 20 Mbit with `b` mined 450 ms later, against 10 of
  10 with claims off. `a` pushes its mined block before its `inv`, so `r1` drew no early claim.
- `evidence/pushed-claims-2630.csv`: #2630 at 9f639f0bb. `r1` stays on `a` in every cell, and the
  `inv` claim gain at `r2` is unchanged.
- `evidence/demo-2630-head.csv`: this branch, built on #2630 at a65b745fc, the regression cell only.
  `r1` stays on `a` in 10 of 10 trials with claims on, against 9 of 10 with claims off.
- `evidence/pushed-claims-2630-06bb2af.csv`: this branch, built on #2630 at 06bb2af4c, every cell of
  the 9f639f0bb run. `r1` stays on `a` in all 60 trials.
- `evidence/pushed-claims-2630-06bb2af-order.csv`: the 50 Mbit cell again with the claims-off arm
  run first. It shows that `a`'s push to `r1` sometimes takes one to three extra RTTs in both arms,
  from the TCP state of the connection that carries it, and that `r2`'s outcome follows it.
