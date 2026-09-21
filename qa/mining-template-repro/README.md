# getblocktemplate withhold repro harness

Reproduces, on a local mining network, the condition in
[#1080](https://github.com/zakura-core/zakura/issues/1080): `getblocktemplate`
returning `template parent changed; retry` instead of work, which miners treat as
lost work.

The harness is a measurement tool, not a test suite. Nothing here runs in CI.

## Why three levers

`get_block_template` has five exits that can withhold work: two silent `continue`s
in the long-poll loop, and three error returns in `finish_mining_template`. #1080
attributes the behavior to equal-height non-finalized reorgs. The levers were built
to test that attribution rather than assume it, and they show the reorg is neither
necessary nor by itself sufficient.

- **Lever A** — one PoW-disabled node, one serialized block producer, many long-poll
  clients. No fork can exist, so every withhold observed is an ordinary forward tip
  advance. `GEN_JOBS=4` raises the block rate and lets natural same-height races
  happen too.
- **Lever B** — a deterministic equal-height reorg, with a matched control. Two
  sibling blocks are built on one parent; equal height means equal work, so
  `Chain::cmp` breaks the tie on raw internal tip-hash bytes. Submitting the
  lesser-hash sibling first forces the best chain sideways, and submitting the
  greater-hash one first does not. Both orders deliver the same two blocks to the
  same node under the same load, so a difference between them isolates the reorg.
- **Lever C** — two nodes on Regtest, each running the internal Equihash solver,
  mining independent chains while partitioned and then joined with `addnode`.

## Requirements

- A `zakurad` release build. Lever C additionally needs `--features internal-miner`,
  which is not a default feature; without it the miner task is a no-op.
- The workspace MSRV is 1.97. If the default toolchain is older, prefix cargo
  commands with a newer toolchain, for example `cargo +1.98.0`.
- `python3` and `curl`.

## Running

```bash
cargo build --release -p zakura --features internal-miner

cd qa/mining-template-repro
                        ./run/lever-a.sh   # forward advance, no reorg
GEN_JOBS=4              ./run/lever-a.sh   # high block rate, natural races
POLL_CLIENTS=24 ROUNDS=6 ./run/lever-b.sh  # deterministic flip plus control
CYCLES=3                ./run/lever-c.sh   # two mining nodes
```

Set `ZAKURAD` to compare two builds:

```bash
ZAKURAD=/path/to/other/zakurad ./run/compare-fix.sh some-label
```

`compare-fix.sh` runs the high-block-rate workload and the deterministic flip
against one binary and writes a single JSON summary, including RPC latency
percentiles and the node's CPU seconds. Retrying a superseded template is not free,
so a change that removes withholds needs to be checked for what it spends instead.

Everything lands in `out/`, which is ignored.

## Attributing a withhold to a specific exit

Client-visible behavior needs no instrumentation: the poller records every error
response, and the Lever B driver reports how many in-flight templates were served
versus withheld.

To attribute a withhold to one of the five exits, apply `instrumentation.patch`,
which adds a `tracing::warn!` and a labeled counter at each one, a log at the
otherwise silent `CheckBlockProposalValidity` rejection, and a log at both chain
channel publication points. It is a measurement patch and is deliberately not
applied to the source tree.

```bash
git apply qa/mining-template-repro/instrumentation.patch
cargo build --release -p zakura --features internal-miner
cd qa/mining-template-repro && ./run/lever-a.sh
```

`run/analyze.py` then classifies each withhold by exit and by whether the best-tip
watch was at the same height as the template's parent (`sideways`, an equal-height
reorg), behind it (`watch_behind`, the gap between the two publication points), or
ahead (`watch_ahead`, an ordinary advance). It also reads same-height best-tip
changes straight off the publication sequence, which is ground truth for "a reorg
happened" independent of what the RPC did.

Note that the probe labels a withhold by priority when several conditions hold at
once, so it does not resolve which disjunct of a compound check fired.

## Results

See [RESULTS.md](RESULTS.md) for the measurements these levers produced on `main`,
on the pre-#1074 code, and on #1088.

## Layout

```text
configs/   node configs: Regtest PoW-off for A and B, two peered miners for C
driver/    Rust: builds sibling blocks, orders them by raw hash, submits them
poller/    long-poll storm client
run/       lever scripts, shared helpers, analyzer, build comparison
```
