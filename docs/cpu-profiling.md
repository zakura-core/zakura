# CPU profiling and block-processing latency

How to get a CPU flamegraph and a per-stage latency breakdown of `zakurad` processing real mainnet blocks, with zero local setup. The [Perf bench workflow](../.github/workflows/zakura-perf-bench.yml) runs each measurement on a **throwaway DigitalOcean droplet** booted from the weekly-baked PR-node golden image, with a clone of the baked mainnet `sandblast` state volume (tip 1,707,210) attached — so there is no shared bench host to queue behind, no snapshot download, droplet size is an input, and an A/B comparison runs both refs **in parallel on identical fresh machines**. Profiling is a sidecar (`perf record` attached to the node), so the node binary and the bench numbers are unchanged.

## Trigger a run

From the Actions UI (`Perf bench (ephemeral droplets)` → Run workflow) or the CLI:

```bash
gh workflow run zakura-perf-bench.yml -f ref=my-branch
```

Useful input combinations:

- `-f baseline_ref=main` — A/B: both refs bench simultaneously on identical droplets, and a compare job adds the blocks/s speedup, a per-function CPU self-share diff table, and a differential flamegraph (`flamegraph-diff.svg`).
- `-f verify_mode=semantic` — same state range, but with `consensus.checkpoint_sync = false`, so every block gets full script + proof verification (the mandatory checkpoints end below the volume tip, so the whole range is semantically verified). This is the "block execution" workload; `checkpoint` (default) is the bulk-sync workload.
- `-f droplet_size=c-32` — more cores per leg; `-f profile=off` — plain bench, no profiling sidecar.
- `-f teardown_after_run=false` — keep the droplets up for SSH inspection (the hourly reaper removes them within 24h; they are tagged `zakura-pr-node`).

Every Monday two scheduled runs profile `main` as standing baselines: 05:17 UTC in `checkpoint` mode and 06:47 UTC in `semantic` mode. Review them under Actions → Perf bench.

## Read the results

Each leg appends to the run's **step summary**: a throughput row (blocks/s, post-commit blocks/s, reached-stop), the bottleneck verdict (commit / download / verify), the **block-latency digest**, and the **CPU digest** (share per thread pool: `rayon`, `commit-compute`, `tokio-runtime-w`, the committer thread; then the hottest functions by self time). A/B runs add the compare section. The per-leg **artifacts** (`zakura-perf-bench-<run>-<leg>`) contain:

| file | what it is |
| --- | --- |
| `flamegraph.svg` | sampled CPU flamegraph; open it in a browser, click to zoom |
| `profile.folded` | folded stacks; re-render or diff locally with [inferno](https://github.com/jonhoo/inferno) |
| `latency.md` / `.json` | per-block commit latency (p50/p90/p99/max, slowest heights, stalls) + per-stage pipeline timings |
| `metrics-final.prom` | last full `/metrics` page of the run (cumulative histograms) |
| `samples.csv` / `samples.jsonl` | height-over-time and the recorded metrics series |
| `zakura-traces.tar.zst` | the raw Zakura JSONL traces (`commit_state.jsonl`, `block_sync.jsonl`, ...) |
| `meta.json`, `verdict.json` | machine-readable leg result + bottleneck verdict |

Interpretation notes:

- Per-block latency comes from the `commit_state.jsonl` trace (`commit_start` → `commit_finish` around the verifier commit). In `checkpoint` mode a block's latency includes waiting for its checkpoint range to fill, so high p99 there is batching, not slow verification; `semantic` mode is true per-block verify+commit latency.
- Stage timings are cumulative Prometheus histograms at run end (ops + mean only; the exporter's summary quantiles are rolling-window values and are deliberately not shown): the `commit-metrics` phases (`update_trees`, `commitment_check`, `checkpoint_compute`), the always-on RocksDB batch-commit histogram, batch-verifier durations (`halo2`, `redpallas`, `groth16`, ...), and the sequencer submit queue wait. In `checkpoint` mode with VCT fast sync (the default) the tree/commitment phases never run, so those rows are absent; `semantic` mode populates them.
- The profile window starts when the sync escapes cold start and lasts `PROFILE_SECONDS` (default 300s at 99 Hz, DWARF unwinding). Droplets expose no PMU, so sampling uses the software `cpu-clock` event — equivalent for on-CPU flamegraph purposes. Release binaries already carry `debug = "line-tables-only"` and full `.eh_frame` (`panic = "unwind"`), which is why no special build is needed.
- Both legs of an A/B fetch from the public P2P network concurrently, so residual noise is peer-delivery variance; identical droplet specs remove the hardware variance a shared host cannot.

## Knobs

Workflow inputs cover the common cases; the droplet-side script (`.github/workflows/scripts/perf-bench-run.sh`) documents the finer-grained environment knobs (`PROFILE_SECONDS`, `PROFILE_FREQ`, `PROFILE_DWARF_STACK`, `CKPT_LIMIT`, `DL_LIMIT`, `P2P_STACK`).

## Local profiling recipes

Linux (any box, any running `zakurad`):

```bash
perf record -F 99 --call-graph dwarf,8192 -p "$(pgrep zakurad)" -- sleep 60
perf script | python3 scripts/zakura-bench-digest.py collapse > zakurad.folded
python3 scripts/zakura-bench-digest.py top --folded zakurad.folded          # markdown digest
inferno-flamegraph < zakurad.folded > zakurad.svg                            # cargo install inferno
```

macOS: `perf` does not exist; use [samply](https://github.com/mstange/samply) (`cargo install samply`, then `samply record -p "$(pgrep zakurad)"`), which opens the Firefox Profiler UI locally.

## Related instrumentation (what this is not)

- `checkpoint-sync-bench.yml` is the older fixed-host sync bench on the `zakura-bench` runner (persistent dashboard, warm caches, serialized runs); this lane is the ephemeral, parallel, profiled successor for CPU/latency questions.
- `cargo bench` criterion microbenches (`benchmarks.yml`, `C-benchmark` PR label) time the crypto primitives in isolation; this lane shows their share of a real sync.
- The `deploy/runner/` cohort harness (`make perf-*`) is the deterministic isolated-cohort deep-dive with per-phase commit attribution.
- The `flamegraph` cargo feature (`tracing.flamegraph` config) renders span wall-time, not sampled CPU, and needs a special build; prefer this lane for CPU questions.
- `zakura-mempool-load.yml` and `zakura-pr-node.yml` cover mempool throughput and long-running real-node behavior on the same droplet chassis.
