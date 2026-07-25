# CPU profiling and block-processing latency

The [Perf bench workflow](../.github/workflows/zakura-perf-bench.yml) captures CPU flamegraphs and block latency on throwaway DigitalOcean droplets. `historical_sync` restores the baked `sandblast` state for fixed-height throughput and optional parallel A/B. `live_head` restores the baked pruned tip, catches up without profiling, then records real network-head traffic for a fixed window using the production default P2P stack. Profiling runs as a sidecar, so the measured binary is unchanged.

## Trigger a run

From the Actions UI (`Perf bench (ephemeral droplets)` → Run workflow) or the CLI:

```bash
gh workflow run zakura-perf-bench.yml -f ref=my-branch
```

Useful input combinations:

- `-f workload=live_head` — single-leg 60-minute observational profile at real mainnet head. Mainnet's production default currently resolves to the legacy P2P stack. The gate requires the verified body tip to match the available header frontier for consecutive samples, at least three live peers, and a fresh estimated tip; the run fails if those conditions are lost for 30 seconds during capture. `baseline_ref` is rejected in this mode; use `head_profile_minutes` for a shorter smoke run.
- `-f baseline_ref=main` — A/B: both refs bench simultaneously on identical droplets, and a compare job adds the blocks/s speedup, a per-function CPU self-share diff table, and a differential flamegraph (`flamegraph-diff.svg`).
- `-f workload=historical_semantic` — the fixed state range with full script and proof verification. `historical_checkpoint` is the default bulk-sync workload.
- `-f droplet_size=c-32` — more cores per leg; `-f profile=off` — plain bench, no profiling sidecar.
- `-f teardown_after_run=false` — keep the droplets up for SSH inspection (the hourly reaper removes them within 24h; they are tagged `zakura-pr-node`).

Every Monday two scheduled runs profile `main` as standing baselines: 05:17 UTC in `checkpoint` mode and 06:47 UTC in `semantic` mode. Review them under Actions → Perf bench.

## Read the results

Each leg appends its result, CPU digest, absolute CPU counters, and block-latency digest to the run summary. Historical A/B runs also add the comparison and bottleneck verdict. Live-head summaries record the baked tip, catch-up time, exact start/end tips and hashes, profile duration, and committed blocks without claiming a speedup. Download `zakura-perf-bench-<run>-primary` and open `flamegraph.svg` in a browser, or run `gh run download <run-id> -n zakura-perf-bench-<run-id>-primary -D out`.

| file | what it is |
| --- | --- |
| `flamegraph.svg` | sampled CPU flamegraph; open it in a browser, click to zoom |
| `profile.folded` | folded stacks; re-render or diff locally with [inferno](https://github.com/jonhoo/inferno) |
| `perf-stat.csv` / `.md` | absolute task clock, cycles, instructions, and instructions per cycle |
| `latency.md` / `.json` | per-block commit latency (p50/p90/p99/max, slowest heights, stalls) + per-stage pipeline timings |
| `metrics-start.prom` / `metrics-final.prom` | live-head measurement boundary snapshots; stage timings use their delta |
| `samples.csv` / `samples.jsonl` | height-over-time samples; historical runs also include the recorded metrics series |
| `zakura-traces.tar.zst` | raw Zakura JSONL traces when the selected stack emits them (`commit_state.jsonl`, `block_sync.jsonl`, ...) |
| `meta.json`, `verdict.json` | machine-readable leg result + bottleneck verdict |

Interpretation notes:

- Per-block latency comes from the `commit_state.jsonl` trace (`commit_start` → `commit_finish` around the verifier commit). In `checkpoint` mode a block's latency includes waiting for its checkpoint range to fill, so high p99 there is batching, not slow verification; `semantic` mode is true per-block verify+commit latency.
- Stage timings use cumulative Prometheus histograms for historical sync and start/end deltas for live head. The exporter's rolling-window quantiles are deliberately omitted.
- Production-default live-head runs use JSON-RPC for health samples, omit the metrics recorder, and scrape Prometheus only at the measurement boundaries, so exporter rendering does not distort the CPU profile. The experimental Zakura stack still uses its metrics-only header frontier.
- Historical profiling starts after the first committed block and defaults to 300 seconds. Live-head profiling starts only after the head gate and defaults to 60 minutes. Sampling uses hardware `cycles` when available, else `cpu-clock`, at 49 Hz with DWARF unwinding.
- Both legs of an A/B fetch from the public P2P network concurrently, so residual noise is peer-delivery variance; identical droplet specs remove the hardware variance a shared host cannot.

## Knobs

Workflow inputs cover the common cases; the droplet-side script (`.github/workflows/scripts/perf-bench-run.sh`) documents the finer-grained profiling and live-head gate knobs. The A/B summary table is rendered by `.github/workflows/scripts/perf-bench-compare.py`, which also decides whether the legs are comparable at all (a leg whose `zakurad` exited non-zero, or produced no readable `meta.json`, suppresses the comparison and the CPU diff).

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
