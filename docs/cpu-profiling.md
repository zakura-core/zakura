# CPU profiling

## Local profiling recipes

Linux (any box, any running `zakurad`):

```bash
perf record -F 99 --call-graph dwarf,8192 -p "$(pgrep zakurad)" -- sleep 60
perf script | python3 scripts/zakura-bench-digest.py collapse > zakurad.folded
python3 scripts/zakura-bench-digest.py top --folded zakurad.folded          # markdown digest
inferno-flamegraph < zakurad.folded > zakurad.svg                            # cargo install inferno
```

macOS: `perf` does not exist; use [samply](https://github.com/mstange/samply) (`cargo install samply`, then `samply record -p "$(pgrep zakurad)"`), which opens the Firefox Profiler UI locally.

## Related instrumentation

- `cargo bench` criterion microbenches (`benchmarks.yml`, `C-benchmark` PR label) time the crypto primitives in isolation.
- The `deploy/runner/` cohort harness (`make perf-*`) is the deterministic isolated-cohort deep-dive with per-phase commit attribution.
- `zakura-mempool-load.yml` and `zakura-pr-node.yml` cover mempool throughput and long-running real-node behavior on ephemeral droplets.
