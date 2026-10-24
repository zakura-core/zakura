# bbtree evaluation

Experimental only. Production dependencies and behavior are unchanged.

The standalone workspace pins XiangpengHao/bbtree at
`8adae97a0ed97e5c98fa2790312eb0aec9719846` (package 0.1.5).
It compares `std::collections::BTreeMap` with `bbtree::unsync::BBTreeMap`.
The concurrent map is intentionally excluded: its API and consistency contract
are not a substitute for the existing exclusive-mutation maps.

## Scope

`build.rs` compiles two copies of the actual
`crates/zakura-state/src/service/non_finalized_state/chain/height_map.rs`, changing
only the outer map import and the Height import. This exercises the existing
snapshot/push, lookup, range, tip, finalization, and rollback benchmark bodies.
A local `Height(u32)` has the same representation and derived numeric ordering;
serialization, consensus validation, the state service, and RocksDB are absent.
This is a real index implementation benchmark, **not a node throughput result**.
The isolation avoids adding an unproven dependency to the production lockfile.

The standalone map comparison covers seeded insertion, hit/miss lookup, forward
and reverse ordered iteration, short ranges, clone/removal, and a mixed workload.
Destruction and cloning are included only in operations whose names say so.
Map results are nanoseconds per complete workload. Height results are the existing
benchmark's nanoseconds per operation, each the median of 21 inner samples.
Nine outer repetitions alternate backend order. A warm-up precedes each map
measurement. Tiny-map samples are especially sensitive to timer noise.

Allocation counts and cumulative requested bytes are collected in a separate
untimed map pass through a forwarding global allocator. They include clone/drop
work where named, exclude fixture construction, and are **not peak memory or RSS**.
Timing still includes the allocator's disabled counting-flag check.

## Reproduce

Use Rust 1.97.1, the committed lockfile, one otherwise idle machine, and identical
flags for both implementations. Do not set `target-cpu=native` for just one side.

```sh
cd experiments/bbtree
cargo +1.97.1 run --release --locked -- --check
cargo +1.97.1 run --release --locked > results/raw.csv
```

The fixed shuffle seed is `0x5eed_cafe_1234_abcd`. Map sizes are 16, 256, 4096,
and 65536 entries; non-finalized height windows are 64, 1000, and 4096.
These bracket small indexes and stress windows, not measured live distributions.
The deterministic differential check runs before every benchmark invocation.

## Status

Source review and benchmark preparation complete; compilation and measurements
pending. Do not adopt or fork based on upstream marketing numbers.

The ordinary `.github/workflows/benchmarks.yml` comparison is enabled with the
`C-benchmark` PR label. It covers existing production benchmarks, not this isolated
workspace. Paid droplet workflows are deliberately not dispatched.
