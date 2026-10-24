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
RUSTFLAGS='' cargo +1.97.1 build --release --locked
taskset -c 0 target/release/zakura-bbtree-experiment > results/raw.csv
python3 summarize.py results/raw.csv > results/summary.md
```

The fixed shuffle seed is `0x5eed_cafe_1234_abcd`. Map sizes are 16, 256, 4096,
and 65536 entries; non-finalized height windows are 64, 1000, and 4096.
These bracket small indexes and stress windows, not measured live distributions.
The deterministic differential check runs before every benchmark invocation.

## Results and recommendation

**Do not adopt for HeightMap; a fork is not justified by this evidence.**
At 1000 heights, snapshot/push is 18% slower with Arc values and 49% slower with
32-byte roots. Finalization is 3.38x and 5.25x slower, respectively; lookup is
27% slower. Tip access improves about 26–30%, and rollback about 3%, but those
wins do not offset the measured regressions without a very different workload.
The outer index has only about 16 chunks at 1000 heights; large-flat-map results
are a poor predictor of this copy-on-write workload.

At 65536 flat u64 keys with 32-byte values, bbtree reduces median insertion time
by 21%, hit/miss lookup by 46%, mixed workload by 31%, and full iteration by 20%.
Removal including cloning is 7% faster, but repeated short ranges are 36% slower.
Insertion uses 4009 versus 8758 allocation calls; requested bytes are only 1.3%
lower (4.04 versus 4.09 MB). Cloning workloads request about 2.2% more bytes.
There is no demonstrated 30% memory saving here, and no whole-node speedup claim.

See [all timings and variance](results/summary.md), [raw samples](results/raw.csv),
[environment and source hashes](results/environment.json), and
[compatibility/inventory review](COMPATIBILITY.md). On this shared AMD EPYC/KVM
machine, large-map lookup CV is 10.5%/16.7% (std/bbtree); some tiny measurements
and height scans are substantially noisier. These are descriptive samples,
not confidence intervals or independent-machine replication. CPU frequency and
host contention were not controlled. Default generic x86_64 code was used.

A future experiment could target a genuinely large, lookup-heavy private index,
with an observed workload distribution. This PR supplies no evidence for a
wholesale migration, native-SIMD deployment, or maintaining a fork.

## Validation

Rust 1.97.1 release build and Clippy with warnings denied passed. The executable
passed 20000 deterministic differential operations, dense chunk boundary/range
checks on empty and populated indexes (all bound kinds and invalid ranges),
snapshot isolation, borrowed String lookup, owned Vec values, duplicate handling,
set uniqueness/order, and a Send/Sync compile check. The range check covers 1458
bound combinations plus seven finalization boundaries. These checks run before
each timing invocation; they are not cargo-test harness tests.

Both source variants run the repository's existing index benchmark unchanged.
The standalone binary disables its cargo-test target: the included production
module's test-only dependencies are intentionally not linked. The full state
crate test suite, upstream fuzz/Miri/Verus suites, other architectures and a
complete node benchmark were not run. IDE diagnostics are unavailable here;
compiler and Clippy checks cover the edited Rust files. Formatting, Markdown
lint, changelog validation and diff checks passed.

The ordinary `.github/workflows/benchmarks.yml` comparison is enabled with the
`C-benchmark` PR label. It covers existing production benchmarks, not this isolated
workspace. Paid droplet workflows are deliberately not dispatched.
