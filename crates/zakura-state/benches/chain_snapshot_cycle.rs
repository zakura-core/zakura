//! Release-mode benchmark for the chain copies a node makes for each new block at the tip.
//!
//! Reports the median and mean time of each step: copying the best chain to add a block,
//! adding it, publishing, copying every chain to drop the finalized root, and publishing
//! again. The defaults match a mainnet node at the tip: a 1000-block rollback window and
//! the most side chains a node keeps.

#![allow(clippy::print_stdout)]

use std::{env, sync::Arc};

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_state::ChainSnapshotBenchmarkConfig;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .expect("benchmark settings are non-negative integers")
        })
        .unwrap_or(default)
}

fn main() {
    let config = ChainSnapshotBenchmarkConfig {
        blocks: env_usize("ZAKURA_CHAIN_SNAPSHOT_BENCH_BLOCKS", 1_000),
        outputs_per_block: env_usize("ZAKURA_CHAIN_SNAPSHOT_BENCH_OUTPUTS", 4),
        side_chains: env_usize("ZAKURA_CHAIN_SNAPSHOT_BENCH_SIDE_CHAINS", 9),
        cycles: env_usize("ZAKURA_CHAIN_SNAPSHOT_BENCH_CYCLES", 300),
        cache_eviction_bytes: env_usize("ZAKURA_CHAIN_SNAPSHOT_BENCH_EVICTION_BYTES", 64 << 20),
    };
    let template: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
        .zcash_deserialize_into()
        .expect("the historical block vector deserializes");

    println!("{config:?}");
    for sample in zakura_state::benchmark_chain_snapshot_cycle(template, config) {
        println!(
            "{:<38} median {:>9} ns  mean {:>9} ns",
            sample.name, sample.median_nanos, sample.mean_nanos,
        );
    }
}
