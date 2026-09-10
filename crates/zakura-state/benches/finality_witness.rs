//! Release-mode RocksDB benchmark for bounded headers-only finality witnesses.

#![allow(clippy::print_stdout)]

use std::env;

fn percentile(values: &[u128], numerator: usize, denominator: usize) -> u128 {
    let index = values.len().saturating_sub(1).saturating_mul(numerator) / denominator;
    values[index]
}

fn main() {
    let advances = env::var("ZAKURA_FINALITY_BENCH_ADVANCES")
        .map(|value| value.parse().expect("advance count must be a u32"))
        .unwrap_or(65_540_u32);
    let depth = env::var("ZAKURA_FINALITY_BENCH_DEPTH")
        .map(|value| value.parse().expect("finality depth must be a u32"))
        .unwrap_or(1_000_u32);
    let genesis = zakura_chain::block::genesis::regtest_genesis_block();
    let report = zakura_state::benchmark_finality_witness(genesis, advances, depth)
        .expect("the finality witness benchmark completes");
    let mut latencies: Vec<_> = report
        .samples
        .iter()
        .map(|sample| sample.elapsed.as_nanos())
        .collect();
    latencies.sort_unstable();
    let ordinary = report
        .samples
        .iter()
        .find(|sample| sample.advance > 1 && sample.history_rows < 65_536)
        .expect("the benchmark has a pre-eviction sample");
    let post_eviction = report.samples.iter().find(|sample| sample.advance > 65_536);
    println!(
        "{{\"advances\":{advances},\"depth\":{depth},\"median_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"startup_ns\":{},\"ordinary_reads\":{},\"ordinary_writes\":{},\"ordinary_batch_bytes\":{},\"one_block_reorg_ns\":{},\"one_block_reorg_reads\":{},\"one_block_reorg_writes\":{},\"one_block_reorg_batch_bytes\":{},\"bounded_reorg_ns\":{},\"bounded_reorg_reads\":{},\"bounded_reorg_writes\":{},\"bounded_reorg_batch_bytes\":{},\"post_eviction_ns\":{},\"final_history_rows\":{},\"final_witness_rows\":{}}}",
        percentile(&latencies, 1, 2),
        percentile(&latencies, 95, 100),
        percentile(&latencies, 99, 100),
        report.startup_elapsed.as_nanos(),
        ordinary.witness_point_reads,
        ordinary.witness_row_writes,
        ordinary.batch_bytes,
        report.one_block_reorg.elapsed.as_nanos(),
        report.one_block_reorg.witness_point_reads,
        report.one_block_reorg.witness_row_writes,
        report.one_block_reorg.batch_bytes,
        report.bounded_reorg.elapsed.as_nanos(),
        report.bounded_reorg.witness_point_reads,
        report.bounded_reorg.witness_row_writes,
        report.bounded_reorg.batch_bytes,
        post_eviction.map_or(0, |sample| sample.elapsed.as_nanos()),
        report.samples.last().map_or(0, |sample| sample.history_rows),
        report.samples.last().map_or(0, |sample| sample.witness_rows),
    );
}
