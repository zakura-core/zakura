//! Release-mode benchmark for the non-finalized chain's height indexes.

#![allow(clippy::print_stdout)]

use std::env;

fn main() {
    let heights = env::var("ZAKURA_HEIGHT_MAP_BENCH_HEIGHTS")
        .map(|value| value.parse().expect("height count must be a u32"))
        .unwrap_or(1_000_u32);
    let samples = env::var("ZAKURA_HEIGHT_MAP_BENCH_SAMPLES")
        .map(|value| value.parse().expect("sample count must be a usize"))
        .unwrap_or(200_usize);
    println!("heights={heights} samples={samples}");
    for sample in zakura_state::benchmark_height_map(heights, samples) {
        println!("{:<26} {:>10.1} ns/op", sample.name, sample.nanos_per_op);
    }
}
