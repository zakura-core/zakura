//! Release-mode benchmark for non-finalized transparent indexes under output-heavy blocks.
//!
//! Reports the time and heap of pushing stuffed blocks onto several chains, finalizing
//! them on every chain while a published snapshot shares the chains, and querying the
//! address index.

#![allow(clippy::print_stdout)]
#![allow(unsafe_code)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_state::{AllocationProbe, TransparentIndexBenchmarkConfig};

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: This wrapper delegates every allocation operation to `System` without
// changing its pointer or layout contract.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        // SAFETY: The caller provides the layout required by `GlobalAlloc`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        // SAFETY: The pointer and layout came from the delegated allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        grow(size);
        // SAFETY: The pointer and layout came from the delegated allocator.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .expect("benchmark settings are positive integers")
        })
        .unwrap_or(default)
}

fn main() {
    let config = TransparentIndexBenchmarkConfig {
        stuffed_blocks: env_usize("ZAKURA_TRANSPARENT_INDEX_BENCH_BLOCKS", 50),
        outputs_per_block: env_usize("ZAKURA_TRANSPARENT_INDEX_BENCH_OUTPUTS", 60_000),
        address_pool: env_usize("ZAKURA_TRANSPARENT_INDEX_BENCH_ADDRESSES", 0),
        side_chains: env_usize("ZAKURA_TRANSPARENT_INDEX_BENCH_SIDE_CHAINS", 9),
        finalizations: env_usize("ZAKURA_TRANSPARENT_INDEX_BENCH_FINALIZATIONS", 3),
        query_samples: env_usize("ZAKURA_TRANSPARENT_INDEX_BENCH_QUERY_SAMPLES", 200),
    };
    let template: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
        .zcash_deserialize_into()
        .expect("the historical block vector deserializes");
    let probe = AllocationProbe {
        live_bytes: || LIVE_BYTES.load(Ordering::Relaxed),
        peak_live_bytes: || PEAK_LIVE_BYTES.load(Ordering::Relaxed),
        reset_peak: || PEAK_LIVE_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed),
    };

    println!("{config:?}");
    for sample in zakura_state::benchmark_transparent_indexes(template, config, probe) {
        println!(
            "{:<44} {:>14} ns  retained {:>8.1} MiB  peak extra {:>8.1} MiB",
            sample.name,
            sample.nanos,
            sample.retained_bytes as f64 / 1_048_576.0,
            sample.peak_extra_bytes as f64 / 1_048_576.0,
        );
    }
}
