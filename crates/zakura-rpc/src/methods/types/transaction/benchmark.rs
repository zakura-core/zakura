//! Opt-in measurements using public transactions from the template-lab replay fixture.
//!
//! Run this test alone in release mode, with one test thread and
//! TEMPLATE_PAYLOAD_FIXTURE pointing at replay-fixture.json. Fixture loading, transaction
//! decoding, and verification-metadata setup are outside the timed sections.

// This test-only allocator forwards every operation to System. Counting allocations also
// reveals duplicate encodings during concurrent cache misses without production test hooks.
#[allow(unsafe_code)]
mod allocations {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    };

    pub(super) static COUNT: AtomicUsize = AtomicUsize::new(0);
    pub(super) static TRACK: AtomicBool = AtomicBool::new(false);
    pub(super) static BYTES: AtomicUsize = AtomicUsize::new(0);
    pub(super) static LIVE: AtomicUsize = AtomicUsize::new(0);
    pub(super) static FILTER: AtomicUsize = AtomicUsize::new(usize::MAX);
    pub(super) static MATCHES: AtomicUsize = AtomicUsize::new(0);
    struct CountingAllocator;

    fn allocated(size: usize) {
        if !TRACK.load(Relaxed) {
            return;
        }
        COUNT.fetch_add(1, Relaxed);
        BYTES.fetch_add(size, Relaxed);
        LIVE.fetch_add(size, Relaxed);
        if size == FILTER.load(Relaxed) {
            MATCHES.fetch_add(1, Relaxed);
        }
    }

    // SAFETY: layouts and pointers are forwarded unchanged to System; counters do not
    // dereference allocations, allocate memory, or change allocator results.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                allocated(layout.size());
            }
            ptr
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc_zeroed(layout) };
            if !ptr.is_null() {
                allocated(layout.size());
            }
            ptr
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            if TRACK.load(Relaxed) {
                LIVE.fetch_sub(layout.size(), Relaxed);
            }
            unsafe {
                System.dealloc(ptr, layout);
            }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let result = unsafe { System.realloc(ptr, layout, size) };
            if !result.is_null() {
                if TRACK.load(Relaxed) {
                    LIVE.fetch_sub(layout.size(), Relaxed);
                }
                allocated(size);
            }
            result
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;
}

use super::{payload::TransactionDataCache, TransactionTemplate};
use crate::server::jsonrpc::{Id, MethodResponse};
use std::{
    hint::black_box,
    sync::{atomic::Ordering::Relaxed, Arc, Barrier},
    time::Instant,
};
use zakura_chain::{
    amount::NonNegative,
    serialization::{BytesInDisplayOrder, ZcashDeserializeInto, ZcashSerialize},
    transaction::{Transaction, VerifiedUnminedTx},
};

fn templates(
    txs: &[VerifiedUnminedTx],
    cache: &TransactionDataCache,
) -> Vec<TransactionTemplate<NonNegative>> {
    txs.iter()
        .map(|tx| TransactionTemplate::from_cached(tx, cache))
        .collect()
}

fn response(txs: &Vec<TransactionTemplate<NonNegative>>) -> MethodResponse {
    MethodResponse::response(Id::Number(1), &Ok(txs), 16 * 1024 * 1024)
}

fn measure(name: &str, iterations: usize, mut work: impl FnMut()) -> serde_json::Value {
    let start = Instant::now();
    for _ in 0..iterations {
        work();
    }
    let elapsed = start.elapsed();
    // Collect allocation counts in a separate pass so counter contention does not inflate
    // the measured benefit of eliminating allocations.
    let count = allocations::COUNT.load(Relaxed);
    let bytes = allocations::BYTES.load(Relaxed);
    allocations::TRACK.store(true, Relaxed);
    for _ in 0..iterations {
        work();
    }
    allocations::TRACK.store(false, Relaxed);
    let count = allocations::COUNT.load(Relaxed) - count;
    let bytes = allocations::BYTES.load(Relaxed) - bytes;
    serde_json::json!({"stage":name, "iterations":iterations,
        "total_ns":elapsed.as_nanos(), "allocations":count, "allocated_bytes":bytes})
}

#[test]
#[ignore = "requires TEMPLATE_PAYLOAD_FIXTURE and an isolated release-mode benchmark host"]
fn benchmark_transaction_payloads() {
    let path = std::env::var("TEMPLATE_PAYLOAD_FIXTURE").expect("set TEMPLATE_PAYLOAD_FIXTURE");
    let fixture: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let txs: Vec<_> = fixture["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .take(247)
        .map(|record| {
            let bytes =
                crate::methods::hex_data::decode(record["data"].as_str().unwrap().as_bytes())
                    .unwrap();
            let tx: Transaction = bytes.as_slice().zcash_deserialize_into().unwrap();
            assert_eq!(tx.version(), 6);
            assert_eq!(tx.shielded_action_counts().ironwood_actions, 2);
            VerifiedUnminedTx::new(
                tx.into(),
                800u64.try_into().unwrap(),
                0,
                0,
                Arc::new(vec![]),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(txs.len(), 247);
    let selected = &txs[..165];
    let cache = TransactionDataCache::default();
    let warm = templates(selected, &cache);
    let original_json: Vec<serde_json::Value> = selected.iter().map(|tx| serde_json::json!({
        "data": crate::methods::hex_data::encode(tx.transaction.transaction().zcash_serialize_to_vec().unwrap()),
        "hash": hex::encode(tx.transaction.id().mined_id().bytes_in_display_order()),
        "authdigest": hex::encode(tx.transaction.id().auth_digest().unwrap().bytes_in_display_order()),
        "depends": [], "fee":800, "sigops":0, "required":false
    })).collect();
    assert_eq!(
        serde_json::to_value(&warm).unwrap(),
        serde_json::to_value(original_json).unwrap()
    );
    let response_bytes = response(&warm).as_json().get().len();
    // Warm serializer/code pages before collecting any stage.
    for _ in 0..10 {
        black_box(response(&warm));
    }
    let mut stages = vec![
        measure("payload_creation", 30, || {
            for tx in selected {
                black_box(TransactionTemplate::from(tx));
            }
        }),
        measure("cold_build_and_response", 30, || {
            let cache = TransactionDataCache::default();
            black_box(response(&templates(selected, &cache)));
        }),
        measure("warm_response", 150, || {
            black_box(response(&warm));
        }),
        measure("warm_build_and_response", 150, || {
            black_box(response(&templates(selected, &cache)));
        }),
    ];
    stages.push(measure("two_overlapping_builds_and_response", 30, || {
        let cache = TransactionDataCache::default();
        let prime = templates(selected, &cache);
        black_box(prime);
        black_box(response(&templates(&txs[82..247], &cache)));
    }));
    allocations::LIVE.store(0, Relaxed);
    allocations::TRACK.store(true, Relaxed);
    let live_before = allocations::LIVE.load(Relaxed);
    let retained_cache = TransactionDataCache::default();
    let readers = templates(selected, &retained_cache);
    let live_with_cache = allocations::LIVE.load(Relaxed);
    let accounting = format!("{retained_cache:?}");
    drop(retained_cache);
    let live_with_readers = allocations::LIVE.load(Relaxed) - accounting.capacity();
    drop(readers);
    let live_after_readers = allocations::LIVE.load(Relaxed) - accounting.capacity();
    allocations::TRACK.store(false, Relaxed);

    let concurrent_cache = TransactionDataCache::default();
    let barrier = Barrier::new(8);
    allocations::MATCHES.store(0, Relaxed);
    allocations::FILTER.store(2 * txs[0].transaction.size(), Relaxed);
    allocations::TRACK.store(true, Relaxed);
    let concurrent_start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                barrier.wait();
                black_box(serde_json::to_vec(&templates(&txs[..1], &concurrent_cache)).unwrap());
            });
        }
    });
    let concurrent_ns = concurrent_start.elapsed().as_nanos();
    allocations::TRACK.store(false, Relaxed);
    allocations::FILTER.store(usize::MAX, Relaxed);
    let encodings = allocations::MATCHES.load(Relaxed);
    println!(
        "PAYLOAD_BENCH {}",
        serde_json::json!({
            "transactions":165, "response_bytes":response_bytes, "stages":stages,
            "cache_accounting":accounting, "live_before":live_before,
            "live_with_cache":live_with_cache, "live_with_readers":live_with_readers,
            "live_after_readers":live_after_readers,
            "concurrent_threads":8, "concurrent_ns":concurrent_ns,
            "concurrent_hex_allocations":encodings,
        })
    );
}
