//! Count allocations separately from the uninstrumented latency benchmark.
#![allow(missing_docs)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use zakura_network::zakura::{bench_work_request, WorkRequestEnd};

struct CountedSystem;

static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn allocated(bytes: usize) {
    CALLS.fetch_add(1, Relaxed);
    BYTES.fetch_add(bytes, Relaxed);
    let live = LIVE.fetch_add(bytes, Relaxed) + bytes;
    PEAK.fetch_max(live, Relaxed);
}

// SAFETY: Each method forwards the unchanged allocation contract to System.
#[allow(unsafe_code, reason = "allocation measurement requires GlobalAlloc")]
unsafe impl GlobalAlloc for CountedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies a valid layout.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies a valid layout.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The caller supplies the live pointer and its allocation layout.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: The caller supplies the live pointer, old layout and valid size.
        let next = unsafe { System.realloc(ptr, layout, size) };
        if !next.is_null() {
            LIVE.fetch_sub(layout.size(), Relaxed);
            allocated(size);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: CountedSystem = CountedSystem;

#[allow(
    clippy::print_stdout,
    reason = "this benchmark emits JSON measurements"
)]
fn main() {
    let repeats = 1_000;
    for count in [1, 16, 128] {
        for end in [
            WorkRequestEnd::Unpublished,
            WorkRequestEnd::Queued,
            WorkRequestEnd::Partial,
            WorkRequestEnd::Received,
            WorkRequestEnd::Reset,
        ] {
            // Initialize lazy process state before taking counter differences.
            bench_work_request(count, 4096, end);
            let calls = CALLS.load(Relaxed);
            let bytes = BYTES.load(Relaxed);
            let live = LIVE.load(Relaxed);
            PEAK.store(live, Relaxed);
            for _ in 0..repeats {
                bench_work_request(count, 4096, end);
            }
            let calls = CALLS.load(Relaxed) - calls;
            let bytes = BYTES.load(Relaxed) - bytes;
            let peak = PEAK.load(Relaxed) - live;
            assert_eq!(
                LIVE.load(Relaxed),
                live,
                "the workload retains no allocation"
            );
            println!(
                "{{\"end\":\"{end:?}\",\"count\":{count},\"repeats\":{repeats},\"calls\":{calls},\"allocated_bytes\":{bytes},\"peak_live_bytes\":{peak}}}"
            );
        }
    }
}
