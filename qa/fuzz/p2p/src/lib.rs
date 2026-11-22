//! Support shared by the P2P machine fuzz targets. A package needs one target,
//! and this library stays valid before any `[[bin]]` exists.
//!
//! [`MeteredAllocator`] measures what a parse allocates, for the targets that
//! bound a parser's allocations by its input's length.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
};

static METERING: AtomicBool = AtomicBool::new(false);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

/// The system allocator, recording the largest request while
/// [`largest_allocation`] runs. A target installs it as its
/// `#[global_allocator]`; libFuzzer runs each input on one thread.
pub struct MeteredAllocator;

fn record(size: usize) {
    if METERING.load(Relaxed) {
        LARGEST.fetch_max(size, Relaxed);
    }
}

// SAFETY: every call forwards unchanged to `System`, which upholds the
// `GlobalAlloc` contract; recording a size touches only atomics.
unsafe impl GlobalAlloc for MeteredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Runs `run` and returns the largest single allocation it requested, in
/// bytes. It measures only under [`MeteredAllocator`]; otherwise it returns 0.
pub fn largest_allocation(run: &mut dyn FnMut()) -> usize {
    LARGEST.store(0, Relaxed);
    METERING.store(true, Relaxed);
    run();
    METERING.store(false, Relaxed);
    LARGEST.load(Relaxed)
}
