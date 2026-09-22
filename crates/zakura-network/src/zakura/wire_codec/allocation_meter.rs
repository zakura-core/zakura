//! A test-only allocator that measures heap bytes requested by one closure.
//!
//! The meter wraps the system allocator for the whole test binary. It counts
//! only on a thread that is inside [`measure_allocated_bytes`], so parallel
//! tests do not disturb each other's counts.
#![allow(unsafe_code)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

thread_local! {
    static METERING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
}

struct AllocationMeter;

impl AllocationMeter {
    fn record(bytes: usize) {
        // `try_with` fails only while the thread's locals are being destroyed;
        // the meter is off then, so skipping the count is correct.
        let _ = METERING.try_with(|metering| {
            if metering.get() {
                let _ = ALLOCATED_BYTES
                    .try_with(|allocated| allocated.set(allocated.get().saturating_add(bytes)));
            }
        });
    }
}

// SAFETY: Every method delegates to `System` with the caller's arguments, so
// the allocator upholds the same contract as `System`. Recording touches only
// const-initialized thread locals, which never allocate.
unsafe impl GlobalAlloc for AllocationMeter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::record(layout.size());
        // SAFETY: The caller upholds `GlobalAlloc::alloc`'s contract for `layout`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::record(layout.size());
        // SAFETY: The caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from `System` through this allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        Self::record(size.saturating_sub(layout.size()));
        // SAFETY: The pointer and layout came from `System` through this allocator.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: AllocationMeter = AllocationMeter;

/// Run `operation` and return its result with the heap bytes it requested on
/// this thread. A growing reallocation counts only its growth.
pub(crate) fn measure_allocated_bytes<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATED_BYTES.with(|allocated| allocated.set(0));
    METERING.with(|metering| metering.set(true));
    let result = operation();
    METERING.with(|metering| metering.set(false));
    let allocated = ALLOCATED_BYTES.with(Cell::get);
    (result, allocated)
}
