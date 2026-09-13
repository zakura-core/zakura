//! Opt-in allocation observations for synchronous tests on one thread.
//!
//! Install [`TrackingAllocator`] as the test binary's global allocator, then use
//! [`measure`] around the production operation. Other threads and the observer's
//! own bookkeeping are excluded. This is not a process RSS measurement.

#![allow(
    unsafe_code,
    reason = "test allocator delegates pointer operations unchanged to System"
)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::RefCell,
    collections::BTreeMap,
};

/// Allocation requests and live allocations made inside one measured operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AllocationStats {
    /// Successful allocation/reallocation calls.
    pub requests: usize,
    /// Sum of requested sizes, including repeated reallocations.
    pub requested_bytes: usize,
    /// Largest individual requested allocation.
    pub largest_request: usize,
    /// Largest simultaneously live allocation total from this operation.
    pub peak_live_bytes: usize,
    /// Bytes from this operation still allocated when it returned.
    pub retained_bytes: usize,
}

#[derive(Default)]
struct Observation {
    allocations: BTreeMap<usize, usize>,
    stats: AllocationStats,
}

thread_local! {
    static ACTIVE: RefCell<Option<Observation>> = const { RefCell::new(None) };
}

fn observe(operation: impl FnOnce(&mut Observation)) {
    let _ = ACTIVE.try_with(|active| {
        // Allocating/freeing the bookkeeping map reenters the allocator. Ignore
        // that operation without borrowing recursively or attributing it to SUT.
        if let Ok(mut active) = active.try_borrow_mut() {
            if let Some(observation) = active.as_mut() {
                operation(observation);
            }
        }
    });
}

fn allocated(pointer: *mut u8, size: usize) {
    if pointer.is_null() {
        return;
    }
    observe(|observation| {
        // usize can represent this platform's pointer address. It is never dereferenced.
        observation.allocations.insert(pointer as usize, size);
        let stats = &mut observation.stats;
        stats.requests += 1;
        stats.requested_bytes += size;
        stats.largest_request = stats.largest_request.max(size);
        stats.retained_bytes += size;
        stats.peak_live_bytes = stats.peak_live_bytes.max(stats.retained_bytes);
    });
}

fn freed(pointer: *mut u8) {
    observe(|observation| {
        // usize preserves the pointer address solely as an allocation identity.
        if let Some(size) = observation.allocations.remove(&(pointer as usize)) {
            observation.stats.retained_bytes -= size;
        }
    });
}

/// System allocator with opt-in thread-local observations.
pub struct TrackingAllocator;

// SAFETY: Every pointer/layout operation is delegated to the System allocator.
// Bookkeeping never dereferences, replaces, or changes the lifetime of a pointer.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplied a valid allocation layout.
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout.size());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplied a valid allocation layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout.size());
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        freed(pointer);
        // SAFETY: The pointer/layout came from this delegated allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: The pointer/layout and new size satisfy GlobalAlloc's contract.
        let replacement = unsafe { System.realloc(pointer, layout, size) };
        if !replacement.is_null() {
            freed(pointer);
            allocated(replacement, size);
        }
        replacement
    }
}

/// Observe allocations made by a synchronous operation on the calling thread.
///
/// Initialize lazy fixtures before calling this function. Do not move measured
/// allocations across threads or await inside the operation. Nested measurement
/// is rejected. A panic ends the observation before it propagates.
pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, AllocationStats) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.with_borrow_mut(|active| *active = None);
        }
    }
    ACTIVE.with_borrow_mut(|active| {
        assert!(active.is_none(), "allocation measurements cannot nest");
        *active = Some(Observation::default());
    });
    let reset = Reset;
    let result = operation();
    let stats = ACTIVE.with_borrow(|active| active.as_ref().unwrap().stats);
    drop(reset);
    (result, stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static ALLOCATOR: TrackingAllocator = TrackingAllocator;

    #[test]
    fn measurement_distinguishes_temporary_peak_from_retained_output() {
        let (retained, measured) = measure(|| {
            let temporary = std::hint::black_box(vec![1u8; 8192]);
            let retained = std::hint::black_box(vec![2u8; 1024]);
            drop(temporary);
            retained
        });
        assert_eq!(measured.largest_request, 8192);
        assert_eq!(measured.peak_live_bytes, 8192 + 1024);
        assert_eq!(measured.retained_bytes, 1024);
        assert_eq!(retained.len(), 1024);
        let (_, empty) = measure(|| ());
        assert_eq!(empty, AllocationStats::default());
    }

    #[test]
    fn allocation_observer_recovers_after_a_panicking_operation() {
        assert!(
            std::panic::catch_unwind(|| measure(|| panic!("controlled test failure"))).is_err()
        );
        let (bytes, measured) = measure(|| std::hint::black_box(vec![0u8; 32]));
        assert_eq!(measured.retained_bytes, bytes.capacity());
    }
}
