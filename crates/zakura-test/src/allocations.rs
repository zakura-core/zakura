//! Check how much memory a function requests during a test.
//!
//! A small message can claim to contain many transactions. Tests use this helper
//! to check whether the decoder reserves memory for data that is missing. It
//! records the largest request, the most memory held at once, and the memory
//! still held when the function returns.
//!
//! For example, a function that holds an 8 KiB temporary buffer and a 1 KiB result
//! at the same time, then frees the temporary buffer, has a 9 KiB peak and leaves
//! 1 KiB allocated at return.
//!
//! To use this helper, install [`TrackingAllocator`] as the test program's
//! allocator, the component that handles memory requests. It passes those
//! requests to Rust's system allocator and records them while [`measure`] runs.
//! Only requests on the calling thread count. Other threads and the helper's
//! own records are excluded. This does not measure the whole program's memory.

// Rust requires `unsafe` to implement its raw memory allocation interface.
// This wrapper lets System allocate and free memory. It records addresses and
// sizes but never reads or writes the allocated memory itself.
#![allow(
    unsafe_code,
    reason = "Rust's allocator interface requires unsafe raw memory operations"
)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::RefCell,
    collections::BTreeMap,
};

/// Memory requested during one call to [`measure`], including temporary buffers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AllocationStats {
    /// Number of successful requests to allocate or resize memory.
    pub requests: usize,
    /// Sum of requested sizes. Resizing a buffer counts its full new size again.
    pub requested_bytes: usize,
    /// Largest number of bytes requested in a single call.
    pub largest_request: usize,
    /// Most bytes from this operation allocated at the same time.
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
        // Updating our records can itself allocate or free memory. Skip those
        // requests so the helper does not count its own memory use.
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

/// Handle the test program's memory requests and record them during [`measure`].
///
/// Install this once with `#[global_allocator]`. Actual allocation and freeing
/// still use [`System`], whether or not a measurement is active.
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

/// Run a function and return both its result and its memory measurements.
///
/// Set up test input and initialize any shared fixtures before measuring, so
/// their memory requests do not count as part of the function being tested.
/// The function must run entirely on this thread, without `await` or moving its
/// allocations to another thread. Calling `measure` inside another `measure`
/// panics, as does calling it in a program that never installed
/// [`TrackingAllocator`]. If the measured function panics, recording stops
/// before it propagates.
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
    // The hooks above only run when `TrackingAllocator` is this program's
    // allocator. Record one deliberate allocation to prove they do: without them
    // every field below stays zero, so a bound like "this requested no memory"
    // would hold for a program that never measured anything. Checking the
    // recorded count must not borrow `ACTIVE`, because `observe` skips its work
    // while the cell is already borrowed and the probe would look unrecorded.
    const PROBE_BYTES: usize = 64;
    let probe = std::hint::black_box(Vec::<u8>::with_capacity(PROBE_BYTES));
    let tracking = ACTIVE.with_borrow(|active| {
        active
            .as_ref()
            .is_some_and(|observation| observation.stats.requests > 0)
    });
    drop(probe);
    assert!(
        tracking,
        "install `TrackingAllocator` with #[global_allocator] to measure allocations"
    );
    ACTIVE.with_borrow_mut(|active| *active = Some(Observation::default()));
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
