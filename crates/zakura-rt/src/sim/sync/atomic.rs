//! Coordination atomics, simulator build.
//!
//! Each type is std's atomic with a preemption point before every operation.
//! The run is single-threaded, so operations are sequentially consistent
//! whatever `Ordering` the code passes. Weak-memory behavior is out of scope
//! (`runtime.md`, Appendix B). Metrics-only atomics belong in
//! [`counter`](super::counter), which has no points.

use std::{fmt, panic::Location, sync::atomic as std_atomic};

pub use std_atomic::Ordering;

use crate::sim::{point, Op};

/// Defines a wrapper around a std integer atomic.
macro_rules! atomic_int {
    ($(#[$doc:meta])* $name:ident, $int:ty) => {
        $(#[$doc])*
        #[derive(Default)]
        #[repr(transparent)]
        pub struct $name(std_atomic::$name);

        impl $name {
            /// A new atomic holding `value`.
            pub const fn new(value: $int) -> Self {
                $name(std_atomic::$name::new(value))
            }

            /// The value, through a unique borrow. Not a point.
            pub fn get_mut(&mut self) -> &mut $int {
                self.0.get_mut()
            }

            /// The value, consuming the atomic.
            pub fn into_inner(self) -> $int {
                self.0.into_inner()
            }

            /// Reads the value.
            #[track_caller]
            pub fn load(&self, order: Ordering) -> $int {
                point(Op::AtomicLoad, Location::caller());
                self.0.load(order)
            }

            /// Writes `value`.
            #[track_caller]
            pub fn store(&self, value: $int, order: Ordering) {
                point(Op::AtomicStore, Location::caller());
                self.0.store(value, order);
            }

            /// Writes `value` and returns the old one.
            #[track_caller]
            pub fn swap(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.swap(value, order)
            }

            /// Writes `new` if the value is `current`.
            #[track_caller]
            pub fn compare_exchange(
                &self,
                current: $int,
                new: $int,
                success: Ordering,
                failure: Ordering,
            ) -> Result<$int, $int> {
                point(Op::AtomicRmw, Location::caller());
                self.0.compare_exchange(current, new, success, failure)
            }

            /// Like `compare_exchange`. It never fails spuriously here,
            /// because the run is single-threaded.
            #[track_caller]
            pub fn compare_exchange_weak(
                &self,
                current: $int,
                new: $int,
                success: Ordering,
                failure: Ordering,
            ) -> Result<$int, $int> {
                point(Op::AtomicRmw, Location::caller());
                self.0.compare_exchange(current, new, success, failure)
            }

            /// Adds `value` and returns the old value. Wraps on overflow.
            #[track_caller]
            pub fn fetch_add(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.fetch_add(value, order)
            }

            /// Subtracts `value` and returns the old value. Wraps on overflow.
            #[track_caller]
            pub fn fetch_sub(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.fetch_sub(value, order)
            }

            /// Bitwise and with `value`. Returns the old value.
            #[track_caller]
            pub fn fetch_and(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.fetch_and(value, order)
            }

            /// Bitwise or with `value`. Returns the old value.
            #[track_caller]
            pub fn fetch_or(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.fetch_or(value, order)
            }

            /// Bitwise xor with `value`. Returns the old value.
            #[track_caller]
            pub fn fetch_xor(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.fetch_xor(value, order)
            }

            /// Keeps the larger of the value and `value`. Returns the old value.
            #[track_caller]
            pub fn fetch_max(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.fetch_max(value, order)
            }

            /// Keeps the smaller of the value and `value`. Returns the old
            /// value.
            #[track_caller]
            pub fn fetch_min(&self, value: $int, order: Ordering) -> $int {
                point(Op::AtomicRmw, Location::caller());
                self.0.fetch_min(value, order)
            }

            /// Applies `f` until it returns `None` or its result is stored.
            /// One point, since nothing else runs between attempts.
            #[track_caller]
            pub fn fetch_update<F>(
                &self,
                set_order: Ordering,
                fetch_order: Ordering,
                f: F,
            ) -> Result<$int, $int>
            where
                F: FnMut($int) -> Option<$int>,
            {
                point(Op::AtomicRmw, Location::caller());
                // Newer std deprecates `fetch_update` for `try_update`,
                // which behaves the same.
                self.0.try_update(set_order, fetch_order, f)
            }

            /// std's `try_update`: [`fetch_update`](Self::fetch_update) by
            /// its newer name.
            #[track_caller]
            pub fn try_update<F>(
                &self,
                set_order: Ordering,
                fetch_order: Ordering,
                f: F,
            ) -> Result<$int, $int>
            where
                F: FnMut($int) -> Option<$int>,
            {
                point(Op::AtomicRmw, Location::caller());
                self.0.try_update(set_order, fetch_order, f)
            }
        }

        impl From<$int> for $name {
            fn from(value: $int) -> Self {
                $name::new(value)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

atomic_int!(
    /// std's `AtomicU8`, with preemption points.
    AtomicU8,
    u8
);
atomic_int!(
    /// std's `AtomicU16`, with preemption points.
    AtomicU16,
    u16
);
atomic_int!(
    /// std's `AtomicU32`, with preemption points.
    AtomicU32,
    u32
);
atomic_int!(
    /// std's `AtomicU64`, with preemption points.
    AtomicU64,
    u64
);
atomic_int!(
    /// std's `AtomicUsize`, with preemption points.
    AtomicUsize,
    usize
);
atomic_int!(
    /// std's `AtomicI32`, with preemption points.
    AtomicI32,
    i32
);
atomic_int!(
    /// std's `AtomicI64`, with preemption points.
    AtomicI64,
    i64
);
atomic_int!(
    /// std's `AtomicIsize`, with preemption points.
    AtomicIsize,
    isize
);

/// std's `AtomicBool`, with preemption points.
#[derive(Default)]
#[repr(transparent)]
pub struct AtomicBool(std_atomic::AtomicBool);

impl AtomicBool {
    /// A new atomic holding `value`.
    pub const fn new(value: bool) -> Self {
        AtomicBool(std_atomic::AtomicBool::new(value))
    }

    /// The value, through a unique borrow. Not a point.
    pub fn get_mut(&mut self) -> &mut bool {
        self.0.get_mut()
    }

    /// The value, consuming the atomic.
    pub fn into_inner(self) -> bool {
        self.0.into_inner()
    }

    /// Reads the value.
    #[track_caller]
    pub fn load(&self, order: Ordering) -> bool {
        point(Op::AtomicLoad, Location::caller());
        self.0.load(order)
    }

    /// Writes `value`.
    #[track_caller]
    pub fn store(&self, value: bool, order: Ordering) {
        point(Op::AtomicStore, Location::caller());
        self.0.store(value, order);
    }

    /// Writes `value` and returns the old one.
    #[track_caller]
    pub fn swap(&self, value: bool, order: Ordering) -> bool {
        point(Op::AtomicRmw, Location::caller());
        self.0.swap(value, order)
    }

    /// Writes `new` if the value is `current`.
    #[track_caller]
    pub fn compare_exchange(
        &self,
        current: bool,
        new: bool,
        success: Ordering,
        failure: Ordering,
    ) -> Result<bool, bool> {
        point(Op::AtomicRmw, Location::caller());
        self.0.compare_exchange(current, new, success, failure)
    }

    /// Like `compare_exchange`. It never fails spuriously here, because the
    /// run is single-threaded.
    #[track_caller]
    pub fn compare_exchange_weak(
        &self,
        current: bool,
        new: bool,
        success: Ordering,
        failure: Ordering,
    ) -> Result<bool, bool> {
        point(Op::AtomicRmw, Location::caller());
        self.0.compare_exchange(current, new, success, failure)
    }

    /// Logical and with `value`. Returns the old value.
    #[track_caller]
    pub fn fetch_and(&self, value: bool, order: Ordering) -> bool {
        point(Op::AtomicRmw, Location::caller());
        self.0.fetch_and(value, order)
    }

    /// Logical or with `value`. Returns the old value.
    #[track_caller]
    pub fn fetch_or(&self, value: bool, order: Ordering) -> bool {
        point(Op::AtomicRmw, Location::caller());
        self.0.fetch_or(value, order)
    }

    /// Logical xor with `value`. Returns the old value.
    #[track_caller]
    pub fn fetch_xor(&self, value: bool, order: Ordering) -> bool {
        point(Op::AtomicRmw, Location::caller());
        self.0.fetch_xor(value, order)
    }

    /// Logical nand with `value`. Returns the old value.
    #[track_caller]
    pub fn fetch_nand(&self, value: bool, order: Ordering) -> bool {
        point(Op::AtomicRmw, Location::caller());
        self.0.fetch_nand(value, order)
    }
}

impl From<bool> for AtomicBool {
    fn from(value: bool) -> Self {
        AtomicBool::new(value)
    }
}

impl fmt::Debug for AtomicBool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
