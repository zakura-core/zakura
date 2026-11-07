//! std's `Weak` with preemption points, simulator build.
//!
//! `upgrade` and the count reads look at the shared reference counts, and
//! code uses them to decide whether an owner is still alive, so each is a
//! point. Make one with [`downgrade`], since `Arc::downgrade` returns std's.

use std::{fmt, panic::Location, sync::Arc};

use crate::sim::{point, Op};

/// std's `Weak`, with preemption points.
pub struct Weak<T: ?Sized>(std::sync::Weak<T>);

/// A weak pointer to `this`, as `Arc::downgrade` makes.
pub fn downgrade<T: ?Sized>(this: &Arc<T>) -> Weak<T> {
    Weak(Arc::downgrade(this))
}

impl<T> Weak<T> {
    /// A weak pointer to nothing, which never upgrades.
    pub const fn new() -> Self {
        Weak(std::sync::Weak::new())
    }
}

impl<T: ?Sized> Weak<T> {
    /// The value, if a strong owner is still alive.
    #[track_caller]
    pub fn upgrade(&self) -> Option<Arc<T>> {
        point(Op::AtomicRmw, Location::caller());
        self.0.upgrade()
    }

    /// Strong owners left.
    #[track_caller]
    pub fn strong_count(&self) -> usize {
        point(Op::AtomicLoad, Location::caller());
        self.0.strong_count()
    }

    /// Weak pointers left, or 0 once no strong owner is.
    #[track_caller]
    pub fn weak_count(&self) -> usize {
        point(Op::AtomicLoad, Location::caller());
        self.0.weak_count()
    }

    /// True if both point to the same allocation.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}

impl<T: ?Sized> Clone for Weak<T> {
    fn clone(&self) -> Self {
        Weak(self.0.clone())
    }
}

impl<T> Default for Weak<T> {
    fn default() -> Self {
        Weak::new()
    }
}

impl<T: ?Sized> fmt::Debug for Weak<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
