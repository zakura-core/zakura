//! Internal helpers for deterministic owned-memory sizing.

use std::mem::size_of;

use crate::BoundedVec;

/// Reports heap allocations owned below an inline value.
///
/// Implementations exclude `size_of::<Self>()`; the owner of a value accounts
/// for its inline storage.
pub(crate) trait DeepOwnedSize {
    fn deep_owned_size_bytes(&self) -> u64;
}

/// Returns the inline size of `T`, saturating if `usize` is wider than `u64`.
pub(crate) fn inline_size_bytes<T>() -> u64 {
    u64::try_from(size_of::<T>()).unwrap_or(u64::MAX)
}

/// Returns the bytes reserved by a `Vec` allocation.
pub(crate) fn vec_capacity_bytes<T>(values: &Vec<T>) -> u64 {
    u64::try_from(values.capacity())
        .unwrap_or(u64::MAX)
        .saturating_mul(inline_size_bytes::<T>())
}

/// Returns the bytes reserved by a `BoundedVec` allocation.
pub(crate) fn bounded_vec_capacity_bytes<T, const LOWER: usize, const UPPER: usize, Witness>(
    values: &BoundedVec<T, LOWER, UPPER, Witness>,
) -> u64 {
    vec_capacity_bytes(values.as_vec())
}
