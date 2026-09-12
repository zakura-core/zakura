//! Retain funding for collection capacity independently of its live entries.

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use super::{
    collection_allocation_bytes, response_memory::ResponseMemoryPermit, ResponseAdmissionError,
};

/// Owns only the backing allocation. Allocations inside each T need their own owners.
#[derive(Debug)]
pub(crate) struct ResponseVec<T> {
    values: Vec<T>,
    // Drop backing storage before releasing its funding.
    funding: Option<ResponseMemoryPermit>,
}

#[derive(Debug)]
pub(crate) struct CapacityPlan<T> {
    capacity: usize,
    bytes: u64,
    element: PhantomData<T>,
}

impl<T> Default for ResponseVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CapacityPlan<T> {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl<T> ResponseVec<T> {
    pub(crate) fn new() -> Self {
        Self {
            values: Vec::new(),
            funding: None,
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.values.capacity()
    }

    /// Plan without allocating. Try geometric growth first and exact growth
    /// when the combined request and capacity plan cannot fit.
    pub(crate) fn plan_capacity(
        &self,
        additional: usize,
        geometric: bool,
    ) -> Result<Option<CapacityPlan<T>>, ResponseAdmissionError> {
        let required = self
            .len()
            .checked_add(additional)
            .ok_or(ResponseAdmissionError::MemoryFull)?;
        if required <= self.capacity() {
            return Ok(None);
        }
        let capacity = if geometric {
            required.max(self.capacity().saturating_mul(2)).max(4)
        } else {
            required
        };
        let bytes =
            collection_allocation_bytes::<T>(capacity).ok_or(ResponseAdmissionError::MemoryFull)?;
        Ok(Some(CapacityPlan {
            capacity,
            bytes,
            element: PhantomData,
        }))
    }

    /// Both old and new allocations stay charged while entries move. Failure
    /// leaves the current entries, capacity, and funding intact.
    pub(crate) fn apply_capacity(
        &mut self,
        plan: Option<CapacityPlan<T>>,
        funding: Option<ResponseMemoryPermit>,
    ) -> Result<(), ResponseAdmissionError> {
        let Some(plan) = plan else {
            assert!(funding.is_none(), "unchanged capacity needs no new funding");
            return Ok(());
        };
        let funding = funding.expect("capacity growth was admitted with its allocation plan");
        assert_eq!(
            funding.bytes(),
            plan.bytes,
            "funding covers the planned backing allocation"
        );
        assert!(
            plan.capacity >= self.len(),
            "the growth plan retains every existing entry"
        );
        let mut next = Vec::new();
        next.try_reserve_exact(plan.capacity)
            .map_err(|_| ResponseAdmissionError::MemoryFull)?;
        assert_eq!(
            next.capacity(),
            plan.capacity,
            "the global allocator preserves exact requested vector capacity"
        );
        next.append(&mut self.values);
        let old = std::mem::replace(&mut self.values, next);
        drop(old);
        self.funding = Some(funding);
        Ok(())
    }

    /// Push only after the allocation plan has been funded and applied.
    pub(crate) fn push(&mut self, value: T) {
        assert!(
            self.len() < self.capacity(),
            "capacity is funded before publishing an entry"
        );
        self.values.push(value);
    }

    pub(crate) fn remove(&mut self, index: usize) -> T {
        self.values.remove(index)
    }
}

impl<T> Deref for ResponseVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}

impl<T> DerefMut for ResponseVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}

impl<'a, T> IntoIterator for &'a ResponseVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut ResponseVec<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter_mut()
    }
}

#[cfg(test)]
mod tests;
