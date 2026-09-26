//! Reserve a tree's storage before inserting response keys.
//!
//! Rust's BTreeSet does not expose capacity or node allocations. We charge a
//! conservative allowance and retain it until the tree is dropped, including
//! when removals leave an empty root. Cold allocation tests check the allowance
//! against the supported compiler's actual tree growth and key replacement.

use super::ResponseIndex;
use crate::zakura::regulation::{collection_allocation_bytes, MemoryFull, ResponseMemoryPermit};
use std::marker::PhantomData;

// The supported standard library uses at most 11 key slots and 12 child links
// per node. Sixteen slots of each also cover the parent link, counters and
// alignment. A full node per live key, plus one spare node, deliberately
// overcharges sparse trees and temporary node splits without relying on their
// minimum occupancy. Recheck the allocation properties when updating Rust.
const NODE_SLOT_ALLOWANCE: usize = 16;

#[derive(Debug)]
pub(crate) struct ResponseIndexPlan<K> {
    entries: usize,
    bytes: u64,
    key: PhantomData<K>,
}

impl<K> ResponseIndexPlan<K> {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl<K: Copy + Ord> ResponseIndex<K> {
    /// Planning changes neither the tree nor its funding.
    pub(crate) fn plan_capacity(
        &self,
        required: usize,
        geometric: bool,
    ) -> Result<Option<ResponseIndexPlan<K>>, MemoryFull> {
        if required <= self.funded_entries {
            return Ok(None);
        }
        let entries = if geometric {
            required.max(self.funded_entries.saturating_mul(2)).max(4)
        } else {
            required
        };
        let bytes = collection_allocation_bytes::<(K, usize)>(NODE_SLOT_ALLOWANCE)
            .and_then(|keys| {
                keys.checked_add(collection_allocation_bytes::<usize>(NODE_SLOT_ALLOWANCE)?)
            })
            .and_then(|node| node.checked_mul(u64::try_from(entries.checked_add(1)?).ok()?))
            .ok_or(MemoryFull)?;
        Ok(Some(ResponseIndexPlan {
            entries,
            bytes,
            key: PhantomData,
        }))
    }

    /// Keep the old allowance until its replacement is fully funded. Inserting,
    /// consuming or removing keys never has to acquire another permit.
    pub(crate) fn apply_capacity_from(
        &mut self,
        plan: Option<ResponseIndexPlan<K>>,
        funding: &mut Option<ResponseMemoryPermit>,
    ) {
        let Some(plan) = plan else {
            return;
        };
        let next = funding
            .as_mut()
            .expect("index growth is part of the admitted plan")
            .split_off(plan.bytes);
        self.funded_entries = plan.entries;
        self.funding = Some(next);
    }

    #[cfg(test)]
    pub(crate) fn funded_bytes_for_test(&self) -> u64 {
        self.funding.as_ref().map_or(0, ResponseMemoryPermit::bytes)
    }
}
