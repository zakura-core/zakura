//! Keep response lookups in step with the live request window.
//!
//! Only the next hash of each range is indexed. Consuming it advances that key,
//! while local deadlines leave it intact. Ending a range removes its keys and
//! moves the last vector entry into its place, avoiding a shift of every range.

use super::{DownloadWindow, OutstandingBlockRange};
use crate::zakura::regulation::{
    CapacityPlan, ResponseAdmissionError, ResponseCreditExceeded, ResponseIndexPlan, ResponseMatch,
    ResponseMemoryPermit,
};

pub(in crate::zakura::block_sync) struct OutstandingCapacityPlan {
    ranges: Option<CapacityPlan<OutstandingBlockRange>>,
    hashes: Option<ResponseIndexPlan>,
    starts: Option<ResponseIndexPlan>,
}

impl OutstandingCapacityPlan {
    pub(in crate::zakura::block_sync) fn bytes(&self) -> Result<u64, ResponseAdmissionError> {
        [
            self.ranges.as_ref().map_or(0, CapacityPlan::bytes),
            self.hashes.as_ref().map_or(0, ResponseIndexPlan::bytes),
            self.starts.as_ref().map_or(0, ResponseIndexPlan::bytes),
        ]
        .into_iter()
        .try_fold(0u64, u64::checked_add)
        .ok_or(ResponseAdmissionError::MemoryFull)
    }
}
use zakura_chain::block;

#[cfg(test)]
mod tests;

impl DownloadWindow {
    /// Fund both lookup keys for every range, even after its last body arrives.
    pub(in crate::zakura::block_sync) fn plan_outstanding_capacity(
        &self,
        geometric: bool,
    ) -> Result<OutstandingCapacityPlan, ResponseAdmissionError> {
        let required = self
            .outstanding
            .len()
            .checked_add(1)
            .ok_or(ResponseAdmissionError::MemoryFull)?;
        Ok(OutstandingCapacityPlan {
            ranges: self.outstanding.plan_capacity(1, geometric)?,
            hashes: self
                .next_response_hashes
                .plan_capacity(required, geometric)?,
            starts: self.response_starts.plan_capacity(required, geometric)?,
        })
    }

    pub(in crate::zakura::block_sync) fn apply_outstanding_capacity(
        &mut self,
        plan: OutstandingCapacityPlan,
        funding: &mut Option<ResponseMemoryPermit>,
    ) -> Result<(), ResponseAdmissionError> {
        self.outstanding.apply_capacity_from(plan.ranges, funding)?;
        self.next_response_hashes
            .apply_capacity_from(plan.hashes, funding);
        self.response_starts
            .apply_capacity_from(plan.starts, funding);
        Ok(())
    }

    pub(in crate::zakura::block_sync) fn push_outstanding(&mut self, range: OutstandingBlockRange) {
        let index = self.outstanding.len();
        self.response_starts
            .insert(range.request.start_height, index);
        if let Some(hash) = range.next_response_hash() {
            self.next_response_hashes.insert(hash.0, index);
        }
        self.outstanding.push(range);
    }

    pub(in crate::zakura::block_sync) fn response_for_hash(
        &self,
        hash: block::Hash,
    ) -> ResponseMatch {
        self.next_response_hashes.find(hash.0)
    }

    /// A rejected byte charge leaves both the credit and lookup key unchanged.
    pub(in crate::zakura::block_sync) fn consume_response(
        &mut self,
        index: usize,
        bytes: u64,
    ) -> Result<(), ResponseCreditExceeded> {
        let range = &mut self.outstanding[index];
        let previous = range.next_response_hash();
        range.response.consume(1, bytes)?;
        if let Some(hash) = previous {
            self.next_response_hashes.remove(hash.0, index);
        }
        if let Some(hash) = range.next_response_hash() {
            self.next_response_hashes.insert(hash.0, index);
        }
        Ok(())
    }

    pub(in crate::zakura::block_sync) fn remove_outstanding(
        &mut self,
        index: usize,
    ) -> OutstandingBlockRange {
        let range = self.outstanding.swap_remove(index);
        self.response_starts
            .remove(range.request.start_height, index);
        if let Some(hash) = range.next_response_hash() {
            self.next_response_hashes.remove(hash.0, index);
        }
        // swap_remove moved the former last range. Repair just its two keys.
        if let Some(moved) = self.outstanding.get(index) {
            let previous_index = self.outstanding.len();
            self.response_starts
                .remove(moved.request.start_height, previous_index);
            self.response_starts
                .insert(moved.request.start_height, index);
            if let Some(hash) = moved.next_response_hash() {
                self.next_response_hashes.remove(hash.0, previous_index);
                self.next_response_hashes.insert(hash.0, index);
            }
        }
        range
    }

    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn clear_outstanding(&mut self) {
        self.outstanding.clear();
        self.next_response_hashes.clear();
        self.response_starts.clear();
    }
}

impl OutstandingBlockRange {
    fn next_response_hash(&self) -> Option<block::Hash> {
        let consumed = usize::try_from(self.response.consumed_objects())
            .expect("consumption cannot exceed the bounded request count");
        self.request
            .expected_blocks
            .get(consumed)
            .map(|expected| expected.hash)
    }
}
