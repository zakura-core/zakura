//! Keep response lookups in step with the live request window.
//!
//! Only the next hash of each range is indexed. Consuming it advances that key,
//! while local deadlines leave it intact. Ending a range removes its keys and
//! moves the last vector entry into its place, avoiding a shift of every range.

use super::{DownloadWindow, OutstandingBlockRange};
use crate::zakura::regulation::{ResponseCreditExceeded, ResponseMatch};
use zakura_chain::block;

#[cfg(test)]
mod tests;

impl DownloadWindow {
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
