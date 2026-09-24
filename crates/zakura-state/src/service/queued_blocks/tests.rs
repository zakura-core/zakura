//! Tests for block queues.

#![allow(clippy::unwrap_in_result)]

mod vectors;

impl super::QueuedBlocks {
    /// Returns the first queued body for state-service test inspection.
    pub(in crate::service) fn get_mut(
        &mut self,
        hash: &zakura_chain::block::Hash,
    ) -> Option<&mut super::QueuedSemanticallyVerified> {
        self.blocks
            .get_mut(hash)
            .and_then(|variants| variants.first_mut())
    }
}
