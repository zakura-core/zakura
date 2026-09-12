//! Registry fixtures without a running request producer.

use super::*;

impl PeerRegistry {
    /// Populate a query fixture without a running request producer.
    pub(in crate::zakura::block_sync) fn set_outstanding(
        &self,
        peer: &ZakuraPeerId,
        generation: u64,
        outstanding: BTreeMap<block::Height, OutstandingMeta>,
    ) {
        let mut peers = self.lock();
        if let Some(entry) = peers
            .get_mut(peer)
            .filter(|entry| entry.generation == generation)
        {
            entry.outstanding.clear();
            for item in outstanding {
                entry.outstanding.push_for_test(item);
            }
        }
    }

    pub(in crate::zakura::block_sync) fn publish_slots(
        &self,
        peer: &ZakuraPeerId,
        generation: u64,
        slots: SlotDiagnostics,
        response_ranges: impl IntoIterator<Item = (block::Height, block::Height)>,
    ) {
        let mut peers = self.lock();
        if let Some(entry) = peers
            .get_mut(peer)
            .filter(|entry| entry.generation == generation)
        {
            entry.slots = slots;
            entry.response_ranges.clear();
            for range in response_ranges {
                entry.response_ranges.push_for_test(range);
            }
            entry.response_ranges.sort_unstable();
        }
    }

    pub(in crate::zakura::block_sync) fn response_capacity_for_test(
        &self,
        peer: &ZakuraPeerId,
    ) -> (usize, usize) {
        self.lock().get(peer).map_or((0, 0), |entry| {
            (
                entry.outstanding.capacity(),
                entry.response_ranges.capacity(),
            )
        })
    }
}
