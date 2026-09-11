//! Observe terminal-handler consumption separately from outstanding-body counts.

use super::*;

impl PeerRegistry {
    pub(in crate::zakura::block_sync) fn observe_exchange_for_test(
        &self,
        peer: &ZakuraPeerId,
        generation: u64,
        published: bool,
    ) {
        if let Some(entry) = self
            .lock()
            .get_mut(peer)
            .filter(|entry| entry.generation == generation)
        {
            if published {
                entry.exchange_counts.0 += 1;
            } else {
                entry.exchange_counts.1 += 1;
            }
        }
    }

    pub(in crate::zakura::block_sync) fn exchange_counts_for_test(&self) -> (u64, u64) {
        self.lock()
            .values()
            .fold((0, 0), |(requests, endings), entry| {
                (
                    requests + entry.exchange_counts.0,
                    endings + entry.exchange_counts.1,
                )
            })
    }
}
