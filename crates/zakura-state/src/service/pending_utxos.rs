//! Pending UTXO tracker for [`AwaitUtxo` requests](crate::Request::AwaitUtxo).

use std::{collections::HashMap, future::Future};

use tokio::sync::broadcast;

use zakura_chain::transparent;

use crate::{BoxError, Response};

/// The fewest `queue` calls between scans for abandoned waiters.
///
/// A scan costs the live waiters plus the calls since the last scan, so it runs after at
/// least as many calls as the live waiters it kept. This
/// keeps the cost of each call constant. Live waiters stay: verification owners bound them.
const MIN_QUEUES_BETWEEN_PRUNES: usize = 1_024;

#[derive(Debug, Default)]
pub struct PendingUtxos {
    waiting: HashMap<transparent::OutPoint, broadcast::Sender<transparent::Utxo>>,
    queued_since_prune: usize,
    live_after_prune: usize,
}

impl PendingUtxos {
    /// Returns a future that will resolve to the `transparent::Output` pointed
    /// to by the given `transparent::OutPoint` when it is available.
    pub fn queue(
        &mut self,
        outpoint: transparent::OutPoint,
    ) -> impl Future<Output = Result<Response, BoxError>> {
        self.queued_since_prune = self.queued_since_prune.saturating_add(1);
        if self.queued_since_prune >= self.live_after_prune.max(MIN_QUEUES_BETWEEN_PRUNES) {
            self.prune();
        }
        let mut receiver = self
            .waiting
            .entry(outpoint)
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(1);
                sender
            })
            .subscribe();

        async move {
            receiver
                .recv()
                .await
                .map(Response::Utxo)
                .map_err(BoxError::from)
        }
    }

    /// Notify all requests waiting for the [`transparent::Utxo`] pointed to by
    /// the given [`transparent::OutPoint`] that the [`transparent::Utxo`] has
    /// arrived.
    #[inline]
    pub fn respond(&mut self, outpoint: &transparent::OutPoint, utxo: transparent::Utxo) {
        if let Some(sender) = self.waiting.remove(outpoint) {
            // Adding the outpoint as a field lets us cross-reference
            // with the trace of the verification that made the request.
            tracing::trace!(?outpoint, "found pending UTXO");
            let _ = sender.send(utxo);
        }
    }

    /// Check the list of pending UTXO requests against the supplied
    /// [`transparent::OrderedUtxo`] index.
    pub fn check_against_ordered(
        &mut self,
        ordered_utxos: &HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
    ) {
        for (outpoint, ordered_utxo) in ordered_utxos.iter() {
            self.respond(outpoint, ordered_utxo.utxo.clone())
        }
    }

    /// Scan the set of waiting utxo requests for channels where all receivers
    /// have been dropped and remove the corresponding sender.
    pub fn prune(&mut self) {
        self.waiting.retain(|_, chan| chan.receiver_count() > 0);
        self.queued_since_prune = 0;
        self.live_after_prune = self.waiting.len();
    }

    /// Returns the number of utxos that are being waited on.
    pub fn len(&self) -> usize {
        self.waiting.len()
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    use zakura_chain::transaction;

    #[test]
    fn queue_removes_abandoned_waiters_between_periodic_prunes() {
        let mut pending = PendingUtxos::default();
        let live = pending.queue(transparent::OutPoint {
            hash: transaction::Hash([1; 32]),
            index: 0,
        });
        for index in 0..4 * MIN_QUEUES_BETWEEN_PRUNES {
            drop(pending.queue(transparent::OutPoint {
                hash: transaction::Hash([0; 32]),
                index: u32::try_from(index).expect("test indexes fit u32"),
            }));
        }
        assert!(pending.len() <= MIN_QUEUES_BETWEEN_PRUNES);
        assert!(
            pending.waiting.contains_key(&transparent::OutPoint {
                hash: transaction::Hash([1; 32]),
                index: 0,
            }),
            "a scan keeps live waiters"
        );
        drop(live);
    }
}
