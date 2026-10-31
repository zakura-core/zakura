//! Pending [`transparent::Output`] tracker for [`AwaitOutput` requests](zakura_node_services::mempool::Request::AwaitOutput).

use std::{collections::HashMap, future::Future};

use tokio::sync::broadcast;

use tower::BoxError;
use zakura_chain::transparent;

use zakura_node_services::mempool::Response;

/// The most distinct outpoints that callers can wait for at once.
///
/// Honest transactions await few mempool outputs, because the mempool limits a
/// package to three transactions. An unknown outpoint stays here until a chain tip
/// change, so verification churn must not grow this map while the tip stalls.
pub(crate) const MAX_PENDING_OUTPUTS: usize = 65_536;

/// The fewest `queue` calls between scans for abandoned waiters.
///
/// A scan costs the live waiters plus the calls since the last scan, so it runs after at
/// least as many calls as the live waiters it kept. This
/// keeps the cost of each call constant while the map stays full of live waiters.
const MIN_QUEUES_BETWEEN_PRUNES: usize = 1_024;

/// A mempool output request refused because too many other requests are waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("too many callers are waiting for mempool outputs")]
pub struct PendingOutputsFull;

/// Pending [`transparent::Output`] tracker for handling the mempool's
/// [`AwaitOutput` requests](zakura_node_services::mempool::Request::AwaitOutput).
#[derive(Debug, Default)]
pub struct PendingOutputs {
    waiting: HashMap<transparent::OutPoint, broadcast::Sender<transparent::Output>>,
    queued_since_prune: usize,
    live_after_prune: usize,
}

impl PendingOutputs {
    /// Returns a future that will resolve to the `transparent::Output` pointed
    /// to by the given `transparent::OutPoint` when it is available.
    ///
    /// The future returns [`PendingOutputsFull`] if a new outpoint would exceed
    /// [`MAX_PENDING_OUTPUTS`]. Abandoned waiters leave the map before this refusal.
    pub fn queue(
        &mut self,
        outpoint: transparent::OutPoint,
    ) -> impl Future<Output = Result<Response, BoxError>> {
        self.queued_since_prune = self.queued_since_prune.saturating_add(1);
        if self.queued_since_prune >= self.live_after_prune.max(MIN_QUEUES_BETWEEN_PRUNES) {
            self.prune();
        }
        let refused =
            self.waiting.len() >= MAX_PENDING_OUTPUTS && !self.waiting.contains_key(&outpoint);
        let receiver = (!refused).then(|| {
            self.waiting
                .entry(outpoint)
                .or_insert_with(|| {
                    let (sender, _) = broadcast::channel(1);
                    sender
                })
                .subscribe()
        });

        async move {
            let mut receiver = receiver.ok_or(PendingOutputsFull)?;
            receiver
                .recv()
                .await
                .map(Response::UnspentOutput)
                .map_err(BoxError::from)
        }
    }

    /// Notify all requests waiting for the [`transparent::Output`] pointed to by
    /// the given [`transparent::OutPoint`] that the [`transparent::Output`] has
    /// arrived.
    #[inline]
    pub fn respond(&mut self, outpoint: &transparent::OutPoint, output: transparent::Output) {
        if let Some(sender) = self.waiting.remove(outpoint) {
            // Adding the outpoint as a field lets us cross-reference
            // with the trace of the verification that made the request.
            tracing::trace!(?outpoint, "found pending mempool output");
            let _ = sender.send(output);
        }
    }

    /// Scan the set of waiting Output requests for channels where all receivers
    /// have been dropped and remove the corresponding sender.
    pub fn prune(&mut self) {
        self.waiting.retain(|_, chan| chan.receiver_count() > 0);
        self.queued_since_prune = 0;
        self.live_after_prune = self.waiting.len();
    }

    /// Clears the inner [`HashMap`] of queued pending output requests.
    pub fn clear(&mut self) {
        self.waiting.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use zakura_chain::transaction;

    const LIVE_INDEX: usize = 4_000_000_000;

    fn outpoint(index: usize) -> transparent::OutPoint {
        transparent::OutPoint {
            hash: transaction::Hash([0; 32]),
            index: u32::try_from(index).expect("test indexes fit u32"),
        }
    }

    #[test]
    fn full_map_refuses_a_new_outpoint_but_shares_an_existing_one() {
        let mut pending = PendingOutputs::default();
        let waiters: Vec<_> = (0..MAX_PENDING_OUTPUTS)
            .map(|index| pending.queue(outpoint(index)).boxed())
            .collect();
        assert_eq!(pending.waiting.len(), MAX_PENDING_OUTPUTS);

        let refused = pending.queue(outpoint(MAX_PENDING_OUTPUTS)).now_or_never();
        let error = refused
            .expect("a refused request does not wait")
            .expect_err("the map is full of live waiters");
        assert!(error.downcast_ref::<PendingOutputsFull>().is_some());
        assert_eq!(pending.waiting.len(), MAX_PENDING_OUTPUTS);

        let mut shared = pending.queue(outpoint(0)).boxed();
        assert!((&mut shared).now_or_never().is_none());
        assert_eq!(pending.waiting.len(), MAX_PENDING_OUTPUTS);
        drop(waiters);
    }

    #[test]
    fn queue_removes_abandoned_waiters_without_a_chain_tip_change() {
        let mut pending = PendingOutputs::default();
        let live = pending.queue(outpoint(LIVE_INDEX));
        for index in 0..4 * MIN_QUEUES_BETWEEN_PRUNES {
            drop(pending.queue(outpoint(index)));
        }
        assert!(
            pending.waiting.len() <= MIN_QUEUES_BETWEEN_PRUNES,
            "abandoned waiters must not accumulate: {}",
            pending.waiting.len()
        );
        assert!(pending.waiting.contains_key(&outpoint(LIVE_INDEX)));
        drop(live);
    }
}
