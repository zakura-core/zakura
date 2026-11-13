//! What follows the pool core's published pool: the announcer and the
//! indexer's change stream.
//!
//! Both read the numbered change log through `PoolReader::view`, which pairs
//! a snapshot with exactly the entries that lead to it, so an addition is
//! never read ahead of the snapshot that holds it.
//!
//! The announcer advertises new transactions to legacy peers, at most every
//! [`TRANSACTION_GOSSIP_DELAY`], as today's gossip task does. A transaction
//! that is visible again after a reorg is announced again, as today's reset
//! re-inserts and re-announces it.
//!
//! The change stream sends at most one event of each kind for each batch, in
//! today's order: added, invalidated, mined. A transaction refused at
//! verification, or removed for any reason but a block mining it, is
//! invalidated, as today.

use std::collections::HashSet;

use futures::{future::BoxFuture, FutureExt};

use tokio::{sync::broadcast, time::Instant};
use tower::{timeout::Timeout, Service, ServiceExt};

use zakura_chain::transaction::UnminedTxId;
use zakura_network::{self as zn, MAX_TX_INV_IN_SENT_MESSAGE};
use zakura_node_services::mempool::{
    port::{Change, ChangeSeq, ChangesSince, PoolReader, RemovalReason},
    MempoolChange,
};

use crate::{
    components::{
        mempool::gossip::{BROADCAST_READY_TIMEOUT, TRANSACTION_GOSSIP_DELAY},
        sync::TIPS_RESPONSE_TIMEOUT,
    },
    BoxError,
};

use super::super::Outbound;

/// The most ids one advertisement carries.
const MAX_ANNOUNCED: usize = MAX_TX_INV_IN_SENT_MESSAGE as usize;

/// The announcer and the change stream.
pub(crate) struct Follower {
    /// A reader, not a handle, so the follower does not keep the core
    /// running while it waits for the core to stop.
    pool: PoolReader,
    outbound: Timeout<Outbound>,
    changes: broadcast::Sender<MempoolChange>,
    cursor: ChangeSeq,
    /// Visible transactions not yet announced.
    pending: HashSet<UnminedTxId>,
}

impl Follower {
    /// A follower of `pool` that announces through `outbound` and sends
    /// the change stream on `changes`.
    pub fn new(
        pool: PoolReader,
        outbound: Outbound,
        changes: broadcast::Sender<MempoolChange>,
    ) -> Self {
        Self {
            pool,
            outbound: Timeout::new(outbound, TIPS_RESPONSE_TIMEOUT),
            changes,
            cursor: ChangeSeq::default(),
            pending: HashSet::new(),
        }
    }

    /// Follows the pool until the core stops.
    ///
    /// An announcement waits for the network beside the change stream, not
    /// in its way, so an unready network never holds up the indexer's
    /// events.
    pub async fn run(mut self) -> Result<(), BoxError> {
        let mut next_announcement = Instant::now();
        let mut announcing: Option<BoxFuture<'static, Option<HashSet<UnminedTxId>>>> = None;
        loop {
            self.read();
            let due = announcing.is_none() && !self.pending.is_empty();
            tokio::select! {
                changed = self.pool.snapshot_changed() => {
                    if !changed {
                        return Ok(());
                    }
                }
                () = tokio::time::sleep_until(next_announcement), if due => {
                    let ids: HashSet<UnminedTxId> =
                        self.pending.iter().copied().take(MAX_ANNOUNCED).collect();
                    announcing = Some(announce(self.outbound.clone(), ids).boxed());
                }
                announced = async { announcing.as_mut().expect("guarded").await },
                    if announcing.is_some() =>
                {
                    announcing = None;
                    next_announcement = Instant::now() + TRANSACTION_GOSSIP_DELAY;
                    // Only ids a broadcast started for leave `pending`.
                    for id in announced.into_iter().flatten() {
                        self.pending.remove(&id);
                    }
                }
            }
        }
    }

    /// Reads the log from the cursor to the latest snapshot.
    fn read(&mut self) {
        let Some(view) = self.pool.view(self.cursor) else {
            return;
        };
        let snapshot = view.snapshot;
        match view.changes {
            ChangesSince::Entries(entries) => {
                let mut batch = Batch::default();
                for entry in entries {
                    if Some(entry.batch) != batch.number {
                        batch.send(&self.changes);
                        batch = Batch {
                            number: Some(entry.batch),
                            ..Batch::default()
                        };
                    }
                    batch.take(entry.change);
                    match entry.change {
                        Change::Added(id) | Change::Revealed { id, .. } => {
                            self.pending.insert(id);
                        }
                        Change::Removed { id, .. } | Change::Hidden(id) => {
                            self.pending.remove(&id);
                        }
                        Change::Refused(_) => {}
                    }
                }
                batch.send(&self.changes);
            }
            ChangesSince::Resync => {
                // The log moved past this reader. The snapshot cannot give back
                // what was refused or removed, so those events are lost. Every
                // visible transaction is announced again.
                tracing::warn!("the pool's change log moved past its follower, resynchronizing");
                metrics::counter!("mempool.gossip.lagged.events.total").increment(1);
                self.pending = snapshot
                    .transactions
                    .iter()
                    .map(|tx| tx.transaction.id())
                    .collect();
            }
        }
        // Only what is still visible is announced.
        self.pending.retain(|id| {
            snapshot
                .transaction(&id.mined_id())
                .is_some_and(|tx| tx.transaction.id() == *id)
        });
        self.cursor = snapshot.last_change;
    }
}

/// Advertises `ids` to the peer set, and returns them once the broadcast has
/// started, or `None` if the network was not ready in time.
///
/// The wait for the network is bounded, as today's gossip bounds it. Ids
/// that were not sent stay pending for a later try.
async fn announce(
    outbound: Timeout<Outbound>,
    ids: HashSet<UnminedTxId>,
) -> Option<HashSet<UnminedTxId>> {
    let network =
        match tokio::time::timeout(BROADCAST_READY_TIMEOUT, outbound.ready_oneshot()).await {
            Ok(Ok(network)) => network,
            Ok(Err(_)) | Err(_) => {
                metrics::counter!("mempool.gossip.network.unready.total").increment(1);
                return None;
            }
        };
    let count = u64::try_from(ids.len()).expect("one advertisement fits in u64");
    let mut network = network;
    let response = network.call(zn::Request::AdvertiseTransactionIds(ids.clone(), None));
    tokio::spawn(async move {
        if let Err(error) = response.await {
            tracing::debug!(%error, "transaction broadcast did not finish");
        }
    });
    metrics::counter!("mempool.gossiped.transactions.total").increment(count);
    Some(ids)
}

/// One batch of the change log, as today's change stream events.
#[derive(Default)]
struct Batch {
    number: Option<u64>,
    added: HashSet<UnminedTxId>,
    invalidated: HashSet<UnminedTxId>,
    mined: HashSet<UnminedTxId>,
}

impl Batch {
    /// Adds one change.
    fn take(&mut self, change: Change) {
        match change {
            Change::Added(id) | Change::Revealed { id, .. } => {
                self.added.insert(id);
            }
            Change::Removed {
                id,
                reason: RemovalReason::Mined,
            } => {
                self.mined.insert(id);
            }
            Change::Removed { id, .. } | Change::Refused(id) => {
                self.invalidated.insert(id);
            }
            Change::Hidden(_) => {}
        }
    }

    /// Sends this batch's events, in today's order. Nobody listening is
    /// fine.
    fn send(&mut self, changes: &broadcast::Sender<MempoolChange>) {
        let added = std::mem::take(&mut self.added);
        if !added.is_empty() {
            let _ = changes.send(MempoolChange::added(added));
        }
        let invalidated = std::mem::take(&mut self.invalidated);
        if !invalidated.is_empty() {
            let _ = changes.send(MempoolChange::invalidated(invalidated));
        }
        let mined = std::mem::take(&mut self.mined);
        if !mined.is_empty() {
            let _ = changes.send(MempoolChange::mined(mined));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        task::{Context, Poll},
        time::Duration,
    };

    use tower::util::BoxCloneSyncService;
    use zakura_chain::{block, parameters::Network};
    use zakura_node_services::mempool::{
        port::{pool_port, ChainTip, Generation, PoolSnapshot},
        MempoolChangeKind, TransactionDependencies,
    };

    use super::*;

    /// A peer set that is ready only while `open` holds, and records each
    /// advertisement.
    #[derive(Clone, Default)]
    struct Gate {
        open: Arc<AtomicBool>,
        advertised: Arc<Mutex<Vec<HashSet<UnminedTxId>>>>,
    }

    impl Service<zn::Request> for Gate {
        type Response = zn::Response;
        type Error = zn::BoxError;
        type Future = BoxFuture<'static, Result<zn::Response, zn::BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            // A closed gate never wakes its caller. Each try polls again.
            if self.open.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn call(&mut self, request: zn::Request) -> Self::Future {
            if let zn::Request::AdvertiseTransactionIds(ids, _) = request {
                self.advertised
                    .lock()
                    .expect("no test panics holding it")
                    .push(ids);
            }
            async { Ok(zn::Response::Nil) }.boxed()
        }
    }

    /// The pool at height 1 holding `txs`.
    fn snapshot(
        txs: Vec<zakura_chain::transaction::VerifiedUnminedTx>,
        last_change: ChangeSeq,
    ) -> PoolSnapshot {
        PoolSnapshot::new(
            ChainTip {
                hash: block::Hash([1; 32]),
                height: block::Height(1),
            },
            Generation(0),
            last_change,
            txs,
            TransactionDependencies::default(),
        )
    }

    /// An unready network neither holds up the change stream nor loses the
    /// ids it could not announce.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_unready_network_neither_stalls_the_change_stream_nor_loses_ids() {
        let mut vectors = Network::Mainnet.unmined_transactions_in_blocks(1..=10);
        let pooled = vectors.next().expect("a vector transaction");
        let refused = vectors.next().expect("a second vector transaction");
        let (handle, inbox) = pool_port();
        let gate = Gate::default();
        let (changes, mut events) = broadcast::channel(16);
        let follower = Follower::new(
            handle.reader(),
            BoxCloneSyncService::new(gate.clone()),
            changes,
        );
        tokio::spawn(follower.run());

        inbox.publish([Change::Added(pooled.transaction.id())], |last| {
            snapshot(vec![pooled.clone()], last)
        });
        let added = events.recv().await.expect("the stream stays open");
        assert_eq!(added.kind, MempoolChangeKind::Added);

        // The announcement is now waiting on the closed network. A refusal
        // still reaches the stream well before that wait ends.
        tokio::time::sleep(TRANSACTION_GOSSIP_DELAY).await;
        inbox.publish([Change::Refused(refused.transaction.id())], |last| {
            snapshot(vec![pooled.clone()], last)
        });
        let invalidated = tokio::time::timeout(Duration::from_millis(10), events.recv())
            .await
            .expect("the change stream is not held up by the network")
            .expect("the stream stays open");
        assert_eq!(invalidated.kind, MempoolChangeKind::Invalidated);
        assert!(gate.advertised.lock().expect("no panics").is_empty());

        // Once the network is ready, the id that waited is announced.
        gate.open.store(true, Ordering::SeqCst);
        tokio::time::sleep(BROADCAST_READY_TIMEOUT + TRANSACTION_GOSSIP_DELAY * 2).await;
        let advertised = gate.advertised.lock().expect("no panics").clone();
        assert_eq!(advertised, vec![[pooled.transaction.id()].into()]);
    }
}
