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
//! The change stream sends the log's batches in order, at most one event of
//! each kind for each, in today's order: added, invalidated, mined. Batches
//! are merged only where that keeps every transaction's events in order. A
//! transaction refused at verification, or removed for any reason but a
//! block mining it, is invalidated, as today.

use std::{collections::HashSet, time::Duration};

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

/// The longest one read waits, in all, for the indexer to drain the change
/// channel. A subscriber that is still behind after it lags, and its stream
/// ends, as a slow subscriber's does today.
const MAX_DRAIN_WAIT: Duration = Duration::from_secs(1);

/// How often a read checks whether the indexer drained the channel.
const DRAIN_POLL: Duration = Duration::from_millis(1);

/// The announcer and the change stream.
pub(crate) struct Follower {
    /// A reader, not a handle, so the follower does not keep the core
    /// running while it waits for the core to stop.
    pool: PoolReader,
    outbound: Timeout<Outbound>,
    changes: broadcast::Sender<MempoolChange>,
    /// A read sends an event only while fewer than this many are queued,
    /// for at most [`MAX_DRAIN_WAIT`]: half the channel's capacity.
    drain_below: usize,
    cursor: ChangeSeq,
    /// Visible transactions not yet announced.
    pending: HashSet<UnminedTxId>,
}

impl Follower {
    /// A follower of `pool` that announces through `outbound` and sends
    /// the change stream on `changes`, a channel of `capacity` events.
    pub fn new(
        pool: PoolReader,
        outbound: Outbound,
        changes: broadcast::Sender<MempoolChange>,
        capacity: usize,
    ) -> Self {
        Self {
            pool,
            outbound: Timeout::new(outbound, TIPS_RESPONSE_TIMEOUT),
            changes,
            drain_below: (capacity / 2).max(1),
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
        // The network, once it is ready. The ids are picked only then, so
        // none removed during the wait is advertised.
        let mut readying: Option<BoxFuture<'static, Option<Timeout<Outbound>>>> = None;
        loop {
            self.read().await;
            let due = readying.is_none() && !self.pending.is_empty();
            tokio::select! {
                changed = self.pool.snapshot_changed() => {
                    if !changed {
                        return Ok(());
                    }
                }
                () = tokio::time::sleep_until(next_announcement), if due => {
                    readying = Some(ready(self.outbound.clone()).boxed());
                }
                network = async { readying.as_mut().expect("guarded").await },
                    if readying.is_some() =>
                {
                    readying = None;
                    next_announcement = Instant::now() + TRANSACTION_GOSSIP_DELAY;
                    if let Some(network) = network {
                        // A removal can have been published while the
                        // network got ready, so read it before choosing ids.
                        self.read().await;
                        self.announce(network);
                    }
                }
            }
        }
    }

    /// Advertises up to one message of the pending ids through `network`,
    /// which is ready. The ids are taken only now, so the advertisement
    /// holds only transactions that are still pooled.
    fn announce(&mut self, mut network: Timeout<Outbound>) {
        let ids: HashSet<UnminedTxId> = self.pending.iter().copied().take(MAX_ANNOUNCED).collect();
        if ids.is_empty() {
            return;
        }
        for id in &ids {
            self.pending.remove(id);
        }
        let count = u64::try_from(ids.len()).expect("one advertisement fits in u64");
        let response = network.call(zn::Request::AdvertiseTransactionIds(ids, None));
        tokio::spawn(async move {
            if let Err(error) = response.await {
                tracing::debug!(%error, "transaction broadcast did not finish");
            }
        });
        metrics::counter!("mempool.gossiped.transactions.total").increment(count);
    }

    /// Reads the log from the cursor to the latest snapshot, and sends its
    /// events.
    async fn read(&mut self) {
        let Some(view) = self.pool.view(self.cursor) else {
            return;
        };
        let snapshot = view.snapshot;
        match view.changes {
            ChangesSince::Entries(entries) => {
                for entry in &entries {
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
                let wait_until = tokio::time::Instant::now() + MAX_DRAIN_WAIT;
                let mut batch = Batch::default();
                for entry in entries {
                    if batch.would_reorder(entry.change) {
                        self.send(std::mem::take(&mut batch), wait_until).await;
                    }
                    batch.take(entry.change);
                }
                self.send(batch, wait_until).await;
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

impl Follower {
    /// Sends `batch`'s events. Before each, waits until fewer than
    /// `drain_below` are queued or `wait_until` passes, so a subscriber that
    /// keeps up is never overrun by one read, however long. Nobody listening
    /// is fine.
    async fn send(&self, batch: Batch, wait_until: tokio::time::Instant) {
        for event in batch.events() {
            while self.changes.len() >= self.drain_below && tokio::time::Instant::now() < wait_until
            {
                tokio::time::sleep(DRAIN_POLL).await;
            }
            let _ = self.changes.send(event);
        }
    }
}

/// The peer set once it is ready, or `None` if it was not ready within
/// [`BROADCAST_READY_TIMEOUT`], as today's gossip bounds the wait.
async fn ready(outbound: Timeout<Outbound>) -> Option<Timeout<Outbound>> {
    match tokio::time::timeout(BROADCAST_READY_TIMEOUT, outbound.ready_oneshot()).await {
        Ok(Ok(network)) => Some(network),
        Ok(Err(_)) | Err(_) => {
            metrics::counter!("mempool.gossip.network.unready.total").increment(1);
            None
        }
    }
}

/// One batch of the change log, as today's change stream events.
#[derive(Default)]
struct Batch {
    added: HashSet<UnminedTxId>,
    invalidated: HashSet<UnminedTxId>,
    mined: HashSet<UnminedTxId>,
}

impl Batch {
    /// Whether taking `change` would send its transaction's events out of
    /// order. The events go out added, then invalidated, then mined, so a
    /// change is out of order if the batch already holds its transaction in
    /// a kind sent after the change's, or in the same kind, which one event
    /// would merge.
    fn would_reorder(&self, change: Change) -> bool {
        match change {
            Change::Added(id) | Change::Revealed { id, .. } => {
                self.added.contains(&id)
                    || self.invalidated.contains(&id)
                    || self.mined.contains(&id)
            }
            Change::Removed {
                id,
                reason: RemovalReason::Mined,
            } => self.mined.contains(&id),
            Change::Hidden(_) => false,
            Change::Removed { id, .. } | Change::Refused(id) => {
                self.invalidated.contains(&id) || self.mined.contains(&id)
            }
        }
    }

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

    /// Its events, in today's order.
    fn events(self) -> impl Iterator<Item = MempoolChange> {
        [
            (!self.added.is_empty()).then(|| MempoolChange::added(self.added)),
            (!self.invalidated.is_empty()).then(|| MempoolChange::invalidated(self.invalidated)),
            (!self.mined.is_empty()).then(|| MempoolChange::mined(self.mined)),
        ]
        .into_iter()
        .flatten()
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
            16,
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

    /// A transaction mined while an announcement waits for the network is
    /// not advertised once the network is ready.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_id_removed_while_the_network_was_unready_is_not_advertised() {
        let pooled = Network::Mainnet
            .unmined_transactions_in_blocks(1..=10)
            .next()
            .expect("a vector transaction");
        let (handle, inbox) = pool_port();
        let gate = Gate::default();
        let (changes, _events) = broadcast::channel(16);
        tokio::spawn(
            Follower::new(
                handle.reader(),
                BoxCloneSyncService::new(gate.clone()),
                changes,
                16,
            )
            .run(),
        );

        inbox.publish([Change::Added(pooled.transaction.id())], |last| {
            snapshot(vec![pooled.clone()], last)
        });
        // The announcement starts waiting on the closed network, and the
        // transaction is mined meanwhile.
        tokio::time::sleep(Duration::from_millis(100)).await;
        inbox.publish(
            [Change::Removed {
                id: pooled.transaction.id(),
                reason: RemovalReason::Mined,
            }],
            |last| snapshot(Vec::new(), last),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        gate.open.store(true, Ordering::SeqCst);
        tokio::time::sleep(BROADCAST_READY_TIMEOUT + TRANSACTION_GOSSIP_DELAY * 2).await;
        assert!(
            gate.advertised.lock().expect("no panics").is_empty(),
            "nothing still pooled was left to advertise"
        );
    }

    /// A burst of inputs, read at once, reaches the indexer as one event per
    /// kind, so it cannot overflow the channel and end the stream. One
    /// transaction refused and then admitted in the same read keeps its
    /// order.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_burst_reaches_the_indexer_as_one_event_per_kind_in_order() {
        let txs: Vec<_> = Network::Mainnet
            .unmined_transactions_in_blocks(1..=10)
            .take(2)
            .collect();
        let (handle, inbox) = pool_port();
        let (changes, mut events) = broadcast::channel(16);
        // 70 inputs, each its own batch, published before the follower
        // reads any of them: 35 admissions that each evict another
        // transaction, as in Terra's example.
        let id = |n: u8| UnminedTxId::Legacy(zakura_chain::transaction::Hash([n; 32]));
        for n in 0..35 {
            inbox.publish([Change::Added(id(n))], |last| snapshot(Vec::new(), last));
            inbox.publish(
                [Change::Removed {
                    id: id(100 + n),
                    reason: RemovalReason::Evicted,
                }],
                |last| snapshot(Vec::new(), last),
            );
        }
        // Then the same transaction is refused and later admitted.
        inbox.publish([Change::Refused(txs[1].transaction.id())], |last| {
            snapshot(Vec::new(), last)
        });
        inbox.publish([Change::Added(txs[1].transaction.id())], |last| {
            snapshot(vec![txs[1].clone()], last)
        });
        tokio::spawn(
            Follower::new(
                handle.reader(),
                BoxCloneSyncService::new(Gate::default()),
                changes,
                16,
            )
            .run(),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut kinds = Vec::new();
        while let Ok(event) = events.try_recv() {
            kinds.push((event.kind, event.tx_ids.contains(&txs[1].transaction.id())));
        }
        assert!(
            kinds.len() <= 6,
            "a few events, not one per input: {kinds:?}"
        );
        let refused = kinds
            .iter()
            .position(|kind| *kind == (MempoolChangeKind::Invalidated, true))
            .expect("the refusal is sent");
        let admitted = kinds
            .iter()
            .position(|kind| *kind == (MempoolChangeKind::Added, true))
            .expect("the admission is sent");
        assert!(refused < admitted, "the refusal comes before the admission");
    }

    /// One read of many add and removal cycles of the same transaction
    /// reaches an indexer that keeps reading as every event, in order, even
    /// though the read holds more events than the channel.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn repeated_changes_to_one_transaction_reach_the_indexer_in_order() {
        let (handle, inbox) = pool_port();
        let (changes, mut events) = broadcast::channel(16);
        let id = UnminedTxId::Legacy(zakura_chain::transaction::Hash([7; 32]));
        for _ in 0..33 {
            inbox.publish([Change::Added(id)], |last| snapshot(Vec::new(), last));
            inbox.publish(
                [Change::Removed {
                    id,
                    reason: RemovalReason::Evicted,
                }],
                |last| snapshot(Vec::new(), last),
            );
        }
        tokio::spawn(
            Follower::new(
                handle.reader(),
                BoxCloneSyncService::new(Gate::default()),
                changes,
                16,
            )
            .run(),
        );

        let mut kinds = Vec::new();
        while kinds.len() < 66 {
            match tokio::time::timeout(Duration::from_secs(5), events.recv()).await {
                Ok(Ok(event)) => kinds.push(event.kind),
                Ok(Err(error)) => panic!("the indexer's channel overflowed: {error:?}"),
                Err(_) => panic!("only {} events arrived", kinds.len()),
            }
        }
        let expected: Vec<_> = (0..33)
            .flat_map(|_| [MempoolChangeKind::Added, MempoolChangeKind::Invalidated])
            .collect();
        assert_eq!(kinds, expected);
    }

    /// An indexer that stops reading lags once the read has waited its
    /// bound, so the follower is not held up for good.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_indexer_that_stops_reading_lags_instead_of_holding_the_follower() {
        let (handle, inbox) = pool_port();
        let (changes, mut events) = broadcast::channel(16);
        let id = UnminedTxId::Legacy(zakura_chain::transaction::Hash([7; 32]));
        for _ in 0..33 {
            inbox.publish([Change::Added(id)], |last| snapshot(Vec::new(), last));
            inbox.publish(
                [Change::Removed {
                    id,
                    reason: RemovalReason::Evicted,
                }],
                |last| snapshot(Vec::new(), last),
            );
        }
        tokio::spawn(
            Follower::new(
                handle.reader(),
                BoxCloneSyncService::new(Gate::default()),
                changes,
                16,
            )
            .run(),
        );
        tokio::time::sleep(MAX_DRAIN_WAIT * 2).await;
        assert!(
            matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Lagged(_))
            ),
            "the stalled indexer is told it lagged"
        );
    }
}
