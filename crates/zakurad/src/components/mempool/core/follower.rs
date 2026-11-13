//! What follows the pool core's published pool: the announcer and the
//! indexer's change stream.
//!
//! Both read the numbered change log through `PoolHandle::view`, which pairs
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

use tokio::{sync::broadcast, time::Instant};
use tower::{timeout::Timeout, Service, ServiceExt};

use zakura_chain::transaction::UnminedTxId;
use zakura_network::{self as zn, MAX_TX_INV_IN_SENT_MESSAGE};
use zakura_node_services::mempool::{
    port::{Change, ChangeSeq, ChangesSince, PoolHandle, RemovalReason},
    MempoolChange,
};

use crate::{
    components::{mempool::gossip::TRANSACTION_GOSSIP_DELAY, sync::TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

use super::super::Outbound;

/// The most ids one advertisement carries.
const MAX_ANNOUNCED: usize = MAX_TX_INV_IN_SENT_MESSAGE as usize;

/// The announcer and the change stream.
pub(crate) struct Follower {
    handle: PoolHandle,
    outbound: Timeout<Outbound>,
    changes: broadcast::Sender<MempoolChange>,
    cursor: ChangeSeq,
    /// Visible transactions not yet announced.
    pending: HashSet<UnminedTxId>,
}

impl Follower {
    /// A follower of `handle` that announces through `outbound` and sends
    /// the change stream on `changes`.
    pub fn new(
        handle: PoolHandle,
        outbound: Outbound,
        changes: broadcast::Sender<MempoolChange>,
    ) -> Self {
        Self {
            handle,
            outbound: Timeout::new(outbound, TIPS_RESPONSE_TIMEOUT),
            changes,
            cursor: ChangeSeq::default(),
            pending: HashSet::new(),
        }
    }

    /// Follows the pool until the core stops.
    pub async fn run(mut self) -> Result<(), BoxError> {
        let mut next_announcement = Instant::now();
        loop {
            self.read();
            let announce = !self.pending.is_empty();
            tokio::select! {
                changed = self.handle.snapshot_changed() => {
                    if !changed {
                        return Ok(());
                    }
                }
                () = tokio::time::sleep_until(next_announcement), if announce => {
                    self.announce().await;
                    next_announcement = Instant::now() + TRANSACTION_GOSSIP_DELAY;
                }
            }
        }
    }

    /// Reads the log from the cursor to the latest snapshot.
    fn read(&mut self) {
        let Some(view) = self.handle.view(self.cursor) else {
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

    /// Advertises up to one message of pending ids to the peer set.
    async fn announce(&mut self) {
        let ids: HashSet<UnminedTxId> = self.pending.iter().copied().take(MAX_ANNOUNCED).collect();
        for id in &ids {
            self.pending.remove(id);
        }
        let count = u64::try_from(ids.len()).expect("one advertisement fits in u64");
        let Ok(network) = self.outbound.ready().await else {
            metrics::counter!("mempool.gossip.network.unready.total").increment(1);
            return;
        };
        let response = network.call(zn::Request::AdvertiseTransactionIds(ids, None));
        tokio::spawn(async move {
            if let Err(error) = response.await {
                tracing::debug!(%error, "transaction broadcast did not finish");
            }
        });
        metrics::counter!("mempool.gossiped.transactions.total").increment(count);
    }
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
