//! The mempool's published snapshot: its verified transactions, tagged with the block they
//! were last checked against.
//!
//! New tips and explicit transaction-list requests publish a snapshot. Other mutations
//! withdraw it until the next list request, avoiding full-mempool copies during admission.
//! A reader uses a published snapshot or requests the current lists from the service.
//!
//! The mempool also publishes its progress on the newest block: that it has started removing
//! the block's transactions and how much work that is, and that it has finished. A reader
//! that cannot wait long for the block's snapshot uses it to decide whether to wait at all.

use std::sync::Arc;

use tokio::sync::watch;
use zakura_chain::{block, transaction::VerifiedUnminedTx};

use super::TransactionDependencies;

/// The mempool's verified transactions as of one block.
///
/// The lists are shared with the mempool, not copied, and never change after publication.
#[derive(Clone, Debug)]
pub struct MempoolSnapshot {
    /// The hash of the last block the mempool checked these transactions against.
    pub last_seen_tip_hash: block::Hash,

    /// The verified transactions.
    pub transactions: Arc<Vec<VerifiedUnminedTx>>,

    /// The transactions among them that spend or create outputs of the others.
    pub transaction_dependencies: Arc<TransactionDependencies>,
}

impl MempoolSnapshot {
    /// Returns true if `other` is for the same block and shares this snapshot's lists.
    ///
    /// The mempool keeps handing out the same shared lists until its transactions change, so
    /// this is true exactly when nothing changed between the two snapshots.
    fn is_unchanged_from(&self, other: &Self) -> bool {
        self.last_seen_tip_hash == other.last_seen_tip_hash
            && Arc::ptr_eq(&self.transactions, &other.transactions)
            && Arc::ptr_eq(
                &self.transaction_dependencies,
                &other.transaction_dependencies,
            )
    }
}

/// The mempool's progress on the newest block it has started to process.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MempoolTipProgress {
    /// The hash of that block.
    pub tip_hash: block::Hash,

    /// How far the mempool is with it.
    pub stage: MempoolTipStage,
}

/// How far the mempool is with a block.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MempoolTipStage {
    /// The mempool is removing the block's mined, conflicting and expired transactions.
    /// Its transactions for this block are not available until it finishes.
    Reconciling(MempoolTipWork),

    /// The mempool has finished the block. Its transactions for the block are published, or
    /// it answers a request for them without further work on the block.
    Reconciled,
}

/// What the mempool held when a block arrived, and how much work removing the block's
/// transactions is.
///
/// Work is counted in the items the mempool handles one by one when a block arrives, called
/// parts here. Each kind is counted separately, so that measurements can weigh them.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct MempoolTipWork {
    /// The number of verified transactions the mempool held when the block arrived.
    ///
    /// The block can only remove some of them, so zero means the mempool has none for this
    /// block. Each one is visited once to check it against the block, to check its expiry,
    /// and to copy it into the published list.
    pub mempool_transactions: usize,

    /// The transparent outputs the block's transactions spend and the nullifiers they reveal,
    /// which the mempool collects to check its transactions against, and the outputs created
    /// by the block's transactions that the mempool held, which it removes with them.
    ///
    /// Outputs created by transactions the mempool never held are not counted: it never
    /// touches them, so a block with many such outputs is not reported as heavy work.
    pub block_parts: usize,

    /// The transparent outputs the mempool's transactions spend, and the nullifiers they
    /// reveal: the parts the mempool checks against the block's.
    pub mempool_parts: usize,
}

impl MempoolTipWork {
    /// Returns the work as one number: each part counts one, and each transaction held counts
    /// one for its visits.
    pub fn work(&self) -> usize {
        self.mempool_transactions
            .saturating_add(self.block_parts)
            .saturating_add(self.mempool_parts)
    }
}

/// What the mempool holds now, kept current as it admits and removes transactions.
///
/// A reader that wakes on a new block before the mempool has reported on it uses this to
/// estimate the block's work at once (see [`MempoolHeld::estimated_tip_work`]).
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct MempoolHeld {
    /// The number of verified transactions.
    pub transactions: usize,

    /// The transparent outputs they spend and the nullifiers they reveal (see
    /// [`MempoolTipWork::mempool_parts`]).
    pub parts: usize,
}

impl MempoolHeld {
    /// Estimates the work the mempool will report for a block of `block_transactions`
    /// transactions, before it has seen the block.
    ///
    /// What the mempool holds is known exactly. The block's parts are not: a block's
    /// transactions mostly come from the mempool, so each is counted at the average parts of
    /// a held transaction, plus one for the output it creates.
    pub fn estimated_tip_work(&self, block_transactions: usize) -> MempoolTipWork {
        let parts_per_transaction = self
            .parts
            .checked_div(self.transactions)
            .unwrap_or(0)
            .saturating_add(1);

        MempoolTipWork {
            mempool_transactions: self.transactions,
            block_parts: block_transactions.saturating_mul(parts_per_transaction),
            mempool_parts: self.parts,
        }
    }
}

/// The mempool's end of the snapshot channel.
#[derive(Debug)]
pub struct MempoolSnapshotSender {
    /// The published snapshot.
    snapshots: watch::Sender<Option<MempoolSnapshot>>,

    /// The mempool's progress on the newest block, or `None` before its first block.
    tip_progress: watch::Sender<Option<MempoolTipProgress>>,

    /// What the mempool holds now, or `None` before it first says.
    held: watch::Sender<Option<MempoolHeld>>,
}

impl MempoolSnapshotSender {
    /// Creates a channel on which no snapshot or progress has been published yet.
    pub fn new() -> Self {
        Self {
            snapshots: watch::Sender::new(None),
            tip_progress: watch::Sender::new(None),
            held: watch::Sender::new(None),
        }
    }

    /// Publishes what the mempool holds now. Readers only read it when they need it, so this
    /// wakes no one.
    pub fn publish_held(&self, held: MempoolHeld) {
        self.held.send_if_modified(|published| {
            let is_changed = *published != Some(held);
            *published = Some(held);
            is_changed
        });
    }

    /// Publishes that the mempool has started on the block `tip_hash`, before it does any of
    /// the `work`.
    pub fn start_tip(&self, tip_hash: block::Hash, work: MempoolTipWork) {
        self.publish_tip_progress(MempoolTipProgress {
            tip_hash,
            stage: MempoolTipStage::Reconciling(work),
        });
    }

    /// Publishes that the mempool has finished the block `tip_hash`.
    pub fn finish_tip(&self, tip_hash: block::Hash) {
        self.publish_tip_progress(MempoolTipProgress {
            tip_hash,
            stage: MempoolTipStage::Reconciled,
        });
    }

    /// Publishes `progress`, waking readers only when it differs from the progress published
    /// last.
    fn publish_tip_progress(&self, progress: MempoolTipProgress) {
        self.tip_progress.send_if_modified(|published| {
            let is_changed = *published != Some(progress);
            *published = Some(progress);
            is_changed
        });
    }

    /// Publishes `snapshot`, or `None` when the mempool is inactive or its lists changed.
    ///
    /// Readers are woken only when it differs from the snapshot published last.
    pub fn publish(&self, snapshot: Option<MempoolSnapshot>) {
        let mut replaced = None;
        self.snapshots.send_if_modified(|published| {
            let is_unchanged = match (&*published, &snapshot) {
                (None, None) => true,
                (Some(published), Some(snapshot)) => snapshot.is_unchanged_from(published),
                (None, Some(_)) | (Some(_), None) => false,
            };

            if !is_unchanged {
                replaced = std::mem::replace(published, snapshot);
            }

            !is_unchanged
        });
        // Releasing the last reader walks every transaction. Do that after the channel's
        // write lock has been released so new readers are not blocked by destruction.
        drop(replaced);
    }

    /// Returns a reader of the snapshots and progress published on this channel.
    pub fn subscribe(&self) -> MempoolSnapshotReceiver {
        MempoolSnapshotReceiver {
            snapshots: self.snapshots.subscribe(),
            tip_progress: self.tip_progress.subscribe(),
            held: self.held.subscribe(),
        }
    }
}

impl Default for MempoolSnapshotSender {
    fn default() -> Self {
        Self::new()
    }
}

/// A reader of the mempool's published snapshots and of its progress on the newest block.
#[derive(Clone, Debug)]
pub struct MempoolSnapshotReceiver {
    /// The published snapshot.
    snapshots: watch::Receiver<Option<MempoolSnapshot>>,

    /// The mempool's progress on the newest block.
    tip_progress: watch::Receiver<Option<MempoolTipProgress>>,

    /// What the mempool holds now, or `None` before it first says.
    held: watch::Receiver<Option<MempoolHeld>>,
}

impl MempoolSnapshotReceiver {
    /// Returns the latest snapshot, or `None` when no current list has been published.
    pub fn latest(&self) -> Option<MempoolSnapshot> {
        self.snapshots.borrow().clone()
    }

    /// Returns what the mempool holds now, or `None` if it has not said yet.
    pub fn held(&self) -> Option<MempoolHeld> {
        *self.held.borrow()
    }

    /// Returns the mempool's progress on the newest block it has started on, without waiting.
    pub fn progress(&self) -> Option<MempoolTipProgress> {
        *self.tip_progress.borrow()
    }

    /// Returns the mempool's progress on the block `tip_hash`, waiting until the mempool has
    /// started on that block. Returns at once if it already has.
    ///
    /// The progress returned is the one published when this is called or first published
    /// afterwards, so a block the mempool is still working on is reported as
    /// [`MempoolTipStage::Reconciling`] and one it finished earlier as
    /// [`MempoolTipStage::Reconciled`].
    ///
    /// Returns `None` if the mempool shut down first. The wait has no bound of its own: a
    /// mempool that is not active, or that never sees `tip_hash`, never ends it.
    pub async fn progress_on_tip(&mut self, tip_hash: block::Hash) -> Option<MempoolTipProgress> {
        self.tip_progress
            .wait_for(|progress| progress.is_some_and(|progress| progress.tip_hash == tip_hash))
            .await
            .ok()
            .and_then(|progress| *progress)
    }

    /// Returns the snapshot the mempool checked against the block `tip_hash`, waiting until
    /// the mempool publishes one. Returns at once if the latest snapshot is already for that
    /// block.
    ///
    /// Returns `None` if the mempool shut down without publishing one. The wait has no bound
    /// of its own: a mempool that is not active, or that never sees `tip_hash`, never ends it.
    /// Admitting a transaction withdraws the published list until a new block or a request for
    /// the full list publishes it again, so a reader that must not wait for that also asks the
    /// mempool.
    pub async fn on_tip(&mut self, tip_hash: block::Hash) -> Option<MempoolSnapshot> {
        self.snapshots
            .wait_for(|snapshot| {
                snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.last_seen_tip_hash == tip_hash)
            })
            .await
            .ok()
            .and_then(|snapshot| snapshot.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A snapshot for the block `tip_hash` with new, empty lists.
    fn snapshot(tip_hash: block::Hash) -> MempoolSnapshot {
        MempoolSnapshot {
            last_seen_tip_hash: tip_hash,
            transactions: Default::default(),
            transaction_dependencies: Default::default(),
        }
    }

    /// A reader gets the snapshot for the block it asks about once the mempool has published
    /// it, whether that happened before or after it asked.
    #[tokio::test]
    async fn reader_gets_the_snapshot_for_its_block() {
        let older_tip = block::Hash([1; 32]);
        let newer_tip = block::Hash([2; 32]);
        let sender = MempoolSnapshotSender::new();
        let mut receiver = sender.subscribe();

        sender.publish(Some(snapshot(older_tip)));
        let published_before = receiver
            .on_tip(older_tip)
            .await
            .expect("the mempool is still running");
        assert_eq!(published_before.last_seen_tip_hash, older_tip);

        let mut waiting = receiver.clone();
        let waiting = tokio::spawn(async move { waiting.on_tip(newer_tip).await });
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "the reader waits while the mempool is on an older block"
        );

        sender.publish(Some(snapshot(newer_tip)));
        let published_after = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the published snapshot wakes the reader")
            .expect("the reader task does not panic")
            .expect("the mempool is still running");
        assert_eq!(published_after.last_seen_tip_hash, newer_tip);
    }

    /// A reader stops waiting when the mempool shuts down, and one that is not active has no
    /// snapshot.
    #[tokio::test]
    async fn reader_gets_nothing_from_an_inactive_or_stopped_mempool() {
        let tip = block::Hash([1; 32]);
        let sender = MempoolSnapshotSender::new();
        let mut receiver = sender.subscribe();
        assert!(receiver.latest().is_none());

        sender.publish(Some(snapshot(tip)));
        sender.publish(None);
        assert!(receiver.latest().is_none());

        drop(sender);
        assert!(receiver.on_tip(tip).await.is_none());
    }

    /// Publishing what was published last wakes no reader, and publishing a change does.
    #[tokio::test]
    async fn only_a_changed_snapshot_wakes_readers() {
        let tip = block::Hash([1; 32]);
        let sender = MempoolSnapshotSender::new();
        let receiver = sender.subscribe();
        let has_news = |receiver: &MempoolSnapshotReceiver| {
            receiver
                .snapshots
                .has_changed()
                .expect("the sender is still alive")
        };
        let mut seen = receiver.clone();

        sender.publish(None);
        assert!(!has_news(&seen), "still not active");

        let first = snapshot(tip);
        sender.publish(Some(first.clone()));
        assert!(has_news(&seen));
        seen.snapshots.mark_unchanged();

        sender.publish(Some(first.clone()));
        assert!(!has_news(&seen), "same block and the same shared lists");

        // Same block and equal contents, but new lists: the mempool's transactions changed.
        sender.publish(Some(snapshot(tip)));
        assert!(has_news(&seen));
        seen.snapshots.mark_unchanged();

        sender.publish(Some(MempoolSnapshot {
            last_seen_tip_hash: block::Hash([2; 32]),
            ..receiver.latest().expect("a snapshot was published")
        }));
        assert!(has_news(&seen), "same lists on a new block");
    }

    /// Work with `mempool_transactions` held and `block_parts` in the block.
    fn work(mempool_transactions: usize, block_parts: usize) -> MempoolTipWork {
        MempoolTipWork {
            mempool_transactions,
            block_parts,
            mempool_parts: 0,
        }
    }

    /// A reader gets the mempool's progress on the block it asks about, whether the mempool
    /// started on that block before or after it asked, and not its progress on another block.
    #[tokio::test]
    async fn reader_gets_the_progress_on_its_block() {
        let older_tip = block::Hash([1; 32]);
        let newer_tip = block::Hash([2; 32]);
        let sender = MempoolSnapshotSender::new();
        let mut receiver = sender.subscribe();

        sender.start_tip(older_tip, work(3, 40));
        assert_eq!(
            receiver.progress_on_tip(older_tip).await,
            Some(MempoolTipProgress {
                tip_hash: older_tip,
                stage: MempoolTipStage::Reconciling(work(3, 40)),
            }),
            "the mempool had already started on the block"
        );

        let mut waiting = receiver.clone();
        let waiting = tokio::spawn(async move { waiting.progress_on_tip(newer_tip).await });
        tokio::task::yield_now().await;
        sender.finish_tip(older_tip);
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "progress on an older block is not progress on the block asked about"
        );

        sender.start_tip(newer_tip, work(2, 7));
        let started_after = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the mempool starting on the block wakes the reader")
            .expect("the reader task does not panic");
        assert_eq!(
            started_after,
            Some(MempoolTipProgress {
                tip_hash: newer_tip,
                stage: MempoolTipStage::Reconciling(work(2, 7)),
            })
        );
    }

    /// The estimate counts what the mempool holds exactly, and each block transaction at the
    /// average parts of a held one plus its output.
    #[test]
    fn estimated_tip_work_uses_the_held_average_for_the_block() {
        let held = MempoolHeld {
            transactions: 4,
            parts: 10,
        };
        assert_eq!(
            held.estimated_tip_work(3),
            MempoolTipWork {
                mempool_transactions: 4,
                block_parts: 3 * (10 / 4 + 1),
                mempool_parts: 10,
            }
        );
        assert_eq!(
            MempoolHeld::default().estimated_tip_work(2),
            MempoolTipWork {
                mempool_transactions: 0,
                block_parts: 2,
                mempool_parts: 0,
            },
            "an empty mempool counts one part for each block transaction"
        );
    }

    /// A reader that asks about a block the mempool has finished is told so, and one that
    /// asks about a block the mempool never starts stops waiting when the mempool shuts down.
    #[tokio::test]
    async fn reader_learns_that_a_block_is_finished_or_that_the_mempool_stopped() {
        let tip = block::Hash([1; 32]);
        let sender = MempoolSnapshotSender::new();
        let mut receiver = sender.subscribe();

        sender.start_tip(tip, work(3, 40));
        sender.finish_tip(tip);
        assert_eq!(
            receiver.progress_on_tip(tip).await,
            Some(MempoolTipProgress {
                tip_hash: tip,
                stage: MempoolTipStage::Reconciled,
            })
        );

        drop(sender);
        assert_eq!(receiver.progress_on_tip(block::Hash([2; 32])).await, None);
    }
}
