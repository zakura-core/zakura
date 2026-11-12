//! The mempool's published snapshot: its verified transactions, tagged with the block they
//! were last checked against.
//!
//! New tips and explicit transaction-list requests publish a snapshot. Other mutations
//! withdraw it until the next list request, avoiding full-mempool copies during admission.
//! A reader uses a published snapshot or requests the current lists from the service.

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

/// The mempool's end of the snapshot channel.
#[derive(Debug)]
pub struct MempoolSnapshotSender(watch::Sender<Option<MempoolSnapshot>>);

impl MempoolSnapshotSender {
    /// Creates a channel on which no snapshot has been published yet.
    pub fn new() -> Self {
        Self(watch::Sender::new(None))
    }

    /// Publishes `snapshot`, or `None` when the mempool is inactive or its lists changed.
    ///
    /// Readers are woken only when it differs from the snapshot published last.
    pub fn publish(&self, snapshot: Option<MempoolSnapshot>) {
        let mut replaced = None;
        self.0.send_if_modified(|published| {
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

    /// Returns a reader of the snapshots published on this channel.
    pub fn subscribe(&self) -> MempoolSnapshotReceiver {
        MempoolSnapshotReceiver(self.0.subscribe())
    }
}

impl Default for MempoolSnapshotSender {
    fn default() -> Self {
        Self::new()
    }
}

/// A reader of the mempool's published snapshots.
#[derive(Clone, Debug)]
pub struct MempoolSnapshotReceiver(watch::Receiver<Option<MempoolSnapshot>>);

impl MempoolSnapshotReceiver {
    /// Returns the latest snapshot, or `None` when no current list has been published.
    pub fn latest(&self) -> Option<MempoolSnapshot> {
        self.0.borrow().clone()
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
        self.0
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
            receiver.0.has_changed().expect("the sender is still alive")
        };
        let mut seen = receiver.clone();

        sender.publish(None);
        assert!(!has_news(&seen), "still not active");

        let first = snapshot(tip);
        sender.publish(Some(first.clone()));
        assert!(has_news(&seen));
        seen.0.mark_unchanged();

        sender.publish(Some(first.clone()));
        assert!(!has_news(&seen), "same block and the same shared lists");

        // Same block and equal contents, but new lists: the mempool's transactions changed.
        sender.publish(Some(snapshot(tip)));
        assert!(has_news(&seen));
        seen.0.mark_unchanged();

        sender.publish(Some(MempoolSnapshot {
            last_seen_tip_hash: block::Hash([2; 32]),
            ..receiver.latest().expect("a snapshot was published")
        }));
        assert!(has_news(&seen), "same lists on a new block");
    }
}
