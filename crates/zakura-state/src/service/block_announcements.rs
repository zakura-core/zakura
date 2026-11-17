//! Committed block announcements published by the state writer.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::Notify;
use zakura_chain::block;

/// The source of a block entering verification. Never persisted or inferred from admission.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BlockOrigin {
    /// Ordinary synchronization, peer delivery, or recovered state.
    #[default]
    Network,
    /// A local RPC submission, with its precomputed serialized size.
    LocalSubmission {
        /// Serialized block size in bytes.
        size: usize,
    },
}

/// A successfully committed block eligible for announcement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedBlockAnnouncement {
    /// Committed block hash.
    pub hash: block::Hash,
    /// Committed block height.
    pub height: block::Height,
    /// Source of this commit, including local block size when known.
    pub origin: BlockOrigin,
}

#[derive(Debug, Default)]
struct Pending {
    local: VecDeque<CommittedBlockAnnouncement>,
    ordinary: Option<CommittedBlockAnnouncement>,
    tip: Option<block::Hash>,
    closed: bool,
}

#[derive(Debug, Default)]
struct Shared {
    pending: Mutex<Pending>,
    changed: Notify,
}

/// The state's single-consumer committed announcement feed.
///
/// Local commits are retained in order, including commits outside the best chain. Ordinary
/// changes coalesce to the latest tip. Dropping a pending receive never consumes an announcement.
#[derive(Debug)]
pub struct BlockAnnouncements(Arc<Shared>);

/// Clones share one publisher lifetime; dropping the last publisher closes the receiver.
#[derive(Clone, Debug)]
pub(crate) struct BlockAnnouncementSender(Arc<Publisher>);

#[derive(Debug)]
struct Publisher(Weak<Shared>);

impl Drop for Publisher {
    fn drop(&mut self) {
        if let Some(shared) = self.0.upgrade() {
            shared
                .pending
                .lock()
                .expect("announcement lock is not poisoned")
                .closed = true;
            shared.changed.notify_one();
        }
    }
}

impl BlockAnnouncementSender {
    pub(crate) fn new() -> (Self, BlockAnnouncements) {
        let shared = Arc::new(Shared::default());
        (
            Self(Arc::new(Publisher(Arc::downgrade(&shared)))),
            BlockAnnouncements(shared),
        )
    }

    /// Publish the selected tip and successful commit as one indivisible routing decision.
    /// The caller publishes readable state before invoking this method.
    pub(crate) fn publish(
        &self,
        tip: Option<(block::Hash, block::Height)>,
        committed: Option<CommittedBlockAnnouncement>,
    ) {
        let Some(shared) = self.0 .0.upgrade() else {
            return;
        };
        let mut pending = shared
            .pending
            .lock()
            .expect("announcement lock is not poisoned");
        let local =
            committed.filter(|block| matches!(block.origin, BlockOrigin::LocalSubmission { .. }));
        if let Some(local) = local {
            pending.local.push_back(local);
        }
        if pending.tip != tip.map(|(hash, _)| hash) {
            pending.tip = tip.map(|(hash, _)| hash);
            pending.ordinary = tip.map(|(hash, height)| CommittedBlockAnnouncement {
                hash,
                height,
                origin: BlockOrigin::Network,
            });
        }
        if local.is_some_and(|local| Some(local.hash) == pending.tip) {
            pending.ordinary = None;
        }
        drop(pending);
        shared.changed.notify_one();
    }
}

impl BlockAnnouncements {
    /// Receive the next local commit, or the latest ordinary tip when `relay_ordinary` is true.
    /// Returns `None` after the publisher closes and all eligible announcements are drained.
    pub async fn recv(&mut self, relay_ordinary: bool) -> Option<CommittedBlockAnnouncement> {
        loop {
            let changed = self.0.changed.notified();
            {
                let mut pending = self
                    .0
                    .pending
                    .lock()
                    .expect("announcement lock is not poisoned");
                if let Some(local) = pending.local.pop_front() {
                    return Some(local);
                }
                if relay_ordinary {
                    if let Some(ordinary) = pending.ordinary.take() {
                        return Some(ordinary);
                    }
                }
                if pending.closed {
                    return None;
                }
            }
            changed.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(n: u8, local: bool) -> CommittedBlockAnnouncement {
        CommittedBlockAnnouncement {
            hash: block::Hash([n; 32]),
            height: block::Height(u32::from(n)),
            origin: if local {
                BlockOrigin::LocalSubmission { size: 100 }
            } else {
                BlockOrigin::Network
            },
        }
    }

    #[tokio::test]
    async fn local_tip_is_announced_once_without_ordinary_relay() {
        let (sender, mut receiver) = BlockAnnouncementSender::new();
        let local = block(1, true);
        sender.publish(Some((local.hash, local.height)), Some(local));
        assert_eq!(receiver.recv(true).await, Some(local));
        assert!(futures::poll!(Box::pin(receiver.recv(true))).is_pending());
        drop(sender);
        assert_eq!(receiver.recv(true).await, None);
    }

    #[tokio::test]
    async fn ordinary_updates_coalesce_and_local_side_blocks_are_retained() {
        let (sender, mut receiver) = BlockAnnouncementSender::new();
        for n in 1..=3 {
            let ordinary = block(n, false);
            let local = block(n + 10, true);
            sender.publish(Some((ordinary.hash, ordinary.height)), Some(local));
        }
        for n in 11..=13 {
            assert_eq!(receiver.recv(false).await, Some(block(n, true)));
        }
        assert!(futures::poll!(Box::pin(receiver.recv(false))).is_pending());
        assert_eq!(receiver.recv(true).await, Some(block(3, false)));
    }

    #[tokio::test]
    async fn a_new_local_tip_clears_an_older_pending_ordinary_tip() {
        let (sender, mut receiver) = BlockAnnouncementSender::new();
        let ordinary = block(1, false);
        sender.publish(Some((ordinary.hash, ordinary.height)), None);
        let local = block(2, true);
        sender.publish(Some((local.hash, local.height)), Some(local));
        assert_eq!(receiver.recv(true).await, Some(local));
        assert!(futures::poll!(Box::pin(receiver.recv(true))).is_pending());
    }

    #[tokio::test]
    async fn cancelling_receive_keeps_later_publication_and_close_drains_it() {
        let (sender, mut receiver) = BlockAnnouncementSender::new();
        {
            let wait = receiver.recv(true);
            tokio::pin!(wait);
            assert!(futures::poll!(wait).is_pending());
        }
        let local = block(1, true);
        sender.publish(Some((local.hash, local.height)), Some(local));
        let clone = sender.clone();
        drop(sender);
        assert_eq!(receiver.recv(true).await, Some(local));
        assert!(futures::poll!(Box::pin(receiver.recv(true))).is_pending());
        drop(clone);
        assert_eq!(receiver.recv(true).await, None);
    }

    #[tokio::test]
    async fn closed_feed_drains_every_queued_local_commit() {
        let (sender, mut receiver) = BlockAnnouncementSender::new();
        sender.publish(None, Some(block(1, true)));
        sender.publish(None, Some(block(2, true)));
        drop(sender);
        assert_eq!(receiver.recv(false).await, Some(block(1, true)));
        assert_eq!(receiver.recv(false).await, Some(block(2, true)));
        assert_eq!(receiver.recv(false).await, None);
    }

    #[tokio::test]
    async fn dropping_receiver_discards_backlog_without_retaining_blocks() {
        let (sender, receiver) = BlockAnnouncementSender::new();
        drop(receiver);
        sender.publish(None, Some(block(1, true)));
        assert!(sender.0 .0.upgrade().is_none());
    }
}
