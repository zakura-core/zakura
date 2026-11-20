//! Tell the node about each block a legacy peer newly announces or starts sending.
//!
//! The node uses the earliest announcement of a block to rank its body, so this module only
//! forwards hashes and reservations. It does not depend on what the node does with them.

use std::{fmt, sync::Arc, time::Duration};

use tokio::time::Instant;
use zakura_chain::block;

use crate::protocol::external::{BlockReservation, ReserveBlockOrder};

/// The most calls one connection makes to each part of the hook in each
/// [`ANNOUNCEMENT_WINDOW`].
///
/// Four calls leave room for both blocks in a tip race and for repeated or bunched
/// announcements, while bounding one connection's share of the hook's work. `inv`s and `block`
/// messages are limited separately, so blocks a peer serves during catch-up cannot use up the
/// calls for its next announcement.
const MAX_ANNOUNCEMENTS_PER_WINDOW: u32 = 4;

/// The window [`MAX_ANNOUNCEMENTS_PER_WINDOW`] applies to.
const ANNOUNCEMENT_WINDOW: Duration = Duration::from_secs(2);

/// Called with the hash of each block a legacy peer announces in a single-block `inv`, and,
/// if set up with [`BlockAnnouncementHook::with_block_messages`], for each `block` message.
///
/// A multi-block `inv` is a reply to a query, not an announcement, so it is not passed on. The
/// default hook does nothing.
#[derive(Clone, Default)]
pub struct BlockAnnouncementHook {
    announced: Option<Arc<dyn Fn(block::Hash) + Send + Sync>>,
    block_messages: Option<BlockMessageHook>,
}

/// The hook's calls for received `block` messages.
#[derive(Clone)]
struct BlockMessageHook {
    reserve: ReserveBlockOrder,
    received: Arc<dyn Fn(block::Hash, BlockReservation) + Send + Sync>,
}

impl BlockAnnouncementHook {
    /// A hook that calls `hook` for each announced block hash.
    ///
    /// `hook` runs on the connection's inbound path, so it must return quickly.
    pub fn new(hook: impl Fn(block::Hash) + Send + Sync + 'static) -> Self {
        Self {
            announced: Some(Arc::new(hook)),
            block_messages: None,
        }
    }

    /// Also calls `reserve` when the header of each received `block` message is read, and
    /// `received` with the block's hash and that reservation once the whole message has passed
    /// its checksum and decoded. A message that fails either is not passed on, so its
    /// reservation goes unused.
    ///
    /// Every `block` message reserves, whether it was requested or pushed, but `received` is
    /// limited per connection like announcements. Both run on the connection's inbound path, so
    /// they must return quickly.
    pub fn with_block_messages(
        mut self,
        reserve: impl Fn() -> u64 + Send + Sync + 'static,
        received: impl Fn(block::Hash, BlockReservation) + Send + Sync + 'static,
    ) -> Self {
        self.block_messages = Some(BlockMessageHook {
            reserve: Arc::new(reserve),
            received: Arc::new(received),
        });
        self
    }

    /// The function a connection's codec calls when a `block` message starts arriving, if this
    /// hook wants them.
    pub(crate) fn block_order_reservation(&self) -> Option<ReserveBlockOrder> {
        self.block_messages
            .as_ref()
            .map(|block_messages| block_messages.reserve.clone())
    }

    /// This hook as one connection uses it, limited per connection.
    pub(crate) fn for_connection(&self) -> ConnectionAnnouncements {
        ConnectionAnnouncements {
            hook: self.clone(),
            announcement_window: Window::default(),
            block_message_window: Window::default(),
        }
    }
}

impl fmt::Debug for BlockAnnouncementHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockAnnouncementHook")
            .field("announced", &self.announced.is_some())
            .field("block_messages", &self.block_messages.is_some())
            .finish()
    }
}

/// One connection's calls to a [`BlockAnnouncementHook`].
#[derive(Debug, Default)]
pub(crate) struct ConnectionAnnouncements {
    hook: BlockAnnouncementHook,
    announcement_window: Window,
    block_message_window: Window,
}

/// The current fixed window of hook calls.
#[derive(Debug, Default)]
struct Window {
    started: Option<Instant>,
    calls: u32,
}

impl ConnectionAnnouncements {
    /// Passes the announced block `hash` to the hook, unless this connection has used its
    /// share of the current window.
    pub(crate) fn announce(&mut self, hash: block::Hash) {
        let Some(hook) = &self.hook.announced else {
            return;
        };
        if self.announcement_window.admit(Instant::now()) {
            hook(hash);
        }
    }

    /// Passes the block `hash`, received in a `block` message that reserved `reservation`, to
    /// the hook, unless this connection has used its share of the current window.
    pub(crate) fn block_received(&mut self, hash: block::Hash, reservation: BlockReservation) {
        let Some(block_messages) = &self.hook.block_messages else {
            return;
        };
        if self.block_message_window.admit(Instant::now()) {
            (block_messages.received)(hash, reservation);
        }
    }
}

impl Window {
    /// Counts a call at `now`, returning whether it fits in the window.
    fn admit(&mut self, now: Instant) -> bool {
        let expired = self
            .started
            .is_none_or(|started| now.saturating_duration_since(started) >= ANNOUNCEMENT_WINDOW);
        if expired {
            self.started = Some(now);
            self.calls = 0;
        }
        if self.calls >= MAX_ANNOUNCEMENTS_PER_WINDOW {
            return false;
        }
        self.calls += 1;
        true
    }
}
