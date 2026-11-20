//! Tell the node about each block a legacy peer newly announces or starts sending.
//!
//! The node uses the first sighting of a block to rank its body, so this module only forwards
//! hashes. It does not depend on what the node does with them.

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use tokio::time::Instant;
use zakura_chain::block;

use crate::protocol::external::BlockHeaderHook;

/// The most sightings one connection hands to the hook in each [`ANNOUNCEMENT_WINDOW`].
///
/// Four calls leave room for both blocks in a tip race and for repeated or bunched
/// announcements, while bounding one connection's share of the hook's work and of the node's
/// claim store. `inv`s and `block` messages share the allowance.
const MAX_ANNOUNCEMENTS_PER_WINDOW: u32 = 4;

/// The window [`MAX_ANNOUNCEMENTS_PER_WINDOW`] applies to.
const ANNOUNCEMENT_WINDOW: Duration = Duration::from_secs(2);

/// What the node does with the blocks legacy peers show it.
///
/// Both methods run on a connection's inbound path, so they must return quickly.
pub trait BlockAnnouncements: Send + Sync {
    /// A legacy peer announced the block `hash` in a single-block `inv`.
    ///
    /// A multi-block `inv` is a reply to a query, not an announcement, so it is not passed on.
    fn announced(&self, hash: block::Hash);

    /// The block header at the start of a `block` message arrived, before the block's
    /// transactions, and hashes to `hash`.
    ///
    /// The message may have been requested or pushed, and may still fail its checksum or fail
    /// to decode. `hash` is of header bytes already received, so, like an announced hash, it
    /// cannot belong to a block that is mined later.
    fn block_header_arrived(&self, hash: block::Hash);
}

/// The node's [`BlockAnnouncements`], shared by every legacy connection. The default hook does
/// nothing.
#[derive(Clone, Default)]
pub struct BlockAnnouncementHook(Option<Arc<dyn BlockAnnouncements>>);

impl BlockAnnouncementHook {
    /// A hook that passes the blocks legacy peers show this node to `node`.
    pub fn new(node: impl BlockAnnouncements + 'static) -> Self {
        Self(Some(Arc::new(node)))
    }

    /// This hook as one connection uses it, limited per connection.
    pub(crate) fn for_connection(&self) -> ConnectionAnnouncements {
        ConnectionAnnouncements {
            hook: self.clone(),
            window: Arc::default(),
        }
    }
}

impl fmt::Debug for BlockAnnouncementHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BlockAnnouncementHook")
            .field(&self.0.is_some())
            .finish()
    }
}

/// One connection's calls to a [`BlockAnnouncementHook`].
///
/// Clones share one allowance, so the connection's message decoder, which sees block headers,
/// and its message filter, which sees `inv`s, are limited together.
#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectionAnnouncements {
    hook: BlockAnnouncementHook,
    window: Arc<Mutex<Window>>,
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
    pub(crate) fn announce(&self, hash: block::Hash) {
        if let Some(node) = self.admitted() {
            node.announced(hash);
        }
    }

    /// The function this connection's decoder calls with the hash of each block header that
    /// starts a `block` message, unless the hook is unset. Each call is passed to the hook
    /// unless this connection has used its share of the current window.
    pub(crate) fn block_header_hook(&self) -> Option<BlockHeaderHook> {
        self.hook.0.as_ref()?;
        let connection = self.clone();
        Some(Arc::new(move |hash| {
            if let Some(node) = connection.admitted() {
                node.block_header_arrived(hash);
            }
        }))
    }

    /// The node to call, if the hook is set and one more call fits in the current window. The
    /// call is counted.
    fn admitted(&self) -> Option<&dyn BlockAnnouncements> {
        let node = self.hook.0.as_deref()?;
        self.window
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .admit(Instant::now())
            .then_some(node)
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
