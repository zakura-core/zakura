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
/// announcements, while bounding one connection's share of the hook's work and of the node's
/// claim store. `inv`s and `block` messages share the allowance, but only `block` messages the
/// hook reports as near the tip count, so blocks a peer serves during catch-up do not use it.
const MAX_ANNOUNCEMENTS_PER_WINDOW: u32 = 4;

/// The window [`MAX_ANNOUNCEMENTS_PER_WINDOW`] applies to.
const ANNOUNCEMENT_WINDOW: Duration = Duration::from_secs(2);

/// What the node does with the blocks legacy peers announce or start sending.
///
/// Every method runs on a connection's inbound path, so it must return quickly.
pub trait BlockAnnouncements: Send + Sync {
    /// A legacy peer announced the block `hash` in a single-block `inv`.
    ///
    /// A multi-block `inv` is a reply to a query, not an announcement, so it is not passed on.
    fn announced(&self, hash: block::Hash);

    /// The header of a `block` message was read, before any of its body was waited for.
    ///
    /// Returns the place the node reserves for the block, or `None` if the node cannot rank
    /// blocks yet. Every `block` message reserves, whether it was requested or pushed.
    fn block_started(&self) -> Option<BlockReservation>;

    /// The `block` message that reserved `reservation` passed its checksum and decoded to the
    /// block `hash` at coinbase `height`. A message that fails either is not passed on, so its
    /// reservation goes unused.
    ///
    /// Returns whether the block was near enough to the tip to need a claim. The calls that
    /// return `true` share each connection's announcement allowance.
    fn block_received(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
        reservation: BlockReservation,
    ) -> bool;
}

/// The node's [`BlockAnnouncements`], shared by every legacy connection. The default hook does
/// nothing.
#[derive(Clone, Default)]
pub struct BlockAnnouncementHook(Option<Arc<dyn BlockAnnouncements>>);

impl BlockAnnouncementHook {
    /// A hook that passes announced and received blocks to `node`.
    pub fn new(node: impl BlockAnnouncements + 'static) -> Self {
        Self(Some(Arc::new(node)))
    }

    /// The function a connection's codec calls when a `block` message starts arriving, if this
    /// hook is set.
    pub(crate) fn block_order_reservation(&self) -> Option<ReserveBlockOrder> {
        let node = self.0.clone()?;
        Some(Arc::new(move || node.block_started()))
    }

    /// This hook as one connection uses it, limited per connection.
    pub(crate) fn for_connection(&self) -> ConnectionAnnouncements {
        ConnectionAnnouncements {
            hook: self.clone(),
            window: Window::default(),
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
#[derive(Debug, Default)]
pub(crate) struct ConnectionAnnouncements {
    hook: BlockAnnouncementHook,
    window: Window,
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
        let Some(node) = &self.hook.0 else {
            return;
        };
        self.window.call(Instant::now(), || {
            node.announced(hash);
            true
        });
    }

    /// Passes the block `hash` at coinbase `height`, received in a `block` message that
    /// reserved `reservation`, to the hook, unless this connection has used its share of the
    /// current window.
    pub(crate) fn block_received(
        &mut self,
        hash: block::Hash,
        height: Option<block::Height>,
        reservation: BlockReservation,
    ) {
        let Some(node) = &self.hook.0 else {
            return;
        };
        self.window.call(Instant::now(), || {
            node.block_received(hash, height, reservation)
        });
    }
}

impl Window {
    /// Runs `call` at `now` if it fits in the window, and counts it if it returns `true`.
    fn call(&mut self, now: Instant, call: impl FnOnce() -> bool) {
        let expired = self
            .started
            .is_none_or(|started| now.saturating_duration_since(started) >= ANNOUNCEMENT_WINDOW);
        if expired {
            self.started = Some(now);
            self.calls = 0;
        }
        if self.calls >= MAX_ANNOUNCEMENTS_PER_WINDOW {
            return;
        }
        if call() {
            self.calls += 1;
        }
    }
}
