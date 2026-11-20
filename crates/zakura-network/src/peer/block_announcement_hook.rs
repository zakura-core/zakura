//! Tell the node about each block a legacy peer newly announces.
//!
//! The node uses the first announcement of a block to rank its body, so this module only
//! forwards hashes. It does not depend on what the node does with them.

use std::{fmt, sync::Arc, time::Duration};

use tokio::time::Instant;
use zakura_chain::block;

/// The most announcements one connection hands to the hook in each [`ANNOUNCEMENT_WINDOW`].
///
/// Four calls leave room for both blocks in a tip race and for repeated or bunched
/// announcements, while bounding one connection's share of the hook's work.
const MAX_ANNOUNCEMENTS_PER_WINDOW: u32 = 4;

/// The window [`MAX_ANNOUNCEMENTS_PER_WINDOW`] applies to.
const ANNOUNCEMENT_WINDOW: Duration = Duration::from_secs(2);

/// Called with the hash of each block a legacy peer announces in a single-block `inv`.
///
/// A multi-block `inv` is a reply to a query, not an announcement, so it is not passed on. The
/// default hook does nothing.
#[derive(Clone, Default)]
pub struct BlockAnnouncementHook(Option<Arc<dyn Fn(block::Hash) + Send + Sync>>);

impl BlockAnnouncementHook {
    /// A hook that calls `hook` for each announced block hash.
    ///
    /// `hook` runs on the connection's inbound path, so it must return quickly.
    pub fn new(hook: impl Fn(block::Hash) + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(hook)))
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

/// One connection's announcements to a [`BlockAnnouncementHook`].
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
        let Some(hook) = &self.hook.0 else {
            return;
        };
        if self.window.admit(Instant::now()) {
            hook(hash);
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
