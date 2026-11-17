//! The schedule that scans a waiter map for abandoned waiters.

/// The fewest queued waiters between scans.
pub const MIN_QUEUES_BETWEEN_PRUNES: usize = 1_024;

/// Decides when a waiter map scans for abandoned waiters.
///
/// A scan costs the live waiters plus the calls since the last scan, so it runs after at
/// least as many calls as the live waiters it kept. This keeps the cost of each call constant.
#[derive(Debug, Default)]
pub struct PruneCadence {
    queued_since_prune: usize,
    live_after_prune: usize,
}

impl PruneCadence {
    /// Count one queued waiter and return whether the map should scan now.
    pub fn queued(&mut self) -> bool {
        self.queued_since_prune = self.queued_since_prune.saturating_add(1);
        self.queued_since_prune >= self.live_after_prune.max(MIN_QUEUES_BETWEEN_PRUNES)
    }

    /// Record a finished scan that kept `live` waiters.
    pub fn pruned(&mut self, live: usize) {
        self.queued_since_prune = 0;
        self.live_after_prune = live;
    }
}
