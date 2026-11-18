//! Whether to pass over a peer that missed an announced block.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Shared with outstanding requests because a disconnect can precede their results.
#[derive(Clone, Debug, Default)]
pub(crate) struct MissedBlock {
    state: Arc<Mutex<State>>,
    generation: u64,
}

#[derive(Debug, Default)]
struct State {
    missed: Option<Instant>,
    generation: u64,
    /// A late failure from a closed connection cannot undo a delivery on its replacement.
    last_delivery_generation: Option<u64>,
    latest_dispatch: Option<Instant>,
    pending: usize,
    /// On reconnect, ignore late results for requests older than the retention window.
    oldest_dispatch: Option<Instant>,
}

impl MissedBlock {
    pub(crate) fn is_set(&self) -> bool {
        self.state
            .lock()
            .expect("missed block lock is never poisoned")
            .missed
            .is_some()
    }

    pub(crate) fn delivered(&self) {
        let mut state = self
            .state
            .lock()
            .expect("missed block lock is never poisoned");
        if self.generation != state.generation {
            return;
        }
        state.missed = None;
        state.last_delivery_generation = Some(state.generation);
    }

    /// Keeps the flag reachable if the socket closes before the peer set receives its result.
    pub(crate) fn request(&self, now: Instant) -> AnnouncedBlockRequest {
        let mut state = self
            .state
            .lock()
            .expect("missed block lock is never poisoned");
        if self.generation != state.generation {
            return AnnouncedBlockRequest {
                source: None,
                started: now,
            };
        }
        state.pending += 1;
        state.latest_dispatch = Some(now);
        AnnouncedBlockRequest {
            source: Some(self.clone()),
            started: now,
        }
    }

    /// A pending request can still set the flag after disconnect. Use its dispatch time so
    /// repeated reconnects or a delayed consumer cannot renew the retention window.
    pub(crate) fn last_activity(&self) -> Option<Instant> {
        let state = self
            .state
            .lock()
            .expect("missed block lock is never poisoned");
        state.missed.max(state.latest_dispatch)
    }

    pub(crate) fn reconnect(&mut self, now: Instant, max_age: Duration) {
        let mut state = self
            .state
            .lock()
            .expect("missed block lock is never poisoned");
        state.missed = state
            .missed
            .filter(|time| now.saturating_duration_since(*time) <= max_age);
        state.oldest_dispatch = now.checked_sub(max_age);
        state.generation = state
            .generation
            .checked_add(1)
            .expect("a peer cannot reconnect u64::MAX times");
        self.generation = state.generation;
    }
}

/// One gossip request to a peer that announced the block. Dropping a cancelled request does
/// not mark a miss.
#[derive(Debug)]
pub(crate) struct AnnouncedBlockRequest {
    source: Option<MissedBlock>,
    started: Instant,
}

impl AnnouncedBlockRequest {
    pub(crate) fn missed(&self) {
        let Some(source) = &self.source else { return };
        let mut state = source
            .state
            .lock()
            .expect("missed block lock is never poisoned");
        if state
            .last_delivery_generation
            .is_none_or(|generation| generation <= source.generation)
            && state
                .oldest_dispatch
                .is_none_or(|oldest| self.started >= oldest)
        {
            state.missed = state.missed.max(Some(self.started));
        }
    }
}

impl Drop for AnnouncedBlockRequest {
    fn drop(&mut self) {
        let Some(source) = &self.source else { return };
        let mut state = source
            .state
            .lock()
            .expect("missed block lock is never poisoned");
        state.pending -= 1;
        if state.pending == 0 {
            state.latest_dispatch = None;
        }
    }
}
