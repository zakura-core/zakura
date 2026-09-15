//! Bounds discovery feedback and applies completed outcomes in request order.

use super::discovery_feedback::{
    Completion, DiscoveryFeedback, EXPIRED, NEUTRAL, PENDING, STALLED, VERIFIED,
};
use futures::task::AtomicWaker;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    task::Context,
    time::Duration,
};
use tokio::time::Instant;

const MAX_PENDING_PER_PEER: usize = 2;
const FEEDBACK_LIFETIME: Duration = Duration::from_secs(16 * 60);
const REPROBE_DELAY: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Connection {
    generation: u64,
    last_selected: u64,
    pending: VecDeque<(Instant, Arc<Completion>)>,
    reprobe_at: Option<Instant>,
}

use crate::PeerSocketAddr;

/// Consecutive empty or failed `FindBlocks` responses tolerated
/// before the peer set disconnects a peer.
pub(super) const FIND_RESPONSE_STALL_THRESHOLD: usize = 3;

#[derive(Default)]
pub(super) struct FindResponseStallTracker {
    counts: HashMap<PeerSocketAddr, usize>,
    connections: HashMap<PeerSocketAddr, Connection>,
    wake: Arc<AtomicWaker>,
    selection: u64,
}

impl FindResponseStallTracker {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Records a stall for `addr`. Returns `true` once the peer reaches
    /// [`FIND_RESPONSE_STALL_THRESHOLD`] — the caller must then disconnect it.
    /// On threshold the entry is removed, so a reconnected peer starts fresh.
    pub(super) fn record_stall(&mut self, addr: PeerSocketAddr) -> bool {
        let count = self.counts.entry(addr).or_default();
        *count += 1;

        if *count >= FIND_RESPONSE_STALL_THRESHOLD {
            self.counts.remove(&addr);
            true
        } else {
            false
        }
    }

    /// Clears tracking for a peer that sent a useful response or disconnected.
    pub(super) fn clear(&mut self, addr: PeerSocketAddr) {
        self.counts.remove(&addr);
        self.connections.remove(&addr);
    }

    pub(super) fn connection(&mut self, addr: PeerSocketAddr, generation: u64) {
        if self
            .connections
            .get(&addr)
            .is_some_and(|c| c.generation != generation)
        {
            self.clear(addr);
        }
        self.connections.entry(addr).or_insert_with(|| Connection {
            generation,
            ..Default::default()
        });
    }

    pub(super) fn last_selected(&self, addr: &PeerSocketAddr) -> u64 {
        self.connections
            .get(addr)
            .map_or(0, |connection| connection.last_selected)
    }

    pub(super) fn selected(&mut self, addr: PeerSocketAddr) {
        self.selection = self
            .selection
            .checked_add(1)
            .expect("selection sequence cannot exhaust during a process lifetime");
        if let Some(connection) = self.connections.get_mut(&addr) {
            connection.last_selected = self.selection;
        }
    }

    pub(super) fn eligible(&self, addr: &PeerSocketAddr) -> bool {
        self.connections.get(addr).is_none_or(|c| {
            c.reprobe_at
                .is_none_or(|deadline| Instant::now() >= deadline)
        })
    }

    pub(super) fn start(&mut self, addr: PeerSocketAddr) -> Option<DiscoveryFeedback> {
        // Checkpoint verification may need more than two discovery responses before
        // any block commits. Saturation skips feedback, never parent acquisition.
        if !self.eligible(&addr)
            || self.connections.get(&addr)?.pending.len() >= MAX_PENDING_PER_PEER
        {
            return None;
        }
        let completion = Arc::new(Completion {
            outcome: AtomicU8::new(PENDING),
            wake: self.wake.clone(),
        });
        self.connections
            .get_mut(&addr)?
            .pending
            .push_back((Instant::now() + FEEDBACK_LIFETIME, completion.clone()));
        Some(DiscoveryFeedback::new(completion))
    }

    pub(super) fn drain(&mut self, cx: &Context<'_>) -> Vec<PeerSocketAddr> {
        self.wake.register(cx.waker());
        let mut outcomes = Vec::new();
        for (&addr, connection) in &mut self.connections {
            while let Some((deadline, completion)) = connection.pending.front() {
                let mut outcome = completion.outcome.load(Ordering::Acquire);
                if outcome == PENDING && Instant::now() >= *deadline {
                    // Retained consumers cannot block request ordering indefinitely.
                    // Reprobe later without adding a misconduct score or clearing earlier stalls.
                    let _ = completion.outcome.compare_exchange(
                        PENDING,
                        EXPIRED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    outcome = completion.outcome.load(Ordering::Acquire);
                }
                if outcome == EXPIRED {
                    connection.reprobe_at = Some(Instant::now() + REPROBE_DELAY);
                }
                if outcome == PENDING {
                    break;
                }
                outcomes.push((addr, outcome));
                connection.pending.pop_front();
            }
        }
        let mut disconnect = Vec::new();
        for (addr, outcome) in outcomes {
            match outcome {
                STALLED if self.record_stall(addr) => disconnect.push(addr),
                VERIFIED => {
                    self.counts.remove(&addr);
                }
                _ => {}
            }
        }
        disconnect
    }

    pub(super) fn pause(&mut self) {
        for connection in self.connections.values_mut() {
            for (_, completion) in &connection.pending {
                let _ = completion.outcome.compare_exchange(
                    PENDING,
                    NEUTRAL,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            connection.reprobe_at = None;
        }
    }

    pub(super) fn retain(&mut self, mut live: impl FnMut(&PeerSocketAddr) -> bool) {
        self.connections.retain(|addr, _| live(addr));
        self.counts.retain(|addr, _| live(addr));
    }
}

#[cfg(test)]
mod tests;
