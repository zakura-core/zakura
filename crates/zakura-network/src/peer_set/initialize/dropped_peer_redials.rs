//! Outbound peers that Zakura dials again soon after a long-lived connection to them closes.
//!
//! A peer that restarts closes every connection, then reopens its listener with every inbound
//! seat empty, and on Mainnet other nodes take those seats within two to three minutes. The
//! address book would not dial the peer again for at least
//! [`MIN_PEER_RECONNECTION_DELAY`](constants::MIN_PEER_RECONNECTION_DELAY), and then only
//! after every address it has never tried. So the crawler dials the peer's address again at
//! about the times in [`DROPPED_PEER_REDIAL_DELAYS`](constants::DROPPED_PEER_REDIAL_DELAYS),
//! scaled for each peer by up to
//! [`DROPPED_PEER_REDIAL_JITTER`](constants::DROPPED_PEER_REDIAL_JITTER), and
//! [`DroppedPeerRedials`] tracks when each redial is due.
//!
//! This is best effort. A Zakura listener with the default per-IP limit refuses an IP address
//! for `MIN_PEER_RECONNECTION_DELAY` after it admits a connection from it, so once a redial
//! reaches the restarted peer and its handshake fails, the later redials are refused too, and
//! the address book's own reconnection takes over.
//!
//! # Security
//!
//! These redials skip the reconnection delay, but they stay bounded:
//! - only a connection that its peer closed is redialed: one that this node closed, to shed
//!   load or to drop a stalled or banned peer, is not, so a redial never undoes this node's
//!   own decision to disconnect a peer,
//! - only a connection that was open for
//!   [`MIN_REDIALED_CONNECTION_AGE`](constants::MIN_REDIALED_CONNECTION_AGE) is redialed, so
//!   a peer causes at most one dial per redial delay for each such connection,
//! - each address has at most one pending set of redials, and at most the outbound
//!   connection limit are pending in total, and
//! - the crawler stops at the first success, counts each redial against the outbound
//!   connection limit, and right before each dial skips a banned or connected peer, and a
//!   peer that another dial is attempting.

use std::{collections::HashMap, time::Duration};

use rand_10::{rng, RngExt};
use tokio::time::Instant;

use crate::{constants, peer_set::ClosedConnection, PeerSocketAddr};

#[cfg(test)]
mod tests;

/// The redials that are pending for outbound peers whose long-lived connections closed.
#[derive(Debug)]
pub struct DroppedPeerRedials {
    /// The pending redials of each peer address.
    pending: HashMap<PeerSocketAddr, PendingRedial>,

    /// How long a connection must have been open for its peer to be redialed.
    min_connection_age: Duration,

    /// How long after the close each redial is due, in order, before jitter.
    delays: Vec<Duration>,

    /// The most that each peer's delays are scaled up or down, at random, as a fraction.
    jitter: f64,

    /// The most peer addresses with pending redials at one time.
    max_pending: usize,
}

/// The redials of one peer address.
#[derive(Copy, Clone, Debug)]
struct PendingRedial {
    /// When the connection to the peer closed.
    closed_at: Instant,

    /// The random factor that scales this peer's delays.
    stretch: f64,

    /// The index in `delays` of the next redial.
    next: usize,

    /// Whether a redial is in progress.
    dialing: bool,
}

impl PendingRedial {
    /// Returns when this peer's redial `delay` after the close is due.
    fn due_at(&self, delay: Duration) -> Instant {
        self.closed_at + delay.mul_f64(self.stretch)
    }
}

impl DroppedPeerRedials {
    /// Returns an empty set of redials, with room for `max_pending` peer addresses.
    pub fn new(max_pending: usize) -> Self {
        Self::with_policy(
            constants::MIN_REDIALED_CONNECTION_AGE,
            constants::DROPPED_PEER_REDIAL_DELAYS.to_vec(),
            constants::DROPPED_PEER_REDIAL_JITTER,
            max_pending,
        )
    }

    /// Returns an empty set of redials with the given policy.
    ///
    /// `delays` must be in time order, and `jitter` must be at least 0 and less than 1.
    fn with_policy(
        min_connection_age: Duration,
        delays: Vec<Duration>,
        jitter: f64,
        max_pending: usize,
    ) -> Self {
        Self {
            pending: HashMap::new(),
            min_connection_age,
            delays,
            jitter,
            max_pending,
        }
    }

    /// Schedules redials of the peer that `closed` was connected to, with its delays scaled by
    /// a random factor within `jitter` of 1.
    ///
    /// Returns `true` if redials are now pending. They are not if the connection was too
    /// short, if every redial time has already passed at `now`, if the address already has
    /// pending redials, or if the cap is reached.
    pub fn schedule(&mut self, closed: ClosedConnection, now: Instant) -> bool {
        let redial = PendingRedial {
            closed_at: closed.closed_at,
            stretch: 1.0 + rng().random_range(-self.jitter..=self.jitter),
            next: 0,
            dialing: false,
        };
        let last_redial = self.delays.last().map(|delay| redial.due_at(*delay));

        if closed.open_for < self.min_connection_age
            || last_redial.is_none_or(|last_redial| last_redial < now)
            || self.pending.len() >= self.max_pending
            || self.pending.contains_key(&closed.addr)
        {
            return false;
        }

        self.pending.insert(closed.addr, redial);

        true
    }

    /// Returns when the next redial is due, if any is waiting.
    pub fn next_due(&self) -> Option<Instant> {
        self.pending
            .values()
            .filter(|redial| !redial.dialing)
            .map(|redial| redial.due_at(self.delays[redial.next]))
            .min()
    }

    /// Returns the addresses whose next redial is due at `now`, and marks each one as dialing.
    ///
    /// If several redial times of an address have passed, it gets one redial, for the latest.
    pub fn take_due(&mut self, now: Instant) -> Vec<PeerSocketAddr> {
        let mut due = Vec::new();

        for (addr, redial) in &mut self.pending {
            if redial.dialing || redial.due_at(self.delays[redial.next]) > now {
                continue;
            }

            while redial.next + 1 < self.delays.len()
                && redial.due_at(self.delays[redial.next + 1]) <= now
            {
                redial.next += 1;
            }

            redial.next += 1;
            redial.dialing = true;
            due.push(*addr);
        }

        due
    }

    /// Records that a redial of `addr` failed. Stops redialing it if no redial is left.
    pub fn failed(&mut self, addr: PeerSocketAddr) {
        let Some(redial) = self.pending.get_mut(&addr) else {
            return;
        };

        if redial.next >= self.delays.len() {
            self.pending.remove(&addr);
        } else {
            redial.dialing = false;
        }
    }

    /// Stops redialing `addr`, because it is connected or must not be dialed.
    pub fn stop(&mut self, addr: PeerSocketAddr) {
        self.pending.remove(&addr);
    }

    /// Returns the number of peer addresses with pending redials.
    pub fn len(&self) -> usize {
        self.pending.len()
    }
}
