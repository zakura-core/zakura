//! Outbound peers that Zakura dials again soon after a long-lived connection to them closes.
//!
//! A peer that restarts closes every connection, then reopens its listener with every inbound
//! seat empty, and on Mainnet other nodes take those seats within two to three minutes. The
//! address book would not dial the peer again for at least
//! [`MIN_PEER_RECONNECTION_DELAY`](constants::MIN_PEER_RECONNECTION_DELAY), and then only
//! after every address it has never tried. So the crawler dials the peer's address again at
//! the times in [`DROPPED_PEER_REDIAL_DELAYS`](constants::DROPPED_PEER_REDIAL_DELAYS), and
//! [`DroppedPeerRedials`] tracks when each redial is due.
//!
//! # Security
//!
//! These redials skip the reconnection delay, but they stay bounded:
//! - only a connection that was open for
//!   [`MIN_REDIALED_CONNECTION_AGE`](constants::MIN_REDIALED_CONNECTION_AGE) is redialed, so
//!   a peer causes at most one dial per redial delay for each such connection,
//! - each address has at most one pending set of redials, and at most the outbound
//!   connection limit are pending in total, and
//! - the crawler stops at the first success, skips banned and connected peers, and counts
//!   each redial against the outbound connection limit.

use std::{collections::HashMap, time::Duration};

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

    /// How long after the close each redial is due, in order.
    delays: Vec<Duration>,

    /// The most peer addresses with pending redials at one time.
    max_pending: usize,
}

/// The redials of one peer address.
#[derive(Copy, Clone, Debug)]
struct PendingRedial {
    /// When the connection to the peer closed.
    closed_at: Instant,

    /// The index in `delays` of the next redial.
    next: usize,

    /// Whether a redial is in progress.
    dialing: bool,
}

impl DroppedPeerRedials {
    /// Returns an empty set of redials, with room for `max_pending` peer addresses.
    pub fn new(max_pending: usize) -> Self {
        Self::with_policy(
            constants::MIN_REDIALED_CONNECTION_AGE,
            constants::DROPPED_PEER_REDIAL_DELAYS.to_vec(),
            max_pending,
        )
    }

    /// Returns an empty set of redials with the given policy.
    ///
    /// `delays` must be in time order.
    fn with_policy(
        min_connection_age: Duration,
        delays: Vec<Duration>,
        max_pending: usize,
    ) -> Self {
        Self {
            pending: HashMap::new(),
            min_connection_age,
            delays,
            max_pending,
        }
    }

    /// Schedules redials of the peer that `closed` was connected to.
    ///
    /// Returns `true` if redials are now pending. They are not if the connection was too
    /// short, if every redial time has already passed at `now`, if the address already has
    /// pending redials, or if the cap is reached.
    pub fn schedule(&mut self, closed: ClosedConnection, now: Instant) -> bool {
        let last_redial = self.delays.last().map(|delay| closed.closed_at + *delay);

        if closed.open_for < self.min_connection_age
            || last_redial.is_none_or(|last_redial| last_redial < now)
            || self.pending.len() >= self.max_pending
            || self.pending.contains_key(&closed.addr)
        {
            return false;
        }

        self.pending.insert(
            closed.addr,
            PendingRedial {
                closed_at: closed.closed_at,
                next: 0,
                dialing: false,
            },
        );

        true
    }

    /// Returns when the next redial is due, if any is waiting.
    pub fn next_due(&self) -> Option<Instant> {
        self.pending
            .values()
            .filter(|redial| !redial.dialing)
            .map(|redial| redial.closed_at + self.delays[redial.next])
            .min()
    }

    /// Returns the addresses whose next redial is due at `now`, and marks each one as dialing.
    ///
    /// If several redial times of an address have passed, it gets one redial, for the latest.
    pub fn take_due(&mut self, now: Instant) -> Vec<PeerSocketAddr> {
        let mut due = Vec::new();

        for (addr, redial) in &mut self.pending {
            if redial.dialing || redial.closed_at + self.delays[redial.next] > now {
                continue;
            }

            while redial.next + 1 < self.delays.len()
                && redial.closed_at + self.delays[redial.next + 1] <= now
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
