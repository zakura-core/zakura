//! Inbound seats held for long-lived peers that have just disconnected.
//!
//! Zakura accepts inbound connections up to a limit, and on Mainnet most listeners are at
//! it. A node that restarts frees its seat on every peer it had dialed, and another peer
//! takes each freed seat within seconds, so the restarted node is refused when it dials
//! again. [`HeldSeats`] keeps the seat of a long-lived inbound peer reserved for its IP
//! address for a short time after it disconnects.
//!
//! # Security
//!
//! A hold gives a peer nothing that staying connected would not:
//! - a seat is held only for an IP address that just occupied one, and only that address
//!   can take it back,
//! - the peer must have closed the connection itself: one that this node closed, to shed load
//!   or to drop a stalled or banned peer, holds no seat, so a hold never undoes this node's
//!   own decision to disconnect a peer,
//! - a ban releases the peer's held seat,
//! - the connection must have been open for the minimum age, and the hold is no longer
//!   than that age, so a peer holds a seat for at most as long as it used it,
//! - each IP address has at most one held seat, and
//! - the number of held seats is capped, so holds keep only a bounded part of the inbound
//!   limit from new peers, and use bounded memory.

use std::{collections::HashMap, net::IpAddr, time::Duration};

use tokio::time::Instant;

use crate::{constants, peer_set::ClosedConnection, BannedIps};

#[cfg(test)]
mod tests;

/// The inbound seats that are reserved for peers that have just disconnected.
#[derive(Debug)]
pub struct HeldSeats {
    /// When the seat held for each IP address is released.
    release_times: HashMap<IpAddr, Instant>,

    /// How long a connection must have been open for its seat to be held.
    min_connection_age: Duration,

    /// How long a seat is held after its connection closes.
    hold_duration: Duration,

    /// The most seats held at one time.
    max_held: usize,

    /// The banned IP addresses, which hold no seats.
    bans: BannedIps,
}

impl HeldSeats {
    /// Returns an empty set of held seats for a listener that accepts
    /// `inbound_connection_limit` connections and refuses `bans`.
    pub fn new(inbound_connection_limit: usize, bans: BannedIps) -> Self {
        Self::with_policy(
            constants::MIN_HELD_SEAT_CONNECTION_AGE,
            constants::INBOUND_SEAT_HOLD_DURATION,
            inbound_connection_limit / constants::INBOUND_SEAT_HOLD_LIMIT_DIVISOR,
            bans,
        )
    }

    /// Returns an empty set of held seats with the given policy.
    fn with_policy(
        min_connection_age: Duration,
        hold_duration: Duration,
        max_held: usize,
        bans: BannedIps,
    ) -> Self {
        Self {
            release_times: HashMap::new(),
            min_connection_age,
            hold_duration,
            max_held,
            bans,
        }
    }

    /// Holds the seat that `closed` left, until `hold_duration` after it closed.
    ///
    /// Returns `true` if the seat is held at `now`. A seat is not held if the connection
    /// was too short, if the hold is already over, if its IP address already has a held
    /// seat or is banned, or if the cap is reached.
    pub fn hold(&mut self, closed: ClosedConnection, now: Instant) -> bool {
        self.release(now);

        let release_time = closed.closed_at + self.hold_duration;

        if closed.open_for < self.min_connection_age
            || release_time <= now
            || self.release_times.len() >= self.max_held
            || self.release_times.contains_key(&closed.addr.ip())
            || self.bans.contains(closed.addr.ip())
        {
            return false;
        }

        self.release_times.insert(closed.addr.ip(), release_time);

        true
    }

    /// Returns the number of seats held at `now` for IP addresses other than `ip`.
    ///
    /// A new connection from `ip` must leave these seats empty.
    pub fn held_for_others(&mut self, ip: IpAddr, now: Instant) -> usize {
        self.release(now);

        self.release_times.len() - usize::from(self.release_times.contains_key(&ip))
    }

    /// Releases the seat held for `ip`, because a connection from `ip` is taking it.
    ///
    /// Returns `true` if a seat was held for `ip` at `now`.
    pub fn take_back(&mut self, ip: IpAddr, now: Instant) -> bool {
        self.release(now);

        self.release_times.remove(&ip).is_some()
    }

    /// Returns the number of seats held, including any released since the last call that
    /// took the current time.
    pub fn len(&self) -> usize {
        self.release_times.len()
    }

    /// Releases every seat whose hold has ended by `now`, or whose IP address is now banned.
    ///
    /// A ban can arrive after a seat is held, because misbehavior reports are applied in
    /// batches. The banned peer is refused before it can take its seat back.
    fn release(&mut self, now: Instant) {
        let bans = &self.bans;
        self.release_times
            .retain(|ip, release_time| *release_time > now && !bans.contains(*ip));
    }
}
