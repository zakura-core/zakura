//! Inbound seats that the listener holds for peers that just left.

use std::{collections::HashMap, net::IpAddr};

use tokio::time::Instant;

use crate::{constants, peer_set::limit::ClosedConnection};

/// The inbound seats of long-lived peers whose connections closed a short time ago.
///
/// A full listener gives a free seat to the next peer that dials, which takes a few
/// seconds on Mainnet. A peer that restarts takes longer than that to dial back, so it
/// would find its seat taken. The listener holds the seat of a connection that was open
/// for [`constants::MIN_HELD_SEAT_CONNECTION`] for [`constants::HELD_SEAT_DURATION`].
/// Only the IP that left can take a held seat.
///
/// At most `inbound limit / HELD_SEAT_LIMIT_DIVISOR` seats are held at once, so peers
/// that all leave together cannot reserve the listener.
///
/// A hold does not depend on why the connection closed. A peer that this node dropped,
/// for example because it stalled, also gets one. That peer could already take the free
/// seat back by dialing again at once, because the listener keeps no record of dropped
/// peers. A banned peer is refused before it reaches its held seat, so that seat stays
/// empty until the hold is over.
#[derive(Debug)]
pub struct HeldSeats {
    /// When each held seat is released, by the IP it is held for.
    release_at: HashMap<IpAddr, Instant>,

    /// The most seats held at once.
    limit: usize,
}

impl HeldSeats {
    /// Returns an empty set of held seats for a listener with `inbound_limit` seats.
    pub fn new(inbound_limit: usize) -> Self {
        Self {
            release_at: HashMap::new(),
            limit: inbound_limit / constants::HELD_SEAT_LIMIT_DIVISOR,
        }
    }

    /// Holds the seat of `closed` for its IP if the connection was long-lived, its hold
    /// is not already over at `now`, and fewer than the limit are held.
    pub fn hold(&mut self, closed: ClosedConnection, now: Instant) {
        let release_at = closed.closed_at + constants::HELD_SEAT_DURATION;
        if closed.open_for < constants::MIN_HELD_SEAT_CONNECTION || release_at <= now {
            return;
        }

        // A hold that is over must not count towards the limit.
        self.release_expired(now);

        if self.release_at.len() < self.limit || self.release_at.contains_key(&closed.ip) {
            self.release_at.insert(closed.ip, release_at);
            metrics::counter!("peer.inbound.held_seats.held").increment(1);
        }
    }

    /// Returns the number of seats held for IPs other than `ip` at `now`, after
    /// releasing the seats whose hold is over.
    pub fn held_for_others(&mut self, ip: IpAddr, now: Instant) -> usize {
        self.release_expired(now);

        self.release_at.len() - usize::from(self.release_at.contains_key(&ip))
    }

    /// Releases the seats whose hold is over at `now`.
    fn release_expired(&mut self, now: Instant) {
        self.release_at.retain(|_ip, release_at| now < *release_at);
    }

    /// Releases the seat held for `ip`, because its peer took it back.
    ///
    /// The hold is over even if the peer's handshake then fails. The listener refuses
    /// another connection from that IP for [`constants::MIN_PEER_RECONNECTION_DELAY`]
    /// either way, which is most of [`constants::HELD_SEAT_DURATION`].
    pub fn release(&mut self, ip: IpAddr) {
        if self.release_at.remove(&ip).is_some() {
            metrics::counter!("peer.inbound.held_seats.taken_back").increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, time::Duration};

    use super::*;

    /// Returns a connection from `10.0.0.<host>` that closed at `closed_at` after
    /// `open_for`.
    fn closed(host: u8, open_for: Duration, closed_at: Instant) -> ClosedConnection {
        ClosedConnection {
            ip: Ipv4Addr::new(10, 0, 0, host).into(),
            open_for,
            closed_at,
        }
    }

    /// A long-lived peer's seat is held for that peer only, until the hold ends or
    /// the peer takes it back.
    #[test]
    fn holds_a_long_lived_peers_seat_for_its_ip() {
        let now = Instant::now();
        let peer = closed(1, constants::MIN_HELD_SEAT_CONNECTION, now);
        let other_ip = closed(2, Duration::ZERO, now).ip;
        let mut seats = HeldSeats::new(100);

        seats.hold(peer, now);
        assert_eq!(seats.held_for_others(other_ip, now), 1);
        assert_eq!(seats.held_for_others(peer.ip, now), 0);

        let last_held = now + constants::HELD_SEAT_DURATION - Duration::from_millis(1);
        assert_eq!(seats.held_for_others(other_ip, last_held), 1);

        seats.release(peer.ip);
        assert_eq!(seats.held_for_others(other_ip, last_held), 0);

        seats.hold(peer, now);
        assert_eq!(
            seats.held_for_others(other_ip, now + constants::HELD_SEAT_DURATION),
            0,
            "the hold is over"
        );
    }

    /// A connection that was not open long enough holds no seat.
    #[test]
    fn short_connections_hold_no_seat() {
        let now = Instant::now();
        let mut seats = HeldSeats::new(100);

        seats.hold(
            closed(
                1,
                constants::MIN_HELD_SEAT_CONNECTION - Duration::from_millis(1),
                now,
            ),
            now,
        );
        seats.hold(closed(2, Duration::ZERO, now), now);

        assert_eq!(
            seats.held_for_others(closed(3, Duration::ZERO, now).ip, now),
            0
        );
    }

    /// The held seats are capped at a share of the inbound limit, and a listener with
    /// too few seats holds none.
    #[test]
    fn held_seats_are_capped() {
        let now = Instant::now();
        let other_ip = closed(200, Duration::ZERO, now).ip;

        for (inbound_limit, expected_held) in [(9, 0), (10, 1), (30, 3), (300, 30)] {
            let mut seats = HeldSeats::new(inbound_limit);
            for host in 0..100 {
                seats.hold(closed(host, constants::MIN_HELD_SEAT_CONNECTION, now), now);
            }

            assert_eq!(
                seats.held_for_others(other_ip, now),
                expected_held,
                "unexpected held seats with an inbound limit of {inbound_limit}"
            );
        }
    }

    /// A hold that is over frees its place under the limit for the next peer that
    /// leaves.
    #[test]
    fn an_expired_hold_does_not_count_towards_the_limit() {
        let now = Instant::now();
        let expired = now + constants::HELD_SEAT_DURATION;
        let first = closed(1, constants::MIN_HELD_SEAT_CONNECTION, now);
        let second = closed(2, constants::MIN_HELD_SEAT_CONNECTION, expired);
        // An inbound limit of 10 holds one seat.
        let mut seats = HeldSeats::new(10);

        seats.hold(first, now);
        seats.hold(second, expired);

        assert_eq!(seats.held_for_others(first.ip, expired), 1);
        assert_eq!(seats.held_for_others(second.ip, expired), 0);
    }
}
