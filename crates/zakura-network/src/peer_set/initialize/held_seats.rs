//! Inbound seats that the listener holds for peers that just left.

use std::{collections::HashMap, net::IpAddr, time::Instant};

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
/// that all leave together cannot reserve the listener. A peer gains nothing by holding
/// a seat that it could not get by staying connected.
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

    /// Holds the seat of `closed` for its IP if the connection was long-lived and
    /// fewer than the limit are held.
    pub fn hold(&mut self, closed: ClosedConnection) {
        if closed.open_for < constants::MIN_HELD_SEAT_CONNECTION {
            return;
        }

        if self.release_at.len() < self.limit || self.release_at.contains_key(&closed.ip) {
            self.release_at
                .insert(closed.ip, closed.closed_at + constants::HELD_SEAT_DURATION);
            metrics::counter!("peer.inbound.held_seats.held").increment(1);
        }
    }

    /// Returns the number of seats held for IPs other than `ip` at `now`, after
    /// releasing the seats whose hold is over.
    pub fn held_for_others(&mut self, ip: IpAddr, now: Instant) -> usize {
        self.release_at.retain(|_ip, release_at| now < *release_at);

        self.release_at.len() - usize::from(self.release_at.contains_key(&ip))
    }

    /// Releases the seat held for `ip`, because its peer took it back.
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

        seats.hold(peer);
        assert_eq!(seats.held_for_others(other_ip, now), 1);
        assert_eq!(seats.held_for_others(peer.ip, now), 0);

        let last_held = now + constants::HELD_SEAT_DURATION - Duration::from_millis(1);
        assert_eq!(seats.held_for_others(other_ip, last_held), 1);

        seats.release(peer.ip);
        assert_eq!(seats.held_for_others(other_ip, last_held), 0);

        seats.hold(peer);
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

        seats.hold(closed(
            1,
            constants::MIN_HELD_SEAT_CONNECTION - Duration::from_millis(1),
            now,
        ));
        seats.hold(closed(2, Duration::ZERO, now));

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
                seats.hold(closed(host, constants::MIN_HELD_SEAT_CONNECTION, now));
            }

            assert_eq!(
                seats.held_for_others(other_ip, now),
                expected_held,
                "unexpected held seats with an inbound limit of {inbound_limit}"
            );
        }
    }
}
