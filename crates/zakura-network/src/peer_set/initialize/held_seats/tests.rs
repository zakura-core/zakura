//! Fixed test vectors for held inbound seats.

use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use tokio::time::Instant;

use zakura_chain::parameters::Network;

use crate::{constants, meta_addr::MetaAddr, peer_set::ClosedConnection, AddressBook, BannedIps};

use super::HeldSeats;

/// The shortest connection that earns a held seat in these tests.
const MIN_AGE: Duration = Duration::from_secs(600);

/// How long a seat is held in these tests.
const HOLD: Duration = Duration::from_secs(180);

/// The smallest step of the clock.
const TICK: Duration = Duration::from_nanos(1);

/// Returns the test peer IP address with `last_octet`.
fn ip(last_octet: u8) -> IpAddr {
    IpAddr::from([192, 0, 2, last_octet])
}

/// Returns a connection from `ip` that closed at `closed_at` after being open for `open_for`.
fn closed(ip: IpAddr, open_for: Duration, closed_at: Instant) -> ClosedConnection {
    ClosedConnection {
        ip,
        open_for,
        closed_at,
    }
}

/// Returns held seats with the test policy, room for `max_held` seats, and no bans.
fn held_seats(max_held: usize) -> HeldSeats {
    HeldSeats::with_policy(MIN_AGE, HOLD, max_held, BannedIps::default())
}

/// A held seat counts against every other address, and only its own address takes it back,
/// once.
#[test]
fn only_the_peer_that_left_takes_its_seat_back() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut seats = held_seats(4);

    assert!(seats.hold(closed(ip(1), MIN_AGE, now), now));
    assert_eq!(seats.len(), 1);

    assert_eq!(seats.held_for_others(ip(2), now), 1);
    assert_eq!(seats.held_for_others(ip(1), now), 0);

    assert!(!seats.take_back(ip(2), now));
    assert_eq!(seats.held_for_others(ip(2), now), 1);

    assert!(seats.take_back(ip(1), now));
    assert_eq!(seats.len(), 0);
    assert_eq!(seats.held_for_others(ip(2), now), 0);
    assert!(!seats.take_back(ip(1), now));
}

/// A connection shorter than the minimum age holds no seat.
#[test]
fn a_short_connection_holds_no_seat() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut seats = held_seats(4);

    assert!(!seats.hold(closed(ip(1), MIN_AGE - TICK, now), now));
    assert_eq!(seats.held_for_others(ip(2), now), 0);

    assert!(seats.hold(closed(ip(1), MIN_AGE, now), now));
    assert_eq!(seats.held_for_others(ip(2), now), 1);
}

/// A held seat is released when its hold ends, to other peers and to its own.
#[test]
fn a_held_seat_is_released_when_its_hold_ends() {
    let _init_guard = zakura_test::init();

    let closed_at = Instant::now();
    let mut seats = held_seats(4);

    assert!(seats.hold(closed(ip(1), MIN_AGE, closed_at), closed_at));

    let last_held = closed_at + HOLD - TICK;
    assert_eq!(seats.held_for_others(ip(2), last_held), 1);

    // The peer's own check comes first, so it cannot rely on an earlier call to release.
    let released = closed_at + HOLD;
    assert!(!seats.take_back(ip(1), released));
    assert_eq!(seats.len(), 0);
    assert_eq!(seats.held_for_others(ip(2), released), 0);
}

/// A hold is measured from when the connection closed, not from when the listener
/// learned about it. A quiet listener can learn about a close long after it happened.
#[test]
fn a_hold_starts_when_the_connection_closed() {
    let _init_guard = zakura_test::init();

    let closed_at = Instant::now();
    let mut seats = held_seats(4);

    let learned_late = closed_at + HOLD - TICK;
    assert!(seats.hold(closed(ip(1), MIN_AGE, closed_at), learned_late));
    assert_eq!(seats.held_for_others(ip(2), learned_late), 1);
    assert_eq!(seats.held_for_others(ip(2), closed_at + HOLD), 0);

    let learned_too_late = closed_at + HOLD;
    assert!(!seats.hold(closed(ip(3), MIN_AGE, closed_at), learned_too_late));
    assert_eq!(seats.len(), 0);
}

/// Held seats stop at the cap. A seat that is taken back or released makes room again.
#[test]
fn held_seats_stop_at_the_cap() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut seats = held_seats(2);

    assert!(seats.hold(closed(ip(1), MIN_AGE, now), now));
    assert!(seats.hold(closed(ip(2), MIN_AGE, now + TICK), now + TICK));
    assert!(!seats.hold(closed(ip(3), MIN_AGE, now + TICK), now + TICK));
    assert_eq!(seats.len(), 2);

    assert!(seats.take_back(ip(1), now + TICK));
    let later = now + TICK + TICK;
    assert!(seats.hold(closed(ip(3), MIN_AGE, later), later));
    assert!(!seats.hold(closed(ip(4), MIN_AGE, later), later));

    // The hold for `ip(2)` ends one tick before the hold for `ip(3)`.
    let first_released = now + TICK + HOLD;
    assert!(seats.hold(closed(ip(4), MIN_AGE, first_released), first_released));
    assert_eq!(seats.len(), 2);
    assert!(!seats.take_back(ip(2), first_released));
    assert!(seats.take_back(ip(4), first_released));
}

/// With no room for held seats, nothing is held.
#[test]
fn a_cap_of_zero_holds_no_seats() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut seats = held_seats(0);

    assert!(!seats.hold(closed(ip(1), MIN_AGE, now), now));
    assert_eq!(seats.held_for_others(ip(2), now), 0);
}

/// An address holds one seat, however many of its connections close.
#[test]
fn an_address_holds_one_seat() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut seats = held_seats(4);

    assert!(seats.hold(closed(ip(1), MIN_AGE, now), now));
    assert!(!seats.hold(closed(ip(1), MIN_AGE, now + TICK), now + TICK));
    assert_eq!(seats.len(), 1);

    // The second close does not extend the first hold.
    assert_eq!(seats.held_for_others(ip(2), now + HOLD), 0);
}

/// A banned peer is refused before it can take a seat back, so no seat is held for it.
#[test]
fn a_banned_peer_holds_no_seat() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut seats = HeldSeats::with_policy(MIN_AGE, HOLD, 4, BannedIps::with_banned_ip(ip(1)));

    assert!(!seats.hold(closed(ip(1), MIN_AGE, now), now));
    assert_eq!(seats.held_for_others(ip(2), now), 0);

    assert!(seats.hold(closed(ip(3), MIN_AGE, now), now));
    assert_eq!(seats.held_for_others(ip(2), now), 1);
}

/// A ban that arrives after a seat is held releases the seat.
#[test]
fn a_ban_releases_a_held_seat() {
    let _init_guard = zakura_test::init();

    let mut address_book = AddressBook::new(
        "0.0.0.0:0".parse().expect("hard-coded address is valid"),
        &Network::Mainnet,
        1,
        tracing::Span::none(),
    );
    let now = Instant::now();
    let mut seats = HeldSeats::with_policy(MIN_AGE, HOLD, 4, address_book.bans());

    assert!(seats.hold(closed(ip(1), MIN_AGE, now), now));
    assert_eq!(seats.held_for_others(ip(2), now), 1);

    address_book.update(MetaAddr::new_misbehavior(
        SocketAddr::new(ip(1), 8233).into(),
        constants::MAX_PEER_MISBEHAVIOR_SCORE,
    ));
    assert!(address_book.bans().contains(ip(1)), "the peer is banned");

    assert_eq!(seats.held_for_others(ip(2), now), 0);
    assert_eq!(seats.len(), 0);
}

/// The production policy holds a tenth of the inbound limit, for long-lived connections.
#[test]
fn the_default_policy_uses_the_configured_constants() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let inbound_limit = 300;
    let max_held = inbound_limit / constants::INBOUND_SEAT_HOLD_LIMIT_DIVISOR;
    assert_eq!(max_held, 30, "a tenth of the default inbound limit");
    let mut seats = HeldSeats::new(inbound_limit, BannedIps::default());

    let too_short = constants::MIN_HELD_SEAT_CONNECTION_AGE - TICK;
    assert!(!seats.hold(closed(ip(0), too_short, now), now));

    for index in 0..max_held {
        let ip = IpAddr::from([198, 51, 100, u8::try_from(index).expect("cap fits in a u8")]);
        assert!(seats.hold(
            closed(ip, constants::MIN_HELD_SEAT_CONNECTION_AGE, now),
            now
        ));
    }
    assert_eq!(seats.len(), max_held);
    assert!(!seats.hold(
        closed(ip(0), constants::MIN_HELD_SEAT_CONNECTION_AGE, now),
        now
    ));

    let last_held = now + constants::INBOUND_SEAT_HOLD_DURATION - TICK;
    assert_eq!(seats.held_for_others(ip(0), last_held), max_held);
    assert_eq!(
        seats.held_for_others(ip(0), now + constants::INBOUND_SEAT_HOLD_DURATION),
        0
    );

    // A limit below the divisor leaves no room for held seats.
    let mut seats = HeldSeats::new(
        constants::INBOUND_SEAT_HOLD_LIMIT_DIVISOR - 1,
        BannedIps::default(),
    );
    assert!(!seats.hold(
        closed(ip(0), constants::MIN_HELD_SEAT_CONNECTION_AGE, now),
        now
    ));
}
