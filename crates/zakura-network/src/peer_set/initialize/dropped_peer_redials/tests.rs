//! Fixed test vectors for redials of dropped outbound peers.

use std::time::Duration;

use tokio::time::Instant;

use crate::{constants, peer_set::ClosedConnection, PeerSocketAddr};

use super::DroppedPeerRedials;

/// The shortest connection whose peer is redialed in these tests.
const MIN_AGE: Duration = Duration::from_secs(600);

/// The smallest step of the clock.
const TICK: Duration = Duration::from_nanos(1);

/// Returns the redial delays used by these tests.
fn delays() -> Vec<Duration> {
    vec![
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
    ]
}

/// Returns the test peer address with `last_octet`.
fn addr(last_octet: u8) -> PeerSocketAddr {
    format!("192.0.2.{last_octet}:8233")
        .parse()
        .expect("test peer address is valid")
}

/// Returns a connection to `addr` that closed at `closed_at` after being open for `open_for`.
fn closed(addr: PeerSocketAddr, open_for: Duration, closed_at: Instant) -> ClosedConnection {
    ClosedConnection {
        addr,
        open_for,
        closed_at,
    }
}

/// Returns redials with the test policy and room for `max_pending` addresses.
fn redials(max_pending: usize) -> DroppedPeerRedials {
    DroppedPeerRedials::with_policy(MIN_AGE, delays(), max_pending)
}

/// Each redial is due at its delay after the close, and the peer is redialed until every
/// delay is used.
#[test]
fn redials_follow_the_delays_until_none_is_left() {
    let _init_guard = zakura_test::init();

    let closed_at = Instant::now();
    let mut redials = redials(4);
    assert!(redials.schedule(closed(addr(1), MIN_AGE, closed_at), closed_at));

    for delay in delays() {
        let due = closed_at + delay;
        assert_eq!(redials.next_due(), Some(due));
        assert_eq!(redials.take_due(due - TICK), vec![]);
        assert_eq!(redials.take_due(due), vec![addr(1)]);

        // A redial in progress is not due again.
        assert_eq!(redials.next_due(), None);
        assert_eq!(redials.take_due(due), vec![]);

        redials.failed(addr(1));
    }

    assert_eq!(redials.len(), 0);
    assert_eq!(redials.next_due(), None);
}

/// A connection shorter than the minimum age is not redialed.
#[test]
fn a_short_connection_is_not_redialed() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut redials = redials(4);

    assert!(!redials.schedule(closed(addr(1), MIN_AGE - TICK, now), now));
    assert_eq!(redials.next_due(), None);
}

/// A stopped peer gets no more redials, and its address can be scheduled again later.
#[test]
fn a_stopped_peer_is_not_redialed() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut redials = redials(4);

    assert!(redials.schedule(closed(addr(1), MIN_AGE, now), now));
    assert_eq!(redials.take_due(now + delays()[0]), vec![addr(1)]);
    redials.stop(addr(1));

    assert_eq!(redials.len(), 0);
    assert_eq!(redials.take_due(now + delays()[2]), vec![]);

    let later = now + MIN_AGE;
    assert!(redials.schedule(closed(addr(1), MIN_AGE, later), later));
}

/// A close that is learned late gets one redial for the latest time that has passed, and
/// none if every time has passed.
#[test]
fn a_late_close_skips_the_redials_it_missed() {
    let _init_guard = zakura_test::init();

    let closed_at = Instant::now();
    let mut redials = redials(4);

    let learned_at = closed_at + delays()[1];
    assert!(redials.schedule(closed(addr(1), MIN_AGE, closed_at), learned_at));
    assert_eq!(redials.take_due(learned_at), vec![addr(1)]);
    redials.failed(addr(1));
    assert_eq!(redials.next_due(), Some(closed_at + delays()[2]));

    let too_late = closed_at + delays()[2] + TICK;
    assert!(!redials.schedule(closed(addr(2), MIN_AGE, closed_at), too_late));

    // At the last redial time, there is still one redial to make.
    redials.stop(addr(1));
    let just_in_time = closed_at + delays()[2];
    assert!(redials.schedule(closed(addr(3), MIN_AGE, closed_at), just_in_time));
    assert_eq!(redials.take_due(just_in_time), vec![addr(3)]);
}

/// An address has one set of redials, and pending addresses stop at the cap.
#[test]
fn pending_redials_stop_at_one_per_address_and_the_cap() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut redials = redials(2);

    assert!(redials.schedule(closed(addr(1), MIN_AGE, now), now));
    assert!(!redials.schedule(closed(addr(1), MIN_AGE, now + TICK), now + TICK));
    assert!(redials.schedule(closed(addr(2), MIN_AGE, now), now));
    assert!(!redials.schedule(closed(addr(3), MIN_AGE, now), now));
    assert_eq!(redials.len(), 2);

    // The second close of an address does not move its redials.
    assert_eq!(redials.next_due(), Some(now + delays()[0]));

    redials.stop(addr(1));
    assert!(redials.schedule(closed(addr(3), MIN_AGE, now), now));
}

/// A peer that is redialed and then fails keeps its later redials, and a failure for an
/// address with no redials does nothing.
#[test]
fn a_failure_keeps_the_later_redials() {
    let _init_guard = zakura_test::init();

    let now = Instant::now();
    let mut redials = redials(4);

    redials.failed(addr(9));
    assert_eq!(redials.len(), 0);

    assert!(redials.schedule(closed(addr(1), MIN_AGE, now), now));
    assert!(redials.schedule(closed(addr(2), MIN_AGE, now + TICK), now + TICK));

    let first_due = now + delays()[0];
    assert_eq!(redials.take_due(first_due), vec![addr(1)]);
    redials.failed(addr(1));

    assert_eq!(redials.take_due(first_due + TICK), vec![addr(2)]);
    assert_eq!(redials.next_due(), Some(now + delays()[1]));
}

/// The production policy redials long-lived connections at the configured delays.
#[test]
fn the_default_policy_uses_the_configured_constants() {
    let _init_guard = zakura_test::init();

    let closed_at = Instant::now();
    let mut redials = DroppedPeerRedials::new(1);

    let too_short = constants::MIN_REDIALED_CONNECTION_AGE - TICK;
    assert!(!redials.schedule(closed(addr(1), too_short, closed_at), closed_at));

    let long_lived = constants::MIN_REDIALED_CONNECTION_AGE;
    assert!(redials.schedule(closed(addr(1), long_lived, closed_at), closed_at));
    assert!(!redials.schedule(closed(addr(2), long_lived, closed_at), closed_at));

    for delay in constants::DROPPED_PEER_REDIAL_DELAYS {
        assert_eq!(redials.take_due(closed_at + delay), vec![addr(1)]);
        redials.failed(addr(1));
    }
    assert_eq!(redials.len(), 0);

    // No room means no redials.
    let mut redials = DroppedPeerRedials::new(0);
    assert!(!redials.schedule(closed(addr(1), long_lived, closed_at), closed_at));
}
