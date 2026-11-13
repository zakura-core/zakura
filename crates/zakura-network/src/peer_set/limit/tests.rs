//! Fixed test vectors for connection counters and the reports their trackers send.

use std::{net::IpAddr, time::Duration};

use tokio::time::Instant;

use super::{ActiveConnectionCounter, ClosedConnection};

/// How long the test connections stay open.
const OPEN_FOR: Duration = Duration::from_secs(600);

/// Returns the peer IP address used by these tests.
fn peer_ip() -> IpAddr {
    "192.0.2.1".parse().expect("hard-coded IP address is valid")
}

/// A tracker with an address reports that address, how long the connection was open, and
/// when it closed. The report arrives with the close, and it is taken once.
#[tokio::test(start_paused = true)]
async fn a_tracker_with_an_address_reports_the_connection_it_closes() {
    let _init_guard = zakura_test::init();

    let mut counter = ActiveConnectionCounter::new_counter();
    let mut tracker = counter.track_connection_from(peer_ip());

    // A handshake that takes a while is not part of the time the connection was open.
    tokio::time::advance(Duration::from_secs(2)).await;
    tracker.mark_open();

    assert_eq!(counter.update_count(), 1);
    assert_eq!(counter.take_closed_connections(), vec![]);

    tokio::time::advance(OPEN_FOR).await;
    let closed_at = Instant::now();
    tracker.mark_closed_by_peer();
    drop(tracker);

    assert_eq!(counter.take_closed_connections(), vec![]);
    assert_eq!(counter.update_count(), 0);
    assert_eq!(
        counter.take_closed_connections(),
        vec![ClosedConnection {
            ip: peer_ip(),
            open_for: OPEN_FOR,
            closed_at,
        }],
    );
    assert_eq!(counter.take_closed_connections(), vec![]);
}

/// A connection that this node closed reports nothing, however long it was open.
#[tokio::test(start_paused = true)]
async fn a_connection_this_node_closed_reports_nothing() {
    let _init_guard = zakura_test::init();

    let mut counter = ActiveConnectionCounter::new_counter();
    let mut tracker = counter.track_connection_from(peer_ip());
    tracker.mark_open();

    tokio::time::advance(OPEN_FOR).await;
    drop(tracker);

    assert_eq!(counter.update_count(), 0);
    assert_eq!(counter.take_closed_connections(), vec![]);
}

/// A connection that fails before it opens was never used, so it reports nothing.
#[tokio::test(start_paused = true)]
async fn a_connection_that_never_opened_reports_nothing() {
    let _init_guard = zakura_test::init();

    let mut counter = ActiveConnectionCounter::new_counter();
    let mut tracker = counter.track_connection_from(peer_ip());

    assert_eq!(counter.update_count(), 1);

    tokio::time::advance(OPEN_FOR).await;
    tracker.mark_closed_by_peer();
    drop(tracker);

    assert_eq!(counter.update_count(), 0);
    assert_eq!(counter.take_closed_connections(), vec![]);
}

/// A tracker without an address reports nothing, so counters that never take reports do
/// not collect them.
#[tokio::test(start_paused = true)]
async fn a_tracker_without_an_address_reports_nothing() {
    let _init_guard = zakura_test::init();

    let mut counter = ActiveConnectionCounter::new_counter();
    let mut tracker = counter.track_connection();
    tracker.mark_open();

    tokio::time::advance(OPEN_FOR).await;
    tracker.mark_closed_by_peer();
    drop(tracker);

    assert_eq!(counter.update_count(), 0);
    assert_eq!(counter.take_closed_connections(), vec![]);
}
