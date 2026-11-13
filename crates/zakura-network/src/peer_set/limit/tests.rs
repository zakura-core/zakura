//! Fixed test vectors for connection counters and the reports their trackers send.

use std::time::Duration;

use tokio::time::Instant;

use crate::PeerSocketAddr;

use super::{ActiveConnectionCounter, ClosedConnection};

/// How long the test connections stay open.
const OPEN_FOR: Duration = Duration::from_secs(600);

/// Returns the peer address used by these tests.
fn peer_addr() -> PeerSocketAddr {
    "192.0.2.1:8233"
        .parse()
        .expect("hard-coded peer address is valid")
}

/// A tracker with an address reports that address, how long the connection was open, and
/// when it closed. The report arrives with the close, and it is taken once.
#[tokio::test(start_paused = true)]
async fn a_tracker_with_an_address_reports_the_connection_it_closes() {
    let _init_guard = zakura_test::init();

    let mut counter = ActiveConnectionCounter::new_counter();
    let mut tracker = counter.track_connection_from(peer_addr());

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
            addr: peer_addr(),
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
    let mut tracker = counter.track_connection_from(peer_addr());
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
    let mut tracker = counter.track_connection_from(peer_addr());

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

/// A tracker that is told its peer after it was created reports that peer, like one created
/// with the address.
#[tokio::test(start_paused = true)]
async fn a_tracker_given_its_address_later_reports_it() {
    let _init_guard = zakura_test::init();

    let mut counter = ActiveConnectionCounter::new_counter();
    let mut tracker = counter.track_connection();
    tracker.set_peer_addr(peer_addr());
    tracker.mark_open();

    tokio::time::advance(OPEN_FOR).await;
    let closed_at = Instant::now();
    tracker.mark_closed_by_peer();
    drop(tracker);

    assert_eq!(counter.update_count(), 0);
    assert_eq!(
        counter.take_closed_connections(),
        vec![ClosedConnection {
            addr: peer_addr(),
            open_for: OPEN_FOR,
            closed_at,
        }],
    );
}

/// Waiting for a change returns once a connection opens or closes, and applies every
/// notification that has arrived.
#[tokio::test(start_paused = true)]
async fn waiting_for_a_change_applies_every_pending_notification() {
    let _init_guard = zakura_test::init();

    let other_addr: PeerSocketAddr = "192.0.2.2:8233".parse().expect("valid test address");
    let mut counter = ActiveConnectionCounter::new_counter();
    let mut first = counter.track_connection_from(peer_addr());
    let mut second = counter.track_connection_from(other_addr);

    // Nothing has opened or closed yet.
    assert!(futures::FutureExt::now_or_never(counter.changed()).is_none());

    first.mark_open();
    second.mark_open();
    tokio::time::advance(OPEN_FOR).await;
    first.mark_closed_by_peer();
    second.mark_closed_by_peer();
    drop(first);
    drop(second);

    // One wait applies both opens and both closes.
    counter.changed().await;
    assert_eq!(counter.take_closed_connections().len(), 2);
    assert_eq!(counter.update_count(), 0);
}
