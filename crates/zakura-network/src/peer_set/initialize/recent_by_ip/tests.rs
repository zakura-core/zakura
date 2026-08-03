//! Fixed test vectors for recent IP limits.

#![allow(clippy::unwrap_in_result)]

use std::time::Duration;

use crate::peer_set::initialize::recent_by_ip::RecentByIp;

#[test]
fn old_connection_attempts_are_pruned() {
    const TEST_TIME_LIMIT: Duration = Duration::from_secs(5);

    let _init_guard = zakura_test::init();

    let mut recent_connections = RecentByIp::new(Some(TEST_TIME_LIMIT), None);
    let ip = "127.0.0.1".parse().expect("should parse");
    let start = std::time::Instant::now();

    assert!(
        !recent_connections.is_past_limit_or_add_at(ip, start),
        "should not be past limit"
    );
    assert!(
        recent_connections.is_past_limit_or_add_at(ip, start),
        "should be past max_connections_per_ip limit"
    );

    assert!(
        recent_connections.is_past_limit_or_add_at(ip, start + TEST_TIME_LIMIT / 3),
        "should still contain entry after a third of the time limit"
    );

    let expired = start + 13 * TEST_TIME_LIMIT / 12;
    assert!(
        !recent_connections.is_past_limit_or_add_at(ip, expired),
        "should prune entry after 13/12 * time_limit"
    );

    const TEST_MAX_CONNS_PER_IP: usize = 3;

    let mut recent_connections =
        RecentByIp::new(Some(TEST_TIME_LIMIT), Some(TEST_MAX_CONNS_PER_IP));
    let start = std::time::Instant::now();

    for _ in 0..TEST_MAX_CONNS_PER_IP {
        assert!(
            !recent_connections.is_past_limit_or_add_at(ip, start),
            "should not be past limit"
        );
    }

    assert!(
        recent_connections.is_past_limit_or_add_at(ip, start),
        "should be past max_connections_per_ip limit"
    );

    assert!(
        recent_connections.is_past_limit_or_add_at(ip, start + TEST_TIME_LIMIT / 3),
        "should still be past limit after a third of the reconnection delay"
    );

    let expired = start + 13 * TEST_TIME_LIMIT / 12;
    assert!(
        !recent_connections.is_past_limit_or_add_at(ip, expired),
        "should prune entry after 13/12 * time_limit"
    );
}
