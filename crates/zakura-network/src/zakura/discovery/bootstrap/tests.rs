//! Tests for the bootstrap dialer's plan.

use std::net::SocketAddr;

use zakura_quic::NodeSecretKey;

use super::*;

/// `count` seeds, each on an address of its own.
fn seeds(count: u8) -> Vec<NodeAddr> {
    (0..count)
        .map(|index| {
            let addr = SocketAddr::from(([198, 51, 100, index], 8233));
            NodeAddr::with_addrs(NodeSecretKey::from_bytes(&[index + 1; 32]).public(), [addr])
        })
        .collect()
}

/// An outbound peer on `ip`, registered at `registered_at`.
fn outbound(byte: u8, ip: Option<[u8; 4]>, registered_at: Instant) -> OutboundPeer {
    OutboundPeer {
        peer_id: ZakuraPeerId::from_node_id(NodeSecretKey::from_bytes(&[byte; 32]).public()),
        host: ip.map(IpAddr::from),
        registered_at,
    }
}

fn coverage(hosts: usize, settled_hosts: usize) -> PeerCoverage {
    PeerCoverage {
        hosts,
        settled_hosts,
        next_settled: None,
    }
}

/// Plans `plan`'s dials at `now`, giving each a fresh token.
fn dial(
    plan: &mut BootstrapPlan,
    coverage: PeerCoverage,
    registered: impl Fn(usize) -> bool,
    now: Instant,
) -> Vec<(usize, CancellationToken)> {
    plan.dials(coverage, registered, now, CancellationToken::new)
        .into_iter()
        .map(|(index, _, token)| (index, token))
        .collect()
}

fn indexes(started: &[(usize, CancellationToken)]) -> Vec<usize> {
    started.iter().map(|(index, _)| *index).collect()
}

/// Coverage counts the hosts of outbound non-seed peers: one per IP, one
/// per peer without a known IP, and settled only after the
/// healthy-connection time. The floor excludes seeds and the settled hosts
/// release them.
#[test]
fn coverage_counts_distinct_hosts() {
    let now = Instant::now();
    let old = now - ZAKURA_REDIAL_HEALTHY_CONNECTION;
    let young = now - (ZAKURA_REDIAL_HEALTHY_CONNECTION - Duration::from_secs(1));
    let peers = [
        // Two peers on one IP are one host.
        outbound(1, Some([203, 0, 113, 1]), old),
        outbound(2, Some([203, 0, 113, 1]), old),
        outbound(3, Some([203, 0, 113, 2]), young),
        // A peer with no known IP is a host of its own.
        outbound(4, None, old),
    ];

    assert_eq!(
        PeerCoverage::of(&peers, now),
        PeerCoverage {
            next_settled: Some(young + ZAKURA_REDIAL_HEALTHY_CONNECTION),
            ..coverage(3, 2)
        }
    );
    assert_eq!(PeerCoverage::of(&[], now), coverage(0, 0));

    assert!(!coverage(BOOTSTRAP_DIAL_FLOOR - 1, 0).excludes_seeds());
    assert!(coverage(BOOTSTRAP_DIAL_FLOOR, 0).excludes_seeds());
    assert!(!coverage(9, 3).releases_seeds(4));
    assert!(coverage(9, 4).releases_seeds(4));
}

/// The release waits for four hosts at the default cap, and for fewer when
/// inbound peers may take all but a few slots, but never fewer than the
/// floor.
#[test]
fn small_dial_reserves_release_with_fewer_hosts() {
    // 92 connections reserve 23 for dials, 19 reserve 4, and 8 reserve 2.
    assert_eq!(release_hosts(23), BOOTSTRAP_RELEASE_HOSTS);
    assert_eq!(release_hosts(4), 2);
    assert_eq!(release_hosts(2), BOOTSTRAP_DIAL_FLOOR);
}

/// A node below the floor dials at most two seeds at once, in order, and
/// dials none once it has enough peers of its own.
#[test]
fn below_the_floor_the_node_dials_two_seeds() {
    let now = Instant::now();
    let mut plan = BootstrapPlan::new(seeds(3), now);

    assert_eq!(
        indexes(&dial(&mut plan, coverage(0, 0), |_| false, now)),
        [0, 1]
    );
    assert!(
        dial(&mut plan, coverage(0, 0), |_| false, now).is_empty(),
        "two seed dials are already running"
    );

    let mut plan = BootstrapPlan::new(seeds(3), now);
    assert!(
        dial(&mut plan, coverage(BOOTSTRAP_DIAL_FLOOR, 0), |_| false, now).is_empty(),
        "a node at the floor dials no seed"
    );

    // A seed that is already registered, say because the seed itself
    // connected, takes one of the two places.
    let mut plan = BootstrapPlan::new(seeds(3), now);
    assert_eq!(
        indexes(&dial(&mut plan, coverage(0, 0), |index| index == 0, now)),
        [1]
    );
}

/// A release cancels the tokens of this dialer's running dials, except the
/// ones kept, and the closed seeds are dialable again without a penalty.
#[test]
fn a_release_closes_only_its_own_dials() {
    let now = Instant::now();
    let mut plan = BootstrapPlan::new(seeds(3), now);
    let started = dial(&mut plan, coverage(0, 0), |_| false, now);
    assert!(plan.has_running_dials());

    assert_eq!(plan.release(|index| index == 1), 1);
    assert!(started[0].1.is_cancelled());
    assert!(!started[1].1.is_cancelled(), "a kept seed stays");
    assert_eq!(plan.release(|_| false), 1, "seed 0 is already released");

    // The closed connection was young, so it scores as a failure.
    plan.finished(0, DialResult::Failed, now);
    assert_eq!(plan.seeds[0].retry_at, now);
    assert_eq!(plan.seeds[0].backoff, DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF);
    plan.finished(1, DialResult::Healthy, now);
    assert!(!plan.has_running_dials());
    assert_eq!(plan.release(|_| false), 0, "nothing runs to close");
}

/// A failed seed backs off, doubling up to the maximum, while the next
/// seeds take its turn. A healthy connection resets the backoff, and the
/// plan reports when the next backoff ends.
#[test]
fn failed_seeds_back_off_and_rotate() {
    let start = Instant::now();
    let mut plan = BootstrapPlan::new(seeds(3), start);
    assert_eq!(
        indexes(&dial(&mut plan, coverage(0, 0), |_| false, start)),
        [0, 1]
    );
    plan.finished(0, DialResult::Failed, start);
    plan.finished(1, DialResult::Failed, start);
    assert_eq!(
        plan.next_retry(start),
        Some(start + DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF)
    );
    assert_eq!(
        indexes(&dial(&mut plan, coverage(0, 0), |_| false, start)),
        [2],
        "the seeds that just failed wait out their backoff"
    );

    let later = start + DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF;
    plan.finished(2, DialResult::Failed, later);
    assert_eq!(
        indexes(&dial(&mut plan, coverage(0, 0), |_| false, later)),
        [0, 1]
    );

    for _ in 0..10 {
        plan.finished(0, DialResult::Failed, start);
    }
    assert_eq!(plan.seeds[0].backoff, DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF);
    assert_eq!(
        plan.seeds[0].retry_at,
        start + DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF
    );

    plan.finished(0, DialResult::Healthy, later);
    assert_eq!(plan.seeds[0].retry_at, later);
    assert_eq!(plan.seeds[0].backoff, DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF);
}
