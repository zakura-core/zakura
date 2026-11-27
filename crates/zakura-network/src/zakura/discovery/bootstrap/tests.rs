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

fn peer_id(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::from_node_id(NodeSecretKey::from_bytes(&[byte; 32]).public())
}

/// An outbound peer discovery opened on `ip`, registered at `registered_at`.
fn outbound(byte: u8, ip: Option<[u8; 4]>, registered_at: Instant) -> OutboundPeer {
    OutboundPeer {
        peer_id: peer_id(byte),
        earlier: Vec::new(),
        host: ip.map(IpAddr::from),
        registered_at,
        opener: ConnectionOpener::Discovery,
    }
}

fn coverage(hosts: usize, settled_hosts: usize) -> PeerCoverage {
    PeerCoverage {
        hosts,
        settled_hosts,
    }
}

fn dialed(started: &[(usize, NodeAddr)]) -> Vec<usize> {
    started.iter().map(|(index, _)| *index).collect()
}

/// Coverage counts the hosts of outbound non-seed peers: one per IP, one per
/// peer without a known IP, and settled only after the healthy-connection
/// time. The floor excludes seeds and the settled hosts release them.
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

    assert_eq!(PeerCoverage::of(&peers, now), coverage(3, 2));
    assert_eq!(PeerCoverage::of(&[], now), coverage(0, 0));

    assert!(!coverage(BOOTSTRAP_DIAL_FLOOR - 1, 0).excludes_seeds());
    assert!(coverage(BOOTSTRAP_DIAL_FLOOR, 0).excludes_seeds());
    assert!(!coverage(9, BOOTSTRAP_RELEASE_HOSTS - 1).releases_seeds());
    assert!(coverage(9, BOOTSTRAP_RELEASE_HOSTS).releases_seeds());
}

/// A peer is a seed when it registered under a seed's configured ID or the
/// ID a dial to it answered with, or when its handshake proved the
/// configured ID. The plan then knows the seed by the ID it registered
/// under, even on a connection that proves nothing earlier.
#[test]
fn seeds_are_matched_by_the_ids_their_handshakes_prove() {
    let now = Instant::now();
    let mut plan = BootstrapPlan::new(seeds(3), now);
    let configured: Vec<NodeId> = plan.configured_ids().collect();
    let dialed_and_moved = peer_id(20).node_id().expect("test peers have node IDs");
    let current = |id: NodeId| {
        if id == configured[1] {
            dialed_and_moved
        } else {
            id
        }
    };
    let mut moved = outbound(21, None, now);
    moved.earlier = vec![configured[2]];
    let peers = [
        OutboundPeer {
            peer_id: ZakuraPeerId::from_node_id(configured[0]),
            ..outbound(0, None, now)
        },
        outbound(20, None, now),
        moved,
        outbound(30, None, now),
    ];

    assert_eq!(
        plan.match_peers(&peers, current),
        [Some(0), Some(1), Some(2), None]
    );
    let moved_id = peer_id(21).node_id().expect("test peers have node IDs");
    assert_eq!(
        plan.ids_of(2, current).collect::<Vec<_>>(),
        [configured[2], moved_id]
    );
    assert_eq!(
        plan.match_peers(&[outbound(21, None, now)], current),
        [Some(2)],
        "the seed is known by its new ID"
    );
}

/// The candidate dialer may hold seeds in half its ordinary slots while the
/// node is below the floor, and in none once seeds are excluded.
#[test]
fn seeds_take_at_most_half_the_ordinary_slots() {
    let policy = |excluded| SeedDialPolicy {
        ids: HashSet::new(),
        excluded,
    };
    assert_eq!(policy(false).seed_connection_cap(10), 5);
    assert_eq!(policy(false).seed_connection_cap(1), 0);
    assert_eq!(policy(true).seed_connection_cap(10), 0);
}

/// A node below the floor dials at most two seeds at once, in order, and
/// dials none once it has enough peers of its own.
#[test]
fn below_the_floor_the_node_dials_two_seeds() {
    let now = Instant::now();
    let mut plan = BootstrapPlan::new(seeds(3), now);

    assert_eq!(dialed(&plan.dials(coverage(0, 0), |_| false, now)), [0, 1]);
    assert!(
        plan.dials(coverage(0, 0), |_| false, now).is_empty(),
        "two seed dials are already running"
    );

    let mut plan = BootstrapPlan::new(seeds(3), now);
    assert!(
        plan.dials(coverage(BOOTSTRAP_DIAL_FLOOR, 0), |_| false, now)
            .is_empty(),
        "a node at the floor dials no seed"
    );

    // A seed that is already registered, say because another dialer or the
    // seed itself connected, takes one of the two places.
    let mut plan = BootstrapPlan::new(seeds(3), now);
    assert_eq!(
        dialed(&plan.dials(coverage(0, 0), |index| index == 0, now)),
        [1]
    );
}

/// A seed whose connection this node closed stays dialable, so the release
/// doesn't count against it. Releasing a seed this dialer isn't dialing
/// changes nothing.
#[test]
fn a_released_seed_is_not_penalised() {
    let now = Instant::now();
    let mut plan = BootstrapPlan::new(seeds(3), now);
    plan.dials(coverage(0, 0), |_| false, now);

    plan.released(0);
    plan.released(2);
    assert!(!plan.seeds[2].released);

    // The closed connection was young, so it scores as a failure.
    plan.finished(0, DialResult::Failed, now);
    assert_eq!(plan.seeds[0].retry_at, now);
    assert_eq!(plan.seeds[0].backoff, DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF);
}

/// A failed seed backs off, doubling up to the maximum, while the next
/// seeds take its turn. A healthy connection resets the backoff.
#[test]
fn failed_seeds_back_off_and_rotate() {
    let start = Instant::now();
    let mut plan = BootstrapPlan::new(seeds(3), start);
    assert_eq!(
        dialed(&plan.dials(coverage(0, 0), |_| false, start)),
        [0, 1]
    );
    plan.finished(0, DialResult::Failed, start);
    plan.finished(1, DialResult::Failed, start);
    assert_eq!(
        dialed(&plan.dials(coverage(0, 0), |_| false, start)),
        [2],
        "the seeds that just failed wait out their backoff"
    );

    let later = start + DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF;
    plan.finished(2, DialResult::Failed, later);
    assert_eq!(
        dialed(&plan.dials(coverage(0, 0), |_| false, later)),
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
