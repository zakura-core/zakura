//! The `discovery_candidate` machine target: decoder-v2 histories through
//! [`CandidateDialerState`], shared by its proptest and its cargo-fuzz target.
//!
//! [`replay_candidate_dial_bytes`] drives the production state as the dialer
//! loop does (see [`machine_test`](crate::zakura::machine_test)), with a
//! simulated supervisor that refuses IPs at its per-IP cap. It checks the
//! state's invariants after every operation, and that settling every attempt
//! releases every reservation. A ranking step forms the preferred set from
//! standings it chooses and runs the preferred pass: the set holds only
//! gateways that may fill a held place, no dial targets a gateway that is in
//! backoff, connected or already being dialed, and a gateway whose ID is
//! below this node's is dialed only once the ranking steps have left it
//! dialable in the set for the grace.
//!
//! It compares outputs only with facts that
//! follow from the operations it chose: which attempts it recorded and has not
//! settled, which IPs each reserved, which addresses each candidate listed,
//! and when each dial failed, succeeded, hit a local limit or opened a round.
//! It never re-derives the backoff schedule or the round cadence. Named
//! scenarios in `tests.rs` pin them.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::{collections::HashMap, time::Instant};

use super::preferred::{
    admit_preferred_dials, dialer_standing, DeferredDials, PreferredRoom, MUTUAL_DIAL_GRACE,
};
use super::state::{CandidateDialerState, DialAttemptKey};
use super::{DiscoveryDialResult, ZAKURA_DISCOVERY_DIAL_INTERVAL};
use crate::zakura::{
    canonical_ip,
    discovery::{protocol::DialTarget, ZakuraDiscoveryDialCandidate},
    machine_test::MAX_HISTORY_BYTES,
    GatewayStanding, PreferredGateways, RankedGateway,
};

pub(super) fn node_id(byte: u8) -> NodeId {
    NodeSecretKey::from_bytes(&[byte; 32]).public()
}

pub(super) fn candidate(
    node_id: NodeId,
    direct_addrs: Vec<SocketAddr>,
) -> ZakuraDiscoveryDialCandidate {
    ZakuraDiscoveryDialCandidate {
        node_id,
        direct_addrs,
        is_static: false,
    }
}

// The replay's universe: a few nodes, canonical IPs, and address variants
// whose canonical IP the table states directly.
const NODES: usize = 3;

const IPS: [Ipv4Addr; 3] = [
    Ipv4Addr::new(93, 184, 216, 34),
    Ipv4Addr::new(198, 51, 100, 7),
    Ipv4Addr::new(203, 0, 113, 9),
];

/// `(canonical IP index, mapped into IPv6, port)` for each address variant.
const ADDRESSES: [(usize, bool, u16); 7] = [
    (0, false, 8233),
    (0, false, 8234),
    (0, true, 8235),
    (1, false, 8233),
    (1, true, 8233),
    (2, false, 8233),
    (2, true, 8236),
];

/// The simulated supervisor's per-IP cap.
const PER_IP_CAP: usize = 2;

const BACKOFF_BASE: Duration = Duration::from_secs(2);

const BACKOFF_MAX: Duration = Duration::from_secs(20);

fn nodes() -> Vec<NodeId> {
    (10..).take(NODES).map(node_id).collect()
}

/// The replayed dialer's own node ID, which sorts between the table's nodes,
/// so a ranking defers some of them and dials others at once.
fn local_node_id() -> NodeId {
    node_id(LOCAL_NODE_SEED)
}

fn table_ip(index: usize) -> IpAddr {
    IpAddr::V4(IPS[index])
}

fn table_address(index: usize) -> SocketAddr {
    let (ip, mapped, port) = ADDRESSES[index];
    let ip = if mapped {
        IpAddr::V6(IPS[ip].to_ipv6_mapped())
    } else {
        IpAddr::V4(IPS[ip])
    };
    SocketAddr::new(ip, port)
}

/// The table index of the canonical `ip`.
fn ip_index(ip: IpAddr) -> usize {
    IPS.iter()
        .position(|table| IpAddr::V4(*table) == ip)
        .expect("the state plans only canonical table IPs")
}

#[derive(Clone, Debug)]
pub(super) enum DialerOp {
    Advance(u64),
    Round,
    Dial {
        node: usize,
        addresses: Vec<usize>,
        refused: [bool; IPS.len()],
    },
    Complete {
        pick: usize,
        result: DiscoveryDialResult,
        admitted: Option<usize>,
    },
    Duplicate {
        pick: usize,
        result: DiscoveryDialResult,
    },
    /// Ranks every node, the first with the most blocks, and runs the
    /// preferred pass.
    Rank {
        /// What the discovery book knows about each node: unmatched, in
        /// backoff, or dialable at its one table address, which the dialer
        /// may still hold in its own backoff.
        book: [GatewayStanding; NODES],
        /// Which nodes are registered.
        registered: [bool; NODES],
        /// The places the set may hold.
        hold_limit: usize,
        /// The pass's free connections, address attempts and dials.
        room: PreferredRoom,
    },
}

/// The one address each node lists when the book could dial it: the first
/// table address of the node's own IP.
const RANKED_ADDRESS: [usize; NODES] = [0, 3, 5];

/// One attempt the replay recorded and has not settled.
#[derive(Debug)]
pub(super) struct Attempt {
    pub(super) key: DialAttemptKey,
    node: usize,
    ips: Vec<usize>,
}

/// What the replay sent the state.
#[derive(Debug)]
pub(super) struct Sent {
    pub(super) now: Instant,
    pub(super) in_flight: Vec<Attempt>,
    settled: Vec<DialAttemptKey>,
    /// When a dial of `(node, ip)` last failed, unless a later dial of the
    /// node registered through that IP.
    failed_at: HashMap<(usize, usize), Instant>,
    /// When the last round opened, or a local limit settled an attempt.
    paused_at: Option<Instant>,
    /// Each preferred set member that every ranking step since the time
    /// given left dialable and undialed.
    preferred_dialable_since: HashMap<NodeId, Instant>,
}

impl Sent {
    /// The in-flight attempts that reserved `ip`, counted from scratch.
    fn reserved(&self, ip: usize) -> usize {
        self.in_flight
            .iter()
            .map(|attempt| attempt.ips.iter().filter(|&&held| held == ip).count())
            .sum()
    }
}

/// Checks the state against what the replay sent.
fn check(state: &CandidateDialerState, sent: &Sent, nodes: &[NodeId]) {
    state.assert_invariants();
    let mut in_flight = state.in_flight_node_ids();
    let mut expected: Vec<_> = sent
        .in_flight
        .iter()
        .map(|attempt| nodes[attempt.node])
        .collect();
    in_flight.sort();
    expected.sort();
    assert_eq!(in_flight, expected, "{PROPERTY}: attempts in flight");

    for ip in 0..IPS.len() {
        let reserved = state.in_flight_dials_to(table_ip(ip));
        assert_eq!(
            reserved,
            sent.reserved(ip),
            "{PROPERTY}: reservations of IP {ip}"
        );
        assert!(reserved <= PER_IP_CAP, "{PROPERTY}");
    }
    // A failed dial backs its IPs off for at least the base backoff, and for
    // at most the maximum.
    for (&(node, ip), &failed_at) in &sent.failed_at {
        let backed_off = state.is_backed_off(nodes[node], table_ip(ip), sent.now);
        let elapsed = sent.now.saturating_duration_since(failed_at);
        if elapsed < BACKOFF_BASE {
            assert!(backed_off, "{PROPERTY}: node {node} lost IP {ip}'s backoff");
        }
        if elapsed >= BACKOFF_MAX {
            assert!(
                !backed_off,
                "{PROPERTY}: node {node} kept IP {ip}'s backoff"
            );
        }
    }
}

const PROPERTY: &str = "discovery/dial-attempts";

const PREFERRED_PROPERTY: &str = "discovery/preferred-dials";

/// The seed of [`local_node_id`].
const LOCAL_NODE_SEED: u8 = 15;

/// Applicable transitions in a decoder-v2 candidate dial history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct CandidateDialReplaySummary {
    /// Attempts accepted with at least one IP reservation.
    pub attempts: usize,
    /// Explicit completions of live attempts.
    pub completions: usize,
    /// Duplicate completions rejected by identity.
    pub duplicates: usize,
    /// Local resource limits that pause dialing.
    pub local_limits: usize,
    /// Failed attempts that establish backoff.
    pub failures: usize,
    /// Round decisions checked at logical time.
    pub rounds: usize,
    /// Rankings formed into a preferred set.
    pub rankings: usize,
    /// Dials the preferred pass started.
    pub preferred_dials: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes through
/// [`CandidateDialerState`]. Decoder v2 reads one operation per byte triple;
/// it adds the ranking step to decoder v1.
/// Logical time advances only through decoded operations.
pub fn replay_candidate_dial_bytes(data: &[u8]) -> CandidateDialReplaySummary {
    run(decode(data), |_, _| {})
}

pub(super) fn decode(data: &[u8]) -> impl Iterator<Item = DialerOp> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks(3)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            let result = match c % 6 {
                0 => DiscoveryDialResult::Registered,
                1 => DiscoveryDialResult::ConnectedElsewhere,
                2 => DiscoveryDialResult::ShortLivedRegistered,
                3 => DiscoveryDialResult::Failed,
                4 => DiscoveryDialResult::AlpnMismatch,
                _ => DiscoveryDialResult::LocalResourceLimit,
            };
            let book = |value: u8| match value % 3 {
                0 => GatewayStanding::Unmatched,
                1 => GatewayStanding::Backoff,
                _ => GatewayStanding::Dialable,
            };
            match a % 6 {
                0 => DialerOp::Advance(u64::from(b) * 100),
                1 => DialerOp::Round,
                2 => DialerOp::Dial {
                    node: usize::from(a / 6) % NODES,
                    addresses: (0..ADDRESSES.len())
                        .filter(|bit| b & (1 << bit) != 0)
                        .collect(),
                    refused: [c & 1 != 0, c & 2 != 0, c & 4 != 0],
                },
                3 => DialerOp::Complete {
                    pick: usize::from(b),
                    result,
                    admitted: (c & 8 != 0).then_some(usize::from(c / 16) % ADDRESSES.len()),
                },
                4 => DialerOp::Duplicate {
                    pick: usize::from(b),
                    result,
                },
                // a % 6 == 5: rank the three nodes and run the preferred pass.
                // Every count is folded into 0..=3, so any byte triple is legal.
                _ => DialerOp::Rank {
                    // b as three base-3 digits, one standing per node: node 0 is
                    // b % 3, node 1 the next digit, node 2 the one after.
                    // 0 is unmatched, 1 is backoff, 2 is dialable.
                    book: [book(b), book(b / 3), book(b / 9)],
                    // The low three bits of c, one bit per node.
                    registered: [c & 1 != 0, c & 2 != 0, c & 4 != 0],
                    // Bits 3 and 4 of c: how many places the set may hold.
                    hold_limit: usize::from(c >> 3) % 4,
                    room: PreferredRoom {
                        // The part of a that a % 6 did not use.
                        connections: usize::from(a / 6) % 4,
                        // The part of b the three digits did not use (3 * 3 * 3 = 27).
                        address_attempts: usize::from(b / 27) % 4,
                        // Bits 5 and 6 of c. Bit 7 is unused.
                        dials: usize::from(c >> 5) % 4,
                    },
                },
            }
        })
}

/// Drives `ops` through a fresh [`CandidateDialerState`], and calls
/// `after_step` after each operation, so a test can plant a fault.
pub(super) fn run(
    ops: impl IntoIterator<Item = DialerOp>,
    mut after_step: impl FnMut(&mut CandidateDialerState, &Sent),
) -> CandidateDialReplaySummary {
    let nodes = nodes();
    let mut state = CandidateDialerState::new((BACKOFF_BASE, BACKOFF_MAX));
    let mut sent = Sent {
        now: Instant::now(),
        in_flight: Vec::new(),
        settled: Vec::new(),
        failed_at: HashMap::default(),
        paused_at: None,
        preferred_dialable_since: HashMap::default(),
    };
    let mut deferred = DeferredDials::new(local_node_id());

    let mut summary = CandidateDialReplaySummary::default();
    for op in ops {
        let now = sent.now;
        match op {
            DialerOp::Advance(ms) => sent.now += Duration::from_millis(ms),
            DialerOp::Round => {
                summary.rounds += 1;
                if state.begin_round(now) {
                    // A round opens no sooner than one interval after the
                    // last round or local limit.
                    assert!(
                        sent.paused_at
                            .is_none_or(|paused| now >= paused + ZAKURA_DISCOVERY_DIAL_INTERVAL),
                        "{PROPERTY}: a round within one interval of the last pause"
                    );
                    sent.paused_at = Some(now);
                }
            }
            DialerOp::Dial {
                node,
                addresses,
                refused,
            } => {
                let dialed = candidate(
                    nodes[node],
                    addresses
                        .iter()
                        .map(|&address| table_address(address))
                        .collect(),
                );
                let planned = state.candidate_addresses(&dialed, now);
                // The plan dials the first listed address of each IP that is
                // not backed off, in the candidate's order.
                let mut expected = Vec::new();
                for &address in &addresses {
                    let ip = ADDRESSES[address].0;
                    if expected.iter().any(|&(_, seen)| seen == ip) {
                        continue;
                    }
                    expected.push((address, ip));
                }
                expected.retain(|&(_, ip)| !state.is_backed_off(nodes[node], table_ip(ip), now));
                let planned: Vec<_> = planned
                    .iter()
                    .map(|planned| (planned.addr, ip_index(planned.ip)))
                    .collect();
                assert_eq!(
                    planned,
                    expected
                        .iter()
                        .map(|&(address, ip)| (table_address(address), ip))
                        .collect::<Vec<_>>(),
                    "{PROPERTY}: planned addresses"
                );

                // The simulated supervisor refuses an IP the run marks
                // refused or one at its cap, counting this dialer's
                // in-flight attempts as the real check does.
                let accepted: Vec<_> = planned
                    .iter()
                    .map(|&(_, ip)| ip)
                    .filter(|&ip| !refused[ip] && sent.reserved(ip) < PER_IP_CAP)
                    .collect();
                let recorded = state.record_attempt(
                    nodes[node],
                    accepted.iter().map(|&ip| table_ip(ip)).collect(),
                );
                let busy = sent.in_flight.iter().any(|attempt| attempt.node == node);
                match recorded {
                    Some(key) => {
                        summary.attempts += 1;
                        assert!(!accepted.is_empty() && !busy, "{PROPERTY}");
                        assert_eq!(key.node_id, nodes[node], "{PROPERTY}");
                        assert!(
                            !sent.settled.contains(&key)
                                && sent.in_flight.iter().all(|attempt| attempt.key != key),
                            "{PROPERTY}: keys are never reused"
                        );
                        sent.in_flight.push(Attempt {
                            key,
                            node,
                            ips: accepted,
                        });
                    }
                    None => assert!(accepted.is_empty() || busy, "{PROPERTY}"),
                }
            }
            DialerOp::Complete {
                pick,
                result,
                admitted,
            } => {
                if sent.in_flight.is_empty() {
                    continue;
                }
                let index = pick % sent.in_flight.len();
                let key = sent.in_flight[index].key;
                assert!(
                    state.complete(
                        key,
                        result,
                        admitted.map(table_address).map(|addr| addr.ip()),
                        now
                    ),
                    "{PROPERTY}"
                );
                let attempt = sent.in_flight.remove(index);
                sent.settled.push(attempt.key);
                summary.completions += 1;
                match result {
                    DiscoveryDialResult::LocalResourceLimit => {
                        summary.local_limits += 1;
                        sent.paused_at = Some(now);
                    }
                    DiscoveryDialResult::Failed | DiscoveryDialResult::AlpnMismatch => {
                        summary.failures += 1;
                        for ip in attempt.ips {
                            sent.failed_at.insert((attempt.node, ip), now);
                        }
                    }
                    DiscoveryDialResult::Registered
                    | DiscoveryDialResult::ConnectedElsewhere
                    | DiscoveryDialResult::ShortLivedRegistered => {
                        if let Some(admitted) = admitted {
                            // The IP that TLS authenticated ends its backoff.
                            let ip = ADDRESSES[admitted].0;
                            sent.failed_at.remove(&(attempt.node, ip));
                            assert!(
                                !state.is_backed_off(nodes[attempt.node], table_ip(ip), now),
                                "{PROPERTY}: an authenticated IP stays backed off"
                            );
                        }
                    }
                }
            }
            DialerOp::Duplicate { pick, result } => {
                if sent.settled.is_empty() {
                    continue;
                }
                summary.duplicates += 1;
                let key = sent.settled[pick % sent.settled.len()];
                assert!(!state.complete(key, result, None, now), "{PROPERTY}");
            }
            DialerOp::Rank {
                book,
                registered,
                hold_limit,
                room,
            } => {
                summary.rankings += 1;
                summary.preferred_dials += rank(
                    &mut state,
                    &mut sent,
                    &mut deferred,
                    &nodes,
                    book,
                    registered,
                    hold_limit,
                    room,
                );
            }
        }
        after_step(&mut state, &sent);
        check(&state, &sent, &nodes);
    }

    // Settling every remaining attempt releases every reservation.
    while let Some(attempt) = sent.in_flight.pop() {
        assert!(
            state.complete(attempt.key, DiscoveryDialResult::Failed, None, sent.now),
            "{PROPERTY}"
        );
        sent.settled.push(attempt.key);
    }
    state.assert_invariants();
    for ip in 0..IPS.len() {
        assert_eq!(state.in_flight_dials_to(table_ip(ip)), 0, "{PROPERTY}");
    }
    assert!(state.in_flight_node_ids().is_empty(), "{PROPERTY}");
    summary
}

/// Forms the preferred set from the standings a ranking step chose, runs the
/// preferred pass over it, records its attempts in `sent`, and returns how
/// many it started.
#[allow(
    clippy::too_many_arguments,
    reason = "one ranking step's decoded inputs and the replay's state"
)]
fn rank(
    state: &mut CandidateDialerState,
    sent: &mut Sent,
    deferred: &mut DeferredDials,
    nodes: &[NodeId],
    book: [GatewayStanding; NODES],
    registered: [bool; NODES],
    hold_limit: usize,
    room: PreferredRoom,
) -> usize {
    let standings: Vec<GatewayStanding> = (0..NODES)
        .map(|node| {
            if registered[node] {
                GatewayStanding::Connected
            } else if sent.in_flight.iter().any(|attempt| attempt.node == node) {
                GatewayStanding::Dialing
            } else {
                let target = match book[node] {
                    GatewayStanding::Unmatched => DialTarget::Unknown,
                    GatewayStanding::Dialable => DialTarget::Dialable(candidate(
                        nodes[node],
                        vec![table_address(RANKED_ADDRESS[node])],
                    )),
                    _ => DialTarget::InBackoff,
                };
                dialer_standing(state, &target, sent.now)
            }
        })
        .collect();
    let ranking = (0..NODES).map(|node| {
        RankedGateway::resolved(
            nodes[node],
            // `NODES` is a small constant, so the count fits.
            (NODES - node) as u32,
            standings[node],
        )
    });
    let formed = PreferredGateways::form(ranking, hold_limit);
    assert!(formed.set().len() <= formed.hold(), "{PREFERRED_PROPERTY}");
    for member in formed.set() {
        let node = nodes
            .iter()
            .position(|node| node == member)
            .expect("the set holds only ranked nodes");
        assert!(
            matches!(
                standings[node],
                GatewayStanding::Dialable | GatewayStanding::Dialing | GatewayStanding::Connected
            ),
            "{PREFERRED_PROPERTY}: node {node} holds a place while {:?}",
            standings[node]
        );
    }

    let dialable: HashMap<NodeId, ZakuraDiscoveryDialCandidate> = (0..NODES)
        .filter(|&node| standings[node] == GatewayStanding::Dialable)
        .map(|node| {
            (
                nodes[node],
                candidate(nodes[node], vec![table_address(RANKED_ADDRESS[node])]),
            )
        })
        .collect();
    let (admitted, claims) = admit_preferred_dials(
        state,
        formed.set(),
        &dialable,
        deferred,
        room,
        |_, in_flight| in_flight < PER_IP_CAP,
        sent.now,
    );
    assert!(
        claims.dials == admitted.len()
            && claims.dials <= room.connections.min(room.dials)
            && claims.address_attempts <= room.address_attempts,
        "{PREFERRED_PROPERTY}: the pass stays in its room"
    );
    for (key, node_addr) in &admitted {
        let node = nodes
            .iter()
            .position(|node| *node == key.node_id)
            .expect("the pass dials only ranked nodes");
        assert!(
            formed.contains(&key.node_id) && standings[node] == GatewayStanding::Dialable,
            "{PREFERRED_PROPERTY}: node {node} dialed while {:?}",
            standings[node]
        );
        assert!(
            key.node_id.as_bytes() > local_node_id().as_bytes()
                || sent
                    .preferred_dialable_since
                    .get(&key.node_id)
                    .is_some_and(|since| sent.now >= *since + MUTUAL_DIAL_GRACE),
            "{PREFERRED_PROPERTY}: node {node}, below this node's ID, dialed before the grace"
        );
        let ips: Vec<usize> = node_addr
            .direct
            .iter()
            .map(|addr| ip_index(canonical_ip(addr.ip())))
            .collect();
        for &ip in &ips {
            assert!(
                !state.is_backed_off(key.node_id, table_ip(ip), sent.now),
                "{PREFERRED_PROPERTY}: node {node} dialed through IP {ip} in backoff"
            );
        }
        sent.in_flight.push(Attempt {
            key: *key,
            node,
            ips,
        });
    }
    let dialed: Vec<NodeId> = admitted.iter().map(|(key, _)| key.node_id).collect();
    sent.preferred_dialable_since = formed
        .set()
        .iter()
        .filter(|member| dialable.contains_key(*member) && !dialed.contains(member))
        .map(|member| {
            let since = sent.preferred_dialable_since.get(member);
            (*member, since.copied().unwrap_or(sent.now))
        })
        .collect();
    admitted.len()
}
