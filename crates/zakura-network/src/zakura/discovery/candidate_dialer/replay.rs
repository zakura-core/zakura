//! The `discovery_candidate` machine target: decoder-v1 histories through
//! [`CandidateDialerState`], shared by its proptest and its cargo-fuzz target.
//!
//! [`replay_candidate_dial_bytes`] drives the production state as the dialer
//! loop does (see [`machine_test`](crate::zakura::machine_test)), with a
//! simulated supervisor that refuses IPs at its per-IP cap. It checks the
//! state's invariants after every operation, and that settling every attempt
//! releases every reservation. It compares outputs only with facts that
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

use super::state::{CandidateDialerState, DialAttemptKey};
use super::{DiscoveryDialResult, ZAKURA_DISCOVERY_DIAL_INTERVAL};
use crate::zakura::{discovery::ZakuraDiscoveryDialCandidate, machine_test::MAX_HISTORY_BYTES};

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
}

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

/// Applicable transitions in a decoder-v1 candidate dial history.
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
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes through
/// [`CandidateDialerState`]. Decoder v1 reads one operation per byte triple.
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
            match a % 5 {
                0 => DialerOp::Advance(u64::from(b) * 100),
                1 => DialerOp::Round,
                2 => DialerOp::Dial {
                    node: usize::from(a / 5) % NODES,
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
                _ => DialerOp::Duplicate {
                    pick: usize::from(b),
                    result,
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
    };

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
