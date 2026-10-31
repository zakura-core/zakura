//! Machine tests for [`CandidateDialerState`]: worked scenarios, the backoff
//! rules, and a proptest against an independent model.
//!
//! The model restates the documented rules. It never calls the code it
//! checks.

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

#[cfg(test)]
use proptest::prelude::*;
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::time::Instant;

#[cfg(test)]
use super::state::{classify_dial, DialFinish, DialObservation, PeerRegistration};
use super::state::{CandidateAddress, CandidateDialerState, DialAttemptKey};
use super::DiscoveryDialResult;
#[cfg(test)]
use crate::zakura::canonical_ip;
use crate::zakura::discovery::ZakuraDiscoveryDialCandidate;

#[cfg(test)]
const DIAL_BACKOFF: (Duration, Duration) = (Duration::from_secs(60), Duration::from_secs(3_600));

fn node_id(byte: u8) -> NodeId {
    NodeSecretKey::from_bytes(&[byte; 32]).public()
}

fn candidate(node_id: NodeId, direct_addrs: Vec<SocketAddr>) -> ZakuraDiscoveryDialCandidate {
    ZakuraDiscoveryDialCandidate {
        node_id,
        direct_addrs,
        is_static: false,
    }
}

/// Records an attempt to `node` that reserves `ips`, then completes it.
#[cfg(test)]
fn settle(
    state: &mut CandidateDialerState,
    node: NodeId,
    ips: &[IpAddr],
    admitted_ip: Option<IpAddr>,
    result: DiscoveryDialResult,
    now: Instant,
) {
    let attempt = state
        .record_attempt(node, ips.to_vec())
        .expect("the node has no attempt in flight");
    assert!(state.complete(attempt, result, admitted_ip, now));
}

// Worked scenario: an attempt's settlement releases the IPs it reserved, not
// the ones its node advertises now.
#[tokio::test(start_paused = true)]
async fn panicked_attempt_releases_its_original_reservations() {
    let a = node_id(1);
    let b = node_id(2);
    let first = IpAddr::from([93, 184, 216, 34]);
    let second = IpAddr::from([198, 51, 100, 7]);
    let moved = IpAddr::from([203, 0, 113, 9]);
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let attempt_a = state
        .record_attempt(a, vec![first, second])
        .expect("an idle dialer records A");
    let attempt_b = state
        .record_attempt(b, vec![first])
        .expect("B is a different node");
    assert_eq!(state.in_flight_dials_to(first), 2);

    // A's record now lists another address. A stays in flight under its old
    // reservations, so no second attempt to A starts.
    let moved_candidate = candidate(a, vec![SocketAddr::new(moved, 8233)]);
    assert_eq!(
        state.candidate_addresses(&moved_candidate, now),
        vec![CandidateAddress {
            addr: SocketAddr::new(moved, 8233),
            ip: moved,
        }]
    );
    assert_eq!(state.record_attempt(a, vec![moved]), None);
    assert_eq!(state.in_flight_dials_to(moved), 0);

    // A's worker panics, which the dialer settles as a failure.
    assert!(state.complete(attempt_a, DiscoveryDialResult::Failed, None, now));
    assert_eq!(state.in_flight_dials_to(first), 1);
    assert_eq!(state.in_flight_dials_to(second), 0);
    assert_eq!(state.in_flight_dials_to(moved), 0);
    assert!(state.is_backed_off(a, first, now));
    assert!(state.is_backed_off(a, second, now));
    assert!(!state.is_backed_off(a, moved, now));
    assert!(!state.is_backed_off(b, first, now));
    assert_eq!(state.in_flight_node_ids(), vec![b]);

    assert!(state.complete(attempt_b, DiscoveryDialResult::Registered, Some(first), now));
    assert_eq!(state.in_flight_dials_to(first), 0);
    assert!(state.in_flight_node_ids().is_empty());
}

// Worked scenario: a duplicate completion of a settled attempt changes
// nothing, even when a newer attempt to the same node is in flight.
#[tokio::test(start_paused = true)]
async fn duplicate_completion_leaves_the_next_attempt_in_flight() {
    let node = node_id(1);
    let other = node_id(2);
    let ip = IpAddr::from([93, 184, 216, 34]);
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let attempt_a = state
        .record_attempt(node, vec![ip])
        .expect("an idle dialer records A");
    assert!(state.complete(attempt_a, DiscoveryDialResult::Registered, Some(ip), now));

    let attempt_b = state
        .record_attempt(node, vec![ip])
        .expect("A settled, so the node can be dialed again");
    let attempt_c = state
        .record_attempt(other, vec![ip])
        .expect("C is a different node");
    assert_ne!(attempt_a, attempt_b);

    for result in [
        DiscoveryDialResult::Failed,
        DiscoveryDialResult::LocalResourceLimit,
        DiscoveryDialResult::Registered,
    ] {
        assert!(!state.complete(attempt_a, result, None, now));
    }
    assert_eq!(state.in_flight_dials_to(ip), 2);
    assert!(!state.is_backed_off(node, ip, now));
    assert!(state.begin_round(now), "a stale local limit must not pause");

    assert!(state.complete(attempt_b, DiscoveryDialResult::Failed, None, now));
    assert!(state.complete(attempt_c, DiscoveryDialResult::Failed, None, now));
    assert_eq!(state.in_flight_dials_to(ip), 0);
}

// A local limit pauses the next round for one tick without backing off the
// candidate's IPs.
#[tokio::test(start_paused = true)]
async fn local_limit_pauses_without_backing_off() {
    let node = node_id(1);
    let ip = IpAddr::from([93, 184, 216, 34]);
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        node,
        &[ip],
        None,
        DiscoveryDialResult::LocalResourceLimit,
        now,
    );

    assert!(!state.is_backed_off(node, ip, now));
    assert!(!state.begin_round(now + Duration::from_millis(999)));
    assert!(state.begin_round(now + Duration::from_secs(1)));
    assert!(state.begin_round(now + Duration::from_secs(1)));
}

#[tokio::test(start_paused = true)]
async fn failed_dials_back_off_only_the_failing_node_for_that_ip() {
    let failing_node = node_id(1);
    let honest_node = node_id(2);
    let ip = IpAddr::from([93, 184, 216, 34]);
    let mapped_ip: IpAddr = "::ffff:93.184.216.34"
        .parse()
        .expect("mapped test address parses");
    let six_to_four_ip: IpAddr = "2002:5db8:d822::"
        .parse()
        .expect("6to4 test address parses");
    let teredo_ip: IpAddr = "2001:0:c000:22d::a247:27dd"
        .parse()
        .expect("Teredo test address parses");
    assert_eq!(canonical_ip(mapped_ip), ip);
    assert_eq!(canonical_ip(six_to_four_ip), ip);
    assert_eq!(canonical_ip(teredo_ip), ip);
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        failing_node,
        &[ip],
        None,
        DiscoveryDialResult::Failed,
        now,
    );
    // The failing node is backed off for this IP, canonicalised so v4/v6
    // variants of the same address share the entry.
    assert!(state.is_backed_off(failing_node, ip, now));
    assert!(state.is_backed_off(failing_node, canonical_ip(mapped_ip), now));
    // A different node advertising the same IP is NOT poisoned: a signature
    // proves node-key control, not IP ownership.
    assert!(!state.is_backed_off(honest_node, ip, now));
    assert!(!state.is_backed_off(failing_node, ip, now + Duration::from_secs(60)));

    let second_attempt = now + Duration::from_secs(60);
    settle(
        &mut state,
        failing_node,
        &[ip],
        None,
        DiscoveryDialResult::Failed,
        second_attempt,
    );
    assert!(state.is_backed_off(failing_node, ip, second_attempt + Duration::from_secs(119)));
    assert!(!state.is_backed_off(failing_node, ip, second_attempt + Duration::from_secs(120)));

    settle(
        &mut state,
        failing_node,
        &[ip],
        Some(ip),
        DiscoveryDialResult::Registered,
        second_attempt,
    );
    // A third failure would wait the first step again only if the success
    // removed the entry.
    settle(
        &mut state,
        failing_node,
        &[ip],
        None,
        DiscoveryDialResult::Failed,
        second_attempt,
    );
    assert!(!state.is_backed_off(failing_node, ip, second_attempt + Duration::from_secs(60)));
}

// V12 F-305587: an ALPN mismatch from one stale address used to put every
// address of the record into the ten-minute floor. The floor now lives on
// the node in the discovery book, and each address gets the ordinary backoff.
#[tokio::test(start_paused = true)]
async fn alpn_mismatch_gives_each_address_the_ordinary_backoff() {
    let node = node_id(1);
    let ips = [
        IpAddr::from([93, 184, 216, 34]),
        IpAddr::from([198, 51, 100, 7]),
    ];
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        node,
        &ips,
        None,
        DiscoveryDialResult::AlpnMismatch,
        now,
    );

    for ip in ips {
        assert!(state.is_backed_off(node, ip, now + Duration::from_secs(59)));
        assert!(!state.is_backed_off(node, ip, now + Duration::from_secs(60)));
    }
}

// V12 F-58: a record can pair one live endpoint with decoy addresses. A
// success through the live endpoint must not reset the decoys' backoff.
#[tokio::test(start_paused = true)]
async fn success_clears_backoff_only_for_the_admitted_ip() {
    let node = node_id(1);
    let admitted = IpAddr::from([93, 184, 216, 34]);
    let decoy = IpAddr::from([198, 51, 100, 7]);
    let mapped_admitted: IpAddr = "::ffff:93.184.216.34"
        .parse()
        .expect("mapped test address parses");
    let now = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    settle(
        &mut state,
        node,
        &[admitted, decoy],
        None,
        DiscoveryDialResult::Failed,
        now,
    );
    for result in [
        DiscoveryDialResult::Registered,
        DiscoveryDialResult::ConnectedElsewhere,
        DiscoveryDialResult::ShortLivedRegistered,
    ] {
        settle(
            &mut state,
            node,
            &[admitted, decoy],
            Some(mapped_admitted),
            result,
            now,
        );
        assert!(!state.is_backed_off(node, admitted, now));
        // The decoy keeps its single failure: its backoff ends after the
        // first step.
        assert!(
            state.is_backed_off(node, decoy, now + Duration::from_secs(59)),
            "{result:?} must keep the decoy's failure metadata"
        );
        assert!(!state.is_backed_off(node, decoy, now + Duration::from_secs(60)));
    }

    // A success this dial didn't authenticate itself resets nothing.
    settle(
        &mut state,
        node,
        &[decoy],
        None,
        DiscoveryDialResult::ConnectedElsewhere,
        now,
    );
    assert!(state.is_backed_off(node, decoy, now));
}

// The model's universe: a few nodes, canonical IPs, and address variants
// whose canonical IP the table states directly.
const MODEL_NODES: usize = 3;
const MODEL_IPS: [Ipv4Addr; 3] = [
    Ipv4Addr::new(93, 184, 216, 34),
    Ipv4Addr::new(198, 51, 100, 7),
    Ipv4Addr::new(203, 0, 113, 9),
];
/// `(canonical IP index, mapped into IPv6, port)` for each address variant.
const MODEL_ADDRESSES: [(usize, bool, u16); 7] = [
    (0, false, 8233),
    (0, false, 8234),
    (0, true, 8235),
    (1, false, 8233),
    (1, true, 8233),
    (2, false, 8233),
    (2, true, 8236),
];
/// The supervisor's simulated per-IP cap.
const MODEL_PER_IP_CAP: usize = 2;
const MODEL_BACKOFF_BASE_MS: u64 = 2_000;
const MODEL_BACKOFF_MAX_MS: u64 = 20_000;
/// One dial interval: the local-limit pause.
const MODEL_PAUSE_MS: u64 = 1_000;

fn model_nodes() -> Vec<NodeId> {
    (10..).take(MODEL_NODES).map(node_id).collect()
}

fn model_ip(index: usize) -> IpAddr {
    IpAddr::V4(MODEL_IPS[index])
}

fn model_address(index: usize) -> SocketAddr {
    let (ip, mapped, port) = MODEL_ADDRESSES[index];
    let ip = if mapped {
        IpAddr::V6(MODEL_IPS[ip].to_ipv6_mapped())
    } else {
        IpAddr::V4(MODEL_IPS[ip])
    };
    SocketAddr::new(ip, port)
}

#[derive(Clone, Debug)]
enum DialerOp {
    Advance(u64),
    Round,
    Dial {
        node: usize,
        addresses: Vec<usize>,
        refused: [bool; MODEL_IPS.len()],
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

#[cfg(test)]
fn dial_result() -> impl Strategy<Value = DiscoveryDialResult> {
    prop_oneof![
        Just(DiscoveryDialResult::Registered),
        Just(DiscoveryDialResult::ConnectedElsewhere),
        Just(DiscoveryDialResult::ShortLivedRegistered),
        Just(DiscoveryDialResult::Failed),
        Just(DiscoveryDialResult::AlpnMismatch),
        Just(DiscoveryDialResult::LocalResourceLimit),
    ]
}

#[cfg(test)]
fn dialer_op() -> impl Strategy<Value = DialerOp> {
    prop_oneof![
        2 => (0u64..25_000).prop_map(DialerOp::Advance),
        2 => Just(DialerOp::Round),
        4 => (
            0..MODEL_NODES,
            prop::collection::vec(0..MODEL_ADDRESSES.len(), 0..6),
            prop::array::uniform3(prop::bool::weighted(0.2)),
        )
            .prop_map(|(node, addresses, refused)| DialerOp::Dial {
                node,
                addresses,
                refused,
            }),
        4 => (
            any::<usize>(),
            dial_result(),
            prop::option::of(0..MODEL_ADDRESSES.len()),
        )
            .prop_map(|(pick, result, admitted)| DialerOp::Complete {
                pick,
                result,
                admitted,
            }),
        1 => (any::<usize>(), dial_result())
            .prop_map(|(pick, result)| DialerOp::Duplicate { pick, result }),
    ]
}

#[derive(Debug)]
struct ModelAttempt {
    key: DialAttemptKey,
    node: usize,
    ips: Vec<usize>,
}

/// The documented candidate-dialer rules, restated over table indexes.
#[derive(Debug, Default)]
struct DialerModel {
    now_ms: u64,
    paused_until_ms: Option<u64>,
    in_flight: Vec<ModelAttempt>,
    settled: Vec<DialAttemptKey>,
    /// `(node, ip)` → `(failure count, retry deadline)`.
    backoff: HashMap<(usize, usize), (u32, u64)>,
}

impl DialerModel {
    fn reserved(&self, ip: usize) -> usize {
        self.in_flight
            .iter()
            .map(|attempt| {
                attempt
                    .ips
                    .iter()
                    .filter(|&&reserved| reserved == ip)
                    .count()
            })
            .sum()
    }

    fn backed_off(&self, node: usize, ip: usize) -> bool {
        self.backoff
            .get(&(node, ip))
            .is_some_and(|&(_, retry_at)| self.now_ms < retry_at)
    }

    fn round(&mut self) -> bool {
        let now = self.now_ms;
        self.backoff
            .retain(|_, &mut (_, retry_at)| now.saturating_sub(retry_at) <= MODEL_BACKOFF_MAX_MS);
        if self.paused_until_ms.is_some_and(|until| now < until) {
            return false;
        }
        self.paused_until_ms = None;
        true
    }

    /// The first address variant of each canonical IP, unless that IP is in
    /// backoff for `node`.
    fn candidate_addresses(&self, node: usize, addresses: &[usize]) -> Vec<(usize, usize)> {
        let mut seen = HashSet::new();
        addresses
            .iter()
            .filter_map(|&address| {
                let ip = MODEL_ADDRESSES[address].0;
                (seen.insert(ip) && !self.backed_off(node, ip)).then_some((address, ip))
            })
            .collect()
    }

    fn complete(&mut self, index: usize, result: DiscoveryDialResult, admitted: Option<usize>) {
        let attempt = self.in_flight.remove(index);
        self.settled.push(attempt.key);
        match result {
            DiscoveryDialResult::LocalResourceLimit => {
                self.paused_until_ms = Some(self.now_ms + MODEL_PAUSE_MS);
            }
            DiscoveryDialResult::Failed | DiscoveryDialResult::AlpnMismatch => {
                for ip in attempt.ips {
                    let count = self
                        .backoff
                        .get(&(attempt.node, ip))
                        .map_or(1, |&(count, _)| count + 1);
                    let wait =
                        (MODEL_BACKOFF_BASE_MS << (count - 1).min(10)).min(MODEL_BACKOFF_MAX_MS);
                    self.backoff
                        .insert((attempt.node, ip), (count, self.now_ms + wait));
                }
            }
            DiscoveryDialResult::Registered
            | DiscoveryDialResult::ConnectedElsewhere
            | DiscoveryDialResult::ShortLivedRegistered => {
                if let Some(admitted) = admitted {
                    self.backoff
                        .remove(&(attempt.node, MODEL_ADDRESSES[admitted].0));
                }
            }
        }
    }
}

fn check_dialer(state: &CandidateDialerState, model: &DialerModel, nodes: &[NodeId], now: Instant) {
    let mut in_flight = state.in_flight_node_ids();
    let unique: HashSet<_> = in_flight.iter().copied().collect();
    assert_eq!(
        unique.len(),
        in_flight.len(),
        "{PROPERTY}: one attempt per node"
    );
    let mut expected: Vec<_> = model
        .in_flight
        .iter()
        .map(|attempt| nodes[attempt.node])
        .collect();
    in_flight.sort();
    expected.sort();
    assert_eq!(in_flight, expected, "{PROPERTY}");

    for ip in 0..MODEL_IPS.len() {
        let reserved = state.in_flight_dials_to(model_ip(ip));
        assert_eq!(
            reserved,
            model.reserved(ip),
            "{PROPERTY}: reservations of IP {}",
            ip
        );
        assert!(reserved <= MODEL_PER_IP_CAP, "{PROPERTY}");
        for (node, &node_id) in nodes.iter().enumerate() {
            assert_eq!(
                state.is_backed_off(node_id, model_ip(ip), now),
                model.backed_off(node, ip),
                "{PROPERTY}: backoff of node {} for IP {}",
                node,
                ip
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

/// Replay decoder-v1 triples against the existing candidate dialer model.
/// Read at most 512 bytes. Logical time advances only through decoded events.
pub fn replay_candidate_dial_bytes(data: &[u8]) -> CandidateDialReplaySummary {
    run(decode(data), |_, _, _| {})
}

fn decode(data: &[u8]) -> impl Iterator<Item = DialerOp> + '_ {
    data[..data.len().min(512)].chunks(3).map(|chunk| {
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
                node: usize::from(a / 5) % MODEL_NODES,
                addresses: (0..MODEL_ADDRESSES.len())
                    .filter(|bit| b & (1 << bit) != 0)
                    .collect(),
                refused: [c & 1 != 0, c & 2 != 0, c & 4 != 0],
            },
            3 => DialerOp::Complete {
                pick: usize::from(b),
                result,
                admitted: (c & 8 != 0).then_some(usize::from(c / 16) % MODEL_ADDRESSES.len()),
            },
            _ => DialerOp::Duplicate {
                pick: usize::from(b),
                result,
            },
        }
    })
}

fn run(
    ops: impl IntoIterator<Item = DialerOp>,
    mut after_step: impl FnMut(&mut CandidateDialerState, &DialerModel, Instant),
) -> CandidateDialReplaySummary {
    let nodes = model_nodes();
    let start = Instant::now();
    let mut state = CandidateDialerState::new((
        Duration::from_millis(MODEL_BACKOFF_BASE_MS),
        Duration::from_millis(MODEL_BACKOFF_MAX_MS),
    ));
    let mut model = DialerModel::default();

    let mut summary = CandidateDialReplaySummary::default();
    for op in ops {
        let now = start + Duration::from_millis(model.now_ms);
        match op {
            DialerOp::Advance(ms) => model.now_ms += ms,
            DialerOp::Round => {
                summary.rounds += 1;
                assert_eq!(state.begin_round(now), model.round(), "{PROPERTY}");
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
                        .map(|&address| model_address(address))
                        .collect(),
                );
                let expected = model.candidate_addresses(node, &addresses);
                let planned = state.candidate_addresses(&dialed, now);
                assert_eq!(
                    &planned,
                    &expected
                        .iter()
                        .map(|&(address, ip)| CandidateAddress {
                            addr: model_address(address),
                            ip: model_ip(ip),
                        })
                        .collect::<Vec<_>>(),
                    "{PROPERTY}"
                );

                // The simulated supervisor refuses an IP the run marks
                // refused or one at its cap, counting this dialer's
                // in-flight attempts as the real check does.
                let accepted: Vec<_> = expected
                    .iter()
                    .map(|&(_, ip)| ip)
                    .filter(|&ip| !refused[ip] && model.reserved(ip) < MODEL_PER_IP_CAP)
                    .collect();
                let recorded = state.record_attempt(
                    nodes[node],
                    accepted.iter().map(|&ip| model_ip(ip)).collect(),
                );
                let busy = model.in_flight.iter().any(|attempt| attempt.node == node);
                match recorded {
                    Some(key) => {
                        summary.attempts += 1;
                        assert!(!accepted.is_empty() && !busy, "{PROPERTY}");
                        assert_eq!(key.node_id, nodes[node], "{PROPERTY}");
                        assert!(
                            !model.settled.contains(&key),
                            "{PROPERTY}: keys are never reused"
                        );
                        model.in_flight.push(ModelAttempt {
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
                if model.in_flight.is_empty() {
                    continue;
                }
                let index = pick % model.in_flight.len();
                let key = model.in_flight[index].key;
                assert!(
                    state.complete(
                        key,
                        result,
                        admitted.map(model_address).map(|addr| addr.ip()),
                        now
                    ),
                    "{PROPERTY}"
                );
                model.complete(index, result, admitted);
                summary.completions += 1;
                summary.local_limits +=
                    usize::from(result == DiscoveryDialResult::LocalResourceLimit);
                summary.failures += usize::from(matches!(
                    result,
                    DiscoveryDialResult::Failed | DiscoveryDialResult::AlpnMismatch
                ));
            }
            DialerOp::Duplicate { pick, result } => {
                if model.settled.is_empty() {
                    continue;
                }
                summary.duplicates += 1;
                let key = model.settled[pick % model.settled.len()];
                assert!(!state.complete(key, result, None, now), "{PROPERTY}");
            }
        }
        let now = start + Duration::from_millis(model.now_ms);
        after_step(&mut state, &model, now);
        check_dialer(&state, &model, &nodes, now);
    }

    // Settling every remaining attempt releases every reservation.
    let now = start + Duration::from_millis(model.now_ms);
    while let Some(attempt) = model.in_flight.first() {
        assert!(
            state.complete(attempt.key, DiscoveryDialResult::Failed, None, now),
            "{PROPERTY}"
        );
        model.complete(0, DiscoveryDialResult::Failed, None);
    }
    check_dialer(&state, &model, &nodes, now);
    for ip in 0..MODEL_IPS.len() {
        assert_eq!(state.in_flight_dials_to(model_ip(ip)), 0, "{PROPERTY}");
    }
    summary
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn candidate_dialer_matches_independent_model(ops in prop::collection::vec(dialer_op(), 1..80)) {
        run(ops, |_, _, _| {});
    }

    #[test]
    fn candidate_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_candidate_dial_bytes(&bytes);
    }
}

#[cfg(test)]
const WITNESS: &[u8] = &[
    2, 7, 0, 3, 0, 3, 4, 0, 3, 0, 30, 0, 1, 0, 0, 2, 7, 0, 3, 0, 5, 1, 0, 0, 0, 10, 0, 1, 0, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn candidate_witness_exercises_identity_backoff_and_local_limits() {
    let summary = replay_candidate_dial_bytes(WITNESS);
    assert_eq!(
        summary,
        CandidateDialReplaySummary {
            attempts: 2,
            completions: 2,
            duplicates: 1,
            local_limits: 1,
            failures: 1,
            rounds: 3,
        }
    );
    assert_eq!(
        replay_candidate_dial_bytes(&[]),
        CandidateDialReplaySummary::default()
    );
    println!("tier=Machine target=discovery_candidate decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "discovery/dial-attempts")]
fn candidate_witness_rejects_premature_reservation_release() {
    run(decode(WITNESS), |state, model, now| {
        if let Some(attempt) = model.in_flight.first() {
            state.complete(attempt.key, DiscoveryDialResult::Registered, None, now);
        }
    });
}

#[test]
#[should_panic(expected = "discovery/dial-attempts")]
fn candidate_witness_rejects_lost_backoff() {
    run(decode(WITNESS), |state, _, now| {
        state.begin_round(now + Duration::from_secs(120));
    });
}

/// Every observation against the outcome the attempt's identity earns. A
/// registration without this attempt's authentication never credits or charges
/// the attempt.
#[test]
fn registrations_credit_only_the_attempt_that_authenticated() {
    use DiscoveryDialResult as Result;
    use PeerRegistration::{Absent, Ended, Live};
    let dial = DialObservation::DialEnded;
    for (observation, authenticated, expected) in [
        (
            dial(DialFinish::Served, Live),
            true,
            Some(Result::ConnectedElsewhere),
        ),
        (
            dial(DialFinish::Failed, Live),
            false,
            Some(Result::ConnectedElsewhere),
        ),
        (
            dial(DialFinish::LocalLimit, Live),
            false,
            Some(Result::ConnectedElsewhere),
        ),
        (
            dial(DialFinish::Served, Ended),
            true,
            Some(Result::ShortLivedRegistered),
        ),
        (
            dial(DialFinish::Failed, Ended),
            true,
            Some(Result::ShortLivedRegistered),
        ),
        (dial(DialFinish::Failed, Ended), false, Some(Result::Failed)),
        (
            dial(DialFinish::LocalLimit, Ended),
            false,
            Some(Result::LocalResourceLimit),
        ),
        (
            dial(DialFinish::AlpnMismatch, Ended),
            false,
            Some(Result::AlpnMismatch),
        ),
        (
            dial(DialFinish::Served, Absent),
            true,
            Some(Result::ShortLivedRegistered),
        ),
        (
            dial(DialFinish::Served, Absent),
            false,
            Some(Result::Failed),
        ),
        (dial(DialFinish::Failed, Absent), true, Some(Result::Failed)),
        (
            dial(DialFinish::LocalLimit, Absent),
            true,
            Some(Result::LocalResourceLimit),
        ),
        (
            dial(DialFinish::AlpnMismatch, Absent),
            false,
            Some(Result::AlpnMismatch),
        ),
        (
            DialObservation::RegistrationHealthy,
            true,
            Some(Result::Registered),
        ),
        (
            DialObservation::RegistrationHealthy,
            false,
            Some(Result::ConnectedElsewhere),
        ),
        (
            DialObservation::RegistrationEnded,
            true,
            Some(Result::ShortLivedRegistered),
        ),
        (DialObservation::RegistrationEnded, false, None),
        (DialObservation::WatchClosed, true, Some(Result::Failed)),
        (DialObservation::WatchClosed, false, Some(Result::Failed)),
    ] {
        assert_eq!(
            classify_dial(observation, authenticated),
            expected,
            "{observation:?} authenticated={authenticated}"
        );
    }
}
