//! The independent model of [`CandidateDialerState`](super::state::CandidateDialerState),
//! shared by its proptest and its cargo-fuzz target. The model restates the
//! documented rules and never calls the code it checks.

use super::state::{CandidateAddress, CandidateDialerState, DialAttemptKey};
use super::DiscoveryDialResult;
use crate::zakura::discovery::ZakuraDiscoveryDialCandidate;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::collections::{HashMap, HashSet};
use zakura_rt::time::Instant;

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

// The model's universe: a few nodes, canonical IPs, and address variants
// whose canonical IP the table states directly.
pub(super) const MODEL_NODES: usize = 3;

const MODEL_IPS: [Ipv4Addr; 3] = [
    Ipv4Addr::new(93, 184, 216, 34),
    Ipv4Addr::new(198, 51, 100, 7),
    Ipv4Addr::new(203, 0, 113, 9),
];

/// `(canonical IP index, mapped into IPv6, port)` for each address variant.
pub(super) const MODEL_ADDRESSES: [(usize, bool, u16); 7] = [
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
pub(super) enum DialerOp {
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

#[derive(Debug)]
pub(super) struct ModelAttempt {
    pub(super) key: DialAttemptKey,
    node: usize,
    ips: Vec<usize>,
}

/// The documented candidate-dialer rules, restated over table indexes.
#[derive(Debug, Default)]
pub(super) struct DialerModel {
    now_ms: u64,
    paused_until_ms: Option<u64>,
    pub(super) in_flight: Vec<ModelAttempt>,
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

pub(super) fn decode(data: &[u8]) -> impl Iterator<Item = DialerOp> + '_ {
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

pub(super) fn run(
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
