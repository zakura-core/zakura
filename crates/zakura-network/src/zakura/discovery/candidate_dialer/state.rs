//! The candidate dialer's policy: which candidate addresses it may dial, the
//! attempts in flight with their per-IP reservations, the backoff each
//! `(node, IP)` pair earned, and the local-limit pause.
//!
//! The dialer loop does the IO: it asks the supervisor for per-IP capacity,
//! spawns and joins the dial workers, and updates the discovery book. These
//! transitions take the current time as an argument.

use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use zakura_quic::NodeId;
use zakura_rt::{
    collections::{hash_map::Entry, HashMap},
    time::Instant,
};

use super::{DiscoveryDialResult, ZAKURA_DISCOVERY_DIAL_INTERVAL};
use crate::zakura::{canonical_ip, discovery::ZakuraDiscoveryDialCandidate};

/// The identity of one candidate dial attempt.
///
/// The runtime join owner attaches this key to completion, panic, and cancellation.
/// Settlement releases exactly the IPs this attempt reserved.
/// A later attempt to the same node gets a new key, so a duplicate completion cannot settle it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct DialAttemptKey {
    /// The dialed node.
    pub(super) node_id: NodeId,
    /// The dialer's attempt counter when it recorded this attempt.
    sequence: u64,
}

/// A candidate address the dialer may reserve: the first address of its IP
/// in record order, with that IP canonicalised.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct CandidateAddress {
    /// The address to dial.
    pub(super) addr: SocketAddr,
    /// The canonical IP that the reservation counts.
    pub(super) ip: IpAddr,
}

#[derive(Debug)]
struct InFlightDial {
    sequence: u64,
    /// The IPs this attempt reserved when the dialer recorded it.
    reserved_ips: Vec<IpAddr>,
}

/// Exponential dial backoff for a single `(node_id, ip)` pair.
///
/// Backoff is keyed by `(NodeId, IpAddr)`, not `IpAddr` alone: a signature on a
/// gossip record proves control of the node key, not ownership of the addresses
/// it advertises. Keying failure backoff by IP alone would let an attacker sign
/// throwaway node ids that all advertise an honest peer's IP and, by failing
/// those dials, back that IP off for every legitimate candidate that shares it.
/// The in-flight reservation cap (`in_flight_by_ip`) stays IP-scoped because it
/// bounds a real local resource (concurrent dials to one address).
#[derive(Copy, Clone, Debug)]
struct DiscoveryIpBackoff {
    failure_count: u32,
    retry_at: Instant,
}

/// The candidate dialer's logical state.
///
/// It records an attempt only after the supervisor accepted at least one of
/// the candidate's IPs. Its per-IP counts are logical reservations: the
/// endpoint still takes the real admission permit inside the dial, and a
/// refusal there returns as [`DiscoveryDialResult::LocalResourceLimit`].
#[derive(Debug)]
pub(super) struct CandidateDialerState {
    backoff_base: Duration,
    backoff_max: Duration,
    /// At most one attempt per node.
    in_flight: HashMap<NodeId, InFlightDial>,
    /// The number of in-flight attempts that reserved each canonical IP.
    in_flight_by_ip: HashMap<IpAddr, usize>,
    backoff_by_node_ip: HashMap<(NodeId, IpAddr), DiscoveryIpBackoff>,
    /// After a dial hits a local limit, wait one tick before the next round:
    /// the limit doesn't back off the candidate, so an immediate retry would
    /// spin while the node stays full.
    paused_until: Option<Instant>,
    next_sequence: u64,
}

impl CandidateDialerState {
    /// Builds an idle dialer whose per-`(node, IP)` failure backoff starts at
    /// `dial_backoff.0` and doubles up to `dial_backoff.1`.
    pub(super) fn new(dial_backoff: (Duration, Duration)) -> Self {
        Self {
            backoff_base: dial_backoff.0,
            backoff_max: dial_backoff.1,
            in_flight: HashMap::default(),
            in_flight_by_ip: HashMap::default(),
            backoff_by_node_ip: HashMap::default(),
            paused_until: None,
            next_sequence: 0,
        }
    }

    /// Starts a dial round at `now`.
    ///
    /// Prunes backoff entries that expired more than the maximum backoff ago,
    /// then returns whether the round may dial. It may not while a local-limit
    /// pause holds; a round at or after the pause's end clears the pause.
    pub(super) fn begin_round(&mut self, now: Instant) -> bool {
        let retention = self.backoff_max;
        self.backoff_by_node_ip
            .retain(|_, backoff| now.saturating_duration_since(backoff.retry_at) <= retention);

        if self.paused_until.is_some_and(|until| now < until) {
            return false;
        }
        self.paused_until = None;
        true
    }

    /// Returns the nodes that have an attempt in flight.
    pub(super) fn in_flight_node_ids(&self) -> Vec<NodeId> {
        self.in_flight.keys().copied().collect()
    }

    /// Returns the number of in-flight attempts that reserved `ip`.
    ///
    /// `ip` must be canonical, as [`CandidateAddress::ip`] is.
    pub(super) fn in_flight_dials_to(&self, ip: IpAddr) -> usize {
        self.in_flight_by_ip.get(&ip).copied().unwrap_or_default()
    }

    /// Returns the addresses of `candidate` that the dialer may reserve at
    /// `now`, before the supervisor's per-IP check.
    ///
    /// The endpoint starts one handshake per address, but the reservation
    /// counts one per IP. Dialing only the first address of each IP, in record
    /// order, keeps the two equal, so a record can't list many ports on one IP
    /// to multiply handshakes past the per-IP cap. An IP in backoff for this
    /// node yields no address.
    pub(super) fn candidate_addresses(
        &self,
        candidate: &ZakuraDiscoveryDialCandidate,
        now: Instant,
    ) -> Vec<CandidateAddress> {
        let mut checked_ips = Vec::new();
        let mut addresses = Vec::new();
        for addr in &candidate.direct_addrs {
            let ip = canonical_ip(addr.ip());
            if checked_ips.contains(&ip) {
                continue;
            }
            checked_ips.push(ip);
            if !self.is_backed_off(candidate.node_id, ip, now) {
                addresses.push(CandidateAddress { addr: *addr, ip });
            }
        }
        addresses
    }

    /// Returns whether `node_id` is in failure backoff for the canonical `ip`
    /// at `now`.
    pub(super) fn is_backed_off(&self, node_id: NodeId, ip: IpAddr, now: Instant) -> bool {
        self.backoff_by_node_ip
            .get(&(node_id, ip))
            .is_some_and(|backoff| now < backoff.retry_at)
    }

    /// Records an attempt to `node_id` that reserves `reserved_ips`, the
    /// canonical IPs the supervisor accepted.
    ///
    /// Returns `None`, and records nothing, when `reserved_ips` is empty or
    /// the node already has an attempt in flight.
    pub(super) fn record_attempt(
        &mut self,
        node_id: NodeId,
        reserved_ips: Vec<IpAddr>,
    ) -> Option<DialAttemptKey> {
        if reserved_ips.is_empty() || self.in_flight.contains_key(&node_id) {
            return None;
        }

        for ip in &reserved_ips {
            *self.in_flight_by_ip.entry(*ip).or_default() += 1;
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.in_flight.insert(
            node_id,
            InFlightDial {
                sequence,
                reserved_ips,
            },
        );
        Some(DialAttemptKey { node_id, sequence })
    }

    /// Settles the attempt `key` with its worker's `result` at `now`.
    ///
    /// Releases the IPs the attempt reserved when the dialer recorded it, even
    /// if the node's addresses changed since. Applies the result's backoff to
    /// those IPs; `admitted_ip` is the IP whose handshake TLS authenticated, if
    /// the dial got that far. A local limit pauses the next round without
    /// backing off the candidate.
    ///
    /// Returns `false`, and changes nothing, when `key` is not in flight.
    pub(super) fn complete(
        &mut self,
        key: DialAttemptKey,
        result: DiscoveryDialResult,
        admitted_ip: Option<IpAddr>,
        now: Instant,
    ) -> bool {
        let attempt = match self.in_flight.entry(key.node_id) {
            Entry::Occupied(attempt) if attempt.get().sequence == key.sequence => attempt.remove(),
            _ => return false,
        };

        if result == DiscoveryDialResult::LocalResourceLimit {
            self.paused_until = Some(now + ZAKURA_DISCOVERY_DIAL_INTERVAL);
        }
        for ip in &attempt.reserved_ips {
            let Some(count) = self.in_flight_by_ip.get_mut(ip) else {
                continue;
            };
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_flight_by_ip.remove(ip);
            }
        }
        self.apply_backoff(key.node_id, &attempt.reserved_ips, admitted_ip, result, now);
        true
    }

    fn apply_backoff(
        &mut self,
        node_id: NodeId,
        ips: &[IpAddr],
        admitted_ip: Option<IpAddr>,
        result: DiscoveryDialResult,
        now: Instant,
    ) {
        match result {
            // A signed record proves control of the node id, not of every address it
            // advertises. Only the address that TLS authenticated earns a reset, so a
            // record can't pair one live endpoint with decoys to clear their backoff.
            DiscoveryDialResult::Registered
            | DiscoveryDialResult::ConnectedElsewhere
            | DiscoveryDialResult::ShortLivedRegistered => {
                if let Some(admitted_ip) = admitted_ip {
                    self.backoff_by_node_ip
                        .remove(&(node_id, canonical_ip(admitted_ip)));
                }
            }
            // The ALPN mismatch floor applies to the node, not to each address: an
            // aggregate dial error doesn't say which address mismatched (DIAL-5).
            DiscoveryDialResult::Failed | DiscoveryDialResult::AlpnMismatch => {
                for ip in ips {
                    let failure_count = self
                        .backoff_by_node_ip
                        .get(&(node_id, *ip))
                        .map_or(1, |backoff| backoff.failure_count.saturating_add(1));
                    let wait = discovery_ip_dial_backoff(
                        failure_count,
                        self.backoff_base,
                        self.backoff_max,
                    );
                    self.backoff_by_node_ip.insert(
                        (node_id, *ip),
                        DiscoveryIpBackoff {
                            failure_count,
                            retry_at: now + wait,
                        },
                    );
                }
            }
            DiscoveryDialResult::LocalResourceLimit => {}
        }
    }
}

fn discovery_ip_dial_backoff(
    failure_count: u32,
    dial_backoff_base: Duration,
    dial_backoff_max: Duration,
) -> Duration {
    let shift = failure_count.saturating_sub(1).min(10);
    dial_backoff_base
        .saturating_mul(1u32 << shift)
        .min(dial_backoff_max)
}
