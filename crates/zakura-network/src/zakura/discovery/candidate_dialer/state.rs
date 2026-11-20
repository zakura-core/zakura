//! The candidate dialer's policy: which candidate addresses it may dial, the
//! attempts in flight with their per-IP reservations, the backoff each
//! `(node, IP)` pair earned, and when the next dial round may start.
//!
//! The dialer loop does the IO: it asks the supervisor for per-IP capacity,
//! spawns and joins the dial workers, and updates the discovery book. These
//! transitions take the current time as an argument.

use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use zakura_quic::NodeId;
use zakura_rt::{
    collections::{hash_map::Entry, HashMap},
    time::Instant,
};

use super::{DiscoveryDialResult, ZAKURA_DISCOVERY_DIAL_INTERVAL};
use crate::zakura::{admission_host, canonical_ip, discovery::ZakuraDiscoveryDialCandidate};

/// The most `(node, IP)` backoff entries the dialer keeps (disco-23).
///
/// Only this node's own failed dials add entries, so the dial rate and an
/// entry's lifetime bound the count. That bound is far above this cap, so
/// a failure past the cap evicts the entry with the earliest `retry_at`.
/// Eviction is harmless: it shortens one backoff for a node ID that an
/// attacker can replace anyway. The cap keeps the map and its index under
/// 16 MiB; `dial_history_stays_under_its_memory_budget` derives both figures.
pub(super) const MAX_DIAL_BACKOFF_ENTRIES: usize = 1 << 15;

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

impl DialAttemptKey {
    /// The attempt's number, which trace rows carry.
    pub(super) fn sequence(self) -> u64 {
        self.sequence
    }
}

/// How one attempt's own dial ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum DialFinish {
    /// The dial registered its peer and finished serving the connection.
    Served,
    /// The dial authenticated its peer, and this node refused to register it
    /// for a reason other than a local limit.
    Refused,
    /// This node hit a local limit, before or at registration.
    LocalLimit,
    /// The peer speaks another ALPN.
    AlpnMismatch,
    /// The dial failed, or its task ended without a result.
    Failed,
}

/// What the supervisor's registrations showed for the attempt's peer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum PeerRegistration {
    /// The peer has not been registered during this attempt.
    Absent,
    /// The peer is registered now.
    Live,
    /// The peer was registered during this attempt, and no longer is.
    Ended,
}

/// What ended the dial worker's wait.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum DialObservation {
    /// The attempt's own dial ended.
    DialEnded(DialFinish, PeerRegistration),
    /// A registration lasted [`ZAKURA_REDIAL_HEALTHY_CONNECTION`](
    /// super::ZAKURA_REDIAL_HEALTHY_CONNECTION).
    RegistrationHealthy,
    /// A registration ended while the attempt's own dial still ran.
    RegistrationEnded,
    /// The registration watch closed.
    WatchClosed,
}

/// Classifies `observation` for one attempt, or returns `None` if the attempt
/// goes on waiting for its own dial.
///
/// A registration is keyed by peer, so it cannot say which connection it came
/// from. `registered` says whether this attempt's own dial reported that the
/// supervisor registered the peer on its connection. A registration without
/// that report belongs to another connection, so it earns this attempt no
/// success and no failure. A dial that authenticated and was then refused
/// before registration, as [`DialFinish::Refused`] says, earns no credit.
pub(super) fn classify_dial(
    observation: DialObservation,
    registered: bool,
) -> Option<DiscoveryDialResult> {
    use DiscoveryDialResult as Result;
    Some(match observation {
        // The registration watch can lag a dial that already finished
        // serving, in either direction.
        DialObservation::DialEnded(_, PeerRegistration::Live | PeerRegistration::Ended)
        | DialObservation::DialEnded(DialFinish::Served, PeerRegistration::Absent)
            if registered =>
        {
            Result::ShortLivedRegistered
        }
        DialObservation::DialEnded(_, PeerRegistration::Live) => Result::ConnectedElsewhere,
        DialObservation::DialEnded(finish, _) => match finish {
            DialFinish::LocalLimit => Result::LocalResourceLimit,
            DialFinish::AlpnMismatch => Result::AlpnMismatch,
            DialFinish::Served | DialFinish::Refused | DialFinish::Failed => Result::Failed,
        },
        DialObservation::RegistrationHealthy if registered => Result::Registered,
        DialObservation::RegistrationHealthy => Result::ConnectedElsewhere,
        DialObservation::RegistrationEnded if registered => Result::ShortLivedRegistered,
        DialObservation::RegistrationEnded => return None,
        DialObservation::WatchClosed => Result::Failed,
    })
}

/// A candidate address the dialer may reserve: the first address of its IP
/// in record order, with that IP canonicalised.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct CandidateAddress {
    /// The address to dial.
    pub(super) addr: SocketAddr,
    /// The address's canonical IP. Failure backoff counts the IP itself, and
    /// the reservation counts its admission host (see [`admission_host`]).
    pub(super) ip: IpAddr,
}

#[derive(Debug)]
struct InFlightDial {
    sequence: u64,
    /// The canonical IPs this attempt dialed when the dialer recorded it. It
    /// reserved each of their admission hosts once.
    reserved_ips: Vec<IpAddr>,
}

/// Exponential dial backoff for a single `(node_id, ip)` pair.
///
/// Backoff is keyed by `(NodeId, IpAddr)`, not `IpAddr` alone: a signature on a
/// gossip record proves control of the node key, not ownership of the addresses
/// it advertises. Keying failure backoff by IP alone would let an attacker sign
/// throwaway node ids that all advertise an honest peer's IP and, by failing
/// those dials, back that IP off for every legitimate candidate that shares it.
/// It keys the exact canonical IP, so a failing address doesn't back off the
/// other addresses of its IPv6 /64. Only the in-flight reservation cap
/// (`in_flight_by_ip`) keys the [`admission_host`], because it mirrors the
/// supervisor's per-host connection cap.
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
    /// The number of in-flight attempts that reserved each admission host.
    in_flight_by_ip: HashMap<IpAddr, usize>,
    backoff_by_node_ip: HashMap<(NodeId, IpAddr), DiscoveryIpBackoff>,
    /// The keys of `backoff_by_node_ip`, earliest `retry_at` first. Pruning
    /// and cap eviction take entries from its front.
    backoff_by_retry_at: BTreeSet<(Instant, NodeId, IpAddr)>,
    /// The earliest time the next round may start, or `None` before the
    /// first round. Each round starts one dial interval after the last, and
    /// a local limit pushes it to one interval after the limit: the limit
    /// doesn't back off the candidate, so an immediate retry would spin
    /// while the node stays full.
    next_round: Option<Instant>,
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
            backoff_by_retry_at: BTreeSet::new(),
            next_round: None,
            next_sequence: 0,
        }
    }

    /// Starts a dial round at `now`.
    ///
    /// Prunes backoff entries that expired more than the maximum backoff ago,
    /// then returns whether the round may dial. It may not before
    /// [`Self::next_round`]. A round that may dial sets the next round one
    /// dial interval after `now`.
    pub(super) fn begin_round(&mut self, now: Instant) -> bool {
        let retention = self.backoff_max;
        while let Some(&(retry_at, node_id, ip)) = self.backoff_by_retry_at.first() {
            if now.saturating_duration_since(retry_at) <= retention {
                break;
            }
            self.remove_backoff(node_id, ip);
        }

        if self.next_round.is_some_and(|next_round| now < next_round) {
            return false;
        }
        self.next_round = Some(now + ZAKURA_DISCOVERY_DIAL_INTERVAL);
        true
    }

    /// Returns the earliest time the next round may start, which is `now`
    /// before the first round.
    pub(super) fn next_round(&self, now: Instant) -> Instant {
        self.next_round.unwrap_or(now)
    }

    /// Returns the nodes that have an attempt in flight.
    pub(super) fn in_flight_node_ids(&self) -> Vec<NodeId> {
        self.in_flight.keys().copied().collect()
    }

    /// Returns the number of in-flight attempts that reserved `ip`'s
    /// admission host.
    pub(super) fn in_flight_dials_to(&self, ip: IpAddr) -> usize {
        self.in_flight_by_ip
            .get(&admission_host(ip))
            .copied()
            .unwrap_or_default()
    }

    /// Returns the addresses of `candidate` that the dialer may reserve at
    /// `now`, before the supervisor's per-IP check.
    ///
    /// Dialing only the first address of each IP, in the candidate's order,
    /// keeps a record from listing many ports on one IP to multiply
    /// handshakes. Distinct IPs in one IPv6 /64 are each dialed, so one
    /// unreachable address doesn't hide the rest, but the attempt reserves
    /// their /64 once, since it can register at most one connection. A record
    /// lists at most `MAX_DIRECT_ADDRS_PER_RECORD` addresses. The book orders a
    /// candidate's addresses dialed first, then confirmed, then the rest, each
    /// group in record order. An IP in backoff for this node yields no
    /// address.
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

    /// Records an attempt to `node_id` that dials `reserved_ips`, the
    /// canonical IPs the supervisor accepted, and reserves their admission
    /// hosts.
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

        self.reserve(&reserved_ips);
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

    /// Counts one more in-flight attempt on each admission host among `ips`.
    fn reserve(&mut self, ips: &[IpAddr]) {
        for host in admission_hosts(ips) {
            let count = self.in_flight_by_ip.entry(host).or_default();
            *count = count
                .checked_add(1)
                .expect("attempts in flight fit usize: each node has at most one");
        }
    }

    /// Settles the attempt `key` with its worker's `result` at `now`.
    ///
    /// Releases the IPs the attempt reserved when the dialer recorded it, even
    /// if the node's addresses changed since. Applies the result's backoff to
    /// those IPs; `admitted_ip` is the IP whose handshake TLS authenticated, if
    /// the dial got that far. A local limit delays the next round to one dial
    /// interval after `now` without backing off the candidate.
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
            let after_limit = now + ZAKURA_DISCOVERY_DIAL_INTERVAL;
            self.next_round = Some(
                self.next_round
                    .map_or(after_limit, |at| at.max(after_limit)),
            );
        }
        for host in admission_hosts(&attempt.reserved_ips) {
            let count = self
                .in_flight_by_ip
                .get_mut(&host)
                .expect("an in-flight attempt keeps a count on each host it reserved");
            *count = count
                .checked_sub(1)
                .expect("a kept count includes this attempt");
            if *count == 0 {
                self.in_flight_by_ip.remove(&host);
            }
        }
        self.apply_backoff(key.node_id, &attempt.reserved_ips, admitted_ip, result, now);
        true
    }

    /// Checks the state's invariants: each admission host's count is the
    /// number of in-flight attempts that reserved it, no attempt holds a
    /// sequence the dialer will hand out again, and each backoff entry
    /// counts at least one failure.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn assert_invariants(&self) {
        let mut reserved: HashMap<IpAddr, usize> = HashMap::default();
        for attempt in self.in_flight.values() {
            for host in admission_hosts(&attempt.reserved_ips) {
                *reserved.entry(host).or_default() += 1;
            }
            assert!(
                attempt.sequence < self.next_sequence,
                "discovery/dial-attempts: attempt {} at or past the next sequence {}",
                attempt.sequence,
                self.next_sequence
            );
        }
        // Not `assert_eq!`: a map's `Debug` order varies between runs, and the
        // simulator's replay compares failure messages exactly.
        assert!(
            self.in_flight_by_ip == reserved,
            "discovery/dial-attempts: reservations disagree with the attempts in flight"
        );
        assert!(
            self.backoff_by_node_ip
                .values()
                .all(|backoff| backoff.failure_count > 0),
            "discovery/dial-attempts: a backoff entry counts no failure"
        );
        let indexed: BTreeSet<_> = self
            .backoff_by_node_ip
            .iter()
            .map(|(&(node_id, ip), backoff)| (backoff.retry_at, node_id, ip))
            .collect();
        assert!(
            self.backoff_by_retry_at == indexed,
            "discovery/dial-attempts: the retry index disagrees with the backoff entries"
        );
        assert!(
            self.backoff_by_node_ip.len() <= MAX_DIAL_BACKOFF_ENTRIES,
            "discovery/dial-attempts: {} backoff entries above the cap",
            self.backoff_by_node_ip.len()
        );
    }

    /// The number of `(node, IP)` backoff entries.
    #[cfg(test)]
    pub(super) fn backoff_len(&self) -> usize {
        self.backoff_by_node_ip.len()
    }

    /// Removes the backoff entry for `node_id` at `ip`, if any.
    fn remove_backoff(&mut self, node_id: NodeId, ip: IpAddr) {
        if let Some(backoff) = self.backoff_by_node_ip.remove(&(node_id, ip)) {
            self.backoff_by_retry_at
                .remove(&(backoff.retry_at, node_id, ip));
        }
    }

    /// Stores `backoff` for `node_id` at `ip`. A new entry past
    /// [`MAX_DIAL_BACKOFF_ENTRIES`] first evicts the entry with the earliest
    /// `retry_at`.
    fn insert_backoff(&mut self, node_id: NodeId, ip: IpAddr, backoff: DiscoveryIpBackoff) {
        self.remove_backoff(node_id, ip);
        if self.backoff_by_node_ip.len() >= MAX_DIAL_BACKOFF_ENTRIES {
            if let Some(&(_, evicted_node, evicted_ip)) = self.backoff_by_retry_at.first() {
                self.remove_backoff(evicted_node, evicted_ip);
            }
        }
        self.backoff_by_retry_at
            .insert((backoff.retry_at, node_id, ip));
        self.backoff_by_node_ip.insert((node_id, ip), backoff);
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
                    self.remove_backoff(node_id, canonical_ip(admitted_ip));
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
                    self.insert_backoff(
                        node_id,
                        *ip,
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

/// The distinct admission hosts of `ips`, in first-seen order. An attempt
/// reserves each host once however many of its addresses it dials.
fn admission_hosts(ips: &[IpAddr]) -> Vec<IpAddr> {
    let mut hosts = Vec::with_capacity(ips.len());
    for ip in ips {
        let host = admission_host(*ip);
        if !hosts.contains(&host) {
            hosts.push(host);
        }
    }
    hosts
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

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;
    use crate::zakura::{
        discovery::protocol::DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX, DEFAULT_ZAKURA_MAX_CONNECTIONS,
        DEFAULT_ZAKURA_MAX_CONNS_PER_IP,
    };

    /// The memory the dial history may hold.
    const DIAL_HISTORY_BUDGET_BYTES: usize = 16 << 20;

    /// disco-23 `dial-history-bounded`.
    ///
    /// Only this node's failed dials add entries. A round dials at most the
    /// connection table's worth of addresses, since discovery's dial ceiling
    /// sits below it, and rounds start at least one dial interval apart. An
    /// entry's `retry_at` is at most the maximum backoff out, and
    /// `begin_round` prunes it the maximum backoff after that. So the
    /// uncapped history holds at most one table of addresses per round over
    /// two maximum backoffs and one interval. That figure is far above the
    /// cap, so the cap sets the history's size, and the cap's map and retry
    /// index fit the budget.
    #[test]
    fn dial_history_stays_under_its_memory_budget() {
        let interval = ZAKURA_DISCOVERY_DIAL_INTERVAL.as_millis();
        let lifetime = 2 * DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX.as_millis() + interval;
        // Each round in the lifetime, and the attempts in flight when it
        // starts, add at most one table of addresses.
        let rounds = usize::try_from(lifetime / interval + 1).expect("the round count fits usize");
        let uncapped = rounds * DEFAULT_ZAKURA_MAX_CONNECTIONS;

        // hashbrown keeps a power-of-two bucket count at most 7/8 full, and
        // one control byte per bucket.
        let map_slot = size_of::<((NodeId, IpAddr), DiscoveryIpBackoff)>() + 1;
        let map_bytes = |entries: usize| (entries * 8 / 7).next_power_of_two() * map_slot;
        // std's B-tree nodes hold 5 to 11 keys, except the root, and an
        // internal node also holds 12 child pointers. Internal nodes have at
        // least 6 children, so they number at most a fifth of the leaves.
        let key = size_of::<(Instant, NodeId, IpAddr)>();
        let node = 16 + 11 * key + 12 * size_of::<usize>();
        let index_bytes = |entries: usize| {
            let leaves = entries.div_ceil(5);
            (leaves + leaves.div_ceil(5)) * node
        };
        let history_bytes = |entries: usize| map_bytes(entries) + index_bytes(entries);

        assert!(
            history_bytes(uncapped) > DIAL_HISTORY_BUDGET_BYTES,
            "the uncapped history fits the budget, so the cap is unneeded"
        );
        let entries = uncapped.min(MAX_DIAL_BACKOFF_ENTRIES);
        assert!(
            history_bytes(entries) <= DIAL_HISTORY_BUDGET_BYTES,
            "{entries} entries take {} bytes, above the {DIAL_HISTORY_BUDGET_BYTES}-byte budget",
            history_bytes(entries)
        );
    }

    /// disco-25 `dials-per-destination`.
    ///
    /// A round reserves a dial to a destination's admission host only while
    /// that host's connections and dials in flight stay below the per-IP
    /// cap, and rounds start at least one dial interval apart. So one
    /// destination receives at most the per-IP cap of new dials per
    /// interval: 16 per second. That is at most the node's whole discovery
    /// dial rate, one connection table of addresses per interval.
    #[test]
    fn one_destination_receives_at_most_the_node_dial_rate() {
        /// The row's figure: discovery dials per second to one destination.
        const DIALS_PER_SECOND: u128 = 16;
        let interval_ms = ZAKURA_DISCOVERY_DIAL_INTERVAL.as_millis();
        // Widening usize to u128 is lossless.
        let per_destination = DEFAULT_ZAKURA_MAX_CONNS_PER_IP as u128 * 1_000 / interval_ms;
        let node_wide = DEFAULT_ZAKURA_MAX_CONNECTIONS as u128 * 1_000 / interval_ms;
        assert!(per_destination <= DIALS_PER_SECOND);
        assert!(per_destination <= node_wide);
    }
}
