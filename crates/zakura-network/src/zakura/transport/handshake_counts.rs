//! Pure admission decisions and charges shared by QUIC and control handshakes.

use std::{net::IpAddr, time::Duration};
use zakura_rt::{collections::HashMap, time::Instant};

use crate::zakura::regulation::Bucket;
use crate::zakura::{admission_host, Cadence, DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF};

/// The result of charging one address for a TLS attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeCadenceAdmission {
    /// The attempt took a token.
    Admit,
    /// The address must wait for its bucket to refill.
    Exhausted,
    /// The bounded address ledger has no entry for this address.
    Capacity,
    /// The node's bucket for the source's validation state is empty.
    NodeExhausted,
}

/// TLS attempts the node starts per second, per source validation state.
const NODE_HANDSHAKES_PER_SECOND: u32 = 24;

/// The node tokens that only a host within its share may take (F-800).
///
/// Above it, any host takes tokens, so one host's default burst of 16
/// (`DEFAULT_ZAKURA_MAX_CONNS_PER_IP`) fits in a full bucket. In it, each
/// host draws on its [`host_share_cadence`], an eighth of the node's rate.
/// A few busy hosts then keep the bucket near the reserve, and the reserve
/// stays open to every other host while fewer than eight hosts spend their
/// whole share.
const NODE_HANDSHAKE_RESERVE: u32 = 8;

/// The hosts whose shares add up to the node's rate.
const NODE_HANDSHAKE_SHARES: u32 = 8;

/// Bounds the TLS attempts the whole node starts, per source validation
/// state, whatever their addresses. A full bucket admits the default inbound
/// handshake share, 24, back to back.
pub fn node_handshake_cadence() -> Cadence {
    let interval = Duration::from_secs(1) / NODE_HANDSHAKES_PER_SECOND;
    Cadence {
        capacity: NODE_HANDSHAKES_PER_SECOND,
        refill_interval: interval,
        send_interval: interval,
    }
}

/// One host's share of the node's reserve: one token, refilling at an
/// eighth of the node's rate. One token, not a burst, so busy hosts can't
/// save up shares and empty the reserve together.
fn host_share_cadence() -> Cadence {
    let interval = node_handshake_cadence().refill_interval * NODE_HANDSHAKE_SHARES;
    Cadence {
        capacity: 1,
        refill_interval: interval,
        send_interval: interval,
    }
}

/// Lets every permitted node behind one address retry at the dialer's minimum interval.
pub fn handshake_cadence(max_connections_per_ip: usize) -> Cadence {
    let capacity = u32::try_from(max_connections_per_ip.max(1)).unwrap_or(u32::MAX);
    Cadence {
        capacity,
        refill_interval: (DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF / capacity)
            .max(Duration::from_nanos(1)),
        send_interval: DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
    }
}

/// Whether an address may start another handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IpAdmission {
    /// Admission has room for this attempt.
    Open,
    /// The last slot requires proof of the source address.
    ValidationRequired,
    /// The address has exhausted its slots.
    Full,
}

/// Counts connections by admission host without locks, tasks, or I/O.
/// The handler holds this machine under its existing synchronous mutex.
#[derive(Debug, Default)]
pub struct HandshakeCounts {
    registered: HashMap<IpAddr, usize>,
    handshaking: HashMap<IpAddr, usize>,
    pending: usize,
    inbound_pending: usize,
    inbound_reservations: usize,
    connections: usize,
    inbound_connections: usize,
    /// Charges of attempts whose source address was validated, by a Retry
    /// token, before TLS.
    validated_cadence: CadenceLedger,
    /// Charges of attempts from unvalidated source addresses. A spoofed
    /// Initial can only charge this ledger, so it never takes a validated
    /// source's tokens or ledger room.
    unvalidated_cadence: CadenceLedger,
}

/// The handshake charges of one source validation state.
#[derive(Debug, Default)]
struct CadenceLedger {
    /// The buckets of each admission host that still owes tokens.
    hosts: HashMap<IpAddr, HostBuckets>,
    /// The node's bucket, created full at the first charge.
    node: Option<Bucket>,
}

/// One admission host's handshake charges.
#[derive(Debug)]
struct HostBuckets {
    /// The host's own cadence, [`handshake_cadence`].
    cadence: Bucket,
    /// The host's share of the node's reserve, [`host_share_cadence`].
    share: Bucket,
}

impl HostBuckets {
    fn new(max_connections_per_ip: usize, now: Instant) -> Self {
        Self {
            cadence: Bucket::new(handshake_cadence(max_connections_per_ip), now),
            share: Bucket::new(host_share_cadence(), now),
        }
    }

    fn refill(&mut self, now: Instant) {
        self.cadence.refill_capped(now);
        self.share.refill_capped(now);
    }

    fn is_full(&self) -> bool {
        self.cadence.is_full() && self.share.is_full()
    }
}

impl CadenceLedger {
    /// The buckets of `host`, refilled at `now`. A new host enters only
    /// while the ledger, after it drops full buckets, holds fewer than
    /// `max_hosts`.
    fn host(
        &mut self,
        host: IpAddr,
        max_connections_per_ip: usize,
        max_hosts: usize,
        now: Instant,
    ) -> Option<&mut HostBuckets> {
        if !self.hosts.contains_key(&host) && self.hosts.len() >= max_hosts {
            self.hosts.retain(|_, buckets| {
                buckets.refill(now);
                !buckets.is_full()
            });
            if self.hosts.len() >= max_hosts {
                return None;
            }
        }
        let buckets = self
            .hosts
            .entry(host)
            .or_insert_with(|| HostBuckets::new(max_connections_per_ip, now));
        buckets.refill(now);
        Some(buckets)
    }
}

impl HandshakeCounts {
    /// Charges an attempt to the ledger of its source's validation state,
    /// `validated`: one token from the node's bucket and one from the
    /// source's admission host. While the node's bucket is in its reserve,
    /// the host also spends a token of its share. A refused attempt keeps
    /// no token.
    ///
    /// The ledger retains every depleted address bucket. Full buckets can be
    /// discarded because a new bucket starts full. Each ledger holds at most
    /// `max_hosts` addresses.
    pub fn admit_cadence(
        &mut self,
        ip: IpAddr,
        validated: bool,
        max_connections_per_ip: usize,
        max_hosts: usize,
        now: Instant,
    ) -> HandshakeCadenceAdmission {
        let ledger = self.cadence_mut(validated);
        let node = ledger
            .node
            .get_or_insert_with(|| Bucket::new(node_handshake_cadence(), now));
        node.refill_capped(now);
        let in_reserve = node.tokens() <= NODE_HANDSHAKE_RESERVE;
        if node.tokens() == 0 {
            return HandshakeCadenceAdmission::NodeExhausted;
        }
        let Some(host) = ledger.host(admission_host(ip), max_connections_per_ip, max_hosts, now)
        else {
            return HandshakeCadenceAdmission::Capacity;
        };
        if host.cadence.tokens() == 0 {
            return HandshakeCadenceAdmission::Exhausted;
        }
        if in_reserve && !host.share.take() {
            return HandshakeCadenceAdmission::NodeExhausted;
        }
        let host_took = host.cadence.take();
        let node_took = ledger.node.as_mut().is_some_and(Bucket::take);
        debug_assert!(host_took && node_took, "both buckets held a token");
        HandshakeCadenceAdmission::Admit
    }

    /// Refunds an attempt QUIC cancelled before TLS to the ledger
    /// [`Self::admit_cadence`] charged for `validated`.
    pub fn refund_cadence(&mut self, ip: IpAddr, validated: bool, now: Instant) {
        let ledger = self.cadence_mut(validated);
        if let Some(node) = ledger.node.as_mut() {
            node.refill_capped(now);
            node.refund();
        }
        if let Some(host) = ledger.hosts.get_mut(&admission_host(ip)) {
            host.refill(now);
            host.cadence.refund();
            host.share.refund();
        }
    }

    /// Counts the bounded address ledger for `validated` sources, including
    /// depleted idle buckets.
    pub fn cadence_hosts(&self, validated: bool) -> usize {
        self.cadence(validated).hosts.len()
    }

    fn cadence(&self, validated: bool) -> &CadenceLedger {
        if validated {
            &self.validated_cadence
        } else {
            &self.unvalidated_cadence
        }
    }

    fn cadence_mut(&mut self, validated: bool) -> &mut CadenceLedger {
        if validated {
            &mut self.validated_cadence
        } else {
            &mut self.unvalidated_cadence
        }
    }

    /// Reads both address ledgers for a simulator boundary sample.
    #[cfg(zakura_sim)]
    pub(crate) fn hosts(&self) -> Vec<(IpAddr, usize, usize)> {
        let mut hosts: Vec<_> = self
            .registered
            .keys()
            .chain(self.handshaking.keys())
            .copied()
            .collect();
        hosts.sort_unstable();
        hosts.dedup();
        hosts
            .into_iter()
            .map(|ip| (ip, self.registered(ip), self.handshaking(ip)))
            .collect()
    }
    /// Counts connection slots from admission until the serving task exits.
    pub fn connections(&self) -> usize {
        self.connections
    }

    /// Counts the inbound share of connection slots.
    pub fn inbound_connections(&self) -> usize {
        self.inbound_connections
    }

    /// Checks whether the connection budget can admit this direction.
    pub fn connections_full(&self, inbound: bool, cap: usize) -> bool {
        self.connections >= cap || (inbound && self.inbound_connections >= inbound_share(cap))
    }

    /// Charges a connection slot while preserving the dial reserve.
    pub fn claim_connection(&mut self, inbound: bool, cap: usize) -> bool {
        if self.connections_full(inbound, cap) {
            return false;
        }
        self.connections += 1;
        if inbound {
            self.inbound_connections += 1;
        }
        true
    }

    /// Releases the slot that `claim_connection` charged.
    pub fn release_connection(&mut self, inbound: bool) {
        self.connections = self
            .connections
            .checked_sub(1)
            .expect("a live connection owns its charge");
        if inbound {
            self.inbound_connections = self
                .inbound_connections
                .checked_sub(1)
                .expect("an inbound connection owns its inbound charge");
        }
    }

    /// Counts control-handshake permits, including this node's dials.
    pub fn pending(&self) -> usize {
        self.pending
    }

    /// Counts the inbound share of control-handshake permits.
    pub fn inbound_pending(&self) -> usize {
        self.inbound_pending
    }

    /// Checks QUIC and control work against the total and inbound budgets.
    pub fn pending_full(&self, transport_pending: usize, cap: usize) -> bool {
        transport_pending.saturating_add(self.pending) >= cap
            || transport_pending.saturating_add(self.inbound_pending) >= inbound_share(cap)
    }

    /// Counts transport owners that have not transferred into control handshakes.
    pub fn transport_pending(&self) -> usize {
        self.inbound_reservations
            .checked_sub(self.inbound_pending)
            .expect("each transferred attempt keeps its reservation until it releases both")
    }

    /// Reserves the shared pending budget before an inbound TLS attempt starts.
    pub fn claim_transport(&mut self, cap: usize) -> bool {
        if self.pending_full(self.transport_pending(), cap) {
            return false;
        }
        self.inbound_reservations += 1;
        true
    }

    /// Releases a transport attempt that did not transfer into control accounting.
    pub fn release_transport(&mut self) {
        assert!(
            self.transport_pending() > 0,
            "an admitted transport owns its reservation"
        );
        self.inbound_reservations -= 1;
    }

    /// Transfers an existing reservation without reacquiring the shared budget.
    pub fn claim_transferred_pending(&mut self) {
        assert!(
            self.transport_pending() > 0,
            "a handoff owns a transport reservation"
        );
        self.pending += 1;
        self.inbound_pending += 1;
    }

    /// Claims a control-handshake permit within the appropriate share.
    pub fn claim_pending(&mut self, inbound: bool, cap: usize) -> bool {
        if inbound {
            if !self.claim_transport(cap) {
                return false;
            }
            self.claim_transferred_pending();
        } else {
            if self.transport_pending().saturating_add(self.pending) >= cap {
                return false;
            }
            self.pending += 1;
        }
        true
    }

    /// Releases the permit that `claim_pending` charged.
    pub fn release_pending(&mut self, inbound: bool) {
        self.pending = self
            .pending
            .checked_sub(1)
            .expect("a live handshake owns its charge");
        if inbound {
            self.inbound_pending = self
                .inbound_pending
                .checked_sub(1)
                .expect("an inbound handshake owns its inbound charge");
            self.inbound_reservations -= 1;
        }
    }

    /// Counts registered connections from the address group containing `ip`.
    pub fn registered(&self, ip: IpAddr) -> usize {
        self.registered
            .get(&admission_host(ip))
            .copied()
            .unwrap_or_default()
    }

    /// Counts inbound control handshakes from the address group containing `ip`.
    pub fn handshaking(&self, ip: IpAddr) -> usize {
        self.handshaking
            .get(&admission_host(ip))
            .copied()
            .unwrap_or_default()
    }

    /// Counts all registered connections.
    pub fn registered_total(&self) -> usize {
        self.registered.values().sum()
    }

    /// Charges a registered connection after duplicate resolution.
    pub fn add_registered(&mut self, ip: IpAddr) {
        *self.registered.entry(admission_host(ip)).or_default() += 1;
    }

    /// Releases a registered connection when its owner exits.
    pub fn remove_registered(&mut self, ip: IpAddr) {
        decrement(&mut self.registered, admission_host(ip));
    }

    /// Charges a connection transferred from QUIC to the control handshake.
    pub fn enter_handshake(&mut self, ip: IpAddr) {
        *self.handshaking.entry(admission_host(ip)).or_default() += 1;
    }

    /// Releases a control handshake at registration or teardown.
    pub fn leave_handshake(&mut self, ip: IpAddr) {
        decrement(&mut self.handshaking, admission_host(ip));
    }

    /// Decides admission using the endpoint's pending QUIC count for this host.
    pub fn ip_admission(
        &self,
        remote_ip: IpAddr,
        pending: usize,
        max_handshakes_per_ip: usize,
        max_connections_per_ip: usize,
        validated: bool,
    ) -> IpAdmission {
        let ip = admission_host(remote_ip);
        let registered = self.registered(ip);
        let in_flight = self.handshaking(ip).saturating_add(pending);
        if in_flight >= max_handshakes_per_ip {
            return IpAdmission::Full;
        }
        if registered.saturating_add(in_flight) < max_connections_per_ip {
            let last_slot = in_flight.saturating_add(1) >= max_handshakes_per_ip
                || registered.saturating_add(in_flight).saturating_add(1) >= max_connections_per_ip;
            return if last_slot && !validated {
                IpAdmission::ValidationRequired
            } else {
                IpAdmission::Open
            };
        }
        // A full IP still gets one handshake at a time: it may be a node
        // redialing to replace its own stale connection, which `register`
        // allows from the same IP. `register` enforces the exact cap for
        // everyone else, so this costs at most one handshake per full IP.
        if registered >= max_connections_per_ip && in_flight == 0 {
            return if validated {
                IpAdmission::Open
            } else {
                IpAdmission::ValidationRequired
            };
        }
        IpAdmission::Full
    }

    /// Allows a same-host redial to replace its incumbent at the address cap.
    pub fn can_register(&self, ip: IpAddr, cap: usize, same_host_redial: bool) -> bool {
        same_host_redial || self.registered(ip) < cap
    }
}

fn decrement(counts: &mut HashMap<IpAddr, usize>, ip: IpAddr) {
    if let Some(count) = counts.get_mut(&ip) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(&ip);
        }
    }
}

/// Leaves a quarter of a cap for dials, with at least one dial slot when possible.
pub fn inbound_share(cap: usize) -> usize {
    let reserve = if cap >= 2 { (cap / 4).max(1) } else { 0 };
    cap - reserve
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_and_control_share_one_pending_budget() {
        let mut counts = HandshakeCounts::default();
        for _ in 0..24 {
            assert!(counts.claim_transport(32));
        }
        assert!(!counts.claim_transport(32));
        for _ in 0..8 {
            assert!(counts.claim_pending(false, 32));
        }
        assert!(!counts.claim_pending(false, 32));
        // A handoff replaces its existing reservation even at capacity.
        counts.claim_transferred_pending();
        assert!(!counts.claim_pending(false, 32));
        counts.release_pending(true);
        assert!(counts.claim_transport(32));
        // A failed transport attempt frees exactly one place for a dial.
        counts.release_transport();
        assert!(counts.claim_pending(false, 32));
        assert!(!counts.claim_pending(false, 32));
    }

    #[test]
    fn handshake_cadence_charges_refills_and_refunds_one_address() {
        let mut counts = HandshakeCounts::default();
        let now = Instant::now();
        let ip = "192.0.2.1".parse().unwrap();
        let alias = "::ffff:192.0.2.1".parse().unwrap();
        for _ in 0..16 {
            assert_eq!(
                counts.admit_cadence(ip, true, 16, 124, now),
                HandshakeCadenceAdmission::Admit
            );
        }
        assert_eq!(
            counts.admit_cadence(alias, true, 16, 124, now),
            HandshakeCadenceAdmission::Exhausted
        );
        let now = now + Duration::from_micros(62_500);
        assert_eq!(
            counts.admit_cadence(alias, true, 16, 124, now),
            HandshakeCadenceAdmission::Admit
        );
        assert_eq!(
            counts.admit_cadence(ip, true, 16, 124, now),
            HandshakeCadenceAdmission::Exhausted
        );
        counts.refund_cadence(ip, true, now);
        assert_eq!(
            counts.admit_cadence(alias, true, 16, 124, now),
            HandshakeCadenceAdmission::Admit
        );
        assert_eq!(
            counts.admit_cadence(ip, true, 16, 124, now),
            HandshakeCadenceAdmission::Exhausted
        );
        assert_eq!(counts.cadence_hosts(true), 1);
    }

    /// A spoofed Initial carries an unvalidated source, so it charges only
    /// the unvalidated ledger: draining that ledger, or filling it with
    /// other addresses, leaves a validated source from the same address
    /// its own tokens and ledger room.
    #[test]
    fn unvalidated_charges_leave_validated_sources_their_cadence() {
        let mut counts = HandshakeCounts::default();
        let now = Instant::now();
        let victim = "192.0.2.1".parse().unwrap();
        let spoofed = "192.0.2.2".parse().unwrap();
        for _ in 0..16 {
            assert_eq!(
                counts.admit_cadence(victim, false, 16, 1, now),
                HandshakeCadenceAdmission::Admit
            );
        }
        assert_eq!(
            counts.admit_cadence(victim, false, 16, 1, now),
            HandshakeCadenceAdmission::Exhausted
        );
        assert_eq!(
            counts.admit_cadence(spoofed, false, 16, 1, now),
            HandshakeCadenceAdmission::Capacity
        );
        assert_eq!(
            counts.admit_cadence(victim, true, 16, 1, now),
            HandshakeCadenceAdmission::Admit,
            "the validated ledger is untouched"
        );
        assert_eq!(
            (counts.cadence_hosts(false), counts.cadence_hosts(true)),
            (1, 1)
        );
        // A refund returns to the ledger the attempt was charged to.
        counts.refund_cadence(victim, true, now);
        assert_eq!(
            counts.admit_cadence(victim, false, 16, 1, now),
            HandshakeCadenceAdmission::Exhausted
        );
        counts.refund_cadence(victim, false, now);
        assert_eq!(
            counts.admit_cadence(victim, false, 16, 1, now),
            HandshakeCadenceAdmission::Admit
        );
    }

    #[test]
    fn handshake_cadence_retains_debt_when_its_address_ledger_is_full() {
        let mut counts = HandshakeCounts::default();
        let now = Instant::now();
        let first = "2001:db8:1::1".parse().unwrap();
        let alias = "2001:db8:1::2".parse().unwrap();
        let other = "2001:db8:2::1".parse().unwrap();
        for _ in 0..16 {
            assert_eq!(
                counts.admit_cadence(first, true, 16, 1, now),
                HandshakeCadenceAdmission::Admit
            );
        }
        assert_eq!(
            counts.admit_cadence(other, true, 16, 1, now),
            HandshakeCadenceAdmission::Capacity
        );
        assert_eq!(
            counts.admit_cadence(alias, true, 16, 1, now),
            HandshakeCadenceAdmission::Exhausted
        );
        assert_eq!(counts.cadence_hosts(true), 1);
        assert_eq!(
            counts.admit_cadence(other, true, 16, 1, now + Duration::from_secs(1)),
            HandshakeCadenceAdmission::Admit
        );
        assert_eq!(counts.cadence_hosts(true), 1);
    }

    /// TLS entries are admitted attempts less the ones QUIC cancelled before
    /// TLS. Each bound is a full bucket plus one token per whole refill
    /// interval since the first charge (conn-7).
    fn assert_within_cadence(entries: u64, cadence: Cadence, elapsed: Duration, what: &str) {
        let refills = elapsed.as_nanos() / cadence.refill_interval.as_nanos();
        let bound = u128::from(cadence.capacity) + refills;
        assert!(
            u128::from(entries) <= bound,
            "{what}: {entries} TLS entries exceed {bound} after {elapsed:?}"
        );
    }

    /// Forty addresses each attempt at once, one nanosecond before, at and
    /// after every node refill boundary. One attempt in five is cancelled
    /// at the next instant, after the boundary it straddles. The node and
    /// every address stay within their cadence, and the node admits its
    /// whole budget, so the bound is exact.
    #[test]
    fn node_cadence_bounds_tls_entries_from_every_address() {
        const HOSTS: u32 = 40;
        let node = node_handshake_cadence();
        let host = handshake_cadence(16);
        let interval = node.refill_interval;
        let start = Instant::now();
        let mut counts = HandshakeCounts::default();
        let mut entries = 0u64;
        let mut host_entries: HashMap<IpAddr, u64> = HashMap::default();
        let mut cancel_next: Vec<IpAddr> = Vec::new();
        let mut attempts = 0u64;
        for validated in [false, true] {
            let mut last = start;
            for boundary in 1..=240u32 {
                let at = start + interval * boundary;
                for now in [
                    at - Duration::from_nanos(1),
                    at,
                    at + Duration::from_nanos(1),
                ] {
                    for ip in cancel_next.drain(..) {
                        counts.refund_cadence(ip, validated, now);
                        entries -= 1;
                        *host_entries
                            .get_mut(&ip)
                            .expect("a cancelled host was admitted") -= 1;
                    }
                    for index in 0..HOSTS {
                        let ip = IpAddr::from([198, 51, 100, u8::try_from(index).unwrap()]);
                        attempts += 1;
                        if counts.admit_cadence(ip, validated, 16, 124, now)
                            == HandshakeCadenceAdmission::Admit
                        {
                            entries += 1;
                            *host_entries.entry(ip).or_default() += 1;
                            if attempts.is_multiple_of(5) {
                                cancel_next.push(ip);
                            }
                        }
                        let elapsed = now.saturating_duration_since(start);
                        assert_within_cadence(entries, node, elapsed, "node");
                        let address = host_entries.get(&ip).copied().unwrap_or_default();
                        assert_within_cadence(address, host, elapsed, "address");
                    }
                    last = now;
                }
            }
            // Every instant had waiting attempts, so the node spent its whole
            // budget, counted from its first charge one nanosecond before the
            // first boundary.
            let first = start + interval - Duration::from_nanos(1);
            let refills = last.saturating_duration_since(first).as_nanos() / interval.as_nanos();
            assert_eq!(
                u128::from(entries),
                u128::from(node.capacity) + refills,
                "the node admits its whole budget"
            );
            entries = 0;
            host_entries.clear();
            cancel_next.clear();
        }
    }

    /// One address retries every millisecond for ten seconds. It stays
    /// within its own cadence, and a fresh address's first attempt is
    /// admitted at any point, because one address's rate is below the
    /// node's (conn-7). The busy address's burst can reach the node's
    /// reserve, where its share refuses it.
    #[test]
    fn a_fresh_address_admits_while_another_is_saturated() {
        let host = handshake_cadence(16);
        assert!(host.refill_interval > node_handshake_cadence().refill_interval);
        for validated in [false, true] {
            let mut counts = HandshakeCounts::default();
            let start = Instant::now();
            let attacker = "203.0.113.1".parse().unwrap();
            let mut entries = 0;
            let mut exhausted = 0;
            for ms in 0..10_000u32 {
                let now = start + Duration::from_millis(u64::from(ms));
                match counts.admit_cadence(attacker, validated, 16, 124, now) {
                    HandshakeCadenceAdmission::Admit => entries += 1,
                    HandshakeCadenceAdmission::Exhausted
                    | HandshakeCadenceAdmission::NodeExhausted => exhausted += 1,
                    HandshakeCadenceAdmission::Capacity => {
                        panic!("one address never fills the ledger")
                    }
                }
                assert_within_cadence(entries, host, now - start, "attacker");
                if ms % 997 == 0 {
                    let fresh = IpAddr::from([192, 0, 2, u8::try_from(ms / 997).unwrap()]);
                    assert_eq!(
                        counts.admit_cadence(fresh, validated, 16, 124, now),
                        HandshakeCadenceAdmission::Admit,
                        "a fresh address at {ms} ms"
                    );
                }
            }
            assert!(exhausted > 9_000, "the attacker is saturated");
        }
    }

    /// F-800: seven addresses each retry every millisecond, far above
    /// their own cadence and together above the node's rate. They keep the
    /// node's bucket near its reserve, but each spends at most its share of
    /// the reserve, so a fresh address and a returning one that retries once
    /// a second are always admitted.
    #[test]
    fn seven_busy_addresses_cannot_refuse_another() {
        let busy: Vec<IpAddr> = (1..=7u8)
            .map(|index| IpAddr::from([203, 0, 113, index]))
            .collect();
        let returning = "192.0.2.200".parse().unwrap();
        for validated in [false, true] {
            let mut counts = HandshakeCounts::default();
            let start = Instant::now();
            let mut node_refusals = 0;
            for ms in 0..10_000u32 {
                let now = start + Duration::from_millis(u64::from(ms));
                for ip in &busy {
                    if counts.admit_cadence(*ip, validated, 16, 124, now)
                        == HandshakeCadenceAdmission::NodeExhausted
                    {
                        node_refusals += 1;
                    }
                }
                if ms % 997 == 0 {
                    let fresh = IpAddr::from([192, 0, 2, u8::try_from(ms / 997).unwrap()]);
                    assert_eq!(
                        counts.admit_cadence(fresh, validated, 16, 124, now),
                        HandshakeCadenceAdmission::Admit,
                        "a fresh address at {ms} ms"
                    );
                }
                if ms % 1_000 == 500 {
                    assert_eq!(
                        counts.admit_cadence(returning, validated, 16, 124, now),
                        HandshakeCadenceAdmission::Admit,
                        "a returning address at {ms} ms"
                    );
                }
            }
            assert!(node_refusals > 0, "the busy addresses reach the reserve");
        }
    }

    /// An empty node bucket refuses before the address is charged, and an
    /// address refusal returns the node's token, so neither ledger pays for
    /// an attempt that never starts TLS.
    #[test]
    fn a_refused_attempt_keeps_no_token() {
        let node = node_handshake_cadence();
        let mut counts = HandshakeCounts::default();
        let now = Instant::now();
        let saturated = "203.0.113.1".parse().unwrap();
        for _ in 0..16 {
            assert_eq!(
                counts.admit_cadence(saturated, true, 16, 124, now),
                HandshakeCadenceAdmission::Admit
            );
        }
        for _ in 0..100 {
            assert_eq!(
                counts.admit_cadence(saturated, true, 16, 124, now),
                HandshakeCadenceAdmission::Exhausted
            );
        }
        let spare = node.capacity - 16;
        for index in 0..spare {
            let ip = IpAddr::from([192, 0, 2, u8::try_from(index).unwrap()]);
            assert_eq!(
                counts.admit_cadence(ip, true, 16, 124, now),
                HandshakeCadenceAdmission::Admit
            );
        }
        let late = "192.0.2.200".parse().unwrap();
        assert_eq!(
            counts.admit_cadence(late, true, 16, 124, now),
            HandshakeCadenceAdmission::NodeExhausted
        );
        assert_eq!(
            counts.cadence_hosts(true),
            1 + usize::try_from(spare).unwrap(),
            "a node refusal adds no address bucket"
        );
        // The other validation state has its own node bucket.
        assert_eq!(
            counts.admit_cadence(late, false, 16, 124, now),
            HandshakeCadenceAdmission::Admit
        );
    }
}
