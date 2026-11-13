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
    cadence: HashMap<IpAddr, Bucket>,
}

impl HandshakeCounts {
    /// Charges an attempt while retaining every depleted address bucket.
    /// Full buckets can be discarded because a new bucket starts full.
    pub fn admit_cadence(
        &mut self,
        ip: IpAddr,
        max_connections_per_ip: usize,
        max_hosts: usize,
        now: Instant,
    ) -> HandshakeCadenceAdmission {
        let host = admission_host(ip);
        if !self.cadence.contains_key(&host) && self.cadence.len() >= max_hosts {
            self.cadence.retain(|_, bucket| {
                bucket.refill(now);
                !bucket.is_full()
            });
            if self.cadence.len() >= max_hosts {
                return HandshakeCadenceAdmission::Capacity;
            }
        }
        let bucket = self
            .cadence
            .entry(host)
            .or_insert_with(|| Bucket::new(handshake_cadence(max_connections_per_ip), now));
        bucket.refill(now);
        if bucket.take() {
            HandshakeCadenceAdmission::Admit
        } else {
            HandshakeCadenceAdmission::Exhausted
        }
    }

    /// Refunds a registered connection or an attempt QUIC never started.
    pub fn refund_cadence(&mut self, ip: IpAddr, now: Instant) {
        if let Some(bucket) = self.cadence.get_mut(&admission_host(ip)) {
            bucket.refill(now);
            bucket.refund();
        }
    }

    /// Reports bucket creation to the simulator without exposing its token balance.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn has_cadence_host(&self, ip: IpAddr) -> bool {
        self.cadence.contains_key(&admission_host(ip))
    }

    /// Counts the bounded address ledger, including depleted idle buckets.
    pub fn cadence_hosts(&self) -> usize {
        self.cadence.len()
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
        self.inbound_reservations - self.inbound_pending
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
                counts.admit_cadence(ip, 16, 124, now),
                HandshakeCadenceAdmission::Admit
            );
        }
        assert_eq!(
            counts.admit_cadence(alias, 16, 124, now),
            HandshakeCadenceAdmission::Exhausted
        );
        let now = now + Duration::from_micros(62_500);
        assert_eq!(
            counts.admit_cadence(alias, 16, 124, now),
            HandshakeCadenceAdmission::Admit
        );
        assert_eq!(
            counts.admit_cadence(ip, 16, 124, now),
            HandshakeCadenceAdmission::Exhausted
        );
        counts.refund_cadence(ip, now);
        assert_eq!(
            counts.admit_cadence(alias, 16, 124, now),
            HandshakeCadenceAdmission::Admit
        );
        assert_eq!(
            counts.admit_cadence(ip, 16, 124, now),
            HandshakeCadenceAdmission::Exhausted
        );
        assert_eq!(counts.cadence_hosts(), 1);
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
                counts.admit_cadence(first, 16, 1, now),
                HandshakeCadenceAdmission::Admit
            );
        }
        assert_eq!(
            counts.admit_cadence(other, 16, 1, now),
            HandshakeCadenceAdmission::Capacity
        );
        assert_eq!(
            counts.admit_cadence(alias, 16, 1, now),
            HandshakeCadenceAdmission::Exhausted
        );
        assert_eq!(counts.cadence_hosts(), 1);
        assert_eq!(
            counts.admit_cadence(other, 16, 1, now + Duration::from_secs(1)),
            HandshakeCadenceAdmission::Admit
        );
        assert_eq!(counts.cadence_hosts(), 1);
    }
}
