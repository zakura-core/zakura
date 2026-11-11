//! Pure admission decisions and charges shared by QUIC and control handshakes.

use std::net::IpAddr;
use zakura_rt::collections::HashMap;

use crate::zakura::admission_host;

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
}

impl HandshakeCounts {
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

    /// Claims a control-handshake permit within the appropriate share.
    pub fn claim_pending(&mut self, inbound: bool, cap: usize) -> bool {
        if self.pending >= cap || (inbound && self.inbound_pending >= inbound_share(cap)) {
            return false;
        }
        self.pending += 1;
        if inbound {
            self.inbound_pending += 1;
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
