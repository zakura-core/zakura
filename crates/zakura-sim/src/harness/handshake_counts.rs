//! Admission histories against the production machine and independent live owners.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};
use zakura_rt::time::Instant;

use zakura_network::zakura::{HandshakeCadenceAdmission, HandshakeCounts, IpAdmission};

use super::{machine::Machine, HarnessEntry};
use crate::property::Status;
use crate::record::machine::{CountsStep, CountsView, FailurePhase, MachineFailure, MachineStep};

/// Pending handshake bounds.
pub const PENDING: &str = "handshake/pending-bounded";
/// Registered address bounds.
pub const ADDRESS: &str = "handshake/connections-per-address-bounded";
/// Outbound reserve protection.
pub const RESERVE: &str = "handshake/dial-reserve-kept";
/// Conservation of admission charges.
pub const COUNTS: &str = "handshake/counts-match-permits";

/// Failed attempts consume only the cadence's rate and capacity.
pub const CADENCE: &str = "handshake/attempts-within-cadence";
/// Attempts inside the cadence retain admission eligibility.
pub const CONFORMANT: &str = "handshake/cadence-never-refuses-conformant";

const HOSTS: usize = 8;
const SLOTS: usize = 128;
const CONNECTION_CAP: usize = 92;
const INBOUND_CONNECTION_CAP: usize = 69;
const PENDING_CAP: usize = 32;
const INBOUND_CAP: usize = 24;
const ADDRESS_CAP: usize = 16;
const HOST_PENDING_CAP: usize = 4;

const MACHINE: Machine = Machine {
    name: "handshake/counts",
    identity: "handshake-counts",
    claim: "Admission charges match live connection owners.",
    rules: &[
        (
            CADENCE,
            "Failed attempts consume only initial, refilled, and refunded tokens.",
        ),
        (
            CONFORMANT,
            "An address with an available token is never refused by the cadence.",
        ),
        (
            PENDING,
            "QUIC and control handshakes stay within total, inbound and address bounds.",
            Status::Known { issue: 2341 },
        ),
        (
            ADDRESS,
            "Registered connections stay within the per-address cap.",
            Status::Active,
        ),
        (
            RESERVE,
            "Inbound connections and handshakes preserve the dial reserve.",
            Status::Active,
        ),
        (
            COUNTS,
            "Machine charges equal the live connection, handshake, and registration owners.",
            Status::Active,
        ),
    ],
    generator: 3,
    budget: 513,
    history,
    reached: |steps, _| {
        steps
            .iter()
            .any(|step| matches!(step, MachineStep::Handshake(step) if step.active))
    },
};

/// The production admission machine harness.
pub const ENTRY: HarnessEntry = crate::machine_entry!(MACHINE);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Quic,
    Control,
    Registered,
}

#[derive(Clone, Copy)]
struct Owner {
    host: usize,
    ip: IpAddr,
    inbound: bool,
    phase: Phase,
}

/// Three address encodings per host: native IPv4, mapped IPv4, or shared /64.
/// The third encoding uses the second half of the eight independent hosts.
fn address(byte: u8) -> (usize, IpAddr) {
    let host = usize::from(byte % 4);
    let ipv4 = Ipv4Addr::new(192, 0, 2, byte % 4 + 1);
    match (byte / 4) % 3 {
        0 => (host, IpAddr::V4(ipv4)),
        1 => (host, IpAddr::V6(ipv4.to_ipv6_mapped())),
        _ => (
            host + 4,
            IpAddr::V6(Ipv6Addr::new(
                0x2001,
                0xdb8,
                0,
                u16::from(byte % 4),
                0,
                0,
                0,
                u16::from(byte) + 1,
            )),
        ),
    }
}

fn canonical_address(host: usize) -> IpAddr {
    if host < 4 {
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, u8::try_from(host + 1).unwrap()))
    } else {
        IpAddr::V6(Ipv6Addr::new(
            0x2001,
            0xdb8,
            0,
            u16::try_from(host - 4).unwrap(),
            0,
            0,
            0,
            1,
        ))
    }
}

fn expected(owners: &[Option<Owner>; SLOTS]) -> CountsView {
    let mut view = CountsView::default();
    for owner in owners.iter().flatten() {
        if !owner.inbound || owner.phase != Phase::Quic {
            view.connections += 1;
            if owner.inbound {
                view.inbound_connections += 1;
            }
        }
        match owner.phase {
            Phase::Quic => {}
            Phase::Control => {
                view.pending += 1;
                if owner.inbound {
                    view.inbound_pending += 1;
                    view.handshaking[owner.host] += 1;
                }
            }
            Phase::Registered => view.registered[owner.host] += 1,
        }
    }
    view
}

fn actual(counts: &HandshakeCounts) -> CountsView {
    CountsView {
        connections: counts.connections(),
        inbound_connections: counts.inbound_connections(),
        registered: std::array::from_fn(|host| counts.registered(canonical_address(host))),
        handshaking: std::array::from_fn(|host| counts.handshaking(canonical_address(host))),
        pending: counts.pending(),
        inbound_pending: counts.inbound_pending(),
    }
}

fn release(counts: &mut HandshakeCounts, owner: Owner) {
    release_handshake(counts, owner);
    if !owner.inbound || owner.phase != Phase::Quic {
        counts.release_connection(owner.inbound);
    }
}

fn release_handshake(counts: &mut HandshakeCounts, owner: Owner) {
    match owner.phase {
        Phase::Quic => {}
        Phase::Control => {
            counts.release_pending(owner.inbound);
            if owner.inbound {
                counts.leave_handshake(owner.ip);
            }
        }
        Phase::Registered => counts.remove_registered(owner.ip),
    }
}

fn failure(failures: &mut Vec<(String, MachineFailure)>, id: &str, message: impl Into<String>) {
    failures.push((
        id.into(),
        MachineFailure {
            property_failure: true,
            phase: FailurePhase::Invariant,
            message: message.into(),
        },
    ));
}

/// Expands a tagged input into an admission burst followed by ordinary operations.
/// The burst makes address, pending, and connection limits reachable in short searches.
fn operations(input: &[u8]) -> Vec<u8> {
    if !input.first().is_some_and(|byte| byte & 0x80 != 0) {
        return input.iter().take(1536).copied().collect();
    }
    let mode = input.get(1).copied().unwrap_or_default() % 3;
    let host = input.get(2).copied().unwrap_or_default() & 0x7f;
    let mut operations = Vec::new();
    match mode {
        0 => {
            for slot in 0..18 {
                operations.extend([0, slot, host, 2, slot, 0, 3, slot, 0]);
            }
        }
        1 => {
            for slot in 0..26 {
                let host = slot % 4 + (slot / 12) * 8;
                operations.extend([0, slot, host, 2, slot, 0]);
            }
            for slot in 26..34 {
                operations.extend([1, slot, 0, 2, slot, 0]);
            }
        }
        _ => {
            for slot in 0..70u8 {
                let host = if slot % 8 < 4 { slot % 8 } else { slot % 8 + 4 };
                operations.extend([0, slot, host, 2, slot, 0, 3, slot, 0]);
            }
            for slot in 70..94 {
                operations.extend([1, slot, 0]);
            }
        }
    }
    operations.extend([4, 0, 0]);
    operations.extend(input.iter().skip(3).take(1536 - operations.len()));
    operations
}

struct CadenceOracle {
    origin: Instant,
    now_ns: u64,
    last: [Option<u64>; HOSTS],
    tokens: [u32; HOSTS],
}

impl CadenceOracle {
    fn now(&self) -> Instant {
        self.origin + Duration::from_nanos(self.now_ns)
    }

    fn refill(&mut self, host: usize) {
        // The harness admits sixteen nodes per address, each retrying once a second.
        let last = self.last[host].get_or_insert(self.now_ns);
        while self.tokens[host] < 16 && self.now_ns - *last >= 62_500_000 {
            *last += 62_500_000;
            self.tokens[host] += 1;
        }
        // A full bucket skips complete refill intervals while retaining their phase.
        *last += ((self.now_ns - *last) / 62_500_000) * 62_500_000;
    }

    fn attempt(
        &mut self,
        counts: &mut HandshakeCounts,
        host: usize,
        ip: IpAddr,
        failures: &mut Vec<(String, MachineFailure)>,
    ) -> bool {
        self.refill(host);
        let admitted =
            counts.admit_cadence(ip, ADDRESS_CAP, CONNECTION_CAP + PENDING_CAP, self.now())
                == HandshakeCadenceAdmission::Admit;
        if admitted && self.tokens[host] == 0 {
            failure(
                failures,
                CADENCE,
                "an exhausted address started another TLS attempt",
            );
        }
        if !admitted && self.tokens[host] != 0 {
            failure(failures, CONFORMANT, "an address with a token was refused");
        }
        if admitted {
            self.tokens[host] = self.tokens[host].saturating_sub(1);
        }
        admitted
    }

    fn refund(&mut self, counts: &mut HandshakeCounts, host: usize, ip: IpAddr) {
        self.refill(host);
        self.tokens[host] = (self.tokens[host] + 1).min(16);
        counts.refund_cadence(ip, self.now());
    }
}

fn history(input: &[u8]) -> Vec<(usize, MachineStep)> {
    let mut counts = HandshakeCounts::default();
    let mut cadence = CadenceOracle {
        origin: Instant::now(),
        now_ns: 0,
        last: [None; HOSTS],
        tokens: [16; HOSTS],
    };
    let mut owners = [None; SLOTS];
    let mut history = Vec::new();
    let mut active = false;
    for (index, bytes) in operations(input).chunks(3).enumerate() {
        let op = bytes[0] % 10;
        let slot = usize::from(*bytes.get(1).unwrap_or(&0)) % SLOTS;
        let arg = *bytes.get(2).unwrap_or(&0);
        let (host, ip) = address(arg);
        let mut failures = Vec::new();
        match (op, owners[slot]) {
            (0 | 1, None) => {
                let inbound = op == 0;
                let validated = arg & 128 == 0;
                let view = expected(&owners);
                let quic_host = owners
                    .iter()
                    .flatten()
                    .filter(|owner| {
                        owner.inbound && owner.phase == Phase::Quic && owner.host == host
                    })
                    .count();
                let quic_total = owners
                    .iter()
                    .flatten()
                    .filter(|owner| owner.inbound && owner.phase == Phase::Quic)
                    .count();
                let admitted = if inbound {
                    !counts.connections_full(true, CONNECTION_CAP)
                        && (counts.ip_admission(
                            ip,
                            quic_host,
                            HOST_PENDING_CAP,
                            ADDRESS_CAP,
                            validated,
                        ) == IpAdmission::Open
                            && !counts.pending_full(quic_total, PENDING_CAP))
                } else {
                    counts.claim_connection(false, CONNECTION_CAP)
                };
                active |= admitted;
                let admitted =
                    admitted && (!inbound || cadence.attempt(&mut counts, host, ip, &mut failures));
                // Count actual admitted owners. Bounds below judge the admission decision.
                if admitted {
                    owners[slot] = Some(Owner {
                        host,
                        ip,
                        inbound,
                        phase: Phase::Quic,
                    });
                    active = true;
                }
                if !inbound
                    && !admitted
                    && view.connections - view.inbound_connections
                        < CONNECTION_CAP - INBOUND_CONNECTION_CAP
                {
                    failure(
                        &mut failures,
                        RESERVE,
                        "inbound load refused a dial before TLS",
                    );
                }
            }
            (2, Some(mut owner)) if owner.phase == Phase::Quic => {
                let view = expected(&owners);
                let connection_claimed =
                    !owner.inbound || counts.claim_connection(true, CONNECTION_CAP);
                let claimed =
                    connection_claimed && counts.claim_pending(owner.inbound, PENDING_CAP);
                if !owner.inbound
                    && view.pending - view.inbound_pending < PENDING_CAP - INBOUND_CAP
                    && !claimed
                {
                    failure(
                        &mut failures,
                        RESERVE,
                        "inbound control work consumed the dial reserve",
                    );
                }
                if claimed {
                    if owner.inbound {
                        counts.enter_handshake(owner.ip);
                    }
                    owner.phase = Phase::Control;
                    owners[slot] = Some(owner);
                } else {
                    if connection_claimed {
                        counts.release_connection(owner.inbound);
                    }
                    owners[slot] = None;
                }
            }
            (3, Some(mut owner)) if owner.phase == Phase::Control => {
                let registered = counts.can_register(owner.ip, ADDRESS_CAP, false);
                release_handshake(&mut counts, owner);
                if registered {
                    if owner.inbound {
                        cadence.refund(&mut counts, owner.host, owner.ip);
                    }
                    counts.add_registered(owner.ip);
                    owner.phase = Phase::Registered;
                    owners[slot] = Some(owner);
                } else {
                    counts.release_connection(owner.inbound);
                    owners[slot] = None;
                }
            }
            (4 | 5, Some(owner)) => {
                release(&mut counts, owner);
                owners[slot] = None;
            }
            (6, Some(owner)) if owner.phase == Phase::Registered => {
                // A winning same-IP redial replaces its incumbent without retaining two owners.
                if !counts.can_register(owner.ip, ADDRESS_CAP, true) {
                    failure(
                        &mut failures,
                        ADDRESS,
                        "same-IP replacement was refused at the cap",
                    );
                } else {
                    counts.remove_registered(owner.ip);
                    counts.add_registered(owner.ip);
                }
            }
            (7, _) => {
                cadence.now_ns += (u64::from(bytes.get(1).copied().unwrap_or(0))
                    + 256 * u64::from(arg))
                    * 1_000_000
            }
            (8, _) => {
                // TLS fails immediately, so the admitted attempt retains its charge.
                active = true;
                cadence.attempt(&mut counts, host, ip, &mut failures);
            }
            (9, Some(owner)) if owner.inbound && owner.phase == Phase::Quic => {
                // A transport Retry or capacity refusal starts no TLS work.
                cadence.refund(&mut counts, owner.host, owner.ip);
                release(&mut counts, owner);
                owners[slot] = None;
            }
            _ => {}
        }
        let actual = actual(&counts);
        let expected = expected(&owners);
        if actual != expected {
            failure(
                &mut failures,
                COUNTS,
                format!("charges {actual:?} differ from owners {expected:?}"),
            );
        }
        let quic_total = owners
            .iter()
            .flatten()
            .filter(|owner| owner.inbound && owner.phase == Phase::Quic)
            .count();
        let per_host_full = (0..HOSTS).any(|host| {
            let quic = owners
                .iter()
                .flatten()
                .filter(|owner| owner.inbound && owner.phase == Phase::Quic && owner.host == host)
                .count();
            actual.handshaking[host] + quic > HOST_PENDING_CAP
        });
        if actual.pending + quic_total > PENDING_CAP
            || actual.inbound_pending + quic_total > INBOUND_CAP
            || per_host_full
        {
            failure(
                &mut failures,
                PENDING,
                "live handshake owners exceed a pending bound",
            );
        }
        if actual.connections > CONNECTION_CAP
            || actual.inbound_connections > INBOUND_CONNECTION_CAP
        {
            failure(
                &mut failures,
                RESERVE,
                "connection owners exceed the total or inbound share",
            );
        }
        if actual.registered.iter().any(|&count| count > ADDRESS_CAP) {
            failure(
                &mut failures,
                ADDRESS,
                "registered owners exceed the address cap",
            );
        }
        let failed = !failures.is_empty();
        history.push((
            index + 1,
            MachineStep::Handshake(CountsStep {
                input: bytes.to_vec(),
                actual,
                expected,
                active,
                failures,
            }),
        ));
        if failed {
            return history;
        }
    }
    for owner in owners.iter_mut().filter_map(Option::take) {
        release(&mut counts, owner);
    }
    let actual = actual(&counts);
    let expected = CountsView::default();
    let mut failures = Vec::new();
    if actual != expected {
        failure(&mut failures, COUNTS, "teardown retained a charge");
    }
    history.push((
        history.len() + 1,
        MachineStep::Handshake(CountsStep {
            input: Vec::new(),
            actual,
            expected,
            active,
            failures,
        }),
    ));
    history
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holds(bytes: &[u8]) {
        let steps = history(bytes);
        assert!(
            steps.iter().all(|(_, step)| step.failure().is_none()),
            "{steps:?}"
        );
    }

    #[test]
    fn failed_attempts_exhaust_aliases_and_time_restores_their_cadence() {
        let mut input = Vec::new();
        for _ in 0..20 {
            input.extend([8, 0, 0, 8, 0, 4]);
        }
        input.extend([7, 125, 0, 8, 0, 0, 8, 0, 4, 8, 0, 0]);
        holds(&input);
    }

    #[test]
    fn registrations_and_transport_retries_refund_their_attempts() {
        let mut input = Vec::new();
        for _ in 0..40 {
            input.extend([0, 0, 0, 9, 0, 0]);
            input.extend([0, 0, 0, 2, 0, 0, 3, 0, 0, 4, 0, 0]);
        }
        holds(&input);
    }

    #[test]
    fn address_aliases_share_counts_and_release() {
        holds(&[
            0, 0, 0, 2, 0, 0, 0, 1, 4, 2, 1, 0, 3, 0, 0, 4, 1, 0, 5, 0, 0,
        ]);
    }

    #[test]
    fn inbound_pressure_preserves_outbound_permits() {
        let mut input = Vec::new();
        for slot in 0..24 {
            input.extend([0, slot, slot % 4 + (slot / 12) * 8]);
            input.extend([2, slot, 0]);
        }
        for slot in 24..32 {
            input.extend([1, slot, 0, 2, slot, 0]);
        }
        holds(&input);
    }

    #[test]
    fn registered_inbound_load_preserves_connection_reserve() {
        let mut input = Vec::new();
        for slot in 0..70u8 {
            let host = if slot % 8 < 4 { slot % 8 } else { slot % 8 + 4 };
            input.extend([0, slot, host, 2, slot, 0, 3, slot, 0]);
        }
        for slot in 70..94u8 {
            input.extend([1, slot, 0]);
        }
        let steps = history(&input);
        holds(&input);
        assert!(steps.iter().any(|(_, step)| matches!(step,
            MachineStep::Handshake(step) if step.actual.connections == CONNECTION_CAP
                && step.actual.inbound_connections == INBOUND_CONNECTION_CAP)));
    }

    #[test]
    fn registered_cap_allows_same_host_replacement() {
        let mut input = Vec::new();
        for slot in 0..18 {
            input.extend([0, slot, 0, 2, slot, 0, 3, slot, 0]);
        }
        input.extend([6, 0, 0]);
        holds(&input);
    }

    #[test]
    fn outbound_control_claims_ignore_pending_quic_owners() {
        let mut input = Vec::new();
        for slot in 0..24u8 {
            let host = if slot % 8 < 4 { slot % 8 } else { slot % 8 + 4 };
            input.extend([0, slot, host]);
        }
        for slot in 24..33 {
            input.extend([1, slot, 0, 2, slot, 0]);
        }
        let steps = history(&input);
        let MachineStep::Handshake(last) = &steps.last().unwrap().1 else {
            panic!("the admission harness records handshake steps");
        };
        assert_eq!(last.actual.pending, 9);
        assert_eq!(last.actual.inbound_pending, 0);
        assert_eq!(last.failures.len(), 1);
        assert_eq!(last.failures[0].0, PENDING);
        assert_eq!(
            last.failures[0].1.message,
            "live handshake owners exceed a pending bound"
        );
        assert!(steps[..steps.len() - 1]
            .iter()
            .all(|(_, step)| step.failure().is_none()));
        (ENTRY.witness)(
            &(ENTRY.draw)(&input),
            crate::PlanInput::Bytes(Vec::new()),
            PENDING,
            crate::witness::Expect::Violated,
            &[],
            &mut crate::NullSink,
        )
        .unwrap();
    }

    #[test]
    fn generated_bursts_reach_each_admission_boundary() {
        for mode in 0..3 {
            let input = [0x80, mode, 0];
            holds(&input);
            assert!(history(&input).iter().any(|(_, step)| matches!(step,
                MachineStep::Handshake(step) if match mode {
                    0 => step.actual.registered[0] == ADDRESS_CAP,
                    1 => step.actual.inbound_pending == INBOUND_CAP && step.actual.pending == PENDING_CAP,
                    _ => step.actual.inbound_connections == INBOUND_CONNECTION_CAP && step.actual.connections == CONNECTION_CAP,
                }
            )), "burst {mode} must reach its admission boundary");
        }
    }
}
