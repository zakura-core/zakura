//! Admission histories against the production machine and independent live owners.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use zakura_network::zakura::{HandshakeCounts, IpAdmission};

use super::{machine::Machine, HarnessEntry};
use crate::record::machine::{CountsStep, CountsView, FailurePhase, MachineFailure, MachineStep};

/// Pending handshake bounds.
pub const PENDING: &str = "handshake/pending-bounded";
/// Registered address bounds.
pub const ADDRESS: &str = "handshake/connections-per-address-bounded";
/// Outbound reserve protection.
pub const RESERVE: &str = "handshake/dial-reserve-kept";
/// Conservation of admission charges.
pub const COUNTS: &str = "handshake/counts-match-permits";

const HOSTS: usize = 8;
const SLOTS: usize = 64;
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
            PENDING,
            "QUIC and control handshakes stay within total, inbound and address bounds.",
        ),
        (
            ADDRESS,
            "Registered connections stay within the per-address cap.",
        ),
        (RESERVE, "Inbound handshakes preserve the dial reserve."),
        (
            COUNTS,
            "Machine charges equal the live handshake and registration owners.",
        ),
    ],
    generator: 1,
    budget: 172,
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
        registered: std::array::from_fn(|host| counts.registered(canonical_address(host))),
        handshaking: std::array::from_fn(|host| counts.handshaking(canonical_address(host))),
        pending: counts.pending(),
        inbound_pending: counts.inbound_pending(),
    }
}

fn release(counts: &mut HandshakeCounts, owner: Owner) {
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

fn history(input: &[u8]) -> Vec<(usize, MachineStep)> {
    let mut counts = HandshakeCounts::default();
    let mut owners = [None; SLOTS];
    let mut history = Vec::new();
    let mut active = false;
    for (index, bytes) in input
        .iter()
        .take(512)
        .copied()
        .collect::<Vec<_>>()
        .chunks(3)
        .enumerate()
    {
        let op = bytes[0] % 7;
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
                let admitted = !inbound
                    || (counts.ip_admission(
                        ip,
                        quic_host,
                        HOST_PENDING_CAP,
                        ADDRESS_CAP,
                        validated,
                    ) == IpAdmission::Open
                        && !counts.pending_full(quic_total, PENDING_CAP));
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
                    && view.pending - view.inbound_pending < PENDING_CAP - INBOUND_CAP
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
                let claimed = counts.claim_pending(owner.inbound, PENDING_CAP);
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
                    owners[slot] = None;
                }
            }
            (3, Some(mut owner)) if owner.phase == Phase::Control => {
                let registered = counts.can_register(owner.ip, ADDRESS_CAP, false);
                release(&mut counts, owner);
                if registered {
                    counts.add_registered(owner.ip);
                    owner.phase = Phase::Registered;
                    owners[slot] = Some(owner);
                } else {
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
    fn registered_cap_allows_same_host_replacement() {
        let mut input = Vec::new();
        for slot in 0..18 {
            input.extend([0, slot, 0, 2, slot, 0, 3, slot, 0]);
        }
        input.extend([6, 0, 0]);
        holds(&input);
    }
}
