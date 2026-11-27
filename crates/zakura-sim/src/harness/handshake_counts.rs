//! Admission histories against the production machine and the live owners
//! the harness tracks.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};
use zakura_rt::time::Instant;

use zakura_network::zakura::{
    GatewayRanking, GatewayStanding, HandshakeCadenceAdmission, HandshakeCounts, IpAdmission,
    RankedGateway, SlotClass, SlotRefusal, SlotSplit,
};
use zakura_quic::{NodeId, NodeSecretKey};

use super::{machine::Machine, HarnessEntry};
use crate::property::Status;
use crate::record::machine::{CountsStep, CountsView, FailurePhase, MachineFailure, MachineStep};
use crate::table::cadence_window::CadenceWindow;

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
/// Remote node IDs the owners draw from, few enough that one node often
/// holds several connection slots.
const NODES: usize = 4;
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
            "TLS attempts consume only initial, refilled, and refunded tokens.",
            Status::Active,
        ),
        (
            CONFORMANT,
            "An address with an available token is never refused by the cadence.",
            Status::Active,
        ),
        (
            PENDING,
            "QUIC and control handshakes stay within total, inbound and address bounds.",
            Status::Active,
        ),
        (
            ADDRESS,
            "Registered connections stay within the per-address cap.",
            Status::Active,
        ),
        (
            RESERVE,
            "Inbound connections and handshakes preserve the dial reserve, and no ordinary slot is granted at or above the ordinary limit.",
            Status::Active,
        ),
        (
            COUNTS,
            "Machine charges equal the live connection, handshake, and registration owners, and so does their split into one preferred slot per preferred gateway holding any, within the hold, and ordinary slots.",
            Status::Active,
        ),
    ],
    generator: 5,
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
    node: NodeId,
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
        counts.release_connection(owner.node, owner.inbound);
    }
}

fn release_handshake(counts: &mut HandshakeCounts, owner: Owner) {
    match owner.phase {
        Phase::Quic => {
            if owner.inbound {
                counts.release_transport();
            }
        }
        Phase::Control => {
            counts.release_pending(owner.inbound);
            if owner.inbound {
                counts.leave_handshake(owner.ip);
            }
        }
        Phase::Registered => counts.remove_registered(owner.ip),
    }
}

/// The live owners' connection slots of `node` in one direction.
fn live_slots(owners: &[Option<Owner>; SLOTS], node: NodeId, inbound: bool) -> usize {
    owners
        .iter()
        .flatten()
        .filter(|owner| {
            owner.node == node
                && owner.inbound == inbound
                && (!owner.inbound || owner.phase != Phase::Quic)
        })
        .count()
}

/// The live owners' slots split by `set`, counted from the owners alone:
/// one preferred slot per member holding any, inbound if it holds an
/// inbound one.
fn owner_split(owners: &[Option<Owner>; SLOTS], set: &[NodeId]) -> SlotSplit {
    let view = expected(owners);
    let holding = |inbound_only: bool| {
        set.iter()
            .filter(|&&member| {
                live_slots(owners, member, true) > 0
                    || (!inbound_only && live_slots(owners, member, false) > 0)
            })
            .count()
    };
    SlotSplit {
        preferred: holding(false),
        ordinary: view.connections - holding(false),
        ordinary_inbound: view.inbound_connections - holding(true),
    }
}

/// Judges one connection claim against the owners before it, under the
/// preferred set the claim was decided with.
fn check_claim(
    failures: &mut Vec<(String, MachineFailure)>,
    owners: &[Option<Owner>; SLOTS],
    counts: &HandshakeCounts,
    node: NodeId,
    inbound: bool,
    claim: Result<SlotClass, SlotRefusal>,
) {
    let preferred = counts.preferred_gateways();
    let before = owner_split(owners, preferred.set());
    if claim.is_ok() && before.ordinary + before.preferred >= CONNECTION_CAP + preferred.hold() {
        failure(
            failures,
            RESERVE,
            "a slot was granted above the ordinary limit plus the active gateway places",
        );
    }
    match claim {
        Ok(SlotClass::Ordinary) => {
            if before.ordinary >= CONNECTION_CAP {
                failure(
                    failures,
                    RESERVE,
                    "an ordinary slot was granted at or above the ordinary limit",
                );
            }
            if inbound && before.ordinary_inbound >= INBOUND_CONNECTION_CAP {
                failure(
                    failures,
                    RESERVE,
                    "an ordinary inbound slot was granted past the inbound share",
                );
            }
        }
        Ok(SlotClass::Preferred) => {
            if !preferred.contains(&node)
                || live_slots(owners, node, true) + live_slots(owners, node, false) > 0
            {
                failure(
                    failures,
                    COUNTS,
                    "a preferred slot went to a node that is not a member holding no slot",
                );
            }
        }
        Err(_) => {}
    }
}

/// Ranks the table in order. The low four bits mark dialable nodes.
fn ranking_from(nodes: &[NodeId; NODES], arg: u8) -> GatewayRanking {
    nodes
        .iter()
        .enumerate()
        .map(|(index, &node)| {
            let standing = if arg & (1 << index) != 0 {
                GatewayStanding::Dialable
            } else {
                GatewayStanding::Unmatched
            };
            // The table holds four nodes, so the count fits.
            RankedGateway::resolved(node, (NODES - index) as u32, standing)
        })
        .collect()
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
        1 if input.get(2).is_some_and(|byte| byte & 0x80 != 0) => {
            for slot in 0..24u8 {
                let host = if slot % 8 < 4 { slot % 8 } else { slot % 8 + 4 };
                operations.extend([0, slot, host]);
            }
            for slot in 24..33 {
                operations.extend([1, slot, 0, 2, slot, 0]);
            }
        }
        1 => {
            for slot in 0..26 {
                let host = slot % 4 + (slot / 12) * 8;
                // Eight addresses would reach the node's reserve, which
                // admits one attempt per address.
                operations.extend([7, NODE_REFILL_MS, 0]);
                operations.extend([0, slot, host, 2, slot, 0]);
            }
            for slot in 26..34 {
                operations.extend([1, slot, 0, 2, slot, 0]);
            }
        }
        _ => {
            for slot in 0..70u8 {
                let host = if slot % 8 < 4 { slot % 8 } else { slot % 8 + 4 };
                operations.extend([7, NODE_REFILL_MS, 0]);
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

/// Milliseconds that refill the node's cadence by at least one token: it
/// refills 24 tokens a second. A burst that waits this long before each
/// attempt is never refused for the node's rate, so it reaches the
/// connection caps.
const NODE_REFILL_MS: u8 = 42;

/// The cadence refills one token per sixteenth of a second: sixteen nodes
/// share an address, and each retries once a second.
const REFILL_NS: u64 = 62_500_000;

/// The harness clock and each host's charges and refunds. It tracks counts
/// only; the machine alone decides admission.
struct CadenceLog {
    origin: Instant,
    now_ns: u64,
    windows: [CadenceWindow; HOSTS],
}

impl CadenceLog {
    fn now(&self) -> Instant {
        self.origin + Duration::from_nanos(self.now_ns)
    }

    fn attempt(
        &mut self,
        counts: &mut HandshakeCounts,
        host: usize,
        ip: IpAddr,
        failures: &mut Vec<(String, MachineFailure)>,
    ) -> bool {
        let admission = counts.admit_cadence(
            ip,
            false,
            ADDRESS_CAP,
            CONNECTION_CAP + PENDING_CAP,
            self.now(),
        );
        let admitted = admission == HandshakeCadenceAdmission::Admit;
        if admitted && !self.windows[host].charge(self.now_ns) {
            failure(
                failures,
                CADENCE,
                "an address started more TLS attempts than its cadence pays for",
            );
        }
        // The node's bucket bounds every address together, so only an
        // address refusal must be earned by that address's charges.
        if !admitted
            && admission != HandshakeCadenceAdmission::NodeExhausted
            && !self.windows[host].refusal_earned(self.now_ns)
        {
            failure(failures, CONFORMANT, "an address with a token was refused");
        }
        admitted
    }

    fn refund(&mut self, counts: &mut HandshakeCounts, host: usize, ip: IpAddr) {
        self.windows[host].refund(self.now_ns);
        counts.refund_cadence(ip, false, self.now());
    }
}

fn history(input: &[u8]) -> Vec<(usize, MachineStep)> {
    let mut counts = HandshakeCounts::default();
    let nodes: [NodeId; NODES] = std::array::from_fn(|index| {
        let seed = u8::try_from(index + 1).expect("the node table is small");
        NodeSecretKey::from_bytes(&[seed; 32]).public()
    });
    // The address cap is sixteen, so it fits `u32`.
    let mut cadence = CadenceLog {
        origin: Instant::now(),
        now_ns: 0,
        windows: std::array::from_fn(|_| CadenceWindow::new(ADDRESS_CAP as u32, REFILL_NS)),
    };
    let mut owners = [None; SLOTS];
    let mut history = Vec::new();
    let mut active = false;
    // Whether a preferred set was ever published. Until then every inbound
    // slot is ordinary, so the inbound share bounds them all.
    let mut published = false;
    for (index, bytes) in operations(input).chunks(3).enumerate() {
        let op = bytes[0] % 11;
        let slot = usize::from(*bytes.get(1).unwrap_or(&0)) % SLOTS;
        let arg = *bytes.get(2).unwrap_or(&0);
        let (host, ip) = address(arg);
        let node = nodes[usize::from(arg >> 4) % NODES];
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
                let claim =
                    (!inbound).then(|| counts.claim_connection(node, false, CONNECTION_CAP));
                if let Some(claim) = claim {
                    check_claim(&mut failures, &owners, &counts, node, false, claim);
                }
                let admitted = if inbound {
                    counts.inbound_place_free(CONNECTION_CAP)
                        && (counts.ip_admission(
                            ip,
                            quic_host,
                            HOST_PENDING_CAP,
                            ADDRESS_CAP,
                            validated,
                        ) == IpAdmission::Open
                            && !counts.pending_full(quic_total, PENDING_CAP))
                } else {
                    claim.is_some_and(|claim| claim.is_ok())
                };
                active |= admitted;
                let admitted =
                    admitted && (!inbound || cadence.attempt(&mut counts, host, ip, &mut failures));
                let admitted = admitted && (!inbound || counts.claim_transport(PENDING_CAP));
                // Count actual admitted owners. Bounds below judge the admission decision.
                if admitted {
                    owners[slot] = Some(Owner {
                        host,
                        ip,
                        node,
                        inbound,
                        phase: Phase::Quic,
                    });
                    active = true;
                }
                // A refused dial must be one the limits explain: a member's
                // first slot is refused only at the limit, and an ordinary
                // dial only once ordinary dials fill the reserve or ordinary inbound slots pass their share.
                if !inbound && !admitted {
                    let preferred = counts.preferred_gateways();
                    let split = owner_split(&owners, preferred.set());
                    let fresh_member = preferred.contains(&node)
                        && live_slots(&owners, node, true) + live_slots(&owners, node, false) == 0;
                    let ordinary_outbound = split.ordinary - split.ordinary_inbound;
                    if (fresh_member && view.connections < CONNECTION_CAP + preferred.hold())
                        || (split.ordinary_inbound <= INBOUND_CONNECTION_CAP
                            && ordinary_outbound < CONNECTION_CAP - INBOUND_CONNECTION_CAP
                            && view.connections < CONNECTION_CAP + preferred.hold())
                    {
                        failure(
                            &mut failures,
                            RESERVE,
                            "inbound load refused a dial before TLS",
                        );
                    }
                }
            }
            (2, Some(mut owner)) if owner.phase == Phase::Quic => {
                let view = expected(&owners);
                let set_before = counts.preferred_gateways().clone();
                let connection_claimed = !owner.inbound || {
                    let claim = counts.claim_connection(owner.node, true, CONNECTION_CAP);
                    check_claim(&mut failures, &owners, &counts, owner.node, true, claim);
                    if claim.is_err() && *counts.preferred_gateways() != set_before {
                        failure(
                            &mut failures,
                            COUNTS,
                            "a refused claim changed the preferred set",
                        );
                    }
                    claim.is_ok()
                };
                let claimed = connection_claimed
                    && if owner.inbound {
                        counts.claim_transferred_pending();
                        true
                    } else {
                        counts.claim_pending(false, PENDING_CAP)
                    };
                if owner.inbound && !claimed {
                    counts.release_transport();
                }
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
                        counts.release_connection(owner.node, owner.inbound);
                    }
                    owners[slot] = None;
                }
            }
            (3, Some(mut owner)) if owner.phase == Phase::Control => {
                let registered = counts.can_register(owner.ip, ADDRESS_CAP, false);
                release_handshake(&mut counts, owner);
                if registered {
                    counts.add_registered(owner.ip);
                    owner.phase = Phase::Registered;
                    owners[slot] = Some(owner);
                } else {
                    counts.release_connection(owner.node, owner.inbound);
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
            (10, _) => {
                // The ranking changes. Slots already held stay held.
                counts.publish_gateway_ranking(
                    &ranking_from(&nodes, arg),
                    usize::from(arg >> 4) % 4,
                    CONNECTION_CAP,
                );
                published = true;
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
        if counts.transport_pending() != quic_total {
            failure(
                &mut failures,
                COUNTS,
                "transport reservations differ from live owners",
            );
        }
        let by_node = counts.summed_node_connections();
        if by_node.total() != actual.connections
            || by_node.inbound != actual.inbound_connections
            || nodes.iter().any(|&node| {
                let slots = counts.node_connections(&node);
                slots.inbound != live_slots(&owners, node, true)
                    || slots.outbound != live_slots(&owners, node, false)
            })
        {
            failure(
                &mut failures,
                COUNTS,
                "per-node connection slots differ from live owners",
            );
        }
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
        let preferred = counts.preferred_gateways();
        let split = counts.slot_split();
        let owners_split = owner_split(&owners, preferred.set());
        if split.preferred + split.ordinary != actual.connections
            || split.ordinary != owners_split.ordinary
            || split.ordinary_inbound != owners_split.ordinary_inbound
            || split.preferred > preferred.hold()
            || preferred.set().len() > preferred.hold()
        {
            failure(
                &mut failures,
                COUNTS,
                format!(
                    "split {split:?} differs from owners {owners_split:?} or passes the hold {}",
                    preferred.hold()
                ),
            );
        }
        if split.preferred != owners_split.preferred {
            failure(
                &mut failures,
                COUNTS,
                "preferred slots differ from the members holding a slot",
            );
        }
        if actual.connections > CONNECTION_CAP + NODES
            || (!published && actual.inbound_connections > INBOUND_CONNECTION_CAP)
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
    if actual != expected || counts.transport_pending() != 0 {
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
    fn transport_retries_refund_their_attempts_and_registrations_keep_theirs() {
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
            input.extend([7, NODE_REFILL_MS, 0]);
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
    fn outbound_control_claims_include_pending_quic_owners() {
        let input = [0x80, 1, 0x80];
        let steps = history(&input);
        assert!(steps.iter().any(|(_, step)| matches!(step,
            MachineStep::Handshake(step) if step.actual.pending == 8 && step.actual.inbound_pending == 0
        )));
        assert!(steps.iter().all(|(_, step)| step.failure().is_none()));
        (ENTRY.witness)(
            &(ENTRY.draw)(&input),
            crate::PlanInput::Bytes(Vec::new()),
            PENDING,
            crate::witness::Expect::Holds,
            &[],
            &mut crate::NullSink,
        )
        .unwrap();
    }

    /// A published hold keeps one place for its gateway: ordinary dials
    /// fill the ordinary limit, the gateway's dial takes an extra place, and a
    /// ranked caller unknown to v2 takes a place it outranks.
    #[test]
    fn a_published_hold_keeps_a_place_for_its_gateway() {
        // A hold limit of one; nodes 1 to 3 dialable, node 0 unknown.
        let mut input = vec![10, 0, 0x1e];
        for slot in 0..92 {
            input.extend([1, slot, 0x30]);
        }
        input.extend([1, 92, 0x30, 1, 93, 0x10]);
        let steps = history(&input);
        holds(&input);
        assert!(steps.iter().any(|(_, step)| matches!(step,
            MachineStep::Handshake(step) if step.actual.connections == CONNECTION_CAP + 1)));

        // Node 0 calls in at the ordinary limit and outranks the member.
        let mut input = vec![10, 0, 0x1e];
        for slot in 0..92 {
            input.extend([1, slot, 0x30]);
        }
        input.extend([0, 92, 0x00, 2, 92, 0]);
        let steps = history(&input);
        holds(&input);
        let (claimed, _) = steps.last().expect("the history ends with its teardown");
        // The claim is the last op, before the teardown step: it fills the
        // limit only if the caller was granted the held place.
        assert!(steps.iter().any(|(number, step)| matches!(step,
            MachineStep::Handshake(step) if *number == claimed - 1
                && step.actual.connections == CONNECTION_CAP + 1
                && step.actual.inbound_connections == 1)));
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
