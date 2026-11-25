//! The mining gateways this node prefers to stay connected to, as the
//! discovery dialer sees them.
//!
//! The node ranks gateways by how many recent blocks name them, puts the IDs
//! it always prefers ahead of them, and hands the result to the dialer as a
//! [`MinerGatewayRanking`]. The dialer ranks it
//! once per change ([`PreferredGatewayInput`]), then each round resolves
//! every ranked gateway ID to the one node it names and classifies it, from
//! the registered peers, its own dials in flight and the discovery book
//! ([`classify_gateways`]). Those node IDs feed the preferred dial pass and
//! the places the connection-slot checks hold.

use std::{net::IpAddr, time::Duration};

use zakura_chain::miner_mesh::GatewayId;
use zakura_quic::{NodeAddr, NodeId};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::watch,
    time::Instant,
};

use super::super::protocol::{DialTarget, ZakuraDiscoveryDialCandidate, ZakuraDiscoveryHandle};
use super::discovery_node_addr_with_reserved_ip_capacity;
use super::state::{CandidateDialerState, DialAttemptKey};
use crate::zakura::{
    GatewayMatch, GatewayStanding, RankedGateway, MAX_PREFERRED_GATEWAYS, MAX_RANKED_GATEWAYS,
};

/// The gateways this node keeps connection places for: the IDs it always
/// prefers, then mining gateways ranked by how many recent best-chain blocks
/// name them.
///
/// The IDs the node always prefers rank first, in the order given, and each
/// keeps its place even while it is unreachable. The other gateways may be
/// in any order: the dialer ranks them by block count, most first, and by
/// lower gateway ID bytes among equal counts. It considers only the first
/// 256 by rank, always-preferred IDs included. zakurad converts its config
/// and the chain crate's gateway counts into this type in one place. It is
/// the node's raw input; the dialer turns it into [`RankedGateway`]s, which
/// carry the node each gateway ID names and each gateway's
/// [`GatewayStanding`] for the connection-slot checks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MinerGatewayRanking {
    /// The IDs the operator configures the node to always prefer, whatever
    /// the blocks say, each with the counted blocks that name it, which
    /// may be none. At most [`MAX_PREFERRED_GATEWAYS`] are kept.
    pub always_preferred: Vec<MinerGateway>,
    /// The gateways named in enough counted blocks for the node to prefer
    /// them.
    pub gateways: Vec<MinerGateway>,
}

/// One entry of a [`MinerGatewayRanking`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MinerGateway {
    /// The gateway ID exactly as the blocks wrote it: the first 20 bytes of
    /// the gateway's v2 node ID.
    pub gateway_id: GatewayId,
    /// How many counted blocks name this gateway.
    pub blocks: u32,
}

/// Converts `ranking` to the gateways the dialer can rank, best first, at
/// most [`MAX_RANKED_GATEWAYS`] plus ties at the cutoff. Each one names no
/// node until [`classify_gateways`] resolves it.
///
/// The IDs the node always prefers come first, in their order, at most
/// [`MAX_PREFERRED_GATEWAYS`] of them; a repeat keeps its first entry. The
/// other gateways follow, ranked by block count, leaving out the IDs already
/// listed, entries with no blocks and repeats of an ID, keeping the repeat
/// with the most blocks. Both drop the gateway ID of `local_node_id`: a node
/// never prefers a gateway ID that its own node ID starts with, even when
/// another node shares it. It ranks before it drops or caps anything, so a
/// dropped entry takes no place and lower-ranked entries can't push out a
/// better one.
fn rank_gateways(ranking: &MinerGatewayRanking, local_node_id: NodeId) -> Vec<RankedGateway> {
    let local_gateway = GatewayId::from_node_id(local_node_id.as_bytes());
    let mut seen = HashSet::new();
    let always_preferred: Vec<RankedGateway> = ranking
        .always_preferred
        .iter()
        .filter(|gateway| gateway.gateway_id != local_gateway)
        .filter(|gateway| seen.insert(gateway.gateway_id))
        .take(MAX_PREFERRED_GATEWAYS)
        .map(|gateway| RankedGateway::always_preferred(gateway.gateway_id, gateway.blocks))
        .collect();
    let mut gateways: Vec<MinerGateway> = ranking
        .gateways
        .iter()
        .filter(|gateway| gateway.blocks > 0)
        .copied()
        .collect();
    gateways.sort_unstable_by(|a, b| {
        b.blocks
            .cmp(&a.blocks)
            .then_with(|| a.gateway_id.cmp(&b.gateway_id))
    });
    let mut ranked: Vec<_> = gateways
        .into_iter()
        .filter(|gateway| seen.insert(gateway.gateway_id))
        .filter(|gateway| gateway.gateway_id != local_gateway)
        .map(|gateway| RankedGateway::unresolved(gateway.gateway_id, gateway.blocks))
        .collect();
    // Admission resolves cutoff ties against the live authenticated connections.
    if let Some(cutoff) = ranked.get(MAX_RANKED_GATEWAYS - always_preferred.len() - 1) {
        let keep = ranked.partition_point(|gateway| gateway.blocks() >= cutoff.blocks());
        ranked.truncate(keep);
    }
    always_preferred.into_iter().chain(ranked).collect()
}

/// The latest ranking the node handed the dialer, converted once per change.
///
/// Without a channel, or once its sender is gone, the ranking is empty.
struct PreferredGatewayInput {
    ranking: Option<watch::Receiver<MinerGatewayRanking>>,
    local_node_id: NodeId,
    ranked: Vec<RankedGateway>,
}

impl PreferredGatewayInput {
    /// Reads `ranking` for a node whose own ID is `local_node_id`.
    fn new(
        mut ranking: Option<watch::Receiver<MinerGatewayRanking>>,
        local_node_id: NodeId,
    ) -> Self {
        let ranked = ranking
            .as_mut()
            .map(|ranking| rank_gateways(&ranking.borrow_and_update(), local_node_id))
            .unwrap_or_default();
        Self {
            ranking,
            local_node_id,
            ranked,
        }
    }

    /// Returns the ranked gateways, best first, converting the ranking again
    /// only if it changed since the last call.
    fn current(&mut self) -> &[RankedGateway] {
        if let Some(ranking) = &mut self.ranking {
            match ranking.has_changed() {
                Ok(false) => {}
                Ok(true) => {
                    self.ranked = rank_gateways(&ranking.borrow_and_update(), self.local_node_id);
                }
                Err(_closed) => {
                    self.ranking = None;
                    self.ranked.clear();
                }
            }
        }
        &self.ranked
    }
}

/// The ranked gateways with their standings this round.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ClassifiedGateways {
    /// Every ranked gateway with its standing, in rank order.
    pub(super) ranking: Vec<RankedGateway>,
    /// The addresses of each gateway the discovery book could dial now.
    pub(super) dialable: HashMap<NodeId, ZakuraDiscoveryDialCandidate>,
}

/// Resolves each of `ranked`, in its order, to the one node its gateway ID
/// names, and classifies it at `now`.
///
/// The nodes a gateway ID can name are the peers in `registered`, the nodes
/// the dialer `state` has in flight and the nodes the discovery book holds,
/// which the book answers for in one lookup that an empty ranking skips.
/// When those hold two or more nodes with one gateway ID, the gateway is
/// ambiguous and names none of them. Otherwise a gateway whose node is
/// registered is connected, even while a dial to it is still in flight; one
/// the dialer has in flight is being dialed; and the book answers for the
/// rest, with [`dialer_standing`] adding the dialer's own backoff to its
/// answer.
pub(super) async fn classify_gateways(
    ranked: &[RankedGateway],
    registered: &[NodeId],
    state: &CandidateDialerState,
    discovery: &ZakuraDiscoveryHandle,
    now: Instant,
) -> ClassifiedGateways {
    let registered: HashSet<NodeId> = registered.iter().copied().collect();
    let in_flight: HashSet<NodeId> = state.in_flight_node_ids().into_iter().collect();

    // The registered and in-flight nodes each ranked gateway ID names.
    let ranked_ids: HashSet<GatewayId> = ranked.iter().map(RankedGateway::gateway).collect();
    let mut local: HashMap<GatewayId, GatewayMatch<NodeId>> = HashMap::default();
    for node in registered.iter().chain(&in_flight) {
        let gateway = GatewayId::from_node_id(node.as_bytes());
        if ranked_ids.contains(&gateway) {
            let known = local.entry(gateway).or_insert(GatewayMatch::None);
            *known = known.with(*node);
        }
    }

    let book_answers = if ranked.is_empty() {
        Vec::new()
    } else {
        let gateways: Vec<GatewayId> = ranked.iter().map(RankedGateway::gateway).collect();
        discovery.gateway_targets(&gateways).await
    };
    assert_eq!(
        book_answers.len(),
        ranked.len(),
        "the book answers once per gateway asked, in order",
    );

    let mut classified = ClassifiedGateways::default();
    for (gateway, book_answer) in ranked.iter().zip(book_answers) {
        let local = local
            .get(&gateway.gateway())
            .copied()
            .unwrap_or(GatewayMatch::None);
        let resolved = match local.merge(book_answer.as_ref().map(|(node, _)| *node)) {
            GatewayMatch::None => *gateway,
            GatewayMatch::Several => gateway.into_ambiguous(),
            GatewayMatch::One(node) => {
                let standing = if registered.contains(&node) {
                    GatewayStanding::Connected
                } else if in_flight.contains(&node) {
                    GatewayStanding::Dialing
                } else {
                    let GatewayMatch::One((_, target)) = book_answer else {
                        unreachable!("a node neither registered nor in flight came from the book");
                    };
                    let standing = dialer_standing(state, &target, now);
                    if let (GatewayStanding::Dialable, DialTarget::Dialable(candidate)) =
                        (standing, target)
                    {
                        classified.dialable.insert(node, candidate);
                    }
                    standing
                };
                gateway.resolved_as(node, standing)
            }
        };
        classified.ranking.push(resolved);
    }
    classified
}

/// What one round's preferred pass dials from: the preferred set, highest
/// rank first, and the addresses of each ranked gateway the discovery book
/// could dial now.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct PreferredRound {
    pub(super) set: Vec<NodeId>,
    pub(super) dialable: HashMap<NodeId, ZakuraDiscoveryDialCandidate>,
}

/// The standing of a gateway that is neither connected nor being dialed, from
/// the discovery book's `target` and the dialer's own `state` at `now`.
///
/// A gateway the book could dial is still in backoff when the dialer has
/// every one of its IPs in backoff, so the next gateway takes its place in
/// the set. Node-wide limits, such as free transport slots, pass and leave
/// the gateway dialable.
pub(super) fn dialer_standing(
    state: &CandidateDialerState,
    target: &DialTarget,
    now: Instant,
) -> GatewayStanding {
    match target {
        DialTarget::Unknown => GatewayStanding::Unmatched,
        DialTarget::InBackoff => GatewayStanding::Backoff,
        DialTarget::Dialable(candidate) if state.candidate_addresses(candidate, now).is_empty() => {
            GatewayStanding::Backoff
        }
        DialTarget::Dialable(_) => GatewayStanding::Dialable,
    }
}

/// The room one round's preferred pass may use.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PreferredRoom {
    /// Free connection slots, held places included.
    pub(super) connections: usize,
    /// Address attempts discovery's transport ceiling has room for.
    pub(super) address_attempts: usize,
    /// Dials that may start before the dialer reaches its limit on dials in
    /// flight.
    pub(super) dials: usize,
}

/// How long a member whose ID is below this node's must stay dialable and
/// unconnected before the preferred pass dials it.
///
/// It covers the offset between two nodes' one-second round timers (up to
/// one round), the lower node's handshake across a continent (a round trip
/// plus TLS, a few tenths of a second), and the skew in when each node sees
/// the ranking change. A gateway with a higher ID that does not prefer this
/// node back is dialed this much later.
pub(super) const MUTUAL_DIAL_GRACE: Duration = Duration::from_secs(5);

/// The set members whose dial waits [`MUTUAL_DIAL_GRACE`], so that two
/// gateways that prefer each other don't dial each other at once.
///
/// A dial holds its connection slot from its start. When two gateways that
/// prefer each other dial each other at once, each one's own dial fills the
/// place it holds for the other, so each refuses the other's call and both
/// dials fail into backoff. A node therefore waits before dialing a member
/// whose node ID bytes are lower than its own: when the two prefer each
/// other, the lower one dials at once and the higher one takes its call in
/// the place it holds, and when the lower one does not prefer this node, the
/// dial starts after the grace.
///
/// Only members that are dialable now wait, so the waiting list never holds
/// more than the preferred set, and a member that connects, is being dialed,
/// goes into backoff or leaves the set starts over.
#[derive(Clone, Debug)]
pub(super) struct DeferredDials {
    local_node_id: NodeId,
    /// Each waiting member, with when this node first found it dialable.
    waiting: HashMap<NodeId, Instant>,
}

impl DeferredDials {
    /// Defers dials for a node whose own ID is `local_node_id`.
    pub(super) fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            waiting: HashMap::default(),
        }
    }

    /// Starts a round: keeps only the members of `set` that are in `dialable`
    /// and have a lower node ID than this node's, each with the time this
    /// node first found it dialable, or `now` if it is new.
    fn refresh(
        &mut self,
        set: &[NodeId],
        dialable: &HashMap<NodeId, ZakuraDiscoveryDialCandidate>,
        now: Instant,
    ) {
        let waited = std::mem::take(&mut self.waiting);
        self.waiting = set
            .iter()
            .filter(|member| {
                dialable.contains_key(*member) && member.as_bytes() < self.local_node_id.as_bytes()
            })
            .map(|member| (*member, waited.get(member).copied().unwrap_or(now)))
            .collect();
    }

    /// Whether a dial to `member` still waits out [`MUTUAL_DIAL_GRACE`] at `now`.
    fn is_waiting(&self, member: &NodeId, now: Instant) -> bool {
        self.waiting
            .get(member)
            .is_some_and(|since| now.saturating_duration_since(*since) < MUTUAL_DIAL_GRACE)
    }

    /// Forgets `member` once its dial starts.
    fn release(&mut self, member: &NodeId) {
        self.waiting.remove(member);
    }
}

/// What a round's preferred pass claimed, which the ordinary pass after it
/// no longer has.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RoundClaims {
    /// Dials started, one connection slot each.
    pub(super) dials: usize,
    /// Address attempts those dials reserve in the transport.
    pub(super) address_attempts: usize,
}

/// Records an attempt in `state` for each member of `set`, highest rank
/// first, that `dialable` lists, until `room` runs out, and returns the
/// attempts with the addresses to dial and what they claimed.
///
/// A member is in `dialable` only when it is neither connected nor being
/// dialed and the discovery book holds it in no backoff. A member with a
/// lower node ID than this node's is dialed only once it has stayed in
/// `dialable` for [`MUTUAL_DIAL_GRACE`], as [`DeferredDials`] explains. The
/// pass keeps the ordinary pass's rules for each attempt: one attempt per
/// node, the per-IP backoff, the supervisor's per-IP check `accepts_ip`, and
/// discovery's transport ceiling, which leaves the dial reserve to bootstrap
/// and maintained dials. It does not apply discovery's soft cap on
/// connections: a member takes a held place, so it only needs a free
/// connection slot.
pub(super) fn admit_preferred_dials(
    state: &mut CandidateDialerState,
    set: &[NodeId],
    dialable: &HashMap<NodeId, ZakuraDiscoveryDialCandidate>,
    deferred: &mut DeferredDials,
    room: PreferredRoom,
    accepts_ip: impl Fn(IpAddr, usize) -> bool,
    now: Instant,
) -> (Vec<(DialAttemptKey, NodeAddr)>, RoundClaims) {
    deferred.refresh(set, dialable, now);
    let mut admitted = Vec::new();
    let mut claims = RoundClaims::default();
    for member in set {
        let free_connections = room.connections.saturating_sub(claims.dials);
        let free_dials = room.dials.saturating_sub(claims.dials);
        let free_attempts = room
            .address_attempts
            .saturating_sub(claims.address_attempts);
        if free_connections == 0 || free_dials == 0 || free_attempts == 0 {
            break;
        }
        let Some(candidate) = dialable.get(member) else {
            continue;
        };
        if deferred.is_waiting(member, now) {
            continue;
        }
        let Some((node_addr, reserved_ips)) =
            discovery_node_addr_with_reserved_ip_capacity(state, candidate, &accepts_ip, now)
        else {
            continue;
        };
        let attempts = reserved_ips.len();
        if attempts > free_attempts {
            continue;
        }
        let Some(attempt) = state.record_attempt(*member, reserved_ips) else {
            continue;
        };
        deferred.release(member);
        claims.dials += 1;
        claims.address_attempts += attempts;
        admitted.push((attempt, node_addr));
    }
    (admitted, claims)
}

/// What the dialer keeps between rounds for its preferred pass: the ranking
/// it reads, the dials it defers, and the attempts it has in flight.
pub(super) struct PreferredPass {
    input: PreferredGatewayInput,
    deferred: DeferredDials,
    /// The most dials, preferred or ordinary, in flight at once.
    max_concurrent_dials: usize,
    /// The attempts in flight that this pass started.
    attempts: HashSet<DialAttemptKey>,
}

impl PreferredPass {
    /// Prepares the pass for a node whose own ID is `local_node_id`, reading
    /// `ranking` (none ranks no gateway), within `max_concurrent_dials`.
    pub(super) fn new(
        ranking: Option<watch::Receiver<MinerGatewayRanking>>,
        local_node_id: NodeId,
        max_concurrent_dials: usize,
    ) -> Self {
        Self {
            input: PreferredGatewayInput::new(ranking, local_node_id),
            deferred: DeferredDials::new(local_node_id),
            max_concurrent_dials,
            attempts: HashSet::default(),
        }
    }

    /// Returns the ranked gateways, best first, as [`PreferredGatewayInput`]
    /// does.
    pub(super) fn ranked(&mut self) -> &[RankedGateway] {
        self.input.current()
    }

    /// Returns the room one round's pass may use, given the free
    /// `connections` and `address_attempts` the endpoint reports and the
    /// dials `state` already has in flight.
    pub(super) fn room(
        &self,
        state: &CandidateDialerState,
        connections: usize,
        address_attempts: usize,
    ) -> PreferredRoom {
        PreferredRoom {
            connections,
            address_attempts,
            dials: self
                .max_concurrent_dials
                .saturating_sub(state.in_flight_count()),
        }
    }

    /// Admits the round's dials as [`admit_preferred_dials`] does, keeping the
    /// grace of the members it defers for the next round.
    pub(super) fn admit(
        &mut self,
        state: &mut CandidateDialerState,
        round: &PreferredRound,
        room: PreferredRoom,
        accepts_ip: impl Fn(IpAddr, usize) -> bool,
        now: Instant,
    ) -> (Vec<(DialAttemptKey, NodeAddr)>, RoundClaims) {
        admit_preferred_dials(
            state,
            &round.set,
            &round.dialable,
            &mut self.deferred,
            room,
            accepts_ip,
            now,
        )
    }

    /// Records that this pass started `attempt`.
    pub(super) fn started(&mut self, attempt: DialAttemptKey) {
        self.attempts.insert(attempt);
    }

    /// Forgets `attempt` as settled, and returns whether this pass started it.
    pub(super) fn settled(&mut self, attempt: &DialAttemptKey) -> bool {
        self.attempts.remove(attempt)
    }

    /// Forgets every attempt, as the dialer does when it shuts down.
    pub(super) fn clear(&mut self) {
        self.attempts.clear();
    }
}

#[cfg(all(test, not(zakura_sim)))]
mod tests {
    use std::net::SocketAddr;

    use zakura_quic::{NodeAddr, NodeSecretKey};

    use super::*;
    use crate::zakura::{
        discovery::{build_discovery_handle, default_advertised_services},
        ZakuraHandshakeConfig, ZakuraPeerId,
    };

    fn node_id(byte: u8) -> NodeId {
        NodeSecretKey::from_bytes(&[byte; 32]).public()
    }

    /// A node ID made of `prefix` in its first 20 bytes and `last` in the
    /// rest, so two such IDs can share a gateway ID.
    fn raw_node_id(prefix: u8, last: u8) -> NodeId {
        let mut bytes = [last; 32];
        bytes[..20].fill(prefix);
        NodeId::from_bytes(&bytes)
    }

    fn gateway_id(node_id: NodeId) -> GatewayId {
        GatewayId::from_node_id(node_id.as_bytes())
    }

    fn gateway(node_id: NodeId, blocks: u32) -> MinerGateway {
        MinerGateway {
            gateway_id: gateway_id(node_id),
            blocks,
        }
    }

    /// The ranked entry for `node_id`'s gateway ID before it is resolved.
    fn unresolved(node_id: NodeId, blocks: u32) -> RankedGateway {
        RankedGateway::unresolved(gateway_id(node_id), blocks)
    }

    fn ranking(gateways: Vec<MinerGateway>) -> MinerGatewayRanking {
        MinerGatewayRanking {
            always_preferred: Vec::new(),
            gateways,
        }
    }

    fn ranked_ids(ranked: &[RankedGateway]) -> Vec<GatewayId> {
        ranked.iter().map(|gateway| gateway.gateway()).collect()
    }

    fn gateway_ids(node_ids: &[NodeId]) -> Vec<GatewayId> {
        node_ids.iter().copied().map(gateway_id).collect()
    }

    fn discovery_handle() -> ZakuraDiscoveryHandle {
        let (_connected_tx, connected) = watch::channel(Vec::<ZakuraPeerId>::new());
        build_discovery_handle(
            NodeSecretKey::generate(),
            None,
            Vec::new(),
            default_advertised_services(),
            &ZakuraHandshakeConfig::for_network(&zakura_chain::parameters::Network::Mainnet),
            92,
            0,
            connected,
        )
        .expect("the test discovery handle builds")
    }

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::from(([93, 184, 216, last], 8233))
    }

    #[test]
    fn ranking_orders_by_blocks_then_id_bytes() {
        let (a, b, c) = (node_id(1), node_id(2), node_id(3));
        let mut tied = [a, b];
        tied.sort_unstable_by_key(|id| *id.as_bytes());

        let ranked = rank_gateways(
            &ranking(vec![
                gateway(tied[1], 5),
                gateway(c, 9),
                gateway(tied[0], 5),
            ]),
            node_id(99),
        );

        assert_eq!(ranked_ids(&ranked), gateway_ids(&[c, tied[0], tied[1]]));
        assert_eq!(
            ranked
                .iter()
                .map(|gateway| gateway.blocks())
                .collect::<Vec<_>>(),
            vec![9, 5, 5]
        );
    }

    #[test]
    fn ranking_drops_the_local_gateway_id() {
        let local = node_id(1);
        let other = node_id(2);
        let ranked = rank_gateways(&ranking(vec![gateway(local, 50), gateway(other, 3)]), local);

        assert_eq!(ranked_ids(&ranked), gateway_ids(&[other]));
    }

    /// A gateway ID that the local node ID starts with is dropped even when
    /// the miners meant another node that shares it.
    #[test]
    fn ranking_drops_a_gateway_id_the_local_node_shares() {
        let local = raw_node_id(1, 0x00);
        let sharing = raw_node_id(1, 0xff);
        let other = raw_node_id(2, 0x00);
        let ranked = rank_gateways(
            &ranking(vec![gateway(sharing, 50), gateway(other, 3)]),
            local,
        );

        assert_eq!(ranked_ids(&ranked), gateway_ids(&[other]));
    }

    #[test]
    fn ranking_keeps_one_entry_per_id() {
        let a = node_id(1);
        let ranked = rank_gateways(
            &ranking(vec![gateway(a, 2), gateway(a, 7), gateway(a, 7)]),
            node_id(99),
        );

        assert_eq!(ranked, vec![unresolved(a, 7)]);
    }

    /// The IDs the node always prefers come first, in their order and with
    /// any block count, and the ranked gateways fill the rest without
    /// repeating them. The node's own ID is never included.
    #[test]
    fn always_preferred_ids_rank_first_and_gateways_fill_the_rest() {
        let local = node_id(1);
        let (first, second, ranked, repeated) = (node_id(2), node_id(3), node_id(4), node_id(5));
        let ranked_list = rank_gateways(
            &MinerGatewayRanking {
                always_preferred: vec![
                    gateway(first, 0),
                    gateway(local, 0),
                    gateway(repeated, 0),
                    gateway(second, 3),
                    gateway(first, 0),
                ],
                gateways: vec![
                    gateway(ranked, 9),
                    gateway(repeated, 50),
                    gateway(local, 60),
                ],
            },
            local,
        );

        assert_eq!(
            ranked_ids(&ranked_list),
            gateway_ids(&[first, repeated, second, ranked])
        );
        assert_eq!(
            ranked_list
                .iter()
                .map(|gateway| (gateway.is_always_preferred(), gateway.blocks()))
                .collect::<Vec<_>>(),
            vec![(true, 0), (true, 0), (true, 3), (false, 9)]
        );
    }

    /// The always-preferred IDs count toward the ranking's cap.
    #[test]
    fn always_preferred_ids_count_toward_the_cap() {
        let always: Vec<_> = (0..3)
            .map(|_| gateway(NodeSecretKey::generate().public(), 0))
            .collect();
        let gateways: Vec<_> = (0..300)
            .map(|blocks| gateway(NodeSecretKey::generate().public(), blocks + 1))
            .collect();
        let ranked = rank_gateways(
            &MinerGatewayRanking {
                always_preferred: always.clone(),
                gateways,
            },
            node_id(99),
        );

        assert_eq!(ranked.len(), MAX_RANKED_GATEWAYS);
        assert_eq!(
            ranked_ids(&ranked[..3]),
            always
                .iter()
                .map(|gateway| gateway.gateway_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn ranking_keeps_every_gateway_under_the_cap() {
        let gateways: Vec<_> = (1..=31).map(|byte| gateway(node_id(byte), 1)).collect();
        let ranked = rank_gateways(&ranking(gateways), node_id(99));
        assert_eq!(ranked.len(), 31);
    }

    #[test]
    fn ranking_drops_gateways_with_no_blocks() {
        let (a, b) = (node_id(1), node_id(2));
        let ranked = rank_gateways(&ranking(vec![gateway(a, 0), gateway(b, 1)]), node_id(99));
        assert_eq!(ranked_ids(&ranked), gateway_ids(&[b]));
    }

    #[test]
    fn ranking_keeps_the_best_gateways_up_to_the_cap() {
        let ids: Vec<NodeId> = (0..300)
            .map(|_| NodeSecretKey::generate().public())
            .collect();
        // Gateway `index` has `300 - index` blocks, so the list is in rank order.
        let mut gateways: Vec<_> = ids
            .iter()
            .zip((1..=300).rev())
            .map(|(node_id, blocks)| gateway(*node_id, blocks))
            .collect();
        // The local node ranks first but is dropped, so it takes no capped place.
        gateways.push(gateway(node_id(99), 1_000));
        gateways.reverse();

        let ranked = rank_gateways(&ranking(gateways), node_id(99));

        assert_eq!(ranked.len(), MAX_RANKED_GATEWAYS);
        assert_eq!(
            ranked_ids(&ranked),
            gateway_ids(&ids[..MAX_RANKED_GATEWAYS])
        );
    }

    #[test]
    fn input_converts_each_change_and_empties_when_closed() {
        let (a, b) = (node_id(1), node_id(2));
        let (sender, receiver) = watch::channel(ranking(vec![gateway(a, 1)]));
        let mut input = PreferredGatewayInput::new(Some(receiver), node_id(99));
        assert_eq!(ranked_ids(input.current()), gateway_ids(&[a]));

        sender.send_replace(ranking(vec![gateway(a, 1), gateway(b, 2)]));
        assert_eq!(ranked_ids(input.current()), gateway_ids(&[b, a]));

        drop(sender);
        assert!(input.current().is_empty());
    }

    #[test]
    fn input_without_a_channel_is_empty() {
        let mut input = PreferredGatewayInput::new(None, node_id(99));
        assert!(input.current().is_empty());
    }

    #[tokio::test]
    async fn classification_reads_registrations_dials_the_book_and_dialer_backoff() {
        let discovery = discovery_handle();
        let connected = node_id(1);
        let dialing = node_id(2);
        let dialable = node_id(3);
        let backoff = node_id(4);
        let unmatched = node_id(5);
        let connected_and_dialing = node_id(6);
        let dialer_backoff = node_id(7);
        for (node, last) in [
            (dialable, 3),
            (backoff, 4),
            (connected, 1),
            (dialer_backoff, 7),
        ] {
            discovery
                .insert_static_candidate(NodeAddr::with_addrs(node, [addr(last)]))
                .await
                .expect("the static candidate is valid");
        }
        discovery.mark_dial_attempt(&backoff).await;
        discovery.mark_dial_failure(&backoff).await;
        let now = Instant::now();
        let mut state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        for node in [dialing, connected_and_dialing] {
            state
                .record_attempt(node, vec![addr(2).ip()])
                .expect("the node has no attempt in flight");
        }
        // The book could dial this one, but its only IP is in this dialer's
        // own backoff.
        let failed = state
            .record_attempt(dialer_backoff, vec![addr(7).ip()])
            .expect("the node has no attempt in flight");
        assert!(state.complete(failed, super::super::DiscoveryDialResult::Failed, None, now));

        let ranked: Vec<_> = [
            connected,
            dialing,
            dialable,
            backoff,
            unmatched,
            connected_and_dialing,
            dialer_backoff,
        ]
        .into_iter()
        .map(|node| unresolved(node, 1))
        .collect();
        let classified = classify_gateways(
            &ranked,
            &[connected, connected_and_dialing],
            &state,
            &discovery,
            now,
        )
        .await;

        let resolutions: Vec<_> = classified
            .ranking
            .iter()
            .map(|gateway| gateway.resolution())
            .collect();
        assert_eq!(
            resolutions,
            vec![
                GatewayMatch::One((connected, GatewayStanding::Connected)),
                GatewayMatch::One((dialing, GatewayStanding::Dialing)),
                GatewayMatch::One((dialable, GatewayStanding::Dialable)),
                GatewayMatch::One((backoff, GatewayStanding::Backoff)),
                GatewayMatch::None,
                GatewayMatch::One((connected_and_dialing, GatewayStanding::Connected)),
                GatewayMatch::One((dialer_backoff, GatewayStanding::Backoff)),
            ],
            "each gateway ID resolves to the one known node that starts with it",
        );
        assert_eq!(
            classified.dialable,
            HashMap::from_iter([(
                dialable,
                ZakuraDiscoveryDialCandidate {
                    node_id: dialable,
                    direct_addrs: vec![addr(3)],
                    is_static: true,
                }
            )])
        );
    }

    /// Classifies `ranked` with nothing in flight, given the peers
    /// `registered` and the static candidates `book`.
    async fn classify_with(
        ranked: &[RankedGateway],
        registered: &[NodeId],
        book: &[NodeId],
    ) -> ClassifiedGateways {
        let discovery = discovery_handle();
        for (index, node) in book.iter().enumerate() {
            let last = u8::try_from(index + 1).expect("a test book holds few nodes");
            discovery
                .insert_static_candidate(NodeAddr::with_addrs(*node, [addr(last)]))
                .await
                .expect("the static candidate is valid");
        }
        let state =
            CandidateDialerState::new((Duration::from_secs(60), Duration::from_secs(3_600)));
        classify_gateways(ranked, registered, &state, &discovery, Instant::now()).await
    }

    #[tokio::test]
    async fn a_gateway_id_resolves_only_to_a_node_that_starts_with_it() {
        let named = raw_node_id(1, 0x00);
        let other = raw_node_id(2, 0x00);

        let classified = classify_with(&[unresolved(named, 1)], &[other], &[named]).await;

        assert_eq!(
            classified.ranking[0].resolution(),
            GatewayMatch::One((named, GatewayStanding::Dialable))
        );
        assert_eq!(
            classified.dialable.keys().copied().collect::<Vec<_>>(),
            vec![named]
        );

        let classified = classify_with(&[unresolved(named, 1)], &[other], &[other]).await;

        assert_eq!(classified.ranking[0].resolution(), GatewayMatch::None);
        assert!(classified.dialable.is_empty());
    }

    /// Two known nodes that share a ranked gateway ID leave it ambiguous,
    /// whether the book holds both or one is a registered peer, and neither
    /// is dialed.
    #[tokio::test]
    async fn a_gateway_id_two_known_nodes_share_is_ambiguous() {
        let first = raw_node_id(1, 0x00);
        let second = raw_node_id(1, 0xff);
        let ranked = [unresolved(first, 1)];

        for (registered, book) in [
            (vec![], vec![first, second]),
            (vec![first], vec![second]),
            (vec![first, second], vec![]),
        ] {
            let classified = classify_with(&ranked, &registered, &book).await;

            assert_eq!(
                classified.ranking[0].resolution(),
                GatewayMatch::Several,
                "registered {registered:?}, book {book:?}",
            );
            assert!(classified.dialable.is_empty());
        }
    }

    /// A gateway tied at the cutoff, past the first 256, resolves through the
    /// book too, so a gateway ID two booked nodes share fails closed there.
    #[tokio::test]
    async fn a_gateway_tied_at_the_cutoff_resolves_like_the_others() {
        let tied = raw_node_id(1, 0x00);
        let sharing = raw_node_id(1, 0xff);
        let mut ranked: Vec<_> = (0..MAX_RANKED_GATEWAYS)
            .map(|_| unresolved(NodeSecretKey::generate().public(), 1))
            .collect();
        ranked.push(unresolved(tied, 1));

        let classified = classify_with(&ranked, &[], &[tied, sharing]).await;

        assert_eq!(classified.ranking.len(), MAX_RANKED_GATEWAYS + 1);
        assert_eq!(
            classified.ranking[MAX_RANKED_GATEWAYS].resolution(),
            GatewayMatch::Several
        );
    }
}
