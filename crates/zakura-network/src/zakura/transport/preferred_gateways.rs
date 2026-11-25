//! The mining gateways this node holds connection places for.
//!
//! Mining pools name their gateway in their coinbases by a [`GatewayId`], the
//! first 20 bytes of its v2 node ID, and a node that prefers gateways (it
//! mines or sets `mining.miner_mesh`) ranks those IDs by how many recent
//! blocks name them. The node keeps up to
//! [`MAX_PREFERRED_GATEWAYS`] connection places that ordinary peers cannot
//! fill, for the highest-ranked gateways it can reach.
//!
//! A ranked gateway ID names whichever node v2 networking knows with an ID
//! that starts with it. When it knows two or more, it cannot tell which one
//! the miners named, so it prefers none of them: the gateway is ambiguous.
//!
//! A node that adds a key has a new node ID, which starts with other bytes,
//! while blocks go on naming the gateway by the earlier one. A connected node
//! therefore also answers for the gateway ID of each earlier ID its
//! handshake proved ([`ProvenNode`]), and keeps the gateway's place under the
//! ID it has now. Two connected nodes that answer for one gateway make it
//! ambiguous like two nodes that share its ID, so one key holder never fills
//! more than the one place. A gateway stays ambiguous until the next ranking
//! is applied, which resolves it again from the nodes connected then.
//!
//! This module only decides how many places are held and which gateways may
//! fill them. The connection-slot checks in `handshake_counts` apply that
//! decision, and with an empty ranking they hold nothing.

use zakura_chain::miner_mesh::GatewayId;
use zakura_quic::NodeId;
use zakura_rt::collections::{HashMap, HashSet};

use super::{GatewayMatch, ProvenNode};

/// The most connection places a node holds for preferred gateways.
pub const MAX_PREFERRED_GATEWAYS: usize = 10;

/// The most gateways a ranking keeps. Two days of blocks can name far more
/// IDs than ten places need, and the slot checks and the dialer walk the
/// ranking, so a longer ranking keeps only its highest-ranked entries.
pub(crate) const MAX_RANKED_GATEWAYS: usize = 256;

/// What v2 networking knows about reaching the one node a ranked gateway ID
/// names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayStanding {
    /// v2 networking has no connection or usable dial address for the node.
    Unmatched,
    /// Known, but waiting out a failed dial before the next one.
    Backoff,
    /// Known, and may be dialed now.
    Dialable,
    /// A dial to it is in flight.
    Dialing,
    /// Connected to this node.
    ///
    /// When the named node loses its last slot, another connected owner
    /// can take its place. Without one, an available gateway enters [`Self::Backoff`].
    Connected,
}

impl GatewayStanding {
    /// Known to v2 networking, so it counts towards the hold.
    fn matched(self) -> bool {
        self != Self::Unmatched
    }

    /// Reachable now, so it may fill a held place.
    fn available(self) -> bool {
        matches!(self, Self::Dialable | Self::Dialing | Self::Connected)
    }
}

/// Which node a ranked gateway ID names among the nodes v2 networking knows,
/// those whose node IDs start with the gateway ID, and how it can reach that
/// node. With two or more such nodes it cannot tell which one the miners
/// named, so it prefers none of them.
pub type GatewayResolution = GatewayMatch<(NodeId, GatewayStanding)>;

/// Where a ranked gateway stands against the places the node holds: its
/// [`GatewayResolution`] read together with the preferred set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayPlace {
    /// In the preferred set, so it may fill a held place.
    Preferred,
    /// Known and reachable now (dialable, being dialed or connected), but
    /// ranked below the gateways that fill the held places.
    Standby,
    /// Known, but waiting out a failed dial or a lost connection. It still
    /// counts toward the shared hold while other available gateways may fill it.
    Backoff,
    /// v2 networking has no connection or usable dial address for it, so it takes no place.
    Unmatched,
    /// v2 networking knows two or more nodes with its gateway ID, so it takes
    /// no place.
    Ambiguous,
}

/// One gateway of the ranking.
///
/// A gateway that names a node names one whose node ID starts with its
/// gateway ID, or a connected node whose handshake proved an earlier ID
/// that does. The constructors derive the gateway ID from the node, and the
/// updates change only the standing or check what the node answers for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RankedGateway {
    gateway: GatewayId,
    blocks: u32,
    resolution: GatewayResolution,
}

impl RankedGateway {
    /// A gateway ID named in `blocks` recent blocks, before any node with it
    /// is known.
    pub fn unresolved(gateway: GatewayId, blocks: u32) -> Self {
        Self {
            gateway,
            blocks,
            resolution: GatewayMatch::None,
        }
    }

    /// The gateway that `node`'s gateway ID names, named in `blocks` recent
    /// blocks, resolved to `node` and reachable as `standing` says.
    pub fn resolved(node: NodeId, blocks: u32, standing: GatewayStanding) -> Self {
        Self {
            gateway: GatewayId::from_node_id(node.as_bytes()),
            blocks,
            resolution: GatewayMatch::One((node, standing)),
        }
    }

    /// The ID the blocks name the gateway by.
    pub fn gateway(&self) -> GatewayId {
        self.gateway
    }

    /// Recent blocks that name the gateway.
    pub fn blocks(&self) -> u32 {
        self.blocks
    }

    /// The node the gateway ID names, and how v2 networking can reach it.
    pub fn resolution(&self) -> GatewayResolution {
        self.resolution
    }

    /// The node the gateway ID names, if exactly one is known.
    pub(crate) fn node(&self) -> Option<NodeId> {
        match self.resolution {
            GatewayMatch::One((node, _)) => Some(node),
            GatewayMatch::None | GatewayMatch::Several => None,
        }
    }

    /// The known nodes with this gateway ID, without how to reach them.
    fn nodes(&self) -> GatewayMatch<NodeId> {
        self.resolution.map(|(node, _)| node)
    }

    /// Returns this gateway once two or more known nodes have its gateway
    /// ID: it names none of them.
    pub(crate) fn into_ambiguous(mut self) -> Self {
        self.resolution = GatewayMatch::Several;
        self
    }

    /// Returns this gateway with the node it names reachable as `standing`.
    /// A gateway that names no node is returned unchanged.
    pub(crate) fn with_standing(mut self, standing: GatewayStanding) -> Self {
        if let GatewayMatch::One((node, _)) = self.resolution {
            self.resolution = GatewayMatch::One((node, standing));
        }
        self
    }

    /// Whether the gateway counts toward the hold: it names one node that
    /// v2 networking knows how to reach.
    fn is_matched(&self) -> bool {
        matches!(self.resolution, GatewayMatch::One((_, standing)) if standing.matched())
    }

    /// The node the gateway names, when it is reachable now and so may fill
    /// a held place.
    fn available_node(&self) -> Option<NodeId> {
        match self.resolution {
            GatewayMatch::One((node, standing)) if standing.available() => Some(node),
            _ => None,
        }
    }

    /// The node the gateway names, when it is connected.
    fn connected_node(&self) -> Option<NodeId> {
        match self.resolution {
            GatewayMatch::One((node, GatewayStanding::Connected)) => Some(node),
            _ => None,
        }
    }

    /// Returns this gateway resolved against `owners`: the connected nodes
    /// that TLS has proved and that answer for its gateway ID, under the ID
    /// they have now or an earlier one.
    ///
    /// With no owner the gateway is unchanged, and with two or more it is
    /// ambiguous. One owner connects it:
    /// - a gateway that named no node, or the owner, names the owner as
    ///   connected;
    /// - so does a gateway that named the same node by another ID, which is
    ///   an ID the owner had before or a node that an earlier connection
    ///   resolved it to. While that ID still `holds_slot`, for a dial that
    ///   no handshake has proved, the gateway is unchanged and the dial
    ///   settles it;
    /// - a gateway that named another node whose ID starts with the gateway
    ///   ID becomes ambiguous.
    fn owned_by(
        mut self,
        owners: GatewayMatch<ProvenNode<'_>>,
        holds_slot: impl Fn(&NodeId) -> bool,
    ) -> Self {
        let owner = match owners {
            GatewayMatch::None => return self,
            GatewayMatch::Several => return self.into_ambiguous(),
            GatewayMatch::One(owner) => owner,
        };
        let named = match self.nodes() {
            GatewayMatch::None => None,
            GatewayMatch::One(named) => Some(named),
            GatewayMatch::Several => return self,
        };
        if let Some(named) = named.filter(|named| *named != owner.id()) {
            // A named node with another prefix got here through a proof,
            // so only the gateway's key holder could have put it there.
            let same_key_holder = owner.had(&named) || !self.gateway.is_prefix_of(named.as_bytes());
            if !same_key_holder {
                return self.into_ambiguous();
            }
            if holds_slot(&named) {
                return self;
            }
        }
        self.resolution = GatewayMatch::One((owner.id(), GatewayStanding::Connected));
        self
    }
}

/// Each gateway of `ranking` that `owners` change, by its index, resolved as
/// [`RankedGateway::owned_by`] says.
///
/// `owners` are all the connected nodes TLS has proved. An owner answers for
/// the gateway that its node ID names and for the gateway that each of its
/// earlier IDs names. `holds_slot` says whether a node ID holds a connection
/// slot.
fn owner_updates<'a>(
    ranking: &[RankedGateway],
    owners: impl IntoIterator<Item = ProvenNode<'a>>,
    holds_slot: impl Fn(&NodeId) -> bool,
) -> Vec<(usize, RankedGateway)> {
    // A node that does not mine ranks no gateway, and every authenticated
    // claim comes through here.
    if ranking.is_empty() {
        return Vec::new();
    }
    let by_gateway: HashMap<GatewayId, usize> = ranking
        .iter()
        .enumerate()
        .map(|(index, gateway)| (gateway.gateway, index))
        .collect();
    let mut owned: HashMap<usize, GatewayMatch<ProvenNode<'a>>> = HashMap::default();
    for owner in owners {
        for id in owner.ids() {
            if let Some(&index) = by_gateway.get(&GatewayId::from_node_id(id.as_bytes())) {
                let known = owned.entry(index).or_insert(GatewayMatch::None);
                *known = known.with(owner);
            }
        }
    }
    owned
        .into_iter()
        .map(|(index, owners)| (index, ranking[index].owned_by(owners, &holds_slot)))
        .filter(|(index, resolved)| ranking[*index] != *resolved)
        .collect()
}

/// A discovery snapshot prepared before taking the connection admission lock.
#[derive(Debug)]
pub struct GatewayRanking {
    candidates: Vec<RankedGateway>,
    /// Only a TLS-proven connection can lift one of these ties above the cutoff.
    tied_at_cutoff: HashMap<GatewayId, RankedGateway>,
}

fn unique_gateways(ranking: impl IntoIterator<Item = RankedGateway>) -> Vec<RankedGateway> {
    let mut seen: HashSet<GatewayId> = HashSet::default();
    ranking
        .into_iter()
        .filter(|gateway| seen.insert(gateway.gateway))
        .collect()
}

impl FromIterator<RankedGateway> for GatewayRanking {
    fn from_iter<T: IntoIterator<Item = RankedGateway>>(ranking: T) -> Self {
        let mut candidates = unique_gateways(ranking);
        let mut tied_at_cutoff = HashMap::default();
        if candidates.len() > MAX_RANKED_GATEWAYS {
            // Ignore the snapshot's connection status. Only the admission lock can
            // tell which of the tied identities still owns an authenticated slot.
            candidates.sort_unstable_by(|a, b| {
                b.blocks
                    .cmp(&a.blocks)
                    .then_with(|| a.gateway.cmp(&b.gateway))
            });
            let cutoff = candidates[MAX_RANKED_GATEWAYS - 1].blocks;
            tied_at_cutoff.extend(
                candidates
                    .drain(MAX_RANKED_GATEWAYS..)
                    .take_while(|gateway| gateway.blocks == cutoff)
                    .map(|gateway| (gateway.gateway, gateway)),
            );
        }
        Self {
            candidates,
            tied_at_cutoff,
        }
    }
}

impl GatewayRanking {
    /// At publication, look up only live owners among the tied overflow entries:
    /// a tied entry joins, once, when an authenticated owner answers for its
    /// gateway ID and the entry names no node, that owner, or an ID the owner
    /// had before. Publication then resolves it
    /// with the owners like every other gateway. Entries below the cutoff score
    /// cannot advance even if they authenticated.
    pub(super) fn candidates<'a>(
        &self,
        owners: impl Iterator<Item = ProvenNode<'a>>,
    ) -> Vec<RankedGateway> {
        let mut candidates = self.candidates.clone();
        if !self.tied_at_cutoff.is_empty() {
            let mut joined: HashSet<GatewayId> = HashSet::default();
            let answered = owners.flat_map(|owner| owner.ids().map(move |id| (owner, id)));
            candidates.extend(answered.filter_map(|(owner, id)| {
                self.tied_at_cutoff
                    .get(&GatewayId::from_node_id(id.as_bytes()))
                    .filter(|gateway| match gateway.nodes() {
                        GatewayMatch::None => true,
                        GatewayMatch::One(named) => named == owner.id() || owner.had(&named),
                        GatewayMatch::Several => false,
                    })
                    .filter(|gateway| joined.insert(gateway.gateway))
                    .copied()
            }));
        }
        candidates
    }

    /// Resolves `candidates` against `owners`, all the connected nodes TLS
    /// has proved, as [`RankedGateway::owned_by`] says. An owner that
    /// connected after discovery's snapshot resolves the gateways it answers
    /// for, tied at the cutoff or not. `holds_slot` says whether a node ID
    /// holds a connection slot.
    pub(super) fn connect_owners<'a>(
        candidates: &mut [RankedGateway],
        owners: impl Iterator<Item = ProvenNode<'a>>,
        holds_slot: impl Fn(&NodeId) -> bool,
    ) {
        for (index, resolved) in owner_updates(candidates, owners, holds_slot) {
            candidates[index] = resolved;
        }
    }
}

/// The ranked gateways, how many connection places the node holds for them,
/// and the gateways that may fill those places: the preferred set.
///
/// The default is an empty ranking, which holds no place.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreferredGateways {
    /// Every ranked gateway once, in rank order.
    ranking: Vec<RankedGateway>,
    /// The hold's ceiling, at most [`MAX_PREFERRED_GATEWAYS`].
    hold_limit: usize,
    /// The places held: the ceiling, or the matched gateways when fewer.
    hold: usize,
    /// The available gateways that fill the held places, in rank order.
    set: Vec<NodeId>,
    /// Rank-order choices to retry when an ordinary inbound place frees.
    deferred: Option<Vec<NodeId>>,
}

impl PreferredGateways {
    /// Forms the preferred set from `ranking`, with at most `hold_limit`
    /// additional places funded for gateways.
    ///
    /// Gateways are ranked by block count, highest first. Within one count a
    /// connected gateway comes first, so a tie at the cutoff keeps the
    /// gateway already connected from round to round; other ties keep the
    /// order of `ranking`. A gateway listed twice keeps its first entry.
    ///
    /// Of a ranking longer than [`MAX_RANKED_GATEWAYS`], only that many are
    /// kept: the highest block counts, then connected gateways, then the
    /// lowest gateway ID bytes.
    ///
    /// The hold is the matched gateways, up to the ceiling, so a gateway in
    /// backoff still counts toward the shared hold. The set is the available
    /// gateways, in rank order, up to the hold. Gateways that name one node
    /// count once in both, so a node that several gateway IDs name, as it is
    /// while blocks name it by an earlier node ID and its new one, fills one
    /// place.
    pub fn form(ranking: impl IntoIterator<Item = RankedGateway>, hold_limit: usize) -> Self {
        Self::form_with_incumbents(unique_gateways(ranking), hold_limit, &[])
    }

    /// When two authenticated gateways tie, keep a gateway already in a preferred place.
    pub(super) fn form_with_incumbents(
        mut ranking: Vec<RankedGateway>,
        hold_limit: usize,
        incumbents: &[NodeId],
    ) -> Self {
        let priority = |gateway: &RankedGateway| {
            let connected = gateway.connected_node();
            (
                gateway.blocks,
                connected.is_some(),
                connected.is_some_and(|node| incumbents.contains(&node)),
            )
        };
        if ranking.len() > MAX_RANKED_GATEWAYS {
            ranking.sort_unstable_by(|a, b| {
                priority(b)
                    .cmp(&priority(a))
                    .then_with(|| a.gateway.cmp(&b.gateway))
            });
            ranking.truncate(MAX_RANKED_GATEWAYS);
        }
        // A stable sort, so equal gateways keep the order they were given.
        ranking.sort_by_key(|gateway| std::cmp::Reverse(priority(gateway)));
        let hold_limit = hold_limit.min(MAX_PREFERRED_GATEWAYS);
        let matched: HashSet<NodeId> = ranking
            .iter()
            .filter(|gateway| gateway.is_matched())
            .filter_map(RankedGateway::node)
            .collect();
        let hold = hold_limit.min(matched.len());
        let set = Self::members(&ranking, hold);
        Self {
            ranking,
            hold_limit,
            hold,
            set,
            deferred: None,
        }
    }

    /// Lets the next ranked gateway take a place while a dialable member waits
    /// for its rollback probe. A waiting gateway still contributes to the hold.
    pub(super) fn defer_dials(&mut self, waiting: impl Fn(NodeId) -> bool) -> bool {
        let mut changed = false;
        for gateway in &mut self.ranking {
            if let GatewayMatch::One((node, GatewayStanding::Dialable)) = gateway.resolution() {
                if waiting(node) {
                    *gateway = gateway.with_standing(GatewayStanding::Backoff);
                    changed = true;
                }
            }
        }
        if changed {
            // Only dialable entries moved to backoff. The ordering, including
            // connected tie breakers, and the number of matched gateways stay.
            self.set = Self::members(&self.ranking, self.hold);
            self.deferred = None;
        }
        changed
    }

    /// Retains eligible inbound incumbents when moving them to ordinary slots
    /// would exceed the inbound share. The ranking and TLS ownership stay current.
    pub(super) fn preserve_inbound_places(
        &mut self,
        incumbents: &[NodeId],
        required: usize,
        inbound: impl Fn(&NodeId) -> bool,
    ) {
        if required == 0 {
            return;
        }
        let reserved = self.set.iter().filter(|node| inbound(node)).count();
        let mut needed = required.saturating_sub(reserved);
        if needed == 0 {
            return;
        }
        let desired = self.set.clone();
        // Prefer the highest-ranked eligible incumbent and displace the
        // lowest-ranked member whose place protects no inbound connection.
        for node in self
            .ranking
            .iter()
            .filter_map(RankedGateway::available_node)
        {
            if !incumbents.contains(&node) || self.set.contains(&node) || !inbound(&node) {
                continue;
            }
            let Some(index) = self.set.iter().rposition(|member| !inbound(member)) else {
                break;
            };
            self.set[index] = node;
            needed -= 1;
            if needed == 0 {
                break;
            }
        }
        if self.set != desired {
            self.set.sort_by_key(|node| {
                self.ranking
                    .iter()
                    .position(|g| g.available_node() == Some(*node))
            });
            self.deferred = Some(desired);
        }
    }

    /// Restores the desired members and returns the incumbents to check
    /// again after a release. An ordinary release usually has nothing to do.
    pub(super) fn retry_deferred(&mut self) -> Option<Vec<NodeId>> {
        let desired = self.deferred.take()?;
        Some(std::mem::replace(&mut self.set, desired))
    }

    /// The nodes of the available gateways in `ranking`, in its order and
    /// each once, up to `hold` of them.
    fn members(ranking: &[RankedGateway], hold: usize) -> Vec<NodeId> {
        let mut set = Vec::with_capacity(hold);
        for node in ranking.iter().filter_map(RankedGateway::available_node) {
            if set.len() == hold {
                break;
            }
            if !set.contains(&node) {
                set.push(node);
            }
        }
        set
    }

    /// The connection places held for the preferred set.
    pub fn hold(&self) -> usize {
        self.hold
    }

    /// How many ranked gateways are ambiguous. A connection can make a
    /// gateway ambiguous and only a new ranking resolves it again, so a
    /// higher count after a proof means the proof disputed a gateway.
    pub(super) fn ambiguous(&self) -> usize {
        self.ranking
            .iter()
            .filter(|gateway| gateway.resolution == GatewayMatch::Several)
            .count()
    }

    /// The preferred set, in rank order, each node once. It never has more
    /// members than [`Self::hold`].
    pub fn set(&self) -> &[NodeId] {
        &self.set
    }

    /// Whether `node` is in the preferred set.
    pub fn contains(&self, node: &NodeId) -> bool {
        self.set.contains(node)
    }

    /// Every ranked gateway once, in rank order, with its place. A ranking
    /// keeps at most 256 gateways.
    pub(crate) fn places(&self) -> impl Iterator<Item = (&RankedGateway, GatewayPlace)> + '_ {
        self.ranking.iter().map(|gateway| {
            let place = match gateway.resolution {
                GatewayMatch::None | GatewayMatch::One((_, GatewayStanding::Unmatched)) => {
                    GatewayPlace::Unmatched
                }
                GatewayMatch::Several => GatewayPlace::Ambiguous,
                GatewayMatch::One((_, GatewayStanding::Backoff)) => GatewayPlace::Backoff,
                GatewayMatch::One((
                    node,
                    GatewayStanding::Dialable
                    | GatewayStanding::Dialing
                    | GatewayStanding::Connected,
                )) => {
                    if self.contains(&node) {
                        GatewayPlace::Preferred
                    } else {
                        GatewayPlace::Standby
                    }
                }
            };
            (gateway, place)
        })
    }

    /// Members whose connections are outside the ordinary discovery budget.
    pub(crate) fn held_places(&self) -> HeldPlaces {
        HeldPlaces {
            members: self.set.clone(),
        }
    }

    /// The set re-formed once connections have proved `owners`, all the
    /// connected nodes TLS has proved; `None` when nothing changes.
    ///
    /// This is where a connection resolves a gateway: each gateway an owner
    /// answers for, under its ID now or an earlier one, becomes connected as
    /// that owner, or ambiguous, as [`RankedGateway::owned_by`] says.
    /// `holds_slot` says whether a node ID holds a connection slot. It
    /// applies when the gateway already owns a preferred place too, so
    /// later callers see its standing in cutoff ties.
    pub(super) fn with_owners<'a>(
        &self,
        owners: impl IntoIterator<Item = ProvenNode<'a>>,
        holds_slot: impl Fn(&NodeId) -> bool,
    ) -> Option<Self> {
        let updates = owner_updates(&self.ranking, owners, holds_slot);
        if updates.is_empty() {
            return None;
        }
        let mut ranking = self.ranking.clone();
        for (index, resolved) in updates {
            ranking[index] = resolved;
        }
        Some(Self::form(ranking, self.hold_limit))
    }

    /// The set re-formed with `node` being dialed, once it keeps only an
    /// outbound slot that no handshake has proved; `None` when no gateway
    /// already names `node`, or it is already being dialed.
    pub(super) fn with_dialing(&self, node: &NodeId) -> Option<Self> {
        self.with_standing_of(node, GatewayStanding::Dialing, |standing| {
            standing != GatewayStanding::Dialing
        })
    }

    /// Recounts a gateway after its named node releases its last slot. A
    /// remaining owner can now take a place that was held by an in-flight
    /// dial. An available gateway without an owner goes into backoff.
    pub(super) fn with_released<'a>(
        &self,
        node: &NodeId,
        owners: impl Iterator<Item = ProvenNode<'a>>,
        holds_slot: impl Fn(&NodeId) -> bool,
    ) -> Option<Self> {
        if !self
            .ranking
            .iter()
            .any(|gateway| gateway.node() == Some(*node))
        {
            return None;
        }
        let mut ranking = self.ranking.clone();
        for gateway in &mut ranking {
            if gateway.available_node() == Some(*node) {
                *gateway = gateway.with_standing(GatewayStanding::Backoff);
            }
        }
        GatewayRanking::connect_owners(&mut ranking, owners, holds_slot);
        (ranking != self.ranking)
            .then(|| Self::form_with_incumbents(ranking, self.hold_limit, &self.set))
    }

    /// The set re-formed with every gateway that already names `node` in
    /// `standing`, when its current standing passes `applies`. `None` when
    /// that changes nothing.
    fn with_standing_of(
        &self,
        node: &NodeId,
        standing: GatewayStanding,
        applies: impl Fn(GatewayStanding) -> bool,
    ) -> Option<Self> {
        let names_node = |gateway: &RankedGateway| {
            matches!(gateway.resolution, GatewayMatch::One((named, current))
                if named == *node && applies(current))
        };
        if !self.ranking.iter().any(names_node) {
            return None;
        }
        let ranking: Vec<_> = self
            .ranking
            .iter()
            .map(|gateway| {
                if names_node(gateway) {
                    gateway.with_standing(standing)
                } else {
                    *gateway
                }
            })
            .collect();
        Some(Self::form(ranking, self.hold_limit))
    }
}

/// Gateway identities excluded from ordinary discovery occupancy.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HeldPlaces {
    /// The preferred set.
    pub(crate) members: Vec<NodeId>,
}

impl HeldPlaces {
    /// Whether the node ID `id`, as raw bytes, is a member.
    pub(crate) fn contains_bytes(&self, id: &[u8]) -> bool {
        self.members.iter().any(|member| member.as_bytes() == id)
    }
}

#[cfg(test)]
mod tests;
