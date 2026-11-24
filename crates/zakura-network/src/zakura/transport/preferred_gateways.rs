//! The mining gateways this node holds connection places for.
//!
//! Mining pools name their gateway in their coinbases by a [`GatewayId`], the
//! first 20 bytes of its v2 node ID, and a mining node ranks those IDs by how
//! many recent blocks name them. The node keeps up to
//! [`MAX_PREFERRED_GATEWAYS`] connection places that ordinary peers cannot
//! fill, for the highest-ranked gateways it can reach.
//!
//! A ranked gateway ID names whichever node v2 networking knows with an ID
//! that starts with it. When it knows two or more, it cannot tell which one
//! the miners named, so it prefers none of them: the gateway is ambiguous.
//!
//! This module only decides how many places are held and which gateways may
//! fill them. The connection-slot checks in `handshake_counts` apply that
//! decision, and with an empty ranking they hold nothing.

use zakura_chain::miner_mesh::GatewayId;
use zakura_quic::NodeId;

use super::GatewayMatch;
use zakura_rt::collections::{HashMap, HashSet};

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
    /// Whatever its standing, a gateway that loses its last connection slot
    /// counts as in [`Self::Backoff`] until the next published ranking.
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
/// A gateway that names a node always names one whose node ID starts with
/// its gateway ID: the constructors derive the gateway ID from that node,
/// and the updates change only the standing or check the prefix.
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

    /// Returns this gateway once a connection has proved that it reaches
    /// `node`.
    ///
    /// When `node`'s ID starts with this gateway ID, a gateway that named no
    /// node, or `node`, names `node` as connected, and one that named
    /// another node becomes ambiguous. A node with another gateway ID changes
    /// nothing.
    pub(crate) fn connected_as(mut self, node: NodeId) -> Self {
        if !self.gateway.is_prefix_of(node.as_bytes()) {
            return self;
        }
        self.resolution = match self.resolution {
            GatewayMatch::None => GatewayMatch::One((node, GatewayStanding::Connected)),
            GatewayMatch::One((known, _)) if known == node => {
                GatewayMatch::One((node, GatewayStanding::Connected))
            }
            GatewayMatch::One(_) | GatewayMatch::Several => GatewayMatch::Several,
        };
        self
    }
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
    /// a tied entry joins, once, when an authenticated owner has its gateway ID
    /// and the entry names no node or that owner. Publication then resolves it
    /// with the owners like every other gateway. Entries below the cutoff score
    /// cannot advance even if they authenticated.
    pub(super) fn candidates<'a>(
        &self,
        authenticated: impl Iterator<Item = &'a NodeId>,
    ) -> Vec<RankedGateway> {
        let mut candidates = self.candidates.clone();
        if !self.tied_at_cutoff.is_empty() {
            let mut joined: HashSet<GatewayId> = HashSet::default();
            candidates.extend(authenticated.filter_map(|node| {
                self.tied_at_cutoff
                    .get(&GatewayId::from_node_id(node.as_bytes()))
                    .filter(|gateway| match gateway.resolution {
                        GatewayMatch::None => true,
                        GatewayMatch::One((named, _)) => named == *node,
                        GatewayMatch::Several => false,
                    })
                    .filter(|gateway| joined.insert(gateway.gateway))
                    .copied()
            }));
        }
        candidates
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
    /// gateways, in rank order, up to the hold.
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
        let matched = ranking
            .iter()
            .filter(|gateway| gateway.is_matched())
            .count();
        let hold = hold_limit.min(matched);
        let set = ranking
            .iter()
            .filter_map(RankedGateway::available_node)
            .take(hold)
            .collect();
        Self {
            ranking,
            hold_limit,
            hold,
            set,
        }
    }

    /// The connection places held for the preferred set.
    pub fn hold(&self) -> usize {
        self.hold
    }

    /// The preferred set, in rank order. It never has more members than
    /// [`Self::hold`].
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

    /// The set re-formed once a connection has proved that it reaches
    /// `node`; `None` when nothing changes.
    ///
    /// This is where a connection resolves a gateway: the gateway that
    /// `node`'s gateway ID names becomes connected as `node`, or ambiguous
    /// when it named another node, as [`RankedGateway::connected_as`] says.
    /// It applies when the gateway already owns a preferred place too, so
    /// later callers see its standing in cutoff ties.
    pub(super) fn with_connected(&self, node: &NodeId) -> Option<Self> {
        let gateway_id = GatewayId::from_node_id(node.as_bytes());
        let index = self
            .ranking
            .iter()
            .position(|gateway| gateway.gateway == gateway_id)?;
        let updated = self.ranking[index].connected_as(*node);
        self.with_updated(index, updated)
    }

    /// The set re-formed with `node` being dialed, once it keeps only an
    /// outbound slot that no handshake has proved; `None` when no gateway
    /// already names `node`, or it is already being dialed.
    pub(super) fn with_dialing(&self, node: &NodeId) -> Option<Self> {
        self.with_standing_of(node, GatewayStanding::Dialing, |standing| {
            standing != GatewayStanding::Dialing
        })
    }

    /// The set re-formed with `node` in backoff, once it has lost its last
    /// connection slot; `None` when no gateway names `node`, or it is
    /// unmatched or already in backoff.
    ///
    /// A gateway that loses its last connection slot counts as in backoff
    /// until the next published ranking. Like a gateway whose dial failed,
    /// it still counts toward the hold and leaves the set so the next
    /// available gateway may fill it, and may still call back in.
    pub(super) fn with_released(&self, node: &NodeId) -> Option<Self> {
        self.with_standing_of(node, GatewayStanding::Backoff, GatewayStanding::available)
    }

    /// The set re-formed with the gateway that already names `node` in
    /// `standing`, when its current standing passes `applies`.
    fn with_standing_of(
        &self,
        node: &NodeId,
        standing: GatewayStanding,
        applies: impl Fn(GatewayStanding) -> bool,
    ) -> Option<Self> {
        let index = self.ranking.iter().position(|gateway| {
            matches!(gateway.resolution, GatewayMatch::One((named, current))
                if named == *node && applies(current))
        })?;
        let updated = self.ranking[index].with_standing(standing);
        self.with_updated(index, updated)
    }

    /// The set re-formed with the gateway at `index` replaced by `updated`;
    /// `None` when that changes nothing.
    fn with_updated(&self, index: usize, updated: RankedGateway) -> Option<Self> {
        if self.ranking[index] == updated {
            return None;
        }
        let mut ranking = self.ranking.clone();
        ranking[index] = updated;
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
