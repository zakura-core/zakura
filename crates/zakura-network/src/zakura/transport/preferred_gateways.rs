//! The mining gateways this node holds connection places for.
//!
//! Mining pools name their gateway's v2 node ID in their coinbases, and a
//! mining node ranks those IDs by how many recent blocks name them. The node
//! keeps up to [`MAX_PREFERRED_GATEWAYS`] connection places that ordinary
//! peers cannot fill, for the highest-ranked gateways it can reach.
//!
//! This module only decides how many places are held and which gateways may
//! fill them. The connection-slot checks in `handshake_counts` apply that
//! decision, and with an empty ranking they hold nothing.

use zakura_quic::NodeId;
use zakura_rt::collections::{HashMap, HashSet};

/// The most connection places a node holds for preferred gateways.
pub const MAX_PREFERRED_GATEWAYS: usize = 10;

/// The most gateways a ranking keeps. Two days of blocks can name far more
/// IDs than ten places need, and the slot checks and the dialer walk the
/// ranking, so a longer ranking keeps only its highest-ranked entries.
pub(crate) const MAX_RANKED_GATEWAYS: usize = 256;

/// What v2 networking knows about a ranked gateway.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayStanding {
    /// v2 networking has no record of the gateway.
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

/// One gateway of the ranking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RankedGateway {
    /// The gateway's v2 node ID.
    pub node: NodeId,
    /// Recent blocks that name the gateway.
    pub blocks: u32,
    /// What v2 networking knows about the gateway.
    pub standing: GatewayStanding,
}

/// A discovery snapshot prepared before taking the connection admission lock.
#[derive(Debug)]
pub struct GatewayRanking {
    candidates: Vec<RankedGateway>,
    /// Only a TLS-proven connection can lift one of these ties above the cutoff.
    tied_at_cutoff: HashMap<NodeId, RankedGateway>,
}

fn unique_gateways(ranking: impl IntoIterator<Item = RankedGateway>) -> Vec<RankedGateway> {
    let mut seen: HashSet<NodeId> = HashSet::default();
    ranking
        .into_iter()
        .filter(|gateway| seen.insert(gateway.node))
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
                    .then_with(|| a.node.as_bytes().cmp(b.node.as_bytes()))
            });
            let cutoff = candidates[MAX_RANKED_GATEWAYS - 1].blocks;
            tied_at_cutoff.extend(
                candidates
                    .drain(MAX_RANKED_GATEWAYS..)
                    .take_while(|gateway| gateway.blocks == cutoff)
                    .map(|gateway| (gateway.node, gateway)),
            );
        }
        Self {
            candidates,
            tied_at_cutoff,
        }
    }
}

impl GatewayRanking {
    /// At publication, look up only live owners among the tied overflow entries.
    /// Entries below the cutoff score cannot advance even if they authenticated.
    pub(super) fn candidates<'a>(
        &self,
        authenticated: impl Iterator<Item = &'a NodeId>,
    ) -> Vec<RankedGateway> {
        let mut candidates = self.candidates.clone();
        if !self.tied_at_cutoff.is_empty() {
            candidates.extend(
                authenticated
                    .filter_map(|node| self.tied_at_cutoff.get(node))
                    .copied(),
            );
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
    /// lowest node ID bytes.
    ///
    /// The hold is the matched gateways, up to the ceiling, so a gateway in
    /// backoff keeps a place it can call into. The set is the available
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
        let connected = |gateway: &RankedGateway| gateway.standing == GatewayStanding::Connected;
        let priority = |gateway: &RankedGateway| {
            (
                gateway.blocks,
                connected(gateway),
                connected(gateway) && incumbents.contains(&gateway.node),
            )
        };
        if ranking.len() > MAX_RANKED_GATEWAYS {
            ranking.sort_unstable_by(|a, b| {
                priority(b)
                    .cmp(&priority(a))
                    .then_with(|| a.node.as_bytes().cmp(b.node.as_bytes()))
            });
            ranking.truncate(MAX_RANKED_GATEWAYS);
        }
        // A stable sort, so equal gateways keep the order they were given.
        ranking.sort_by_key(|gateway| std::cmp::Reverse(priority(gateway)));
        let hold_limit = hold_limit.min(MAX_PREFERRED_GATEWAYS);
        let matched = ranking
            .iter()
            .filter(|gateway| gateway.standing.matched())
            .count();
        let hold = hold_limit.min(matched);
        let set = ranking
            .iter()
            .filter(|gateway| gateway.standing.available())
            .map(|gateway| gateway.node)
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

    /// Members whose connections are outside the ordinary discovery budget.
    pub(crate) fn held_places(&self) -> HeldPlaces {
        HeldPlaces {
            members: self.set.clone(),
        }
    }

    /// Records a ranked gateway as connected, including when it already owns
    /// a preferred place. Later callers then see its standing in cutoff ties.
    pub(super) fn with_connected(&self, node: &NodeId) -> Option<Self> {
        self.with_standing(node, GatewayStanding::Connected)
    }

    pub(super) fn with_standing(&self, node: &NodeId, standing: GatewayStanding) -> Option<Self> {
        let index = self
            .ranking
            .iter()
            .position(|gateway| gateway.node == *node && gateway.standing != standing)?;
        let mut ranking = self.ranking.clone();
        ranking[index].standing = standing;
        Some(Self::form(ranking, self.hold_limit))
    }

    /// The set re-formed with `node` in backoff, once it has lost its last
    /// connection slot; `None` when `node` is unranked, unmatched or
    /// already in backoff.
    ///
    /// A gateway that loses its last connection slot counts as in backoff
    /// until the next published ranking. Like a gateway whose dial failed,
    /// it keeps its held place reserved, leaves the set so the next
    /// available gateway may fill it, and may still call back in.
    pub(super) fn with_released(&self, node: &NodeId) -> Option<Self> {
        let index = self
            .ranking
            .iter()
            .position(|gateway| gateway.node == *node && gateway.standing.available())?;
        let mut ranking = self.ranking.clone();
        ranking[index].standing = GatewayStanding::Backoff;
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
