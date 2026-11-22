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
use zakura_rt::collections::HashSet;

use super::inbound_share;

/// The most connection places a node holds for preferred gateways.
pub const MAX_PREFERRED_GATEWAYS: usize = 10;

/// The most gateways a ranking keeps. Two days of blocks can name far more
/// IDs than ten places need, and the slot checks and the dialer walk the
/// ranking, so a longer ranking keeps only its highest-ranked entries.
pub(crate) const MAX_RANKED_GATEWAYS: usize = 256;

/// The places a node with `max_connections` slots may hold for preferred
/// gateways while it keeps `bootstrap_headroom` slots for its bootstrap
/// peers: what the dial quarter of the limit leaves beside the bootstrap
/// places, and at most [`MAX_PREFERRED_GATEWAYS`].
///
/// The inbound share is untouched, so a small limit shrinks the hold, never
/// the places inbound peers may take.
pub fn preferred_hold_limit(max_connections: usize, bootstrap_headroom: usize) -> usize {
    let dial_quarter = max_connections.saturating_sub(inbound_share(max_connections));
    dial_quarter
        .saturating_sub(bootstrap_headroom)
        .min(MAX_PREFERRED_GATEWAYS)
}

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
    /// places (see [`preferred_hold_limit`]).
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
        let connected = |gateway: &RankedGateway| gateway.standing == GatewayStanding::Connected;
        let mut seen: HashSet<NodeId> = HashSet::default();
        let mut ranking: Vec<RankedGateway> = ranking
            .into_iter()
            .filter(|gateway| seen.insert(gateway.node))
            .collect();
        if ranking.len() > MAX_RANKED_GATEWAYS {
            ranking.sort_unstable_by(|a, b| {
                b.blocks
                    .cmp(&a.blocks)
                    .then_with(|| connected(b).cmp(&connected(a)))
                    .then_with(|| a.node.as_bytes().cmp(b.node.as_bytes()))
            });
            ranking.truncate(MAX_RANKED_GATEWAYS);
        }
        // A stable sort, so equal gateways keep the order they were given.
        ranking.sort_by(|a, b| {
            b.blocks
                .cmp(&a.blocks)
                .then_with(|| connected(b).cmp(&connected(a)))
        });
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

    /// The hold and the set, the part discovery needs.
    pub(crate) fn held_places(&self) -> HeldPlaces {
        HeldPlaces {
            hold: self.hold,
            members: self.set.clone(),
        }
    }

    /// The set re-formed as if the ranked gateway `node` were connected,
    /// when that brings `node` into the set; `None` when `node` is
    /// unranked, already a member, or still outranked.
    ///
    /// A gateway that calls in is connected, whatever its backoff or
    /// whether v2 networking knew it, so it may take the place it ranks for.
    /// A member it displaces stays connected, and its slot counts as
    /// ordinary from then on.
    pub(super) fn with_caller(&self, node: &NodeId) -> Option<Self> {
        if self.contains(node) {
            return None;
        }
        let index = self
            .ranking
            .iter()
            .position(|gateway| gateway.node == *node)?;
        // With every place filled the hold can't grow, and a caller below
        // the last member's count can't pass it, since being connected only
        // reorders gateways of equal count.
        if self.set.len() == self.hold_limit {
            let last = self.set.last()?;
            // Every member is ranked, because the set is formed from the
            // ranking, so the last member's count is always found.
            let outranked = self
                .ranking
                .iter()
                .find(|gateway| gateway.node == *last)
                .is_some_and(|last| self.ranking[index].blocks < last.blocks);
            if outranked {
                return None;
            }
        }
        let mut ranking = self.ranking.clone();
        ranking[index].standing = GatewayStanding::Connected;
        let formed = Self::form(ranking, self.hold_limit);
        formed.contains(node).then_some(formed)
    }
}

/// The connection places held for preferred gateways and the gateways that
/// fill them: what discovery needs to keep its ordinary dials out of them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HeldPlaces {
    /// The places held.
    pub(crate) hold: usize,
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
