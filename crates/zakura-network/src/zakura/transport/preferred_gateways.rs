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
    /// v2 networking has no connection or usable dial address for the gateway.
    Unmatched,
    /// v2 networking knows two or more nodes with the gateway ID, so it
    /// prefers none of them.
    Ambiguous,
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
    /// Known to v2 networking as one node, so it counts towards the hold.
    fn matched(self) -> bool {
        !matches!(self, Self::Unmatched | Self::Ambiguous)
    }

    /// Reachable now, so it may fill a held place.
    fn available(self) -> bool {
        matches!(self, Self::Dialable | Self::Dialing | Self::Connected)
    }
}

/// Where a ranked gateway stands against the places the node holds: its
/// [`GatewayStanding`] read together with the preferred set.
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RankedGateway {
    /// The ID the blocks name the gateway by.
    pub gateway: GatewayId,
    /// The v2 node ID of the one node v2 networking knows with the gateway
    /// ID, or `None` while it knows none, or two or more.
    ///
    /// A gateway that is dialable, being dialed, connected or in backoff
    /// always has one.
    pub node: Option<NodeId>,
    /// Recent blocks that name the gateway.
    pub blocks: u32,
    /// What v2 networking knows about the gateway.
    pub standing: GatewayStanding,
}

impl RankedGateway {
    /// Returns this gateway once v2 networking knows `node`, whose ID starts
    /// with this gateway ID.
    ///
    /// The gateway names `node` when it named no node or `node` already.
    /// When it named another node, two known nodes share the gateway ID, so
    /// it becomes [`GatewayStanding::Ambiguous`] and names neither. An
    /// ambiguous gateway stays ambiguous.
    pub(crate) fn with_node(mut self, node: NodeId) -> Self {
        debug_assert!(self.gateway.is_prefix_of(node.as_bytes()));
        match self.node {
            _ if self.standing == GatewayStanding::Ambiguous => self,
            None => {
                self.node = Some(node);
                self
            }
            Some(known) if known == node => self,
            Some(_) => self.into_ambiguous(),
        }
    }

    /// Returns this gateway once v2 networking knows two or more nodes with
    /// its gateway ID: it names none of them.
    pub(crate) fn into_ambiguous(mut self) -> Self {
        self.node = None;
        self.standing = GatewayStanding::Ambiguous;
        self
    }

    /// A gateway named by `node`'s gateway ID and known as `node`.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(crate) fn of_node(node: NodeId, blocks: u32, standing: GatewayStanding) -> Self {
        Self {
            gateway: GatewayId::from_node_id(node.as_bytes()),
            node: Some(node),
            blocks,
            standing,
        }
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
    /// At publication, look up only live owners among the tied overflow entries,
    /// by their gateway IDs. Each tied entry joins once, however many owners
    /// share its gateway ID. Entries below the cutoff score cannot advance even
    /// if they authenticated.
    pub(super) fn candidates<'a>(
        &self,
        authenticated: impl Iterator<Item = &'a NodeId>,
    ) -> Vec<RankedGateway> {
        let mut candidates = self.candidates.clone();
        if !self.tied_at_cutoff.is_empty() {
            let mut joined: HashSet<GatewayId> = HashSet::default();
            candidates.extend(
                authenticated
                    .map(|node| GatewayId::from_node_id(node.as_bytes()))
                    .filter_map(|gateway| self.tied_at_cutoff.get(&gateway))
                    .filter(|gateway| joined.insert(gateway.gateway))
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
        let connected = |gateway: &RankedGateway| gateway.standing == GatewayStanding::Connected;
        let priority = |gateway: &RankedGateway| {
            (
                gateway.blocks,
                connected(gateway),
                connected(gateway) && gateway.node.is_some_and(|node| incumbents.contains(&node)),
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
            .filter(|gateway| gateway.standing.matched())
            .count();
        let hold = hold_limit.min(matched);
        let set = ranking
            .iter()
            .filter(|gateway| gateway.standing.available())
            .filter_map(|gateway| gateway.node)
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
            let place = match gateway.standing {
                GatewayStanding::Unmatched => GatewayPlace::Unmatched,
                GatewayStanding::Ambiguous => GatewayPlace::Ambiguous,
                GatewayStanding::Backoff => GatewayPlace::Backoff,
                GatewayStanding::Dialable
                | GatewayStanding::Dialing
                | GatewayStanding::Connected => {
                    if gateway.node.is_some_and(|node| self.contains(&node)) {
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

    /// Records a ranked gateway as connected, including when it already owns
    /// a preferred place. Later callers then see its standing in cutoff ties.
    pub(super) fn with_connected(&self, node: &NodeId) -> Option<Self> {
        self.with_standing(node, GatewayStanding::Connected)
    }

    /// The set re-formed with the gateway that `node`'s gateway ID names in
    /// `standing`, once that gateway names `node`; `None` when nothing
    /// changes.
    ///
    /// A gateway that named no node now names `node`. A gateway that named
    /// another node becomes ambiguous instead, as [`RankedGateway::with_node`]
    /// says, and `standing` is not applied.
    pub(super) fn with_standing(&self, node: &NodeId, standing: GatewayStanding) -> Option<Self> {
        let gateway_id = GatewayId::from_node_id(node.as_bytes());
        let index = self
            .ranking
            .iter()
            .position(|gateway| gateway.gateway == gateway_id)?;
        let current = self.ranking[index];
        let mut updated = current.with_node(*node);
        if updated.node == Some(*node) {
            updated.standing = standing;
        }
        if updated == current {
            return None;
        }
        let mut ranking = self.ranking.clone();
        ranking[index] = updated;
        Some(Self::form(ranking, self.hold_limit))
    }

    /// The set re-formed with `node` in backoff, once it has lost its last
    /// connection slot; `None` when `node` is unranked, unmatched or
    /// already in backoff.
    ///
    /// A gateway that loses its last connection slot counts as in backoff
    /// until the next published ranking. Like a gateway whose dial failed,
    /// it still counts toward the hold and leaves the set so the next
    /// available gateway may fill it, and may still call back in.
    pub(super) fn with_released(&self, node: &NodeId) -> Option<Self> {
        let index = self
            .ranking
            .iter()
            .position(|gateway| gateway.node == Some(*node) && gateway.standing.available())?;
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
