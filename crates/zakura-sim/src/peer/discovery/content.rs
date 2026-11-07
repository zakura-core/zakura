//! The discovery content labeller (disc-3.1): `BadContent` for a
//! well-formed discovery frame whose fields break a discovery rule.
//!
//! It follows the discovery specification and the fixture's own record of
//! what it changed, never production's validators. It reads the frame's
//! message, the node's and the peer's identities, the node's `GetPeers`
//! limit for a `Peers`, and the records whose signature the fixture
//! forged, and labels each rule below:
//!
//! - `hello`: a forged signature, the wrong network or chain, a protocol
//!   range that doesn't overlap the node's, or a record whose node ID isn't
//!   the authenticated peer's. An expired or far-future record, a private or
//!   missing address, the node's own record and a book limit are local
//!   policy, so they stay `Conformant`.
//! - `peers`: a relayed record with a forged signature or the wrong network
//!   or chain, two records of one node, or more records than the node's
//!   `GetPeers` asked for. Expiry and a protocol range that doesn't overlap
//!   the node's stay `Conformant`: a relay can't change a third party's
//!   record, and a record can expire in transit.
//! - `services`: a node ID that isn't the peer's, or two summaries of one
//!   service.
//! - `get-peers`: a limit outside 1 to 32, excluded node IDs that aren't
//!   sorted and unique, or a wanted list with a repeated service or more
//!   than 8.
//! - `get-services`: a wanted list with a repeated service or more than 32.
//!
//! A malformed record or node ID can't be written as a message, so the
//! fixture never sends one, and the labeller has no rule for it.

use zakura_network::zakura::{
    DiscoveryMessage, ZakuraHandshakeConfig, ZakuraNodeRecord, ZakuraServiceId,
    MAX_DISCOVERY_RECORDS_PER_RESPONSE, MAX_GET_PEERS_WANTED_SERVICES, MAX_GET_SERVICES_WANTED,
};
use zakura_quic::NodeId;

use crate::model::{Label, Labels};

/// What the labeller knows of one discovery stream's two ends.
#[derive(Clone, Debug)]
pub(crate) struct DiscoveryContent {
    /// The node's network, chain and protocol range.
    node: ZakuraHandshakeConfig,
    /// The node ID the node authenticated the peer as.
    peer: NodeId,
}

/// The records of one frame whose signature the fixture forged, by their
/// place in the message: 0 for a `Hello`'s record.
pub(crate) type Forged = [usize];

/// True if `items` repeat an item.
fn repeats<T: PartialEq>(items: &[T]) -> bool {
    items
        .iter()
        .enumerate()
        .any(|(index, item)| items[..index].contains(item))
}

/// True if `wanted` repeats a service or names more than `max`.
fn bad_wanted(wanted: &[ZakuraServiceId], max: usize) -> bool {
    wanted.len() > max || repeats(wanted)
}

impl DiscoveryContent {
    /// The labeller of a stream from the node, which speaks `node`, to the
    /// peer authenticated as `peer`.
    pub(crate) fn new(node: ZakuraHandshakeConfig, peer: NodeId) -> Self {
        DiscoveryContent { node, peer }
    }

    /// True if `record` names another network or chain than the node's.
    fn foreign(&self, record: &ZakuraNodeRecord) -> bool {
        record.body.network_id != self.node.network_id || record.body.chain_id != self.node.chain_id
    }

    /// True if `record`'s protocol range shares no version with the node's.
    fn incompatible(&self, record: &ZakuraNodeRecord) -> bool {
        record.body.zakura_protocol_max < self.node.zakura_protocol_min
            || record.body.zakura_protocol_min > self.node.zakura_protocol_max
    }

    /// `BadContent` if the peer's `message` breaks a content rule. `forged`
    /// names its records whose signature the fixture forged, and `limit` is
    /// the node's `GetPeers` limit that a `Peers` answers, if it answers
    /// one.
    pub(crate) fn label(
        &self,
        message: &DiscoveryMessage,
        forged: &Forged,
        limit: Option<u16>,
    ) -> Labels {
        let bad = match message {
            DiscoveryMessage::Hello { record } => {
                !forged.is_empty()
                    || self.foreign(record)
                    || self.incompatible(record)
                    || record.body.node_id != self.peer
            }
            DiscoveryMessage::Peers { records } => {
                let node_ids: Vec<NodeId> =
                    records.iter().map(|record| record.body.node_id).collect();
                !forged.is_empty()
                    || records.iter().any(|record| self.foreign(record))
                    || repeats(&node_ids)
                    || limit.is_some_and(|limit| records.len() > usize::from(limit))
            }
            DiscoveryMessage::Services(services) => {
                let ids: Vec<&ZakuraServiceId> = services
                    .summaries
                    .iter()
                    .map(|summary| &summary.service_id)
                    .collect();
                services.node_id != self.peer || repeats(&ids)
            }
            DiscoveryMessage::GetPeers(query) => {
                let excluded = &query.exclude_node_ids;
                !(1..=MAX_DISCOVERY_RECORDS_PER_RESPONSE).contains(&usize::from(query.limit))
                    || excluded.windows(2).any(|pair| pair[0] >= pair[1])
                    || bad_wanted(&query.wanted_services, MAX_GET_PEERS_WANTED_SERVICES)
            }
            DiscoveryMessage::GetServices(query) => {
                bad_wanted(&query.wanted_services, MAX_GET_SERVICES_WANTED)
            }
        };
        Labels::NONE.with_if(bad, Label::BadContent)
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::Network;
    use zakura_network::zakura::{GetPeers, GetServices};
    use zakura_quic::NodeSecretKey;

    use super::*;
    use crate::peer::discovery::{record, services, unix_now, FIXTURE_ADDR};

    /// The peer's key, and a labeller of its stream from a Regtest node.
    fn labeller() -> (NodeSecretKey, ZakuraHandshakeConfig, DiscoveryContent) {
        let handshake =
            ZakuraHandshakeConfig::for_network(&Network::new_regtest(Default::default()));
        let peer = NodeSecretKey::from_bytes(&[7; 32]);
        let content = DiscoveryContent::new(handshake, peer.public());
        (peer, handshake, content)
    }

    /// The label of `message`, with no record forged and no limit.
    fn plain(content: &DiscoveryContent, message: &DiscoveryMessage) -> Label {
        content.label(message, &[], None).first()
    }

    /// A `Hello` breaks a rule only through what a peer controls: its
    /// signature, network, chain, protocol range and node ID. Expiry and
    /// addresses are local policy.
    #[test]
    fn hello_rules_follow_the_specification() {
        let (peer, handshake, content) = labeller();
        let hello = |record| DiscoveryMessage::Hello { record };
        let own = record(&peer, &handshake, unix_now(), vec![FIXTURE_ADDR]);
        assert_eq!(plain(&content, &hello(own.clone())), Label::Conformant);
        assert_eq!(
            content.label(&hello(own.clone()), &[0], None).first(),
            Label::BadContent
        );
        let mut expired = own.clone();
        expired.body.expires_at_unix_secs = 0;
        expired.body.direct_addrs.clear();
        assert_eq!(plain(&content, &hello(expired)), Label::Conformant);
        let mut chain = own.clone();
        chain.body.chain_id[0] ^= 1;
        assert_eq!(plain(&content, &hello(chain)), Label::BadContent);
        let mut range = own.clone();
        range.body.zakura_protocol_min = handshake.zakura_protocol_max + 1;
        range.body.zakura_protocol_max = handshake.zakura_protocol_max + 1;
        assert_eq!(plain(&content, &hello(range.clone())), Label::BadContent);
        let other = record(
            &NodeSecretKey::from_bytes(&[8; 32]),
            &handshake,
            unix_now(),
            vec![FIXTURE_ADDR],
        );
        assert_eq!(plain(&content, &hello(other.clone())), Label::BadContent);

        // A relay may carry a record of another protocol range, but not of
        // another chain, nor two of one node, nor more than the limit.
        let peers = |records| DiscoveryMessage::Peers { records };
        assert_eq!(
            plain(&content, &peers(vec![other.clone(), range])),
            Label::Conformant
        );
        let mut network = other.clone();
        network.body.network_id = zakura_network::zakura::ZakuraNetworkId::Mainnet;
        assert_eq!(plain(&content, &peers(vec![network])), Label::BadContent);
        assert_eq!(
            plain(&content, &peers(vec![other.clone(), other.clone()])),
            Label::BadContent
        );
        assert_eq!(
            content
                .label(&peers(vec![own, other.clone()]), &[], Some(1))
                .first(),
            Label::BadContent
        );
        assert_eq!(
            content.label(&peers(vec![other]), &[0], Some(1)).first(),
            Label::BadContent
        );
    }

    /// `Services` must be the peer's own, and the requests' fields must be
    /// within the codec's content rules.
    #[test]
    fn services_and_requests_follow_the_specification() {
        let (peer, _, content) = labeller();
        let answer = DiscoveryMessage::Services(services(peer.public()));
        assert_eq!(plain(&content, &answer), Label::Conformant);
        let other =
            DiscoveryMessage::Services(services(NodeSecretKey::from_bytes(&[8; 32]).public()));
        assert_eq!(plain(&content, &other), Label::BadContent);

        let get_peers = |limit, exclude_node_ids| {
            DiscoveryMessage::GetPeers(GetPeers {
                limit,
                wanted_services: Vec::new(),
                exclude_node_ids,
            })
        };
        assert_eq!(
            plain(&content, &get_peers(32, vec![[1; 32], [2; 32]])),
            Label::Conformant
        );
        assert_eq!(
            plain(&content, &get_peers(0, Vec::new())),
            Label::BadContent
        );
        assert_eq!(
            plain(&content, &get_peers(33, Vec::new())),
            Label::BadContent
        );
        assert_eq!(
            plain(&content, &get_peers(1, vec![[2; 32], [1; 32]])),
            Label::BadContent
        );
        let get_services =
            |wanted_services| DiscoveryMessage::GetServices(GetServices { wanted_services });
        let discovery = ZakuraServiceId::discovery();
        assert_eq!(
            plain(&content, &get_services(vec![discovery.clone()])),
            Label::Conformant
        );
        assert_eq!(
            plain(&content, &get_services(vec![discovery.clone(), discovery])),
            Label::BadContent
        );
    }
}
