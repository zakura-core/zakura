//! The stream conformance suite over discovery's stream.
//!
//! Exchange `e` asks for wanted service `x{e}`. Its `Peers` answer carries one
//! record with sequence `e`, and its `Services` answer expires at `e`.

use zakura_chain::primitives::ed25519::Signature;
use zakura_quic::NodeSecretKey;

use super::{
    protocol::{
        DiscoveryMessage, GetPeers, GetServices, Services, ZakuraNodeRecord, ZakuraNodeRecordBody,
        ZakuraServiceId, MSG_DISCOVERY_GET_PEERS, MSG_DISCOVERY_GET_SERVICES, MSG_DISCOVERY_HELLO,
        MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES,
    },
    service::discovery_streams,
};
use crate::zakura::{
    testkit::stream_conformance::{
        stream_conformance_suite, StreamConformance, SubscriptionUpdate,
    },
    MessageRule, ZakuraNetworkId,
};

#[derive(Debug)]
struct DiscoveryConformance;

fn wanted(exchange: u32) -> Vec<ZakuraServiceId> {
    vec![ZakuraServiceId::new(format!("x{exchange}")).expect("test service ids are short ASCII")]
}

fn wanted_exchange(services: &[ZakuraServiceId]) -> u32 {
    services
        .first()
        .and_then(|service| service.as_str().strip_prefix('x'))
        .and_then(|exchange| exchange.parse().ok())
        .expect("the harness sends one wanted service per exchange")
}

fn record(exchange: u32) -> ZakuraNodeRecord {
    // The codec does not verify signatures.
    ZakuraNodeRecord {
        body: ZakuraNodeRecordBody {
            node_id: NodeSecretKey::from_bytes(&[4; 32]).public(),
            direct_addrs: Vec::new(),
            services: Vec::new(),
            zakura_protocol_min: 1,
            zakura_protocol_max: 1,
            network_id: ZakuraNetworkId::Regtest,
            chain_id: [7; 32],
            sequence: u64::from(exchange),
            expires_at_unix_secs: 0,
            extensions: Vec::new(),
        },
        signature: Signature::from([0xab; 64]),
    }
}

fn record_exchange(record: &ZakuraNodeRecord) -> u32 {
    u32::try_from(record.body.sequence).expect("the harness numbers exchanges with u32")
}

impl StreamConformance for DiscoveryConformance {
    type Message = DiscoveryMessage;

    fn message(row: &MessageRule, exchange: u32) -> DiscoveryMessage {
        let Ok(message_type) = u8::try_from(row.message_type) else {
            unreachable!("discovery has no row {}", row.message_type);
        };
        match message_type {
            MSG_DISCOVERY_HELLO => DiscoveryMessage::Hello {
                record: record(exchange),
            },
            MSG_DISCOVERY_GET_PEERS => DiscoveryMessage::GetPeers(GetPeers {
                limit: 1,
                wanted_services: wanted(exchange),
                exclude_node_ids: Vec::new(),
            }),
            MSG_DISCOVERY_PEERS => DiscoveryMessage::Peers {
                records: vec![record(exchange)],
            },
            MSG_DISCOVERY_GET_SERVICES => DiscoveryMessage::GetServices(GetServices {
                wanted_services: wanted(exchange),
            }),
            MSG_DISCOVERY_SERVICES => DiscoveryMessage::Services(Services {
                node_id: record(exchange).body.node_id,
                expires_at_unix_secs: u64::from(exchange),
                summaries: Vec::new(),
            }),
            other => unreachable!("discovery has no row {other}"),
        }
    }

    fn exchange(message: &DiscoveryMessage) -> u32 {
        match message {
            DiscoveryMessage::Hello { record } => record_exchange(record),
            DiscoveryMessage::GetPeers(GetPeers {
                wanted_services, ..
            }) => wanted_exchange(wanted_services),
            DiscoveryMessage::Peers { records } => records
                .first()
                .map(record_exchange)
                .expect("the harness answers with one record"),
            DiscoveryMessage::GetServices(query) => wanted_exchange(&query.wanted_services),
            DiscoveryMessage::Services(services) => u32::try_from(services.expires_at_unix_secs)
                .expect("the harness numbers exchanges with u32"),
        }
    }

    fn update(_row: &MessageRule, _update: SubscriptionUpdate) -> DiscoveryMessage {
        unreachable!("discovery has no subscription row")
    }

    fn read_update(_message: &DiscoveryMessage) -> Option<SubscriptionUpdate> {
        None
    }

    fn page(_row: &MessageRule, _key: u32, _cursor: u32) -> DiscoveryMessage {
        unreachable!("discovery has no subscription row")
    }

    fn read_page(_message: &DiscoveryMessage) -> Option<(u32, u32)> {
        None
    }
}

stream_conformance_suite!(
    discovery_stream_conformance,
    *discovery_streams(),
    DiscoveryConformance
);
