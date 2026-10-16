//! Discovery's message family against its rows.
//!
//! The suite checks every row's bounds with the samples below. The fixtures
//! after it build each row's longest payload byte by byte, without the codec,
//! so a wrong constant and a matching wrong encoder cannot agree.

use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::OnceLock,
    time::Duration,
};

use proptest::prelude::*;
use zakura_chain::{block, primitives::ed25519::Signature};
use zakura_quic::{NodeId, NodeSecretKey};

use super::{
    protocol::{
        BlockSyncServiceSummary, DiscoveryMessage, DiscoveryServiceSummary, GetServices,
        ServiceSummaryEnvelope, Services, ZakuraNodeRecord, ZakuraNodeRecordBody, ZakuraServiceId,
        MAX_DIRECT_ADDRS_PER_RECORD, MAX_DISCOVERY_EXCLUDED_NODE_IDS,
        MAX_DISCOVERY_RECORDS_PER_RESPONSE, MAX_GET_PEERS_WANTED_SERVICES, MAX_GET_SERVICES_WANTED,
        MAX_NODE_RECORD_BODY_BYTES, MAX_SERVICES_PER_RECORD, MAX_SERVICE_SUMMARIES_PER_RESPONSE,
        MAX_SERVICE_SUMMARY_BYTES, MAX_ZAKURA_SERVICE_ID_BYTES, MSG_DISCOVERY_GET_PEERS,
        MSG_DISCOVERY_GET_SERVICES, MSG_DISCOVERY_HELLO, MSG_DISCOVERY_PEERS,
        MSG_DISCOVERY_SERVICES, SUMMARY_TAG_BLOCK_SYNC_V1, SUMMARY_TAG_DISCOVERY_V1,
    },
    service::discovery_streams,
    wire::{GET_PEERS, GET_SERVICES, HELLO, PEERS, SEND_INTERVAL, SERVICES},
};
use crate::zakura::{
    check_frame_filter,
    wire_codec::{
        decode_frame, encode_frame,
        message_suite::{check_layout_carries_family, message_suite, MessageSample, Violation},
        WireError,
    },
    Frame, MessageRole, MessageRule, ZakuraNetworkId,
};

/// A tag no release assigns, so its summary bytes stay opaque.
const UNKNOWN_TAG: u16 = 0xfff0;

/// Valid node ids in byte order: enough to fill an exclusion list and a
/// full `Peers` message.
fn node_ids() -> &'static [NodeId] {
    static NODE_IDS: OnceLock<Vec<NodeId>> = OnceLock::new();
    NODE_IDS.get_or_init(|| {
        let mut ids: Vec<NodeId> = (0..=MAX_DISCOVERY_EXCLUDED_NODE_IDS)
            .map(|index| {
                let mut seed = [0u8; 32];
                seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
                NodeSecretKey::from_bytes(&seed).public()
            })
            .collect();
        ids.sort_by_key(|id| *id.as_bytes());
        ids
    })
}

/// A valid service id of exactly `len` bytes, distinct for each `index`.
fn service_id(index: usize, len: usize) -> ZakuraServiceId {
    let mut id = format!("s{index}.");
    while id.len() < len {
        id.push('x');
    }
    id.truncate(len);
    ZakuraServiceId::new(id).expect("test service ids are short ASCII")
}

fn longest_service(index: usize) -> ZakuraServiceId {
    service_id(index, MAX_ZAKURA_SERVICE_ID_BYTES)
}

fn record(body: ZakuraNodeRecordBody) -> ZakuraNodeRecord {
    // The codec does not verify signatures.
    ZakuraNodeRecord {
        body,
        signature: Signature::from([0xab; 64]),
    }
}

fn shortest_body(node_id: NodeId) -> ZakuraNodeRecordBody {
    ZakuraNodeRecordBody {
        node_id,
        direct_addrs: Vec::new(),
        services: Vec::new(),
        zakura_protocol_min: 1,
        zakura_protocol_max: 1,
        network_id: ZakuraNetworkId::Mainnet,
        chain_id: [7; 32],
        sequence: 0,
        expires_at_unix_secs: 0,
        extensions: Vec::new(),
    }
}

fn longest_ipv6(index: u8) -> SocketAddr {
    SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, u16::from(index)),
        u16::MAX,
        u32::MAX,
        u32::MAX,
    ))
}

/// A record body with the most and longest addresses and services.
fn longest_body(node_id: NodeId) -> ZakuraNodeRecordBody {
    ZakuraNodeRecordBody {
        direct_addrs: (0..MAX_DIRECT_ADDRS_PER_RECORD)
            .map(|index| longest_ipv6(index as u8))
            .collect(),
        services: (0..MAX_SERVICES_PER_RECORD).map(longest_service).collect(),
        zakura_protocol_min: 0,
        zakura_protocol_max: u16::MAX,
        sequence: u64::MAX,
        expires_at_unix_secs: u64::MAX,
        ..shortest_body(node_id)
    }
}

fn longest_get_peers() -> DiscoveryMessage {
    DiscoveryMessage::GetPeers {
        limit: MAX_DISCOVERY_RECORDS_PER_RESPONSE as u16,
        wanted_services: (0..MAX_GET_PEERS_WANTED_SERVICES)
            .map(longest_service)
            .collect(),
        exclude_node_ids: node_ids()[..MAX_DISCOVERY_EXCLUDED_NODE_IDS].to_vec(),
    }
}

fn longest_peers() -> DiscoveryMessage {
    DiscoveryMessage::Peers {
        records: node_ids()[..MAX_DISCOVERY_RECORDS_PER_RESPONSE]
            .iter()
            .map(|node_id| record(longest_body(*node_id)))
            .collect(),
    }
}

fn longest_get_services() -> DiscoveryMessage {
    DiscoveryMessage::GetServices(GetServices {
        wanted_services: (0..MAX_GET_SERVICES_WANTED).map(longest_service).collect(),
    })
}

fn longest_services() -> DiscoveryMessage {
    DiscoveryMessage::Services(Services {
        node_id: node_ids()[0],
        expires_at_unix_secs: u64::MAX,
        summaries: (0..MAX_SERVICE_SUMMARIES_PER_RESPONSE)
            .map(|index| ServiceSummaryEnvelope {
                service_id: longest_service(index),
                summary_tag: UNKNOWN_TAG,
                summary_bytes: vec![0x5a; MAX_SERVICE_SUMMARY_BYTES],
            })
            .collect(),
    })
}

fn known_summaries() -> DiscoveryMessage {
    DiscoveryMessage::Services(Services {
        node_id: node_ids()[1],
        expires_at_unix_secs: 1,
        summaries: vec![
            ServiceSummaryEnvelope::discovery(&DiscoveryServiceSummary {
                peer_exchange_slots_free: 3,
                max_records_per_response: 32,
                expected_disconnect_after_exchange: true,
            })
            .expect("discovery summary encodes"),
            ServiceSummaryEnvelope::block_sync(&BlockSyncServiceSummary {
                servable_low: block::Height(1),
                servable_high: block::Height(9),
                tip_hash: block::Hash([3; 32]),
                free_slots: 4,
                max_blocks_per_response: 16,
                max_response_bytes: 1 << 20,
            })
            .expect("block-sync summary encodes"),
        ],
    })
}

fn frame(message_type: u8, payload: Vec<u8>) -> Frame {
    Frame {
        message_type: u16::from(message_type),
        flags: 0,
        payload,
    }
}

fn payload(message: &DiscoveryMessage) -> Vec<u8> {
    encode_frame(message)
        .expect("a valid message encodes")
        .payload
}

/// The bytes of a node id that is not a curve point.
fn invalid_node_id() -> [u8; 32] {
    (0..=u8::MAX)
        .map(|byte| [byte; 32])
        .find(|bytes| NodeId::from_bytes(bytes).is_err())
        .expect("some repeated byte is not a curve point")
}

/// A `Hello` frame around raw body bytes.
fn hello_with_body(body: &[u8]) -> Frame {
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(body);
    out.extend_from_slice(&[0xab; 64]);
    frame(MSG_DISCOVERY_HELLO, out)
}

/// A shortest record body whose lists are `lists`: the address count and
/// addresses, then the service count and service ids.
fn raw_body(node_id: [u8; 32], lists: &[u8], protocol: (u16, u16), network: u32) -> Vec<u8> {
    let mut out = node_id.to_vec();
    out.extend_from_slice(lists);
    out.extend_from_slice(&protocol.0.to_le_bytes());
    out.extend_from_slice(&protocol.1.to_le_bytes());
    out.extend_from_slice(&network.to_le_bytes());
    out.extend_from_slice(&[7; 32]);
    out.extend_from_slice(&[0; 16]);
    out
}

fn valid_node_id() -> [u8; 32] {
    *node_ids()[0].as_bytes()
}

/// Empty address and service lists.
const NO_LISTS: [u8; 4] = [0; 4];

fn get_peers_payload(limit: u16, services: &[&str], excluded: &[[u8; 32]]) -> Vec<u8> {
    let mut out = limit.to_le_bytes().to_vec();
    out.extend_from_slice(&(services.len() as u16).to_le_bytes());
    for service in services {
        out.extend_from_slice(&(service.len() as u16).to_le_bytes());
        out.extend_from_slice(service.as_bytes());
    }
    out.extend_from_slice(&(excluded.len() as u16).to_le_bytes());
    for node_id in excluded {
        out.extend_from_slice(node_id);
    }
    out
}

fn services_payload(envelopes: &[(&str, u16, &[u8])]) -> Vec<u8> {
    let mut out = valid_node_id().to_vec();
    out.extend_from_slice(&u64::MAX.to_le_bytes());
    out.extend_from_slice(&(envelopes.len() as u16).to_le_bytes());
    for (service, tag, bytes) in envelopes {
        out.extend_from_slice(&(service.len() as u16).to_le_bytes());
        out.extend_from_slice(service.as_bytes());
        out.extend_from_slice(&tag.to_le_bytes());
        out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

fn arbitrary_addr() -> BoxedStrategy<SocketAddr> {
    prop_oneof![
        (any::<[u8; 4]>(), any::<u16>())
            .prop_map(|(ip, port)| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(ip), port))),
        (any::<[u8; 16]>(), any::<u16>(), any::<u32>(), any::<u32>()).prop_map(
            |(ip, port, flowinfo, scope_id)| SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(ip),
                port,
                flowinfo,
                scope_id
            ))
        ),
    ]
    .boxed()
}

fn arbitrary_service() -> BoxedStrategy<ZakuraServiceId> {
    (0..64usize, 1..=MAX_ZAKURA_SERVICE_ID_BYTES)
        .prop_map(|(index, len)| service_id(index, len))
        .boxed()
}

/// Keeps the first item for each key.
fn dedup_by_key<T, K: PartialEq>(items: &mut Vec<T>, key: impl Fn(&T) -> K) {
    let mut seen = Vec::new();
    items.retain(|item| {
        let key = key(item);
        let fresh = !seen.contains(&key);
        seen.push(key);
        fresh
    });
}

/// Unique service ids, at most `max` of them.
fn arbitrary_unique_services(max: usize) -> BoxedStrategy<Vec<ZakuraServiceId>> {
    proptest::collection::vec(arbitrary_service(), 0..=max)
        .prop_map(|mut services| {
            dedup_by_key(&mut services, ZakuraServiceId::clone);
            services
        })
        .boxed()
}

fn arbitrary_record() -> BoxedStrategy<ZakuraNodeRecord> {
    let network = prop_oneof![
        Just(ZakuraNetworkId::Mainnet),
        Just(ZakuraNetworkId::Testnet),
        Just(ZakuraNetworkId::Regtest),
        Just(ZakuraNetworkId::Configured),
    ];
    (
        proptest::sample::select(node_ids().to_vec()),
        proptest::collection::vec(arbitrary_addr(), 0..=MAX_DIRECT_ADDRS_PER_RECORD),
        proptest::collection::vec(arbitrary_service(), 0..=MAX_SERVICES_PER_RECORD),
        (any::<u16>(), any::<u16>()),
        network,
        any::<[u8; 32]>(),
        (any::<u64>(), any::<u64>()),
        proptest::collection::vec(any::<u8>(), 0..=MAX_NODE_RECORD_BODY_BYTES),
        any::<[u8; 64]>(),
    )
        .prop_map(
            |(
                node_id,
                direct_addrs,
                services,
                (a, b),
                network_id,
                chain_id,
                (sequence, expiry),
                extensions,
                signature,
            )| {
                let mut body = ZakuraNodeRecordBody {
                    node_id,
                    direct_addrs,
                    services,
                    zakura_protocol_min: a.min(b),
                    zakura_protocol_max: a.max(b),
                    network_id,
                    chain_id,
                    sequence,
                    expires_at_unix_secs: expiry,
                    extensions: Vec::new(),
                };
                let mut fixed = Vec::new();
                super::wire::write_record_body(&body, &mut fixed);
                let room = MAX_NODE_RECORD_BODY_BYTES - fixed.len();
                body.extensions = extensions;
                body.extensions.truncate(room);
                ZakuraNodeRecord {
                    body,
                    signature: Signature::from(signature),
                }
            },
        )
        .boxed()
}

fn arbitrary_envelope() -> BoxedStrategy<ServiceSummaryEnvelope> {
    (
        arbitrary_service(),
        (UNKNOWN_TAG..=u16::MAX),
        proptest::collection::vec(any::<u8>(), 0..=MAX_SERVICE_SUMMARY_BYTES),
    )
        .prop_map(
            |(service_id, summary_tag, summary_bytes)| ServiceSummaryEnvelope {
                service_id,
                summary_tag,
                summary_bytes,
            },
        )
        .boxed()
}

impl MessageSample for DiscoveryMessage {
    fn samples() -> Vec<Self> {
        vec![
            Self::Hello {
                record: record(shortest_body(node_ids()[0])),
            },
            Self::Hello {
                record: record(longest_body(node_ids()[0])),
            },
            Self::Hello {
                record: record(ZakuraNodeRecordBody {
                    direct_addrs: vec!["45.33.30.1:8233".parse().expect("test address parses")],
                    services: vec![ZakuraServiceId::discovery()],
                    // An unknown TLV: type 0x7001, length 3.
                    extensions: vec![0x01, 0x70, 0x03, 0x00, 0xaa, 0xbb, 0xcc],
                    ..shortest_body(node_ids()[1])
                }),
            },
            Self::GetPeers {
                limit: 1,
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            },
            longest_get_peers(),
            Self::Peers {
                records: Vec::new(),
            },
            longest_peers(),
            Self::GetServices(GetServices {
                wanted_services: Vec::new(),
            }),
            longest_get_services(),
            Self::Services(Services {
                node_id: node_ids()[0],
                expires_at_unix_secs: 0,
                summaries: Vec::new(),
            }),
            longest_services(),
            known_summaries(),
        ]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        prop_oneof![
            arbitrary_record().prop_map(|record| Self::Hello { record }),
            (
                1..=MAX_DISCOVERY_RECORDS_PER_RESPONSE as u16,
                arbitrary_unique_services(MAX_GET_PEERS_WANTED_SERVICES),
                proptest::sample::subsequence(
                    node_ids().to_vec(),
                    0..=MAX_DISCOVERY_EXCLUDED_NODE_IDS
                ),
            )
                .prop_map(|(limit, wanted_services, exclude_node_ids)| {
                    Self::GetPeers {
                        limit,
                        wanted_services,
                        exclude_node_ids,
                    }
                }),
            proptest::collection::vec(arbitrary_record(), 0..=4).prop_map(|mut records| {
                dedup_by_key(&mut records, |record| record.body.node_id);
                Self::Peers { records }
            }),
            arbitrary_unique_services(MAX_GET_SERVICES_WANTED)
                .prop_map(|wanted_services| Self::GetServices(GetServices { wanted_services })),
            (
                proptest::sample::select(node_ids().to_vec()),
                any::<u64>(),
                proptest::collection::vec(
                    arbitrary_envelope(),
                    0..=MAX_SERVICE_SUMMARIES_PER_RESPONSE
                ),
            )
                .prop_map(|(node_id, expires_at_unix_secs, mut summaries)| {
                    dedup_by_key(&mut summaries, |summary| summary.service_id.clone());
                    Self::Services(Services {
                        node_id,
                        expires_at_unix_secs,
                        summaries,
                    })
                }),
        ]
        .boxed()
    }

    fn violations() -> Vec<Violation<Self>> {
        let low = *node_ids()[0].as_bytes();
        let high = *node_ids()[1].as_bytes();
        let one_service = [&1u16.to_le_bytes()[..], &1u16.to_le_bytes(), b"a"].concat();
        let mut too_many_peers = (MAX_DISCOVERY_RECORDS_PER_RESPONSE as u16 + 1)
            .to_le_bytes()
            .to_vec();
        too_many_peers.resize(PEERS.payload.max(), 0);
        let mut oversize_body = ((MAX_NODE_RECORD_BODY_BYTES + 1) as u32)
            .to_le_bytes()
            .to_vec();
        oversize_body.resize(HELLO.payload.max(), 0);
        let mut duplicate_peers = 2u16.to_le_bytes().to_vec();
        let shortest = payload(&Self::Hello {
            record: record(shortest_body(node_ids()[0])),
        });
        duplicate_peers.extend_from_slice(&shortest);
        duplicate_peers.extend_from_slice(&shortest);

        vec![
            Violation {
                name: "peer limit of zero",
                frame: frame(MSG_DISCOVERY_GET_PEERS, get_peers_payload(0, &[], &[])),
                rejected_by: |error| *error == WireError::OutOfRange("peer limit"),
            },
            Violation {
                name: "peer limit above a full Peers message",
                frame: frame(
                    MSG_DISCOVERY_GET_PEERS,
                    get_peers_payload(MAX_DISCOVERY_RECORDS_PER_RESPONSE as u16 + 1, &[], &[]),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("peer limit"),
            },
            Violation {
                name: "excluded node ids out of order",
                frame: frame(
                    MSG_DISCOVERY_GET_PEERS,
                    get_peers_payload(1, &[], &[high, low]),
                ),
                rejected_by: |error| {
                    *error == WireError::OutOfRange("sorted unique excluded node ids")
                },
            },
            Violation {
                name: "excluded node id repeated",
                frame: frame(
                    MSG_DISCOVERY_GET_PEERS,
                    get_peers_payload(1, &[], &[low, low]),
                ),
                rejected_by: |error| {
                    *error == WireError::OutOfRange("sorted unique excluded node ids")
                },
            },
            Violation {
                name: "excluded node id is not a curve point",
                frame: frame(
                    MSG_DISCOVERY_GET_PEERS,
                    get_peers_payload(1, &[], &[invalid_node_id()]),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("excluded node id"),
            },
            Violation {
                name: "excluded count beyond the payload",
                frame: frame(MSG_DISCOVERY_GET_PEERS, {
                    let mut out = get_peers_payload(1, &[], &[low]);
                    out[4..6].copy_from_slice(&2u16.to_le_bytes());
                    out
                }),
                rejected_by: |error| {
                    matches!(error, WireError::CountExceedsPayload { count: 2, .. })
                },
            },
            Violation {
                name: "wanted service repeated",
                frame: frame(
                    MSG_DISCOVERY_GET_PEERS,
                    get_peers_payload(1, &["a", "a"], &[]),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("unique service ids"),
            },
            Violation {
                name: "too many wanted services",
                frame: frame(
                    MSG_DISCOVERY_GET_PEERS,
                    get_peers_payload(1, &["a"; MAX_GET_PEERS_WANTED_SERVICES + 1], &[]),
                ),
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { .. }),
            },
            Violation {
                name: "GetServices service repeated",
                frame: frame(
                    MSG_DISCOVERY_GET_SERVICES,
                    [&2u16.to_le_bytes()[..], &[1, 0], b"a", &[1, 0], b"a"].concat(),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("unique service ids"),
            },
            Violation {
                name: "too many GetServices services",
                frame: frame(
                    MSG_DISCOVERY_GET_SERVICES,
                    ((MAX_GET_SERVICES_WANTED + 1) as u16)
                        .to_le_bytes()
                        .to_vec(),
                ),
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { .. }),
            },
            Violation {
                name: "too many Peers records",
                frame: frame(MSG_DISCOVERY_PEERS, too_many_peers),
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { .. }),
            },
            Violation {
                name: "Peers record node id repeated",
                frame: frame(MSG_DISCOVERY_PEERS, duplicate_peers),
                rejected_by: |error| *error == WireError::OutOfRange("unique record node ids"),
            },
            Violation {
                name: "record body shorter than its fixed fields",
                frame: frame(MSG_DISCOVERY_HELLO, {
                    let mut out = 91u32.to_le_bytes().to_vec();
                    out.resize(HELLO.payload.min(), 0);
                    out
                }),
                rejected_by: |error| *error == WireError::OutOfRange("record body length"),
            },
            Violation {
                name: "record body above its cap",
                frame: frame(MSG_DISCOVERY_HELLO, oversize_body),
                rejected_by: |error| *error == WireError::OutOfRange("record body length"),
            },
            Violation {
                name: "record node id is not a curve point",
                frame: hello_with_body(&raw_body(invalid_node_id(), &NO_LISTS, (1, 1), 1)),
                rejected_by: |error| *error == WireError::OutOfRange("record node id"),
            },
            Violation {
                name: "record protocol range runs backwards",
                frame: hello_with_body(&raw_body(valid_node_id(), &NO_LISTS, (2, 1), 1)),
                rejected_by: |error| *error == WireError::OutOfRange("protocol range"),
            },
            Violation {
                name: "record network id unknown",
                frame: hello_with_body(&raw_body(valid_node_id(), &NO_LISTS, (1, 1), 99)),
                rejected_by: |error| *error == WireError::OutOfRange("network id"),
            },
            Violation {
                name: "record address family unknown",
                frame: hello_with_body(&raw_body(
                    valid_node_id(),
                    &[&1u16.to_le_bytes()[..], &[9, 1, 2, 3, 4, 0, 0], &[0, 0]].concat(),
                    (1, 1),
                    1,
                )),
                rejected_by: |error| *error == WireError::OutOfRange("address family"),
            },
            Violation {
                name: "too many record addresses",
                frame: hello_with_body(&raw_body(
                    valid_node_id(),
                    &[
                        &((MAX_DIRECT_ADDRS_PER_RECORD + 1) as u16).to_le_bytes()[..],
                        &[0; 2],
                    ]
                    .concat(),
                    (1, 1),
                    1,
                )),
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { .. }),
            },
            Violation {
                name: "record service id empty",
                frame: hello_with_body(&raw_body(
                    valid_node_id(),
                    &[&[0, 0][..], &1u16.to_le_bytes(), &[0, 0]].concat(),
                    (1, 1),
                    1,
                )),
                rejected_by: |error| *error == WireError::OutOfRange("service id"),
            },
            Violation {
                name: "record service id too long",
                frame: hello_with_body(&raw_body(
                    valid_node_id(),
                    &[
                        &[0, 0][..],
                        &1u16.to_le_bytes(),
                        &((MAX_ZAKURA_SERVICE_ID_BYTES + 1) as u16).to_le_bytes(),
                        &[b'a'; MAX_ZAKURA_SERVICE_ID_BYTES + 1],
                    ]
                    .concat(),
                    (1, 1),
                    1,
                )),
                rejected_by: |error| *error == WireError::OutOfRange("service id"),
            },
            Violation {
                name: "record service id not ASCII",
                frame: hello_with_body(&raw_body(
                    valid_node_id(),
                    &[&[0, 0][..], &1u16.to_le_bytes(), &[1, 0], &[0xff]].concat(),
                    (1, 1),
                    1,
                )),
                rejected_by: |error| *error == WireError::OutOfRange("service id"),
            },
            Violation {
                name: "too many record services",
                frame: hello_with_body(&raw_body(
                    valid_node_id(),
                    &[
                        &[0, 0][..],
                        &((MAX_SERVICES_PER_RECORD + 1) as u16).to_le_bytes(),
                        &one_service[2..].repeat(MAX_SERVICES_PER_RECORD + 1),
                    ]
                    .concat(),
                    (1, 1),
                    1,
                )),
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { .. }),
            },
            Violation {
                name: "summary service repeated",
                frame: frame(
                    MSG_DISCOVERY_SERVICES,
                    services_payload(&[("a", UNKNOWN_TAG, &[]), ("a", UNKNOWN_TAG, &[])]),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("unique summary service ids"),
            },
            Violation {
                name: "known summary tag on another service",
                frame: frame(
                    MSG_DISCOVERY_SERVICES,
                    services_payload(&[("a", SUMMARY_TAG_DISCOVERY_V1, &[0; 5])]),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("summary tag service"),
            },
            Violation {
                name: "known summary payload malformed",
                frame: frame(
                    MSG_DISCOVERY_SERVICES,
                    services_payload(&[(
                        ZakuraServiceId::block_sync().as_str(),
                        SUMMARY_TAG_BLOCK_SYNC_V1,
                        &[0; 3],
                    )]),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("service summary"),
            },
            Violation {
                name: "summary above its cap",
                frame: frame(
                    MSG_DISCOVERY_SERVICES,
                    services_payload(&[("a", UNKNOWN_TAG, &[0; MAX_SERVICE_SUMMARY_BYTES + 1])]),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("service summary length"),
            },
            Violation {
                name: "too many summaries",
                frame: frame(MSG_DISCOVERY_SERVICES, {
                    let mut out = services_payload(&[]);
                    let count = (MAX_SERVICE_SUMMARIES_PER_RESPONSE + 1) as u16;
                    out[40..42].copy_from_slice(&count.to_le_bytes());
                    out
                }),
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { .. }),
            },
        ]
    }
}

message_suite!(discovery_family, DiscoveryMessage);

/// The encoder refuses each message the decoder would reject, so this node
/// never sends a frame its peers must refuse.
#[test]
fn the_encoder_refuses_what_the_decoder_rejects() {
    let get_peers = |limit: u16, wanted: Vec<ZakuraServiceId>, excluded: Vec<NodeId>| {
        DiscoveryMessage::GetPeers {
            limit,
            wanted_services: wanted,
            exclude_node_ids: excluded,
        }
    };
    let services = |summaries: Vec<ServiceSummaryEnvelope>| {
        DiscoveryMessage::Services(Services {
            node_id: node_ids()[0],
            expires_at_unix_secs: 1,
            summaries,
        })
    };
    let envelope = |index: usize| ServiceSummaryEnvelope {
        service_id: service_id(index, 4),
        summary_tag: UNKNOWN_TAG,
        summary_bytes: Vec::new(),
    };
    let ids = node_ids();
    let too_many = |len: usize| (0..len).map(|index| service_id(index, 4)).collect();
    let crossed_protocols = record(ZakuraNodeRecordBody {
        zakura_protocol_min: 2,
        zakura_protocol_max: 1,
        ..shortest_body(ids[0])
    });

    let refused = [
        ("a zero limit", get_peers(0, Vec::new(), Vec::new())),
        (
            "a limit above one response",
            get_peers(
                MAX_DISCOVERY_RECORDS_PER_RESPONSE as u16 + 1,
                Vec::new(),
                Vec::new(),
            ),
        ),
        (
            "a repeated wanted service",
            get_peers(1, vec![service_id(0, 4), service_id(0, 4)], Vec::new()),
        ),
        (
            "too many wanted services",
            get_peers(1, too_many(MAX_GET_PEERS_WANTED_SERVICES + 1), Vec::new()),
        ),
        (
            "unsorted exclusions",
            get_peers(1, Vec::new(), vec![ids[1], ids[0]]),
        ),
        (
            "a repeated exclusion",
            get_peers(1, Vec::new(), vec![ids[0], ids[0]]),
        ),
        (
            "too many exclusions",
            get_peers(
                1,
                Vec::new(),
                ids[..=MAX_DISCOVERY_EXCLUDED_NODE_IDS].to_vec(),
            ),
        ),
        (
            "a repeated record",
            DiscoveryMessage::Peers {
                records: vec![record(shortest_body(ids[0])); 2],
            },
        ),
        (
            "too many records",
            DiscoveryMessage::Peers {
                records: ids[..=MAX_DISCOVERY_RECORDS_PER_RESPONSE]
                    .iter()
                    .map(|node_id| record(shortest_body(*node_id)))
                    .collect(),
            },
        ),
        (
            "a crossed protocol range",
            DiscoveryMessage::Hello {
                record: crossed_protocols,
            },
        ),
        (
            "a repeated GetServices wanted service",
            DiscoveryMessage::GetServices(GetServices {
                wanted_services: vec![service_id(0, 4), service_id(0, 4)],
            }),
        ),
        (
            "too many GetServices wanted services",
            DiscoveryMessage::GetServices(GetServices {
                wanted_services: too_many(MAX_GET_SERVICES_WANTED + 1),
            }),
        ),
        (
            "a repeated summary",
            services(vec![envelope(0), envelope(0)]),
        ),
        (
            "too many summaries",
            services(
                (0..=MAX_SERVICE_SUMMARIES_PER_RESPONSE)
                    .map(envelope)
                    .collect(),
            ),
        ),
        (
            "an oversized summary",
            services(vec![ServiceSummaryEnvelope {
                summary_bytes: vec![0; MAX_SERVICE_SUMMARY_BYTES + 1],
                ..envelope(0)
            }]),
        ),
    ];
    for (case, message) in refused {
        assert!(encode_frame(&message).is_err(), "the encoder sent {case}");
    }
}

#[test]
fn the_stream_carries_exactly_the_family_rows() {
    check_layout_carries_family::<DiscoveryMessage>(discovery_streams());
}

#[test]
fn readers_follow_the_stream() {
    check_frame_filter(discovery_streams());
}

/// Hello, GetPeers, and GetServices share one cadence: a 15 s sender floor,
/// and a bucket of 42 that regains one message every 7 s.
#[test]
fn rows_with_a_cadence_record_bursts_and_forward() {
    for row in [HELLO, GET_PEERS, GET_SERVICES] {
        let cadence = match row.role {
            MessageRole::Announcement { cadence } => cadence,
            MessageRole::Request {
                max_in_flight: 1,
                cadence: Some(cadence),
            } => cadence,
            role => panic!("row {} has role {role:?}", row.message_type),
        };
        assert_eq!(cadence.send_interval, SEND_INTERVAL);
        assert_eq!(cadence.send_interval, Duration::from_secs(15));
        assert_eq!(cadence.capacity, 42);
        assert_eq!(cadence.refill_interval, Duration::from_secs(7));
    }
    for (row, request) in [(PEERS, GET_PEERS), (SERVICES, GET_SERVICES)] {
        assert_eq!(
            row.role,
            MessageRole::Response {
                request: request.message_type,
                ends_exchange: true,
            }
        );
    }
}

/// Each fixture writes a row's longest payload by hand. Decoding it and
/// encoding the result must return the same bytes, and the length must be the
/// row's maximum.
fn check_fixture(message_type: u8, bytes: Vec<u8>, expected_len: usize) {
    assert_eq!(bytes.len(), expected_len);
    let frame = frame(message_type, bytes);
    let rule = MessageRule::find(super::wire::RULES, frame.message_type)
        .expect("fixture names a discovery row");
    assert_eq!(rule.payload.max(), expected_len);
    let message: DiscoveryMessage = decode_frame(&frame).expect("fixture decodes");
    assert_eq!(payload(&message), frame.payload);
}

fn fixture_service(index: usize) -> Vec<u8> {
    let mut out = 32u16.to_le_bytes().to_vec();
    out.extend_from_slice(longest_service(index).as_str().as_bytes());
    out
}

fn fixture_record(node_id: &NodeId) -> Vec<u8> {
    let mut body = node_id.as_bytes().to_vec();
    body.extend_from_slice(&8u16.to_le_bytes());
    for index in 0..8u8 {
        let SocketAddr::V6(addr) = longest_ipv6(index) else {
            unreachable!("longest_ipv6 builds IPv6 addresses");
        };
        body.push(6);
        body.extend_from_slice(&addr.ip().octets());
        body.extend_from_slice(&addr.port().to_le_bytes());
        body.extend_from_slice(&addr.flowinfo().to_le_bytes());
        body.extend_from_slice(&addr.scope_id().to_le_bytes());
    }
    body.extend_from_slice(&8u16.to_le_bytes());
    for index in 0..8 {
        body.extend_from_slice(&fixture_service(index));
    }
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&u16::MAX.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&[7; 32]);
    body.extend_from_slice(&u64::MAX.to_le_bytes());
    body.extend_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(body.len(), 580);

    let mut out = 580u32.to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out.extend_from_slice(&[0xab; 64]);
    out
}

#[test]
fn hello_fixture_is_648_bytes() {
    check_fixture(MSG_DISCOVERY_HELLO, fixture_record(&node_ids()[0]), 648);
}

#[test]
fn shortest_hello_is_160_bytes() {
    let bytes = hello_with_body(&raw_body(valid_node_id(), &NO_LISTS, (1, 1), 1)).payload;
    assert_eq!(bytes.len(), 160);
    assert_eq!(HELLO.payload.min(), 160);
    let message: DiscoveryMessage =
        decode_frame(&frame(MSG_DISCOVERY_HELLO, bytes.clone())).expect("fixture decodes");
    assert_eq!(payload(&message), bytes);
}

#[test]
fn get_peers_fixture_is_8470_bytes() {
    let mut bytes = 32u16.to_le_bytes().to_vec();
    bytes.extend_from_slice(&8u16.to_le_bytes());
    for index in 0..8 {
        bytes.extend_from_slice(&fixture_service(index));
    }
    bytes.extend_from_slice(&256u16.to_le_bytes());
    for node_id in &node_ids()[..256] {
        bytes.extend_from_slice(node_id.as_bytes());
    }
    check_fixture(MSG_DISCOVERY_GET_PEERS, bytes, 8_470);
}

#[test]
fn peers_fixture_is_20738_bytes() {
    let mut bytes = 32u16.to_le_bytes().to_vec();
    for node_id in &node_ids()[..32] {
        bytes.extend_from_slice(&fixture_record(node_id));
    }
    check_fixture(MSG_DISCOVERY_PEERS, bytes, 20_738);
}

#[test]
fn get_services_fixture_is_1090_bytes() {
    let mut bytes = 32u16.to_le_bytes().to_vec();
    for index in 0..32 {
        bytes.extend_from_slice(&fixture_service(index));
    }
    check_fixture(MSG_DISCOVERY_GET_SERVICES, bytes, 1_090);
}

#[test]
fn services_fixture_is_2394_bytes() {
    let mut bytes = valid_node_id().to_vec();
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());
    bytes.extend_from_slice(&8u16.to_le_bytes());
    for index in 0..8 {
        bytes.extend_from_slice(&fixture_service(index));
        bytes.extend_from_slice(&UNKNOWN_TAG.to_le_bytes());
        bytes.extend_from_slice(&256u16.to_le_bytes());
        bytes.extend_from_slice(&[0x5a; 256]);
    }
    check_fixture(MSG_DISCOVERY_SERVICES, bytes, 2_394);
}
