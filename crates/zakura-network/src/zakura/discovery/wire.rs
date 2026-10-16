//! Stream 4, version 2: the frame header carries the discriminator, and the
//! decoder bounds every count, string, and record before it allocates.
//!
//! Fields keep version 1's order and integer encodings without its payload
//! discriminator. Each row's maximum is the largest encoding under the limits
//! in [`super::protocol`]; `wire_tests` pins each one with a fixture built by hand.

use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    time::Duration,
};

use zakura_chain::primitives::ed25519::Signature;
use zakura_quic::NodeId;

use super::protocol::{
    network_id_from_code, summary_tag_service, validate_known_summary_payload,
    validate_record_body_bounds, DiscoveryMessage, GetServices, ServiceSummaryEnvelope, Services,
    ZakuraNodeRecord, ZakuraNodeRecordBody, ZakuraServiceId, MAX_DIRECT_ADDRS_PER_RECORD,
    MAX_DISCOVERY_EXCLUDED_NODE_IDS, MAX_DISCOVERY_RECORDS_PER_RESPONSE,
    MAX_GET_PEERS_WANTED_SERVICES, MAX_GET_SERVICES_WANTED, MAX_NODE_RECORD_BODY_BYTES,
    MAX_NODE_RECORD_BYTES, MAX_SERVICES_PER_RECORD, MAX_SERVICE_SUMMARIES_PER_RESPONSE,
    MAX_SERVICE_SUMMARY_BYTES, MAX_ZAKURA_SERVICE_ID_BYTES, MSG_DISCOVERY_GET_PEERS,
    MSG_DISCOVERY_GET_SERVICES, MSG_DISCOVERY_HELLO, MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES,
};
use crate::zakura::{
    wire_codec::{BoundedReader, WireError, WireMessage},
    Cadence, MessageRole, MessageRule, PayloadLen,
};

/// The shortest gap a sender leaves between two `Hello`, `GetPeers`, or
/// `GetServices` messages.
pub(crate) const SEND_INTERVAL: Duration = Duration::from_secs(15);

/// The receiver's bucket for each row with a cadence.
///
/// The capacity covers a connection-wide outage (see [`Cadence::min_capacity`]),
/// so the bucket observes bursts; exhaustion is recorded and the frame still
/// reaches the handler.
const CADENCE: Cadence = Cadence {
    capacity: Cadence::min_capacity(SEND_INTERVAL),
    refill_interval: Duration::from_secs(7),
    send_interval: SEND_INTERVAL,
};

pub(super) const NODE_ID_BYTES: usize = 32;
pub(super) const SIGNATURE_BYTES: usize = 64;
const SOCKET_ADDR_V4: u8 = 4;
const SOCKET_ADDR_V6: u8 = 6;
const SOCKET_ADDR_V4_BYTES: usize = 1 + 4 + 2;
const SOCKET_ADDR_V6_BYTES: usize = 1 + 16 + 2 + 4 + 4;
const SERVICE_ID_MIN_BYTES: usize = 2 + 1;
const SERVICE_ID_MAX_BYTES: usize = 2 + MAX_ZAKURA_SERVICE_ID_BYTES;

/// A record body's fixed fields: node id, empty address and service lists,
/// protocol range, network id, chain id, sequence, and expiry.
pub(crate) const RECORD_BODY_FIXED_BYTES: usize = NODE_ID_BYTES + 2 + 2 + 2 + 2 + 4 + 32 + 8 + 8;
/// The smallest encoded record: its length, the fixed body, and the signature.
pub(crate) const NODE_RECORD_MIN_BYTES: usize = 4 + RECORD_BODY_FIXED_BYTES + SIGNATURE_BYTES;

/// A `GetPeers` with no wanted services and no exclusions: the limit and both
/// counts.
const GET_PEERS_MIN_BYTES: usize = 2 + 2 + 2;
const GET_PEERS_MAX_BYTES: usize = 2
    + 2
    + MAX_GET_PEERS_WANTED_SERVICES * SERVICE_ID_MAX_BYTES
    + 2
    + MAX_DISCOVERY_EXCLUDED_NODE_IDS * NODE_ID_BYTES;
/// The largest `Peers` payload.
pub(crate) const PEERS_MAX_BYTES: usize =
    2 + MAX_DISCOVERY_RECORDS_PER_RESPONSE * MAX_NODE_RECORD_BYTES;
const GET_SERVICES_MAX_BYTES: usize = 2 + MAX_GET_SERVICES_WANTED * SERVICE_ID_MAX_BYTES;
/// A `Services` payload's fixed prefix: node id, expiry, and summary count.
pub(crate) const SERVICES_FIXED_BYTES: usize = NODE_ID_BYTES + 8 + 2;
const SUMMARY_ENVELOPE_MIN_BYTES: usize = SERVICE_ID_MIN_BYTES + 2 + 2;
/// The largest encoded summary envelope.
pub(crate) const SUMMARY_ENVELOPE_MAX_BYTES: usize =
    SERVICE_ID_MAX_BYTES + 2 + 2 + MAX_SERVICE_SUMMARY_BYTES;
const SERVICES_MAX_BYTES: usize =
    SERVICES_FIXED_BYTES + MAX_SERVICE_SUMMARIES_PER_RESPONSE * SUMMARY_ENVELOPE_MAX_BYTES;

// The body cap leaves no extension room in a record with the most and largest
// addresses and services.
const _: () = assert!(
    RECORD_BODY_FIXED_BYTES
        + MAX_DIRECT_ADDRS_PER_RECORD * SOCKET_ADDR_V6_BYTES
        + MAX_SERVICES_PER_RECORD * SERVICE_ID_MAX_BYTES
        == MAX_NODE_RECORD_BODY_BYTES
);

const fn row(message_type: u8, payload: PayloadLen, role: MessageRole) -> MessageRule {
    MessageRule {
        // A u8 discriminator always fits in u16.
        message_type: message_type as u16,
        payload,
        role,
    }
}

pub(crate) const HELLO: MessageRule = row(
    MSG_DISCOVERY_HELLO,
    PayloadLen::between(NODE_RECORD_MIN_BYTES, MAX_NODE_RECORD_BYTES),
    MessageRole::Announcement { cadence: CADENCE },
);

pub(crate) const GET_PEERS: MessageRule = row(
    MSG_DISCOVERY_GET_PEERS,
    PayloadLen::between(GET_PEERS_MIN_BYTES, GET_PEERS_MAX_BYTES),
    MessageRole::Request {
        max_in_flight: 1,
        cadence: Some(CADENCE),
    },
);

pub(crate) const PEERS: MessageRule = row(
    MSG_DISCOVERY_PEERS,
    PayloadLen::between(2, PEERS_MAX_BYTES),
    MessageRole::Response {
        request: GET_PEERS.message_type,
        ends_exchange: true,
    },
);

pub(crate) const GET_SERVICES: MessageRule = row(
    MSG_DISCOVERY_GET_SERVICES,
    PayloadLen::between(2, GET_SERVICES_MAX_BYTES),
    MessageRole::Request {
        max_in_flight: 1,
        cadence: Some(CADENCE),
    },
);

pub(crate) const SERVICES: MessageRule = row(
    MSG_DISCOVERY_SERVICES,
    PayloadLen::between(SERVICES_FIXED_BYTES, SERVICES_MAX_BYTES),
    MessageRole::Response {
        request: GET_SERVICES.message_type,
        ends_exchange: true,
    },
);

/// Every discovery row.
pub(crate) const RULES: &[MessageRule] = &[HELLO, GET_PEERS, PEERS, GET_SERVICES, SERVICES];

/// Heap that decoding one record body may request: its address and service
/// lists, each service id's bytes, and the extension area.
const RECORD_HEAP_BYTES: usize = MAX_DIRECT_ADDRS_PER_RECORD * size_of::<SocketAddr>()
    + MAX_SERVICES_PER_RECORD * (size_of::<ZakuraServiceId>() + MAX_ZAKURA_SERVICE_ID_BYTES)
    + MAX_NODE_RECORD_BODY_BYTES;

/// Heap for a list of at most `max` service ids read from `payload_len` bytes.
fn service_ids_heap_bytes(max: usize, payload_len: usize) -> usize {
    max.min(payload_len / SERVICE_ID_MIN_BYTES)
        * (size_of::<ZakuraServiceId>() + MAX_ZAKURA_SERVICE_ID_BYTES)
}

impl WireMessage for DiscoveryMessage {
    type Error = WireError;
    const RULES: &'static [MessageRule] = RULES;

    fn message_type(&self) -> u16 {
        u16::from(match self {
            Self::Hello { .. } => MSG_DISCOVERY_HELLO,
            Self::GetPeers { .. } => MSG_DISCOVERY_GET_PEERS,
            Self::Peers { .. } => MSG_DISCOVERY_PEERS,
            Self::GetServices(_) => MSG_DISCOVERY_GET_SERVICES,
            Self::Services(_) => MSG_DISCOVERY_SERVICES,
        })
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        match self {
            Self::Hello { record } => write_record(record, out),
            Self::GetPeers {
                limit,
                wanted_services,
                exclude_node_ids,
            } => {
                check_limit(*limit)?;
                out.extend_from_slice(&limit.to_le_bytes());
                write_wanted(wanted_services, MAX_GET_PEERS_WANTED_SERVICES, out)?;
                write_list(
                    exclude_node_ids,
                    MAX_DISCOVERY_EXCLUDED_NODE_IDS,
                    out,
                    |node_id, out| {
                        out.extend_from_slice(node_id.as_bytes());
                        Ok(())
                    },
                )?;
                check_sorted_unique(exclude_node_ids)
            }
            Self::Peers { records } => {
                write_list(
                    records,
                    MAX_DISCOVERY_RECORDS_PER_RESPONSE,
                    out,
                    write_record,
                )?;
                check_unique(
                    records,
                    |record| &record.body.node_id,
                    "unique record node ids",
                )
            }
            Self::GetServices(query) => {
                write_wanted(&query.wanted_services, MAX_GET_SERVICES_WANTED, out)
            }
            Self::Services(services) => {
                out.extend_from_slice(services.node_id.as_bytes());
                out.extend_from_slice(&services.expires_at_unix_secs.to_le_bytes());
                write_list(
                    &services.summaries,
                    MAX_SERVICE_SUMMARIES_PER_RESPONSE,
                    out,
                    write_envelope,
                )?;
                check_unique(
                    &services.summaries,
                    |envelope| &envelope.service_id,
                    "unique summary service ids",
                )
            }
        }
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        let Ok(tag) = u8::try_from(message_type) else {
            return Err(WireError::UnknownMessageType(message_type));
        };
        Ok(match tag {
            MSG_DISCOVERY_HELLO => Self::Hello {
                record: read_record(reader)?,
            },
            MSG_DISCOVERY_GET_PEERS => {
                let limit = read_u16(reader, "peer limit")?;
                check_limit(limit)?;
                let wanted_services = read_wanted(reader, MAX_GET_PEERS_WANTED_SERVICES)?;
                let exclude_node_ids = read_list(
                    reader,
                    MAX_DISCOVERY_EXCLUDED_NODE_IDS,
                    NODE_ID_BYTES,
                    "excluded node id count",
                    |reader| read_node_id(reader, "excluded node id"),
                )?;
                check_sorted_unique(&exclude_node_ids)?;
                Self::GetPeers {
                    limit,
                    wanted_services,
                    exclude_node_ids,
                }
            }
            MSG_DISCOVERY_PEERS => {
                let records = read_list(
                    reader,
                    MAX_DISCOVERY_RECORDS_PER_RESPONSE,
                    NODE_RECORD_MIN_BYTES,
                    "record count",
                    read_record,
                )?;
                check_unique(
                    &records,
                    |record| &record.body.node_id,
                    "unique record node ids",
                )?;
                Self::Peers { records }
            }
            MSG_DISCOVERY_GET_SERVICES => Self::GetServices(GetServices {
                wanted_services: read_wanted(reader, MAX_GET_SERVICES_WANTED)?,
            }),
            MSG_DISCOVERY_SERVICES => {
                let node_id = read_node_id(reader, "services node id")?;
                let expires_at_unix_secs = read_u64(reader, "services expiry")?;
                let summaries = read_list(
                    reader,
                    MAX_SERVICE_SUMMARIES_PER_RESPONSE,
                    SUMMARY_ENVELOPE_MIN_BYTES,
                    "summary count",
                    read_envelope,
                )?;
                check_unique(
                    &summaries,
                    |envelope| &envelope.service_id,
                    "unique summary service ids",
                )?;
                Self::Services(Services {
                    node_id,
                    expires_at_unix_secs,
                    summaries,
                })
            }
            _ => return Err(WireError::UnknownMessageType(message_type)),
        })
    }

    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize {
        let Ok(tag) = u8::try_from(message_type) else {
            return 0;
        };
        match tag {
            MSG_DISCOVERY_HELLO => RECORD_HEAP_BYTES,
            MSG_DISCOVERY_GET_PEERS => {
                service_ids_heap_bytes(MAX_GET_PEERS_WANTED_SERVICES, payload_len)
                    + MAX_DISCOVERY_EXCLUDED_NODE_IDS.min(payload_len / NODE_ID_BYTES)
                        * size_of::<NodeId>()
            }
            MSG_DISCOVERY_PEERS => {
                MAX_DISCOVERY_RECORDS_PER_RESPONSE.min(payload_len / NODE_RECORD_MIN_BYTES)
                    * (size_of::<ZakuraNodeRecord>() + RECORD_HEAP_BYTES)
            }
            MSG_DISCOVERY_GET_SERVICES => {
                service_ids_heap_bytes(MAX_GET_SERVICES_WANTED, payload_len)
            }
            MSG_DISCOVERY_SERVICES => {
                MAX_SERVICE_SUMMARIES_PER_RESPONSE.min(payload_len / SUMMARY_ENVELOPE_MIN_BYTES)
                    * (size_of::<ServiceSummaryEnvelope>()
                        + MAX_ZAKURA_SERVICE_ID_BYTES
                        + MAX_SERVICE_SUMMARY_BYTES)
            }
            _ => 0,
        }
    }
}

fn check_limit(limit: u16) -> Result<(), WireError> {
    if !(1..=MAX_DISCOVERY_RECORDS_PER_RESPONSE).contains(&usize::from(limit)) {
        return Err(WireError::OutOfRange("peer limit"));
    }
    Ok(())
}

fn check_sorted_unique(node_ids: &[NodeId]) -> Result<(), WireError> {
    if node_ids
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        return Err(WireError::OutOfRange("sorted unique excluded node ids"));
    }
    Ok(())
}

/// Reject a list in which two items share a key. Each such list holds at most
/// 32 items, so a quadratic scan is cheapest.
fn check_unique<T, K: PartialEq>(
    items: &[T],
    key: impl Fn(&T) -> &K,
    what: &'static str,
) -> Result<(), WireError> {
    for (index, item) in items.iter().enumerate() {
        if items[..index]
            .iter()
            .any(|earlier| key(earlier) == key(item))
        {
            return Err(WireError::OutOfRange(what));
        }
    }
    Ok(())
}

// Every list bound fits the `u16` count. Widening u16 to usize is lossless.
const _: () = assert!(
    MAX_DIRECT_ADDRS_PER_RECORD <= u16::MAX as usize
        && MAX_SERVICES_PER_RECORD <= u16::MAX as usize
        && MAX_GET_PEERS_WANTED_SERVICES <= u16::MAX as usize
        && MAX_GET_SERVICES_WANTED <= u16::MAX as usize
        && MAX_DISCOVERY_EXCLUDED_NODE_IDS <= u16::MAX as usize
        && MAX_DISCOVERY_RECORDS_PER_RESPONSE <= u16::MAX as usize
        && MAX_SERVICE_SUMMARIES_PER_RESPONSE <= u16::MAX as usize
);

/// Write a `u16` count. Every caller bounds `count` by a list bound.
fn write_count(count: usize, out: &mut Vec<u8>) {
    // Every list bound fits u16, as asserted above.
    out.extend_from_slice(&(count as u16).to_le_bytes());
}

/// Write a counted list of at most `max` items.
fn write_list<T>(
    items: &[T],
    max: usize,
    out: &mut Vec<u8>,
    mut write_item: impl FnMut(&T, &mut Vec<u8>) -> Result<(), WireError>,
) -> Result<(), WireError> {
    if items.len() > max {
        return Err(WireError::CountOutOfRange {
            // Widening usize to u64 is lossless on supported targets.
            count: items.len() as u64,
            min: 0,
            max,
        });
    }
    write_count(items.len(), out);
    for item in items {
        write_item(item, out)?;
    }
    Ok(())
}

fn read_u16(reader: &mut BoundedReader<'_>, item: &'static str) -> Result<u16, WireError> {
    Ok(u16::from_le_bytes(reader.array(item)?))
}

fn read_u32(reader: &mut BoundedReader<'_>, item: &'static str) -> Result<u32, WireError> {
    Ok(u32::from_le_bytes(reader.array(item)?))
}

fn read_u64(reader: &mut BoundedReader<'_>, item: &'static str) -> Result<u64, WireError> {
    Ok(u64::from_le_bytes(reader.array(item)?))
}

/// Read a `u16` count, bounded by `max` and by the items of at least
/// `min_item` bytes that the rest of the payload can hold.
fn read_count(
    reader: &mut BoundedReader<'_>,
    max: usize,
    min_item: usize,
    item: &'static str,
) -> Result<usize, WireError> {
    let raw = read_u16(reader, item)?;
    let count = usize::from(raw);
    if count > max {
        return Err(WireError::CountOutOfRange {
            count: u64::from(raw),
            min: 0,
            max,
        });
    }
    if count.saturating_mul(min_item) > reader.remaining() {
        return Err(WireError::CountExceedsPayload {
            count: u64::from(raw),
            remaining: reader.remaining(),
        });
    }
    Ok(count)
}

/// Read a counted list, bounded as [`read_count`] bounds it.
fn read_list<'a, T>(
    reader: &mut BoundedReader<'a>,
    max: usize,
    min_item: usize,
    what: &'static str,
    mut read_item: impl FnMut(&mut BoundedReader<'a>) -> Result<T, WireError>,
) -> Result<Vec<T>, WireError> {
    let count = read_count(reader, max, min_item, what)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(read_item(reader)?);
    }
    Ok(items)
}

fn read_node_id(reader: &mut BoundedReader<'_>, item: &'static str) -> Result<NodeId, WireError> {
    NodeId::from_bytes(&reader.array(item)?).map_err(|_| WireError::OutOfRange(item))
}

/// Write a request's wanted list, whose service ids are unique.
fn write_wanted(
    services: &[ZakuraServiceId],
    max: usize,
    out: &mut Vec<u8>,
) -> Result<(), WireError> {
    write_list(services, max, out, |service, out| {
        write_service_id(service, out);
        Ok(())
    })?;
    check_unique(services, |service| service, "unique service ids")
}

/// Read a request's wanted list, whose service ids are unique.
fn read_wanted(
    reader: &mut BoundedReader<'_>,
    max: usize,
) -> Result<Vec<ZakuraServiceId>, WireError> {
    let services = read_list(
        reader,
        max,
        SERVICE_ID_MIN_BYTES,
        "service count",
        read_service_id,
    )?;
    check_unique(&services, |service| service, "unique service ids")?;
    Ok(services)
}

pub(super) fn write_service_id(service: &ZakuraServiceId, out: &mut Vec<u8>) {
    let bytes = service.as_str().as_bytes();
    // A service id has at most 32 bytes.
    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Read a service id. The length check precedes the copy, so a service id
/// never allocates more than its bound.
fn read_service_id(reader: &mut BoundedReader<'_>) -> Result<ZakuraServiceId, WireError> {
    let len = usize::from(read_u16(reader, "service id length")?);
    if len > MAX_ZAKURA_SERVICE_ID_BYTES {
        return Err(WireError::OutOfRange("service id"));
    }
    let bytes = reader.bytes(len, "service id")?;
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|service| ZakuraServiceId::new(service).ok())
        .ok_or(WireError::OutOfRange("service id"))
}

fn write_record(record: &ZakuraNodeRecord, out: &mut Vec<u8>) -> Result<(), WireError> {
    validate_record_body_bounds(&record.body).map_err(|_| WireError::OutOfRange("record body"))?;
    let length_at = out.len();
    out.extend_from_slice(&[0; 4]);
    write_record_body(&record.body, out);
    let body_len = out.len() - length_at - 4;
    if body_len > MAX_NODE_RECORD_BODY_BYTES {
        return Err(WireError::OutOfRange("record body length"));
    }
    // The body cap fits in u32.
    out[length_at..length_at + 4].copy_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&record.signature.to_bytes());
    Ok(())
}

/// Write a record body's canonical bytes, which its signature covers. The
/// caller has checked the body with `validate_record_body_bounds`.
pub(super) fn write_record_body(body: &ZakuraNodeRecordBody, out: &mut Vec<u8>) {
    out.extend_from_slice(body.node_id.as_bytes());
    write_count(body.direct_addrs.len(), out);
    for addr in &body.direct_addrs {
        write_socket_addr(addr, out);
    }
    write_count(body.services.len(), out);
    for service in &body.services {
        write_service_id(service, out);
    }
    out.extend_from_slice(&body.zakura_protocol_min.to_le_bytes());
    out.extend_from_slice(&body.zakura_protocol_max.to_le_bytes());
    out.extend_from_slice(&body.network_id.code().to_le_bytes());
    out.extend_from_slice(&body.chain_id);
    out.extend_from_slice(&body.sequence.to_le_bytes());
    out.extend_from_slice(&body.expires_at_unix_secs.to_le_bytes());
    out.extend_from_slice(&body.extensions);
}

/// The length [`write_record`] writes for `record`, without encoding it.
pub(super) fn record_len(record: &ZakuraNodeRecord) -> usize {
    let body = &record.body;
    let addrs: usize = body
        .direct_addrs
        .iter()
        .map(|addr| match addr {
            SocketAddr::V4(_) => SOCKET_ADDR_V4_BYTES,
            SocketAddr::V6(_) => SOCKET_ADDR_V6_BYTES,
        })
        .sum();
    let services: usize = body
        .services
        .iter()
        .map(|service| 2 + service.as_str().len())
        .sum();
    4 + RECORD_BODY_FIXED_BYTES + addrs + services + body.extensions.len() + SIGNATURE_BYTES
}

fn read_record(reader: &mut BoundedReader<'_>) -> Result<ZakuraNodeRecord, WireError> {
    let body_len = read_u32(reader, "record body length")?;
    let body_len = usize::try_from(body_len)
        .ok()
        .filter(|&len| (RECORD_BODY_FIXED_BYTES..=MAX_NODE_RECORD_BODY_BYTES).contains(&len))
        .ok_or(WireError::OutOfRange("record body length"))?;
    let mut body = BoundedReader::new(reader.bytes(body_len, "record body")?);
    let body = read_record_body(&mut body)?;
    let signature = Signature::from(reader.array::<SIGNATURE_BYTES>("record signature")?);
    Ok(ZakuraNodeRecord { body, signature })
}

/// Read a whole record body: the fixed fields, then the extension area.
fn read_record_body(reader: &mut BoundedReader<'_>) -> Result<ZakuraNodeRecordBody, WireError> {
    let node_id = read_node_id(reader, "record node id")?;
    let direct_addrs = read_list(
        reader,
        MAX_DIRECT_ADDRS_PER_RECORD,
        SOCKET_ADDR_V4_BYTES,
        "direct address count",
        read_socket_addr,
    )?;
    let services = read_list(
        reader,
        MAX_SERVICES_PER_RECORD,
        SERVICE_ID_MIN_BYTES,
        "service count",
        read_service_id,
    )?;
    let zakura_protocol_min = read_u16(reader, "protocol minimum")?;
    let zakura_protocol_max = read_u16(reader, "protocol maximum")?;
    if zakura_protocol_min > zakura_protocol_max {
        return Err(WireError::OutOfRange("protocol range"));
    }
    let network_id = network_id_from_code(read_u32(reader, "network id")?)
        .map_err(|_| WireError::OutOfRange("network id"))?;
    let chain_id = reader.array("chain id")?;
    let sequence = read_u64(reader, "record sequence")?;
    let expires_at_unix_secs = read_u64(reader, "record expiry")?;
    // Every byte after the fixed fields is the signed extension area.
    let extensions = reader.take_remaining().to_vec();
    Ok(ZakuraNodeRecordBody {
        node_id,
        direct_addrs,
        services,
        zakura_protocol_min,
        zakura_protocol_max,
        network_id,
        chain_id,
        sequence,
        expires_at_unix_secs,
        extensions,
    })
}

fn write_socket_addr(addr: &SocketAddr, out: &mut Vec<u8>) {
    match addr {
        SocketAddr::V4(addr) => {
            out.push(SOCKET_ADDR_V4);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_le_bytes());
        }
        SocketAddr::V6(addr) => {
            out.push(SOCKET_ADDR_V6);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_le_bytes());
            out.extend_from_slice(&addr.flowinfo().to_le_bytes());
            out.extend_from_slice(&addr.scope_id().to_le_bytes());
        }
    }
}

/// Read one address. Its contents are address policy, which the handler
/// applies: a relay cannot change a third party's signed bytes.
fn read_socket_addr(reader: &mut BoundedReader<'_>) -> Result<SocketAddr, WireError> {
    let [family] = reader.array("address family")?;
    Ok(match family {
        SOCKET_ADDR_V4 => {
            let ip = Ipv4Addr::from(reader.array::<4>("IPv4 address")?);
            SocketAddr::V4(SocketAddrV4::new(ip, read_u16(reader, "port")?))
        }
        SOCKET_ADDR_V6 => {
            let ip = Ipv6Addr::from(reader.array::<16>("IPv6 address")?);
            let port = read_u16(reader, "port")?;
            let flowinfo = read_u32(reader, "IPv6 flow info")?;
            let scope_id = read_u32(reader, "IPv6 scope id")?;
            SocketAddr::V6(SocketAddrV6::new(ip, port, flowinfo, scope_id))
        }
        _ => return Err(WireError::OutOfRange("address family")),
    })
}

/// Check a known summary's tag against its service id, and its payload
/// against its decoder. Unknown tags stay opaque and length-bounded.
fn check_envelope(envelope: &ServiceSummaryEnvelope) -> Result<(), WireError> {
    if summary_tag_service(envelope.summary_tag)
        .is_some_and(|service| service != envelope.service_id.as_str())
    {
        return Err(WireError::OutOfRange("summary tag service"));
    }
    validate_known_summary_payload(envelope.summary_tag, &envelope.summary_bytes)
        .map_err(|_| WireError::OutOfRange("service summary"))
}

fn write_envelope(envelope: &ServiceSummaryEnvelope, out: &mut Vec<u8>) -> Result<(), WireError> {
    if envelope.summary_bytes.len() > MAX_SERVICE_SUMMARY_BYTES {
        return Err(WireError::OutOfRange("service summary length"));
    }
    check_envelope(envelope)?;
    write_service_id(&envelope.service_id, out);
    out.extend_from_slice(&envelope.summary_tag.to_le_bytes());
    // The summary cap fits in u16.
    out.extend_from_slice(&(envelope.summary_bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(&envelope.summary_bytes);
    Ok(())
}

fn read_envelope(reader: &mut BoundedReader<'_>) -> Result<ServiceSummaryEnvelope, WireError> {
    let service_id = read_service_id(reader)?;
    let summary_tag = read_u16(reader, "summary tag")?;
    let len = usize::from(read_u16(reader, "summary length")?);
    if len > MAX_SERVICE_SUMMARY_BYTES {
        return Err(WireError::OutOfRange("service summary length"));
    }
    let envelope = ServiceSummaryEnvelope {
        service_id,
        summary_tag,
        summary_bytes: reader.bytes(len, "service summary")?.to_vec(),
    };
    check_envelope(&envelope)?;
    Ok(envelope)
}
