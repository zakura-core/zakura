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
    encode_record_body_to, network_id_from_code, summary_tag_service,
    validate_known_summary_payload, validate_record_body_bounds, DiscoveryMessage, GetServices,
    ServiceSummaryEnvelope, Services, ZakuraNodeRecord, ZakuraNodeRecordBody, ZakuraServiceId,
    MAX_DIRECT_ADDRS_PER_RECORD, MAX_DISCOVERY_EXCLUDED_NODE_IDS,
    MAX_DISCOVERY_RECORDS_PER_RESPONSE, MAX_GET_PEERS_WANTED_SERVICES, MAX_GET_SERVICES_WANTED,
    MAX_NODE_RECORD_BODY_BYTES, MAX_NODE_RECORD_BYTES, MAX_SERVICES_PER_RECORD,
    MAX_SERVICE_SUMMARIES_PER_RESPONSE, MAX_SERVICE_SUMMARY_BYTES, MAX_ZAKURA_SERVICE_ID_BYTES,
    MSG_DISCOVERY_GET_PEERS, MSG_DISCOVERY_GET_SERVICES, MSG_DISCOVERY_HELLO, MSG_DISCOVERY_PEERS,
    MSG_DISCOVERY_SERVICES,
};
use crate::zakura::{
    regulation::ResponseCap,
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

const NODE_ID_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
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

const GET_PEERS_MAX_BYTES: usize = 2
    + 2
    + MAX_GET_PEERS_WANTED_SERVICES * SERVICE_ID_MAX_BYTES
    + 2
    + MAX_DISCOVERY_EXCLUDED_NODE_IDS * NODE_ID_BYTES;
/// The largest `Peers` payload.
pub(crate) const PEERS_MAX_BYTES: usize =
    2 + MAX_DISCOVERY_RECORDS_PER_RESPONSE * MAX_NODE_RECORD_BYTES;

/// The most a `Peers` answer to a `GetPeers` with `limit` may carry: one frame
/// with the record count and at most `limit` records. The responder's `Serve`
/// reserves output for it.
pub(crate) fn peers_cap(limit: u16) -> ResponseCap {
    // Widening usize to u64 is lossless on supported targets.
    ResponseCap {
        frames: 0,
        bytes: 2 + u64::from(limit) * MAX_NODE_RECORD_BYTES as u64,
    }
}
const GET_SERVICES_MAX_BYTES: usize = 2 + MAX_GET_SERVICES_WANTED * SERVICE_ID_MAX_BYTES;
/// A `Services` payload's fixed prefix: node id, expiry, and summary count.
pub(crate) const SERVICES_FIXED_BYTES: usize = NODE_ID_BYTES + 8 + 2;
const SUMMARY_ENVELOPE_MIN_BYTES: usize = SERVICE_ID_MIN_BYTES + 2 + 2;
/// The largest encoded summary envelope.
pub(crate) const SUMMARY_ENVELOPE_MAX_BYTES: usize =
    SERVICE_ID_MAX_BYTES + 2 + 2 + MAX_SERVICE_SUMMARY_BYTES;
const SERVICES_MAX_BYTES: usize =
    SERVICES_FIXED_BYTES + MAX_SERVICE_SUMMARIES_PER_RESPONSE * SUMMARY_ENVELOPE_MAX_BYTES;

/// The most a `Services` answer to a `GetServices` naming `wanted` services may
/// carry: the fixed prefix and one largest envelope per allowed summary. A
/// nonempty wanted list allows one summary per wanted service, and an empty
/// list asks for every service. Both allow at most 8 summaries. The
/// responder's `Serve` reserves output for it.
pub(crate) fn services_cap(wanted: usize) -> ResponseCap {
    let summaries = match wanted {
        0 => MAX_SERVICE_SUMMARIES_PER_RESPONSE,
        wanted => wanted.min(MAX_SERVICE_SUMMARIES_PER_RESPONSE),
    };
    // Widening usize to u64 is lossless on supported targets.
    ResponseCap {
        frames: 0,
        bytes: (SERVICES_FIXED_BYTES + summaries * SUMMARY_ENVELOPE_MAX_BYTES) as u64,
    }
}

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
    PayloadLen::between(2 + 2 + 2, GET_PEERS_MAX_BYTES),
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
                write_service_ids(wanted_services, MAX_GET_PEERS_WANTED_SERVICES, true, out)?;
                check_sorted_unique(exclude_node_ids)?;
                write_count(exclude_node_ids.len(), MAX_DISCOVERY_EXCLUDED_NODE_IDS, out)?;
                for node_id in exclude_node_ids {
                    out.extend_from_slice(node_id);
                }
                Ok(())
            }
            Self::Peers { records } => {
                write_count(records.len(), MAX_DISCOVERY_RECORDS_PER_RESPONSE, out)?;
                for (index, record) in records.iter().enumerate() {
                    if records[..index]
                        .iter()
                        .any(|earlier| earlier.body.node_id == record.body.node_id)
                    {
                        return Err(WireError::OutOfRange("unique record node ids"));
                    }
                    write_record(record, out)?;
                }
                Ok(())
            }
            Self::GetServices(query) => {
                write_service_ids(&query.wanted_services, MAX_GET_SERVICES_WANTED, true, out)
            }
            Self::Services(services) => {
                out.extend_from_slice(services.node_id.as_bytes());
                out.extend_from_slice(&services.expires_at_unix_secs.to_le_bytes());
                write_count(
                    services.summaries.len(),
                    MAX_SERVICE_SUMMARIES_PER_RESPONSE,
                    out,
                )?;
                for (index, envelope) in services.summaries.iter().enumerate() {
                    if services.summaries[..index]
                        .iter()
                        .any(|earlier| earlier.service_id == envelope.service_id)
                    {
                        return Err(WireError::OutOfRange("unique summary service ids"));
                    }
                    write_envelope(envelope, out)?;
                }
                Ok(())
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
                let wanted_services =
                    read_service_ids(reader, MAX_GET_PEERS_WANTED_SERVICES, true)?;
                let count = read_count(
                    reader,
                    MAX_DISCOVERY_EXCLUDED_NODE_IDS,
                    NODE_ID_BYTES,
                    "excluded node id count",
                )?;
                let mut exclude_node_ids = Vec::with_capacity(count);
                for _ in 0..count {
                    // Raw bytes: the responder only compares them, so it never
                    // pays a curve point decompression per excluded id.
                    exclude_node_ids.push(reader.array("excluded node id")?);
                }
                check_sorted_unique(&exclude_node_ids)?;
                Self::GetPeers {
                    limit,
                    wanted_services,
                    exclude_node_ids,
                }
            }
            MSG_DISCOVERY_PEERS => {
                let count = read_count(
                    reader,
                    MAX_DISCOVERY_RECORDS_PER_RESPONSE,
                    NODE_RECORD_MIN_BYTES,
                    "record count",
                )?;
                let mut records: Vec<ZakuraNodeRecord> = Vec::with_capacity(count);
                for _ in 0..count {
                    let record = read_record(reader)?;
                    if records
                        .iter()
                        .any(|earlier| earlier.body.node_id == record.body.node_id)
                    {
                        return Err(WireError::OutOfRange("unique record node ids"));
                    }
                    records.push(record);
                }
                Self::Peers { records }
            }
            MSG_DISCOVERY_GET_SERVICES => Self::GetServices(GetServices {
                wanted_services: read_service_ids(reader, MAX_GET_SERVICES_WANTED, true)?,
            }),
            MSG_DISCOVERY_SERVICES => {
                let node_id = read_node_id(reader, "services node id")?;
                let expires_at_unix_secs = read_u64(reader, "services expiry")?;
                let count = read_count(
                    reader,
                    MAX_SERVICE_SUMMARIES_PER_RESPONSE,
                    SUMMARY_ENVELOPE_MIN_BYTES,
                    "summary count",
                )?;
                let mut summaries: Vec<ServiceSummaryEnvelope> = Vec::with_capacity(count);
                for _ in 0..count {
                    let envelope = read_envelope(reader)?;
                    if summaries
                        .iter()
                        .any(|earlier| earlier.service_id == envelope.service_id)
                    {
                        return Err(WireError::OutOfRange("unique summary service ids"));
                    }
                    summaries.push(envelope);
                }
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

fn check_sorted_unique(node_ids: &[[u8; NODE_ID_BYTES]]) -> Result<(), WireError> {
    if node_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(WireError::OutOfRange("sorted unique excluded node ids"));
    }
    Ok(())
}

fn write_count(count: usize, max: usize, out: &mut Vec<u8>) -> Result<(), WireError> {
    if count > max {
        return Err(WireError::CountOutOfRange {
            count: u64::try_from(count).unwrap_or(u64::MAX),
            min: 0,
            max,
        });
    }
    let count = u16::try_from(count).map_err(|_| WireError::OutOfRange("list count"))?;
    out.extend_from_slice(&count.to_le_bytes());
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
    let count = usize::from(read_u16(reader, item)?);
    if count > max {
        return Err(WireError::CountOutOfRange {
            count: u64::from(u16::try_from(count).unwrap_or(u16::MAX)),
            min: 0,
            max,
        });
    }
    if count.saturating_mul(min_item) > reader.remaining() {
        return Err(WireError::CountExceedsPayload {
            count: u64::from(u16::try_from(count).unwrap_or(u16::MAX)),
            remaining: reader.remaining(),
        });
    }
    Ok(count)
}

fn read_node_id(reader: &mut BoundedReader<'_>, item: &'static str) -> Result<NodeId, WireError> {
    NodeId::from_bytes(&reader.array(item)?).map_err(|_| WireError::OutOfRange(item))
}

fn check_service_id(bytes: &[u8]) -> Result<(), WireError> {
    if !(1..=MAX_ZAKURA_SERVICE_ID_BYTES).contains(&bytes.len()) || !bytes.is_ascii() {
        return Err(WireError::OutOfRange("service id"));
    }
    Ok(())
}

fn write_service_ids(
    services: &[ZakuraServiceId],
    max: usize,
    unique: bool,
    out: &mut Vec<u8>,
) -> Result<(), WireError> {
    write_count(services.len(), max, out)?;
    for (index, service) in services.iter().enumerate() {
        if unique && services[..index].contains(service) {
            return Err(WireError::OutOfRange("unique service ids"));
        }
        write_service_id(service, out)?;
    }
    Ok(())
}

fn write_service_id(service: &ZakuraServiceId, out: &mut Vec<u8>) -> Result<(), WireError> {
    let bytes = service.as_str().as_bytes();
    check_service_id(bytes)?;
    // A checked service id has at most 32 bytes.
    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_service_ids(
    reader: &mut BoundedReader<'_>,
    max: usize,
    unique: bool,
) -> Result<Vec<ZakuraServiceId>, WireError> {
    let count = read_count(reader, max, SERVICE_ID_MIN_BYTES, "service count")?;
    let mut services: Vec<ZakuraServiceId> = Vec::with_capacity(count);
    for _ in 0..count {
        let service = read_service_id(reader)?;
        if unique && services.contains(&service) {
            return Err(WireError::OutOfRange("unique service ids"));
        }
        services.push(service);
    }
    Ok(services)
}

fn read_service_id(reader: &mut BoundedReader<'_>) -> Result<ZakuraServiceId, WireError> {
    let len = usize::from(read_u16(reader, "service id length")?);
    if len > MAX_ZAKURA_SERVICE_ID_BYTES {
        return Err(WireError::OutOfRange("service id"));
    }
    let bytes = reader.bytes(len, "service id")?;
    check_service_id(bytes)?;
    let service = std::str::from_utf8(bytes).map_err(|_| WireError::OutOfRange("service id"))?;
    ZakuraServiceId::new(service).map_err(|_| WireError::OutOfRange("service id"))
}

fn write_record(record: &ZakuraNodeRecord, out: &mut Vec<u8>) -> Result<(), WireError> {
    validate_record_body_bounds(&record.body).map_err(|_| WireError::OutOfRange("record body"))?;
    let length_at = out.len();
    out.extend_from_slice(&[0; 4]);
    encode_record_body_to(&record.body, out).map_err(|_| WireError::OutOfRange("record body"))?;
    let body_len = out.len() - length_at - 4;
    if body_len > MAX_NODE_RECORD_BODY_BYTES {
        return Err(WireError::OutOfRange("record body length"));
    }
    // The body cap fits in u32.
    out[length_at..length_at + 4].copy_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&record.signature.to_bytes());
    Ok(())
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
    let count = read_count(
        reader,
        MAX_DIRECT_ADDRS_PER_RECORD,
        SOCKET_ADDR_V4_BYTES,
        "direct address count",
    )?;
    let mut direct_addrs = Vec::with_capacity(count);
    for _ in 0..count {
        direct_addrs.push(read_socket_addr(reader)?);
    }
    let services = read_service_ids(reader, MAX_SERVICES_PER_RECORD, false)?;
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
    write_service_id(&envelope.service_id, out)?;
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
