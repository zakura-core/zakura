//! One-shot response reuse through the real discovery codec. GetPeers has no
//! wire request id, so this adapter allows one outstanding query per scope.
//! Production discovery migration and subscription behavior remain separate.

use super::*;
use crate::zakura::{
    discovery::{
        DiscoveryMessage, DiscoveryRecordValidationContext, ZakuraNodeRecord, ZakuraNodeRecordBody,
        ZakuraServiceId, MAX_DISCOVERY_MESSAGE_BYTES, MSG_DISCOVERY_PEERS,
    },
    CloseCause, Frame, ZakuraHandshakeConfig,
};
use iroh::EndpointId;
use tokio_util::sync::CancellationToken;
use zakura_chain::parameters::Network;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    NoAuthorization,
    Envelope,
    Bytes,
    Count,
    Encoding,
    Identity,
    Accepted,
}

struct DiscoveryResponse {
    scope: ResponseScope,
    authorization: Option<ResponseAuthorization>,
    writer: ResponseWritePermission,
    credit: ResponseCredit,
    excluded: Option<EndpointId>,
    started: bool,
    interested: bool,
    decodes: usize,
    handled: usize,
    connection: CancellationToken,
}

impl DiscoveryResponse {
    fn new(limit: u16, max_bytes: u64, exclude: bool) -> Self {
        let query = DiscoveryMessage::GetPeers {
            limit,
            wanted_services: vec![],
            exclude_node_ids: if exclude {
                vec![records()[1].body.node_id]
            } else {
                vec![]
            },
        }
        .encode()
        .unwrap();
        let DiscoveryMessage::GetPeers {
            limit,
            exclude_node_ids,
            ..
        } = DiscoveryMessage::decode(&query).unwrap()
        else {
            panic!("the fixture encoded a GetPeers query")
        };
        let connection = CancellationToken::new();
        let scope = ResponseScope::new(connection.clone(), CloseCause::new());
        let authorization = scope
            .authorize_with_metadata(u64::try_from(std::mem::size_of::<Self>()).unwrap())
            .unwrap();
        let writer = authorization.write_permission();
        assert!(writer.publish(|| {}));
        Self {
            scope,
            authorization: Some(authorization),
            writer,
            credit: ResponseCredit::new(u64::from(limit), max_bytes),
            excluded: exclude_node_ids.first().copied(),
            started: false,
            interested: true,
            decodes: 0,
            handled: 0,
            connection,
        }
    }

    fn start(&mut self) -> bool {
        let started = self.writer.try_start(|| true);
        self.started |= started;
        started
    }

    fn receive(&mut self, frame: &Frame, handler_fails: bool) -> Verdict {
        if !self.started || self.authorization.is_none() || self.connection.is_cancelled() {
            return Verdict::NoAuthorization;
        }
        if frame.message_type != 1 || frame.flags != 0 {
            return Verdict::Envelope;
        }
        let bytes = u64::try_from(frame.payload.len()).unwrap();
        if self.credit.check(0, bytes).is_err() {
            return Verdict::Bytes;
        }
        // Only the fixed type/count prefix is inspected before bounded decode.
        let [kind, low, high, ..] = frame.payload.as_slice() else {
            return Verdict::Encoding;
        };
        if *kind != MSG_DISCOVERY_PEERS {
            return Verdict::Encoding;
        }
        let count = u64::from(u16::from_le_bytes([*low, *high]));
        if self.credit.check(count, bytes).is_err() {
            return Verdict::Count;
        }
        self.decodes += 1;
        let Ok(DiscoveryMessage::Peers { records }) = DiscoveryMessage::decode(&frame.payload)
        else {
            return Verdict::Encoding;
        };
        if records.iter().any(|record| {
            Some(record.body.node_id) == self.excluded || record.verify(&record_context()).is_err()
        }) {
            return Verdict::Identity;
        }
        self.credit.consume(count, bytes).unwrap();
        // A legal empty Peers is the whole one-shot response. It has no separate
        // ending frame, unlike GetBlocks, and consumes authorization exactly once.
        let mut authorization = self.authorization.take().unwrap();
        authorization.finish();
        if self.interested && !handler_fails {
            self.handled += 1;
        }
        Verdict::Accepted
    }
}

fn record_context() -> DiscoveryRecordValidationContext {
    let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    DiscoveryRecordValidationContext {
        expected_network_id: handshake.network_id,
        expected_chain_id: handshake.chain_id,
        current_unix_secs: 1_800_000_000,
        supported_protocol_min: handshake.zakura_protocol_min,
        supported_protocol_max: handshake.zakura_protocol_max,
        max_record_ttl: std::time::Duration::from_secs(120),
        clock_skew_tolerance: std::time::Duration::ZERO,
    }
}

fn records() -> &'static [ZakuraNodeRecord; 2] {
    static RECORDS: std::sync::OnceLock<[ZakuraNodeRecord; 2]> = std::sync::OnceLock::new();
    RECORDS.get_or_init(|| {
        [1, 2].map(|byte| {
            let secret = iroh::SecretKey::from_bytes(&[byte; 32]);
            let context = record_context();
            ZakuraNodeRecord::sign(
                ZakuraNodeRecordBody {
                    node_id: secret.public(),
                    direct_addrs: vec!["8.8.8.8:8234".parse().unwrap()],
                    services: vec![ZakuraServiceId::discovery()],
                    zakura_protocol_min: context.supported_protocol_min,
                    zakura_protocol_max: context.supported_protocol_max,
                    network_id: context.expected_network_id,
                    chain_id: context.expected_chain_id,
                    sequence: 1,
                    expires_at_unix_secs: context.current_unix_secs + 60,
                },
                &secret,
            )
            .unwrap()
        })
    })
}

// The oracle uses these generated facts, not decoding or production transitions.
struct ResponseCase {
    frame: Frame,
    count: u64,
    excluded_identity: bool,
    encoding_valid: bool,
    prefix_valid: bool,
}

fn response_case(choice: u8) -> ResponseCase {
    let values = match choice {
        0 => vec![],
        2 => records().to_vec(),
        3 => vec![records()[1].clone()],
        _ => vec![records()[0].clone()],
    };
    let count = u64::try_from(values.len()).unwrap();
    let mut payload = DiscoveryMessage::Peers { records: values }
        .encode()
        .unwrap();
    if choice == 4 {
        payload.push(0);
    } else if choice == 5 {
        payload[0] = 0;
    } else if choice == 6 {
        payload.truncate(1);
    }
    ResponseCase {
        frame: Frame {
            message_type: 1,
            flags: u16::from(choice == 7),
            payload,
        },
        count,
        excluded_identity: matches!(choice, 2 | 3),
        encoding_valid: !matches!(choice, 4..=6),
        prefix_valid: !matches!(choice, 5 | 6),
    }
}

proptest! {
    #[test]
    fn discovery_response_histories_use_shared_authorization_and_credit(
        limit in 1u16..=2,
        exclude in any::<bool>(),
        byte_boundary in 0usize..4,
        actions in prop::collection::vec((0u8..5, 0u8..8, any::<bool>()), 1..80),
    ) {
        let one_bytes = response_case(1).frame.payload.len();
        let bytes = u64::try_from([3, one_bytes - 1, one_bytes, MAX_DISCOVERY_MESSAGE_BYTES][byte_boundary]).unwrap();
        let mut adapter = DiscoveryResponse::new(limit, bytes, exclude);
        let (mut started, mut ended, mut closed, mut retired, mut interested) = (false, false, false, false, true);
        let (mut consumed_count, mut consumed_bytes, mut handled) = (0, 0, 0);
        for (action, choice, handler_fails) in actions {
            match action {
                0 => {
                    let expected = !started && !ended && !closed && !retired;
                    prop_assert_eq!(adapter.start(), expected);
                    started |= expected;
                }
                1 => {
                    let case = response_case(choice);
                    let payload_bytes = u64::try_from(case.frame.payload.len()).unwrap();
                    let expected = if !started || ended || closed {
                        Verdict::NoAuthorization
                    } else if choice == 7 {
                        Verdict::Envelope
                    } else if payload_bytes > bytes {
                        Verdict::Bytes
                    } else if !case.prefix_valid {
                        Verdict::Encoding
                    } else if case.count > u64::from(limit) {
                        Verdict::Count
                    } else if !case.encoding_valid {
                        Verdict::Encoding
                    } else if exclude && case.excluded_identity {
                        Verdict::Identity
                    } else {
                        Verdict::Accepted
                    };
                    let before_decodes = adapter.decodes;
                    prop_assert_eq!(adapter.receive(&case.frame, handler_fails), expected);
                    if matches!(expected, Verdict::NoAuthorization | Verdict::Envelope | Verdict::Bytes | Verdict::Count)
                        || !case.prefix_valid
                    {
                        prop_assert_eq!(adapter.decodes, before_decodes);
                    }
                    if expected == Verdict::Accepted {
                        ended = true;
                        consumed_count = case.count;
                        consumed_bytes = payload_bytes;
                        handled += usize::from(interested && !handler_fails);
                    }
                }
                2 => {
                    // Losing local interest is not a response terminal.
                    interested = false;
                    adapter.interested = false;
                }
                3 => {
                    retired = true;
                    closed |= started && !ended;
                    prop_assert_eq!(adapter.scope.retire(), !closed);
                }
                _ => {
                    closed = true;
                    adapter.connection.cancel();
                }
            }
            prop_assert_eq!(adapter.connection.is_cancelled(), closed);
            prop_assert_eq!(adapter.credit.consumed_objects(), consumed_count);
            prop_assert_eq!(adapter.credit.consumed_bytes(), consumed_bytes);
            prop_assert_eq!(adapter.handled, handled);
        }
        let connection = adapter.connection.clone();
        closed |= started && !ended;
        drop(adapter);
        prop_assert_eq!(connection.is_cancelled(), closed);
    }
}

#[test]
fn empty_discovery_response_completes_once_even_after_local_interest_ends() {
    let mut adapter = DiscoveryResponse::new(1, 3, false);
    assert!(adapter.start());
    adapter.interested = false;
    let empty = response_case(0).frame;
    assert_eq!(adapter.receive(&empty, true), Verdict::Accepted);
    assert_eq!(adapter.credit.consumed_objects(), 0);
    assert_eq!(adapter.credit.consumed_bytes(), 3);
    assert_eq!(adapter.receive(&empty, false), Verdict::NoAuthorization);
    assert_eq!(adapter.handled, 0);
    assert!(adapter.scope.retire());
    assert!(!adapter.connection.is_cancelled());
}

#[test]
fn discovery_response_checks_real_records_and_does_not_restore_failed_handling() {
    for handler_fails in [false, true] {
        let mut adapter =
            DiscoveryResponse::new(1, u64::try_from(MAX_DISCOVERY_MESSAGE_BYTES).unwrap(), true);
        assert!(adapter.start());
        let excluded = response_case(3).frame;
        assert_eq!(adapter.receive(&excluded, false), Verdict::Identity);
        assert_eq!(adapter.credit.consumed_objects(), 0);
        let allowed = response_case(1).frame;
        let mut forged = allowed.clone();
        // The last 64 bytes are the record's signature in the production codec.
        *forged.payload.last_mut().unwrap() ^= 1;
        assert_eq!(adapter.receive(&forged, false), Verdict::Identity);
        assert_eq!(adapter.credit.consumed_objects(), 0);
        assert_eq!(adapter.receive(&allowed, handler_fails), Verdict::Accepted);
        assert_eq!(adapter.credit.consumed_objects(), 1);
        assert_eq!(
            adapter.credit.consumed_bytes(),
            u64::try_from(allowed.payload.len()).unwrap()
        );
        assert_eq!(adapter.receive(&allowed, false), Verdict::NoAuthorization);
        assert_eq!(adapter.handled, usize::from(!handler_fails));
        assert!(adapter.scope.retire());
        assert!(!adapter.connection.is_cancelled());
    }
}

#[test]
fn discovery_response_rejects_unauthorized_counts_and_bytes_before_allocation() {
    for (choice, start, limit, bytes, expected) in [
        (
            1,
            false,
            1,
            MAX_DISCOVERY_MESSAGE_BYTES,
            Verdict::NoAuthorization,
        ),
        (
            1,
            true,
            1,
            response_case(1).frame.payload.len() - 1,
            Verdict::Bytes,
        ),
        (2, true, 1, MAX_DISCOVERY_MESSAGE_BYTES, Verdict::Count),
        (7, true, 1, MAX_DISCOVERY_MESSAGE_BYTES, Verdict::Envelope),
    ] {
        let mut adapter = DiscoveryResponse::new(limit, u64::try_from(bytes).unwrap(), false);
        if start {
            assert!(adapter.start());
        }
        let frame = response_case(choice).frame;
        let (actual, allocation) =
            zakura_test::allocations::measure(|| adapter.receive(&frame, false));
        assert_eq!(actual, expected);
        assert_eq!(allocation.requested_bytes, 0);
        assert_eq!(adapter.decodes, 0);
        assert_eq!(adapter.credit.consumed_objects(), 0);
        assert_eq!(adapter.credit.consumed_bytes(), 0);
    }
}
