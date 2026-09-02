//! Property-test pilot for the stream-6 `GetBlocks` message.
//!
//! The candidate contract in this module is deliberately independent of the
//! production codec. It uses wire literals and byte parsing rather than the
//! codec's constants or validation helpers, so agreement is meaningful. The
//! rule ledger makes every rule and every known divergence visible in one local
//! report:
//!
//! `./scripts/test-p2p-message-contracts.sh --message get-blocks`
//!
//! | Field | Contract |
//! | --- | --- |
//! | Message | `BlockSyncMessage::GetBlocks` |
//! | Stream | kind 6, version 2 |
//! | Direction | request |
//! | Discriminator | outer and inner type 2 |
//! | Layout | `[2][start_height: u32 LE][count: u32 LE]` |
//! | Candidate maximum | 9 payload bytes |
//! | Contract source | proposed peer-message regulation design |

use proptest::test_runner::TestCaseResult;
use proptest::{collection::vec, prelude::*};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

use super::super::super::{
    pipe::block_sync_guard, wire::*, BlockSyncPeerSession, BlockSyncWireError, MAX_BS_FRAME_BYTES,
};
use super::support::{run_contract_report, CaseCensus, ContractRule, RuleStatus};
use crate::zakura::{framed_channel, Admit, Frame, ZakuraPeerId, FRAME_HEADER_BYTES};
use zakura_chain::block;

// These literals define the candidate contract. Do not replace them with
// production constants: `production_constants_match_candidate_contract`
// separately detects drift between this oracle and the implementation.
const CONTRACT_DISCRIMINATOR: u8 = 2;
const CONTRACT_WIRE_BYTES: usize = 9;
const CONTRACT_HEIGHT_MAX: u32 = 0x7fff_ffff;
const CONTRACT_COUNT_MAX: u32 = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContractGetBlocks {
    start_height: u32,
    count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContractReject {
    Length(usize),
    Discriminator(u8),
    Height(u32),
    Count(u32),
    RangeEnd { start_height: u32, count: u32 },
}

const CONTRACT_RULES: &[ContractRule] = &[
    ContractRule {
        id: "GB-01",
        requirement: "the payload discriminator is 2",
        status: RuleStatus::Conformant,
        evidence: get_blocks_contract_gb01_discriminator,
    },
    ContractRule {
        id: "GB-02",
        requirement: "the canonical payload is exactly 9 little-endian bytes",
        status: RuleStatus::Conformant,
        evidence: get_blocks_contract_gb02_golden_vectors,
    },
    ContractRule {
        id: "GB-03",
        requirement: "count is in 1..=128",
        status: RuleStatus::Conformant,
        evidence: get_blocks_contract_gb03_exhaustive_count_boundary,
    },
    ContractRule {
        id: "GB-04",
        requirement: "start_height does not exceed the supported height",
        status: RuleStatus::Conformant,
        evidence: get_blocks_contract_gb04_height_boundary,
    },
    ContractRule {
        id: "GB-05",
        requirement: "start_height + count - 1 does not exceed the supported height",
        status: RuleStatus::CandidateContractDivergence {
            current: "the codec validates height and count separately",
            target: "reject a range whose inclusive end exceeds 0x7fffffff",
        },
        evidence: get_blocks_contract_gb05_documents_range_end_divergence,
    },
    ContractRule {
        id: "GB-06",
        requirement: "the decoder consumes the payload exactly, with no trailing bytes",
        status: RuleStatus::Conformant,
        evidence: get_blocks_contract_gb06_rejects_trailing_bytes,
    },
    ContractRule {
        id: "GB-07",
        requirement: "frame flags are zero",
        status: RuleStatus::Conformant,
        evidence: get_blocks_contract_gb07_rejects_nonzero_flags,
    },
    ContractRule {
        id: "GB-08",
        requirement: "the outer frame type agrees with the payload discriminator",
        status: RuleStatus::Conformant,
        evidence: get_blocks_contract_gb08_checks_outer_type,
    },
    ContractRule {
        id: "GB-09",
        requirement: "the declared GetBlocks length is capped before payload allocation",
        status: RuleStatus::CandidateContractDivergence {
            current: "transport admission uses the stream-wide 3 MiB payload cap",
            target: "use the message-specific 9-byte cap before allocation",
        },
        evidence: get_blocks_contract_gb09_documents_transport_cap_divergence,
    },
];

fn contract_decode(bytes: &[u8]) -> Result<ContractGetBlocks, ContractReject> {
    if bytes.len() != CONTRACT_WIRE_BYTES {
        return Err(ContractReject::Length(bytes.len()));
    }

    if bytes[0] != CONTRACT_DISCRIMINATOR {
        return Err(ContractReject::Discriminator(bytes[0]));
    }

    let start_height = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let count = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);

    if start_height > CONTRACT_HEIGHT_MAX {
        return Err(ContractReject::Height(start_height));
    }
    if !(1..=CONTRACT_COUNT_MAX).contains(&count) {
        return Err(ContractReject::Count(count));
    }

    let range_end = u64::from(start_height) + u64::from(count) - 1;
    if range_end > u64::from(CONTRACT_HEIGHT_MAX) {
        return Err(ContractReject::RangeEnd {
            start_height,
            count,
        });
    }

    Ok(ContractGetBlocks {
        start_height,
        count,
    })
}

fn contract_encode(message: ContractGetBlocks) -> [u8; CONTRACT_WIRE_BYTES] {
    let mut bytes = [0; CONTRACT_WIRE_BYTES];
    bytes[0] = CONTRACT_DISCRIMINATOR;
    bytes[1..5].copy_from_slice(&message.start_height.to_le_bytes());
    bytes[5..9].copy_from_slice(&message.count.to_le_bytes());
    bytes
}

fn candidate_payload(discriminator: u8, start_height: u32, count: u32, trailing: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CONTRACT_WIRE_BYTES + trailing.len());
    bytes.push(discriminator);
    bytes.extend_from_slice(&start_height.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(trailing);
    bytes
}

fn production_get_blocks(bytes: &[u8]) -> Option<ContractGetBlocks> {
    match BlockSyncMessage::decode(bytes) {
        Ok(BlockSyncMessage::GetBlocks {
            start_height,
            count,
        }) => Some(ContractGetBlocks {
            start_height: start_height.0,
            count,
        }),
        Ok(_) | Err(_) => None,
    }
}

fn assert_contract_alignment(bytes: &[u8]) -> TestCaseResult {
    let contract = contract_decode(bytes);
    let production = production_get_blocks(bytes);

    match contract {
        Ok(expected) => prop_assert_eq!(production, Some(expected)),
        Err(ContractReject::RangeEnd {
            start_height,
            count,
        }) => {
            prop_assert_eq!(
                production,
                Some(ContractGetBlocks {
                    start_height,
                    count,
                })
            );
        }
        Err(_) => prop_assert_eq!(production, None),
    }

    if let Ok(message @ BlockSyncMessage::GetBlocks { .. }) = BlockSyncMessage::decode(bytes) {
        let reencoded = message.encode().map_err(|error| {
            TestCaseError::fail(format!("a decoded GetBlocks must re-encode: {error}"))
        })?;
        prop_assert_eq!(reencoded, bytes, "accepted encodings are canonical");
    }

    Ok(())
}

fn legal_get_blocks() -> impl Strategy<Value = ContractGetBlocks> {
    prop_oneof![
        4 => Just(1),
        2 => Just(2),
        2 => Just(127),
        4 => Just(CONTRACT_COUNT_MAX),
        8 => 1..=CONTRACT_COUNT_MAX,
    ]
    .prop_flat_map(|count| {
        let max_start = CONTRACT_HEIGHT_MAX - (count - 1);
        prop_oneof![
            4 => Just(0),
            2 => Just(1),
            4 => Just(max_start),
            8 => 0..=max_start,
        ]
        .prop_map(move |start_height| ContractGetBlocks {
            start_height,
            count,
        })
    })
}

fn discriminator_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![8 => Just(CONTRACT_DISCRIMINATOR), 2 => any::<u8>()]
}

fn height_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![
        2 => Just(0),
        2 => Just(CONTRACT_HEIGHT_MAX),
        2 => Just(CONTRACT_HEIGHT_MAX + 1),
        2 => Just(u32::MAX),
        8 => any::<u32>(),
    ]
}

fn count_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![
        3 => Just(0),
        3 => Just(1),
        1 => Just(2),
        1 => Just(127),
        3 => Just(CONTRACT_COUNT_MAX),
        3 => Just(CONTRACT_COUNT_MAX + 1),
        2 => Just(u32::MAX),
        8 => any::<u32>(),
    ]
}

fn trailing_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![8 => Just(Vec::new()), 2 => vec(any::<u8>(), 1..=8)]
}

fn frame_type_strategy() -> impl Strategy<Value = u16> {
    prop_oneof![8 => Just(u16::from(CONTRACT_DISCRIMINATOR)), 2 => any::<u16>()]
}

fn frame_flags_strategy() -> impl Strategy<Value = u16> {
    prop_oneof![8 => Just(0), 2 => any::<u16>()]
}

#[test]
fn get_blocks_contract_report() {
    run_contract_report(
        "GetBlocks",
        "GB",
        CONTRACT_RULES,
        get_blocks_contract_compound_case_matrix,
        4,
    );
}

#[test]
fn production_constants_match_candidate_contract() {
    assert_eq!(MSG_BS_GET_BLOCKS, CONTRACT_DISCRIMINATOR);
    assert_eq!(MAX_BS_BLOCKS_PER_REQUEST, CONTRACT_COUNT_MAX);
    assert_eq!(block::Height::MAX.0, CONTRACT_HEIGHT_MAX);
}

fn get_blocks_contract_gb01_discriminator() -> CaseCensus {
    let canonical = contract_encode(ContractGetBlocks {
        start_height: 0,
        count: 1,
    });

    assert_eq!(
        contract_decode(&canonical),
        Ok(ContractGetBlocks {
            start_height: 0,
            count: 1,
        })
    );
    assert_eq!(
        production_get_blocks(&canonical),
        Some(ContractGetBlocks {
            start_height: 0,
            count: 1,
        })
    );

    for discriminator in u8::MIN..=u8::MAX {
        if discriminator == CONTRACT_DISCRIMINATOR {
            continue;
        }
        let bytes = candidate_payload(discriminator, 0, 1, &[]);
        assert_eq!(
            contract_decode(&bytes),
            Err(ContractReject::Discriminator(discriminator))
        );
        assert_eq!(production_get_blocks(&bytes), None);
    }

    CaseCensus::new(1, usize::from(u8::MAX), 0, 0)
}

fn get_blocks_contract_gb02_golden_vectors() -> CaseCensus {
    let vectors = [
        (
            ContractGetBlocks {
                start_height: 0,
                count: 1,
            },
            [2, 0, 0, 0, 0, 1, 0, 0, 0],
        ),
        (
            ContractGetBlocks {
                start_height: 0x0102_0304,
                count: 128,
            },
            [2, 4, 3, 2, 1, 128, 0, 0, 0],
        ),
        (
            ContractGetBlocks {
                start_height: 0x7fff_ff80,
                count: 128,
            },
            [2, 128, 255, 255, 127, 128, 0, 0, 0],
        ),
        (
            ContractGetBlocks {
                start_height: CONTRACT_HEIGHT_MAX,
                count: 1,
            },
            [2, 255, 255, 255, 127, 1, 0, 0, 0],
        ),
    ];

    for (message, expected) in vectors {
        assert_eq!(contract_encode(message), expected);
        assert_eq!(contract_decode(&expected), Ok(message));
        assert_eq!(production_get_blocks(&expected), Some(message));

        let production = BlockSyncMessage::GetBlocks {
            start_height: block::Height(message.start_height),
            count: message.count,
        };
        assert_eq!(
            production.encode().expect("golden message encodes"),
            expected
        );
    }

    CaseCensus::legal(vectors.len())
}

fn get_blocks_contract_gb03_exhaustive_count_boundary() -> CaseCensus {
    for count in 0..=CONTRACT_COUNT_MAX + 1 {
        let bytes = candidate_payload(CONTRACT_DISCRIMINATOR, 0, count, &[]);
        let accepted = (1..=CONTRACT_COUNT_MAX).contains(&count);

        assert_eq!(contract_decode(&bytes).is_ok(), accepted, "count {count}");
        assert_eq!(
            production_get_blocks(&bytes).is_some(),
            accepted,
            "count {count}"
        );

        let encoded = BlockSyncMessage::GetBlocks {
            start_height: block::Height(0),
            count,
        }
        .encode();
        assert_eq!(encoded.is_ok(), accepted, "encoded count {count}");

        match count {
            0 => assert!(matches!(
                BlockSyncMessage::decode(&bytes),
                Err(BlockSyncWireError::ZeroBlockCount)
            )),
            1..=CONTRACT_COUNT_MAX => {}
            _ => assert!(matches!(
                BlockSyncMessage::decode(&bytes),
                Err(BlockSyncWireError::BlockCountLimit {
                    actual,
                    max: CONTRACT_COUNT_MAX,
                }) if actual == count
            )),
        }
    }

    CaseCensus::new(
        usize::try_from(CONTRACT_COUNT_MAX).expect("count cap fits usize"),
        2,
        0,
        0,
    )
}

fn get_blocks_contract_gb04_height_boundary() -> CaseCensus {
    let maximum = candidate_payload(CONTRACT_DISCRIMINATOR, CONTRACT_HEIGHT_MAX, 1, &[]);
    assert_eq!(
        contract_decode(&maximum),
        Ok(ContractGetBlocks {
            start_height: CONTRACT_HEIGHT_MAX,
            count: 1,
        })
    );
    assert_eq!(
        production_get_blocks(&maximum),
        Some(ContractGetBlocks {
            start_height: CONTRACT_HEIGHT_MAX,
            count: 1,
        })
    );

    for start_height in [CONTRACT_HEIGHT_MAX + 1, u32::MAX] {
        let bytes = candidate_payload(CONTRACT_DISCRIMINATOR, start_height, 1, &[]);
        assert_eq!(
            contract_decode(&bytes),
            Err(ContractReject::Height(start_height))
        );
        assert!(matches!(
            BlockSyncMessage::decode(&bytes),
            Err(BlockSyncWireError::HeightOutOfRange(actual)) if actual == start_height
        ));
    }

    CaseCensus::new(1, 2, 0, 0)
}

fn get_blocks_contract_gb05_documents_range_end_divergence() -> CaseCensus {
    let mut legal = 0usize;
    let mut divergences = 0usize;

    for count in 1..=CONTRACT_COUNT_MAX {
        let last_legal_start = CONTRACT_HEIGHT_MAX - (count - 1);
        let legal_bytes = candidate_payload(CONTRACT_DISCRIMINATOR, last_legal_start, count, &[]);
        let legal_message = ContractGetBlocks {
            start_height: last_legal_start,
            count,
        };
        assert_eq!(contract_decode(&legal_bytes), Ok(legal_message));
        assert_eq!(production_get_blocks(&legal_bytes), Some(legal_message));
        legal = legal.saturating_add(1);

        if last_legal_start == CONTRACT_HEIGHT_MAX {
            continue;
        }

        let first_overflowing_start = last_legal_start + 1;
        let divergence_bytes =
            candidate_payload(CONTRACT_DISCRIMINATOR, first_overflowing_start, count, &[]);
        let current_message = ContractGetBlocks {
            start_height: first_overflowing_start,
            count,
        };
        assert_eq!(
            contract_decode(&divergence_bytes),
            Err(ContractReject::RangeEnd {
                start_height: first_overflowing_start,
                count,
            })
        );
        assert_eq!(
            production_get_blocks(&divergence_bytes),
            Some(current_message)
        );
        assert_eq!(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(first_overflowing_start),
                count,
            }
            .encode()
            .expect("the current encoder validates height and count separately"),
            divergence_bytes
        );
        divergences = divergences.saturating_add(1);
    }

    assert_eq!(divergences, 127);
    CaseCensus::new(legal, 0, 0, divergences)
}

fn get_blocks_contract_gb06_rejects_trailing_bytes() -> CaseCensus {
    let canonical = contract_encode(ContractGetBlocks {
        start_height: 0,
        count: 1,
    });

    for length in 0..CONTRACT_WIRE_BYTES {
        assert_eq!(
            contract_decode(&canonical[..length]),
            Err(ContractReject::Length(length))
        );
        assert_eq!(production_get_blocks(&canonical[..length]), None);
    }

    for trailing_len in 1..=8 {
        let trailing = vec![0xa5; trailing_len];
        let bytes = candidate_payload(CONTRACT_DISCRIMINATOR, 0, 1, &trailing);
        assert_eq!(
            contract_decode(&bytes),
            Err(ContractReject::Length(CONTRACT_WIRE_BYTES + trailing_len))
        );
        assert_eq!(production_get_blocks(&bytes), None);
        assert!(matches!(
            BlockSyncMessage::decode(&bytes),
            Err(BlockSyncWireError::TrailingBytes)
        ));
    }

    CaseCensus::new(0, CONTRACT_WIRE_BYTES + 8, 0, 0)
}

fn get_blocks_contract_gb07_rejects_nonzero_flags() -> CaseCensus {
    let payload = contract_encode(ContractGetBlocks {
        start_height: 0,
        count: 1,
    });

    for flags in 1..=u16::MAX {
        let frame = Frame {
            message_type: u16::from(CONTRACT_DISCRIMINATOR),
            flags,
            payload: payload.to_vec(),
        };
        assert!(matches!(
            BlockSyncMessage::decode_frame(frame),
            Err(BlockSyncWireError::UnsupportedFlags(actual)) if actual == flags
        ));
    }

    CaseCensus::new(0, usize::from(u16::MAX), 0, 0)
}

fn get_blocks_contract_gb08_checks_outer_type() -> CaseCensus {
    let payload = contract_encode(ContractGetBlocks {
        start_height: 0,
        count: 1,
    });

    for outer_type in u16::MIN..=u16::MAX {
        let result = BlockSyncMessage::decode_frame(Frame {
            message_type: outer_type,
            flags: 0,
            payload: payload.to_vec(),
        });

        if outer_type == u16::from(CONTRACT_DISCRIMINATOR) {
            assert!(matches!(result, Ok(BlockSyncMessage::GetBlocks { .. })));
        } else if u8::try_from(outer_type).is_ok() {
            assert!(matches!(
                result,
                Err(BlockSyncWireError::MismatchedFrameMessageType {
                    frame,
                    payload: CONTRACT_DISCRIMINATOR,
                }) if frame == outer_type
            ));
        } else {
            assert!(matches!(
                result,
                Err(BlockSyncWireError::UnknownFrameMessageType(frame)) if frame == outer_type
            ));
        }
    }

    CaseCensus::new(1, usize::from(u16::MAX), 0, 0)
}

fn get_blocks_contract_gb09_documents_transport_cap_divergence() -> CaseCensus {
    let payload = candidate_payload(CONTRACT_DISCRIMINATOR, 0, 1, &[0]);
    let frame = Frame {
        message_type: u16::from(CONTRACT_DISCRIMINATOR),
        flags: 0,
        payload,
    };

    assert_eq!(frame.payload.len(), CONTRACT_WIRE_BYTES + 1);
    assert!(
        MAX_BS_FRAME_BYTES
            > u32::try_from(FRAME_HEADER_BYTES + CONTRACT_WIRE_BYTES)
                .expect("the GetBlocks frame length fits in u32")
    );
    let encoded_frame = frame
        .encode(MAX_BS_FRAME_BYTES)
        .expect("the stream-wide transport cap accepts the 10-byte payload");
    assert_eq!(
        Frame::decode(&encoded_frame, MAX_BS_FRAME_BYTES)
            .expect("the stream-wide transport cap decodes the 10-byte payload"),
        frame
    );
    assert_eq!(block_sync_guard().admit(&frame), Admit::Pass);
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame),
        Err(BlockSyncWireError::TrailingBytes)
    ));

    CaseCensus::divergence(1)
}

fn get_blocks_contract_compound_case_matrix() -> CaseCensus {
    let mut cases = 0usize;

    for discriminator in [1, CONTRACT_DISCRIMINATOR] {
        for start_height in [0, CONTRACT_HEIGHT_MAX, CONTRACT_HEIGHT_MAX + 1] {
            for count in [0, 1, CONTRACT_COUNT_MAX, CONTRACT_COUNT_MAX + 1] {
                for trailing in [false, true] {
                    for outer_type in [1, u16::from(CONTRACT_DISCRIMINATOR), 256] {
                        for flags in [0, 1] {
                            let mut violations = 0usize;
                            violations += usize::from(discriminator != CONTRACT_DISCRIMINATOR);
                            violations += usize::from(start_height > CONTRACT_HEIGHT_MAX);
                            violations += usize::from(!(1..=CONTRACT_COUNT_MAX).contains(&count));
                            if start_height <= CONTRACT_HEIGHT_MAX
                                && (1..=CONTRACT_COUNT_MAX).contains(&count)
                            {
                                let range_end = u64::from(start_height) + u64::from(count) - 1;
                                violations +=
                                    usize::from(range_end > u64::from(CONTRACT_HEIGHT_MAX));
                            }
                            violations += usize::from(trailing);
                            violations += usize::from(flags != 0);
                            violations += usize::from(outer_type != u16::from(discriminator));

                            if violations < 2 {
                                continue;
                            }

                            let trailing_bytes = if trailing { &[0xa5][..] } else { &[] };
                            let payload = candidate_payload(
                                discriminator,
                                start_height,
                                count,
                                trailing_bytes,
                            );
                            let decode_result = BlockSyncMessage::decode_frame(Frame {
                                message_type: outer_type,
                                flags,
                                payload,
                            });
                            assert!(
                                decode_result.is_err(),
                                "every compound-invalid GetBlocks matrix case has at least one \
                                 rule the current decoder rejects"
                            );
                            cases = cases.saturating_add(1);
                        }
                    }
                }
            }
        }
    }

    CaseCensus::compound(cases)
}

#[tokio::test]
async fn get_blocks_contract_production_path_uses_typed_session() {
    let cases = [
        ContractGetBlocks {
            start_height: 1,
            count: 1,
        },
        ContractGetBlocks {
            start_height: 3_000_000,
            count: 16,
        },
        ContractGetBlocks {
            start_height: CONTRACT_HEIGHT_MAX - (CONTRACT_COUNT_MAX - 1),
            count: CONTRACT_COUNT_MAX,
        },
    ];
    let (outbound, mut received) = framed_channel(cases.len());
    let peer = ZakuraPeerId::new(vec![0x42; 32]).expect("test peer ID is within bounds");
    let session = BlockSyncPeerSession::for_test(peer, outbound, CancellationToken::new());

    for expected in cases {
        session
            .try_send_get_blocks(block::Height(expected.start_height), expected.count)
            .expect("candidate-legal request enters the real outbound queue");
        let frame = timeout(Duration::from_secs(1), received.recv())
            .await
            .expect("the bounded in-memory transport responds")
            .expect("the outbound channel stays open");

        assert_eq!(frame.message_type, u16::from(CONTRACT_DISCRIMINATOR));
        assert_eq!(frame.flags, 0);
        assert_eq!(contract_decode(&frame.payload), Ok(expected));
        assert!(matches!(
            BlockSyncMessage::decode_frame(frame),
            Ok(BlockSyncMessage::GetBlocks { .. })
        ));
    }
}

proptest! {
    #[test]
    fn get_blocks_contract_legal_messages_match_golden_encoding(message in legal_get_blocks()) {
        let bytes = contract_encode(message);

        prop_assert_eq!(contract_decode(&bytes), Ok(message));
        prop_assert_eq!(production_get_blocks(&bytes), Some(message));

        let production = BlockSyncMessage::GetBlocks {
            start_height: block::Height(message.start_height),
            count: message.count,
        };
        prop_assert_eq!(
            production.encode().expect("candidate-legal GetBlocks encodes"),
            bytes
        );
    }

    #[test]
    fn get_blocks_contract_structured_inputs_match_or_documented_divergence(
        discriminator in discriminator_strategy(),
        start_height in height_strategy(),
        count in count_strategy(),
        trailing in trailing_strategy(),
    ) {
        let bytes = candidate_payload(discriminator, start_height, count, &trailing);
        assert_contract_alignment(&bytes)?;
    }

    #[test]
    fn get_blocks_contract_arbitrary_shallow_payloads_never_panic(
        payload in vec(any::<u8>(), 0..=64),
    ) {
        assert_contract_alignment(&payload)?;
    }

    #[test]
    fn get_blocks_contract_arbitrary_shallow_frames_never_panic(
        message_type in frame_type_strategy(),
        flags in frame_flags_strategy(),
        payload in vec(any::<u8>(), 0..=64),
    ) {
        let _decode_result = BlockSyncMessage::decode_frame(Frame {
            message_type,
            flags,
            payload,
        });
    }
}
