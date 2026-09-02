//! Executable candidate contract for the stream-6 `Status` message.
//!
//! | Field | Contract |
//! | --- | --- |
//! | Message | `BlockSyncMessage::Status` |
//! | Stream | kind 6, version 2 |
//! | Direction | announcement |
//! | Discriminator | outer and inner type 1 |
//! | Layout | `[1][low: u32 LE][high: u32 LE][tip_hash: 32][three u32 LE caps]` |
//! | Candidate maximum | 53 payload bytes |
//! | Contract source | proposed peer-message regulation design |

use proptest::test_runner::TestCaseResult;
use proptest::{collection::vec, prelude::*};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

use super::super::super::{
    config::MAX_BS_INFLIGHT_REQUESTS, pipe::block_sync_guard, BlockSyncPeerSession,
    BlockSyncStatus, BlockSyncWireError, MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_FRAME_BYTES,
    MAX_BS_RESPONSE_BYTES, MSG_BS_STATUS,
};
use super::support::{run_contract_report, CaseCensus, ContractRule, RuleStatus};
use crate::zakura::{
    framed_channel, Admit, BlockSyncMessage, Frame, ZakuraBlockSyncConfig, ZakuraPeerId,
    FRAME_HEADER_BYTES,
};
use zakura_chain::block;

// Keep these literals independent of production constants. The dedicated
// constant-alignment test reports drift without weakening the oracle.
const CONTRACT_DISCRIMINATOR: u8 = 1;
const CONTRACT_WIRE_BYTES: usize = 53;
const CONTRACT_HEIGHT_MAX: u32 = 0x7fff_ffff;
const CONTRACT_MAX_BLOCKS: u32 = 128;
const CONTRACT_MAX_INFLIGHT: u32 = 32_768;
const CONTRACT_MAX_RESPONSE_BYTES: u32 = 33_554_432;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContractStatus {
    servable_low: u32,
    servable_high: u32,
    tip_hash: [u8; 32],
    max_blocks_per_response: u32,
    max_inflight_requests: u32,
    max_response_bytes: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContractReject {
    Length(usize),
    Discriminator(u8),
    ServableLow(u32),
    ServableHigh(u32),
    InvertedRange { low: u32, high: u32 },
    MaxBlocks(u32),
    MaxInflight(u32),
    MaxResponseBytes(u32),
}

const CONTRACT_RULES: &[ContractRule] = &[
    ContractRule {
        id: "ST-01",
        requirement: "the payload discriminator is 1",
        status: RuleStatus::Conformant,
        evidence: status_contract_st01_discriminator,
    },
    ContractRule {
        id: "ST-02",
        requirement: "the canonical payload is exactly 53 little-endian bytes",
        status: RuleStatus::Conformant,
        evidence: status_contract_st02_golden_vectors,
    },
    ContractRule {
        id: "ST-03",
        requirement: "servable_low does not exceed the supported height",
        status: RuleStatus::Conformant,
        evidence: status_contract_st03_low_height_boundary,
    },
    ContractRule {
        id: "ST-04",
        requirement: "servable_high does not exceed the supported height",
        status: RuleStatus::Conformant,
        evidence: status_contract_st04_high_height_boundary,
    },
    ContractRule {
        id: "ST-05",
        requirement: "servable_low is less than or equal to servable_high during decode",
        status: RuleStatus::CandidateContractDivergence {
            current: "the codec accepts an inverted range; the peer routine rejects it later",
            target: "reject the inverted range during bounded decode",
        },
        evidence: status_contract_st05_documents_inverted_range_divergence,
    },
    ContractRule {
        id: "ST-06",
        requirement: "max_blocks_per_response is in 1..=128",
        status: RuleStatus::CandidateContractDivergence {
            current: "the encoder and decoder clamp the advertisement",
            target: "reject an out-of-range wire value instead of clamping",
        },
        evidence: status_contract_st06_documents_block_cap_clamping,
    },
    ContractRule {
        id: "ST-07",
        requirement: "max_inflight_requests is in 1..=32768",
        status: RuleStatus::CandidateContractDivergence {
            current: "the decoder clamps, while the typed encoder can emit an invalid value",
            target: "reject an out-of-range wire value and never emit one",
        },
        evidence: status_contract_st07_documents_inflight_cap_clamping,
    },
    ContractRule {
        id: "ST-08",
        requirement: "max_response_bytes is in 1..=33554432",
        status: RuleStatus::CandidateContractDivergence {
            current: "the decoder clamps; the encoder raises zero but does not cap the maximum",
            target: "reject an out-of-range wire value and never emit one",
        },
        evidence: status_contract_st08_documents_response_cap_clamping,
    },
    ContractRule {
        id: "ST-09",
        requirement: "the decoder consumes the payload exactly, with no trailing bytes",
        status: RuleStatus::Conformant,
        evidence: status_contract_st09_rejects_noncanonical_lengths,
    },
    ContractRule {
        id: "ST-10",
        requirement: "frame flags are zero",
        status: RuleStatus::Conformant,
        evidence: status_contract_st10_rejects_nonzero_flags,
    },
    ContractRule {
        id: "ST-11",
        requirement: "the outer frame type agrees with the payload discriminator",
        status: RuleStatus::Conformant,
        evidence: status_contract_st11_checks_outer_type,
    },
    ContractRule {
        id: "ST-12",
        requirement: "the declared Status length is capped before payload allocation",
        status: RuleStatus::CandidateContractDivergence {
            current: "transport admission uses the stream-wide 3 MiB payload cap",
            target: "use the message-specific 53-byte cap before allocation",
        },
        evidence: status_contract_st12_documents_transport_cap_divergence,
    },
];

fn contract_decode(bytes: &[u8]) -> Result<ContractStatus, ContractReject> {
    if bytes.len() != CONTRACT_WIRE_BYTES {
        return Err(ContractReject::Length(bytes.len()));
    }
    if bytes[0] != CONTRACT_DISCRIMINATOR {
        return Err(ContractReject::Discriminator(bytes[0]));
    }

    let servable_low = read_u32(bytes, 1);
    let servable_high = read_u32(bytes, 5);
    let mut tip_hash = [0; 32];
    tip_hash.copy_from_slice(&bytes[9..41]);
    let max_blocks_per_response = read_u32(bytes, 41);
    let max_inflight_requests = read_u32(bytes, 45);
    let max_response_bytes = read_u32(bytes, 49);

    if servable_low > CONTRACT_HEIGHT_MAX {
        return Err(ContractReject::ServableLow(servable_low));
    }
    if servable_high > CONTRACT_HEIGHT_MAX {
        return Err(ContractReject::ServableHigh(servable_high));
    }
    if servable_low > servable_high {
        return Err(ContractReject::InvertedRange {
            low: servable_low,
            high: servable_high,
        });
    }
    if !(1..=CONTRACT_MAX_BLOCKS).contains(&max_blocks_per_response) {
        return Err(ContractReject::MaxBlocks(max_blocks_per_response));
    }
    if !(1..=CONTRACT_MAX_INFLIGHT).contains(&max_inflight_requests) {
        return Err(ContractReject::MaxInflight(max_inflight_requests));
    }
    if !(1..=CONTRACT_MAX_RESPONSE_BYTES).contains(&max_response_bytes) {
        return Err(ContractReject::MaxResponseBytes(max_response_bytes));
    }

    Ok(ContractStatus {
        servable_low,
        servable_high,
        tip_hash,
        max_blocks_per_response,
        max_inflight_requests,
        max_response_bytes,
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn contract_encode(status: ContractStatus) -> [u8; CONTRACT_WIRE_BYTES] {
    let mut bytes = [0; CONTRACT_WIRE_BYTES];
    bytes[0] = CONTRACT_DISCRIMINATOR;
    bytes[1..5].copy_from_slice(&status.servable_low.to_le_bytes());
    bytes[5..9].copy_from_slice(&status.servable_high.to_le_bytes());
    bytes[9..41].copy_from_slice(&status.tip_hash);
    bytes[41..45].copy_from_slice(&status.max_blocks_per_response.to_le_bytes());
    bytes[45..49].copy_from_slice(&status.max_inflight_requests.to_le_bytes());
    bytes[49..53].copy_from_slice(&status.max_response_bytes.to_le_bytes());
    bytes
}

fn candidate_payload(discriminator: u8, status: ContractStatus, trailing: &[u8]) -> Vec<u8> {
    let mut bytes = contract_encode(status).to_vec();
    bytes[0] = discriminator;
    bytes.extend_from_slice(trailing);
    bytes
}

fn production_status(bytes: &[u8]) -> Option<ContractStatus> {
    match BlockSyncMessage::decode(bytes) {
        Ok(BlockSyncMessage::Status(status)) => Some(contract_status(status)),
        Ok(_) | Err(_) => None,
    }
}

fn contract_status(status: BlockSyncStatus) -> ContractStatus {
    ContractStatus {
        servable_low: status.servable_low.0,
        servable_high: status.servable_high.0,
        tip_hash: status.tip_hash.0,
        max_blocks_per_response: status.max_blocks_per_response,
        max_inflight_requests: status.max_inflight_requests,
        max_response_bytes: status.max_response_bytes,
    }
}

fn production_status_value(status: ContractStatus) -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height(status.servable_low),
        servable_high: block::Height(status.servable_high),
        tip_hash: block::Hash(status.tip_hash),
        max_blocks_per_response: status.max_blocks_per_response,
        max_inflight_requests: status.max_inflight_requests,
        max_response_bytes: status.max_response_bytes,
    }
}

fn current_normalized_status(status: ContractStatus) -> ContractStatus {
    ContractStatus {
        max_blocks_per_response: status.max_blocks_per_response.clamp(1, CONTRACT_MAX_BLOCKS),
        max_inflight_requests: status.max_inflight_requests.clamp(1, CONTRACT_MAX_INFLIGHT),
        max_response_bytes: status
            .max_response_bytes
            .clamp(1, CONTRACT_MAX_RESPONSE_BYTES),
        ..status
    }
}

fn ordinary_status() -> ContractStatus {
    ContractStatus {
        servable_low: 1,
        servable_high: 42,
        tip_hash: [7; 32],
        max_blocks_per_response: 16,
        max_inflight_requests: 4,
        max_response_bytes: CONTRACT_MAX_RESPONSE_BYTES,
    }
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    let compact: String = hex.split_whitespace().collect();
    hex::decode(compact).expect("the hand-written golden vector is valid hex")
}

fn legal_height_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![
        3 => Just(0),
        2 => Just(1),
        3 => Just(CONTRACT_HEIGHT_MAX),
        8 => 0..=CONTRACT_HEIGHT_MAX,
    ]
}

fn legal_block_cap_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![
        3 => Just(1),
        2 => Just(2),
        3 => Just(CONTRACT_MAX_BLOCKS),
        8 => 1..=CONTRACT_MAX_BLOCKS,
    ]
}

fn legal_inflight_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![
        3 => Just(1),
        2 => Just(2),
        3 => Just(CONTRACT_MAX_INFLIGHT),
        8 => 1..=CONTRACT_MAX_INFLIGHT,
    ]
}

fn legal_response_bytes_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![
        3 => Just(1),
        2 => Just(2),
        3 => Just(CONTRACT_MAX_RESPONSE_BYTES),
        8 => 1..=CONTRACT_MAX_RESPONSE_BYTES,
    ]
}

fn legal_status_strategy() -> impl Strategy<Value = ContractStatus> {
    legal_height_strategy().prop_flat_map(|servable_low| {
        (
            servable_low..=CONTRACT_HEIGHT_MAX,
            any::<[u8; 32]>(),
            legal_block_cap_strategy(),
            legal_inflight_strategy(),
            legal_response_bytes_strategy(),
        )
            .prop_map(
                move |(
                    servable_high,
                    tip_hash,
                    max_blocks_per_response,
                    max_inflight_requests,
                    max_response_bytes,
                )| ContractStatus {
                    servable_low,
                    servable_high,
                    tip_hash,
                    max_blocks_per_response,
                    max_inflight_requests,
                    max_response_bytes,
                },
            )
    })
}

fn single_invalid_status_strategy() -> impl Strategy<Value = (ContractStatus, ContractReject)> {
    legal_status_strategy().prop_flat_map(|legal| {
        prop_oneof![
            Just((
                ContractStatus {
                    servable_low: 1,
                    servable_high: 0,
                    ..legal
                },
                ContractReject::InvertedRange { low: 1, high: 0 },
            )),
            Just((
                ContractStatus {
                    max_blocks_per_response: 0,
                    ..legal
                },
                ContractReject::MaxBlocks(0),
            )),
            Just((
                ContractStatus {
                    max_blocks_per_response: CONTRACT_MAX_BLOCKS + 1,
                    ..legal
                },
                ContractReject::MaxBlocks(CONTRACT_MAX_BLOCKS + 1),
            )),
            Just((
                ContractStatus {
                    max_inflight_requests: 0,
                    ..legal
                },
                ContractReject::MaxInflight(0),
            )),
            Just((
                ContractStatus {
                    max_inflight_requests: CONTRACT_MAX_INFLIGHT + 1,
                    ..legal
                },
                ContractReject::MaxInflight(CONTRACT_MAX_INFLIGHT + 1),
            )),
            Just((
                ContractStatus {
                    max_response_bytes: 0,
                    ..legal
                },
                ContractReject::MaxResponseBytes(0),
            )),
            Just((
                ContractStatus {
                    max_response_bytes: CONTRACT_MAX_RESPONSE_BYTES + 1,
                    ..legal
                },
                ContractReject::MaxResponseBytes(CONTRACT_MAX_RESPONSE_BYTES + 1),
            )),
        ]
    })
}

fn raw_height_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![
        2 => Just(0),
        2 => Just(CONTRACT_HEIGHT_MAX),
        2 => Just(CONTRACT_HEIGHT_MAX + 1),
        2 => Just(u32::MAX),
        8 => any::<u32>(),
    ]
}

fn raw_bounded_strategy(maximum: u32) -> impl Strategy<Value = u32> {
    prop_oneof![
        2 => Just(0),
        2 => Just(1),
        2 => Just(maximum),
        2 => Just(maximum + 1),
        8 => any::<u32>(),
    ]
}

fn raw_status_strategy() -> impl Strategy<Value = ContractStatus> {
    (
        raw_height_strategy(),
        raw_height_strategy(),
        any::<[u8; 32]>(),
        raw_bounded_strategy(CONTRACT_MAX_BLOCKS),
        raw_bounded_strategy(CONTRACT_MAX_INFLIGHT),
        raw_bounded_strategy(CONTRACT_MAX_RESPONSE_BYTES),
    )
        .prop_map(
            |(
                servable_low,
                servable_high,
                tip_hash,
                max_blocks_per_response,
                max_inflight_requests,
                max_response_bytes,
            )| ContractStatus {
                servable_low,
                servable_high,
                tip_hash,
                max_blocks_per_response,
                max_inflight_requests,
                max_response_bytes,
            },
        )
}

fn discriminator_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![8 => Just(CONTRACT_DISCRIMINATOR), 2 => any::<u8>()]
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

fn assert_structured_alignment(
    discriminator: u8,
    status: ContractStatus,
    trailing: &[u8],
) -> TestCaseResult {
    let bytes = candidate_payload(discriminator, status, trailing);
    let contract = contract_decode(&bytes);
    let production = production_status(&bytes);

    match contract {
        Ok(expected) => prop_assert_eq!(production, Some(expected)),
        Err(
            ContractReject::Length(_)
            | ContractReject::Discriminator(_)
            | ContractReject::ServableLow(_)
            | ContractReject::ServableHigh(_),
        ) => prop_assert_eq!(production, None),
        Err(
            ContractReject::InvertedRange { .. }
            | ContractReject::MaxBlocks(_)
            | ContractReject::MaxInflight(_)
            | ContractReject::MaxResponseBytes(_),
        ) => prop_assert_eq!(production, Some(current_normalized_status(status))),
    }

    Ok(())
}

#[test]
fn status_contract_report() {
    run_contract_report(
        "Status",
        "ST",
        CONTRACT_RULES,
        status_contract_compound_case_matrix,
        4,
    );
}

#[test]
fn status_production_constants_match_candidate_contract() {
    assert_eq!(MSG_BS_STATUS, CONTRACT_DISCRIMINATOR);
    assert_eq!(MAX_BS_BLOCKS_PER_REQUEST, CONTRACT_MAX_BLOCKS);
    assert_eq!(MAX_BS_INFLIGHT_REQUESTS, CONTRACT_MAX_INFLIGHT);
    assert_eq!(MAX_BS_RESPONSE_BYTES, CONTRACT_MAX_RESPONSE_BYTES);
    assert_eq!(block::Height::MAX.0, CONTRACT_HEIGHT_MAX);
}

fn status_contract_st01_discriminator() -> CaseCensus {
    let status = ordinary_status();
    let canonical = contract_encode(status);
    assert_eq!(contract_decode(&canonical), Ok(status));
    assert_eq!(production_status(&canonical), Some(status));

    for discriminator in u8::MIN..=u8::MAX {
        if discriminator == CONTRACT_DISCRIMINATOR {
            continue;
        }
        let bytes = candidate_payload(discriminator, status, &[]);
        assert_eq!(
            contract_decode(&bytes),
            Err(ContractReject::Discriminator(discriminator))
        );
        assert_eq!(production_status(&bytes), None);
    }

    CaseCensus::new(1, usize::from(u8::MAX), 0, 0)
}

fn status_contract_st02_golden_vectors() -> CaseCensus {
    let minimum = ContractStatus {
        servable_low: 0,
        servable_high: 0,
        tip_hash: [0; 32],
        max_blocks_per_response: 1,
        max_inflight_requests: 1,
        max_response_bytes: 1,
    };
    let ordinary_maximum = ContractStatus {
        servable_low: 0x0102_0304,
        servable_high: 0x0506_0708,
        tip_hash: [0xab; 32],
        max_blocks_per_response: CONTRACT_MAX_BLOCKS,
        max_inflight_requests: CONTRACT_MAX_INFLIGHT,
        max_response_bytes: CONTRACT_MAX_RESPONSE_BYTES,
    };
    let ordered_bytes = ContractStatus {
        servable_low: 0x0a0b_0c0d,
        servable_high: 0x1a1b_1c1d,
        tip_hash: [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ],
        max_blocks_per_response: 2,
        max_inflight_requests: 0x0102,
        max_response_bytes: 0x0102_0304,
    };
    let vectors = [
        (
            minimum,
            hex_bytes(
                "01 00000000 00000000
                 0000000000000000000000000000000000000000000000000000000000000000
                 01000000 01000000 01000000",
            ),
        ),
        (
            ordinary_maximum,
            hex_bytes(
                "01 04030201 08070605
                 abababababababababababababababababababababababababababababababab
                 80000000 00800000 00000002",
            ),
        ),
        (
            ordered_bytes,
            hex_bytes(
                "01 0d0c0b0a 1d1c1b1a
                 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
                 02000000 02010000 04030201",
            ),
        ),
    ];

    for (status, expected) in &vectors {
        assert_eq!(contract_encode(*status).as_slice(), expected.as_slice());
        assert_eq!(contract_decode(expected), Ok(*status));
        assert_eq!(production_status(expected), Some(*status));
        let production = BlockSyncMessage::Status(production_status_value(*status))
            .encode()
            .expect("candidate-legal golden Status encodes");
        assert_eq!(production.as_slice(), expected.as_slice());
    }

    CaseCensus::legal(vectors.len())
}

fn status_contract_st03_low_height_boundary() -> CaseCensus {
    let legal = ContractStatus {
        servable_low: CONTRACT_HEIGHT_MAX,
        servable_high: CONTRACT_HEIGHT_MAX,
        ..ordinary_status()
    };
    let legal_bytes = contract_encode(legal);
    assert_eq!(contract_decode(&legal_bytes), Ok(legal));
    assert_eq!(production_status(&legal_bytes), Some(legal));

    for servable_low in [CONTRACT_HEIGHT_MAX + 1, u32::MAX] {
        let invalid = ContractStatus {
            servable_low,
            servable_high: servable_low,
            ..ordinary_status()
        };
        let bytes = contract_encode(invalid);
        assert_eq!(
            contract_decode(&bytes),
            Err(ContractReject::ServableLow(servable_low))
        );
        assert_eq!(production_status(&bytes), None);
    }

    CaseCensus::new(1, 2, 0, 0)
}

fn status_contract_st04_high_height_boundary() -> CaseCensus {
    let legal = ContractStatus {
        servable_low: 0,
        servable_high: CONTRACT_HEIGHT_MAX,
        ..ordinary_status()
    };
    let legal_bytes = contract_encode(legal);
    assert_eq!(contract_decode(&legal_bytes), Ok(legal));
    assert_eq!(production_status(&legal_bytes), Some(legal));

    for servable_high in [CONTRACT_HEIGHT_MAX + 1, u32::MAX] {
        let invalid = ContractStatus {
            servable_low: 0,
            servable_high,
            ..ordinary_status()
        };
        let bytes = contract_encode(invalid);
        assert_eq!(
            contract_decode(&bytes),
            Err(ContractReject::ServableHigh(servable_high))
        );
        assert_eq!(production_status(&bytes), None);
    }

    CaseCensus::new(1, 2, 0, 0)
}

fn status_contract_st05_documents_inverted_range_divergence() -> CaseCensus {
    let heights = [0, 1, 2, CONTRACT_HEIGHT_MAX - 1, CONTRACT_HEIGHT_MAX];
    let mut legal = 0usize;
    let mut divergences = 0usize;

    for low in heights {
        for high in heights {
            let status = ContractStatus {
                servable_low: low,
                servable_high: high,
                ..ordinary_status()
            };
            let bytes = contract_encode(status);
            if low <= high {
                assert_eq!(contract_decode(&bytes), Ok(status));
                assert_eq!(production_status(&bytes), Some(status));
                legal = legal.saturating_add(1);
            } else {
                assert_eq!(
                    contract_decode(&bytes),
                    Err(ContractReject::InvertedRange { low, high })
                );
                assert_eq!(production_status(&bytes), Some(status));
                assert_eq!(
                    BlockSyncMessage::Status(production_status_value(status))
                        .encode()
                        .expect("the current encoder permits an inverted range"),
                    bytes
                );
                divergences = divergences.saturating_add(1);
            }
        }
    }

    CaseCensus::new(legal, 0, 0, divergences)
}

fn status_contract_st06_documents_block_cap_clamping() -> CaseCensus {
    let mut legal = 0usize;
    let mut divergences = 0usize;

    for max_blocks_per_response in (0..=CONTRACT_MAX_BLOCKS + 1).chain([u32::MAX]) {
        let status = ContractStatus {
            max_blocks_per_response,
            ..ordinary_status()
        };
        let bytes = contract_encode(status);
        if (1..=CONTRACT_MAX_BLOCKS).contains(&max_blocks_per_response) {
            assert_eq!(contract_decode(&bytes), Ok(status));
            assert_eq!(production_status(&bytes), Some(status));
            legal = legal.saturating_add(1);
        } else {
            assert_eq!(
                contract_decode(&bytes),
                Err(ContractReject::MaxBlocks(max_blocks_per_response))
            );
            let normalized = current_normalized_status(status);
            assert_eq!(production_status(&bytes), Some(normalized));
            assert_eq!(
                BlockSyncMessage::Status(production_status_value(status))
                    .encode()
                    .expect("the current encoder clamps the block count"),
                contract_encode(normalized)
            );
            divergences = divergences.saturating_add(1);
        }
    }

    CaseCensus::new(legal, 0, 0, divergences)
}

fn status_contract_st07_documents_inflight_cap_clamping() -> CaseCensus {
    let mut legal = 0usize;
    let mut divergences = 0usize;

    for max_inflight_requests in (0..=CONTRACT_MAX_INFLIGHT + 1).chain([u32::MAX]) {
        let status = ContractStatus {
            max_inflight_requests,
            ..ordinary_status()
        };
        let bytes = contract_encode(status);
        let encoded = BlockSyncMessage::Status(production_status_value(status))
            .encode()
            .expect("the current typed Status encoder accepts the field");
        assert_eq!(encoded, bytes);

        if (1..=CONTRACT_MAX_INFLIGHT).contains(&max_inflight_requests) {
            assert_eq!(contract_decode(&bytes), Ok(status));
            assert_eq!(production_status(&bytes), Some(status));
            legal = legal.saturating_add(1);
        } else {
            assert_eq!(
                contract_decode(&bytes),
                Err(ContractReject::MaxInflight(max_inflight_requests))
            );
            assert_eq!(
                production_status(&bytes),
                Some(current_normalized_status(status))
            );
            divergences = divergences.saturating_add(1);
        }
    }

    CaseCensus::new(legal, 0, 0, divergences)
}

fn status_contract_st08_documents_response_cap_clamping() -> CaseCensus {
    let values = [
        0,
        1,
        2,
        CONTRACT_MAX_RESPONSE_BYTES - 1,
        CONTRACT_MAX_RESPONSE_BYTES,
        CONTRACT_MAX_RESPONSE_BYTES + 1,
        u32::MAX,
    ];
    let mut legal = 0usize;
    let mut divergences = 0usize;

    for max_response_bytes in values {
        let status = ContractStatus {
            max_response_bytes,
            ..ordinary_status()
        };
        let bytes = contract_encode(status);
        if (1..=CONTRACT_MAX_RESPONSE_BYTES).contains(&max_response_bytes) {
            assert_eq!(contract_decode(&bytes), Ok(status));
            assert_eq!(production_status(&bytes), Some(status));
            assert_eq!(
                BlockSyncMessage::Status(production_status_value(status))
                    .encode()
                    .expect("candidate-legal Status encodes"),
                bytes
            );
            legal = legal.saturating_add(1);
        } else {
            assert_eq!(
                contract_decode(&bytes),
                Err(ContractReject::MaxResponseBytes(max_response_bytes))
            );
            assert_eq!(
                production_status(&bytes),
                Some(current_normalized_status(status))
            );
            let encoded = BlockSyncMessage::Status(production_status_value(status))
                .encode()
                .expect("the current typed Status encoder accepts the field");
            if max_response_bytes == 0 {
                assert_eq!(encoded, contract_encode(current_normalized_status(status)));
            } else {
                assert_eq!(encoded, bytes);
            }
            divergences = divergences.saturating_add(1);
        }
    }

    CaseCensus::new(legal, 0, 0, divergences)
}

fn status_contract_st09_rejects_noncanonical_lengths() -> CaseCensus {
    let canonical = contract_encode(ordinary_status());

    for length in 0..CONTRACT_WIRE_BYTES {
        assert_eq!(
            contract_decode(&canonical[..length]),
            Err(ContractReject::Length(length))
        );
        assert_eq!(production_status(&canonical[..length]), None);
    }
    for trailing_len in 1..=8 {
        let trailing = vec![0xa5; trailing_len];
        let bytes = candidate_payload(CONTRACT_DISCRIMINATOR, ordinary_status(), &trailing);
        assert_eq!(
            contract_decode(&bytes),
            Err(ContractReject::Length(CONTRACT_WIRE_BYTES + trailing_len))
        );
        assert!(matches!(
            BlockSyncMessage::decode(&bytes),
            Err(BlockSyncWireError::TrailingBytes)
        ));
    }

    CaseCensus::new(0, CONTRACT_WIRE_BYTES + 8, 0, 0)
}

fn status_contract_st10_rejects_nonzero_flags() -> CaseCensus {
    let payload = contract_encode(ordinary_status());

    for flags in 1..=u16::MAX {
        assert!(matches!(
            BlockSyncMessage::decode_frame(Frame {
                message_type: u16::from(CONTRACT_DISCRIMINATOR),
                flags,
                payload: payload.to_vec(),
            }),
            Err(BlockSyncWireError::UnsupportedFlags(actual)) if actual == flags
        ));
    }

    CaseCensus::new(0, usize::from(u16::MAX), 0, 0)
}

fn status_contract_st11_checks_outer_type() -> CaseCensus {
    let payload = contract_encode(ordinary_status());

    for outer_type in u16::MIN..=u16::MAX {
        let result = BlockSyncMessage::decode_frame(Frame {
            message_type: outer_type,
            flags: 0,
            payload: payload.to_vec(),
        });
        if outer_type == u16::from(CONTRACT_DISCRIMINATOR) {
            assert!(matches!(result, Ok(BlockSyncMessage::Status(_))));
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

fn status_contract_st12_documents_transport_cap_divergence() -> CaseCensus {
    let payload = candidate_payload(CONTRACT_DISCRIMINATOR, ordinary_status(), &[0]);
    let frame = Frame {
        message_type: u16::from(CONTRACT_DISCRIMINATOR),
        flags: 0,
        payload,
    };

    assert_eq!(frame.payload.len(), CONTRACT_WIRE_BYTES + 1);
    assert!(
        MAX_BS_FRAME_BYTES
            > u32::try_from(FRAME_HEADER_BYTES + CONTRACT_WIRE_BYTES)
                .expect("the Status frame length fits in u32")
    );
    let encoded = frame
        .encode(MAX_BS_FRAME_BYTES)
        .expect("the stream-wide transport cap accepts the 54-byte payload");
    assert_eq!(
        Frame::decode(&encoded, MAX_BS_FRAME_BYTES)
            .expect("the stream-wide transport cap decodes the 54-byte payload"),
        frame
    );
    assert_eq!(block_sync_guard().admit(&frame), Admit::Pass);
    assert!(matches!(
        BlockSyncMessage::decode_frame(frame),
        Err(BlockSyncWireError::TrailingBytes)
    ));

    CaseCensus::divergence(1)
}

fn status_contract_compound_case_matrix() -> CaseCensus {
    let mut cases = 0usize;

    for discriminator in [CONTRACT_DISCRIMINATOR, 2] {
        for servable_low in [0, CONTRACT_HEIGHT_MAX, CONTRACT_HEIGHT_MAX + 1] {
            for servable_high in [0, CONTRACT_HEIGHT_MAX, CONTRACT_HEIGHT_MAX + 1] {
                for max_blocks_per_response in [0, 1, CONTRACT_MAX_BLOCKS, CONTRACT_MAX_BLOCKS + 1]
                {
                    for max_inflight_requests in
                        [0, 1, CONTRACT_MAX_INFLIGHT, CONTRACT_MAX_INFLIGHT + 1]
                    {
                        for max_response_bytes in [
                            0,
                            1,
                            CONTRACT_MAX_RESPONSE_BYTES,
                            CONTRACT_MAX_RESPONSE_BYTES + 1,
                        ] {
                            for trailing in [false, true] {
                                for outer_type in [u16::from(CONTRACT_DISCRIMINATOR), 2, 256] {
                                    for flags in [0, 1] {
                                        let mut violations = 0usize;
                                        violations +=
                                            usize::from(discriminator != CONTRACT_DISCRIMINATOR);
                                        violations +=
                                            usize::from(servable_low > CONTRACT_HEIGHT_MAX);
                                        violations +=
                                            usize::from(servable_high > CONTRACT_HEIGHT_MAX);
                                        if servable_low <= CONTRACT_HEIGHT_MAX
                                            && servable_high <= CONTRACT_HEIGHT_MAX
                                        {
                                            violations += usize::from(servable_low > servable_high);
                                        }
                                        violations += usize::from(
                                            !(1..=CONTRACT_MAX_BLOCKS)
                                                .contains(&max_blocks_per_response),
                                        );
                                        violations += usize::from(
                                            !(1..=CONTRACT_MAX_INFLIGHT)
                                                .contains(&max_inflight_requests),
                                        );
                                        violations += usize::from(
                                            !(1..=CONTRACT_MAX_RESPONSE_BYTES)
                                                .contains(&max_response_bytes),
                                        );
                                        violations += usize::from(trailing);
                                        violations += usize::from(flags != 0);
                                        violations +=
                                            usize::from(outer_type != u16::from(discriminator));

                                        if violations < 2 {
                                            continue;
                                        }

                                        let status = ContractStatus {
                                            servable_low,
                                            servable_high,
                                            tip_hash: [0x5a; 32],
                                            max_blocks_per_response,
                                            max_inflight_requests,
                                            max_response_bytes,
                                        };
                                        let trailing_bytes =
                                            if trailing { &[0xa5][..] } else { &[] };
                                        let payload = candidate_payload(
                                            discriminator,
                                            status,
                                            trailing_bytes,
                                        );
                                        let decode_result = BlockSyncMessage::decode_frame(Frame {
                                            message_type: outer_type,
                                            flags,
                                            payload,
                                        });
                                        let current_accepts = discriminator
                                            == CONTRACT_DISCRIMINATOR
                                            && servable_low <= CONTRACT_HEIGHT_MAX
                                            && servable_high <= CONTRACT_HEIGHT_MAX
                                            && !trailing
                                            && outer_type == u16::from(CONTRACT_DISCRIMINATOR)
                                            && flags == 0;
                                        if current_accepts {
                                            let BlockSyncMessage::Status(decoded) = decode_result
                                                .expect("only ledgered Status divergences remain")
                                            else {
                                                panic!("the accepted frame must decode as Status");
                                            };
                                            assert_eq!(
                                                contract_status(decoded),
                                                current_normalized_status(status)
                                            );
                                        } else {
                                            assert!(
                                                decode_result.is_err(),
                                                "a current conformant rule rejects this compound case"
                                            );
                                        }
                                        cases = cases.saturating_add(1);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    CaseCensus::compound(cases)
}

#[tokio::test]
async fn status_contract_production_path_uses_real_profiles_and_typed_session() {
    let default_profile = ZakuraBlockSyncConfig::default().initial_status();
    let configured_profile = ZakuraBlockSyncConfig {
        max_blocks_per_response: 73,
        max_inflight_requests: 12_345,
        max_response_bytes: 1_234_567,
        ..ZakuraBlockSyncConfig::default()
    }
    .initial_status();
    let maximum_profile = ZakuraBlockSyncConfig {
        max_blocks_per_response: CONTRACT_MAX_BLOCKS,
        max_inflight_requests: CONTRACT_MAX_INFLIGHT,
        max_response_bytes: CONTRACT_MAX_RESPONSE_BYTES,
        ..ZakuraBlockSyncConfig::default()
    }
    .initial_status();
    let frontier_profile = BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(3_000_000),
        tip_hash: block::Hash([0x6c; 32]),
        max_blocks_per_response: 16,
        max_inflight_requests: 64,
        max_response_bytes: CONTRACT_MAX_RESPONSE_BYTES,
    };
    assert_eq!(
        contract_status(configured_profile),
        ContractStatus {
            servable_low: 0,
            servable_high: 0,
            tip_hash: [0; 32],
            max_blocks_per_response: 73,
            max_inflight_requests: 12_345,
            max_response_bytes: 1_234_567,
        },
        "the real config-to-Status path preserves each independently chosen capacity"
    );

    let profiles = [
        default_profile,
        configured_profile,
        maximum_profile,
        frontier_profile,
    ];
    let (outbound, mut received) = framed_channel(profiles.len());
    let peer = ZakuraPeerId::new(vec![0x24; 32]).expect("test peer ID is within bounds");
    let session = BlockSyncPeerSession::for_test(peer, outbound, CancellationToken::new());

    for profile in profiles {
        session
            .try_send_status(profile)
            .expect("a real local Status profile enters the outbound queue");
        let frame = timeout(Duration::from_secs(1), received.recv())
            .await
            .expect("the bounded in-memory transport responds")
            .expect("the outbound channel stays open");
        let expected = contract_status(profile);

        assert_eq!(frame.message_type, u16::from(CONTRACT_DISCRIMINATOR));
        assert_eq!(frame.flags, 0);
        assert_eq!(contract_decode(&frame.payload), Ok(expected));
        assert!(matches!(
            BlockSyncMessage::decode_frame(frame),
            Ok(BlockSyncMessage::Status(_))
        ));
    }
}

proptest! {
    #[test]
    fn status_contract_legal_messages_match_golden_encoding(status in legal_status_strategy()) {
        let bytes = contract_encode(status);

        prop_assert_eq!(contract_decode(&bytes), Ok(status));
        prop_assert_eq!(production_status(&bytes), Some(status));
        prop_assert_eq!(
            BlockSyncMessage::Status(production_status_value(status))
                .encode()
                .expect("candidate-legal Status encodes"),
            bytes
        );
    }

    #[test]
    fn status_contract_single_rule_invalid_cases_show_current_behavior(
        (status, expected_reject) in single_invalid_status_strategy(),
    ) {
        let bytes = contract_encode(status);

        prop_assert_eq!(contract_decode(&bytes), Err(expected_reject));
        prop_assert_eq!(
            production_status(&bytes),
            Some(current_normalized_status(status))
        );
    }

    #[test]
    fn status_contract_structured_inputs_match_or_documented_divergence(
        discriminator in discriminator_strategy(),
        status in raw_status_strategy(),
        trailing in trailing_strategy(),
    ) {
        assert_structured_alignment(discriminator, status, &trailing)?;
    }

    #[test]
    fn status_contract_arbitrary_shallow_frames_never_panic(
        message_type in frame_type_strategy(),
        flags in frame_flags_strategy(),
        payload in vec(any::<u8>(), 0..=96),
    ) {
        let _decode_result = BlockSyncMessage::decode_frame(Frame {
            message_type,
            flags,
            payload,
        });
    }
}
