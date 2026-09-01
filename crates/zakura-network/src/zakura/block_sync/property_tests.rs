use std::fmt::Debug;

use proptest::{prelude::*, test_runner::TestCaseResult};
use zakura_chain::block;

use super::{
    config::MAX_BS_RESPONSE_BYTES, declaration::GET_BLOCKS, service::block_sync_streams, wire::*,
    BlockSyncWireError,
};
use crate::zakura::{message_payload_cap, Frame, FRAME_HEADER_BYTES};

trait MessagePropertySpec {
    type Value: Clone + Debug + Eq;

    fn payload_cap() -> u32;
    fn legal_strategy() -> BoxedStrategy<Self::Value>;
    fn boundary_values() -> Vec<Self::Value>;
    fn encode(value: &Self::Value) -> Result<Vec<u8>, BlockSyncWireError>;
    fn decode(bytes: &[u8]) -> Result<Self::Value, BlockSyncWireError>;
    fn encode_frame(value: &Self::Value) -> Result<Frame, BlockSyncWireError>;
    fn decode_frame(frame: Frame) -> Result<Self::Value, BlockSyncWireError>;
}

fn property_result<T, E: Debug>(result: Result<T, E>) -> Result<T, TestCaseError> {
    result.map_err(|error| TestCaseError::fail(format!("unexpected error: {error:?}")))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GetBlocksValue {
    start_height: block::Height,
    count: u32,
}

struct GetBlocksSpec;

impl MessagePropertySpec for GetBlocksSpec {
    type Value = GetBlocksValue;

    fn payload_cap() -> u32 {
        GET_BLOCKS.payload_cap
    }

    fn legal_strategy() -> BoxedStrategy<Self::Value> {
        let generated = (
            block::Height::MIN.0..=block::Height::MAX.0,
            1..=GET_BLOCKS.max_count,
        )
            .prop_filter_map(
                "the requested range ends at a supported height",
                |(start, count)| {
                    start
                        .checked_add(count - 1)
                        .filter(|last| *last <= block::Height::MAX.0)
                        .map(|_| GetBlocksValue {
                            start_height: block::Height(start),
                            count,
                        })
                },
            );

        prop_oneof![
            3 => proptest::sample::select(Self::boundary_values()),
            7 => generated,
        ]
        .boxed()
    }

    fn boundary_values() -> Vec<Self::Value> {
        vec![
            GetBlocksValue {
                start_height: block::Height::MIN,
                count: 1,
            },
            GetBlocksValue {
                start_height: block::Height::MIN,
                count: GET_BLOCKS.max_count,
            },
            GetBlocksValue {
                start_height: block::Height(block::Height::MAX.0 - (GET_BLOCKS.max_count - 1)),
                count: GET_BLOCKS.max_count,
            },
            GetBlocksValue {
                start_height: block::Height::MAX,
                count: 1,
            },
        ]
    }

    fn encode(value: &Self::Value) -> Result<Vec<u8>, BlockSyncWireError> {
        BlockSyncMessage::GetBlocks {
            start_height: value.start_height,
            count: value.count,
        }
        .encode()
    }

    fn decode(bytes: &[u8]) -> Result<Self::Value, BlockSyncWireError> {
        match BlockSyncMessage::decode(bytes)? {
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => Ok(GetBlocksValue {
                start_height,
                count,
            }),
            message => Err(BlockSyncWireError::UnknownMessageType(
                message.message_type(),
            )),
        }
    }

    fn encode_frame(value: &Self::Value) -> Result<Frame, BlockSyncWireError> {
        BlockSyncMessage::GetBlocks {
            start_height: value.start_height,
            count: value.count,
        }
        .encode_frame()
    }

    fn decode_frame(frame: Frame) -> Result<Self::Value, BlockSyncWireError> {
        match BlockSyncMessage::decode_frame(frame)? {
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => Ok(GetBlocksValue {
                start_height,
                count,
            }),
            message => Err(BlockSyncWireError::UnknownMessageType(
                message.message_type(),
            )),
        }
    }
}

fn check_legal_message<S: MessagePropertySpec>(value: S::Value) -> TestCaseResult {
    let payload = property_result(S::encode(&value))?;
    let payload_cap = property_result(usize::try_from(S::payload_cap()))?;
    prop_assert!(payload.len() <= payload_cap);
    prop_assert_eq!(property_result(S::decode(&payload))?, value.clone());
    let decoded = property_result(S::decode(&payload))?;
    prop_assert_eq!(property_result(S::encode(&decoded))?, payload.clone());

    let frame = property_result(S::encode_frame(&value))?;
    prop_assert_eq!(frame.payload.len(), payload.len());
    prop_assert_eq!(property_result(S::decode_frame(frame))?, value);
    Ok(())
}

macro_rules! get_blocks_violations {
    ($($variant:ident),+ $(,)?) => {
        #[derive(Copy, Clone, Debug, Eq, PartialEq)]
        enum GetBlocksViolation {
            $($variant),+
        }

        impl GetBlocksViolation {
            const ALL: &'static [Self] = &[$(Self::$variant),+];
        }
    };
}

get_blocks_violations! {
    ZeroCount,
    CountAboveCap,
    RangeAboveMaxHeight,
    Truncated,
    TrailingByte,
    UnknownPayloadType,
    MismatchedFrameType,
    ReservedFrameFlags,
}

fn violation_strategy() -> impl Strategy<Value = GetBlocksViolation> {
    proptest::sample::select(GetBlocksViolation::ALL.to_vec())
}

fn legal_payload() -> Vec<u8> {
    GetBlocksSpec::encode(&GetBlocksValue {
        start_height: block::Height::MIN,
        count: 1,
    })
    .expect("the fixed legal GetBlocks value encodes")
}

fn check_violation(violation: GetBlocksViolation) {
    let mut payload = legal_payload();

    match violation {
        GetBlocksViolation::ZeroCount => {
            payload[5..9].copy_from_slice(&0u32.to_le_bytes());
            assert!(matches!(
                BlockSyncMessage::decode(&payload),
                Err(BlockSyncWireError::ZeroBlockCount)
            ));
        }
        GetBlocksViolation::CountAboveCap => {
            payload[5..9].copy_from_slice(&(GET_BLOCKS.max_count + 1).to_le_bytes());
            assert!(matches!(
                BlockSyncMessage::decode(&payload),
                Err(BlockSyncWireError::BlockCountLimit { .. })
            ));
        }
        GetBlocksViolation::RangeAboveMaxHeight => {
            payload[1..5].copy_from_slice(&block::Height::MAX.0.to_le_bytes());
            payload[5..9].copy_from_slice(&2u32.to_le_bytes());
            assert!(matches!(
                BlockSyncMessage::decode(&payload),
                Err(BlockSyncWireError::BlockRangeOverflow { .. })
            ));
        }
        GetBlocksViolation::Truncated => {
            payload.pop();
            assert!(matches!(
                BlockSyncMessage::decode(&payload),
                Err(BlockSyncWireError::Io(_))
            ));
        }
        GetBlocksViolation::TrailingByte => {
            payload.push(0);
            assert!(matches!(
                BlockSyncMessage::decode(&payload),
                Err(BlockSyncWireError::TrailingBytes)
            ));
        }
        GetBlocksViolation::UnknownPayloadType => {
            payload[0] = u8::MAX;
            assert!(matches!(
                BlockSyncMessage::decode(&payload),
                Err(BlockSyncWireError::UnknownMessageType(u8::MAX))
            ));
        }
        GetBlocksViolation::MismatchedFrameType => {
            let frame = Frame {
                message_type: u16::from(MSG_BS_STATUS),
                flags: 0,
                payload,
            };
            assert!(matches!(
                BlockSyncMessage::decode_frame(frame),
                Err(BlockSyncWireError::MismatchedFrameMessageType { .. })
            ));
        }
        GetBlocksViolation::ReservedFrameFlags => {
            let frame = Frame {
                message_type: u16::from(MSG_BS_GET_BLOCKS),
                flags: 1,
                payload,
            };
            assert!(matches!(
                BlockSyncMessage::decode_frame(frame),
                Err(BlockSyncWireError::UnsupportedFlags(1))
            ));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn get_blocks_legal_values_satisfy_baseline_properties(
        value in GetBlocksSpec::legal_strategy(),
    ) {
        check_legal_message::<GetBlocksSpec>(value)?;
    }

    #[test]
    fn get_blocks_bounded_payload_decode_is_total_and_canonical(
        tail in proptest::collection::vec(
            any::<u8>(),
            0..usize::try_from(GET_BLOCKS.payload_cap).expect("u32 payload caps fit usize"),
        ),
    ) {
        let mut payload = Vec::with_capacity(tail.len() + 1);
        payload.push(MSG_BS_GET_BLOCKS);
        payload.extend_from_slice(&tail);

        if let Ok(message) = BlockSyncMessage::decode(&payload) {
            prop_assert!(
                matches!(&message, BlockSyncMessage::GetBlocks { .. }),
                "a payload with the GetBlocks discriminator decoded as another message"
            );
            prop_assert_eq!(property_result(message.encode())?, payload);
        }
    }

    #[test]
    fn get_blocks_generated_single_rule_violations_are_rejected(
        violation in violation_strategy(),
    ) {
        check_violation(violation);
    }

    #[test]
    fn get_blocks_work_charge_matches_the_specification(
        count in 1..=GET_BLOCKS.max_count,
        local_max_blocks in 1..=GET_BLOCKS.max_count,
        local_max_response_bytes in 1..=MAX_BS_RESPONSE_BYTES,
    ) {
        let bounded_count = u64::from(count.min(local_max_blocks));
        let body_bytes = bounded_count
            .saturating_mul(block::MAX_BLOCK_BYTES)
            .min(u64::from(local_max_response_bytes));
        let expected = u64::from(GET_BLOCKS.payload_cap)
            + bounded_count
            + body_bytes
            + GET_BLOCKS.request_overhead;

        prop_assert_eq!(
            GET_BLOCKS.work_charge(count, local_max_blocks, local_max_response_bytes),
            expected,
        );
        prop_assert!(expected >= GET_BLOCKS.request_overhead);
    }
}

#[test]
fn get_blocks_deterministic_coverage_is_closed() {
    for value in GetBlocksSpec::boundary_values() {
        check_legal_message::<GetBlocksSpec>(value)
            .expect("each declared GetBlocks boundary satisfies the baseline properties");
    }
    for violation in GetBlocksViolation::ALL {
        check_violation(*violation);
    }
}

#[test]
fn get_blocks_declaration_drives_the_preallocation_cap() {
    let stream = block_sync_streams()[0];
    let stream_payload_cap = stream
        .frame_cap
        .checked_sub(u32::try_from(FRAME_HEADER_BYTES).expect("the frame header size fits u32"))
        .expect("the block-sync frame cap includes its header");

    assert_eq!(
        message_payload_cap(
            stream_payload_cap,
            u16::from(MSG_BS_GET_BLOCKS),
            stream.message_payload_caps,
        ),
        GET_BLOCKS.payload_cap,
    );
    assert_eq!(GET_BLOCKS.allocation_cap, 0);
}
