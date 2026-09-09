//! Rules for reading a GetBlocks request and limiting the response we may send.

use super::*;
use crate::zakura::regulation::RequestPolicy;

/// Checks request fields and sets response limits. The peer routine checks the
/// session's initial Status before allowing work to start. A peer may request a
/// completed range again because we don't know which blocks it has kept.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksPolicy {
    /// Maximum number of blocks we may send in one response.
    max_count: u32,
    /// Maximum total block bytes we may send in one response.
    /// Message tags and the ending message are counted separately.
    max_response_bytes: u64,
}

impl GetBlocksPolicy {
    /// Allow one response per authenticated peer, including work from old sessions.
    pub(super) const PEER_PRODUCERS: usize = 1;
    /// One message tag, a four-byte start height, and a four-byte block count.
    const REQUEST_PAYLOAD_BYTES: usize = 9;

    /// The transport and decoder use the same GetBlocks payload limit.
    pub(super) const PAYLOAD_LIMITS: &'static [(u16, usize)] = &[
        // Every u8 discriminator fits in the frame header's u16 message type.
        (
            super::super::wire::MSG_BS_GET_BLOCKS as u16,
            Self::REQUEST_PAYLOAD_BYTES,
        ),
    ];

    /// Use the same response limits that we advertise to peers.
    pub(super) fn new(config: &ZakuraBlockSyncConfig) -> Self {
        Self {
            max_count: inbound_get_blocks_count_limit(config),
            max_response_bytes: u64::from(config.advertised_max_response_bytes()),
        }
    }

    /// Maximum response payload bytes, including message tags and the ending message.
    pub(super) fn response_cap_for_count(&self, requested_count: u32) -> Result<u64, &'static str> {
        // A peer may ask for more blocks than we send in one response.
        let count = requested_count.min(self.max_count);
        let block_bytes = u64::from(count)
            .checked_mul(block::MAX_BLOCK_BYTES)
            .ok_or("GetBlocks response-cap multiplication overflowed")?;
        // Cap the block bytes, then allow one tag per block and the ending message.
        let response_cap = GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
            .checked_add(u64::from(count))
            .and_then(|bytes| bytes.checked_add(block_bytes.min(self.max_response_bytes)))
            .ok_or("GetBlocks response-cap addition overflowed")?;
        Ok(response_cap)
    }
}

impl RequestPolicy for GetBlocksPolicy {
    type Request = GetBlocksRequest;
    type Error = BlockSyncWireError;

    /// Read the requested height and count, rejecting invalid fields or extra data.
    fn decode(&self, frame: Frame) -> Result<GetBlocksRequest, BlockSyncWireError> {
        if frame.payload.len() > Self::REQUEST_PAYLOAD_BYTES {
            return Err(BlockSyncWireError::OversizedPayload {
                actual: frame.payload.len(),
                max: Self::REQUEST_PAYLOAD_BYTES,
            });
        }
        // The existing decoder checks the fields, flags, and matching message tags.
        match BlockSyncMessage::decode_frame(frame)? {
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => Ok(GetBlocksRequest {
                start_height,
                count,
            }),
            message => Err(BlockSyncWireError::UnknownMessageType(
                message.message_type(),
            )),
        }
    }

    /// Give shared regulation the size limit it must enforce while we queue frames.
    fn response_cap(&self, request: &GetBlocksRequest) -> u64 {
        self.response_cap_for_count(request.count)
            .expect("validated GetBlocks limits bound response arithmetic")
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn frame(start_height: block::Height, count: u32) -> Frame {
        Frame {
            message_type: u16::from(super::super::super::wire::MSG_BS_GET_BLOCKS),
            flags: 0,
            payload: BlockSyncMessage::GetBlocks {
                start_height,
                count,
            }
            .encode()
            .unwrap(),
        }
    }

    #[test]
    fn declaration_uses_codec_and_advertised_response_limits() {
        let config = ZakuraBlockSyncConfig::default();
        let policy = GetBlocksPolicy::new(&config);
        let request = policy.decode(frame(block::Height(42), 128)).unwrap();
        assert_eq!(request.start_height, block::Height(42));
        assert_eq!(
            request.count, 128,
            "decode preserves the request; serving selects its prefix"
        );
        assert_eq!(policy.response_cap(&request), block::MAX_BLOCK_BYTES + 10);
    }

    #[test]
    fn get_blocks_range_accepts_the_last_supported_height() {
        let policy = GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default());
        for count in 1..=MAX_BS_BLOCKS_PER_REQUEST {
            let start_height = block::Height(block::Height::MAX.0 - (count - 1));
            let request = policy.decode(frame(start_height, count)).unwrap();
            assert_eq!(request.start_height, start_height);
            assert_eq!(request.count, count);
        }
    }

    #[test]
    fn get_blocks_range_rejects_an_end_above_the_supported_height() {
        let policy = GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default());
        for count in 2..=MAX_BS_BLOCKS_PER_REQUEST {
            let valid_start = block::Height(block::Height::MAX.0 - (count - 1));
            let invalid_start = block::Height(valid_start.0 + 1);
            // Shift a legal range forward one height without using the encoder
            // to build the invalid input. The start still fits; its end does not.
            let mut invalid = frame(valid_start, count);
            invalid.payload[1..5].copy_from_slice(&invalid_start.0.to_le_bytes());
            assert!(matches!(
                policy.decode(invalid),
                Err(BlockSyncWireError::HeightOutOfRange(height))
                    if height == block::Height::MAX.0 + 1
            ));
            assert!(matches!(
                BlockSyncMessage::GetBlocks { start_height: invalid_start, count }.encode_frame(),
                Err(BlockSyncWireError::HeightOutOfRange(height))
                    if height == block::Height::MAX.0 + 1
            ));
        }
    }

    #[test]
    fn get_blocks_range_encoder_rejects_arithmetic_overflow() {
        assert!(matches!(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(u32::MAX),
                count: 2
            }
            .encode_frame(),
            Err(BlockSyncWireError::NumericOverflow(_))
        ));
    }

    #[test]
    fn declaration_rejects_noncanonical_or_invalid_requests() {
        let policy = GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default());
        let valid = frame(block::Height(42), 1);
        let mut trailing = valid.clone();
        trailing.payload.push(0);
        assert!(policy.decode(trailing).is_err());
        let mut zero_count = valid.clone();
        zero_count.payload[5..9].fill(0);
        assert!(matches!(
            policy.decode(zero_count),
            Err(BlockSyncWireError::ZeroBlockCount)
        ));
        let mut flags = valid.clone();
        flags.flags = 1;
        assert!(matches!(
            policy.decode(flags),
            Err(BlockSyncWireError::UnsupportedFlags(1))
        ));
        let mut mismatch = valid;
        mismatch.message_type = u16::from(super::super::super::wire::MSG_BS_BLOCKS_DONE);
        assert!(matches!(
            policy.decode(mismatch),
            Err(BlockSyncWireError::MismatchedFrameMessageType { .. })
        ));
    }

    /// Include well-shaped requests so generated cases exercise acceptance too.
    /// Build the bytes directly: the production encoder would reject bad fields.
    fn request_payloads() -> impl Strategy<Value = Vec<u8>> {
        let fields = (
            prop_oneof![3 => Just(2u8), 1 => any::<u8>()],
            prop_oneof![0u32..=block::Height::MAX.0, any::<u32>()],
            prop_oneof![3 => 1u32..=128, 1 => any::<u32>()],
        )
            .prop_map(|(tag, start, count)| {
                let mut payload = vec![tag];
                payload.extend_from_slice(&start.to_le_bytes());
                payload.extend_from_slice(&count.to_le_bytes());
                payload
            });
        prop_oneof![fields, prop::collection::vec(any::<u8>(), 0..=16)]
    }

    proptest! {
        #[test]
        fn get_blocks_decode_accepts_only_exact_valid_requests(
            payload in request_payloads(),
            message_type in prop_oneof![3 => Just(2u16), 1 => any::<u16>()],
            flags in prop_oneof![3 => Just(0u16), 1 => any::<u16>()],
        ) {
            // Independent wire rules: exactly nine bytes, matching GetBlocks
            // tags, no flags, and a nonempty range within supported heights.
            // Wider arithmetic also catches ranges that overflow u32.
            let expected = match payload.as_slice() {
                [2, a, b, c, d, e, f, g, h] if message_type == 2 && flags == 0 => {
                    let start = u32::from_le_bytes([*a, *b, *c, *d]);
                    let count = u32::from_le_bytes([*e, *f, *g, *h]);
                    ((1..=128).contains(&count)
                        && u64::from(start) + u64::from(count)
                            <= u64::from(block::Height::MAX.0) + 1)
                        .then_some((start, count))
                }
                _ => None,
            };
            let policy = GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default());
            // Proptest reports and shrinks any panic from the production decoder.
            let actual = policy.decode(Frame { message_type, flags, payload });
            prop_assert_eq!(
                actual.ok().map(|request| (request.start_height.0, request.count)),
                expected,
            );
        }
    }
}
