//! GetBlocks' finite-request declaration: its codec, response bound, and work scope.

use super::*;
use crate::zakura::regulation::RequestPolicy;

/// A request accepted by the existing GetBlocks codec.
#[derive(Debug)]
pub(super) struct GetBlocksRequest {
    pub(super) start_height: block::Height,
    pub(super) count: u32,
}

/// GetBlocks uses bounded decoding and one finite response producer per session.
/// Session authorization remains at the peer boundary before work admission.
/// Completed ranges may be retried; this policy does not infer peer chain state.
#[derive(Clone, Debug)]
pub(super) struct GetBlocksPolicy {
    max_count: u32,
    max_response_bytes: u64,
}

impl GetBlocksPolicy {
    pub(super) const SESSION_PRODUCERS: usize = 1;
    const REQUEST_PAYLOAD_BYTES: usize = 9;

    pub(super) fn new(config: &ZakuraBlockSyncConfig) -> Self {
        Self {
            max_count: inbound_get_blocks_count_limit(config),
            max_response_bytes: u64::from(config.advertised_max_response_bytes()),
        }
    }

    pub(super) fn response_cap_for_count(&self, requested_count: u32) -> Result<u64, &'static str> {
        let count = requested_count.min(self.max_count);
        let block_bytes = u64::from(count)
            .checked_mul(block::MAX_BLOCK_BYTES)
            .ok_or("GetBlocks response-cap multiplication overflowed")?;
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

    fn decode(&self, frame: Frame) -> Result<GetBlocksRequest, BlockSyncWireError> {
        if frame.payload.len() > Self::REQUEST_PAYLOAD_BYTES {
            return Err(BlockSyncWireError::OversizedPayload {
                actual: frame.payload.len(),
                max: Self::REQUEST_PAYLOAD_BYTES,
            });
        }
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

    fn response_cap(&self, request: &GetBlocksRequest) -> u64 {
        self.response_cap_for_count(request.count)
            .expect("validated GetBlocks limits bound response arithmetic")
    }
}

#[cfg(test)]
mod tests {
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
}
