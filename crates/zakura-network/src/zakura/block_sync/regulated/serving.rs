//! Storage and body-byte accounting. Shared `Serve` owns scheduling and output.

use std::sync::Arc;

use futures::future::BoxFuture;
use zakura_chain::{block, serialization::ZcashSerialize};

use super::wire::{Message, Range};
use crate::zakura::{
    block_sync::{MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_RESPONSE_BYTES},
    regulation::{
        Produce, Responded, ResponseCap, ResponseSink, ServeEnd, SinkProgress, WorkLease,
    },
    wire_codec::WireError,
};

/// The source moves the lease into the actual blocking job and returns it with
/// the result. Cancellation must not release execution while that job runs.
pub(super) trait Source: Send + Sync + 'static {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>>;
}

pub(super) struct Read {
    pub(super) range: Range,
    pub(super) max_body_bytes: u32,
    pub(super) lease: WorkLease,
}

/// Blocks drop before their execution lease. The source limits the result to
/// the requested count and body bytes before retaining it.
pub(super) struct ReadResult {
    pub(super) blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
    pub(super) lease: WorkLease,
}

pub(super) struct Server<S> {
    source: Arc<S>,
    max_blocks: u32,
    max_body_bytes: u32,
}

impl<S> Server<S> {
    pub(super) fn new(
        source: Arc<S>,
        max_blocks: u32,
        max_body_bytes: u32,
    ) -> Result<Self, WireError> {
        if !(1..=MAX_BS_BLOCKS_PER_REQUEST).contains(&max_blocks)
            || !(1..=MAX_BS_RESPONSE_BYTES).contains(&max_body_bytes)
        {
            return Err(WireError::OutOfRange("GetBlocks serving limits"));
        }
        Ok(Self {
            source,
            max_blocks,
            max_body_bytes,
        })
    }
}

impl<S: Source> Produce for Server<S> {
    type Request = Range;
    type Message = Message;

    fn response_cap(&self, range: &Range) -> ResponseCap {
        Range {
            count: range.count.min(self.max_blocks),
            ..*range
        }
        .response_cap(self.max_body_bytes)
    }

    async fn produce(
        &self,
        range: &Range,
        lease: WorkLease,
        sink: ResponseSink<Message>,
    ) -> Result<Responded, ServeEnd> {
        if lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        let read_range = Range {
            count: range.count.min(self.max_blocks),
            ..*range
        };
        let result = self
            .source
            .read(Read {
                range: read_range,
                max_body_bytes: self.max_body_bytes,
                lease,
            })
            .await
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        let requested = *range;
        let max_body_bytes = self.max_body_bytes;
        // Encoding can be expensive. Await the actual job, even after session
        // cancellation, and keep the source's lease with its retained blocks.
        tokio::task::spawn_blocking(move || {
            encode_response(requested, read_range.count, max_body_bytes, result, sink)
        })
        .await
        .map_err(|error| ServeEnd::LocalFault(error.to_string()))?
    }

    fn local_failure(&self, range: &Range, sent: SinkProgress) -> Message {
        ending(*range, sent.frames)
    }
}

fn encode_response(
    requested: Range,
    max_blocks: u32,
    max_body_bytes: u32,
    result: ReadResult,
    mut sink: ResponseSink<Message>,
) -> Result<Responded, ServeEnd> {
    let mut returned = 0;
    let mut body_bytes = 0_u64;
    for (height, block, _) in &result.blocks {
        if result.lease.is_cancelled() {
            return Err(ServeEnd::Cancelled);
        }
        if returned == max_blocks || height.0 != requested.start.0 + returned {
            break;
        }
        let bytes = block
            .zcash_serialize_to_vec()
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        // usize fits u64 on the supported 64-bit targets.
        let len = bytes.len() as u64;
        if len > block::MAX_BLOCK_BYTES {
            return Err(ServeEnd::LocalFault(
                "stored block exceeds the block byte limit".into(),
            ));
        }
        if len > u64::from(max_body_bytes).saturating_sub(body_bytes) {
            break;
        }
        sink.send(&Message::Block(bytes))
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        body_bytes += len;
        returned += 1;
    }
    if result.lease.is_cancelled() {
        return Err(ServeEnd::Cancelled);
    }
    sink.finish(&ending(requested, returned))
        .map_err(|error| ServeEnd::LocalFault(error.to_string()))
}

fn ending(range: Range, returned: u32) -> Message {
    if returned == 0 {
        Message::RangeUnavailable(range)
    } else {
        Message::BlocksDone {
            start: range.start,
            returned,
        }
    }
}
